//! B2-1 crash-injection: a reader restarted after an interrupted commit must
//! only ever see fully-committed snapshots.
//!
//! The commit protocol is: write data files → append file metadata → write the
//! snapshot manifest → append the snapshot log → atomically update the `latest`
//! version hint. Only that last step publishes. These tests forge the
//! intermediate on-disk state a crash could leave behind and assert the reader
//! stays isolated on the previous snapshot.

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use gtv_catalog::{
    CommitOp, CommitOptions, DataFileId, FsCatalog, NewFile, PartitionSpec, SnapshotId,
};

fn root(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!("gtv_crash_{}_{}", tag, std::process::id()))
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("v", DataType::Float64, false),
    ]))
}

fn batch(ids: &[u64]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(UInt64Array::from(ids.to_vec())) as ArrayRef,
            Arc::new(Float64Array::from(
                ids.iter().map(|i| *i as f64).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .unwrap()
}

#[test]
fn interrupted_commit_is_invisible_after_restart() {
    let dir = root("interrupted");
    let _ = fs::remove_dir_all(&dir);
    let cat = FsCatalog::open(&dir).unwrap();
    let table = cat
        .create_table("t", schema(), PartitionSpec::single())
        .unwrap();

    // One fully committed snapshot: 2 rows.
    let s1 = cat
        .commit(
            table,
            CommitOp::Append,
            vec![NewFile::unpartitioned(batch(&[1, 2]))],
            &CommitOptions::default(),
        )
        .unwrap();
    let committed = cat.files(table, s1).unwrap();
    assert_eq!(committed.len(), 1);
    assert_eq!(committed.iter().map(|f| f.row_count).sum::<u64>(), 2);
    drop(cat);

    // Forge a crash after every write *except* the atomic version-hint update:
    // a new data-file record, its manifest and a snapshot-log entry all exist.
    let tdir = dir.join("metadata").join(table.to_string());
    let mut orphan = cat_files_first(&tdir);
    orphan.file_id = DataFileId::new();
    orphan.row_count = 5;

    let mut files_log = fs::read_to_string(tdir.join("files.jsonl")).unwrap();
    files_log.push_str(&serde_json::to_string(&orphan).unwrap());
    files_log.push('\n');
    fs::write(tdir.join("files.jsonl"), files_log).unwrap();

    let mut orphan_snap = serde_json::from_slice::<gtv_catalog::Snapshot>(
        &fs::read(tdir.join("manifests").join(format!("{s1}.json"))).unwrap(),
    )
    .unwrap();
    orphan_snap.snapshot_id = SnapshotId::new();
    orphan_snap.parent = Some(s1);
    orphan_snap.files = vec![orphan.file_id];
    orphan_snap.op = CommitOp::Append;
    let orphan_id = orphan_snap.snapshot_id;
    fs::write(
        tdir.join("manifests")
            .join(format!("{orphan_id}.json")),
        serde_json::to_vec_pretty(&orphan_snap).unwrap(),
    )
    .unwrap();
    let mut snaps = fs::read_to_string(tdir.join("snapshots.jsonl")).unwrap();
    snaps.push_str(&serde_json::to_string(&orphan_snap).unwrap());
    snaps.push('\n');
    fs::write(tdir.join("snapshots.jsonl"), snaps).unwrap();
    // NB: `latest` deliberately left pointing at s1.

    // Restart and verify reader isolation.
    let cat = FsCatalog::open(&dir).unwrap();
    assert_eq!(cat.latest(table).unwrap(), Some(s1), "hint must be unchanged");
    let files = cat.files(table, s1).unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files.iter().map(|f| f.row_count).sum::<u64>(), 2);
    // The orphan manifest is on disk but unreachable through `latest`.
    assert!(cat.snapshot(table, orphan_id).is_ok());

    // The next commit must build on the committed snapshot, not the orphan.
    let s2 = cat
        .commit(
            table,
            CommitOp::Append,
            vec![NewFile::unpartitioned(batch(&[3]))],
            &CommitOptions::default(),
        )
        .unwrap();
    let snap2 = cat.snapshot(table, s2).unwrap();
    assert_eq!(snap2.parent, Some(s1));
    assert_eq!(snap2.files.len(), 2, "s1's file + the new one");

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn torn_version_hint_is_surfaced_as_corruption() {
    let dir = root("torn");
    let _ = fs::remove_dir_all(&dir);
    let cat = FsCatalog::open(&dir).unwrap();
    let table = cat
        .create_table("t", schema(), PartitionSpec::single())
        .unwrap();
    let _ = cat
        .commit(
            table,
            CommitOp::Append,
            vec![NewFile::unpartitioned(batch(&[1]))],
            &CommitOptions::default(),
        )
        .unwrap();
    // A partial write would be caught by atomic rename, but if a foreign/torn
    // hint ever appears the reader must refuse rather than guess a snapshot.
    fs::write(
        dir.join("metadata").join(table.to_string()).join("latest"),
        b"not-a-snapshot-id",
    )
    .unwrap();
    let cat = FsCatalog::open(&dir).unwrap();
    assert!(matches!(
        cat.latest(table),
        Err(gtv_catalog::CatalogError::Corrupt(_))
    ));
    let _ = fs::remove_dir_all(&dir);
}

fn cat_files_first(tdir: &std::path::Path) -> gtv_catalog::DataFile {
    let text = fs::read_to_string(tdir.join("files.jsonl")).unwrap();
    serde_json::from_str(text.lines().next().unwrap()).unwrap()
}
