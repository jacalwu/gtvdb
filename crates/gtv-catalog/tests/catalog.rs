//! End-to-end tests for the filesystem catalog: atomic commits, snapshot
//! isolation, idempotent replay, schema evolution and event-time pruning.

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use gtv_catalog::{
    CommitOp, CommitOptions, FsCatalog, NewFile, PartitionSpec, PartitionValue, ScanFilter,
    SchemaChange,
};

fn root(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("gtv_cat_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("event_time", DataType::Int64, false),
    ]))
}

fn batch(ids: &[u64], values: &[f64], times: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(UInt64Array::from(ids.to_vec())),
            Arc::new(Float64Array::from(values.to_vec())),
            Arc::new(Int64Array::from(times.to_vec())),
        ],
    )
    .unwrap()
}

fn fresh(tag: &str) -> (FsCatalog, gtv_catalog::TableId) {
    let cat = FsCatalog::open(root(tag)).unwrap();
    let table = cat
        .create_table("trades", schema(), PartitionSpec::single())
        .unwrap();
    cat.set_event_time_column(table, Some("event_time".into()))
        .unwrap();
    (cat, table)
}

#[test]
fn commit_append_is_immutable_and_readable() {
    let (cat, table) = fresh("append");
    assert!(cat.latest(table).unwrap().is_none());

    let s1 = cat
        .commit(
            table,
            CommitOp::Append,
            vec![NewFile::unpartitioned(batch(&[1, 2, 3], &[1.0, 2.0, 3.0], &[10, 20, 30]))],
            &CommitOptions::default(),
        )
        .unwrap();
    let f1 = cat.files(table, s1).unwrap();
    assert_eq!(f1.len(), 1);
    assert_eq!(f1[0].row_count, 3);
    assert!(!f1[0].checksum.is_empty());
    assert!(std::path::Path::new(&f1[0].path).exists());

    let s2 = cat
        .commit(
            table,
            CommitOp::Append,
            vec![NewFile::unpartitioned(batch(&[4], &[4.0], &[40]))],
            &CommitOptions::default(),
        )
        .unwrap();
    assert_eq!(cat.files(table, s2).unwrap().len(), 2);
    // The earlier snapshot is immutable: still one file.
    assert_eq!(cat.files(table, s1).unwrap().len(), 1);
    assert_eq!(cat.latest(table).unwrap(), Some(s2));
}

#[test]
fn snapshot_as_of_replays_system_time() {
    let (cat, table) = fresh("sys_asof");
    let s1 = cat
        .commit(
            table,
            CommitOp::Append,
            vec![NewFile::unpartitioned(batch(&[1], &[1.0], &[10]))],
            &CommitOptions::default(),
        )
        .unwrap();
    // Distinct system timestamps (commit `created_at` is wall-clock ns).
    std::thread::sleep(std::time::Duration::from_millis(2));
    let s2 = cat
        .commit(
            table,
            CommitOp::Append,
            vec![NewFile::unpartitioned(batch(&[2], &[2.0], &[20]))],
            &CommitOptions::default(),
        )
        .unwrap();

    let snaps = cat.snapshots(table).unwrap();
    assert_eq!(snaps.len(), 2);
    assert_eq!(snaps[0].snapshot_id, s1);
    assert_eq!(snaps[1].snapshot_id, s2);
    let (t1, t2) = (snaps[0].created_at, snaps[1].created_at);
    assert!(t1 < t2, "system timestamps must be ordered: {t1} !< {t2}");

    // Before the first commit nothing was known; each cut pins its snapshot.
    assert_eq!(cat.snapshot_as_of(table, t1 - 1).unwrap(), None);
    assert_eq!(cat.snapshot_as_of(table, t1).unwrap(), Some(s1));
    assert_eq!(cat.snapshot_as_of(table, t2 - 1).unwrap(), Some(s1));
    assert_eq!(cat.snapshot_as_of(table, t2).unwrap(), Some(s2));
    assert_eq!(cat.snapshot_as_of(table, i64::MAX).unwrap(), Some(s2));
    // The old cut still resolves to the old snapshot's files.
    assert_eq!(cat.files(table, s1).unwrap().len(), 1);
    assert_eq!(cat.files(table, s2).unwrap().len(), 2);
}

#[test]
fn overwrite_replaces_files() {
    let (cat, table) = fresh("overwrite");
    cat.commit(
        table,
        CommitOp::Append,
        vec![NewFile::unpartitioned(batch(&[1], &[1.0], &[1]))],
        &CommitOptions::default(),
    )
    .unwrap();
    let s2 = cat
        .commit(
            table,
            CommitOp::Overwrite,
            vec![NewFile::unpartitioned(batch(&[2, 3], &[2.0, 3.0], &[2, 3]))],
            &CommitOptions::default(),
        )
        .unwrap();
    let files = cat.files(table, s2).unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].row_count, 2);
}

#[test]
fn idempotent_replay_returns_same_snapshot() {
    let (cat, table) = fresh("idem");
    let opts = CommitOptions {
        idempotency_key: Some("batch-1".into()),
        event_time_column: None,
    };
    let a = cat
        .commit(
            table,
            CommitOp::Append,
            vec![NewFile::unpartitioned(batch(&[1], &[1.0], &[1]))],
            &opts,
        )
        .unwrap();
    let b = cat
        .commit(
            table,
            CommitOp::Append,
            vec![NewFile::unpartitioned(batch(&[1], &[1.0], &[1]))],
            &opts,
        )
        .unwrap();
    assert_eq!(a, b, "replaying the same key must not create a new snapshot");
    assert_eq!(cat.files(table, a).unwrap().len(), 1);
}

#[test]
fn event_time_pruning_selects_overlapping_files() {
    let (cat, table) = fresh("prune");
    let s = cat
        .commit(
            table,
            CommitOp::Append,
            vec![
                NewFile::unpartitioned(batch(&[1], &[1.0], &[100])),
                NewFile::unpartitioned(batch(&[2], &[2.0], &[1000])),
            ],
            &CommitOptions::default(),
        )
        .unwrap();
    assert_eq!(cat.files(table, s).unwrap().len(), 2);

    let early = cat
        .scan(
            table,
            s,
            &ScanFilter {
                event_time_min: Some(0),
                event_time_max: Some(500),
            },
        )
        .unwrap();
    assert_eq!(early.len(), 1);
    assert_eq!(early[0].row_count, 1);

    let late = cat
        .scan(
            table,
            s,
            &ScanFilter {
                event_time_min: Some(900),
                event_time_max: None,
            },
        )
        .unwrap();
    assert_eq!(late.len(), 1);
}

#[test]
fn schema_evolution_and_incompatible_rejection() {
    let (cat, table) = fresh("schema");
    let v2 = cat
        .evolve_schema(
            table,
            SchemaChange::AddColumn {
                field: Field::new("tag", DataType::Utf8, true),
                default: None,
            },
        )
        .unwrap();
    assert_eq!(v2.0, 2);
    let s = cat.schema(table, v2).unwrap();
    assert!(s.field_with_name("tag").is_ok());
    // v1 is still readable.
    let v1 = cat.schema(table, gtv_catalog::SchemaVersion(1)).unwrap();
    assert!(v1.field_with_name("tag").is_err());

    // Dropping a column is incompatible.
    let bad = cat.evolve_schema(
        table,
        SchemaChange::RenameColumn {
            from: "missing".into(),
            to: "x".into(),
        },
    );
    assert!(bad.is_err());
}

#[test]
fn partitions_are_recorded_and_pathed() {
    let (cat, table) = fresh("part");
    let s = cat
        .commit(
            table,
            CommitOp::Append,
            vec![NewFile {
                batch: batch(&[1], &[1.0], &[1]),
                partition: vec![PartitionValue::Str("2024-01-01".into())],
            }],
            &CommitOptions::default(),
        )
        .unwrap();
    let f = cat.files(table, s).unwrap();
    assert_eq!(f[0].partition.len(), 1);
    assert!(f[0].path.contains("2024-01-01"));
}

#[test]
fn tmp_files_are_not_visible() {
    let (cat, table) = fresh("isolation");
    let s = cat
        .commit(
            table,
            CommitOp::Append,
            vec![NewFile::unpartitioned(batch(&[1], &[1.0], &[1]))],
            &CommitOptions::default(),
        )
        .unwrap();
    // Drop a stray temp file into the data directory; readers use the manifest,
    // so it must be ignored.
    let real = &cat.files(table, s).unwrap()[0].path;
    let stray = format!("{real}.tmp");
    std::fs::write(&stray, b"garbage").unwrap();
    assert_eq!(cat.files(table, s).unwrap().len(), 1);
    let _ = std::fs::remove_file(&stray);
}

#[test]
fn lineage_append_and_lookup() {
    let cat = FsCatalog::open(root("lineage")).unwrap();
    let mut rec = gtv_catalog::ExecutionRecord::begin("SELECT 1", "0.1.0");
    rec.finish("deadbeef".into(), 1);
    let id = rec.execution_id;
    cat.append_lineage(&rec).unwrap();

    let got = cat.lineage(id).unwrap().expect("record found");
    assert_eq!(got.output_checksum, "deadbeef");
    assert_eq!(got.output_rows, 1);
    assert_eq!(cat.lineage_records().unwrap().len(), 1);
}
