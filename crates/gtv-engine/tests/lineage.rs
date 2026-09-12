//! Lineage tests for `GtvContext::execute_with_lineage` (B2-3).

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use gtv_catalog::{CommitOp, CommitOptions, FsCatalog, NewFile, PartitionSpec, SnapshotId, TableId, TableRef};
use gtv_engine::{ExecutionOptions, GtvContext, ReplayError};

fn batch() -> (Arc<Schema>, RecordBatch) {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let b = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))],
    )
    .unwrap();
    (schema, b)
}

#[tokio::test]
async fn records_source_tables_and_output_checksum() {
    let ctx = GtvContext::new();
    let (schema, b) = batch();
    ctx.register_batches("t", schema, vec![b]).unwrap();
    let table_id = TableId::new();
    let snapshot_id = SnapshotId::new();
    ctx.set_table_source(
        "t",
        TableRef {
            table_id,
            snapshot_id,
            schema_version: 1,
            table_name: "t".into(),
        },
    );

    let (_batches, rec) = ctx
        .execute_with_lineage("SELECT count(*) AS n FROM t", ExecutionOptions::default())
        .await
        .unwrap();

    assert_eq!(rec.output_rows, 1);
    assert!(!rec.output_checksum.is_empty());
    assert_eq!(rec.query_hash.len(), 64); // blake3 hex
    assert_eq!(rec.source_tables.len(), 1);
    assert_eq!(rec.source_tables[0].table_id, table_id);
    assert_eq!(rec.source_tables[0].snapshot_id, snapshot_id);

    // Deterministic re-run yields the same output checksum.
    let (_b2, rec2) = ctx
        .execute_with_lineage("SELECT count(*) AS n FROM t", ExecutionOptions::default())
        .await
        .unwrap();
    assert_eq!(rec.output_checksum, rec2.output_checksum);
}

#[tokio::test]
async fn attach_model_index_and_cutoff() {
    use gtv_catalog::{IndexRef, ModelRef};
    let ctx = GtvContext::new();
    let (schema, b) = batch();
    ctx.register_batches("t", schema, vec![b]).unwrap();

    let opts = ExecutionOptions {
        model_versions: vec![ModelRef {
            model_id: "gbdt".into(),
            version: "3".into(),
        }],
        index_snapshots: vec![IndexRef {
            index_id: gtv_catalog::IndexId::new(),
            version: 2,
            metric: "cosine".into(),
            dim: 768,
        }],
        scenario_version: Some("STRESS-2024".into()),
        business_cutoff: Some(1_700_000_000_000_000_000),
        runtime_params: serde_json::json!({"threshold": 0.75}),
    };
    let (_b, rec) = ctx
        .execute_with_lineage("SELECT * FROM t", opts)
        .await
        .unwrap();
    assert_eq!(rec.model_versions.len(), 1);
    assert_eq!(rec.index_snapshots[0].dim, 768);
    assert_eq!(rec.scenario_version.as_deref(), Some("STRESS-2024"));
    assert_eq!(rec.business_cutoff, Some(1_700_000_000_000_000_000));
    assert_eq!(rec.output_rows, 3);
}

fn tmp_root(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("gtv_lineage_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn schema_v() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
}

fn batch_v(values: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        schema_v(),
        vec![Arc::new(Int64Array::from(values.to_vec()))],
    )
    .unwrap()
}

#[tokio::test]
async fn replay_uses_pinned_snapshot() {
    let root = tmp_root("pin");
    let cat = FsCatalog::open(&root).unwrap();
    let schema = schema_v();
    let table = cat
        .create_table("t", schema.clone(), PartitionSpec::single())
        .unwrap();

    // v1: first committed snapshot.
    cat.commit(
        table,
        CommitOp::Append,
        vec![NewFile::unpartitioned(batch_v(&[1, 2, 3]))],
        &CommitOptions::default(),
    )
    .unwrap();
    let snap1 = cat.latest(table).unwrap().unwrap();

    let ctx = GtvContext::new();
    ctx.register_batches("t", schema.clone(), vec![batch_v(&[1, 2, 3])])
        .unwrap();
    ctx.set_table_source(
        "t",
        TableRef {
            table_id: table,
            snapshot_id: snap1,
            schema_version: 1,
            table_name: "t".into(),
        },
    );

    let (_b, rec) = ctx
        .execute_with_lineage("SELECT v FROM t", ExecutionOptions::default())
        .await
        .unwrap();
    assert_eq!(rec.source_tables.len(), 1);
    cat.append_lineage(&rec).unwrap();
    let recorded_checksum = rec.output_checksum.clone();

    // v2: the table advances; the live context now holds the new data.
    cat.commit(
        table,
        CommitOp::Overwrite,
        vec![NewFile::unpartitioned(batch_v(&[9, 9, 9, 9]))],
        &CommitOptions::default(),
    )
    .unwrap();
    ctx.register_batches("t", schema.clone(), vec![batch_v(&[9, 9, 9, 9])])
        .unwrap();

    // Replay must reload snapshot 1 and reproduce the original checksum.
    let out = ctx.replay(&cat, rec.execution_id, false).await.unwrap();
    let rows: i64 = out.iter().map(|b| b.num_rows() as i64).sum();
    assert_eq!(rows, 3, "replay used a newer snapshot");
    let checksum = gtv_engine::lineage::output_checksum(&out);
    assert_eq!(checksum, recorded_checksum);

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn replay_refuses_nondeterministic_udf() {
    let root = tmp_root("nondet");
    let cat = FsCatalog::open(&root).unwrap();
    let ctx = GtvContext::new();
    let (schema, b) = batch();
    ctx.register_batches("t", schema, vec![b]).unwrap();

    let (_b, rec) = ctx
        .execute_with_lineage("SELECT random() AS r FROM t", ExecutionOptions::default())
        .await
        .unwrap();
    assert!(
        rec.udf_versions
            .iter()
            .any(|u| u.name == "random" && u.nondeterministic),
        "random() should be flagged nondeterministic: {:?}",
        rec.udf_versions
    );
    cat.append_lineage(&rec).unwrap();

    let err = ctx
        .replay(&cat, rec.execution_id, false)
        .await
        .expect_err("replay must refuse random()");
    assert!(matches!(err, ReplayError::Nondeterministic(_)), "{err}");

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn lineage_is_queryable_via_sql() {
    let root = tmp_root("sql");
    let cat = FsCatalog::open(&root).unwrap();
    let ctx = GtvContext::new();
    let (schema, b) = batch();
    ctx.register_batches("t", schema, vec![b]).unwrap();

    let (_b, rec) = ctx
        .execute_with_lineage("SELECT count(*) AS n FROM t", ExecutionOptions::default())
        .await
        .unwrap();
    cat.append_lineage(&rec).unwrap();

    ctx.register_lineage(&cat.lineage_records().unwrap()).unwrap();
    let rows = ctx
        .sql("SELECT execution_id, output_rows FROM gtv_lineage")
        .await
        .unwrap();
    assert_eq!(rows[0].num_rows(), 1);

    let _ = std::fs::remove_dir_all(&root);
}
