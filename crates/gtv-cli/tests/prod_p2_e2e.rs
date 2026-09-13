//! prod_p2 end-to-end demo: load → catalog commit → governed embedding index →
//! query → lineage → DQ gate → replay.
//!
//! Exercises B2-1..B2-5 together against a temporary `GTV_HOME`-style catalog.

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int64Array,
    RecordBatch, StringArray, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use gtv_catalog::{
    embedding_schema, CommitOp, CommitOptions, DqRule, FsCatalog, GateDecisionRecord, NewFile,
    OverrideRecord, PartitionSpec, TableRef,
};
use gtv_core::{Metric, VectorIndex};
use gtv_engine::{ExecutionOptions, GtvContext};
use gtv_index::BuildOptions;
use gtv_index_store::{build_and_save, EmbeddingIndexSpec, IndexStore};

fn root(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!("gtv_e2e_{}_{}", tag, std::process::id()))
}

fn risk_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("v", DataType::Float64, false),
        Field::new("ts", DataType::Int64, false),
    ]))
}

fn risk_batch() -> RecordBatch {
    RecordBatch::try_new(
        risk_schema(),
        vec![
            Arc::new(UInt64Array::from(vec![1u64, 2, 3])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])) as ArrayRef,
            Arc::new(Int64Array::from(vec![10i64, 11, 12])) as ArrayRef,
        ],
    )
    .unwrap()
}

fn embedding_batch() -> RecordBatch {
    let v: Vec<f32> = vec![1.0, 0.0, 0.0, 1.0, 1.0, 1.0];
    let n = 3;
    let list = FixedSizeListArray::try_new(
        Arc::new(Field::new("item", DataType::Float32, true)),
        2,
        Arc::new(Float32Array::from(v)) as ArrayRef,
        None,
    )
    .unwrap();
    let cols: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(vec![1u64, 2, 3])) as ArrayRef,
        Arc::new(list),
        Arc::new(StringArray::from(vec!["m"; n])) as ArrayRef,
        Arc::new(StringArray::from(vec!["1"; n])) as ArrayRef,
        Arc::new(StringArray::from(vec![None::<&str>; n])) as ArrayRef,
        Arc::new(UInt32Array::from(vec![2u32; n])) as ArrayRef,
        Arc::new(StringArray::from(vec!["l2"; n])) as ArrayRef,
        Arc::new(BooleanArray::from(vec![true; n])) as ArrayRef,
        Arc::new(Int64Array::from(vec![0i64; n])) as ArrayRef,
        Arc::new(Int64Array::from(vec![0i64; n])) as ArrayRef,
        Arc::new(Int64Array::from(vec![None::<i64>; n])) as ArrayRef,
        Arc::new(StringArray::from(vec!["sha256:abc"; n])) as ArrayRef,
        Arc::new(StringArray::from(vec!["fv1"; n])) as ArrayRef,
        Arc::new(StringArray::from(vec!["acme"; n])) as ArrayRef,
        Arc::new(StringArray::from(vec![None::<&str>; n])) as ArrayRef,
    ];
    RecordBatch::try_new(embedding_schema(2), cols).unwrap()
}

#[tokio::test]
async fn prod_p2_end_to_end() {
    let home = root("home");
    let idx = root("idx");
    let _ = fs::remove_dir_all(&home);
    let _ = fs::remove_dir_all(&idx);

    // ---- B2-1: load + atomic catalog commit --------------------------------
    let cat = FsCatalog::open(&home).unwrap();
    let table = cat
        .create_table("risk", risk_schema(), PartitionSpec::single())
        .unwrap();
    let snap = cat
        .commit(
            table,
            CommitOp::Append,
            vec![NewFile::unpartitioned(risk_batch())],
            &CommitOptions::default(),
        )
        .unwrap();
    let files = cat.files(table, snap).unwrap();
    assert_eq!(files.iter().map(|f| f.row_count).sum::<u64>(), 3);

    let ctx = GtvContext::new();
    ctx.register_batches("risk", risk_schema(), vec![risk_batch()])
        .unwrap();
    ctx.set_table_source(
        "risk",
        TableRef {
            table_id: table,
            snapshot_id: snap,
            schema_version: 1,
            table_name: "risk".into(),
        },
    );

    // ---- B2-4 + B2-2: governed embedding index -----------------------------
    let emb = embedding_batch();
    ctx.register_embedding("emb", &emb).unwrap();

    let spec = EmbeddingIndexSpec {
        name: "emb_idx".into(),
        table: Some(table),
        corpus_snapshot_id: Some(snap),
        model_id: "m".into(),
        model_version: "1".into(),
        feature_version: "fv1".into(),
        metric: Metric::L2,
        dim: 2,
        normalized: true,
        build_options: BuildOptions::Flat,
    };
    let store = IndexStore::open(&idx).unwrap();
    let (version, manifest) = build_and_save(
        &store,
        &spec,
        &emb,
        gtv_catalog::schema::now_ns(),
        Some("acme"),
    )
    .unwrap();
    assert_eq!(version.version, 1);
    assert_eq!(manifest.row_count, 3);
    assert_eq!(manifest.feature_version, "fv1");

    // Reload from disk (no re-insert) and query it.
    let loaded = store.load("emb_idx", None).unwrap();
    let hits = loaded.index.search(&[1.0, 0.0], 2, None).unwrap();
    assert_eq!(hits[0].id, 1);
    ctx.register_any_index("emb_idx", loaded.index.clone());

    // ---- query surface -----------------------------------------------------
    let ann = ctx
        .sql("SELECT id FROM ann('emb_idx', '1.0,0.0', 2)")
        .await
        .unwrap();
    let ids = ann[0]
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(ids.value(0), 1);

    let es = ctx
        .sql("SELECT id, model_version FROM embedding_search('emb', '1.0,0.0', 2, 'acme')")
        .await
        .unwrap();
    assert_eq!(es[0].num_rows(), 2);

    // ---- B2-3: lineage + replay -------------------------------------------
    let (out, record) = ctx
        .execute_with_lineage("SELECT count(*) AS n FROM risk", ExecutionOptions::default())
        .await
        .unwrap();
    assert_eq!(record.source_tables.len(), 1, "risk snapshot pinned");
    cat.append_lineage(&record).unwrap();
    ctx.register_lineage(&cat.lineage_records().unwrap())
        .unwrap();

    let lineage_rows = ctx
        .sql("SELECT execution_id FROM gtv_lineage")
        .await
        .unwrap();
    assert_eq!(lineage_rows[0].num_rows(), 1);

    let replayed = ctx.replay(&cat, record.execution_id, false).await.unwrap();
    assert_eq!(
        gtv_engine::lineage::output_checksum(&replayed),
        record.output_checksum,
        "replay must be byte-identical"
    );
    assert_eq!(out[0].num_rows(), 1);

    // ---- B2-5: DQ gate + override audit -----------------------------------
    let rules = vec![
        DqRule::Completeness {
            column: "v".into(),
            min_ratio: 0.99,
        },
        DqRule::Range {
            column: "v".into(),
            min: Some(0.0),
            max: Some(10.0),
        },
        DqRule::Freshness {
            event_time_col: "ts".into(),
            max_lag_ns: 1,
        },
    ];
    let (decision, _) = ctx.evaluate_dq("risk", &rules).unwrap();
    assert!(!decision.pass, "stale ts must fail freshness");

    // Record the decision against the execution id (auditable together).
    cat.append_gate_decision(&GateDecisionRecord {
        execution_id: Some(record.execution_id),
        target: "risk".into(),
        snapshot_id: Some(snap),
        pass: decision.pass,
        failures: decision.failures.clone(),
        overridden: false,
        override_reason: None,
        override_approver: None,
        decided_at: gtv_catalog::schema::now_ns(),
    })
    .unwrap();
    assert_eq!(
        cat.gate_decisions().unwrap()[0].execution_id,
        Some(record.execution_id)
    );

    // A blocking failure is waived only by a matching override.
    assert_eq!(
        gtv_engine::dq::blocking_failures(&decision, &[], "risk").len(),
        1
    );
    cat.append_override(&OverrideRecord::new(
        "risk",
        "freshness",
        "historical backfill, approved",
        "risk-owner",
        Some(record.execution_id),
    ))
    .unwrap();
    assert!(gtv_engine::dq::blocking_failures(&decision, &cat.overrides().unwrap(), "risk")
        .is_empty());

    let _ = fs::remove_dir_all(&home);
    let _ = fs::remove_dir_all(&idx);
}
