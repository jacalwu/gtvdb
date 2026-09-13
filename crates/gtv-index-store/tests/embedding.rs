//! Embedding → index integration tests (B2-4): spec/data mismatch rejection.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, Int64Array, RecordBatch, StringArray,
    UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field};
use gtv_catalog::embedding_schema;
use gtv_core::{Metric, VectorIndex};
use gtv_index::BuildOptions;
use gtv_index_store::{
    build_and_save, build_index_from_embeddings, EmbeddingIndexError, EmbeddingIndexSpec,
    IndexStore,
};

struct Row {
    id: u64,
    v: Vec<f32>,
    model: &'static str,
    metric: &'static str,
    dim_col: u32,
}

fn batch(list_dim: i32, rows: &[Row]) -> RecordBatch {
    let mut flat: Vec<f32> = Vec::new();
    for r in rows {
        assert_eq!(r.v.len(), list_dim as usize);
        flat.extend_from_slice(&r.v);
    }
    let list = FixedSizeListArray::try_new(
        Arc::new(Field::new("item", DataType::Float32, true)),
        list_dim,
        Arc::new(Float32Array::from(flat)) as ArrayRef,
        None,
    )
    .unwrap();
    let n = rows.len();
    let cols: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(rows.iter().map(|r| r.id).collect::<Vec<_>>())),
        Arc::new(list),
        Arc::new(StringArray::from(rows.iter().map(|r| r.model).collect::<Vec<_>>())),
        Arc::new(StringArray::from(vec!["1"; n])),
        Arc::new(StringArray::from(vec![None::<&str>; n])),
        Arc::new(UInt32Array::from(rows.iter().map(|r| r.dim_col).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|r| r.metric).collect::<Vec<_>>())),
        Arc::new(BooleanArray::from(vec![true; n])),
        Arc::new(Int64Array::from(vec![0i64; n])),
        Arc::new(Int64Array::from(vec![0i64; n])),
        Arc::new(Int64Array::from(vec![None::<i64>; n])),
        Arc::new(StringArray::from(vec!["sha256:x"; n])),
        Arc::new(StringArray::from(vec!["fv1"; n])),
        Arc::new(StringArray::from(vec!["acme"; n])),
        Arc::new(StringArray::from(vec![None::<&str>; n])),
    ];
    RecordBatch::try_new(embedding_schema(list_dim as u32), cols).unwrap()
}

fn rows() -> Vec<Row> {
    vec![
        Row { id: 1, v: vec![1.0, 0.0], model: "bge", metric: "l2", dim_col: 2 },
        Row { id: 2, v: vec![0.0, 1.0], model: "bge", metric: "l2", dim_col: 2 },
    ]
}

fn spec() -> EmbeddingIndexSpec {
    EmbeddingIndexSpec {
        name: "emb".into(),
        table: None,
        corpus_snapshot_id: None,
        model_id: "bge".into(),
        model_version: "1".into(),
        feature_version: "fv1".into(),
        metric: Metric::L2,
        dim: 2,
        normalized: true,
        build_options: BuildOptions::Flat,
    }
}

#[test]
fn builds_when_spec_matches() {
    let b = batch(2, &rows());
    let (index, gov) = build_index_from_embeddings(&spec(), &b, i64::MAX, None).unwrap();
    assert_eq!(index.dim(), 2);
    assert_eq!(index.metric(), Metric::L2);
    assert_eq!(gov.model_id, "bge");
}

#[test]
fn rejects_dimension_mismatch() {
    let mut s = spec();
    s.dim = 3;
    let err = build_index_from_embeddings(&s, &batch(2, &rows()), i64::MAX, None).unwrap_err();
    assert!(matches!(err, EmbeddingIndexError::Dimension { spec: 3, data: 2 }), "{err}");
}

#[test]
fn rejects_model_mismatch() {
    let mut s = spec();
    s.model_id = "other".into();
    let err = build_index_from_embeddings(&s, &batch(2, &rows()), i64::MAX, None).unwrap_err();
    assert!(matches!(err, EmbeddingIndexError::Model { .. }), "{err}");
}

#[test]
fn rejects_metric_mismatch() {
    let mut s = spec();
    s.metric = Metric::Cosine;
    let err = build_index_from_embeddings(&s, &batch(2, &rows()), i64::MAX, None).unwrap_err();
    assert!(matches!(err, EmbeddingIndexError::Metric { .. }), "{err}");
}

#[test]
fn rejects_mixed_models_in_batch() {
    let mut r = rows();
    r[1].model = "other";
    let err = build_index_from_embeddings(&spec(), &batch(2, &r), i64::MAX, None).unwrap_err();
    assert!(matches!(err, EmbeddingIndexError::Governance(_)), "{err}");
}

#[test]
fn save_persists_governance() {
    let dir = std::env::temp_dir().join(format!("gtv_emb_idx_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let store = IndexStore::open(&dir).unwrap();
    let (v, manifest) = build_and_save(&store, &spec(), &batch(2, &rows()), i64::MAX, None).unwrap();
    assert_eq!(v.version, 1);
    assert_eq!(manifest.model_id, "bge");
    assert_eq!(manifest.feature_version, "fv1");
    assert!(manifest.normalized);
    assert_eq!(manifest.row_count, 2);
    let _ = std::fs::remove_dir_all(&dir);
}
