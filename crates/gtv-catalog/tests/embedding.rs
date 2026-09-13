//! Tests for the standard embedding schema and governance (B2-4).

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, Int64Array, RecordBatch, StringArray,
    UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field};
use gtv_catalog::{
    active_entity_ids, embedding_schema, filter_active, read_embeddings, validate_embedding_batch,
};

struct Spec {
    id: u64,
    v: Vec<f32>,
    model: &'static str,
    version: &'static str,
    dim_col: u32,
    metric: &'static str,
    normalized: bool,
    from: i64,
    to: Option<i64>,
    source_hash: &'static str,
    tenant: &'static str,
}

impl Spec {
    fn new(id: u64, v: Vec<f32>) -> Self {
        let dim = v.len() as u32;
        Self {
            id,
            v,
            model: "text-embed",
            version: "1",
            dim_col: dim,
            metric: "l2",
            normalized: true,
            from: 0,
            to: None,
            source_hash: "sha256:abc",
            tenant: "acme",
        }
    }
}

fn batch(list_dim: i32, specs: &[Spec]) -> RecordBatch {
    let mut flat: Vec<f32> = Vec::new();
    for s in specs {
        assert_eq!(s.v.len(), list_dim as usize, "test vector width");
        flat.extend_from_slice(&s.v);
    }
    let list = FixedSizeListArray::try_new(
        Arc::new(Field::new("item", DataType::Float32, true)),
        list_dim,
        Arc::new(Float32Array::from(flat)) as ArrayRef,
        None,
    )
    .unwrap();

    let cols: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(specs.iter().map(|s| s.id).collect::<Vec<_>>())),
        Arc::new(list),
        Arc::new(StringArray::from(
            specs.iter().map(|s| s.model).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            specs.iter().map(|s| s.version).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(vec![None::<&str>; specs.len()])),
        Arc::new(UInt32Array::from(
            specs.iter().map(|s| s.dim_col).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            specs.iter().map(|s| s.metric).collect::<Vec<_>>(),
        )),
        Arc::new(BooleanArray::from(
            specs.iter().map(|s| s.normalized).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(vec![0i64; specs.len()])),
        Arc::new(Int64Array::from(
            specs.iter().map(|s| s.from).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            specs.iter().map(|s| s.to).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            specs.iter().map(|s| s.source_hash).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(vec!["fv1"; specs.len()])),
        Arc::new(StringArray::from(
            specs.iter().map(|s| s.tenant).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(vec![None::<&str>; specs.len()])),
    ];

    // Match the canonical schema field-for-field.
    let schema = embedding_schema(list_dim as u32);
    RecordBatch::try_new(schema, cols).unwrap()
}

fn rows() -> Vec<Spec> {
    vec![
        Spec::new(1, vec![1.0, 0.0]),
        Spec::new(2, vec![0.0, 1.0]),
        Spec::new(3, vec![1.0, 1.0]),
    ]
}

#[test]
fn validates_and_extracts_provenance() {
    let b = batch(2, &rows());
    let g = validate_embedding_batch(&b).unwrap();
    assert_eq!(g.model_id, "text-embed");
    assert_eq!(g.model_version, "1");
    assert_eq!(g.dimension, 2);
    assert_eq!(g.distance_metric, "l2");
    assert!(g.normalized);

    let (ids, vecs, prov) = read_embeddings(&b).unwrap();
    assert_eq!(ids, vec![1, 2, 3]);
    assert_eq!(vecs[2], vec![1.0, 1.0]);
    assert_eq!(prov[0].model_id, "text-embed");
    assert_eq!(prov[0].source_hash, "sha256:abc");
    assert_eq!(prov[0].feature_version, "fv1");
    assert_eq!(prov[0].tenant_id, "acme");
}

#[test]
fn rejects_dimension_mismatch() {
    let mut r = rows();
    r[1].dim_col = 3; // FixedSizeList length is 2
    let err = validate_embedding_batch(&batch(2, &r)).unwrap_err();
    assert!(err.to_string().contains("dimension"), "{err}");
}

#[test]
fn rejects_mixed_model() {
    let mut r = rows();
    r[2].model = "other-embed";
    let err = validate_embedding_batch(&batch(2, &r)).unwrap_err();
    assert!(err.to_string().contains("mixed model_id"), "{err}");
}

#[test]
fn rejects_mixed_metric() {
    let mut r = rows();
    r[1].metric = "cosine";
    let err = validate_embedding_batch(&batch(2, &r)).unwrap_err();
    assert!(err.to_string().contains("mixed distance_metric"), "{err}");
}

#[test]
fn rejects_empty_source_hash() {
    let mut r = rows();
    r[1].source_hash = "";
    let err = validate_embedding_batch(&batch(2, &r)).unwrap_err();
    assert!(err.to_string().contains("source_hash"), "{err}");
}

#[test]
fn expiry_and_tenant_filter() {
    let mut r = rows();
    r[0].tenant = "acme";
    r[1].tenant = "globex";
    r[2].tenant = "acme";
    r[2].from = 0;
    r[2].to = Some(100); // expired at as_of = 200
    let b = batch(2, &r);

    // Tenant acme, active: only entity 1 (entity 3 expired).
    assert_eq!(active_entity_ids(&b, 200, Some("acme")).unwrap(), vec![1]);
    // No tenant filter: entities 1 + 2 (3 still expired).
    assert_eq!(active_entity_ids(&b, 200, None).unwrap(), vec![1, 2]);
    // Before expiry: entity 3 visible to acme.
    assert_eq!(active_entity_ids(&b, 50, Some("acme")).unwrap(), vec![1, 3]);
    // Cross-tenant isolation: globex never sees acme rows.
    assert_eq!(active_entity_ids(&b, 50, Some("globex")).unwrap(), vec![2]);

    let filtered = filter_active(&b, 200, Some("acme")).unwrap();
    assert_eq!(filtered.num_rows(), 1);
}
