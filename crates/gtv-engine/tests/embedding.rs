//! Governed embedding search tests (B2-4): provenance, expiry, tenant isolation.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, Int64Array, RecordBatch, StringArray,
    UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field};
use gtv_catalog::embedding_schema;
use gtv_engine::GtvContext;

struct Row {
    id: u64,
    v: Vec<f32>,
    model: &'static str,
    version: &'static str,
    dim_col: u32,
    from: i64,
    to: Option<i64>,
    source_hash: &'static str,
    tenant: &'static str,
}

impl Row {
    fn new(id: u64, v: Vec<f32>) -> Self {
        let dim = v.len() as u32;
        Self {
            id,
            v,
            model: "bge",
            version: "1",
            dim_col: dim,
            from: 0,
            to: None,
            source_hash: "sha256:abc",
            tenant: "acme",
        }
    }
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
        Arc::new(StringArray::from(rows.iter().map(|r| r.version).collect::<Vec<_>>())),
        Arc::new(StringArray::from(vec![None::<&str>; n])),
        Arc::new(UInt32Array::from(rows.iter().map(|r| r.dim_col).collect::<Vec<_>>())),
        Arc::new(StringArray::from(vec!["l2"; n])),
        Arc::new(BooleanArray::from(vec![true; n])),
        Arc::new(Int64Array::from(vec![0i64; n])),
        Arc::new(Int64Array::from(rows.iter().map(|r| r.from).collect::<Vec<_>>())),
        Arc::new(Int64Array::from(rows.iter().map(|r| r.to).collect::<Vec<_>>())),
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.source_hash).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(vec!["fv1"; n])),
        Arc::new(StringArray::from(rows.iter().map(|r| r.tenant).collect::<Vec<_>>())),
        Arc::new(StringArray::from(vec![None::<&str>; n])),
    ];
    RecordBatch::try_new(embedding_schema(list_dim as u32), cols).unwrap()
}

fn ids_of(batches: &[RecordBatch]) -> Vec<u64> {
    let mut out = Vec::new();
    for b in batches {
        let ids = b
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        out.extend((0..b.num_rows()).map(|i| ids.value(i)));
    }
    out
}

fn strings_of(batches: &[RecordBatch], col: &str) -> Vec<String> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column_by_name(col)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        out.extend((0..b.num_rows()).map(|i| arr.value(i).to_string()));
    }
    out
}

fn collection() -> Vec<Row> {
    vec![
        Row::new(1, vec![1.0, 0.0]),
        Row::new(2, vec![0.0, 1.0]),
        Row::new(3, vec![1.0, 1.0]),
    ]
}

#[tokio::test]
async fn search_reports_provenance() {
    let ctx = GtvContext::new();
    ctx.register_embedding("emb", &batch(2, &collection())).unwrap();

    let out = ctx
        .sql("SELECT * FROM embedding_search('emb', '1.0,0.0', 2, 'acme')")
        .await
        .unwrap();
    assert_eq!(ids_of(&out), vec![1, 3]);
    assert_eq!(strings_of(&out, "model_id"), vec!["bge", "bge"]);
    assert_eq!(strings_of(&out, "model_version"), vec!["1", "1"]);
    assert_eq!(
        strings_of(&out, "source_hash"),
        vec!["sha256:abc", "sha256:abc"]
    );
    assert_eq!(strings_of(&out, "feature_version"), vec!["fv1", "fv1"]);
}

#[tokio::test]
async fn expired_embeddings_are_not_hit() {
    let ctx = GtvContext::new();
    let mut rows = collection();
    rows[1].to = Some(100); // entity 2 expires at t=100
    ctx.register_embedding("emb", &batch(2, &rows)).unwrap();

    // Before expiry all three are visible.
    let before = ctx
        .sql("SELECT * FROM embedding_search('emb', '0.0,1.0', 3, 'acme', 50)")
        .await
        .unwrap();
    assert_eq!(ids_of(&before).len(), 3);

    // After expiry entity 2 is gone (the query vector is now nearest to 3/1).
    let after = ctx
        .sql("SELECT * FROM embedding_search('emb', '0.0,1.0', 3, 'acme', 200)")
        .await
        .unwrap();
    assert!(!ids_of(&after).contains(&2), "expired entity 2 returned");
    assert_eq!(ids_of(&after), vec![3, 1]);
}

#[tokio::test]
async fn tenant_isolation() {
    let ctx = GtvContext::new();
    let mut rows = collection();
    rows[0].tenant = "acme";
    rows[1].tenant = "globex";
    rows[2].tenant = "globex";
    ctx.register_embedding("emb", &batch(2, &rows)).unwrap();

    let acme = ctx
        .sql("SELECT * FROM embedding_search('emb', '1.0,0.0', 5, 'acme')")
        .await
        .unwrap();
    assert_eq!(ids_of(&acme), vec![1]);

    let globex = ctx
        .sql("SELECT * FROM embedding_search('emb', '1.0,0.0', 5, 'globex')")
        .await
        .unwrap();
    assert_eq!(ids_of(&globex), vec![3, 2]);

    // Admin view sees both tenants.
    let all = ctx
        .sql("SELECT * FROM embedding_search('emb', '1.0,0.0', 5, '*')")
        .await
        .unwrap();
    assert_eq!(ids_of(&all).len(), 3);
}

#[tokio::test]
async fn register_rejects_dimension_mismatch() {
    let ctx = GtvContext::new();
    let mut rows = collection();
    rows[1].dim_col = 3; // FixedSizeList length is 2
    let err = ctx
        .register_embedding("emb", &batch(2, &rows))
        .expect_err("must reject");
    assert!(err.to_string().contains("dimension"), "{err}");
}

#[tokio::test]
async fn register_rejects_mixed_model() {
    let ctx = GtvContext::new();
    let mut rows = collection();
    rows[2].model = "other";
    let err = ctx
        .register_embedding("emb", &batch(2, &rows))
        .expect_err("must reject");
    assert!(err.to_string().contains("mixed model_id"), "{err}");
}
