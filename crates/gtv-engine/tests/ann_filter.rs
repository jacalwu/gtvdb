//! B3-3: filter-aware ANN via the SQL `ann(...)` / `ann_explain(...)` surface.

use arrow::array::{BooleanArray, Float64Array, StringArray, UInt64Array};
use gtv_core::{Metric, VectorIndex};
use gtv_engine::GtvContext;
use gtv_index::{AnyIndex, BuildOptions, FlatIndex};

struct Lcg(u64);
impl Lcg {
    fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }
    fn f32(&mut self) -> f32 {
        (self.next_u64() >> 40) as f32 / (1u64 << 24) as f32
    }
}

fn corpus(n: usize, dim: usize) -> (Vec<u64>, Vec<Vec<f32>>) {
    let mut rng = Lcg(42);
    let ids = (0..n as u64).collect();
    let vs = (0..n)
        .map(|_| (0..dim).map(|_| rng.f32()).collect())
        .collect();
    (ids, vs)
}

fn qstr(v: &[f32]) -> String {
    v.iter()
        .map(|x| x.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn ids_of(batches: &[arrow::record_batch::RecordBatch]) -> Vec<u64> {
    let mut out = Vec::new();
    for b in batches {
        let a = b
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        out.extend((0..b.num_rows()).map(|i| a.value(i)));
    }
    out
}

/// Comma-separated allow-list of every `every`-th id.
fn allowed_ids(ids: &[u64], every: u64) -> String {
    ids.iter()
        .copied()
        .filter(|id| id % every == 0)
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn mask_from(ids: &[u64], allowed: &[u64]) -> BooleanArray {
    BooleanArray::from(ids.iter().map(|id| allowed.contains(id)).collect::<Vec<_>>())
}

fn hnsw(ids: &[u64], vs: &[Vec<f32>]) -> AnyIndex {
    AnyIndex::build(
        ids.to_vec(),
        vs.to_vec(),
        Metric::L2,
        &BuildOptions::Hnsw {
            m: 16,
            ef_construction: 100,
            ef_search: 100,
        },
    )
    .unwrap()
}

#[tokio::test]
async fn ann_filter_matches_flat_oracle_on_allowed_ids() {
    let (ids, vs) = corpus(200, 6);
    let ctx = GtvContext::new();
    ctx.register_any_index("idx", hnsw(&ids, &vs));

    let flat = FlatIndex::with_metric(ids.clone(), vs.clone(), Metric::L2).unwrap();
    let allowed: Vec<u64> = ids.iter().copied().filter(|id| id % 10 == 0).collect();
    let q = vs[3].clone();

    let out = ctx
        .sql(&format!(
            "SELECT id FROM ann('idx', '{}', 5, 'l2', '{}')",
            qstr(&q),
            allowed_ids(&ids, 10)
        ))
        .await
        .unwrap();
    let got = ids_of(&out);
    assert!(!got.is_empty());
    for id in &got {
        assert_eq!(id % 10, 0, "id {id} leaked past the filter");
    }
    let oracle = flat.search(&q, 5, Some(&mask_from(&ids, &allowed))).unwrap();
    let hits = got
        .iter()
        .filter(|id| oracle.iter().any(|h| h.id == **id))
        .count();
    assert!(hits >= 4, "adaptive hit {hits}/5 vs oracle {oracle:?}");
}

#[tokio::test]
async fn forced_exact_equals_flat_oracle() {
    let (ids, vs) = corpus(150, 5);
    let dim = 5;
    let flattened: Vec<f32> = vs.iter().flatten().copied().collect();
    let ivf = AnyIndex::Ivf(
        gtv_index::IvfIndex::with_metric(ids.clone(), flattened, dim, 16, 2, Metric::L2).unwrap(),
    );
    let ctx = GtvContext::new();
    ctx.register_any_index("ivf", ivf);

    let flat = FlatIndex::with_metric(ids.clone(), vs.clone(), Metric::L2).unwrap();
    let q = vs[10].clone();
    let out = ctx
        .sql(&format!(
            "SELECT id FROM ann('ivf', '{}', 8, 'l2', '{}', 'exact')",
            qstr(&q),
            allowed_ids(&ids, 7)
        ))
        .await
        .unwrap();
    let got = ids_of(&out);

    let allowed: Vec<u64> = ids.iter().copied().filter(|id| id % 7 == 0).collect();
    let oracle: Vec<u64> = flat
        .search(&q, 8, Some(&mask_from(&ids, &allowed)))
        .unwrap()
        .iter()
        .map(|h| h.id)
        .collect();
    assert_eq!(got, oracle);
}

/// A temporal predicate (business `valid_from` slice) and a metadata predicate
/// (even ids) both compile into one allow-list, so they apply simultaneously.
#[tokio::test]
async fn ann_filter_composes_with_temporal_predicate() {
    let (ids, vs) = corpus(200, 6);
    let ctx = GtvContext::new();
    ctx.register_any_index("idx", hnsw(&ids, &vs));

    // valid_from = id % 10; as-of 3 keeps ids whose bucket <= 3. Even ids only.
    let as_of = 3u64;
    let allowed: Vec<u64> = ids
        .iter()
        .copied()
        .filter(|id| id % 10 <= as_of && id % 2 == 0)
        .collect();
    let filter = allowed
        .iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let q = vs[4].clone();

    let out = ctx
        .sql(&format!(
            "SELECT id FROM ann('idx', '{}', 5, 'l2', '{}', 'exact')",
            qstr(&q),
            filter
        ))
        .await
        .unwrap();
    let got = ids_of(&out);
    assert!(!got.is_empty());
    for id in &got {
        assert!(
            id % 10 <= as_of && id % 2 == 0,
            "id {id} violated the temporal + metadata predicate"
        );
    }

    let oracle: Vec<u64> = FlatIndex::with_metric(ids.clone(), vs.clone(), Metric::L2)
        .unwrap()
        .search(&q, 5, Some(&mask_from(&ids, &allowed)))
        .unwrap()
        .iter()
        .map(|h| h.id)
        .collect();
    assert_eq!(got, oracle);
}

async fn explain(ctx: &GtvContext, sql: &str) -> (String, f64, u64) {
    let out = ctx.sql(sql).await.unwrap();
    let b = &out[0];
    let strategy = b
        .column_by_name("strategy")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0)
        .to_string();
    let recall = b
        .column_by_name("recall_estimate")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    let candidates = b
        .column_by_name("candidate_count")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0);
    (strategy, recall, candidates)
}

#[tokio::test]
async fn ann_explain_selects_strategy_by_selectivity() {
    let (ids, vs) = corpus(200, 6);
    let ctx = GtvContext::new();
    ctx.register_any_index("idx", hnsw(&ids, &vs));
    let q = qstr(&vs[1]);

    // Very selective (1/200 = 0.5%) → pre_filter_exact.
    let (s, r, _) = explain(
        &ctx,
        &format!(
            "SELECT * FROM ann_explain('idx', '{q}', 5, 'l2', '{}')",
            ids[1]
        ),
    )
    .await;
    assert_eq!(s, "pre_filter_exact");
    assert!((0.0..=1.0).contains(&r));

    // Mid selectivity (20/200 = 10%) → oversampled_hnsw for HNSW.
    let (s, r, c) = explain(
        &ctx,
        &format!(
            "SELECT * FROM ann_explain('idx', '{q}', 5, 'l2', '{}')",
            allowed_ids(&ids, 10)
        ),
    )
    .await;
    assert_eq!(s, "oversampled_hnsw");
    assert!(c >= 5);
    assert!(r > 0.5, "recall estimate too low: {r}");

    // Forced exact.
    let (s, r, _) = explain(
        &ctx,
        &format!(
            "SELECT * FROM ann_explain('idx', '{q}', 5, 'l2', '{}', 'exact')",
            allowed_ids(&ids, 10)
        ),
    )
    .await;
    assert_eq!(s, "exact");
    assert!((r - 1.0).abs() < 1e-9);
}

#[tokio::test]
async fn ann_explain_ivf_dispatch() {
    let (ids, vs) = corpus(200, 6);
    let flattened: Vec<f32> = vs.iter().flatten().copied().collect();
    let ivf = AnyIndex::Ivf(
        gtv_index::IvfIndex::with_metric(ids.clone(), flattened, 6, 16, 2, Metric::L2).unwrap(),
    );
    let ctx = GtvContext::new();
    ctx.register_any_index("ivf", ivf);
    let q = qstr(&vs[2]);

    // Mid selectivity → filtered_ivf.
    let (s, _, _) = explain(
        &ctx,
        &format!(
            "SELECT * FROM ann_explain('ivf', '{q}', 5, 'l2', '{}')",
            allowed_ids(&ids, 10)
        ),
    )
    .await;
    assert_eq!(s, "filtered_ivf");

    // High selectivity (50%) → post_filter_rerank.
    let (s, _, _) = explain(
        &ctx,
        &format!(
            "SELECT * FROM ann_explain('ivf', '{q}', 5, 'l2', '{}')",
            allowed_ids(&ids, 2)
        ),
    )
    .await;
    assert_eq!(s, "post_filter_rerank");
}

#[tokio::test]
async fn ann_rejects_metric_mismatch_with_filter() {
    let (ids, vs) = corpus(40, 4);
    let flat = AnyIndex::build(ids.clone(), vs.clone(), Metric::L2, &BuildOptions::Flat).unwrap();
    let ctx = GtvContext::new();
    ctx.register_any_index("flat", flat);
    let err = ctx
        .sql("SELECT * FROM ann('flat', '0,0,0,0', 3, 'cosine', '0,1,2')")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("metric mismatch"), "{err}");
}
