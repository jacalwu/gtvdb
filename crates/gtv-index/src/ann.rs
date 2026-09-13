//! B3-3: filter-aware ANN routing + exact rerank.
//!
//! A filtered vector query has to choose *how* to combine the metadata filter
//! with the nearest-neighbour search. The right answer depends on the filter
//! selectivity:
//!
//! * very selective (< 1%) — an exact scan over the allowed ids beats any ANN;
//! * mid selectivity (1–20%) — probe more IVF cells (or raise HNSW `ef`) and
//!   exactly rerank the survivors;
//! * high selectivity (>= 20%) — a normal ANN with an oversampled candidate set
//!   and a final exact rerank;
//! * regulatory / high-risk — force the exact oracle.
//!
//! [`plan_ann`] picks a [`AnnStrategy`] from the selectivity and the index type;
//! [`execute_ann`] runs it and returns the reranked hits plus [`AnnTelemetry`].
//! [`estimate_recall`] samples queries against the exact oracle so recall can be
//! monitored over time.

use std::cmp::Ordering;
use std::time::Instant;

use arrow::array::BooleanArray;
use gtv_core::{Metric, Result, VectorHit, VectorIndex};

use crate::index::{AnyIndex, IndexType};

/// How a filtered ANN query is executed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnnStrategy {
    /// Exact oracle scan (brute force / full IVF probe), mask applied.
    Exact,
    /// Build the allowed-id mask first, then exact-scan only the survivors.
    PreFilterExact,
    /// IVF only: probe cells and widen the probe set until enough allowed hits.
    FilteredIvf,
    /// HNSW: raise `ef` proportional to `1 / selectivity`, then rerank.
    OversampledHnsw,
    /// General ANN with an oversampled candidate set, then exact rerank.
    PostFilterRerank,
}

impl AnnStrategy {
    pub fn as_str(&self) -> &'static str {
        match self {
            AnnStrategy::Exact => "exact",
            AnnStrategy::PreFilterExact => "pre_filter_exact",
            AnnStrategy::FilteredIvf => "filtered_ivf",
            AnnStrategy::OversampledHnsw => "oversampled_hnsw",
            AnnStrategy::PostFilterRerank => "post_filter_rerank",
        }
    }
}

/// Tunable thresholds for the planner.
#[derive(Debug, Clone, PartialEq)]
pub struct AnnConfig {
    /// Selectivity below this uses the exact pre-filter scan.
    pub prefilter_threshold: f64,
    /// Selectivity below this uses FilteredIvf / OversampledHnsw.
    pub filtered_threshold: f64,
    /// Candidate oversampling safety factor (`k / selectivity * safety`).
    pub safety: f64,
    /// Hard cap on HNSW `ef`.
    pub ef_max: usize,
    /// Hard cap on the candidate oversample factor.
    pub max_oversample: usize,
    /// Force the exact oracle (regulatory / high-risk).
    pub force_exact: bool,
}

impl Default for AnnConfig {
    fn default() -> Self {
        Self {
            prefilter_threshold: 0.01,
            filtered_threshold: 0.20,
            safety: 1.5,
            ef_max: 512,
            max_oversample: 64,
            force_exact: false,
        }
    }
}

/// The chosen execution strategy plus its parameters and rationale.
#[derive(Debug, Clone, PartialEq)]
pub struct AnnPlan {
    pub strategy: AnnStrategy,
    /// Candidate oversample factor (`candidates = k * oversample`).
    pub oversample: usize,
    /// Whether candidates are re-scored against the original `f32` vectors.
    pub exact_rerank: bool,
    pub reason: String,
}

/// Choose a strategy from the filter selectivity and index type.
pub fn plan_ann(index_type: IndexType, selectivity: f64, k: usize, cfg: &AnnConfig) -> AnnPlan {
    let sel = selectivity.clamp(0.0, 1.0);
    if cfg.force_exact {
        return AnnPlan {
            strategy: AnnStrategy::Exact,
            oversample: 1,
            exact_rerank: false,
            reason: "forced exact (regulatory / high-risk)".into(),
        };
    }
    if k == 0 {
        return AnnPlan {
            strategy: AnnStrategy::Exact,
            oversample: 1,
            exact_rerank: false,
            reason: "k = 0".into(),
        };
    }
    let oversample = ((cfg.safety / sel.max(1e-6)).ceil() as usize).clamp(1, cfg.max_oversample);

    // No filter: a plain ANN (still reranked for a stable ordering).
    if sel >= 1.0 {
        return AnnPlan {
            strategy: AnnStrategy::PostFilterRerank,
            oversample: 1,
            exact_rerank: true,
            reason: "no filter".into(),
        };
    }

    if sel < cfg.prefilter_threshold {
        return AnnPlan {
            strategy: AnnStrategy::PreFilterExact,
            oversample: 1,
            exact_rerank: false,
            reason: format!("selectivity {sel:.4} < prefilter threshold {:.4}", cfg.prefilter_threshold),
        };
    }

    if sel < cfg.filtered_threshold {
        let strategy = match index_type {
            IndexType::Ivf => AnnStrategy::FilteredIvf,
            IndexType::Hnsw => AnnStrategy::OversampledHnsw,
            IndexType::Flat => AnnStrategy::PreFilterExact,
        };
        return AnnPlan {
            strategy,
            oversample,
            exact_rerank: true,
            reason: format!("mid selectivity {sel:.4} (oversample {oversample})"),
        };
    }

    let strategy = match index_type {
        IndexType::Flat => AnnStrategy::PreFilterExact,
        IndexType::Ivf => AnnStrategy::PostFilterRerank,
        IndexType::Hnsw => AnnStrategy::OversampledHnsw,
    };
    AnnPlan {
        strategy,
        oversample,
        exact_rerank: true,
        reason: format!("high selectivity {sel:.4} (oversample {oversample})"),
    }
}

/// A hit carrying both its ANN candidate score and its exact `f32` score.
#[derive(Debug, Clone, PartialEq)]
pub struct RerankedHit {
    pub id: u64,
    pub approx: f32,
    pub exact: f32,
}

/// The reranked hits and the two parallel score vectors.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RerankResult {
    pub hits: Vec<RerankedHit>,
    pub approx_scores: Vec<f32>,
    pub exact_scores: Vec<f32>,
}

/// Where the wall-clock time went for one query.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LatencyBreakdown {
    pub filter_us: u64,
    pub ann_us: u64,
    pub rerank_us: u64,
}

impl LatencyBreakdown {
    pub fn total_us(&self) -> u64 {
        self.filter_us + self.ann_us + self.rerank_us
    }
}

/// Per-query observability for the chosen strategy.
#[derive(Debug, Clone, PartialEq)]
pub struct AnnTelemetry {
    pub strategy: AnnStrategy,
    /// Candidates returned by the ANN stage.
    pub candidate_count: u64,
    /// Hits that survived filtering + rerank (<= k).
    pub filtered_count: u64,
    pub oversample: usize,
    /// Sampled recall estimate, when measured.
    pub recall_estimate: Option<f64>,
    pub latency: LatencyBreakdown,
    pub reason: String,
}

/// Count the allowed positions of a mask (or all rows when there is no mask).
pub fn allowed_count(len: usize, mask: Option<&BooleanArray>) -> usize {
    match mask {
        Some(m) => (0..len).filter(|&i| m.value(i)).count(),
        None => len,
    }
}

fn rerank(index: &AnyIndex, query: &[f32], k: usize, approx: &[VectorHit]) -> RerankResult {
    let metric = index.metric();
    let q: Vec<f32> = if metric.requires_normalization() {
        let mut q = query.to_vec();
        metric.normalize_in_place(&mut q);
        q
    } else {
        query.to_vec()
    };
    let mut hits: Vec<RerankedHit> = approx
        .iter()
        .map(|h| {
            let exact = index
                .vector_for_id(h.id)
                .map(|v| metric.distance(&q, v))
                .unwrap_or(h.distance);
            RerankedHit {
                id: h.id,
                approx: h.distance,
                exact,
            }
        })
        .collect();
    hits.sort_by(|a, b| {
        a.exact
            .partial_cmp(&b.exact)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.id.cmp(&b.id))
    });
    hits.truncate(k);
    let approx_scores = hits.iter().map(|h| h.approx).collect();
    let exact_scores = hits.iter().map(|h| h.exact).collect();
    RerankResult {
        hits,
        approx_scores,
        exact_scores,
    }
}

/// Execute `plan` over `index`, returning the reranked result and telemetry.
pub fn execute_ann(
    index: &AnyIndex,
    query: &[f32],
    k: usize,
    mask: Option<&BooleanArray>,
    plan: &AnnPlan,
) -> Result<(RerankResult, AnnTelemetry)> {
    let t0 = Instant::now();
    let _allowed = allowed_count(index.len(), mask);
    let filter_us = t0.elapsed().as_micros() as u64;

    let t1 = Instant::now();
    let fetch = (k * plan.oversample).max(k);
    let candidates: Vec<VectorHit> = match plan.strategy {
        AnnStrategy::Exact | AnnStrategy::PreFilterExact => index.exact_search(query, k, mask)?,
        AnnStrategy::FilteredIvf => match index {
            AnyIndex::Ivf(ivf) => ivf.filtered_search(query, fetch, mask)?,
            other => other.search(query, fetch, mask)?,
        },
        AnnStrategy::OversampledHnsw => match index {
            AnyIndex::Hnsw(hnsw) => hnsw.search_with_ef(query, fetch, fetch, mask)?,
            other => other.search(query, fetch, mask)?,
        },
        AnnStrategy::PostFilterRerank => index.search(query, fetch, mask)?,
    };
    let candidate_count = candidates.len() as u64;
    let ann_us = t1.elapsed().as_micros() as u64;

    let t2 = Instant::now();
    let result = rerank(index, query, k, &candidates);
    let rerank_us = t2.elapsed().as_micros() as u64;
    let filtered_count = result.hits.len() as u64;

    let telemetry = AnnTelemetry {
        strategy: plan.strategy,
        candidate_count,
        filtered_count,
        oversample: plan.oversample,
        recall_estimate: None,
        latency: LatencyBreakdown {
            filter_us,
            ann_us,
            rerank_us,
        },
        reason: plan.reason.clone(),
    };
    Ok((result, telemetry))
}

/// Sample `queries` against the exact oracle and return mean Recall@K for
/// `plan` (the adaptive strategy).
pub fn estimate_recall(
    index: &AnyIndex,
    queries: &[Vec<f32>],
    k: usize,
    mask: Option<&BooleanArray>,
    plan: &AnnPlan,
) -> Result<f64> {
    if queries.is_empty() || k == 0 {
        return Ok(1.0);
    }
    let mut sum = 0.0f64;
    for q in queries {
        let (approx, _) = execute_ann(index, q, k, mask, plan)?;
        let exact = index.exact_search(q, k, mask)?;
        let denom = exact.len().min(k).max(1) as f64;
        let hits = approx
            .hits
            .iter()
            .filter(|h| exact.iter().any(|e| e.id == h.id))
            .count();
        sum += hits as f64 / denom;
    }
    Ok(sum / queries.len() as f64)
}

/// Default candidate grid for recall/latency sweeps of a fixed index.
pub fn recall_curve(
    index: &AnyIndex,
    queries: &[Vec<f32>],
    k: usize,
    mask: Option<&BooleanArray>,
    cfg: &AnnConfig,
) -> Result<Vec<(AnnStrategy, f64, u64)>> {
    let selectivity = allowed_count(index.len(), mask) as f64 / index.len().max(1) as f64;
    let plan = plan_ann(index.index_type(), selectivity, k, cfg);
    let recall = estimate_recall(index, queries, k, mask, &plan)?;
    // Baseline: force exact for the reference latency.
    let mut exact_cfg = cfg.clone();
    exact_cfg.force_exact = true;
    let exact_plan = plan_ann(index.index_type(), selectivity, k, &exact_cfg);
    // Measure each plan over the same queries (latency averaged).
    let mut out = Vec::new();
    for candidate in [plan, exact_plan] {
        let mut total_us = 0u64;
        for q in queries {
            let (_, tel) = execute_ann(index, q, k, mask, &candidate)?;
            total_us += tel.latency.total_us();
        }
        let us = total_us / queries.len().max(1) as u64;
        let r = if candidate.strategy == AnnStrategy::Exact {
            1.0
        } else {
            recall
        };
        out.push((candidate.strategy, r, us));
    }
    Ok(out)
}

/// The metric required for a query, exposed so callers can report distances the
/// same way `ann` / `knn` do.
pub fn metric_of(index: &AnyIndex) -> Metric {
    index.metric()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flat::FlatIndex;
    use crate::hnsw::HnswIndex;
    use crate::ivf::IvfIndex;
    use gtv_core::Metric;

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

    fn corpus(seed: u64, n: usize, dim: usize) -> (Vec<u64>, Vec<Vec<f32>>) {
        let mut rng = Lcg(seed);
        let ids = (0..n as u64).collect();
        let vs = (0..n)
            .map(|_| (0..dim).map(|_| rng.f32()).collect())
            .collect();
        (ids, vs)
    }

    fn mask_of(ids: &[u64], allowed: &[u64]) -> BooleanArray {
        let set: std::collections::HashSet<u64> = allowed.iter().copied().collect();
        BooleanArray::from(ids.iter().map(|id| set.contains(id)).collect::<Vec<_>>())
    }

    fn flat(vs: &[Vec<f32>]) -> Vec<f32> {
        vs.iter().flatten().copied().collect()
    }

    #[test]
    fn planner_dispatch_by_selectivity() {
        let cfg = AnnConfig::default();
        assert_eq!(
            plan_ann(IndexType::Hnsw, 0.001, 10, &cfg).strategy,
            AnnStrategy::PreFilterExact
        );
        assert_eq!(
            plan_ann(IndexType::Hnsw, 0.05, 10, &cfg).strategy,
            AnnStrategy::OversampledHnsw
        );
        assert_eq!(
            plan_ann(IndexType::Ivf, 0.05, 10, &cfg).strategy,
            AnnStrategy::FilteredIvf
        );
        assert_eq!(
            plan_ann(IndexType::Ivf, 0.5, 10, &cfg).strategy,
            AnnStrategy::PostFilterRerank
        );
        assert_eq!(
            plan_ann(IndexType::Flat, 0.5, 10, &cfg).strategy,
            AnnStrategy::PreFilterExact
        );
        assert_eq!(
            plan_ann(IndexType::Flat, 1.0, 10, &cfg).strategy,
            AnnStrategy::PostFilterRerank
        );
        let mut forced = cfg.clone();
        forced.force_exact = true;
        assert_eq!(
            plan_ann(IndexType::Hnsw, 0.5, 10, &forced).strategy,
            AnnStrategy::Exact
        );
    }

    #[test]
    fn exact_strategy_equals_flat_oracle() {
        let (ids, vs) = corpus(1, 200, 6);
        let allowed: Vec<u64> = ids.iter().copied().filter(|id| id % 7 == 0).collect();
        let mask = mask_of(&ids, &allowed);
        let flat = FlatIndex::with_metric(ids.clone(), vs.clone(), Metric::L2).unwrap();
        let hnsw = HnswIndex::build_with_metric(ids.clone(), vs.clone(), 16, 100, 100, Metric::L2)
            .unwrap();
        let mut cfg = AnnConfig::default();
        cfg.force_exact = true;
        let q = vs[3].clone();
        let plan = plan_ann(IndexType::Hnsw, 0.1, 10, &cfg);
        let (res, tel) = execute_ann(&AnyIndex::Hnsw(hnsw), &q, 10, Some(&mask), &plan).unwrap();
        let oracle = flat.search(&q, 10, Some(&mask)).unwrap();
        assert_eq!(tel.strategy, AnnStrategy::Exact);
        assert_eq!(
            res.hits.iter().map(|h| h.id).collect::<Vec<_>>(),
            oracle.iter().map(|h| h.id).collect::<Vec<_>>()
        );
    }

    #[test]
    fn mid_selectivity_ivf_recall_is_high() {
        let (ids, vs) = corpus(2, 400, 8);
        // 10% selectivity.
        let allowed: Vec<u64> = ids.iter().copied().filter(|id| id % 10 == 0).collect();
        let mask = mask_of(&ids, &allowed);
        let ivf = IvfIndex::with_metric(ids.clone(), flat(&vs), 8, 16, 2, Metric::L2).unwrap();
        let index = AnyIndex::Ivf(ivf);
        let cfg = AnnConfig::default();
        let plan = plan_ann(IndexType::Ivf, 0.10, 10, &cfg);
        assert_eq!(plan.strategy, AnnStrategy::FilteredIvf);
        let queries: Vec<Vec<f32>> = (0..20).map(|i| vs[i * 7].clone()).collect();
        let recall = estimate_recall(&index, &queries, 10, Some(&mask), &plan).unwrap();
        assert!(recall > 0.7, "filtered IVF recall too low: {recall}");
    }

    #[test]
    fn telemetry_fields_are_populated() {
        let (ids, vs) = corpus(3, 120, 4);
        let allowed: Vec<u64> = ids.iter().copied().filter(|id| id % 2 == 0).collect();
        let mask = mask_of(&ids, &allowed);
        let hnsw =
            HnswIndex::build_with_metric(ids.clone(), vs.clone(), 16, 100, 100, Metric::L2).unwrap();
        let index = AnyIndex::Hnsw(hnsw);
        let cfg = AnnConfig::default();
        let plan = plan_ann(IndexType::Hnsw, 0.5, 5, &cfg);
        let (res, tel) = execute_ann(&index, &vs[1], 5, Some(&mask), &plan).unwrap();
        assert_eq!(tel.strategy, AnnStrategy::OversampledHnsw);
        assert!(tel.candidate_count > 0);
        assert!(tel.filtered_count <= 5);
        assert!(tel.oversample >= 1);
        assert_eq!(
            tel.latency.total_us(),
            tel.latency.filter_us + tel.latency.ann_us + tel.latency.rerank_us
        );
        assert_eq!(res.hits.len(), res.exact_scores.len());
        assert_eq!(res.hits.len(), res.approx_scores.len());
    }

    #[test]
    fn high_selectivity_recall_matches_low_filter_loss() {
        let (ids, vs) = corpus(4, 300, 6);
        // 50% selectivity.
        let allowed: Vec<u64> = ids.iter().copied().filter(|id| id % 2 == 0).collect();
        let mask = mask_of(&ids, &allowed);
        let hnsw =
            HnswIndex::build_with_metric(ids.clone(), vs.clone(), 16, 100, 100, Metric::L2).unwrap();
        let index = AnyIndex::Hnsw(hnsw);
        let cfg = AnnConfig::default();
        let plan = plan_ann(IndexType::Hnsw, 0.5, 10, &cfg);
        let queries: Vec<Vec<f32>> = (0..30).map(|i| vs[i * 3].clone()).collect();
        let recall = estimate_recall(&index, &queries, 10, Some(&mask), &plan).unwrap();
        assert!(recall > 0.8, "high-selectivity recall too low: {recall}");
    }

    #[test]
    fn recall_curve_reports_adaptive_and_exact() {
        let (ids, vs) = corpus(5, 200, 6);
        let allowed: Vec<u64> = ids.iter().copied().filter(|id| id % 5 == 0).collect();
        let mask = mask_of(&ids, &allowed);
        let ivf = IvfIndex::with_metric(ids.clone(), flat(&vs), 6, 16, 4, Metric::L2).unwrap();
        let index = AnyIndex::Ivf(ivf);
        let queries: Vec<Vec<f32>> = (0..15).map(|i| vs[i * 4].clone()).collect();
        let curve = recall_curve(&index, &queries, 5, Some(&mask), &AnnConfig::default()).unwrap();
        assert_eq!(curve.len(), 2);
        assert!(curve.iter().any(|(s, _, _)| *s == AnnStrategy::Exact));
        assert!(curve.iter().all(|(_, r, _)| *r >= 0.0 && *r <= 1.0));
    }
}
