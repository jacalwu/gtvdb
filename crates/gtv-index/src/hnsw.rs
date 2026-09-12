//! Approximate nearest-neighbor search via a Hierarchical Navigable Small
//! World (HNSW) graph, built from scratch with a deterministic PRNG so builds
//! are reproducible (and testable without external randomness).
//!
//! Supports [`Metric::L2`], [`Metric::Cosine`] and [`Metric::Ip`]. Cosine rows
//! (and the query) are unit-normalized, after which the score is `1 - dot`.
//!
//! > **Memory layout note**: this version still stores each node as a
//! > `Vec<f32>` + `Vec<Vec<usize>>` (pointer-chasing). The contiguous-layout
//! > refactor is tracked as B1-4 in `prod_p1.md`; the metric contract added here
//! > is a prerequisite for it.

use std::borrow::Cow;
use std::collections::HashSet;

use arrow::array::BooleanArray;
use gtv_core::{GtvError, Metric, Result, VectorHit, VectorIndex};

/// Deterministic 64-bit splitmix PRNG (seeded) — keeps HNSW builds reproducible.
#[derive(Debug, Clone)]
struct SplitMix64(u64);

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        SplitMix64(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform float in [0, 1).
    fn next_f32(&mut self) -> f32 {
        (self.next_u64() >> 40) as f32 / (1u64 << 24) as f32
    }
}

struct Node {
    id: u64,
    vector: Vec<f32>,
    /// `layers[l]` = neighbor node positions at layer `l`.
    layers: Vec<Vec<usize>>,
}

/// Approximate K-NN over an HNSW graph.
///
/// The bitmask is indexed by node *position* (0..n), identical to
/// [`FlatIndex`](crate::FlatIndex).
pub struct HnswIndex {
    nodes: Vec<Node>,
    entry: usize,
    dim: usize,
    metric: Metric,
    /// Max neighbors per node at layer >= 1.
    m: usize,
    /// Max neighbors per node at layer 0 (usually 2 * `m`).
    m0: usize,
    ef_construction: usize,
    ef_search: usize,
    max_level: usize,
    rng: SplitMix64,
}

impl HnswIndex {
    /// New L2 index.
    pub fn new(m: usize, ef_construction: usize, ef_search: usize) -> Self {
        Self::with_metric(m, ef_construction, ef_search, Metric::L2)
    }

    /// New index with an explicit metric.
    pub fn with_metric(
        m: usize,
        ef_construction: usize,
        ef_search: usize,
        metric: Metric,
    ) -> Self {
        HnswIndex {
            nodes: Vec::new(),
            entry: 0,
            dim: 0,
            metric,
            m: m.max(1),
            m0: (m * 2).max(1),
            ef_construction: ef_construction.max(1),
            ef_search: ef_search.max(1),
            max_level: 0,
            rng: SplitMix64::new(0x243F_6A88_85A3_08D3),
        }
    }

    /// Convenience: build an L2 index from scratch, inserting each vector in order.
    pub fn build(
        ids: Vec<u64>,
        vectors: Vec<Vec<f32>>,
        m: usize,
        ef_construction: usize,
        ef_search: usize,
    ) -> Result<Self> {
        Self::build_with_metric(ids, vectors, m, ef_construction, ef_search, Metric::L2)
    }

    /// Build an index with an explicit metric.
    pub fn build_with_metric(
        ids: Vec<u64>,
        vectors: Vec<Vec<f32>>,
        m: usize,
        ef_construction: usize,
        ef_search: usize,
        metric: Metric,
    ) -> Result<Self> {
        if ids.len() != vectors.len() {
            return Err(GtvError::InvalidArgument(
                "ids and vectors length mismatch".into(),
            ));
        }
        let mut index = Self::with_metric(m, ef_construction, ef_search, metric);
        for (id, v) in ids.into_iter().zip(vectors) {
            index.insert(id, v)?;
        }
        Ok(index)
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    pub fn metric(&self) -> Metric {
        self.metric
    }

    /// Metric-aware distance; rows and query are normalized for Cosine, so the
    /// full `Metric::distance` formula stays cheap.
    #[inline]
    fn dist(&self, a: &[f32], b: &[f32]) -> f32 {
        self.metric.distance(a, b)
    }

    /// Normalize the query when the metric requires it.
    fn prepare_query<'a>(&self, query: &'a [f32]) -> Cow<'a, [f32]> {
        if self.metric.requires_normalization() {
            let mut q = query.to_vec();
            self.metric.normalize_in_place(&mut q);
            Cow::Owned(q)
        } else {
            Cow::Borrowed(query)
        }
    }

    /// Sample a level from the geometric-ish distribution `floor(-ln(u) * mL)`.
    fn random_level(&mut self) -> usize {
        let ml = 1.0 / (self.m as f32).ln();
        let u = self.rng.next_f32().max(1e-6);
        (-u.ln() * ml) as usize
    }

    /// Greedy (ef = 1) descent to the nearest node at `layer` from `start`.
    fn greedy_descend(&self, query: &[f32], mut cur: usize, layer: usize) -> (f32, usize) {
        let mut cur_d = self.dist(query, &self.nodes[cur].vector);
        loop {
            let mut best = (cur_d, cur);
            for &nb in &self.nodes[cur].layers[layer] {
                let d = self.dist(query, &self.nodes[nb].vector);
                if d < best.0 {
                    best = (d, nb);
                }
            }
            if best.1 == cur {
                return best;
            }
            cur = best.1;
            cur_d = best.0;
        }
    }

    /// Beam search at `layer`, returning up to `ef` nearest nodes (filtered).
    fn search_layer(
        &self,
        query: &[f32],
        eps: &[(f32, usize)],
        ef: usize,
        layer: usize,
        mask: Option<&BooleanArray>,
    ) -> Vec<(f32, usize)> {
        let mut visited: HashSet<usize> = HashSet::new();
        let mut candidates: Vec<(f32, usize)> = Vec::new();
        let mut results: Vec<(f32, usize)> = Vec::new();

        for &(d, i) in eps {
            visited.insert(i);
            candidates.push((d, i));
            if mask.map_or(true, |m| m.value(i)) {
                results.push((d, i));
            }
        }
        sort_asc(&mut candidates);
        sort_asc(&mut results);

        while let Some((cd, ci)) = candidates.first().copied() {
            candidates.remove(0);
            let farthest = results.last().map(|r| r.0).unwrap_or(f32::INFINITY);
            if results.len() >= ef && cd > farthest {
                break;
            }
            let neighbors = self.nodes[ci].layers[layer].clone();
            for nb in neighbors {
                if !visited.insert(nb) {
                    continue;
                }
                let d = self.dist(query, &self.nodes[nb].vector);
                candidates.push((d, nb));
                if mask.map_or(true, |m| m.value(nb)) {
                    results.push((d, nb));
                }
            }
            sort_asc(&mut candidates);
            sort_asc(&mut results);
            if results.len() > ef {
                results.truncate(ef);
            }
        }
        results
    }

    /// Insert a single node, wiring it into the graph bidirectionally.
    pub fn insert(&mut self, id: u64, mut vector: Vec<f32>) -> Result<()> {
        if self.dim == 0 {
            self.dim = vector.len();
        } else if vector.len() != self.dim {
            return Err(GtvError::DimensionMismatch {
                index: self.dim,
                query: vector.len(),
            });
        }
        self.metric.normalize_in_place(&mut vector);

        let level = self.random_level();
        let new_idx = self.nodes.len();
        self.nodes.push(Node {
            id,
            vector,
            layers: vec![Vec::new(); level + 1],
        });

        if new_idx == 0 {
            self.entry = 0;
            self.max_level = level;
            return Ok(());
        }

        // Greedy descent from the top entry down to `level + 1`.
        let mut cur = self.entry;
        let mut cur_d = self.dist(&self.nodes[new_idx].vector, &self.nodes[cur].vector);
        for l in ((level + 1)..=self.max_level).rev() {
            let (d, n) = self.greedy_descend(&self.nodes[new_idx].vector, cur, l);
            cur = n;
            cur_d = d;
        }

        // Wire into each layer from min(level, max_level) down to 0.
        let top = level.min(self.max_level);
        for l in (0..=top).rev() {
            let eps = [(cur_d, cur)];
            let mut candidates = self.search_layer(
                &self.nodes[new_idx].vector,
                &eps,
                self.ef_construction,
                l,
                None,
            );
            // Drop the query node itself if it leaked in (it cannot, but be safe).
            candidates.retain(|&(_, i)| i != new_idx);
            let max_deg = if l == 0 { self.m0 } else { self.m };
            let selected = take_nearest(&candidates, max_deg);

            for &nb in &selected {
                self.nodes[new_idx].layers[l].push(nb);
                self.nodes[nb].layers[l].push(new_idx);
                self.prune(nb, l, max_deg);
            }
            // After wiring the first (bottom) layer, subsequent layers use the
            // result of the layer below as the entry point.
            if let Some(&(d, n)) = candidates.first() {
                cur = n;
                cur_d = d;
            }
        }

        if level > self.max_level {
            self.max_level = level;
            self.entry = new_idx;
        }
        Ok(())
    }

    /// Keep only the `max_deg` nearest neighbors of `node` at `layer`.
    fn prune(&mut self, node: usize, layer: usize, max_deg: usize) {
        let neighbors = self.nodes[node].layers[layer].clone();
        if neighbors.len() <= max_deg {
            return;
        }
        let target = self.nodes[node].vector.clone();
        let metric = self.metric;
        let mut scored: Vec<(f32, usize)> = neighbors
            .into_iter()
            .map(|nb| (metric.distance(&target, &self.nodes[nb].vector), nb))
            .collect();
        sort_asc(&mut scored);
        scored.truncate(max_deg);
        self.nodes[node].layers[layer] = scored.into_iter().map(|(_, i)| i).collect();
    }

    /// Search returning `(distance, external id)` pairs, sorted ascending.
    fn search_scored(&self, query: &[f32], k: usize, mask: Option<&BooleanArray>) -> Vec<(f32, u64)> {
        if self.nodes.is_empty() {
            return Vec::new();
        }
        let query = self.prepare_query(query);
        let ef = self.ef_search.max(k);
        let mut cur = self.entry;
        let mut cur_d = self.dist(&query, &self.nodes[cur].vector);
        for l in (1..=self.max_level).rev() {
            let (d, n) = self.greedy_descend(&query, cur, l);
            cur = n;
            cur_d = d;
        }
        let mut results = self.search_layer(&query, &[(cur_d, cur)], ef, 0, mask);
        sort_asc(&mut results);
        results.truncate(k);
        results
            .into_iter()
            .map(|(d, i)| (d, self.nodes[i].id))
            .collect()
    }
}

fn sort_asc(v: &mut [(f32, usize)]) {
    v.sort_by(|a, b| {
        a.0.partial_cmp(&b.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.1.cmp(&b.1))
    });
}

fn take_nearest(candidates: &[(f32, usize)], n: usize) -> Vec<usize> {
    candidates.iter().take(n).map(|&(_, i)| i).collect()
}

impl VectorIndex for HnswIndex {
    fn metric(&self) -> Metric {
        self.metric
    }

    fn dim(&self) -> usize {
        self.dim
    }

    fn search(
        &self,
        query: &[f32],
        k: usize,
        filter_mask: Option<&BooleanArray>,
    ) -> Result<Vec<VectorHit>> {
        if query.len() != self.dim {
            return Err(GtvError::DimensionMismatch {
                index: self.dim,
                query: query.len(),
            });
        }
        if let Some(mask) = filter_mask {
            if mask.len() != self.nodes.len() {
                return Err(GtvError::InvalidArgument(
                    "filter mask length mismatch".into(),
                ));
            }
        }
        if k == 0 {
            return Ok(Vec::new());
        }
        Ok(self
            .search_scored(query, k, filter_mask)
            .into_iter()
            .map(|(distance, id)| VectorHit { id, distance })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_search_recovers_nearest() {
        let mut index = HnswIndex::new(4, 20, 20);
        index.insert(0, vec![0.0, 0.0]).unwrap();
        index.insert(1, vec![1.0, 1.0]).unwrap();
        index.insert(2, vec![5.0, 5.0]).unwrap();

        let got = index.search_knn(&[0.1, 0.1], 3, None).unwrap();
        assert_eq!(got.values().as_ref(), &[0, 1, 2]);
    }

    #[test]
    fn knn_respects_bitmask() {
        let mut index = HnswIndex::new(4, 20, 20);
        index.insert(0, vec![0.0, 0.0]).unwrap();
        index.insert(1, vec![1.0, 1.0]).unwrap();
        index.insert(2, vec![5.0, 5.0]).unwrap();

        let mask = BooleanArray::from(vec![false, true, true]);
        let got = index.search_knn(&[5.0, 5.0], 3, Some(&mask)).unwrap();
        assert_eq!(got.values().as_ref(), &[2, 1]);
    }

    #[test]
    fn build_matches_flat_recall_on_random_data() {
        use crate::FlatIndex;
        let mut rng = SplitMix64::new(42);
        let ids: Vec<u64> = (0..50).collect();
        let mut vectors = Vec::new();
        for _ in 0..50 {
            vectors.push(vec![
                rng.next_f32(),
                rng.next_f32(),
                rng.next_f32(),
                rng.next_f32(),
            ]);
        }
        let flat = FlatIndex::new(ids.clone(), vectors.clone()).unwrap();
        let hnsw = HnswIndex::build(ids, vectors, 8, 40, 40).unwrap();

        let query = vec![0.5, 0.5, 0.5, 0.5];
        let exact = flat.search_knn(&query, 5, None).unwrap();
        let approx = hnsw.search_knn(&query, 5, None).unwrap();
        let exact: Vec<u64> = exact.values().to_vec();
        let approx: Vec<u64> = approx.values().to_vec();
        // HNSW must return the exact nearest neighbor as its top-1 on this size.
        assert_eq!(exact[0], approx[0]);
    }

    #[test]
    fn cosine_reports_and_orders() {
        let ids = vec![0u64, 1, 2];
        let vectors = vec![
            vec![1.0f32, 0.0],
            vec![0.9, 0.1],
            vec![0.0, 1.0],
        ];
        let index =
            HnswIndex::build_with_metric(ids, vectors, 4, 20, 20, Metric::Cosine).unwrap();
        assert_eq!(index.metric(), Metric::Cosine);
        let hits = index.search(&[1.0, 0.0], 3, None).unwrap();
        let got: Vec<u64> = hits.iter().map(|h| h.id).collect();
        assert_eq!(got[0], 0);
    }

    #[test]
    fn dimension_mismatch_is_typed() {
        let mut index = HnswIndex::new(4, 20, 20);
        index.insert(0, vec![0.0, 0.0]).unwrap();
        assert!(matches!(
            index.search(&[1.0], 1, None),
            Err(GtvError::DimensionMismatch { .. })
        ));
    }
}
