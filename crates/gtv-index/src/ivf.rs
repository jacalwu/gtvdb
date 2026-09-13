//! Inverted-file (IVF) K-NN index — a sublinear index that keeps full `f32`
//! precision for both the corpus and the distances.
//!
//! The corpus is partitioned into `nlist` Voronoi cells by a coarse quantizer
//! (centroids sampled deterministically from the corpus — no product
//! quantization, no reduced-precision encoding). Vectors are **reordered so each
//! cell is a contiguous run**, which turns a query into: (1) rank the centroids,
//! (2) read the `nprobe` nearest cells as fully-sequential streams, and (3) an
//! exact `f32` scan over those candidates. The sublinearity comes from pruning
//! cells, never from lowering precision: every returned neighbor carries zero
//! quantization error.
//!
//! At 1M × 512-dim the exact flat scan must stream 2 GB; the IVF probe at
//! `nlist=1024, nprobe=32` reads only ~64 MB of contiguous data.
//!
//! Supports [`Metric::L2`], [`Metric::Cosine`] and [`Metric::Ip`]; Cosine rows
//! (and the query) are unit-normalized before assignment and search.

use std::borrow::Cow;
use std::cmp::Ordering;

use arrow::array::BooleanArray;
use gtv_core::{GtvError, Metric, Result, VectorHit, VectorIndex};

use crate::bytes::{metric_code, metric_from_code, Reader, Writer};
use crate::flat::{metric_distance, par_topk_over, FlatIndex};

const IVF_MAGIC: &[u8; 8] = b"GIVFv1\0\0";
const IVF_MAGIC_V2: &[u8; 8] = b"GIVFv2\0\0";

/// Deterministic 64-bit splitmix PRNG (same construction as HNSW) so k-means
/// training is reproducible for a given seed.
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
    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// Configuration for the IVF coarse quantizer.
#[derive(Debug, Clone, PartialEq)]
pub struct KMeansConfig {
    pub nlist: usize,
    /// Lloyd iterations per restart.
    pub max_iters: usize,
    /// k-means++ restarts; the lowest-inertia result wins.
    pub restarts: usize,
    /// Train on a deterministic sample of at most this many rows.
    pub sample: Option<usize>,
    pub seed: u64,
    /// Split cells larger than `mean + split_threshold_k * std` after training.
    pub split_oversized: bool,
    pub split_threshold_k: f64,
}

impl KMeansConfig {
    /// Conservative defaults for a given `nlist` (no cell splitting, full corpus).
    pub fn for_nlist(nlist: usize) -> Self {
        Self {
            nlist,
            max_iters: 25,
            restarts: 3,
            sample: None,
            seed: 0x9E37_79B9_7F4A_7C15,
            split_oversized: false,
            split_threshold_k: 3.0,
        }
    }
}

impl Default for KMeansConfig {
    fn default() -> Self {
        Self::for_nlist(16)
    }
}

/// Training provenance persisted with a `GIVFv2` payload.
#[derive(Debug, Clone, PartialEq)]
pub struct IvfTrainingMeta {
    /// `true` when k-means was used (vs. uniform sampling).
    pub kmeans: bool,
    pub seed: u64,
    pub restarts: u32,
    pub iters: u32,
    pub sample: u64,
    /// Final within-cluster sum of squared distances (diagnostic).
    pub inertia: f64,
}

/// Population statistics of the Voronoi cells.
#[derive(Debug, Clone, PartialEq)]
pub struct CellStats {
    pub counts: Vec<u32>,
    pub min: u32,
    pub max: u32,
    pub mean: f64,
    pub std: f64,
}

impl CellStats {
    /// Compute stats from per-cell counts.
    pub fn compute(counts: &[u32]) -> Self {
        if counts.is_empty() {
            return Self {
                counts: Vec::new(),
                min: 0,
                max: 0,
                mean: 0.0,
                std: 0.0,
            };
        }
        let n = counts.len() as f64;
        let mean = counts.iter().map(|c| *c as f64).sum::<f64>() / n;
        let var = counts
            .iter()
            .map(|c| {
                let d = *c as f64 - mean;
                d * d
            })
            .sum::<f64>()
            / n;
        Self {
            min: counts.iter().copied().min().unwrap_or(0),
            max: counts.iter().copied().max().unwrap_or(0),
            mean,
            std: var.sqrt(),
            counts: counts.to_vec(),
        }
    }
}

/// Why a cell layout should be retrained.
#[derive(Debug, Clone, PartialEq)]
pub enum RetrainTrigger {
    None,
    /// Cells are unbalanced: `ratio = max_count / mean` exceeds the threshold.
    PopulationImbalance { ratio: f64 },
    /// Recall fell below target (measured by the tuning/oracle harness).
    RecallBelow { target: f64, observed: f64 },
}

/// Search for the cheapest `(nlist, nprobe)` meeting a Recall@K target.
#[derive(Debug, Clone)]
pub struct TuneConfig {
    pub target_recall: f64,
    pub k: usize,
    /// Candidate `(nlist, nprobe)` pairs to try.
    pub candidates: Vec<(usize, usize)>,
    /// Number of sampled queries used to estimate recall.
    pub queries: usize,
    pub seed: u64,
}

/// The winning `(nlist, nprobe)` and its measured recall / cost.
#[derive(Debug, Clone, PartialEq)]
pub struct TuneResult {
    pub nlist: usize,
    pub nprobe: usize,
    pub recall: f64,
    /// Mean number of candidate vectors scanned per query (cost proxy).
    pub probed_rows: f64,
}

/// Inverted-file index with an exact `f32` scan over the probed cells.
#[derive(Debug, Clone)]
pub struct IvfIndex {
    /// Row-major `f32` corpus, reordered so cell `l` occupies the contiguous
    /// vector range `list_offsets[l]..list_offsets[l+1]`.
    data: Vec<f32>,
    /// Ids reordered to match `data`.
    ids: Vec<u64>,
    /// Original input position of each reordered vector (the `BooleanArray`
    /// filter-mask domain, kept consistent with [`crate::FlatIndex`]).
    orig_pos: Vec<u32>,
    dim: usize,
    metric: Metric,
    nlist: usize,
    nprobe: usize,
    /// Coarse quantizer: `nlist × dim` centroids, row-major.
    centroids: Vec<f32>,
    /// `list_offsets[l..l+1]` = vector range of cell `l` in `data`/`ids`.
    list_offsets: Vec<u32>,
    /// How the coarse quantizer was trained (`None` for legacy/uniform builds).
    training: Option<IvfTrainingMeta>,
}

impl IvfIndex {
    /// Build an L2 index from a contiguous row-major `f32` corpus.
    pub fn new(
        ids: Vec<u64>,
        data: Vec<f32>,
        dim: usize,
        nlist: usize,
        nprobe: usize,
    ) -> Result<Self> {
        Self::with_metric(ids, data, dim, nlist, nprobe, Metric::L2)
    }

    /// Build an index with an explicit metric, training the coarse quantizer
    /// with k-means++ / Lloyd (see [`KMeansConfig::for_nlist`]). Large corpora
    /// are trained on a deterministic 50k sample.
    pub fn with_metric(
        ids: Vec<u64>,
        data: Vec<f32>,
        dim: usize,
        nlist: usize,
        nprobe: usize,
        metric: Metric,
    ) -> Result<Self> {
        let mut cfg = KMeansConfig::for_nlist(nlist);
        if ids.len() > 50_000 {
            cfg.sample = Some(50_000);
        }
        Self::with_config(ids, data, dim, nlist, nprobe, metric, &cfg)
    }

    /// Build with an explicit k-means configuration. Centroids are trained with
    /// k-means++ + Lloyd (multiple restarts, lowest inertia), empty cells are
    /// reseeded to the farthest point and — when enabled — oversized cells are
    /// split. The corpus is reordered so each cell is a contiguous run.
    pub fn with_config(
        ids: Vec<u64>,
        mut data: Vec<f32>,
        dim: usize,
        nlist: usize,
        nprobe: usize,
        metric: Metric,
        cfg: &KMeansConfig,
    ) -> Result<Self> {
        if dim == 0 {
            return Err(GtvError::InvalidArgument("zero-dimension vectors".into()));
        }
        let n = ids.len();
        if data.len() != n * dim {
            return Err(GtvError::InvalidArgument(
                "data length != ids.len() * dim".into(),
            ));
        }
        if n == 0 {
            return Err(GtvError::InvalidArgument("empty index".into()));
        }
        let nlist = nlist.max(1).min(n);
        let mut cfg = cfg.clone();
        cfg.nlist = nlist;

        // Cosine needs unit rows so `1 - dot` is the cosine distance.
        if metric.requires_normalization() {
            for row in data.chunks_mut(dim) {
                metric.normalize_in_place(row);
            }
        }

        let (mut centroids, mut labels, inertia) = kmeans_train(&data, n, dim, metric, &cfg);
        if cfg.split_oversized {
            (centroids, labels) =
                split_oversized_cells(&data, n, dim, metric, centroids, labels, &cfg);
        }
        let nlist = centroids.len() / dim;
        let nprobe = nprobe.clamp(1, nlist);
        let training = Some(IvfTrainingMeta {
            kmeans: true,
            seed: cfg.seed,
            restarts: cfg.restarts as u32,
            iters: cfg.max_iters as u32,
            sample: cfg.sample.map(|s| s.min(n)).unwrap_or(n) as u64,
            inertia,
        });
        Self::assemble(
            ids, data, dim, nlist, nprobe, metric, centroids, labels, training,
        )
    }

    /// Assemble an index from a coarse quantizer (`centroids`) and per-row cell
    /// `labels` (length `ids.len()`), reordering the corpus by cell.
    #[allow(clippy::too_many_arguments)]
    fn assemble(
        ids: Vec<u64>,
        data: Vec<f32>,
        dim: usize,
        nlist: usize,
        nprobe: usize,
        metric: Metric,
        centroids: Vec<f32>,
        labels: Vec<u32>,
        training: Option<IvfTrainingMeta>,
    ) -> Result<Self> {
        let n = ids.len();
        debug_assert_eq!(labels.len(), n);
        debug_assert_eq!(centroids.len(), nlist * dim);

        // Cell sizes → prefix-sum offsets (in vector units).
        let mut counts = vec![0u32; nlist];
        for &l in &labels {
            counts[l as usize] += 1;
        }
        let mut list_offsets = vec![0u32; nlist + 1];
        for l in 0..nlist {
            list_offsets[l + 1] = list_offsets[l] + counts[l];
        }

        // Reorder corpus + ids + original positions so each cell is contiguous.
        let mut reordered = vec![0.0f32; n * dim];
        let mut reordered_ids = vec![0u64; n];
        let mut orig_pos = vec![0u32; n];
        let mut cursors = list_offsets.clone();
        for i in 0..n {
            let l = labels[i] as usize;
            let dst = cursors[l] as usize;
            reordered[dst * dim..(dst + 1) * dim].copy_from_slice(&data[i * dim..(i + 1) * dim]);
            reordered_ids[dst] = ids[i];
            orig_pos[dst] = i as u32;
            cursors[l] += 1;
        }

        Ok(Self {
            data: reordered,
            ids: reordered_ids,
            orig_pos,
            dim,
            metric,
            nlist,
            nprobe,
            centroids,
            list_offsets,
            training,
        })
    }

    /// Build the historical way: evenly-spaced uniform centroid sampling. Kept
    /// as a deterministic baseline / fallback for recall comparisons.
    ///
    /// `nlist` is clamped to `n`; `nprobe` is clamped to `nlist`. Every vector
    /// is assigned to its nearest centroid and the corpus is reordered by cell
    /// for sequential probe reads.
    pub fn with_uniform(
        ids: Vec<u64>,
        mut data: Vec<f32>,
        dim: usize,
        nlist: usize,
        nprobe: usize,
        metric: Metric,
    ) -> Result<Self> {
        if dim == 0 {
            return Err(GtvError::InvalidArgument("zero-dimension vectors".into()));
        }
        let n = ids.len();
        if data.len() != n * dim {
            return Err(GtvError::InvalidArgument(
                "data length != ids.len() * dim".into(),
            ));
        }
        if n == 0 {
            return Err(GtvError::InvalidArgument("empty index".into()));
        }
        let nlist = nlist.max(1).min(n);
        let nprobe = nprobe.max(1).min(nlist);

        // Cosine needs unit rows so `1 - dot` is the cosine distance.
        if metric.requires_normalization() {
            for row in data.chunks_mut(dim) {
                metric.normalize_in_place(row);
            }
        }

        // Coarse quantizer: evenly-spaced deterministic centroid sampling.
        let mut centroids = Vec::with_capacity(nlist * dim);
        for c in 0..nlist {
            let src = c * n / nlist;
            centroids.extend_from_slice(&data[src * dim..(src + 1) * dim]);
        }
        if metric.requires_normalization() {
            for row in centroids.chunks_mut(dim) {
                metric.normalize_in_place(row);
            }
        }

        #[cfg(target_arch = "x86_64")]
        let use_simd =
            std::arch::is_x86_feature_detected!("avx2") && std::arch::is_x86_feature_detected!("fma");
        #[cfg(not(target_arch = "x86_64"))]
        let use_simd = false;

        // Assign every vector to its nearest centroid (parallel, exact f32).
        use rayon::prelude::*;
        let labels: Vec<u32> = (0..n)
            .into_par_iter()
            .map(|i| {
                let v = &data[i * dim..(i + 1) * dim];
                nearest_centroid(v, &centroids, nlist, dim, use_simd, metric)
            })
            .collect();

        // Cell sizes → prefix-sum offsets (in vector units).
        let mut counts = vec![0u32; nlist];
        for &l in &labels {
            counts[l as usize] += 1;
        }
        let mut list_offsets = vec![0u32; nlist + 1];
        for l in 0..nlist {
            list_offsets[l + 1] = list_offsets[l] + counts[l];
        }

        // Reorder corpus + ids + original positions so each cell is contiguous.
        let mut reordered = vec![0.0f32; n * dim];
        let mut reordered_ids = vec![0u64; n];
        let mut orig_pos = vec![0u32; n];
        let mut cursors = list_offsets.clone();
        for i in 0..n {
            let l = labels[i] as usize;
            let dst = cursors[l] as usize;
            reordered[dst * dim..(dst + 1) * dim].copy_from_slice(&data[i * dim..(i + 1) * dim]);
            reordered_ids[dst] = ids[i];
            orig_pos[dst] = i as u32;
            cursors[l] += 1;
        }

        Ok(Self {
            data: reordered,
            ids: reordered_ids,
            orig_pos,
            dim,
            metric,
            nlist,
            nprobe,
            centroids,
            list_offsets,
            training: None,
        })
    }

    pub fn len(&self) -> usize {
        self.ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    pub fn metric(&self) -> Metric {
        self.metric
    }

    pub fn nlist(&self) -> usize {
        self.nlist
    }

    pub fn nprobe(&self) -> usize {
        self.nprobe
    }

    /// Reconstruct from raw (already-normalized) components, e.g. after loading
    /// a persisted index. No retraining is performed.
    #[allow(clippy::too_many_arguments)]
    pub fn from_raw(
        data: Vec<f32>,
        ids: Vec<u64>,
        orig_pos: Vec<u32>,
        dim: usize,
        metric: Metric,
        nlist: usize,
        nprobe: usize,
        centroids: Vec<f32>,
        list_offsets: Vec<u32>,
    ) -> Result<Self> {
        Self::from_raw_with_training(
            data, ids, orig_pos, dim, metric, nlist, nprobe, centroids, list_offsets, None,
        )
    }

    /// Like [`IvfIndex::from_raw`] but restores the coarse-quantizer training
    /// metadata persisted by a `GIVFv2` payload.
    #[allow(clippy::too_many_arguments)]
    pub fn from_raw_with_training(
        data: Vec<f32>,
        ids: Vec<u64>,
        orig_pos: Vec<u32>,
        dim: usize,
        metric: Metric,
        nlist: usize,
        nprobe: usize,
        centroids: Vec<f32>,
        list_offsets: Vec<u32>,
        training: Option<IvfTrainingMeta>,
    ) -> Result<Self> {
        let n = ids.len();
        if dim == 0 || data.len() != n * dim {
            return Err(GtvError::InvalidArgument("ivf: bad data length".into()));
        }
        if orig_pos.len() != n {
            return Err(GtvError::InvalidArgument("ivf: bad orig_pos length".into()));
        }
        if centroids.len() != nlist * dim {
            return Err(GtvError::InvalidArgument("ivf: bad centroid length".into()));
        }
        if list_offsets.len() != nlist + 1
            || list_offsets.last().copied().unwrap_or(0) as usize != n
        {
            return Err(GtvError::InvalidArgument("ivf: bad list offsets".into()));
        }
        Ok(Self {
            data,
            ids,
            orig_pos,
            dim,
            metric,
            nlist,
            nprobe,
            centroids,
            list_offsets,
            training,
        })
    }

    /// The training provenance, when the index was built by this crate.
    pub fn training(&self) -> Option<&IvfTrainingMeta> {
        self.training.as_ref()
    }

    /// Population statistics of the Voronoi cells.
    pub fn cell_stats(&self) -> CellStats {
        let counts: Vec<u32> = (0..self.nlist)
            .map(|l| self.list_offsets[l + 1] - self.list_offsets[l])
            .collect();
        CellStats::compute(&counts)
    }

    /// Whether the cell layout should be retrained: `true` when the largest
    /// cell exceeds `ratio_threshold × mean`.
    pub fn retrain_trigger(&self, ratio_threshold: f64) -> RetrainTrigger {
        let stats = self.cell_stats();
        if stats.mean > 0.0 {
            let ratio = stats.max as f64 / stats.mean;
            if ratio > ratio_threshold {
                return RetrainTrigger::PopulationImbalance { ratio };
            }
        }
        RetrainTrigger::None
    }

    /// Number of candidate vectors a query would scan (sum of probed cell
    /// sizes) — the IVF cost proxy used by [`tune_ivf`].
    pub fn probed_rows(&self, query: &[f32]) -> usize {
        let q = self.prepare_query(query);
        let use_simd = simd_enabled();
        let mut cd: Vec<(f32, u32)> = (0..self.nlist)
            .map(|c| {
                let row = &self.centroids[c * self.dim..(c + 1) * self.dim];
                (metric_distance(&q, row, self.metric, use_simd), c as u32)
            })
            .collect();
        cd.sort_by(|a, b| {
            a.0.partial_cmp(&b.0)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.1.cmp(&b.1))
        });
        cd.truncate(self.nprobe.min(self.nlist));
        cd.iter()
            .map(|(_, c)| {
                (self.list_offsets[*c as usize + 1] - self.list_offsets[*c as usize]) as usize
            })
            .sum()
    }

    /// Serialize to the versioned `GIVFv1` (uniform) or `GIVFv2` (with
    /// training metadata) format.
    pub fn to_bytes(&self) -> Vec<u8> {
        let n = self.ids.len();
        let magic = if self.training.is_some() {
            IVF_MAGIC_V2
        } else {
            IVF_MAGIC
        };
        let mut w = Writer::with_capacity(
            64 + n * 12
                + self.data.len() * 4
                + self.centroids.len() * 4
                + self.list_offsets.len() * 4,
        );
        w.bytes(magic)
            .u32(self.dim as u32)
            .u8(metric_code(self.metric))
            .u32(self.nlist as u32)
            .u32(self.nprobe as u32)
            .u64(n as u64);
        for id in &self.ids {
            w.u64(*id);
        }
        for p in &self.orig_pos {
            w.u32(*p);
        }
        for v in &self.data {
            w.f32(*v);
        }
        for c in &self.centroids {
            w.f32(*c);
        }
        for o in &self.list_offsets {
            w.u32(*o);
        }
        if let Some(t) = &self.training {
            w.u8(t.kmeans as u8)
                .u64(t.seed)
                .u32(t.restarts)
                .u32(t.iters)
                .u64(t.sample)
                .u64(t.inertia.to_bits());
        }
        w.into_vec()
    }

    /// Deserialize a [`IvfIndex::to_bytes`] payload (`GIVFv1` or `GIVFv2`).
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut r = Reader::new(bytes);
        let magic = r.take(8)?;
        let is_v2 = magic == &IVF_MAGIC_V2[..];
        if magic != &IVF_MAGIC[..] && !is_v2 {
            return Err(GtvError::InvalidArgument("ivf: bad magic".into()));
        }
        let dim = r.u32()? as usize;
        let metric = metric_from_code(r.u8()?)?;
        let nlist = r.u32()? as usize;
        let nprobe = r.u32()? as usize;
        let n = r.u64()? as usize;
        let mut ids = Vec::with_capacity(n);
        for _ in 0..n {
            ids.push(r.u64()?);
        }
        let mut orig_pos = Vec::with_capacity(n);
        for _ in 0..n {
            orig_pos.push(r.u32()?);
        }
        let mut data = Vec::with_capacity(n * dim);
        for _ in 0..n * dim {
            data.push(r.f32()?);
        }
        let mut centroids = Vec::with_capacity(nlist * dim);
        for _ in 0..nlist * dim {
            centroids.push(r.f32()?);
        }
        let mut list_offsets = Vec::with_capacity(nlist + 1);
        for _ in 0..nlist + 1 {
            list_offsets.push(r.u32()?);
        }
        let training = if is_v2 {
            Some(IvfTrainingMeta {
                kmeans: r.u8()? != 0,
                seed: r.u64()?,
                restarts: r.u32()?,
                iters: r.u32()?,
                sample: r.u64()?,
                inertia: f64::from_bits(r.u64()?),
            })
        } else {
            None
        };
        Self::from_raw_with_training(
            data,
            ids,
            orig_pos,
            dim,
            metric,
            nlist,
            nprobe,
            centroids,
            list_offsets,
            training,
        )
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

    /// Exact `f32` top-K over the `nprobe` nearest cells.
    fn search_scored(&self, query: &[f32], k: usize, mask: Option<&BooleanArray>) -> Vec<(f32, u64)> {
        #[cfg(target_arch = "x86_64")]
        let use_simd =
            std::arch::is_x86_feature_detected!("avx2") && std::arch::is_x86_feature_detected!("fma");
        #[cfg(not(target_arch = "x86_64"))]
        let use_simd = false;

        let query = self.prepare_query(query);
        let metric = self.metric;

        // 1. Distance to every centroid, then keep the `nprobe` nearest.
        let nprobe = self.nprobe.min(self.nlist);
        let mut cd: Vec<(f32, u32)> = (0..self.nlist)
            .map(|c| {
                let row = &self.centroids[c * self.dim..(c + 1) * self.dim];
                (
                    metric_distance(&query, row, metric, use_simd),
                    c as u32,
                )
            })
            .collect();
        cd.sort_by(|a, b| {
            a.0.partial_cmp(&b.0)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.1.cmp(&b.1))
        });
        cd.truncate(nprobe);

        // 2. Gather candidate vector indices from the probed cells (contiguous
        //    runs), dropping any position the caller's bitmask excludes.
        let mut positions: Vec<u32> = Vec::new();
        for &(_, c) in &cd {
            let lo = self.list_offsets[c as usize];
            let hi = self.list_offsets[c as usize + 1];
            match mask {
                Some(m) => positions.extend(
                    (lo..hi).filter(|&p| m.value(self.orig_pos[p as usize] as usize)),
                ),
                None => positions.extend(lo..hi),
            }
        }

        // 3. Exact bounded top-K scan over the candidates (parallel, sequential
        //    reads within each contiguous cell).
        let data = &self.data;
        let ids = &self.ids;
        let dim = self.dim;
        let n = ids.len();
        par_topk_over(&positions, k, |pos| {
            let i = pos as usize;
            // IVF probes are *indirect* row reads (positions within probed lists):
            // prefetch the next row so the 2 KB DRAM fetch overlaps the current
            // row's distance computation (hardware prefetch cannot follow the
            // non-contiguous list positions).
            #[cfg(target_arch = "x86_64")]
            if i + 1 < n {
                unsafe {
                    std::arch::x86_64::_mm_prefetch(
                        data.as_ptr().add((i + 1) * dim) as *const _,
                        std::arch::x86_64::_MM_HINT_T0,
                    );
                }
            }
            let row = &data[i * dim..(i + 1) * dim];
            (metric_distance(&query, row, metric, use_simd), ids[i])
        })
    }
}

/// Find the nearest centroid to `v` (index only). Branches on `metric` once per
/// vector so the L2 path keeps its inlined AVX2 kernel and the other metrics
/// use the (SIMD-accelerated) generic distance.
#[inline]
fn nearest_centroid(
    v: &[f32],
    centroids: &[f32],
    nlist: usize,
    dim: usize,
    use_simd: bool,
    metric: Metric,
) -> u32 {
    if metric == Metric::L2 {
        #[cfg(target_arch = "x86_64")]
        {
            if use_simd {
                // SAFETY: AVX2+FMA were detected by the caller.
                return unsafe { nearest_centroid_avx2(v, centroids, nlist, dim) };
            }
        }
        #[cfg(not(target_arch = "x86_64"))]
        let _ = use_simd;
    }
    nearest_centroid_metric(v, centroids, nlist, dim, metric, use_simd)
}

#[inline]
fn nearest_centroid_metric(
    v: &[f32],
    centroids: &[f32],
    nlist: usize,
    dim: usize,
    metric: Metric,
    use_simd: bool,
) -> u32 {
    let mut best_i = 0u32;
    let mut best_d = f32::INFINITY;
    for c in 0..nlist {
        let row = &centroids[c * dim..(c + 1) * dim];
        let d = metric_distance(v, row, metric, use_simd);
        if d < best_d {
            best_d = d;
            best_i = c as u32;
        }
    }
    best_i
}

/// AVX2+FMA nearest-centroid search: the 4-accumulator squared-L2 kernel is
/// inlined per centroid (this function has the target features, unlike the
/// generic dispatch), so 1M × 1024 assignments run at SIMD throughput.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2", enable = "fma")]
unsafe fn nearest_centroid_avx2(v: &[f32], centroids: &[f32], nlist: usize, dim: usize) -> u32 {
    use std::arch::x86_64::*;
    let mut best_i = 0u32;
    let mut best_d = f32::INFINITY;

    for c in 0..nlist {
        let row = centroids.as_ptr().add(c * dim);
        let mut acc0 = _mm256_setzero_ps();
        let mut acc1 = _mm256_setzero_ps();
        let mut acc2 = _mm256_setzero_ps();
        let mut acc3 = _mm256_setzero_ps();
        let mut i = 0usize;

        while i + 32 <= dim {
            let q0 = _mm256_loadu_ps(v.as_ptr().add(i));
            let r0 = _mm256_loadu_ps(row.add(i));
            let d0 = _mm256_sub_ps(q0, r0);
            acc0 = _mm256_fmadd_ps(d0, d0, acc0);

            let q1 = _mm256_loadu_ps(v.as_ptr().add(i + 8));
            let r1 = _mm256_loadu_ps(row.add(i + 8));
            let d1 = _mm256_sub_ps(q1, r1);
            acc1 = _mm256_fmadd_ps(d1, d1, acc1);

            let q2 = _mm256_loadu_ps(v.as_ptr().add(i + 16));
            let r2 = _mm256_loadu_ps(row.add(i + 16));
            let d2 = _mm256_sub_ps(q2, r2);
            acc2 = _mm256_fmadd_ps(d2, d2, acc2);

            let q3 = _mm256_loadu_ps(v.as_ptr().add(i + 24));
            let r3 = _mm256_loadu_ps(row.add(i + 24));
            let d3 = _mm256_sub_ps(q3, r3);
            acc3 = _mm256_fmadd_ps(d3, d3, acc3);

            i += 32;
        }
        while i + 8 <= dim {
            let q = _mm256_loadu_ps(v.as_ptr().add(i));
            let r = _mm256_loadu_ps(row.add(i));
            let d = _mm256_sub_ps(q, r);
            acc0 = _mm256_fmadd_ps(d, d, acc0);
            i += 8;
        }

        let acc = _mm256_add_ps(_mm256_add_ps(acc0, acc1), _mm256_add_ps(acc2, acc3));
        let mut buf = [0.0f32; 8];
        _mm256_storeu_ps(buf.as_mut_ptr(), acc);
        let mut d = buf.iter().sum::<f32>();
        for j in i..dim {
            let dd = v[j] - *row.add(j);
            d += dd * dd;
        }

        if d < best_d {
            best_d = d;
            best_i = c as u32;
        }
    }
    best_i
}

/// Whether the x86 AVX2+FMA kernels are usable on this host.
fn simd_enabled() -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        std::arch::is_x86_feature_detected!("avx2")
            && std::arch::is_x86_feature_detected!("fma")
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        false
    }
}

/// Assign every row of `data` to its nearest centroid (parallel, exact f32).
fn assign_all(
    data: &[f32],
    n: usize,
    dim: usize,
    centroids: &[f32],
    nlist: usize,
    metric: Metric,
) -> Vec<u32> {
    use rayon::prelude::*;
    let use_simd = simd_enabled();
    (0..n)
        .into_par_iter()
        .map(|i| {
            nearest_centroid(
                &data[i * dim..(i + 1) * dim],
                centroids,
                nlist,
                dim,
                use_simd,
                metric,
            )
        })
        .collect()
}

/// k-means++ seeding: first centroid uniform, then each new centroid is
/// sampled with probability proportional to its squared distance to the
/// nearest chosen centroid.
fn kmeans_plus_plus(
    data: &[f32],
    n: usize,
    dim: usize,
    nlist: usize,
    metric: Metric,
    rng: &mut SplitMix64,
) -> Vec<f32> {
    let mut centroids = Vec::with_capacity(nlist * dim);
    if n == 0 {
        return centroids;
    }
    let first = (rng.next_u64() as usize) % n;
    centroids.extend_from_slice(&data[first * dim..(first + 1) * dim]);

    let mut d2 = vec![f32::INFINITY; n];
    for c in 1..nlist {
        let last = &centroids[(c - 1) * dim..c * dim];
        let mut sum = 0.0f64;
        for i in 0..n {
            let d = metric_distance(&data[i * dim..(i + 1) * dim], last, metric, false).max(0.0);
            if d < d2[i] {
                d2[i] = d;
            }
            sum += d2[i] as f64;
        }
        let chosen = if sum <= 0.0 {
            (rng.next_u64() as usize) % n
        } else {
            let target = rng.next_f64() * sum;
            let mut acc = 0.0f64;
            let mut pick = n - 1;
            for i in 0..n {
                acc += d2[i] as f64;
                if acc >= target {
                    pick = i;
                    break;
                }
            }
            pick
        };
        centroids.extend_from_slice(&data[chosen * dim..(chosen + 1) * dim]);
    }
    if metric.requires_normalization() {
        for row in centroids.chunks_mut(dim) {
            metric.normalize_in_place(row);
        }
    }
    centroids
}

/// One Lloyd update: centroid = mean of its assigned rows. Empty cells are
/// reseeded to the point farthest from its (new) centroid, so no cell stays
/// empty. Returns the new centroids and whether any cell was empty.
fn recompute_centroids(
    data: &[f32],
    labels: &[u32],
    n: usize,
    dim: usize,
    nlist: usize,
    metric: Metric,
    old: &[f32],
) -> (Vec<f32>, bool) {
    let mut sums = vec![0.0f64; nlist * dim];
    let mut counts = vec![0u32; nlist];
    for i in 0..n {
        let l = labels[i] as usize;
        counts[l] += 1;
        for d in 0..dim {
            sums[l * dim + d] += data[i * dim + d] as f64;
        }
    }

    let mut newc = vec![0.0f32; nlist * dim];
    let mut any_empty = false;
    for l in 0..nlist {
        if counts[l] > 0 {
            for d in 0..dim {
                newc[l * dim + d] = (sums[l * dim + d] / counts[l] as f64) as f32;
            }
        } else {
            any_empty = true;
        }
    }
    if metric.requires_normalization() {
        for row in newc.chunks_mut(dim) {
            metric.normalize_in_place(row);
        }
    }

    if any_empty {
        let mut used = vec![false; n];
        for l in 0..nlist {
            if counts[l] != 0 {
                continue;
            }
            // Farthest point from its current centroid (excluding points
            // already claimed by another reseeded cell).
            let mut best_i = usize::MAX;
            let mut best_d = f32::NEG_INFINITY;
            for i in 0..n {
                if used[i] {
                    continue;
                }
                let cl = labels[i] as usize;
                let c = &newc[cl * dim..(cl + 1) * dim];
                let d = metric_distance(&data[i * dim..(i + 1) * dim], c, metric, false);
                if d > best_d {
                    best_d = d;
                    best_i = i;
                }
            }
            if best_i == usize::MAX {
                best_i = l % n.max(1);
            }
            used[best_i] = true;
            newc[l * dim..(l + 1) * dim].copy_from_slice(&data[best_i * dim..(best_i + 1) * dim]);
            if metric.requires_normalization() {
                let off = l * dim;
                let row = &mut newc[off..off + dim];
                metric.normalize_in_place(row);
            }
            counts[l] = 1;
        }
    }

    let _ = old;
    (newc, any_empty)
}

/// Sum of within-cluster distances (the k-means objective).
fn inertia_of(
    data: &[f32],
    labels: &[u32],
    centroids: &[f32],
    dim: usize,
    metric: Metric,
) -> f64 {
    let mut s = 0.0f64;
    for (i, &l) in labels.iter().enumerate() {
        let l = l as usize;
        s += metric_distance(
            &data[i * dim..(i + 1) * dim],
            &centroids[l * dim..(l + 1) * dim],
            metric,
            false,
        ) as f64;
    }
    s
}

/// Train an IVF coarse quantizer with k-means++ / Lloyd.
///
/// Returns `(centroids, assignments, inertia)` where `assignments` has one
/// entry per input row (`n`), not just per training-sample row. Deterministic
/// for a given `cfg.seed`. Multiple restarts keep the lowest-inertia result and
/// empty cells are reseeded to the farthest point.
pub fn kmeans_train(
    data: &[f32],
    n: usize,
    dim: usize,
    metric: Metric,
    cfg: &KMeansConfig,
) -> (Vec<f32>, Vec<u32>, f64) {
    let nlist = cfg.nlist.max(1).min(n.max(1));
    if n == 0 || dim == 0 {
        return (Vec::new(), Vec::new(), 0.0);
    }
    let mut rng = SplitMix64::new(cfg.seed);

    // Deterministic training sample (without replacement).
    let sample_n = cfg.sample.map(|s| s.clamp(nlist, n)).unwrap_or(n);
    let train: Vec<f32> = if sample_n >= n {
        data[..n * dim].to_vec()
    } else {
        let mut idx: Vec<usize> = (0..n).collect();
        for i in 0..sample_n {
            let j = i + (rng.next_u64() as usize) % (n - i);
            idx.swap(i, j);
        }
        let mut t = Vec::with_capacity(sample_n * dim);
        for &i in &idx[..sample_n] {
            t.extend_from_slice(&data[i * dim..(i + 1) * dim]);
        }
        t
    };

    let mut best_centroids = Vec::new();
    let mut best_inertia = f64::INFINITY;
    for _ in 0..cfg.restarts.max(1) {
        let mut centroids = kmeans_plus_plus(&train, sample_n, dim, nlist, metric, &mut rng);
        let mut labels;
        for _ in 0..cfg.max_iters.max(1) {
            labels = assign_all(&train, sample_n, dim, &centroids, nlist, metric);
            let (newc, _empty) =
                recompute_centroids(&train, &labels, sample_n, dim, nlist, metric, &centroids);
            let converged = newc == centroids;
            centroids = newc;
            if converged {
                break;
            }
        }
        labels = assign_all(&train, sample_n, dim, &centroids, nlist, metric);
        let inertia = inertia_of(&train, &labels, &centroids, dim, metric);
        if inertia < best_inertia {
            best_inertia = inertia;
            best_centroids = centroids;
        }
    }

    let mut centroids = best_centroids;
    let mut labels_all = assign_all(data, n, dim, &centroids, nlist, metric);

    // The final assignment over the *full* corpus can still leave a cell empty
    // (a centroid whose nearest neighbours were all claimed elsewhere). Reseed
    // empty cells to the farthest point until the layout is dense.
    for _ in 0..8 {
        let mut counts = vec![0u32; nlist];
        for &l in &labels_all {
            counts[l as usize] += 1;
        }
        let empties: Vec<usize> = (0..nlist).filter(|&l| counts[l] == 0).collect();
        if empties.is_empty() {
            break;
        }
        let mut used = vec![false; n];
        for l in empties {
            let mut best_i = usize::MAX;
            let mut best_d = f32::NEG_INFINITY;
            for i in 0..n {
                if used[i] {
                    continue;
                }
                let cl = labels_all[i] as usize;
                let d = metric_distance(
                    &data[i * dim..(i + 1) * dim],
                    &centroids[cl * dim..(cl + 1) * dim],
                    metric,
                    false,
                );
                if d > best_d {
                    best_d = d;
                    best_i = i;
                }
            }
            if best_i == usize::MAX {
                best_i = l % n;
            }
            used[best_i] = true;
            centroids[l * dim..(l + 1) * dim]
                .copy_from_slice(&data[best_i * dim..(best_i + 1) * dim]);
            if metric.requires_normalization() {
                let off = l * dim;
                metric.normalize_in_place(&mut centroids[off..off + dim]);
            }
        }
        labels_all = assign_all(data, n, dim, &centroids, nlist, metric);
    }

    let inertia = inertia_of(data, &labels_all, &centroids, dim, metric);
    (centroids, labels_all, inertia)
}

/// Split oversized cells (count > `mean + k·std`) by adding a centroid at the
/// farthest point of the largest cell, then refining with Lloyd. Repeats until
/// the population is balanced or a growth cap is hit.
fn split_oversized_cells(
    data: &[f32],
    n: usize,
    dim: usize,
    metric: Metric,
    mut centroids: Vec<f32>,
    mut labels: Vec<u32>,
    cfg: &KMeansConfig,
) -> (Vec<f32>, Vec<u32>) {
    let mut nlist = centroids.len() / dim;
    let max_lists = (cfg.nlist.max(1) * 4).max(nlist);
    for _guard in 0..16 {
        if nlist >= max_lists {
            break;
        }
        let mut counts = vec![0u32; nlist];
        for &l in &labels {
            counts[l as usize] += 1;
        }
        let stats = CellStats::compute(&counts);
        if stats.std <= 0.0 || stats.mean <= 0.0 {
            break;
        }
        let threshold = stats.mean + cfg.split_threshold_k * stats.std;
        if stats.max as f64 <= threshold {
            break;
        }
        let big = counts
            .iter()
            .enumerate()
            .max_by_key(|(_, c)| **c)
            .map(|(i, _)| i)
            .unwrap_or(0);
        let c = &centroids[big * dim..(big + 1) * dim];
        let far = (0..n)
            .filter(|&i| labels[i] as usize == big)
            .max_by(|&a, &b| {
                let da = metric_distance(&data[a * dim..(a + 1) * dim], c, metric, false);
                let db = metric_distance(&data[b * dim..(b + 1) * dim], c, metric, false);
                da.partial_cmp(&db).unwrap_or(Ordering::Equal)
            })
            .unwrap_or(0);
        centroids.extend_from_slice(&data[far * dim..(far + 1) * dim]);
        nlist += 1;
        for _ in 0..5 {
            labels = assign_all(data, n, dim, &centroids, nlist, metric);
            let (newc, _empty) = recompute_centroids(data, &labels, n, dim, nlist, metric, &centroids);
            centroids = newc;
        }
        labels = assign_all(data, n, dim, &centroids, nlist, metric);
    }
    (centroids, labels)
}

/// Search the candidate `(nlist, nprobe)` grid for the cheapest combination
/// whose measured Recall@K against a flat oracle meets `cfg.target_recall`.
/// Ties are broken by the number of scanned candidate rows (cost proxy).
pub fn tune_ivf(
    ids: &[u64],
    data: &[f32],
    dim: usize,
    metric: Metric,
    cfg: &TuneConfig,
) -> Result<TuneResult> {
    let curve = tune_ivf_curve(ids, data, dim, metric, cfg)?;
    select_tuned(&curve, cfg.target_recall)
        .ok_or_else(|| GtvError::InvalidArgument("ivf: no tuning candidates".into()))
}

/// Pick the cheapest curve entry meeting `target_recall` (highest recall when
/// none do). Accepts an already-computed [`tune_ivf_curve`].
pub fn select_tuned(curve: &[TuneResult], target_recall: f64) -> Option<TuneResult> {
    let mut best: Option<TuneResult> = None;
    for cand in curve {
        let better = match &best {
            None => true,
            Some(b) => {
                let b_ok = b.recall + 1e-9 >= target_recall;
                let c_ok = cand.recall + 1e-9 >= target_recall;
                match (b_ok, c_ok) {
                    (true, true) => cand.probed_rows < b.probed_rows,
                    (false, true) => true,
                    (true, false) => false,
                    (false, false) => cand.recall > b.recall,
                }
            }
        };
        if better {
            best = Some(cand.clone());
        }
    }
    best
}

/// Evaluate every `(nlist, nprobe)` candidate, returning the full
/// recall/cost curve in candidate order (for reporting / plotting).
pub fn tune_ivf_curve(
    ids: &[u64],
    data: &[f32],
    dim: usize,
    metric: Metric,
    cfg: &TuneConfig,
) -> Result<Vec<TuneResult>> {
    let n = ids.len();
    if n == 0 {
        return Err(GtvError::InvalidArgument("ivf: empty corpus".into()));
    }
    if cfg.candidates.is_empty() {
        return Err(GtvError::InvalidArgument("ivf: no tuning candidates".into()));
    }
    let k = cfg.k.max(1).min(n);
    let oracle = FlatIndex::from_flat_metric(ids.to_vec(), data.to_vec(), dim, metric)?;

    let mut rng = SplitMix64::new(cfg.seed);
    let qn = cfg.queries.max(1).min(n);
    let mut qidx: Vec<usize> = (0..n).collect();
    for i in 0..qn {
        let j = i + (rng.next_u64() as usize) % (n - i);
        qidx.swap(i, j);
    }
    let qidx = &qidx[..qn];

    let mut out = Vec::with_capacity(cfg.candidates.len());
    for &(nlist, nprobe) in &cfg.candidates {
        let index = IvfIndex::with_metric(ids.to_vec(), data.to_vec(), dim, nlist, nprobe, metric)?;
        let mut recall_sum = 0.0f64;
        let mut rows_sum = 0usize;
        for &qi in qidx {
            let q = &data[qi * dim..(qi + 1) * dim];
            let exact: Vec<u64> = oracle.search(q, k, None)?.iter().map(|h| h.id).collect();
            let got: Vec<u64> = index.search(q, k, None)?.iter().map(|h| h.id).collect();
            let hits = got.iter().filter(|id| exact.contains(id)).count();
            recall_sum += hits as f64 / k as f64;
            rows_sum += index.probed_rows(q);
        }
        out.push(TuneResult {
            nlist,
            nprobe,
            recall: recall_sum / qn as f64,
            probed_rows: rows_sum as f64 / qn as f64,
        });
    }
    Ok(out)
}

impl VectorIndex for IvfIndex {
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
            if mask.len() != self.ids.len() {
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
    use crate::flat::FlatIndex;

    #[test]
    fn ivf_recovers_nearest_on_small_corpus() {
        // 6 nodes, 2 dims. With nlist=3, nprobe=3 the scan degenerates to exact.
        let data = vec![
            0.0, 0.0, // 0
            1.0, 1.0, // 1
            5.0, 5.0, // 2
            5.2, 5.1, // 3
            9.0, 9.0, // 4
            9.1, 9.0, // 5
        ];
        let ids: Vec<u64> = (0..6).collect();
        let ivf = IvfIndex::new(ids.clone(), data.clone(), 2, 3, 3).unwrap();
        let got = ivf.search_knn(&[5.0, 5.0], 3, None).unwrap();
        // Nearest to (5,5) is node 2, then 3, then 1.
        assert_eq!(got.values().as_ref(), &[2, 3, 1]);

        let flat = FlatIndex::from_flat(ids, data, 2).unwrap();
        let exact = flat.search_knn(&[5.0, 5.0], 3, None).unwrap();
        assert_eq!(got.values().as_ref(), exact.values().as_ref());
    }

    #[test]
    fn ivf_cosine_matches_flat() {
        let data = vec![
            3.0, 4.0, // 0 -> (0.6, 0.8)
            1.0, 0.0, // 1
            0.0, 1.0, // 2
            3.0, 4.0, // 3 (same direction as 0)
        ];
        let ids: Vec<u64> = (0..4).collect();
        let ivf =
            IvfIndex::with_metric(ids.clone(), data.clone(), 2, 4, 4, Metric::Cosine).unwrap();
        assert_eq!(ivf.metric(), Metric::Cosine);
        let flat =
            FlatIndex::from_flat_metric(ids, data, 2, Metric::Cosine).unwrap();
        let q = [1.0f32, 0.0];
        let got: Vec<u64> = ivf.search(&q, 4, None).unwrap().iter().map(|h| h.id).collect();
        let exact: Vec<u64> = flat.search(&q, 4, None).unwrap().iter().map(|h| h.id).collect();
        assert_eq!(got, exact);
    }

    #[test]
    fn ivf_dimension_mismatch_is_typed() {
        let ivf = IvfIndex::new(vec![0, 1], vec![0.0, 0.0, 1.0, 1.0], 2, 2, 2).unwrap();
        assert!(matches!(
            ivf.search(&[1.0], 1, None),
            Err(GtvError::DimensionMismatch { .. })
        ));
    }

    #[test]
    fn bytes_round_trip() {
        let data = vec![
            0.0f32, 0.0, 1.0, 1.0, 5.0, 5.0, 5.2, 5.1, 9.0, 9.0, 9.1, 9.0,
        ];
        let ids: Vec<u64> = (0..6).collect();
        let ivf = IvfIndex::new(ids, data, 2, 3, 3).unwrap();
        let bytes = ivf.to_bytes();
        let back = IvfIndex::from_bytes(&bytes).unwrap();
        assert_eq!(back.nlist(), 3);
        assert_eq!(back.nprobe(), 3);
        let a: Vec<u64> = ivf
            .search(&[5.0, 5.0], 3, None)
            .unwrap()
            .iter()
            .map(|h| h.id)
            .collect();
        let b: Vec<u64> = back
            .search(&[5.0, 5.0], 3, None)
            .unwrap()
            .iter()
            .map(|h| h.id)
            .collect();
        assert_eq!(a, b);
    }

    // -- B3-4 k-means coarse quantizer --------------------------------------

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

    /// Sorted, highly imbalanced corpus: 500 points near 0, 90 near 50, 10 near
    /// 200 (dim 8). Sorted layout makes uniform centroid sampling oversample the
    /// first cluster, which is exactly what k-means fixes.
    fn clustered(seed: u64) -> (Vec<u64>, Vec<f32>, usize) {
        let dim = 8;
        let mut rng = Lcg(seed);
        let mut data = Vec::new();
        for (center, count) in [(0.0f32, 500usize), (50.0, 90), (200.0, 10)] {
            for _ in 0..count {
                for _ in 0..dim {
                    data.push(center + (rng.f32() - 0.5) * 2.0);
                }
            }
        }
        let n = data.len() / dim;
        let ids: Vec<u64> = (0..n as u64).collect();
        (ids, data, dim)
    }

    fn recall_at(
        index: &dyn VectorIndex,
        oracle: &dyn VectorIndex,
        data: &[f32],
        dim: usize,
        k: usize,
        queries: &[usize],
    ) -> f64 {
        let mut sum = 0.0;
        for &qi in queries {
            let q = &data[qi * dim..(qi + 1) * dim];
            let exact: Vec<u64> = oracle.search(q, k, None).unwrap().iter().map(|h| h.id).collect();
            let got: Vec<u64> = index.search(q, k, None).unwrap().iter().map(|h| h.id).collect();
            sum += got.iter().filter(|id| exact.contains(id)).count() as f64 / k as f64;
        }
        sum / queries.len() as f64
    }

    #[test]
    fn kmeans_recall_beats_uniform_on_imbalanced_corpus() {
        let (ids, data, dim) = clustered(7);
        let n = ids.len();
        let (k, nlist, nprobe) = (5usize, 3usize, 1usize);
        let oracle = FlatIndex::from_flat_metric(ids.clone(), data.clone(), dim, Metric::L2).unwrap();
        let kmeans = IvfIndex::with_metric(ids.clone(), data.clone(), dim, nlist, nprobe, Metric::L2)
            .unwrap();
        let uniform = IvfIndex::with_uniform(ids.clone(), data.clone(), dim, nlist, nprobe, Metric::L2)
            .unwrap();
        let queries: Vec<usize> = (0..n).step_by(17).collect();
        let rk = recall_at(&kmeans, &oracle, &data, dim, k, &queries);
        let ru = recall_at(&uniform, &oracle, &data, dim, k, &queries);
        assert!(rk > ru, "k-means recall {rk} should beat uniform {ru}");
        assert!(rk > 0.8, "k-means recall too low: {rk}");
    }

    #[test]
    fn kmeans_is_deterministic_for_a_seed() {
        let (ids, data, dim) = clustered(11);
        let cfg = KMeansConfig {
            nlist: 6,
            max_iters: 20,
            restarts: 3,
            sample: Some(300),
            seed: 42,
            split_oversized: false,
            split_threshold_k: 3.0,
        };
        let a = IvfIndex::with_config(ids.clone(), data.clone(), dim, 6, 2, Metric::L2, &cfg).unwrap();
        let b = IvfIndex::with_config(ids.clone(), data.clone(), dim, 6, 2, Metric::L2, &cfg).unwrap();
        assert_eq!(a.to_bytes(), b.to_bytes(), "same seed must reproduce centroids");

        let mut other = cfg.clone();
        other.seed = 43;
        let c = IvfIndex::with_config(ids, data, dim, 6, 2, Metric::L2, &other).unwrap();
        assert_ne!(a.to_bytes(), c.to_bytes(), "different seed should differ");
    }

    #[test]
    fn kmeans_leaves_no_empty_cells() {
        let (ids, data, dim) = clustered(3);
        let n = ids.len();
        let ivf = IvfIndex::with_metric(ids, data, dim, 16, 4, Metric::L2).unwrap();
        let stats = ivf.cell_stats();
        assert!(stats.min >= 1, "empty cell present: min={}", stats.min);
        assert_eq!(stats.counts.iter().map(|c| *c as usize).sum::<usize>(), n);
        assert_eq!(ivf.retrain_trigger(100.0), RetrainTrigger::None);
    }

    #[test]
    fn oversized_cells_are_split() {
        let (ids, data, dim) = clustered(5);
        let base = KMeansConfig {
            nlist: 2,
            max_iters: 20,
            restarts: 2,
            sample: None,
            seed: 1,
            split_oversized: false,
            split_threshold_k: 0.5,
        };
        let plain = IvfIndex::with_config(ids.clone(), data.clone(), dim, 2, 2, Metric::L2, &base).unwrap();
        let mut split = base.clone();
        split.split_oversized = true;
        let grown = IvfIndex::with_config(ids, data, dim, 2, 2, Metric::L2, &split).unwrap();
        assert!(
            grown.nlist() > plain.nlist(),
            "oversized cell should be split: {} -> {}",
            plain.nlist(),
            grown.nlist()
        );
        assert!(grown.training().is_some());
    }

    #[test]
    fn nprobe_recall_curve_is_monotonic() {
        let (ids, data, dim) = clustered(9);
        let n = ids.len();
        let oracle = FlatIndex::from_flat_metric(ids.clone(), data.clone(), dim, Metric::L2).unwrap();
        let cfg = KMeansConfig::for_nlist(8);
        let queries: Vec<usize> = (0..n).step_by(29).collect();
        let mut prev = -1.0f64;
        for nprobe in 1..=4 {
            let ivf = IvfIndex::with_config(ids.clone(), data.clone(), dim, 8, nprobe, Metric::L2, &cfg)
                .unwrap();
            let r = recall_at(&ivf, &oracle, &data, dim, 5, &queries);
            assert!(r + 1e-9 >= prev, "recall dropped at nprobe={nprobe}: {prev} -> {r}");
            prev = r;
        }
    }

    #[test]
    fn tune_ivf_meets_target_at_lowest_cost() {
        let (ids, data, dim) = clustered(13);
        let cfg = TuneConfig {
            target_recall: 0.9,
            k: 5,
            candidates: vec![(3, 1), (3, 2), (3, 3), (8, 1), (8, 2)],
            queries: 60,
            seed: 5,
        };
        let best = tune_ivf(&ids, &data, dim, Metric::L2, &cfg).unwrap();
        assert!(
            best.recall + 1e-9 >= cfg.target_recall,
            "tuned result below target: {best:?}"
        );
        assert!(best.probed_rows <= ids.len() as f64);
        assert!(cfg.candidates.contains(&(best.nlist, best.nprobe)));
    }

    #[test]
    fn training_metadata_round_trips() {
        let (ids, data, dim) = clustered(2);
        let cfg = KMeansConfig {
            nlist: 4,
            max_iters: 10,
            restarts: 2,
            sample: Some(200),
            seed: 99,
            split_oversized: false,
            split_threshold_k: 3.0,
        };
        let ivf = IvfIndex::with_config(ids, data, dim, 4, 2, Metric::L2, &cfg).unwrap();
        let bytes = ivf.to_bytes();
        assert_eq!(&bytes[..8], &IVF_MAGIC_V2[..], "k-means builds write v2");
        let back = IvfIndex::from_bytes(&bytes).unwrap();
        assert_eq!(back.training(), ivf.training());
        assert!(back.training().unwrap().kmeans);
        assert_eq!(back.to_bytes(), bytes);
    }

    #[test]
    fn tune_ivf_curve_has_one_row_per_candidate() {
        let (ids, data, dim) = clustered(13);
        let candidates = vec![(3, 1), (3, 2), (8, 1)];
        let cfg = TuneConfig {
            target_recall: 0.9,
            k: 5,
            candidates: candidates.clone(),
            queries: 40,
            seed: 5,
        };
        let curve = tune_ivf_curve(&ids, &data, dim, Metric::L2, &cfg).unwrap();
        assert_eq!(curve.len(), candidates.len());
        for (row, &(nlist, nprobe)) in curve.iter().zip(candidates.iter()) {
            assert_eq!((row.nlist, row.nprobe), (nlist, nprobe));
            assert!(row.recall >= 0.0 && row.recall <= 1.0);
            assert!(row.probed_rows >= 0.0);
        }
    }

    #[test]
    fn retrain_trigger_flags_skewed_layout() {
        // 100 copies of the origin plus one far outlier, uniform nlist=2 → one
        // cell absorbs almost everything.
        let mut data = Vec::new();
        for _ in 0..100 {
            data.extend_from_slice(&[0.0f32, 0.0]);
        }
        data.extend_from_slice(&[1000.0f32, 1000.0]);
        let ids: Vec<u64> = (0..101).collect();
        let ivf = IvfIndex::with_uniform(ids, data, 2, 2, 1, Metric::L2).unwrap();
        match ivf.retrain_trigger(1.5) {
            RetrainTrigger::PopulationImbalance { ratio } => assert!(ratio > 1.5, "ratio {ratio}"),
            other => panic!("expected imbalance, got {other:?}"),
        }
    }
}
