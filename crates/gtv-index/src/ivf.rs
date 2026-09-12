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

use crate::flat::{metric_distance, par_topk_over};

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

    /// Build an index with an explicit metric.
    ///
    /// `nlist` is clamped to `n`; `nprobe` is clamped to `nlist`. Centroids are
    /// sampled evenly across the corpus (deterministic, reproducible), then every
    /// vector is assigned to its nearest centroid and the corpus is reordered by
    /// cell for sequential probe reads.
    pub fn with_metric(
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
}
