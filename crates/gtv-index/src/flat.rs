//! Exact brute-force K-NN index (reference implementation for tests).
//!
//! Vectors are stored row-major in a single contiguous `Vec<f32>` (not
//! `Vec<Vec<f32>>`), which turns the linear scan from a pointer-chase per vector
//! into one streaming pass. Distances use an AVX2+FMA kernel when available
//! (scalar otherwise), the scan is parallelized with rayon, and the final ranking
//! is a bounded top-K *per-thread* selection — the full `N × (f32, u64)` score
//! vector is never materialized, so the only `O(N)` allocation is the input
//! read itself (memory0copy.md: no intermediate score array written to DRAM).

use std::cmp::Ordering;

use arrow::array::{BooleanArray, UInt64Array};
use gtv_core::{GtvError, Result, VectorIndex};

/// Exact K-NN via a linear scan over every vector, optionally restricted to
/// the nodes allowed by a [`BooleanArray`] bitmask.
///
/// The bitmask is indexed by the *position* of the vector in the index
/// (0..n); it is the caller's job to build a mask aligned with that layout.
#[derive(Debug, Clone)]
pub struct FlatIndex {
    ids: Vec<u64>,
    /// Row-major: vector `i` occupies `data[i * dim .. (i + 1) * dim]`.
    data: Vec<f32>,
    dim: usize,
}

impl FlatIndex {
    pub fn new(ids: Vec<u64>, vectors: Vec<Vec<f32>>) -> Result<Self> {
        if ids.len() != vectors.len() {
            return Err(GtvError::InvalidArgument(
                "ids and vectors length mismatch".into(),
            ));
        }
        let Some(first) = vectors.first() else {
            return Err(GtvError::InvalidArgument("empty index".into()));
        };
        let dim = first.len();
        if dim == 0 {
            return Err(GtvError::InvalidArgument("zero-dimension vectors".into()));
        }
        for v in &vectors {
            if v.len() != dim {
                return Err(GtvError::InvalidArgument(
                    "inconsistent vector dimensions".into(),
                ));
            }
        }
        let n = ids.len();
        let mut data = Vec::with_capacity(n * dim);
        for v in &vectors {
            data.extend_from_slice(v);
        }
        Ok(Self { ids, data, dim })
    }

    /// Build from an already-contiguous row-major buffer (vector `i` occupies
    /// `data[i * dim .. (i + 1) * dim]`), avoiding the re-copy that [`new`] pays
    /// when handed a `Vec<Vec<f32>>`.
    pub fn from_flat(ids: Vec<u64>, data: Vec<f32>, dim: usize) -> Result<Self> {
        if dim == 0 {
            return Err(GtvError::InvalidArgument("zero-dimension vectors".into()));
        }
        if data.len() != ids.len() * dim {
            return Err(GtvError::InvalidArgument(
                "data length != ids.len() * dim".into(),
            ));
        }
        Ok(Self { ids, data, dim })
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

    /// Zero-copy access to the contiguous row-major vector buffer (vector `i`
    /// occupies `data[i * dim .. (i + 1) * dim]`). Used by the CUDA K-NN path to
    /// upload the corpus without materializing a second host copy.
    pub fn data(&self) -> &[f32] {
        &self.data
    }

    /// Zero-copy access to the vector ids, aligned with [`FlatIndex::data`].
    pub fn ids(&self) -> &[u64] {
        &self.ids
    }
}

// ---------------------------------------------------------------------------
// Squared-L2 kernels
// ---------------------------------------------------------------------------

#[inline]
pub(crate) fn squared_l2_scalar(query: &[f32], row: &[f32]) -> f32 {
    let mut sum = 0.0f32;
    for (x, y) in query.iter().zip(row) {
        let d = x - y;
        sum += d * d;
    }
    sum
}

/// AVX2 + FMA squared-L2 with four independent accumulators (ILP) and 32-float
/// unrolled steps, so the FMA dependency chain no longer stalls the pipeline.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2", enable = "fma")]
#[inline]
pub(crate) unsafe fn squared_l2_avx2(query: &[f32], row: &[f32]) -> f32 {
    use std::arch::x86_64::*;
    let n = query.len();
    let mut acc0 = _mm256_setzero_ps();
    let mut acc1 = _mm256_setzero_ps();
    let mut acc2 = _mm256_setzero_ps();
    let mut acc3 = _mm256_setzero_ps();
    let mut i = 0usize;

    // 32 floats / iteration (4 × 8-lane FMA), covering dim = 512 in 16 steps.
    while i + 32 <= n {
        let q0 = _mm256_loadu_ps(query.as_ptr().add(i));
        let r0 = _mm256_loadu_ps(row.as_ptr().add(i));
        let d0 = _mm256_sub_ps(q0, r0);
        acc0 = _mm256_fmadd_ps(d0, d0, acc0);

        let q1 = _mm256_loadu_ps(query.as_ptr().add(i + 8));
        let r1 = _mm256_loadu_ps(row.as_ptr().add(i + 8));
        let d1 = _mm256_sub_ps(q1, r1);
        acc1 = _mm256_fmadd_ps(d1, d1, acc1);

        let q2 = _mm256_loadu_ps(query.as_ptr().add(i + 16));
        let r2 = _mm256_loadu_ps(row.as_ptr().add(i + 16));
        let d2 = _mm256_sub_ps(q2, r2);
        acc2 = _mm256_fmadd_ps(d2, d2, acc2);

        let q3 = _mm256_loadu_ps(query.as_ptr().add(i + 24));
        let r3 = _mm256_loadu_ps(row.as_ptr().add(i + 24));
        let d3 = _mm256_sub_ps(q3, r3);
        acc3 = _mm256_fmadd_ps(d3, d3, acc3);

        i += 32;
    }
    // 8-float tail into the first accumulator.
    while i + 8 <= n {
        let q = _mm256_loadu_ps(query.as_ptr().add(i));
        let r = _mm256_loadu_ps(row.as_ptr().add(i));
        let d = _mm256_sub_ps(q, r);
        acc0 = _mm256_fmadd_ps(d, d, acc0);
        i += 8;
    }

    // Horizontal reduction of the four accumulators.
    let acc = _mm256_add_ps(_mm256_add_ps(acc0, acc1), _mm256_add_ps(acc2, acc3));
    let mut buf = [0.0f32; 8];
    _mm256_storeu_ps(buf.as_mut_ptr(), acc);
    let mut sum = buf.iter().sum::<f32>();

    // Scalar tail (< 8 elements).
    for j in i..n {
        let d = query[j] - row[j];
        sum += d * d;
    }
    sum
}

/// Dispatch to the fastest available squared-L2 kernel for this host.
#[inline]
pub(crate) fn squared_l2(query: &[f32], row: &[f32], use_simd: bool) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        if use_simd {
            // SAFETY: guarded by the runtime AVX2+FMA detection performed by the caller.
            return unsafe { squared_l2_avx2(query, row) };
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = use_simd;
    squared_l2_scalar(query, row)
}

// ---------------------------------------------------------------------------
// Bounded parallel top-K
// ---------------------------------------------------------------------------

#[inline]
pub(crate) fn cmp_scored(a: &(f32, u64), b: &(f32, u64)) -> Ordering {
    a.0.partial_cmp(&b.0)
        .unwrap_or(Ordering::Equal)
        .then_with(|| a.1.cmp(&b.1))
}

/// Parallel top-K scan. Each rayon thread keeps a bounded candidate buffer and
/// periodically re-selects the best `k`, so the `N × (f32, u64)` score vector is
/// never written to DRAM; the final cross-thread merge reduces at most
/// `threads × k` items. `score(i)` returns `(distance, id)` for row `i`.
fn par_topk(
    n: usize,
    k: usize,
    allowed: Option<&BooleanArray>,
    score: impl Fn(usize) -> (f32, u64) + Sync + Send,
) -> Vec<(f32, u64)> {
    use rayon::prelude::*;
    if k == 0 || n == 0 {
        return Vec::new();
    }
    let cap = (2 * k).max(32);
    let mut topk: Vec<(f32, u64)> = (0..n)
        .into_par_iter()
        .filter(|&i| allowed.map_or(true, |m| m.value(i)))
        .fold(
            || Vec::with_capacity(cap),
            |mut acc, i| {
                acc.push(score(i));
                if acc.len() >= 2 * k {
                    acc.select_nth_unstable_by(k - 1, cmp_scored);
                    acc.truncate(k);
                }
                acc
            },
        )
        .reduce(
            || Vec::with_capacity(cap),
            |mut a, mut b| {
                a.append(&mut b);
                a
            },
        );

    let kk = k.min(topk.len());
    if kk > 0 && kk < topk.len() {
        topk.select_nth_unstable_by(kk - 1, cmp_scored);
    }
    topk.truncate(kk);
    topk.sort_by(cmp_scored);
    topk
}

/// Bounded parallel top-K over an explicit set of corpus positions (used by the
/// inverted-file probe scan, whose candidate set is a union of list runs).
pub(crate) fn par_topk_over(
    positions: &[u32],
    k: usize,
    score: impl Fn(u32) -> (f32, u64) + Sync + Send,
) -> Vec<(f32, u64)> {
    use rayon::prelude::*;
    if k == 0 || positions.is_empty() {
        return Vec::new();
    }
    let cap = (2 * k).max(32);
    let mut topk: Vec<(f32, u64)> = positions
        .par_iter()
        .copied()
        .fold(
            || Vec::with_capacity(cap),
            |mut acc, i| {
                acc.push(score(i));
                if acc.len() >= 2 * k {
                    acc.select_nth_unstable_by(k - 1, cmp_scored);
                    acc.truncate(k);
                }
                acc
            },
        )
        .reduce(
            || Vec::with_capacity(cap),
            |mut a, mut b| {
                a.append(&mut b);
                a
            },
        );
    let kk = k.min(topk.len());
    if kk > 0 && kk < topk.len() {
        topk.select_nth_unstable_by(kk - 1, cmp_scored);
    }
    topk.truncate(kk);
    topk.sort_by(cmp_scored);
    topk
}

impl VectorIndex for FlatIndex {
    fn search_knn(
        &self,
        query: &[f32],
        k: usize,
        filter_mask: Option<&BooleanArray>,
    ) -> Result<UInt64Array> {
        if query.len() != self.dim {
            return Err(GtvError::InvalidArgument(format!(
                "query dim {} != index dim {}",
                query.len(),
                self.dim
            )));
        }
        if let Some(mask) = filter_mask {
            if mask.len() != self.ids.len() {
                return Err(GtvError::InvalidArgument(
                    "filter mask length mismatch".into(),
                ));
            }
        }
        if k == 0 {
            return Ok(UInt64Array::from(Vec::<u64>::new()));
        }

        let n = self.ids.len();
        let dim = self.dim;
        let data = &self.data;
        let ids = &self.ids;

        #[cfg(target_arch = "x86_64")]
        let use_simd =
            std::arch::is_x86_feature_detected!("avx2") && std::arch::is_x86_feature_detected!("fma");
        #[cfg(not(target_arch = "x86_64"))]
        let use_simd = false;

        let scored = par_topk(n, k, filter_mask, |i| {
            let row = &data[i * dim..(i + 1) * dim];
            (squared_l2(query, row, use_simd), ids[i])
        });

        Ok(UInt64Array::from(
            scored.into_iter().map(|(_, id)| id).collect::<Vec<u64>>(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::BooleanArray;

    fn idx() -> FlatIndex {
        // 3 nodes, 2 dims: (0,0), (1,1), (5,5)
        FlatIndex::new(
            vec![0, 1, 2],
            vec![
                vec![0.0, 0.0],
                vec![1.0, 1.0],
                vec![5.0, 5.0],
            ],
        )
        .unwrap()
    }

    #[test]
    fn knn_orders_by_distance() {
        let index = idx();
        let got = index.search_knn(&[0.1, 0.1], 3, None).unwrap();
        assert_eq!(got.values().as_ref(), &[0, 1, 2]);
    }

    #[test]
    fn knn_respects_bitmask() {
        let index = idx();
        let mask = BooleanArray::from(vec![false, true, true]);
        let got = index.search_knn(&[5.0, 5.0], 3, Some(&mask)).unwrap();
        assert_eq!(got.values().as_ref(), &[2, 1]);
    }

    #[test]
    fn knn_truncates_to_k() {
        let index = idx();
        let got = index.search_knn(&[0.0, 0.0], 2, None).unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got.values().as_ref(), &[0, 1]);
    }

    #[test]
    fn rejects_dimension_mismatch() {
        let index = idx();
        assert!(index.search_knn(&[1.0], 2, None).is_err());
    }
}
