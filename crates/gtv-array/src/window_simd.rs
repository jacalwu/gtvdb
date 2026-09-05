//! Fused single-pass rolling-window kernel with non-temporal (NT) stores.
//!
//! `fused_ma_ms_delta` computes `mavg`, `msum` and `deltas` in a single scan of
//! `values`, reading the input once instead of three times, and writes the three
//! outputs with non-temporal stores to skip the read-for-ownership (RFO) fetch
//! on the write side.
//!
//! Design notes
//! ------------
//! * **Fusion** — one pass shares the running `sum` between `msum` and `mavg`,
//!   so the input is read once and DRAM read traffic drops from `3x` to `1x`
//!   versus three separate `window::{mavg, msum, deltas}` calls.
//! * **Non-temporal stores** — a streaming write to a fresh cache line normally
//!   triggers a 64-byte RFO read before the line can be modified.
//!   `_mm256_stream_pd` / `_mm_stream_pd` write-combine instead, so the output
//!   side costs one write rather than a read + a write. NT stores only make
//!   sense for write-once output (never re-read while hot), which is exactly
//!   the fused kernel's contract.
//! * **Write-once buffers** — outputs are allocated with `set_len` and written
//!   exactly once, avoiding the `vec![0.0; len]` memset (a second full write of
//!   every output byte).
//! * **Bit-exact semantics** — every path runs the *same* scalar running-sum
//!   recurrence and IEEE `f64` division (see [`step`]), so scalar / SSE2 / AVX2
//!   produce bit-identical results to each other and to
//!   `window::{mavg, msum, deltas}`. The store instruction (regular vs NT)
//!   never changes a value.
//! * **Runtime dispatch + runtime alignment check** — the ISA is selected with
//!   `is_x86_feature_detected!` (never `cfg!(target_feature)`, which is a
//!   compile-time-only test). NT intrinsics need an aligned destination
//!   (`_mm256_stream_pd`: 32 B, `_mm_stream_pd`: 16 B), so each kernel verifies
//!   the allocator's alignment and steps down to a narrower schedule — 32 B →
//!   AVX2 NT4 → 16 B → SSE2 NT2 → scalar — rather than faulting. The default
//!   system allocator returns 16 B-aligned heap blocks and page-aligned large
//!   blocks, so NT stores are active in practice.

/// Compute `(mavg, msum, deltas)` in a single fused pass, with kdb semantics.
///
/// Equivalent to calling `window::mavg`, `window::msum` and `window::deltas`
/// separately, but with one input scan and (on x86_64) non-temporal stores.
pub fn fused_ma_ms_delta(values: &[f64], n: usize) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    assert!(n >= 1, "window size must be >= 1");

    #[cfg(target_arch = "x86_64")]
    {
        // Runtime dispatch: the build machine's features are irrelevant here.
        if std::arch::is_x86_feature_detected!("avx2") {
            // SAFETY: guarded by is_x86_feature_detected! above.
            return unsafe { fused_avx2(values, n) };
        }
        if std::arch::is_x86_feature_detected!("sse2") {
            // SAFETY: guarded by is_x86_feature_detected! above.
            return unsafe { fused_sse2(values, n) };
        }
    }

    fused_scalar(values, n)
}

/// Allocate a `len`-element `Vec<f64>` without zero-filling.
///
/// # Safety
/// The caller must write every element before the returned `Vec` is exposed or
/// read. All call sites below satisfy this by writing every index in order.
#[inline]
#[allow(clippy::uninit_vec)] // deliberate: outputs are written exactly once
unsafe fn uninit_vec(len: usize) -> Vec<f64> {
    let mut v = Vec::with_capacity(len);
    // SAFETY: capacity >= len; every element is written by the caller.
    v.set_len(len);
    v
}

/// Compute `(msum, mavg, deltas)` at index `i`, advancing the running `sum`
/// and the previous value `prev`.
///
/// This is the *single* source of the numeric semantics: every schedule below
/// (scalar, SSE2, AVX2) calls it in order, so all results are bit-identical.
///
/// # Safety
/// `i` must be `< values.len()`, and `i - n` must be in range whenever `i >= n`
/// (guaranteed by `n >= 1` and `i < values.len()`).
#[inline]
unsafe fn step(values: &[f64], n: usize, i: usize, sum: &mut f64, prev: &mut f64) -> (f64, f64, f64) {
    let v = *values.get_unchecked(i);
    *sum += v;
    if i >= n {
        *sum -= *values.get_unchecked(i - n);
    }
    let ms = *sum;
    let ma = ms / (i + 1).min(n) as f64;
    let dl = if i == 0 { v } else { v - *prev };
    *prev = v;
    (ms, ma, dl)
}

/// Scalar-store kernel writing every index of `[0, len)` to the three outputs.
///
/// # Safety
/// `ma`/`ms`/`dl` must be valid for `len` `f64` writes; `values` valid for
/// `len` reads.
unsafe fn scalar_kernel(
    values: &[f64],
    n: usize,
    ma: *mut f64,
    ms: *mut f64,
    dl: *mut f64,
    len: usize,
) {
    let mut sum = 0.0f64;
    let mut prev = 0.0f64;
    for i in 0..len {
        let (m, a, d) = step(values, n, i, &mut sum, &mut prev);
        *ms.add(i) = m;
        *ma.add(i) = a;
        *dl.add(i) = d;
    }
}

/// Portable scalar fallback: one fused pass, regular stores.
fn fused_scalar(values: &[f64], n: usize) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let len = values.len();
    // SAFETY: every element of each vector is written by scalar_kernel.
    unsafe {
        let mut ma = uninit_vec(len);
        let mut ms = uninit_vec(len);
        let mut dl = uninit_vec(len);
        scalar_kernel(values, n, ma.as_mut_ptr(), ms.as_mut_ptr(), dl.as_mut_ptr(), len);
        (ma, ms, dl)
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn sse2_kernel(
    values: &[f64],
    n: usize,
    ma: *mut f64,
    ms: *mut f64,
    dl: *mut f64,
    len: usize,
) {
    use std::arch::x86_64::{_mm_set_pd, _mm_sfence, _mm_stream_pd};

    // `_mm_stream_pd` needs a 16-byte-aligned destination. If any output buffer
    // is misaligned, fall back to the scalar schedule (same results).
    let aligned16 = (ma as usize) & 15 == 0 && (ms as usize) & 15 == 0 && (dl as usize) & 15 == 0;
    if !aligned16 {
        scalar_kernel(values, n, ma, ms, dl, len);
        return;
    }

    let mut sum = 0.0f64;
    let mut prev = 0.0f64;
    let mut i = 0usize;

    // NT2 bulk: two elements per iteration, one 16-byte NT store per output.
    while i + 1 < len {
        let (m0, a0, d0) = step(values, n, i, &mut sum, &mut prev);
        let (m1, a1, d1) = step(values, n, i + 1, &mut sum, &mut prev);
        _mm_stream_pd(ms.add(i), _mm_set_pd(m1, m0));
        _mm_stream_pd(ma.add(i), _mm_set_pd(a1, a0));
        _mm_stream_pd(dl.add(i), _mm_set_pd(d1, d0));
        i += 2;
    }
    // Order the NT stores before the regular tail stores below.
    _mm_sfence();

    // Odd-length tail.
    while i < len {
        let (m, a, d) = step(values, n, i, &mut sum, &mut prev);
        *ms.add(i) = m;
        *ma.add(i) = a;
        *dl.add(i) = d;
        i += 1;
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2_kernel(
    values: &[f64],
    n: usize,
    ma: *mut f64,
    ms: *mut f64,
    dl: *mut f64,
    len: usize,
) {
    use std::arch::x86_64::{_mm256_set_pd, _mm256_stream_pd, _mm_sfence};

    // `_mm256_stream_pd` needs a 32-byte-aligned destination.
    let aligned32 = (ma as usize) & 31 == 0 && (ms as usize) & 31 == 0 && (dl as usize) & 31 == 0;
    if !aligned32 {
        // AVX2 is a superset of SSE2, so this is a safe call and keeps the
        // NT2/scalar schedules in one place.
        sse2_kernel(values, n, ma, ms, dl, len);
        return;
    }

    let mut sum = 0.0f64;
    let mut prev = 0.0f64;
    let mut i = 0usize;

    // NT4 bulk: four elements per iteration, one 32-byte NT store per output.
    while i + 3 < len {
        let (m0, a0, d0) = step(values, n, i, &mut sum, &mut prev);
        let (m1, a1, d1) = step(values, n, i + 1, &mut sum, &mut prev);
        let (m2, a2, d2) = step(values, n, i + 2, &mut sum, &mut prev);
        let (m3, a3, d3) = step(values, n, i + 3, &mut sum, &mut prev);
        _mm256_stream_pd(ms.add(i), _mm256_set_pd(m3, m2, m1, m0));
        _mm256_stream_pd(ma.add(i), _mm256_set_pd(a3, a2, a1, a0));
        _mm256_stream_pd(dl.add(i), _mm256_set_pd(d3, d2, d1, d0));
        i += 4;
    }
    _mm_sfence();

    // Tail (up to three elements).
    while i < len {
        let (m, a, d) = step(values, n, i, &mut sum, &mut prev);
        *ms.add(i) = m;
        *ma.add(i) = a;
        *dl.add(i) = d;
        i += 1;
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn fused_sse2(values: &[f64], n: usize) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let len = values.len();
    // SAFETY: every element of each vector is written by sse2_kernel.
    let mut ma = uninit_vec(len);
    let mut ms = uninit_vec(len);
    let mut dl = uninit_vec(len);
    sse2_kernel(values, n, ma.as_mut_ptr(), ms.as_mut_ptr(), dl.as_mut_ptr(), len);
    (ma, ms, dl)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn fused_avx2(values: &[f64], n: usize) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let len = values.len();
    // SAFETY: every element of each vector is written by avx2_kernel.
    let mut ma = uninit_vec(len);
    let mut ms = uninit_vec(len);
    let mut dl = uninit_vec(len);
    avx2_kernel(values, n, ma.as_mut_ptr(), ms.as_mut_ptr(), dl.as_mut_ptr(), len);
    (ma, ms, dl)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference(values: &[f64], n: usize) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
        (
            crate::window::mavg(values, n),
            crate::window::msum(values, n),
            crate::window::deltas(values),
        )
    }

    #[test]
    fn fused_matches_individual_ops() {
        let values: Vec<f64> = (0..1000)
            .map(|i| (i as f64 * 0.1).sin() + i as f64)
            .collect();
        for &n in &[1usize, 2, 3, 7, 64, 1000, 2000] {
            let (a, b, c) = fused_ma_ms_delta(&values, n);
            let (ra, rb, rc) = reference(&values, n);
            assert_eq!(a, ra, "mavg mismatch n={n}");
            assert_eq!(b, rb, "msum mismatch n={n}");
            assert_eq!(c, rc, "deltas mismatch n={n}");
        }
    }

    #[test]
    fn fused_matches_kdb_semantics() {
        let v = [1.0, 2.0, 3.0, 4.0, 5.0];
        let (a, b, c) = fused_ma_ms_delta(&v, 3);
        assert_eq!(a, vec![1.0, 1.5, 2.0, 3.0, 4.0]);
        assert_eq!(b, vec![1.0, 3.0, 6.0, 9.0, 12.0]);
        assert_eq!(c, vec![1.0, 1.0, 1.0, 1.0, 1.0]);
    }

    #[test]
    fn fused_empty_and_singleton() {
        assert_eq!(fused_ma_ms_delta(&[], 3), (vec![], vec![], vec![]));
        let (a, b, c) = fused_ma_ms_delta(&[2.5], 1);
        assert_eq!(a, vec![2.5]);
        assert_eq!(b, vec![2.5]);
        assert_eq!(c, vec![2.5]);
    }

    #[test]
    fn fused_window_larger_than_input() {
        let v = [1.0, 2.0, 3.0];
        let (a, b, _c) = fused_ma_ms_delta(&v, 10);
        assert_eq!(a, vec![1.0, 1.5, 2.0]);
        assert_eq!(b, vec![1.0, 3.0, 6.0]);
    }

    #[test]
    fn fused_randomized_matches_individual_ops() {
        // Odd sizes exercise the tail paths of every schedule.
        for len in [0usize, 1, 2, 3, 4, 5, 7, 15, 16, 17, 63, 64, 65, 129, 1000] {
            let values: Vec<f64> = (0..len)
                .map(|i| ((i as f64) * 1.7).sin() * 1e6 + (i as f64 * 0.3).cos())
                .collect();
            for &n in &[1usize, 2, 3, 5, 16, 64, 1000] {
                let (a, b, c) = fused_ma_ms_delta(&values, n);
                let (ra, rb, rc) = reference(&values, n);
                assert_eq!(a, ra, "mavg mismatch len={len} n={n}");
                assert_eq!(b, rb, "msum mismatch len={len} n={n}");
                assert_eq!(c, rc, "deltas mismatch len={len} n={n}");
            }
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn all_schedules_are_bit_exact() {
        // Large enough to force page-aligned (32 B) allocations, plus small
        // sizes that land on the heap allocator (16 B alignment).
        for len in [7usize, 1000, 1_000_000, 4_000_000] {
            let values: Vec<f64> = (0..len)
                .map(|i| (i as f64 * 0.7).cos() * 100.0 + (i as f64 * 0.13).sin())
                .collect();
            for &n in &[1usize, 2, 3, 64, 1000, 4_000_000] {
                let (ra, rb, rc) = fused_scalar(&values, n);
                let mut scalar_path_ran = false;

                if std::arch::is_x86_feature_detected!("sse2") {
                    // SAFETY: guarded by is_x86_feature_detected!.
                    let (a, b, c) = unsafe { fused_sse2(&values, n) };
                    assert_eq!((a, b, c), (ra.clone(), rb.clone(), rc.clone()), "sse2 len={len} n={n}");
                    scalar_path_ran = true;
                }
                if std::arch::is_x86_feature_detected!("avx2") {
                    // SAFETY: guarded by is_x86_feature_detected!.
                    let (a, b, c) = unsafe { fused_avx2(&values, n) };
                    assert_eq!(
                        (a, b, c),
                        (ra.clone(), rb.clone(), rc.clone()),
                        "avx2 len={len} n={n}"
                    );
                    scalar_path_ran = true;
                }
                // fused_scalar must match the public entry point too.
                let (a, b, c) = fused_ma_ms_delta(&values, n);
                assert_eq!((a, b, c), (ra, rb, rc), "dispatch len={len} n={n}");

                if !scalar_path_ran {
                    panic!("no x86 SIMD path was exercised on x86_64");
                }
            }
        }
    }
}
