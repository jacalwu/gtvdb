//! HFT feature + performance benchmark (TC1–TC5 from `HFT_TESTCASE.md`).
//!
//! Generates deterministic synthetic Tick / Account-Transfer data matching the
//! two canonical Arrow schemas, round-trips a sample through CSV (the Data
//! Loader), then times the five HFT test cases at 100k / 1M / 5M rows where the
//! algorithm permits. Results are written to `testcase/hft/RESULTS.md`.
//!
//! Run with:
//!   cargo run --release -p gtv-cli --example hft_bench
//!
//! Design notes vs. the checklist:
//!   * The benchmark crate is `gtv-cli` (not `gtvdb-core`): the TC logic spans
//!     gtv-core / gtv-array / gtv-index / gtv-pattern, whose union lives in the
//!     CLI crate. There is no crate named `gtvdb-core`.
//!   * Timing uses the repo's existing `Instant` micro-benchmark pattern
//!     (`bench_asof`, `bench_temporal`) rather than Criterion — Criterion is not
//!     a workspace dependency and adds nothing at these one-shot scales.
//!   * Memory is the *logical* input footprint (rows × bytes/row), not peak RSS.

use std::collections::HashMap;
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
    ArrayRef, Float64Array, StringArray, TimestampNanosecondArray, UInt16Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;

use gtv_array::window::msum;
use gtv_core::temporal::{
    build_zone_maps, point_in_time_range, temporal_mask_full, temporal_mask_pruned, ZoneMap,
};
use gtv_core::{TemporalCSR, VectorIndex};
use gtv_index::{FlatIndex, IvfIndex};
use gtv_pattern::{find, Pattern};

const OUT_DIR: &str = "testcase/hft";
const DATA_DIR: &str = "testcase/hft/data";

// ---------------------------------------------------------------------------
// Deterministic PRNG (SplitMix64) — reproducible data without external deps.
// ---------------------------------------------------------------------------

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
    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

// ---------------------------------------------------------------------------
// Data generation
// ---------------------------------------------------------------------------

/// Ascending timestamps + a correlated random-walk price series.
fn gen_series(n: usize, seed: u64, dt: i64) -> (Vec<i64>, Vec<f64>) {
    let mut rng = SplitMix64::new(seed);
    let mut ts = Vec::with_capacity(n);
    let mut px = Vec::with_capacity(n);
    let mut price = 100.0f64;
    for i in 0..n {
        // dt spacing with sub-dt jitter keeps the series strictly ascending.
        ts.push(i as i64 * dt + (rng.next_u64() % (dt as u64 / 2)) as i64);
        price += (rng.next_f64() - 0.5) * 0.2;
        px.push(price);
    }
    (ts, px)
}

/// Bid/ask price + size columns for the OFI micro-structure feature.
///
/// Sizes are returned as `f64` (exact for the 1..=10_000 range) so the OFI path
/// stays purely float and auto-vectorizes; the canonical `UInt64` size columns
/// live in the separate `ticks` CSV schema, unaffected here.
fn gen_order_flow(n: usize, seed: u64) -> (Vec<f64>, Vec<f64>, Vec<u16>, Vec<u16>) {
    let mut rng = SplitMix64::new(seed);
    let mut bid = Vec::with_capacity(n);
    let mut ask = Vec::with_capacity(n);
    let mut bid_sz = Vec::with_capacity(n);
    let mut ask_sz = Vec::with_capacity(n);
    let mut mid = 100.0f64;
    for _ in 0..n {
        mid += (rng.next_f64() - 0.5) * 0.2;
        let spread = 0.01 + rng.next_f64() * 0.02;
        bid.push(mid - spread / 2.0);
        ask.push(mid + spread / 2.0);
        // Sizes are exact integers in [1, 10000]; `u16` keeps the two size columns
        // at 2 B/row instead of 8 B/row, cutting the streaming footprint of TC2 by
        // 16 MB at 1M rows (the pass is DRAM-bandwidth-bound, not instruction-bound).
        bid_sz.push((1 + rng.next_u64() % 10_000) as u16);
        ask_sz.push((1 + rng.next_u64() % 10_000) as u16);
    }
    (bid, ask, bid_sz, ask_sz)
}

/// Random 512-dim embeddings (unit-agnostic; distances are squared L2), generated
/// directly into a contiguous row-major buffer (vector `i` occupies
/// `data[i * dim .. (i + 1) * dim]`) so the index build is a single move, not a
/// re-copy from `Vec<Vec<f32>>`.
fn gen_embeddings(n: usize, dim: usize, seed: u64) -> (Vec<u64>, Vec<f32>) {
    let mut rng = SplitMix64::new(seed);
    let ids: Vec<u64> = (0..n as u64).collect();
    let mut data = Vec::with_capacity(n * dim);
    for _ in 0..n * dim {
        data.push((rng.next_u64() >> 40) as f32 / (1u64 << 24) as f32);
    }
    (ids, data)
}

// ---------------------------------------------------------------------------
// Test Case 1 — cross-asset as-of temporal join (lead-lag)
// ---------------------------------------------------------------------------

/// Single-pass multi-column as-of join (tuning2.md §1): one monotonic sweep
/// projects both price and spread, writing tight `Vec<f64>` (no `Option` / NaN
/// for misses) instead of two `Vec<Option<f64>>` passes.
///
/// `left_ts` must be ascending (the common kdb `aj` case). Each 64k-row chunk
/// binary-searches its right-table start (`partition_point`, O(log M)) then does
/// a lock-free two-pointer sweep; chunks run in parallel via rayon.
#[allow(dead_code)] // v1 baseline (tuning2.md §1), superseded by asof_join_multi_l2_bucket
fn asof_join_multi_fast(
    left_ts: &[i64],
    right_ts: &[i64],
    right_price: &[f64],
    right_spread: &[f64],
    tolerance_ns: i64,
) -> (Vec<f64>, Vec<f64>) {
    use rayon::prelude::*;
    let len = left_ts.len();
    let mut out_price = vec![f64::NAN; len];
    let mut out_spread = vec![f64::NAN; len];
    const CHUNK: usize = 65_536;
    out_price
        .par_chunks_mut(CHUNK)
        .zip(out_spread.par_chunks_mut(CHUNK))
        .enumerate()
        .for_each(|(ci, (p, s))| {
            let start = ci * CHUNK;
            // Number of right rows `<= left_ts[start]`; the last match is `j - 1`.
            let mut j = right_ts.partition_point(|&ts| ts <= left_ts[start]);
            for i in 0..p.len() {
                let l_ts = left_ts[start + i];
                while j < right_ts.len() && right_ts[j] <= l_ts {
                    j += 1;
                }
                if j > 0 && l_ts - right_ts[j - 1] <= tolerance_ns {
                    p[i] = right_price[j - 1];
                    s[i] = right_spread[j - 1];
                }
            }
        });
    (out_price, out_spread)
}

/// Consume a `Vec<MaybeUninit<f64>>` whose elements were all written.
///
/// # Safety
/// Caller guarantees every element has been initialized.
unsafe fn assume_init_f64(v: Vec<MaybeUninit<f64>>) -> Vec<f64> {
    let mut v = std::mem::ManuallyDrop::new(v);
    Vec::from_raw_parts(v.as_mut_ptr().cast::<f64>(), v.len(), v.capacity())
}

/// TC1 v2 (follow-up to tuning2.md §1): break the memory wall with four
/// orthogonal tweaks —
///   1. L2-sized chunks (8k rows ≈ 192 KB) instead of 64k, so a thread's working
///      set stays in its private L2 rather than thrashing L3/DRAM.
///   2. Zero-init output (`MaybeUninit`, no `f64::NAN` memset) so the parallel
///      pass writes full cache lines without a prior read-for-ownership.
///   3. O(1) time-bucket radix index replacing the per-chunk `partition_point`
///      (kills the O(log M) random cache misses).
///   4. Bounded thread count (see `init_thread_pool`) to match memory channels.
///   5. Branchless hot loop: slice iteration (no `start + i` re-add / bounds
///      checks), `get_unchecked` on the sweep, no redundant `r_idx < len` branch,
///      a bitmask select (no data-dependent branch) and `_mm_prefetch` ahead.
///
/// `bucket_ms` is the coarse bucket width in ns (e.g. 1 ms). `left_ts` must be
/// ascending. Correctness is asserted against [`asof_join_multi_ref`] in `main`.
fn asof_join_multi_l2_bucket(
    left_ts: &[i64],
    right_ts: &[i64],
    right_price: &[f64],
    right_spread: &[f64],
    tolerance_ns: i64,
    bucket_ms: i64,
) -> (Vec<f64>, Vec<f64>) {
    use rayon::prelude::*;
    let len = left_ts.len();
    if len == 0 || right_ts.is_empty() {
        return (vec![f64::NAN; len], vec![f64::NAN; len]);
    }

    // O(M) time-bucket index: bucket_offsets[b] = first right row with
    // timestamp >= (min_r_ts + b * bucket_ms). Sparse buckets forward-fill.
    let min_r_ts = right_ts[0];
    let max_r_ts = *right_ts.last().unwrap();
    let num_buckets = ((max_r_ts - min_r_ts) / bucket_ms + 1) as usize;
    let mut bucket_offsets = vec![0usize; num_buckets + 1];
    {
        let mut b_curr = 0usize;
        for (i, &ts) in right_ts.iter().enumerate() {
            let b = ((ts - min_r_ts) / bucket_ms) as usize;
            while b_curr <= b {
                bucket_offsets[b_curr] = i;
                b_curr += 1;
            }
        }
        for b in b_curr..=num_buckets {
            bucket_offsets[b] = right_ts.len();
        }
    }

    // Zero-init output: uninitialized, written exactly once per element below.
    let mut out_p: Vec<MaybeUninit<f64>> = Vec::with_capacity(len);
    let mut out_s: Vec<MaybeUninit<f64>> = Vec::with_capacity(len);
    // SAFETY: `MaybeUninit` may legitimately be uninitialized; every element is
    // written before `assume_init_f64` consumes the vec.
    unsafe {
        out_p.set_len(len);
        out_s.set_len(len);
    }

    const CHUNK: usize = 8192;
    out_p
        .par_chunks_mut(CHUNK)
        .zip(out_s.par_chunks_mut(CHUNK))
        .enumerate()
        .for_each(|(ci, (p, s))| {
            let start = ci * CHUNK;
            let n = p.len();

            // O(1) bucket lookup for this chunk's starting right-table row.
            let l_start_ts = left_ts[start];
            let b_idx = if l_start_ts < min_r_ts {
                0
            } else {
                (((l_start_ts - min_r_ts) / bucket_ms) as usize).min(num_buckets)
            };
            let mut r_idx = bucket_offsets[b_idx];
            if r_idx > 0 {
                r_idx -= 1;
            }

            let l_slice = &left_ts[start..start + n];
            let right_len = right_ts.len();

            for (i, &l_ts) in l_slice.iter().enumerate() {
                // SAFETY: the guard keeps r_idx + 1 < right_len, so the unchecked
                // read of right_ts[r_idx + 1] is in-bounds.
                while r_idx + 1 < right_len
                    && unsafe { *right_ts.get_unchecked(r_idx + 1) } <= l_ts
                {
                    r_idx += 1;
                }

                // SAFETY: r_idx < right_len always holds here (it starts <= len-1
                // and the while only advances while r_idx + 1 < right_len), and the
                // three right-table arrays share the same length by construction.
                let diff = l_ts - unsafe { *right_ts.get_unchecked(r_idx) };
                let is_valid = (diff >= 0) & (diff <= tolerance_ns);

                // Prefetch the right-table value ~one cache line ahead so the next
                // few iterations hit L1 instead of stalling on DRAM.
                #[cfg(target_arch = "x86_64")]
                if r_idx + 8 < right_len {
                    unsafe {
                        std::arch::x86_64::_mm_prefetch(
                            right_price.as_ptr().add(r_idx + 8) as *const _,
                            std::arch::x86_64::_MM_HINT_T0,
                        );
                        std::arch::x86_64::_mm_prefetch(
                            right_spread.as_ptr().add(r_idx + 8) as *const _,
                            std::arch::x86_64::_MM_HINT_T0,
                        );
                    }
                }

                // Branchless select: valid keeps the raw bits, invalid yields NaN
                // (0x7ff8_0000_0000_0000). No data-dependent branch / pipeline flush.
                let mask = (is_valid as u64).wrapping_neg(); // 0 or u64::MAX
                let nan = f64::NAN.to_bits();
                let raw_p = unsafe { *right_price.get_unchecked(r_idx) }.to_bits();
                let raw_s = unsafe { *right_spread.get_unchecked(r_idx) }.to_bits();
                p[i].write(f64::from_bits((raw_p & mask) | (nan & !mask)));
                s[i].write(f64::from_bits((raw_s & mask) | (nan & !mask)));
            }
        });

    // SAFETY: every element of both buffers was written exactly once above.
    unsafe { (assume_init_f64(out_p), assume_init_f64(out_s)) }
}

/// Non-temporal store of a single `f64` (bypasses cache, skips the
/// read-for-ownership a normal cold-cache-line store would trigger).
#[cfg(target_arch = "x86_64")]
#[inline]
#[target_feature(enable = "sse2")]
unsafe fn nt_store_f64(p: *mut f64, v: f64) {
    std::arch::x86_64::_mm_stream_si64(p as *mut i64, v.to_bits() as i64);
}

/// TC1 v4 ("ultimate" CPU): payload decoupling + non-temporal stores.
///
/// Phase 1 sweeps only the two timestamp arrays (8 B each) and writes a 4-byte
/// matched index — the price/spread payload is never touched during the search.
/// Phase 2 gathers the payload in a second, fully-sequential pass and writes the
/// output with non-temporal stores, eliminating the read-for-ownership double-write
/// that a normal `Vec<f64>` store incurs on cold cache lines.
///
/// `bucket_ms` is the coarse bucket width (e.g. 1 ms). `left_ts` must be ascending.
/// Correctness asserted against [`asof_join_multi_ref`] in `main`.
fn asof_join_cpu_ultimate(
    left_ts: &[i64],
    right_ts: &[i64],
    right_price: &[f64],
    right_spread: &[f64],
    tolerance_ns: i64,
    bucket_ms: i64,
) -> (Vec<f64>, Vec<f64>) {
    use rayon::prelude::*;
    let len = left_ts.len();
    if len == 0 || right_ts.is_empty() {
        return (vec![f64::NAN; len], vec![f64::NAN; len]);
    }

    // O(M) time-bucket index (shared with v3): bucket_offsets[b] = first right
    // row with timestamp >= min_r_ts + b * bucket_ms.
    let min_r_ts = right_ts[0];
    let max_r_ts = *right_ts.last().unwrap();
    let num_buckets = ((max_r_ts - min_r_ts) / bucket_ms + 1) as usize;
    let mut bucket_offsets = vec![0usize; num_buckets + 1];
    {
        let mut b_curr = 0usize;
        for (i, &ts) in right_ts.iter().enumerate() {
            let b = ((ts - min_r_ts) / bucket_ms) as usize;
            while b_curr <= b {
                bucket_offsets[b_curr] = i;
                b_curr += 1;
            }
        }
        for b in b_curr..=num_buckets {
            bucket_offsets[b] = right_ts.len();
        }
    }

    // Phase 1 — index-only sweep (4-byte match index, payload untouched).
    let mut matched_idx = vec![-1i32; len];
    const CHUNK: usize = 8192;
    let right_len = right_ts.len();
    matched_idx
        .par_chunks_mut(CHUNK)
        .enumerate()
        .for_each(|(ci, out)| {
            let start = ci * CHUNK;
            let n = out.len();
            let l_start_ts = left_ts[start];
            let b_idx = if l_start_ts < min_r_ts {
                0
            } else {
                (((l_start_ts - min_r_ts) / bucket_ms) as usize).min(num_buckets)
            };
            let mut r_idx = bucket_offsets[b_idx];
            if r_idx > 0 {
                r_idx -= 1;
            }
            let l_slice = &left_ts[start..start + n];
            for (i, &l_ts) in l_slice.iter().enumerate() {
                // SAFETY: the guard keeps r_idx + 1 < right_len, so the unchecked
                // read of right_ts[r_idx + 1] is in-bounds.
                while r_idx + 1 < right_len
                    && unsafe { *right_ts.get_unchecked(r_idx + 1) } <= l_ts
                {
                    r_idx += 1;
                }
                let diff = l_ts - unsafe { *right_ts.get_unchecked(r_idx) };
                if diff >= 0 && diff <= tolerance_ns {
                    out[i] = r_idx as i32;
                }
            }
        });

    // Phase 2 — sequential payload gather + non-temporal store.
    let mut out_price: Vec<MaybeUninit<f64>> = Vec::with_capacity(len);
    let mut out_spread: Vec<MaybeUninit<f64>> = Vec::with_capacity(len);
    // SAFETY: every element is written exactly once in phase 2 below.
    unsafe {
        out_price.set_len(len);
        out_spread.set_len(len);
    }
    out_price
        .par_chunks_mut(CHUNK)
        .zip(out_spread.par_chunks_mut(CHUNK))
        .zip(matched_idx.par_chunks(CHUNK))
        .for_each(|((op, os), mi)| {
            for i in 0..mi.len() {
                let r = mi[i];
                let (pv, sv) = if r >= 0 {
                    let j = r as usize;
                    (right_price[j], right_spread[j])
                } else {
                    (f64::NAN, f64::NAN)
                };
                #[cfg(target_arch = "x86_64")]
                unsafe {
                    nt_store_f64(op.as_mut_ptr().add(i).cast::<f64>(), pv);
                    nt_store_f64(os.as_mut_ptr().add(i).cast::<f64>(), sv);
                }
                #[cfg(not(target_arch = "x86_64"))]
                {
                    op[i].write(pv);
                    os[i].write(sv);
                }
            }
        });

    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_sfence();
    }

    // SAFETY: every element of both buffers was written exactly once above.
    unsafe { (assume_init_f64(out_price), assume_init_f64(out_spread)) }
}

/// TC1 fused with a downstream feature (zero-copy / operator fusion).
///
/// Instead of materializing the 16 MB `(price, spread)` join result, this consumes
/// the aligned payload inside the sweep and writes only the fused feature —
/// `rel_spread = spread / price` — a single f64 per row (8 MB). The intermediate
/// never leaves the per-thread working set, eliminating the 16 MB output write
/// (plus its read-for-ownership) and the downstream re-read of that buffer.
///
/// A pure reduction downstream (e.g. an aggregate) would shrink the output to ~0
/// and leave only the ~32 MB input read — ~10 ms at this host's bandwidth.
fn asof_join_fused(
    left_ts: &[i64],
    right_ts: &[i64],
    right_price: &[f64],
    right_spread: &[f64],
    tolerance_ns: i64,
    bucket_ms: i64,
) -> Vec<f64> {
    use rayon::prelude::*;
    let len = left_ts.len();
    if len == 0 || right_ts.is_empty() {
        return vec![f64::NAN; len];
    }

    let min_r_ts = right_ts[0];
    let max_r_ts = *right_ts.last().unwrap();
    let num_buckets = ((max_r_ts - min_r_ts) / bucket_ms + 1) as usize;
    let mut bucket_offsets = vec![0usize; num_buckets + 1];
    {
        let mut b_curr = 0usize;
        for (i, &ts) in right_ts.iter().enumerate() {
            let b = ((ts - min_r_ts) / bucket_ms) as usize;
            while b_curr <= b {
                bucket_offsets[b_curr] = i;
                b_curr += 1;
            }
        }
        for b in b_curr..=num_buckets {
            bucket_offsets[b] = right_ts.len();
        }
    }

    let mut out: Vec<MaybeUninit<f64>> = Vec::with_capacity(len);
    // SAFETY: every element is written exactly once below.
    unsafe {
        out.set_len(len);
    }

    const CHUNK: usize = 8192;
    let right_len = right_ts.len();
    out.par_chunks_mut(CHUNK)
        .enumerate()
        .for_each(|(ci, o)| {
            let start = ci * CHUNK;
            let n = o.len();
            let l_start_ts = left_ts[start];
            let b_idx = if l_start_ts < min_r_ts {
                0
            } else {
                (((l_start_ts - min_r_ts) / bucket_ms) as usize).min(num_buckets)
            };
            let mut r_idx = bucket_offsets[b_idx];
            if r_idx > 0 {
                r_idx -= 1;
            }
            let l_slice = &left_ts[start..start + n];
            for (i, &l_ts) in l_slice.iter().enumerate() {
                while r_idx + 1 < right_len
                    && unsafe { *right_ts.get_unchecked(r_idx + 1) } <= l_ts
                {
                    r_idx += 1;
                }
                let diff = l_ts - unsafe { *right_ts.get_unchecked(r_idx) };
                // Fused: consume price/spread here, never materialize them.
                let val = if diff >= 0 && diff <= tolerance_ns {
                    let p = unsafe { *right_price.get_unchecked(r_idx) };
                    let s = unsafe { *right_spread.get_unchecked(r_idx) };
                    s / p
                } else {
                    f64::NAN
                };
                o[i].write(val);
            }
        });

    // SAFETY: every element was written exactly once above.
    unsafe { assume_init_f64(out) }
}

/// Naive single-threaded reference (order-independent) for asserting the
/// parallel path.
fn asof_join_multi_ref(
    left_ts: &[i64],
    right_ts: &[i64],
    right_price: &[f64],
    right_spread: &[f64],
    tolerance_ns: i64,
) -> (Vec<f64>, Vec<f64>) {
    left_ts
        .iter()
        .map(|&l| {
            let j = right_ts.partition_point(|&ts| ts <= l);
            if j > 0 && l - right_ts[j - 1] <= tolerance_ns {
                (right_price[j - 1], right_spread[j - 1])
            } else {
                (f64::NAN, f64::NAN)
            }
        })
        .unzip()
}

/// TC1: cross-asset lead-lag feature via the single-pass multi-column join.
fn tc1_compute(a_times: &[i64], b_times: &[i64], b_prices: &[f64], b_spread: &[f64]) {
    let _ = asof_join_multi_l2_bucket(a_times, b_times, b_prices, b_spread, 500_000, 1_000_000);
}

/// NaN-aware f64 slice equality (for asserting the parallel vs reference paths).
fn f64_slice_eq(a: &[f64], b: &[f64]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b)
            .all(|(x, y)| x == y || (x.is_nan() && y.is_nan()))
}

/// Relative-tolerance f64 slice comparison for *fused* (reassociated) reductions.
///
/// A chunked/parallel rolling sum seeds each chunk from a fresh local window and
/// therefore reorders the additions relative to a single sequential accumulator,
/// which perturbs the last few ULPs. This compares within `rtol` (relative to the
/// larger magnitude, floored at 1.0) so genuine logic bugs — which differ by
/// orders of magnitude — still fail loudly.
fn f64_slice_close(a: &[f64], b: &[f64], rtol: f64) -> bool {
    a.len() == b.len()
        && a.iter().zip(b).all(|(x, y)| {
            let scale = x.abs().max(y.abs()).max(1.0);
            (x - y).abs() <= rtol * scale
        })
}

// ---------------------------------------------------------------------------
// Test Case 2 — order-flow imbalance (OFI), rolling 100-tick
// ---------------------------------------------------------------------------

/// OFI_t = bid_size_t · ΔBidPrice_t − ask_size_t · ΔAskPrice_t, then `msum[100]`.
///
/// Naive two-pass reference: materialize `ofi`, then roll it with [`msum`].
/// Returns the rolling window so [`tc2_compute_fused`] can be asserted against it.
fn tc2_compute(bid: &[f64], ask: &[f64], bid_sz: &[u16], ask_sz: &[u16]) -> Vec<f64> {
    let n = bid.len();
    let mut ofi = Vec::with_capacity(n);
    let mut prev_bid = bid[0];
    let mut prev_ask = ask[0];
    for i in 0..n {
        let d_bid = if i == 0 { 0.0 } else { bid[i] - prev_bid };
        let d_ask = if i == 0 { 0.0 } else { ask[i] - prev_ask };
        prev_bid = bid[i];
        prev_ask = ask[i];
        ofi.push(bid_sz[i] as f64 * d_bid - ask_sz[i] as f64 * d_ask);
    }
    msum(&ofi, 100)
}

/// TC2 fused: compute `OFI_t` on the fly and fold it into the rolling 100-tick
/// window sum in a single parallel pass — the intermediate `ofi` array is never
/// materialized, halving the moved bytes (56 MB → 40 MB at 1M rows).
///
/// The window is only 100 ticks, so each chunk overlaps the previous chunk by
/// `W + 1` rows (one extra for the lag-1 price delta) and seeds its ring buffer
/// by recomputing those OFI values from the raw inputs, which stay hot in L2.
/// This is DRAM-bandwidth-bound (like TC1's fused join), not allocation-bound.
fn tc2_compute_fused(bid: &[f64], ask: &[f64], bid_sz: &[u16], ask_sz: &[u16]) -> Vec<f64> {
    use rayon::prelude::*;
    const W: usize = 100;
    const CHUNK: usize = 8192;
    let n = bid.len();

    let mut out: Vec<MaybeUninit<f64>> = Vec::with_capacity(n);
    // SAFETY: every element is written exactly once by the chunk pass below.
    unsafe {
        out.set_len(n);
    }

    out.par_chunks_mut(CHUNK).enumerate().for_each(|(ci, o)| {
        let s = ci * CHUNK;
        let e = s + o.len();

        // Seed the window with the `W` OFI values preceding this chunk (plus one
        // row for the lag-1 price delta), recomputed from the raw inputs.
        let lo = s.saturating_sub(W + 1);
        let mut win = 0.0f64;
        let mut ring = [0.0f64; W];
        let mut pos = 0usize;
        let mut prev_bid = bid[lo];
        let mut prev_ask = ask[lo];
        for i in lo + 1..s {
            let ofi = bid_sz[i] as f64 * (bid[i] - prev_bid) - ask_sz[i] as f64 * (ask[i] - prev_ask);
            prev_bid = bid[i];
            prev_ask = ask[i];
            win += ofi;
            ring[pos % W] = ofi;
            pos += 1;
        }

        // Main pass over this chunk: fold OFI into the trailing window sum.
        // The output is written with non-temporal stores so the freshly allocated
        // `out` buffer doesn't incur a read-for-ownership on every cache line.
        for i in s..e {
            let ofi = if i == 0 {
                0.0
            } else {
                bid_sz[i] as f64 * (bid[i] - prev_bid) - ask_sz[i] as f64 * (ask[i] - prev_ask)
            };
            prev_bid = bid[i];
            prev_ask = ask[i];
            win += ofi;
            if pos >= W {
                win -= ring[pos % W];
            }
            ring[pos % W] = ofi;
            pos += 1;
            // SAFETY: `i - s` is within `o` (the current chunk), and `win` is a
            // fully-initialized f64.
            unsafe {
                nt_store_f64(o.as_mut_ptr().add(i - s).cast::<f64>(), win);
            }
        }
    });

    // SAFETY: every element was written exactly once above.
    unsafe { assume_init_f64(out) }
}

/// Compute the raw OFI deltas with an AVX2 kernel: four `f64` lanes per step
/// (`_mm256_sub_pd` on the lag-1 price delta, then multiply by the widened
/// sizes). The `u16` size columns are widened `u16 → u32 → f64` in registers.
/// `raw[0]` is always 0 (no lag at the first tick). This is the hft_tc2-tc4.md
/// §1.2 SIMD pass; the O(1) trailing-window sum is applied afterwards.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[target_feature(enable = "sse4.1")]
unsafe fn ofi_deltas_avx2(
    bid: &[f64],
    ask: &[f64],
    bid_sz: &[u16],
    ask_sz: &[u16],
    raw: &mut [f64],
) {
    use std::arch::x86_64::*;

    let n = bid.len();
    raw[0] = 0.0;

    let mut i = 1usize;
    while i + 4 <= n {
        // Price deltas: cur[i..i+4] − prev[i-1..i+3], four lanes at once.
        let cur_b = _mm256_loadu_pd(bid.as_ptr().add(i));
        let prv_b = _mm256_loadu_pd(bid.as_ptr().add(i - 1));
        let d_b = _mm256_sub_pd(cur_b, prv_b);

        let cur_a = _mm256_loadu_pd(ask.as_ptr().add(i));
        let prv_a = _mm256_loadu_pd(ask.as_ptr().add(i - 1));
        let d_a = _mm256_sub_pd(cur_a, prv_a);

        // Widen the four u16 sizes at i..i+4 to f64 (load 8×u16, take the low
        // 4, zero-extend to u32, then convert to f64).
        let bsz = _mm_loadu_si128(bid_sz.as_ptr().add(i) as *const __m128i);
        let bsz32 = _mm_cvtepu16_epi32(bsz);
        let bsz_f = _mm256_cvtepi32_pd(bsz32);

        let asz = _mm_loadu_si128(ask_sz.as_ptr().add(i) as *const __m128i);
        let asz32 = _mm_cvtepu16_epi32(asz);
        let asz_f = _mm256_cvtepi32_pd(asz32);

        // OFI = bid_sz·ΔBid − ask_sz·ΔAsk, four independent lanes.
        let ofi = _mm256_sub_pd(_mm256_mul_pd(bsz_f, d_b), _mm256_mul_pd(asz_f, d_a));
        _mm256_storeu_pd(raw.as_mut_ptr().add(i), ofi);
        i += 4;
    }

    // Scalar tail: the remaining < 4 elements (or the whole array when n < 4).
    let mut prev_b = bid[i - 1];
    let mut prev_a = ask[i - 1];
    for j in i..n {
        raw[j] = bid_sz[j] as f64 * (bid[j] - prev_b) - ask_sz[j] as f64 * (ask[j] - prev_a);
        prev_b = bid[j];
        prev_a = ask[j];
    }
}

/// Scalar fallback for the raw OFI deltas (non-x86_64 hosts, or no AVX2).
fn ofi_deltas_scalar(
    bid: &[f64],
    ask: &[f64],
    bid_sz: &[u16],
    ask_sz: &[u16],
    raw: &mut [f64],
) {
    let n = bid.len();
    raw[0] = 0.0;
    let mut prev_b = bid[0];
    let mut prev_a = ask[0];
    for i in 1..n {
        raw[i] = bid_sz[i] as f64 * (bid[i] - prev_b) - ask_sz[i] as f64 * (ask[i] - prev_a);
        prev_b = bid[i];
        prev_a = ask[i];
    }
}

/// TC2 SIMD: AVX2 4-way f64 OFI deltas + O(1) trailing-window sum.
///
/// This is the direct translation of `compute_ofi_rolling_simd` from
/// hft_tc2-tc4.md §1.2: pass 1 vectorizes the `OFI_t` deltas, pass 2 applies
/// the recurrence `RollingOFI[i] = RollingOFI[i-1] + OFI[i] − OFI[i-W]` (the
/// same O(1) recurrence as `gtv_array::msum`). It stays single-threaded so the
/// SIMD gain is measured cleanly against [`tc2_compute_fused`], which trades
/// SIMD for rayon chunk parallelism.
fn tc2_compute_simd(
    bid: &[f64],
    ask: &[f64],
    bid_sz: &[u16],
    ask_sz: &[u16],
    window: usize,
) -> Vec<f64> {
    let n = bid.len();
    if n == 0 {
        return Vec::new();
    }
    let mut raw = vec![0.0f64; n];
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("avx2") {
            // SAFETY: AVX2 was detected at runtime; u16 sizes are exact in f64.
            unsafe { ofi_deltas_avx2(bid, ask, bid_sz, ask_sz, &mut raw) };
            return msum(&raw, window);
        }
    }
    ofi_deltas_scalar(bid, ask, bid_sz, ask_sz, &mut raw);
    msum(&raw, window)
}

// ---------------------------------------------------------------------------
// Test Case 3 — spoofing / wash-trading cycle detection (A→B→C→A)
// ---------------------------------------------------------------------------

struct Tc3Build {
    csr: TemporalCSR,
    pattern: Pattern,
    wash: WashTradeDetector,
    valid_at: i64,
    amount: HashMap<(u64, u64, i64), f64>,
    build_us: f64,
}

/// Wash-trade detector: a CSR whose per-source column array is sorted by
/// destination, so the closing `C→A` edge is found by binary search instead of a
/// linear scan (hft_tc2-tc4.md §2.1). Amounts travel alongside the edges so the
/// 0.1% deviation prune happens *inside* the innermost loop (§2.2), and matches
/// are returned as flat `(a, b, c)` tuples — zero per-match heap allocations.
struct WashTradeDetector {
    node_count: usize,
    row_ptr: Vec<u32>,
    /// Per-source run sorted by `dst` (ascending), enabling `binary_search`.
    col_dst: Vec<u64>,
    col_amt: Vec<f64>,
    col_vf: Vec<i64>,
    col_vt: Vec<i64>,
    max_valid_from: i64,
    min_valid_to: i64,
}

impl WashTradeDetector {
    fn build(
        src: &[u64],
        dst: &[u64],
        vf: &[i64],
        vt: &[i64],
        amount: &[f64],
        node_count: usize,
    ) -> Self {
        assert_eq!(src.len(), dst.len());
        assert_eq!(src.len(), vf.len());
        assert_eq!(src.len(), vt.len());
        assert_eq!(src.len(), amount.len());

        // Sort edges by (src, dst) so each source's run is dst-ascending.
        let mut order: Vec<usize> = (0..src.len()).collect();
        order.sort_unstable_by_key(|&i| (src[i], dst[i]));

        let mut row_ptr = vec![0u32; node_count + 1];
        let mut col_dst = Vec::with_capacity(src.len());
        let mut col_amt = Vec::with_capacity(src.len());
        let mut col_vf = Vec::with_capacity(src.len());
        let mut col_vt = Vec::with_capacity(src.len());
        let mut max_valid_from = i64::MIN;
        let mut min_valid_to = i64::MAX;
        for &i in &order {
            row_ptr[src[i] as usize + 1] += 1;
            col_dst.push(dst[i]);
            col_amt.push(amount[i]);
            col_vf.push(vf[i]);
            col_vt.push(vt[i]);
            max_valid_from = max_valid_from.max(vf[i]);
            min_valid_to = min_valid_to.min(vt[i]);
        }
        for n in 0..node_count {
            row_ptr[n + 1] += row_ptr[n];
        }

        Self {
            node_count,
            row_ptr,
            col_dst,
            col_amt,
            col_vf,
            col_vt,
            max_valid_from,
            min_valid_to,
        }
    }

    /// True when *every* edge is active at `t` (O(1), like `TemporalCSR`).
    #[inline]
    fn all_active_at(&self, t: i64) -> bool {
        t >= self.max_valid_from && t < self.min_valid_to
    }

    /// Binary search for a `src → dst` edge in the dst-sorted run; returns the
    /// flat column-array position, or `None`.
    #[inline]
    fn edge(&self, src: u64, dst: u64) -> Option<usize> {
        let s = src as usize;
        if s >= self.node_count {
            return None;
        }
        let lo = self.row_ptr[s] as usize;
        let hi = self.row_ptr[s + 1] as usize;
        let p = self.col_dst[lo..hi].binary_search(&dst).ok()?;
        Some(lo + p)
    }

    /// Find every `A→B→C→A` wash cycle active at `valid_at`, with strictly
    /// increasing event times and pairwise amount deviation within `tolerance`.
    ///
    /// Rayon parallelizes the independent start-node scan; the only per-start
    /// allocation is the per-chunk result `Vec` (none per match).
    fn detect(&self, valid_at: i64, tolerance: f64, limit: usize) -> Vec<(u32, u32, u32)> {
        use rayon::prelude::*;

        if limit == 0 {
            return Vec::new();
        }
        let all_active = self.all_active_at(valid_at);
        const CHUNK: usize = 8192;
        let starts: Vec<u32> = (0..self.node_count as u32).collect();
        let parts: Vec<Vec<(u32, u32, u32)>> = starts
            .par_chunks(CHUNK)
            .map(|chunk| {
                let mut local = Vec::new();
                for &a in chunk {
                    self.detect_from(a, valid_at, all_active, tolerance, limit, &mut local);
                    if local.len() >= limit {
                        break;
                    }
                }
                local
            })
            .collect();

        let mut out = Vec::new();
        for mut local in parts {
            out.append(&mut local);
            if out.len() >= limit {
                out.truncate(limit);
                break;
            }
        }
        out
    }

    /// Scan one start node `a` for a 3-cycle rooted there, pushing matches into
    /// `out`. The `A→B`/`B→C` amount prune runs before the `C→A` binary search
    /// so non-wash paths never pay for the lookup.
    #[inline]
    fn detect_from(
        &self,
        a: u32,
        valid_at: i64,
        all_active: bool,
        tolerance: f64,
        limit: usize,
        out: &mut Vec<(u32, u32, u32)>,
    ) {
        let a64 = a as u64;
        let a_lo = self.row_ptr[a as usize] as usize;
        let a_hi = self.row_ptr[a as usize + 1] as usize;
        for i in a_lo..a_hi {
            let b = self.col_dst[i];
            if b == a64 {
                continue;
            }
            let t0 = self.col_vf[i];
            if !all_active && (t0 > valid_at || valid_at >= self.col_vt[i]) {
                continue;
            }
            let amt_ab = self.col_amt[i];
            if amt_ab == 0.0 {
                continue;
            }

            let b_usize = b as usize;
            if b_usize >= self.node_count {
                continue;
            }
            let b_lo = self.row_ptr[b_usize] as usize;
            let b_hi = self.row_ptr[b_usize + 1] as usize;
            for j in b_lo..b_hi {
                let c = self.col_dst[j];
                if c == a64 || c == b {
                    continue;
                }
                let t1 = self.col_vf[j];
                if t1 <= t0 {
                    continue;
                }
                if !all_active && (t1 > valid_at || valid_at >= self.col_vt[j]) {
                    continue;
                }
                let amt_bc = self.col_amt[j];
                // Amount early-prune: skip if A→B and B→C deviate > tolerance.
                if (amt_ab - amt_bc).abs() / amt_ab > tolerance {
                    continue;
                }

                // Closing edge C→A via binary search (dst-sorted run).
                let Some(k) = self.edge(c, a64) else {
                    continue;
                };
                let t2 = self.col_vf[k];
                if t2 <= t1 {
                    continue;
                }
                if !all_active && (t2 > valid_at || valid_at >= self.col_vt[k]) {
                    continue;
                }
                let amt_ca = self.col_amt[k];
                if amt_bc == 0.0 || (amt_bc - amt_ca).abs() / amt_bc > tolerance {
                    continue;
                }

                out.push((a, b as u32, c as u32));
                if out.len() >= limit {
                    return;
                }
            }
        }
    }
}

/// Plant `cycles` A→B→C→A cycles with equal amounts inside a `node_count` graph,
/// then detect them with `Pattern::ring(3)` + an amount-deviation (< 0.1%) filter.
fn tc3_build(node_count: usize, cycles: usize) -> Tc3Build {
    let cycle_nodes = cycles * 3;
    let chain_nodes = node_count - cycle_nodes;

    let mut src: Vec<u64> = Vec::with_capacity(node_count);
    let mut dst: Vec<u64> = Vec::with_capacity(node_count);
    let mut vf: Vec<i64> = Vec::with_capacity(node_count);
    let mut vt: Vec<i64> = Vec::with_capacity(node_count);
    let mut et: Vec<u16> = Vec::with_capacity(node_count);
    let mut amounts: Vec<f64> = Vec::with_capacity(node_count);
    let mut amount: HashMap<(u64, u64, i64), f64> = HashMap::new();

    const BIG: i64 = 1_000_000_000; // 1s validity -> active for every query we use
    // Chain spine: no back-edges, so no false cycles.
    for i in 0..chain_nodes - 1 {
        let (s, d, f) = (i as u64, i as u64 + 1, i as i64);
        let amt = 1000.0 + (i % 7) as f64;
        src.push(s);
        dst.push(d);
        vf.push(f);
        vt.push(f + BIG);
        et.push(1);
        amounts.push(amt);
        amount.insert((s, d, f), amt);
    }
    // Planted wash cycles: A→B→C→A with identical amounts and strictly
    // increasing event times (1000 < 2000 < 3000) inside a 10ms window.
    for k in 0..cycles {
        let a = (chain_nodes + k * 3) as u64;
        let b = a + 1;
        let c = a + 2;
        let edges = [(a, b, 1000i64), (b, c, 2000i64), (c, a, 3000i64)];
        for &(s, d, f) in &edges {
            src.push(s);
            dst.push(d);
            vf.push(f);
            vt.push(f + BIG);
            et.push(1);
            amounts.push(1000.0);
            amount.insert((s, d, f), 1000.0);
        }
    }

    let valid_at = (chain_nodes - 1) as i64;

    let build_start = Instant::now();
    let wash = WashTradeDetector::build(&src, &dst, &vf, &vt, &amounts, node_count);
    let csr = TemporalCSR::from_arrays(
        &UInt64Array::from(src),
        &UInt64Array::from(dst),
        &TimestampNanosecondArray::from(vf),
        &TimestampNanosecondArray::from(vt),
        &UInt16Array::from(et),
        node_count,
    )
    .expect("build CSR");
    let pattern = Pattern::ring(3);
    let build_us = build_start.elapsed().as_secs_f64() * 1e6;

    Tc3Build {
        csr,
        pattern,
        wash,
        valid_at,
        amount,
        build_us,
    }
}

/// Reference detection: generic `ring(3)` pattern match + post-filter on amount
/// deviation (kept for correctness cross-checking the fast CSR detector).
fn tc3_query(b: &Tc3Build) -> usize {
    let matches = find(&b.csr, &b.pattern, b.valid_at, 1_000_000).expect("find cycles");
    matches
        .iter()
        .filter(|m| {
            let amounts: Vec<f64> = m
                .edges
                .iter()
                .map(|e| b.amount[&(e.src, e.dst, e.valid_from)])
                .collect();
            let min = amounts.iter().cloned().fold(f64::INFINITY, f64::min);
            let max = amounts.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            max > 0.0 && (max - min) / max < 0.001
        })
        .count()
}

/// Fast detection: in-loop amount pruning + dst binary search + zero per-match
/// allocation (the optimization this file's guide calls for in §2).
fn tc3_query_fast(b: &Tc3Build) -> usize {
    b.wash.detect(b.valid_at, 0.001, 1_000_000).len()
}

// ---------------------------------------------------------------------------
// Test Case 4 — alt-data vector K-NN + temporal volatility
// ---------------------------------------------------------------------------

struct Tc4Build {
    index: FlatIndex,
    query: Vec<f32>,
    prices: Vec<f64>,
    n: usize,
    build_us: f64,
}

/// Full-corpus exact 512-dim K-NN (FlatIndex), then for each of the top-10 the
/// volatility (±100ms) of its own time series. Events are spaced 1ms apart, so a
/// ±100ms window is ±100 neighbours around the hit.
fn tc4_build(n: usize, dim: usize) -> Tc4Build {
    let (ids, data) = gen_embeddings(n, dim, 0x5EED_0004);
    let query: Vec<f32> = gen_embeddings(1, dim, 0x0FF5_0004).1;
    // A correlated price series to compute volatility over the ±100ms window.
    let prices: Vec<f64> = {
        let mut rng = SplitMix64::new(0x1CA0_0004);
        let mut p = 100.0f64;
        (0..n)
            .map(|_| {
                p += (rng.next_f64() - 0.5) * 0.2;
                p
            })
            .collect()
    };

    let build_start = Instant::now();
    let index = FlatIndex::from_flat(ids, data, dim).expect("build flat index");
    let build_us = build_start.elapsed().as_secs_f64() * 1e6;

    Tc4Build {
        index,
        query,
        prices,
        n,
        build_us,
    }
}

/// Shared volatility tail for TC4: for each top-K hit, the standard deviation of
/// the hit's own price series over the ±100ms window.
fn tc4_volatility(hit_ids: &[usize], prices: &[f64], n: usize) -> f64 {
    let mut vol = 0.0f64;
    for &i in hit_ids {
        let lo = i.saturating_sub(100);
        let hi = (i + 100).min(n - 1);
        let mean = prices[lo..=hi].iter().sum::<f64>() / (hi - lo + 1) as f64;
        let var = prices[lo..=hi]
            .iter()
            .map(|&p| (p - mean) * (p - mean))
            .sum::<f64>()
            / (hi - lo + 1) as f64;
        vol += var.sqrt();
    }
    vol
}

/// CPU exact K-NN: top-10 ids ordered by distance (id tie-break), matching the
/// ordering the CUDA path must reproduce.
fn tc4_cpu_knn(b: &Tc4Build) -> Vec<u64> {
    let hits = b.index.search_knn(&b.query, 10, None).expect("knn search");
    hits.values().as_ref().to_vec()
}

fn tc4_query(b: &Tc4Build) -> usize {
    let top = tc4_cpu_knn(b);
    let hit_ids: Vec<usize> = top.iter().map(|&id| id as usize).collect();
    std::hint::black_box(tc4_volatility(&hit_ids, &b.prices, b.n));
    top.len()
}

/// Inverted-file index built from the *same* deterministic corpus as the exact
/// path (same seed), so a recall@10 comparison against [`tc4_cpu_knn`] is
/// meaningful. Full `f32` precision is kept — the sublinearity comes from
/// pruning cells, never from quantizing the data.
struct Tc4Ivf {
    index: IvfIndex,
    query: Vec<f32>,
    prices: Vec<f64>,
    n: usize,
    build_us: f64,
}

fn tc4_ivf_build(n: usize, dim: usize, nlist: usize, nprobe: usize) -> Tc4Ivf {
    let (ids, data) = gen_embeddings(n, dim, 0x5EED_0004);
    let query: Vec<f32> = gen_embeddings(1, dim, 0x0FF5_0004).1;
    let prices: Vec<f64> = {
        let mut rng = SplitMix64::new(0x1CA0_0004);
        let mut p = 100.0f64;
        (0..n)
            .map(|_| {
                p += (rng.next_f64() - 0.5) * 0.2;
                p
            })
            .collect()
    };

    let build_start = Instant::now();
    let index = IvfIndex::new(ids, data, dim, nlist, nprobe).expect("build ivf index");
    let build_us = build_start.elapsed().as_secs_f64() * 1e6;

    Tc4Ivf {
        index,
        query,
        prices,
        n,
        build_us,
    }
}

fn tc4_ivf_query(b: &Tc4Ivf) -> Vec<u64> {
    let hits = b.index.search_knn(&b.query, 10, None).expect("ivf knn");
    hits.values().as_ref().to_vec()
}

/// Full IVF query: top-10 ids + the ±100ms volatility tail (same shape as the
/// exact TC4 query, so the two rows are latency-comparable).
fn tc4_ivf_query_full(b: &Tc4Ivf) -> usize {
    let top = tc4_ivf_query(b);
    let hit_ids: Vec<usize> = top.iter().map(|&id| id as usize).collect();
    std::hint::black_box(tc4_volatility(&hit_ids, &b.prices, b.n));
    top.len()
}

fn recall_at_10(exact: &[u64], approx: &[u64]) -> f64 {
    let hit = exact.iter().filter(|e| approx.contains(e)).count();
    hit as f64 / exact.len().max(1) as f64
}

// ---------------------------------------------------------------------------
// TC4 CUDA acceleration (optional — compile with `--features cuda`)
// ---------------------------------------------------------------------------

#[cfg(feature = "cuda")]
struct Tc4Cuda {
    ctx: Arc<cudarc::driver::CudaContext>,
    func: cudarc::driver::CudaFunction,
    // Resident device buffers — uploaded once at build, reused across queries.
    d_data_t: cudarc::driver::CudaSlice<f32>, // column-major corpus
    d_query: cudarc::driver::CudaSlice<f32>,
    d_out_id: cudarc::driver::CudaSlice<i32>,
    d_out_dist: cudarc::driver::CudaSlice<f32>,
    n: i32,
    dim: i32,
    num_blocks: u32,
    build_us: f64,
}

/// One-time GPU setup: NVRTC-compile `knn_top10.cu`, transpose the corpus to
/// column-major on the host (build-time, one-time), and upload corpus + query so
/// a query only launches the kernel and downloads the tiny per-block candidate
/// buffer. memory0copy.md: the corpus stays resident and the N intermediate
/// distances never cross the PCIe bus; the host transpose staging buffer is
/// freed the moment it is on device.
#[cfg(feature = "cuda")]
fn tc4_cuda_build(data: &[f32], query: &[f32], n: usize, dim: usize) -> Tc4Cuda {
    use cudarc::driver::CudaContext;
    use cudarc::nvrtc::compile_ptx;

    let t0 = Instant::now();
    let ctx = CudaContext::new(0).expect("init CUDA context");
    let ptx = compile_ptx(include_str!("knn_top10.cu")).expect("NVRTC compile knn kernel");
    let module = ctx.load_module(ptx).expect("load PTX module");
    let func = module.load_function("knn_top10_kernel").expect("load knn kernel");
    let stream = ctx.default_stream();

    // Column-major transpose (build-time) so warps read coalesced.
    let mut data_t = vec![0.0f32; n * dim];
    for d in 0..dim {
        for i in 0..n {
            data_t[d * n + i] = data[i * dim + d];
        }
    }

    let d_data_t = stream.clone_htod(&data_t).expect("H2D corpus");
    let d_query = stream.clone_htod(query).expect("H2D query");
    let threads: u32 = 256;
    let num_blocks = ((n as u32) + threads - 1) / threads;
    let cand = (num_blocks as usize) * 10;
    let d_out_id = stream.alloc_zeros::<i32>(cand).expect("alloc out id");
    let d_out_dist = stream.alloc_zeros::<f32>(cand).expect("alloc out dist");
    drop(data_t); // free the host staging copy as soon as it is on device

    Tc4Cuda {
        build_us: t0.elapsed().as_secs_f64() * 1e6,
        ctx,
        func,
        d_data_t,
        d_query,
        d_out_id,
        d_out_dist,
        n: n as i32,
        dim: dim as i32,
        num_blocks,
    }
}

/// Launch the fused distance + top-K kernel and download only the per-block
/// candidates, then merge/sort on the host into the final top-10 ids.
#[cfg(feature = "cuda")]
fn tc4_cuda_query(c: &mut Tc4Cuda, k: usize) -> Vec<u64> {
    use cudarc::driver::{LaunchConfig, PushKernelArg};

    let stream = c.ctx.default_stream();
    let cfg = LaunchConfig {
        grid_dim: (c.num_blocks, 1, 1),
        block_dim: (256, 1, 1),
        shared_mem_bytes: 0,
    };

    let mut lb = stream.launch_builder(&c.func);
    lb.arg(&c.d_data_t)
        .arg(&c.d_query)
        .arg(&c.n)
        .arg(&c.dim)
        .arg(&mut c.d_out_id)
        .arg(&mut c.d_out_dist);
    unsafe { lb.launch(cfg) }.expect("launch knn kernel");

    let ids: Vec<i32> = stream.clone_dtoh(&c.d_out_id).expect("D2H ids");
    let dists: Vec<f32> = stream.clone_dtoh(&c.d_out_dist).expect("D2H dists");

    let mut cand: Vec<(f32, i32)> = dists
        .iter()
        .zip(ids.iter())
        .filter(|(_, &i)| i >= 0)
        .map(|(&d, &i)| (d, i))
        .collect();
    cand.sort_by(|a, b| {
        a.0.partial_cmp(&b.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.1.cmp(&b.1))
    });
    cand.iter().take(k).map(|(_, i)| *i as u64).collect()
}

/// CUDA TC4 query: fused device K-NN + host volatility tail.
#[cfg(feature = "cuda")]
fn tc4_query_cuda(b: &Tc4Build, c: &mut Tc4Cuda) -> usize {
    let top = tc4_cuda_query(c, 10);
    let hit_ids: Vec<usize> = top.iter().map(|&id| id as usize).collect();
    std::hint::black_box(tc4_volatility(&hit_ids, &b.prices, b.n));
    top.len()
}

// ---------------------------------------------------------------------------
// Test Case 5 — point-in-time order-book snapshot (O(log N) binary slice +
// O(N) zone-map reference)
// ---------------------------------------------------------------------------

struct Tc5Build {
    zones: Vec<ZoneMap>,
    vf: Vec<i64>,
    vt: Vec<i64>,
    t: i64,
    active: usize,
    build_us: f64,
}

/// `valid_from` ascending with a fixed duration (`valid_to = valid_from + 100`)
/// means both temporal columns are sorted, so the active-at-`T` set is a
/// *contiguous* index range — recoverable in O(log N) with zero writes.
fn tc5_build(n: usize, chunk: usize) -> Tc5Build {
    const DURATION: i64 = 100;
    let vf: Vec<i64> = (0..n as i64).collect();
    let vt: Vec<i64> = vf.iter().map(|&f| f + DURATION).collect();
    let t = (n as i64) / 2;

    let build_start = Instant::now();
    let zones = build_zone_maps(&vf, &vt, chunk);
    let build_us = build_start.elapsed().as_secs_f64() * 1e6;

    // Correctness: O(log N) binary range == pruned mask == full scan.
    let full = temporal_mask_full(&vf, &vt, t);
    let pruned = temporal_mask_pruned(&vf, &vt, t, &zones);
    assert_eq!(full, pruned, "zone-map mask diverges from full scan");
    let range = point_in_time_range(&vf, &vt, t);
    let active = range.end - range.start;
    assert_eq!(
        active,
        (0..n).filter(|&i| pruned.value(i)).count(),
        "binary slice count diverges from mask"
    );

    Tc5Build {
        zones,
        vf,
        vt,
        t,
        active,
        build_us,
    }
}

/// O(log N) zero-copy snapshot: two `partition_point` boundary searches return
/// the active row range; no mask is materialized and no row is read/written.
fn tc5_query_bin(b: &Tc5Build) -> usize {
    let range = point_in_time_range(&b.vf, &b.vt, b.t);
    range.end - range.start
}

/// O(N) zone-map reference: builds a bitmask by pruning whole 128-edge chunks.
fn tc5_query(b: &Tc5Build) {
    let _ = temporal_mask_pruned(&b.vf, &b.vt, b.t, &b.zones);
}

// ---------------------------------------------------------------------------
// Timing harness
// ---------------------------------------------------------------------------

fn bench_us<F: FnMut()>(iters: usize, mut f: F) -> f64 {
    f(); // warmup
    // Report the best (min) iteration: most robust to machine noise / scheduler
    // interference on a shared WSL2 host, and represents the achievable floor.
    let mut best = f64::INFINITY;
    for _ in 0..iters {
        let t = Instant::now();
        std::hint::black_box(f());
        best = best.min(t.elapsed().as_secs_f64() * 1e6);
    }
    best
}

fn fmt_ms(us: f64) -> String {
    if us >= 1000.0 {
        format!("{:.2} ms", us / 1000.0)
    } else {
        format!("{:.1} µs", us)
    }
}

fn fmt_mb(bytes: f64) -> String {
    format!("{:.1}", bytes / (1024.0 * 1024.0))
}

// ---------------------------------------------------------------------------
// Data loader (CSV round-trip)
// ---------------------------------------------------------------------------

fn write_ticks_csv(path: &str, n: usize) {
    let (ts, price) = gen_series(n, 0x71C5, 1000);
    let mut rng = SplitMix64::new(0x71C6);
    let mut out =
        String::from("symbol,timestamp,price,volume,bid_price_1,ask_price_1,bid_size_1,ask_size_1\n");
    for i in 0..n {
        let bid = price[i] - 0.01;
        let ask = price[i] + 0.01;
        out.push_str(&format!(
            "{},{},{},{:.6},{:.6},{:.6},{},{}\n",
            if i % 2 == 0 { "0700.HK" } else { "3690.HK" },
            ts[i],
            price[i],
            1 + rng.next_u64() % 1000,
            bid,
            ask,
            1 + rng.next_u64() % 10_000,
            1 + rng.next_u64() % 10_000,
        ));
    }
    std::fs::write(path, out).expect("write ticks csv");
}

fn write_transfers_csv(path: &str, n: usize) {
    let mut rng = SplitMix64::new(0xA771);
    let mut out = String::from("src_account,dst_account,valid_from,valid_to,amount\n");
    for i in 0..n {
        out.push_str(&format!(
            "{},{},{},{},{:.6}\n",
            rng.next_u64() % (n as u64),
            rng.next_u64() % (n as u64),
            i,
            i + 1_000_000,
            1000.0 + (rng.next_f64() - 0.5) * 10.0,
        ));
    }
    std::fs::write(path, out).expect("write transfers csv");
}

fn read_ticks_csv(path: &str) -> RecordBatch {
    let text = std::fs::read_to_string(path).expect("read ticks csv");
    let mut symbol = Vec::new();
    let mut ts = Vec::new();
    let mut price = Vec::new();
    let mut volume = Vec::new();
    let mut bid = Vec::new();
    let mut ask = Vec::new();
    let mut bid_sz = Vec::new();
    let mut ask_sz = Vec::new();
    for line in text.lines().skip(1) {
        let f: Vec<&str> = line.split(',').collect();
        if f.len() < 8 {
            continue;
        }
        symbol.push(f[0].to_string());
        ts.push(f[1].parse::<i64>().unwrap());
        price.push(f[2].parse::<f64>().unwrap());
        volume.push(f[3].parse::<u64>().unwrap());
        bid.push(f[4].parse::<f64>().unwrap());
        ask.push(f[5].parse::<f64>().unwrap());
        bid_sz.push(f[6].parse::<u64>().unwrap());
        ask_sz.push(f[7].parse::<u64>().unwrap());
    }
    RecordBatch::try_new(
        Arc::new(ticks_schema()),
        vec![
            Arc::new(StringArray::from(symbol)) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(ts)) as ArrayRef,
            Arc::new(Float64Array::from(price)) as ArrayRef,
            Arc::new(UInt64Array::from(volume)) as ArrayRef,
            Arc::new(Float64Array::from(bid)) as ArrayRef,
            Arc::new(Float64Array::from(ask)) as ArrayRef,
            Arc::new(UInt64Array::from(bid_sz)) as ArrayRef,
            Arc::new(UInt64Array::from(ask_sz)) as ArrayRef,
        ],
    )
    .expect("ticks record batch")
}

fn read_transfers_csv(path: &str) -> RecordBatch {
    let text = std::fs::read_to_string(path).expect("read transfers csv");
    let mut src = Vec::new();
    let mut dst = Vec::new();
    let mut vf = Vec::new();
    let mut vt = Vec::new();
    let mut amount = Vec::new();
    for line in text.lines().skip(1) {
        let f: Vec<&str> = line.split(',').collect();
        if f.len() < 5 {
            continue;
        }
        src.push(f[0].parse::<u64>().unwrap());
        dst.push(f[1].parse::<u64>().unwrap());
        vf.push(f[2].parse::<i64>().unwrap());
        vt.push(f[3].parse::<i64>().unwrap());
        amount.push(f[4].parse::<f64>().unwrap());
    }
    RecordBatch::try_new(
        Arc::new(transfers_schema()),
        vec![
            Arc::new(UInt64Array::from(src)) as ArrayRef,
            Arc::new(UInt64Array::from(dst)) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(vf)) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(vt)) as ArrayRef,
            Arc::new(Float64Array::from(amount)) as ArrayRef,
        ],
    )
    .expect("transfers record batch")
}

fn ticks_schema() -> Schema {
    Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("price", DataType::Float64, false),
        Field::new("volume", DataType::UInt64, false),
        Field::new("bid_price_1", DataType::Float64, false),
        Field::new("ask_price_1", DataType::Float64, false),
        Field::new("bid_size_1", DataType::UInt64, false),
        Field::new("ask_size_1", DataType::UInt64, false),
    ])
}

fn transfers_schema() -> Schema {
    Schema::new(vec![
        Field::new("src_account", DataType::UInt64, false),
        Field::new("dst_account", DataType::UInt64, false),
        Field::new(
            "valid_from",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "valid_to",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("amount", DataType::Float64, false),
    ])
}

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

struct Row {
    tc: &'static str,
    scale: usize,
    build_us: f64,
    query_us: f64,
    bytes: f64,
    threshold_us: Option<f64>,
    note: String,
}

impl Row {
    fn pass(&self) -> &'static str {
        match self.threshold_us {
            Some(t) if self.query_us <= t => "✅ PASS",
            Some(_) => "❌ FAIL",
            None => "—",
        }
    }
}

/// Configure the global rayon pool once. Bounding threads to the number of
/// memory channels (4–8) beats "all cores" for this memory-bound sweep — too
/// many threads queue on the memory controller. Overridable via `GTV_HFT_THREADS`.
fn init_thread_pool() -> usize {
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    let threads = std::env::var("GTV_HFT_THREADS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(8)
        .clamp(1, cpus);
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build_global()
        .expect("build global rayon pool");
    threads
}

// ---------------------------------------------------------------------------
// TC1 CUDA acceleration (optional — compile with `--features cuda`)
// ---------------------------------------------------------------------------

#[cfg(feature = "cuda")]
struct Tc1Cuda {
    ctx: Arc<cudarc::driver::CudaContext>,
    // NOTE: `func` holds the module's `Arc<CudaModule>` internally, keeping it alive.
    func: cudarc::driver::CudaFunction,
    // Resident device buffers — uploaded once at build, reused across queries.
    d_left: cudarc::driver::CudaSlice<i64>,
    d_right: cudarc::driver::CudaSlice<i64>,
    d_price: cudarc::driver::CudaSlice<f64>,
    d_spread: cudarc::driver::CudaSlice<f64>,
    d_bucket: cudarc::driver::CudaSlice<i32>,
    d_out: cudarc::driver::CudaSlice<f64>,
    left_len: i32,
    right_len: i32,
    num_buckets: i32,
    min_r_ts: i64,
    bucket_ms: i64,
    tol: i64,
    chunk: i32,
    build_us: f64,
}

/// One-time GPU setup: context, NVRTC compile of `asof_join.cu`, module + function
/// load, then upload every input (right table + O(1) time-bucket index + left) so
/// the merge-join query only launches the kernel and downloads the 8 MB output.
#[cfg(feature = "cuda")]
fn tc1_cuda_build(
    left_ts: &[i64],
    right_ts: &[i64],
    right_price: &[f64],
    right_spread: &[f64],
    tolerance_ns: i64,
    bucket_ms: i64,
) -> Tc1Cuda {
    use cudarc::driver::CudaContext;
    use cudarc::nvrtc::compile_ptx;

    let t0 = Instant::now();
    let ctx = CudaContext::new(0).expect("init CUDA context");
    let ptx = compile_ptx(include_str!("asof_join.cu")).expect("NVRTC compile kernel");
    let module = ctx.load_module(ptx).expect("load PTX module");
    let func = module
        .load_function("asof_merge_fused_kernel")
        .expect("load kernel fn");
    let stream = ctx.default_stream();

    // Host-side O(M) time-bucket index (identical to the CPU v3/v5 path), built
    // once and uploaded; it is tiny (≈ (span/bucket_ms) + 1 ints).
    let min_r_ts = right_ts[0];
    let max_r_ts = *right_ts.last().unwrap();
    let num_buckets = ((max_r_ts - min_r_ts) / bucket_ms + 1) as i32;
    let mut bucket_offsets = vec![0i32; (num_buckets + 1) as usize];
    {
        let mut b_curr = 0i32;
        for (i, &ts) in right_ts.iter().enumerate() {
            let b = ((ts - min_r_ts) / bucket_ms) as i32;
            while b_curr <= b {
                bucket_offsets[b_curr as usize] = i as i32;
                b_curr += 1;
            }
        }
        for b in b_curr..=num_buckets {
            bucket_offsets[b as usize] = right_ts.len() as i32;
        }
    }

    // Resident uploads — never re-transferred per query.
    let d_left = stream.clone_htod(left_ts).expect("H2D left");
    let d_right = stream.clone_htod(right_ts).expect("H2D right");
    let d_price = stream.clone_htod(right_price).expect("H2D price");
    let d_spread = stream.clone_htod(right_spread).expect("H2D spread");
    let d_bucket = stream.clone_htod(&bucket_offsets).expect("H2D bucket");
    let d_out = stream.alloc_zeros::<f64>(left_ts.len()).expect("alloc out");

    Tc1Cuda {
        build_us: t0.elapsed().as_secs_f64() * 1e6,
        ctx,
        func,
        d_left,
        d_right,
        d_price,
        d_spread,
        d_bucket,
        d_out,
        left_len: left_ts.len() as i32,
        right_len: right_ts.len() as i32,
        num_buckets,
        min_r_ts,
        bucket_ms,
        tol: tolerance_ns,
        chunk: 32,
    }
}

/// Per-query cost: launch the fused merge kernel (all data resident) and download
/// the single 8 MB output. No input H2D — the only host traffic is the 8 MB D2H.
#[cfg(feature = "cuda")]
fn tc1_cuda_query(b: &mut Tc1Cuda) -> Vec<f64> {
    use cudarc::driver::{LaunchConfig, PushKernelArg};

    let stream = b.ctx.default_stream();
    let threads = 256u32;
    let total_chunks = (b.left_len as u32 + b.chunk as u32 - 1) / b.chunk as u32;
    let blocks = (total_chunks + threads - 1) / threads;
    let cfg = LaunchConfig {
        grid_dim: (blocks, 1, 1),
        block_dim: (threads, 1, 1),
        shared_mem_bytes: 0,
    };

    let mut lb = stream.launch_builder(&b.func);
    lb.arg(&b.d_left)
        .arg(&b.d_right)
        .arg(&b.d_price)
        .arg(&b.d_spread)
        .arg(&b.d_bucket)
        .arg(&mut b.d_out)
        .arg(&b.left_len)
        .arg(&b.right_len)
        .arg(&b.num_buckets)
        .arg(&b.min_r_ts)
        .arg(&b.bucket_ms)
        .arg(&b.tol)
        .arg(&b.chunk);
    unsafe { lb.launch(cfg) }.expect("launch kernel");

    stream.clone_dtoh(&b.d_out).expect("D2H out")
}

/// Runtime CUDA switch: `USE_CUDA=1` enables the GPU path. If the binary lacks
/// the `cuda` feature or the GPU can't initialize, fall back to CPU with a warning.
fn detect_cuda() -> bool {
    #[cfg(feature = "cuda")]
    {
        let mut on = std::env::var("USE_CUDA").map(|v| v == "1").unwrap_or(false);
        if on {
            if let Err(e) = cudarc::driver::CudaContext::new(0) {
                eprintln!("[CUDA] USE_CUDA=1 but GPU init failed: {e:?} — falling back to CPU.");
                eprintln!("[CUDA]   WSL2 hint: if nvidia-smi shows a GPU but cuInit reports NO_DEVICE,");
                eprintln!("[CUDA]   run with LD_LIBRARY_PATH=/usr/lib/wsl/lib to use the WSL forwarding stub.");
                on = false;
            }
        }
        on
    }
    #[cfg(not(feature = "cuda"))]
    {
        if std::env::var("USE_CUDA").map(|v| v == "1").unwrap_or(false) {
            eprintln!("[CUDA] USE_CUDA=1 but binary lacks `--features cuda`; using CPU.");
        }
        false
    }
}

fn main() {
    std::fs::create_dir_all(DATA_DIR).expect("create data dir");
    let threads = init_thread_pool();
    let use_cuda = detect_cuda();
    let use_gpu = cfg!(feature = "cuda") && use_cuda;

    // ---- Data loader (CSV round-trip on a representative sample) ----
    let sample = 10_000usize;
    write_ticks_csv(&format!("{DATA_DIR}/ticks.csv"), sample);
    write_transfers_csv(&format!("{DATA_DIR}/account_transfers.csv"), sample);
    let ticks = read_ticks_csv(&format!("{DATA_DIR}/ticks.csv"));
    let transfers = read_transfers_csv(&format!("{DATA_DIR}/account_transfers.csv"));
    let loader_ok = ticks.num_rows() == sample && transfers.num_rows() == sample;

    let mut rows: Vec<Row> = Vec::new();

    // ---- TC1: cross-asset as-of join ----
    // Correctness: the parallel multi-column path must match the naive reference.
    {
        let (a_times, _) = gen_series(10_000, 0xA1, 1000);
        let (b_times, b_prices) = gen_series(10_000, 0xA2, 1000);
        let b_spread: Vec<f64> = b_prices.iter().map(|p| 0.02 + p * 0.0001).collect();
        let (rp, rs) = asof_join_multi_ref(&a_times, &b_times, &b_prices, &b_spread, 500_000);
        let (fp, fs) = asof_join_multi_l2_bucket(
            &a_times,
            &b_times,
            &b_prices,
            &b_spread,
            500_000,
            1_000_000,
        );
        assert!(f64_slice_eq(&fp, &rp), "TC1 v3 price != reference");
        assert!(f64_slice_eq(&fs, &rs), "TC1 v3 spread != reference");

        let (up, us) = asof_join_cpu_ultimate(
            &a_times,
            &b_times,
            &b_prices,
            &b_spread,
            500_000,
            1_000_000,
        );
        assert!(f64_slice_eq(&up, &rp), "TC1 v4 price != reference");
        assert!(f64_slice_eq(&us, &rs), "TC1 v4 spread != reference");

        let fused = asof_join_fused(&a_times, &b_times, &b_prices, &b_spread, 500_000, 1_000_000);
        let fused_ref: Vec<f64> = rp
            .iter()
            .zip(&rs)
            .map(|(&p, &s)| if p.is_nan() { f64::NAN } else { s / p })
            .collect();
        assert!(f64_slice_eq(&fused, &fused_ref), "TC1 fused != reference");

        #[cfg(feature = "cuda")]
        {
            if use_gpu {
                let mut c = tc1_cuda_build(
                    &a_times, &b_times, &b_prices, &b_spread, 500_000, 1_000_000,
                );
                let cf = tc1_cuda_query(&mut c);
                assert!(f64_slice_eq(&cf, &fused), "TC1 cuda merge != fused");
            }
        }
    }
    for &n in &[100_000usize, 1_000_000, 5_000_000] {
        let (a_times, _) = gen_series(n, 0xA1, 1000);
        let (b_times, b_prices) = gen_series(n, 0xA2, 1000);
        let b_spread: Vec<f64> = b_prices.iter().map(|p| 0.02 + p * 0.0001).collect();
        let iters = if n >= 5_000_000 { 3 } else if n >= 1_000_000 { 5 } else { 10 };

        // CPU v3 baseline (branchless single-pass).
        let q3 = bench_us(iters, || tc1_compute(&a_times, &b_times, &b_prices, &b_spread));
        rows.push(Row {
            tc: "TC1",
            scale: n,
            build_us: 0.0,
            query_us: q3,
            bytes: (n * 32) as f64,
            threshold_us: if n == 1_000_000 { Some(5_000.0) } else { None },
            note: "as-of join v3 (bucket + 8k chunk + 8t + branchless/prefetch), 500µs lag".into(),
        });

        // CPU v4 (payload decoupling + non-temporal stores).
        let q4 = bench_us(iters, || {
            let _ = asof_join_cpu_ultimate(
                &a_times,
                &b_times,
                &b_prices,
                &b_spread,
                500_000,
                1_000_000,
            );
        });
        rows.push(Row {
            tc: "TC1",
            scale: n,
            build_us: 0.0,
            query_us: q4,
            bytes: (n * 32) as f64,
            threshold_us: if n == 1_000_000 { Some(5_000.0) } else { None },
            note: "as-of join v4 (payload decoupling + NT store), 500µs lag".into(),
        });

        // TC1 fused with a downstream feature (zero-copy / no 16MB materialization).
        let qf = bench_us(iters, || {
            let _ = asof_join_fused(&a_times, &b_times, &b_prices, &b_spread, 500_000, 1_000_000);
        });
        rows.push(Row {
            tc: "TC1",
            scale: n,
            build_us: 0.0,
            query_us: qf,
            bytes: (n * 32) as f64,
            threshold_us: if n == 1_000_000 { Some(5_000.0) } else { None },
            note: "as-of join fused (→ rel-spread 8MB out, no 16MB write), 500µs lag".into(),
        });

        // CUDA merge-join (resident data + fused feature), only with GPU present.
        #[cfg(feature = "cuda")]
        {
            if use_gpu {
                let mut c = tc1_cuda_build(
                    &a_times, &b_times, &b_prices, &b_spread, 500_000, 1_000_000,
                );
                let qc = bench_us(iters, || {
                    let _ = tc1_cuda_query(&mut c);
                });
                rows.push(Row {
                    tc: "TC1",
                    scale: n,
                    build_us: c.build_us,
                    query_us: qc,
                    bytes: (n * 32) as f64,
                    threshold_us: if n == 1_000_000 { Some(5_000.0) } else { None },
                    note: "as-of join CUDA merge (resident + fused 8MB out), 500µs lag".into(),
                });
            }
        }
    }

    // ---- TC2: OFI rolling 100 ----
    // Correctness: the fused parallel pass (no intermediate) must match the
    // two-pass reference (`ofi` -> `msum[100]`) to within floating-point
    // reassociation (the chunked window reorders the additions).
    {
        let (bid, ask, bid_sz, ask_sz) = gen_order_flow(10_000, 0xB1);
        let fused = tc2_compute_fused(&bid, &ask, &bid_sz, &ask_sz);
        let reference = tc2_compute(&bid, &ask, &bid_sz, &ask_sz);
        let simd = tc2_compute_simd(&bid, &ask, &bid_sz, &ask_sz, 100);
        assert!(
            f64_slice_close(&fused, &reference, 1e-9),
            "TC2 fused rolling sum != reference msum"
        );
        assert!(
            f64_slice_close(&simd, &reference, 1e-12),
            "TC2 SIMD rolling sum != reference msum"
        );
    }
    for &n in &[100_000usize, 1_000_000, 5_000_000] {
        let (bid, ask, bid_sz, ask_sz) = gen_order_flow(n, 0xB1);
        let iters = if n >= 5_000_000 { 3 } else if n >= 1_000_000 { 5 } else { 10 };
        let q = bench_us(iters, || {
            std::hint::black_box(tc2_compute_fused(&bid, &ask, &bid_sz, &ask_sz));
        });
        rows.push(Row {
            tc: "TC2",
            scale: n,
            build_us: 0.0,
            query_us: q,
            bytes: (n * 32) as f64,
            threshold_us: if n == 1_000_000 { Some(2_000.0) } else { None },
            note: "OFI = e·ΔBid − f·ΔAsk, fused rolling msum[100] (no intermediate)".into(),
        });
    }
    // TC2 SIMD variant: AVX2 4-way f64 OFI deltas + O(1) sliding sum.
    for &n in &[100_000usize, 1_000_000, 5_000_000] {
        let (bid, ask, bid_sz, ask_sz) = gen_order_flow(n, 0xB1);
        let iters = if n >= 5_000_000 { 3 } else if n >= 1_000_000 { 5 } else { 10 };
        let q = bench_us(iters, || {
            std::hint::black_box(tc2_compute_simd(&bid, &ask, &bid_sz, &ask_sz, 100));
        });
        rows.push(Row {
            tc: "TC2-SIMD",
            scale: n,
            build_us: 0.0,
            query_us: q,
            bytes: (n * 32) as f64,
            threshold_us: None,
            note: "OFI deltas via AVX2 (4-way f64) + O(1) msum[100]".into(),
        });
    }

    // ---- TC3: wash-trading cycle detection ----
    // Correctness: the CSR detector (in-loop amount prune + dst binary search)
    // must agree with the reference ring(3) + post-filter count.
    {
        let small = tc3_build(10_000, 20);
        assert_eq!(
            tc3_query_fast(&small),
            tc3_query(&small),
            "TC3 CSR detector != reference ring(3) filter"
        );
    }
    for &n in &[100_000usize, 500_000] {
        let cycles = if n >= 500_000 { 100 } else { 20 };
        let b = tc3_build(n, cycles);
        let matches = tc3_query_fast(&b);
        let iters = if n >= 500_000 { 3 } else { 5 };
        let q = bench_us(iters, || {
            let _ = tc3_query_fast(&b);
        });
        rows.push(Row {
            tc: "TC3",
            scale: n,
            build_us: b.build_us,
            query_us: q,
            bytes: (n * 48) as f64,
            threshold_us: if n == 500_000 { Some(10_000.0) } else { None },
            note: format!("CSR 3-cycle + in-loop amount prune + dst binary search; {} matches", matches),
        });
    }

    // ---- TC4: 512-dim K-NN + temporal volatility ----
    for &n in &[100_000usize, 1_000_000] {
        let dim = 512;
        let b = tc4_build(n, dim);
        let exact_ids = tc4_cpu_knn(&b);
        let iters = if n >= 1_000_000 { 3 } else { 10 };
        let q = bench_us(iters, || {
            let _ = tc4_query(&b);
        });
        rows.push(Row {
            tc: "TC4",
            scale: n,
            build_us: b.build_us,
            query_us: q,
            bytes: (n * dim * 4) as f64,
            threshold_us: Some(8_000.0),
            note: format!("FlatIndex exact 512-dim (CPU AVX2+FMA), top-{} + ±100ms vol", exact_ids.len()),
        });

        // IVF: coarse partition + exact f32 probe scan (no quantization).
        let ivf_b = tc4_ivf_build(n, dim, 1024, 32);
        let ivf_ids = tc4_ivf_query(&ivf_b);
        let recall = recall_at_10(&exact_ids, &ivf_ids);
        let qivf = bench_us(iters, || {
            let _ = tc4_ivf_query_full(&ivf_b);
        });
        rows.push(Row {
            tc: "TC4",
            scale: n,
            build_us: ivf_b.build_us,
            query_us: qivf,
            bytes: (n * dim * 4) as f64,
            threshold_us: Some(8_000.0),
            note: format!("IVF exact-f32 (nlist=1024, nprobe=32), top-10 + vol; recall@10={:.1}%", recall * 100.0),
        });

        // CUDA exact K-NN: fused distance + top-K, column-major coalesced scan.
        #[cfg(feature = "cuda")]
        {
            if use_gpu {
                let mut c = tc4_cuda_build(b.index.data(), &b.query, b.n, b.index.dim());
                assert_eq!(
                    tc4_cuda_query(&mut c, 10),
                    tc4_cpu_knn(&b),
                    "TC4 CUDA top-10 != CPU top-10"
                );
                let qc = bench_us(iters, || {
                    let _ = tc4_query_cuda(&b, &mut c);
                });
                rows.push(Row {
                    tc: "TC4",
                    scale: n,
                    build_us: c.build_us,
                    query_us: qc,
                    bytes: (n * dim * 4) as f64,
                    threshold_us: Some(8_000.0),
                    note: format!("CUDA exact 512-dim, fused dist+top-K (column-major coalesced), top-{} + vol", top_k),
                });
            }
        }
    }

    // ---- TC5: point-in-time snapshot (O(log N) binary slice) ----
    for &n in &[100_000usize, 1_000_000, 5_000_000] {
        let b = tc5_build(n, 128);

        // O(log N) zero-copy binary slice (primary).
        let qb = bench_us(2000, || {
            let _ = tc5_query_bin(&b);
        });
        rows.push(Row {
            tc: "TC5",
            scale: n,
            build_us: 0.0,
            query_us: qb,
            bytes: (n * 16) as f64,
            threshold_us: if n == 5_000_000 { Some(1_000.0) } else { None },
            note: format!("binary slice O(log N) zero-copy; {} active orders", b.active),
        });

        // O(N) zone-map prune (reference).
        let qz = bench_us(2000, || tc5_query(&b));
        rows.push(Row {
            tc: "TC5-zone",
            scale: n,
            build_us: b.build_us,
            query_us: qz,
            bytes: (n * 16) as f64,
            threshold_us: if n == 5_000_000 { Some(1_000.0) } else { None },
            note: format!("zone-map prune (O(N) mask, reference); {} active", b.active),
        });
    }

    // ---- Report ----
    let mut md = String::new();
    md.push_str("# HFT 功能與效能驗證結果\n\n");
    md.push_str(
        "> 依據 `HFT_TESTCASE.md` 五大案例，於 release 模式測量 Latency / Throughput / 邏輯記憶體。\n",
    );
    md.push_str(
        "> 資料為**確定性合成資料**（SplitMix64），schema 完全符合規格；CSV 樣本已存至 `data/`。\n\n",
    );
    md.push_str(&format!(
        "- Data Loader：`ticks.csv` {} 列、`account_transfers.csv` {} 列 → RecordBatch 往返{}。\n",
        ticks.num_rows(),
        transfers.num_rows(),
        if loader_ok { " ✅ OK" } else { " ❌ MISMATCH" }
    ));
    md.push_str(
        "- 說明：TC3/TC4 的 build 為一次性索引建構（不計入查詢門檻）；TC4 CPU 精確路徑為 FlatIndex（AVX2+FMA SIMD + rayon + bounded top-K），IVF 路徑為倒排索引（coarse 1024-cell 分割 + 精確 f32 探測掃描，無量化），CUDA 路徑則為 column-major 合併掃描（fused distance + top-K）。\n",
    );
    md.push_str(&format!(
        "- Rayon 執行緒：{}（可用 `GTV_HFT_THREADS` 環境變數調整，建議 4–8）。\n",
        threads
    ));
    md.push_str(&format!(
        "- TC1 引擎：{}\n\n",
        if use_gpu {
            "CUDA（`--features cuda`，RTX 4060）"
        } else {
            "純 CPU（v3 branchless / v4 payload-decoupling + NT-store）"
        }
    ));

    md.push_str("| TC | 描述 | 規模 | Build | 查詢 Latency | Throughput (rows/s) | 記憶體 (MB) | 門檻 | 結果 |\n");
    md.push_str("|----|------|-----:|------:|-------------:|--------------------:|-----------:|------|------|\n");
    for r in &rows {
        let thr = r.query_us / 1e6; // seconds
        let tput = if thr > 0.0 {
            format!("{:.0}", r.scale as f64 / thr)
        } else {
            "—".into()
        };
        let threshold = match r.threshold_us {
            Some(t) => format!("< {} ms", t / 1000.0),
            None => "—".into(),
        };
        md.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            r.tc,
            r.note,
            r.scale,
            if r.build_us > 0.0 { fmt_ms(r.build_us) } else { "—".into() },
            fmt_ms(r.query_us),
            tput,
            fmt_mb(r.bytes),
            threshold,
            r.pass(),
        ));
    }

    md.push_str("\n## 門檻達成摘要\n\n");
    let threshold_rows: Vec<&Row> = rows.iter().filter(|r| r.threshold_us.is_some()).collect();
    let passed = threshold_rows
        .iter()
        .filter(|r| r.query_us <= r.threshold_us.unwrap())
        .count();
    md.push_str(&format!(
        "- 指定門檻測試：{} 項，通過 {} 項，未通過 {} 項。\n",
        threshold_rows.len(),
        passed,
        threshold_rows.len() - passed
    ));
    for r in &threshold_rows {
        md.push_str(&format!(
            "- {}（{}）：{} {} {}\n",
            r.tc,
            r.scale,
            fmt_ms(r.query_us),
            if r.pass().starts_with('✅') { "≤" } else { ">" },
            fmt_ms(r.threshold_us.unwrap()),
        ));
    }

    std::fs::write(format!("{OUT_DIR}/RESULTS.md"), &md).expect("write results");
    println!("{md}");
}
