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
    build_zone_maps, temporal_mask_full, temporal_mask_pruned, ZoneMap,
};
use gtv_core::{TemporalCSR, VectorIndex};
use gtv_index::FlatIndex;
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
fn gen_order_flow(n: usize, seed: u64) -> (Vec<f64>, Vec<f64>, Vec<u64>, Vec<u64>) {
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
        bid_sz.push(1 + rng.next_u64() % 10_000);
        ask_sz.push(1 + rng.next_u64() % 10_000);
    }
    (bid, ask, bid_sz, ask_sz)
}

/// Random 512-dim embeddings (unit-agnostic; distances are squared L2).
fn gen_embeddings(n: usize, dim: usize, seed: u64) -> (Vec<u64>, Vec<Vec<f32>>) {
    let mut rng = SplitMix64::new(seed);
    let ids: Vec<u64> = (0..n as u64).collect();
    let mut vectors = Vec::with_capacity(n);
    for _ in 0..n {
        let mut v = Vec::with_capacity(dim);
        for _ in 0..dim {
            v.push((rng.next_u64() >> 40) as f32 / (1u64 << 24) as f32);
        }
        vectors.push(v);
    }
    (ids, vectors)
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

// ---------------------------------------------------------------------------
// Test Case 2 — order-flow imbalance (OFI), rolling 100-tick
// ---------------------------------------------------------------------------

/// OFI_t = bid_size_t · ΔBidPrice_t − ask_size_t · ΔAskPrice_t, then `msum[100]`.
fn tc2_compute(bid: &[f64], ask: &[f64], bid_sz: &[u64], ask_sz: &[u64]) {
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
    let _ = msum(&ofi, 100);
}

// ---------------------------------------------------------------------------
// Test Case 3 — spoofing / wash-trading cycle detection (A→B→C→A)
// ---------------------------------------------------------------------------

struct Tc3Build {
    csr: TemporalCSR,
    pattern: Pattern,
    valid_at: i64,
    amount: HashMap<(u64, u64, i64), f64>,
    build_us: f64,
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
    let mut amount: HashMap<(u64, u64, i64), f64> = HashMap::new();

    const BIG: i64 = 1_000_000_000; // 1s validity -> active for every query we use
    // Chain spine: no back-edges, so no false cycles.
    for i in 0..chain_nodes - 1 {
        let (s, d, f) = (i as u64, i as u64 + 1, i as i64);
        src.push(s);
        dst.push(d);
        vf.push(f);
        vt.push(f + BIG);
        et.push(1);
        amount.insert((s, d, f), 1000.0 + (i % 7) as f64);
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
            amount.insert((s, d, f), 1000.0);
        }
    }

    let valid_at = (chain_nodes - 1) as i64;

    let build_start = Instant::now();
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
        valid_at,
        amount,
        build_us,
    }
}

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
    let (ids, vectors) = gen_embeddings(n, dim, 0x5EED_0004);
    let query: Vec<f32> = gen_embeddings(1, dim, 0x0FF5_0004).1.into_iter().next().unwrap();
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
    let index = FlatIndex::new(ids, vectors).expect("build flat index");
    let build_us = build_start.elapsed().as_secs_f64() * 1e6;

    Tc4Build {
        index,
        query,
        prices,
        n,
        build_us,
    }
}

fn tc4_query(b: &Tc4Build) -> usize {
    let hits = b.index.search_knn(&b.query, 10, None).expect("knn search");
    let hit_ids: Vec<usize> = hits.values().as_ref().iter().map(|&id| id as usize).collect();
    let mut vol = 0.0f64;
    for &i in &hit_ids {
        let lo = i.saturating_sub(100);
        let hi = (i + 100).min(b.n - 1);
        let mean = b.prices[lo..=hi].iter().sum::<f64>() / (hi - lo + 1) as f64;
        let var = b.prices[lo..=hi]
            .iter()
            .map(|&p| (p - mean) * (p - mean))
            .sum::<f64>()
            / (hi - lo + 1) as f64;
        vol += var.sqrt();
    }
    std::hint::black_box(vol);
    hit_ids.len()
}

// ---------------------------------------------------------------------------
// Test Case 5 — point-in-time order-book snapshot via zone-map pruning
// ---------------------------------------------------------------------------

struct Tc5Build {
    zones: Vec<ZoneMap>,
    vf: Vec<i64>,
    vt: Vec<i64>,
    t: i64,
    active: usize,
    build_us: f64,
}

/// `valid_from` ascending with a fixed duration gives strong temporal locality:
/// a single `T` overlaps only a handful of 128-edge chunks.
fn tc5_build(n: usize, chunk: usize) -> Tc5Build {
    const DURATION: i64 = 100;
    let vf: Vec<i64> = (0..n as i64).collect();
    let vt: Vec<i64> = vf.iter().map(|&f| f + DURATION).collect();
    let t = (n as i64) / 2;

    let build_start = Instant::now();
    let zones = build_zone_maps(&vf, &vt, chunk);
    let build_us = build_start.elapsed().as_secs_f64() * 1e6;

    // Correctness: pruned mask must equal a full scan (untimed).
    let full = temporal_mask_full(&vf, &vt, t);
    let pruned = temporal_mask_pruned(&vf, &vt, t, &zones);
    assert_eq!(full, pruned, "zone-map mask diverges from full scan");
    let active = (0..n).filter(|&i| pruned.value(i)).count();

    Tc5Build {
        zones,
        vf,
        vt,
        t,
        active,
        build_us,
    }
}

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
    left: Vec<i64>,
    right: Vec<i64>,
    price: Vec<f64>,
    spread: Vec<f64>,
    tol: i64,
    build_us: f64,
}

/// One-time GPU setup: context, NVRTC compile of `asof_join.cu`, module + function
/// load. Inputs are owned copies so the query can re-upload without borrowing.
#[cfg(feature = "cuda")]
fn tc1_cuda_build(
    left_ts: &[i64],
    right_ts: &[i64],
    right_price: &[f64],
    right_spread: &[f64],
    tolerance_ns: i64,
) -> Tc1Cuda {
    use cudarc::driver::CudaContext;
    use cudarc::nvrtc::compile_ptx;

    let t0 = Instant::now();
    let ctx = CudaContext::new(0).expect("init CUDA context");
    let ptx = compile_ptx(include_str!("asof_join.cu")).expect("NVRTC compile kernel");
    let module = ctx.load_module(ptx).expect("load PTX module");
    let func = module
        .load_function("asof_join_cuda_kernel")
        .expect("load kernel fn");
    Tc1Cuda {
        build_us: t0.elapsed().as_secs_f64() * 1e6,
        ctx,
        func,
        left: left_ts.to_vec(),
        right: right_ts.to_vec(),
        price: right_price.to_vec(),
        spread: right_spread.to_vec(),
        tol: tolerance_ns,
    }
}

/// Full per-query cost: upload all inputs, launch the kernel, download outputs.
#[cfg(feature = "cuda")]
fn tc1_cuda_query(b: &Tc1Cuda) -> (Vec<f64>, Vec<f64>) {
    use cudarc::driver::{LaunchConfig, PushKernelArg};

    let stream = b.ctx.default_stream();
    let left_len = b.left.len();
    let right_len = b.right.len();
    let left_len_i = left_len as i32;
    let right_len_i = right_len as i32;

    let d_left = stream.clone_htod(&b.left).expect("H2D left");
    let d_right = stream.clone_htod(&b.right).expect("H2D right");
    let d_price = stream.clone_htod(&b.price).expect("H2D price");
    let d_spread = stream.clone_htod(&b.spread).expect("H2D spread");
    let mut d_op = stream.alloc_zeros::<f64>(left_len).expect("alloc out");
    let mut d_os = stream.alloc_zeros::<f64>(left_len).expect("alloc out");

    let cfg = LaunchConfig::for_num_elems(left_len as u32);
    let mut lb = stream.launch_builder(&b.func);
    lb.arg(&d_left)
        .arg(&d_right)
        .arg(&d_price)
        .arg(&d_spread)
        .arg(&mut d_op)
        .arg(&mut d_os)
        .arg(&left_len_i)
        .arg(&right_len_i)
        .arg(&b.tol);
    unsafe { lb.launch(cfg) }.expect("launch kernel");

    let op = stream.clone_dtoh(&d_op).expect("D2H out");
    let os = stream.clone_dtoh(&d_os).expect("D2H out");
    (op, os)
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

        #[cfg(feature = "cuda")]
        {
            if use_gpu {
                let c = tc1_cuda_build(&a_times, &b_times, &b_prices, &b_spread, 500_000);
                let (cp, cs) = tc1_cuda_query(&c);
                assert!(f64_slice_eq(&cp, &rp), "TC1 cuda price != reference");
                assert!(f64_slice_eq(&cs, &rs), "TC1 cuda spread != reference");
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

        // CUDA (only when the feature is on and a GPU is present).
        #[cfg(feature = "cuda")]
        {
            if use_gpu {
                let c = tc1_cuda_build(&a_times, &b_times, &b_prices, &b_spread, 500_000);
                let qc = bench_us(iters, || {
                    let _ = tc1_cuda_query(&c);
                });
                rows.push(Row {
                    tc: "TC1",
                    scale: n,
                    build_us: c.build_us,
                    query_us: qc,
                    bytes: (n * 32) as f64,
                    threshold_us: if n == 1_000_000 { Some(5_000.0) } else { None },
                    note: "as-of join CUDA (RTX 4060, per-thread binary search), 500µs lag".into(),
                });
            }
        }
    }

    // ---- TC2: OFI rolling 100 ----
    for &n in &[100_000usize, 1_000_000, 5_000_000] {
        let (bid, ask, bid_sz, ask_sz) = gen_order_flow(n, 0xB1);
        let iters = if n >= 5_000_000 { 3 } else if n >= 1_000_000 { 5 } else { 10 };
        let q = bench_us(iters, || tc2_compute(&bid, &ask, &bid_sz, &ask_sz));
        rows.push(Row {
            tc: "TC2",
            scale: n,
            build_us: 0.0,
            query_us: q,
            bytes: (n * 32) as f64,
            threshold_us: if n == 1_000_000 { Some(2_000.0) } else { None },
            note: "OFI = e·ΔBid − f·ΔAsk, msum[100]".into(),
        });
    }

    // ---- TC3: wash-trading cycle detection ----
    for &n in &[100_000usize, 500_000] {
        let cycles = if n >= 500_000 { 100 } else { 20 };
        let b = tc3_build(n, cycles);
        let matches = tc3_query(&b);
        let iters = if n >= 500_000 { 3 } else { 5 };
        let q = bench_us(iters, || {
            let _ = tc3_query(&b);
        });
        rows.push(Row {
            tc: "TC3",
            scale: n,
            build_us: b.build_us,
            query_us: q,
            bytes: (n * 48) as f64,
            threshold_us: if n == 500_000 { Some(10_000.0) } else { None },
            note: format!("ring(3) + amount<0.1% filter; {} matches", matches),
        });
    }

    // ---- TC4: 512-dim K-NN + temporal volatility ----
    for &n in &[100_000usize, 1_000_000] {
        let dim = 512;
        let b = tc4_build(n, dim);
        let top_k = tc4_query(&b);
        let iters = if n >= 1_000_000 { 2 } else { 3 };
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
            note: format!("FlatIndex exact 512-dim, top-{} + ±100ms vol", top_k),
        });
    }

    // ---- TC5: point-in-time snapshot (zone-map pruning) ----
    for &n in &[100_000usize, 1_000_000, 5_000_000] {
        let b = tc5_build(n, 128);
        let q = bench_us(2000, || tc5_query(&b));
        rows.push(Row {
            tc: "TC5",
            scale: n,
            build_us: b.build_us,
            query_us: q,
            bytes: (n * 16) as f64,
            threshold_us: if n == 5_000_000 { Some(1_000.0) } else { None },
            note: format!("zone-map snapshot; {} active orders", b.active),
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
        "- 說明：TC3/TC4 的 build 為一次性索引建構（不計入查詢門檻）；TC4 使用精確 FlatIndex（scalar），512 維大規模 ANN 需另建 HNSW/SIMD。\n",
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
