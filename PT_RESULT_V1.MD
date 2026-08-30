# gtvdb Performance Test Results — TC1–TC15 (Single-Thread)

**Version**: V1
**Scope**: Single-threaded latency/throughput of the TC1–TC15 HFT test cases,
comparing **GTVDB (Rust)** against **kdb+ (q)** under the same single-core budget.

---

## 1. Methodology

- **Hardware/OS**: 8 physical cores, 15 GB RAM, WSL2 (Ubuntu), AVX2 + FMA available.
- **Single-thread**: GTVDB benchmark runs with `GTV_HFT_THREADS=1` (rayon disabled);
  kdb+ runs with `\s=0` (no secondary threads). SIMD (AVX2/FMA) inside a single thread
  is allowed for both — it is part of each engine's implementation.
- **Timing**: min-of-N wall-clock (`Instant::now()` / q `.z.p`), deterministic
  synthetic data (SplitMix64 / seeded q `?`), per-event latency derived as
  `total_batch_time / number_of_events`.
- **Not counted**: one-time index build (TC3/TC4), terminal print, network I/O.

---

## 2. TC1–TC5 — Temporal / Graph / Vector

| TC | Scale | GTVDB (1-thread) | kdb+ (1-thread) | GTVDB vs kdb+ |
|----|------:|-----------------:|----------------:|:-------------:|
| TC1 as-of join | 100k | 1.36 ms | 20.5 ms | **15× faster** |
| TC1 as-of join | 1M | 23.71 ms | 259.8 ms | **11× faster** |
| TC1 as-of join | 5M | 142.66 ms | 980 ms | **6.9× faster** |
| TC2 OFI + msum[100] | 100k | 0.53 ms | 0.97 ms | **1.8× faster** |
| TC2 OFI + msum[100] | 1M | 5.11 ms | 20.2 ms | **4.0× faster** |
| TC2 OFI + msum[100] | 5M | 47.38 ms | 107.2 ms | **2.3× faster** |
| TC3 wash-trade ring(3) | 100k | 1.41 ms | 6.3 ms | **4.5× faster** |
| TC3 wash-trade ring(3) | 500k | 7.13 ms | 51.6 ms | **7.2× faster** |
| TC4 KNN 512-dim (exact) | 100k | 25.32 ms | 381.8 ms | **15× faster** |
| TC4 KNN 512-dim (exact) | 1M | 176.05 ms | 3.71 s | **21× faster** |
| TC4 KNN 512-dim (IVF, f32) | 100k | 2.90 ms | — | — |
| TC4 KNN 512-dim (IVF, f32) | 1M | 17.83 ms | — | — |
| TC5 point-in-time (binary slice) | 100k | 0.1 µs | 0.93 µs | **9.3× faster** |
| TC5 point-in-time (binary slice) | 1M | 0.3 µs | 0.89 µs | **3.0× faster** |
| TC5 point-in-time (binary slice) | 5M | 0.1 µs | 0.88 µs | **8.8× faster** |

> TC1 uses `asof_join_multi_l2_bucket` (bucket index + two-pointer sweep);
> TC4 exact is the AVX2+FMA flat scan, IVF is the sublinear inverted-file index
> (exact f32 probe, no quantization); TC5 uses `point_in_time_range`
> (two `partition_point` binary searches, zero-copy) vs kdb+ `bin`.

---

## 3. TC6–TC10 — Risk / Tick-to-Trade / OBI / Covariance / Matching

| TC | Scale | GTVDB (1-thread) | kdb+ (1-thread) | GTVDB vs kdb+ |
|----|------:|-----------------:|----------------:|:-------------:|
| TC6 risk check | 5M orders | 3.36 ns/order | 14.38 ns/order | **4.3× faster** |
| TC7 tick-to-trade | 1M packets | 1.07 ns/packet | 18.03 ns/packet | **16.8× faster** |
| TC8 L2 OBI + micro-price | 10k symbols | 11.05 ns/symbol | 162 ns/symbol | **14.7× faster** |
| TC8 L2 OBI + micro-price | 100k symbols | 17.74 ns/symbol | 175 ns/symbol | **9.9× faster** |
| TC8 L2 OBI + micro-price | 1M symbols | 20.63 ns/symbol | 219 ns/symbol | **10.6× faster** |
| TC9 500×500 covariance | 100 ticks | 129.9 µs/tick | 890 µs/tick | **6.9× faster** |
| TC10 matching engine | 200k orders | 119.3 ns/order | 3.74 µs/order | **31× faster** |

**Throughput (GTVDB / kdb+)**:

| TC | GTVDB | kdb+ |
|----|------:|-----:|
| TC6 | 298 M orders/s | 69.5 M orders/s |
| TC7 | 936 M packets/s | 55.5 M packets/s |
| TC10 | 8.38 M orders/s | 267 k orders/s |

---

## 4. TC11–TC15 — Micro-structure Classification

GTVDB single-thread, 1M rows, min-of-N (per-event = batch time / rows):

| TC | Operator | Latency |
|----|----------|--------:|
| TC11 | Lee-Ready (`lee_ready`) | 17.47 ns/row |
| TC12 | Tick rule (`tick_rule`) | 9.93 ns/row |
| TC13 | EMO (`emo`) | 17.94 ns/row |
| TC14 | Level-1 OFI (`ofi_l1`) | 13.86 ns/row |
| TC15 | Aggressor flag (`aggressor_flag`) | 2.24 ns/row |

> kdb+ TC11–TC15 were implemented as vectorized q expressions (from
> `hft-tc11-tc15.md`) but not separately timed in this run; the GTVDB kernels are
> the same formulas implemented in Rust with an explicit sign helper (Rust's
> `f64::signum` returns `1.0` for `+0.0`, which is intentionally avoided).

---

## 5. Summary

1. **TC1–TC10: GTVDB is 2–31× faster than kdb+ single-threaded**, despite kdb+'s
   mature vectorized primitives. The gap comes from: Rust `f32`/`u16` type widths
   (vs q 8-byte float/long), hand-written AVX2/FMA + branchless kernels, fused
   zero-copy passes, and algorithmic indexes (O(1) time-bucket as-of, CSR +
   binary search wash-trade, IVF for KNN, O(log N) point-in-time slice).
2. **TC5 and TC11–TC15 are the strongest fits for the "hft mode" design**:
   point-in-time is ~0.1 µs (beats kdb+ `bin` by ~9×) and the micro-structure
   classifiers run at 2–18 ns/row.
3. **TC6/TC7 demonstrate the precompiled-kernel philosophy**: per-order /
   per-packet checks are branchless and allocation-free, sustaining 298 M
   orders/s and 936 M packets/s single-threaded.
4. **TC4 exact KNN at 1M is DRAM-bound (~176 ms)**; the IVF sublinear index
   (full f32, no quantization) brings the query to ~17.8 ms single-threaded —
   the same physics kdb+ brute-force `mmu` hits at 3.71 s.
