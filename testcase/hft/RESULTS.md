# HFT 功能與效能驗證結果

> 依據 `HFT_TESTCASE.md` 五大案例，於 release 模式測量 Latency / Throughput / 邏輯記憶體。
> 資料為**確定性合成資料**（SplitMix64），schema 完全符合規格；CSV 樣本已存至 `data/`。

- Data Loader：`ticks.csv` 10000 列、`account_transfers.csv` 10000 列 → RecordBatch 往返 ✅ OK。
- 說明：TC3/TC4 的 build 為一次性索引建構（不計入查詢門檻）；TC4 CPU 精確路徑為 FlatIndex（AVX2+FMA SIMD + rayon + bounded top-K），IVF 路徑為倒排索引（coarse 1024-cell 分割 + 精確 f32 探測掃描，無量化），CUDA 路徑則為 column-major 合併掃描（fused distance + top-K）。
- Rayon 執行緒：1（可用 `GTV_HFT_THREADS` 環境變數調整，建議 4–8）。
- TC1 引擎：純 CPU（v3 branchless / v4 payload-decoupling + NT-store）

| TC | 描述 | 規模 | Build | 查詢 Latency | Throughput (rows/s) | 記憶體 (MB) | 門檻 | 結果 |
|----|------|-----:|------:|-------------:|--------------------:|-----------:|------|------|
| TC1 | as-of join v3 (bucket + 8k chunk + 8t + branchless/prefetch), 500µs lag | 100000 | — | 1.34 ms | 74546922 | 3.1 | — | — |
| TC1 | as-of join v4 (payload decoupling + NT store), 500µs lag | 100000 | — | 1.36 ms | 73356808 | 3.1 | — | — |
| TC1 | as-of join fused (→ rel-spread 8MB out, no 16MB write), 500µs lag | 100000 | — | 1.16 ms | 86172353 | 3.1 | — | — |
| TC1 | as-of join v3 (bucket + 8k chunk + 8t + branchless/prefetch), 500µs lag | 1000000 | — | 25.09 ms | 39848984 | 30.5 | < 5 ms | ❌ FAIL |
| TC1 | as-of join v4 (payload decoupling + NT store), 500µs lag | 1000000 | — | 34.73 ms | 28789865 | 30.5 | < 5 ms | ❌ FAIL |
| TC1 | as-of join fused (→ rel-spread 8MB out, no 16MB write), 500µs lag | 1000000 | — | 14.20 ms | 70399631 | 30.5 | < 5 ms | ❌ FAIL |
| TC1 | as-of join v3 (bucket + 8k chunk + 8t + branchless/prefetch), 500µs lag | 5000000 | — | 182.06 ms | 27464077 | 152.6 | — | — |
| TC1 | as-of join v4 (payload decoupling + NT store), 500µs lag | 5000000 | — | 158.90 ms | 31465900 | 152.6 | — | — |
| TC1 | as-of join fused (→ rel-spread 8MB out, no 16MB write), 500µs lag | 5000000 | — | 82.82 ms | 60369706 | 152.6 | — | — |
| TC2 | OFI = e·ΔBid − f·ΔAsk, fused rolling msum[100] (no intermediate) | 100000 | — | 611.4 µs | 163551020 | 3.1 | — | — |
| TC2 | OFI = e·ΔBid − f·ΔAsk, fused rolling msum[100] (no intermediate) | 1000000 | — | 5.74 ms | 174245140 | 30.5 | < 2 ms | ❌ FAIL |
| TC2 | OFI = e·ΔBid − f·ΔAsk, fused rolling msum[100] (no intermediate) | 5000000 | — | 53.94 ms | 92687341 | 152.6 | — | — |
| TC2-SIMD | OFI deltas via AVX2 (4-way f64) + O(1) msum[100] | 100000 | — | 986.6 µs | 101362926 | 3.1 | — | — |
| TC2-SIMD | OFI deltas via AVX2 (4-way f64) + O(1) msum[100] | 1000000 | — | 12.65 ms | 79074005 | 30.5 | — | — |
| TC2-SIMD | OFI deltas via AVX2 (4-way f64) + O(1) msum[100] | 5000000 | — | 102.45 ms | 48802497 | 152.6 | — | — |
| TC3 | CSR 3-cycle + in-loop amount prune + dst binary search; 20 matches | 100000 | 5.60 ms | 1.65 ms | 60695264 | 4.6 | — | — |
| TC3 | CSR 3-cycle + in-loop amount prune + dst binary search; 100 matches | 500000 | 37.61 ms | 8.49 ms | 58859946 | 22.9 | < 10 ms | ✅ PASS |
| TC4 | FlatIndex exact 512-dim (CPU AVX2+FMA), top-10 + ±100ms vol | 100000 | 9.0 µs | 29.17 ms | 3428221 | 195.3 | < 8 ms | ❌ FAIL |
| TC4 | IVF exact-f32 (nlist=1024, nprobe=32), top-10 + vol; recall@10=0.0% | 100000 | 10979.69 ms | 2.85 ms | 35064515 | 195.3 | < 8 ms | ✅ PASS |
| TC4 | FlatIndex exact 512-dim (CPU AVX2+FMA), top-10 + ±100ms vol | 1000000 | 0.6 µs | 248.86 ms | 4018294 | 1953.1 | < 8 ms | ❌ FAIL |
| TC4 | IVF exact-f32 (nlist=1024, nprobe=32), top-10 + vol; recall@10=40.0% | 1000000 | 107450.18 ms | 25.52 ms | 39178175 | 1953.1 | < 8 ms | ❌ FAIL |
| TC5 | binary slice O(log N) zero-copy; 100 active orders | 100000 | — | 0.1 µs | 666666666667 | 1.5 | — | — |
| TC5-zone | zone-map prune (O(N) mask, reference); 100 active | 100000 | 121.1 µs | 1.5 µs | 68634179822 | 1.5 | — | — |
| TC5 | binary slice O(log N) zero-copy; 100 active orders | 1000000 | — | 0.3 µs | 3649635036496 | 15.3 | — | — |
| TC5-zone | zone-map prune (O(N) mask, reference); 100 active | 1000000 | 2.43 ms | 9.2 µs | 109134562916 | 15.3 | — | — |
| TC5 | binary slice O(log N) zero-copy; 100 active orders | 5000000 | — | 0.1 µs | 33333333333333 | 76.3 | < 1 ms | ✅ PASS |
| TC5-zone | zone-map prune (O(N) mask, reference); 100 active | 5000000 | 7.90 ms | 50.9 µs | 98256922200 | 76.3 | < 1 ms | ✅ PASS |

## 門檻達成摘要

- 指定門檻測試：11 項，通過 4 項，未通過 7 項。
- TC1（1000000）：25.09 ms > 5.00 ms
- TC1（1000000）：34.73 ms > 5.00 ms
- TC1（1000000）：14.20 ms > 5.00 ms
- TC2（1000000）：5.74 ms > 2.00 ms
- TC3（500000）：8.49 ms ≤ 10.00 ms
- TC4（100000）：29.17 ms > 8.00 ms
- TC4（100000）：2.85 ms ≤ 8.00 ms
- TC4（1000000）：248.86 ms > 8.00 ms
- TC4（1000000）：25.52 ms > 8.00 ms
- TC5（5000000）：0.1 µs ≤ 1.00 ms
- TC5-zone（5000000）：50.9 µs ≤ 1.00 ms
