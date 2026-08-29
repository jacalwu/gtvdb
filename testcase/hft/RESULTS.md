# HFT 功能與效能驗證結果

> 依據 `HFT_TESTCASE.md` 五大案例，於 release 模式測量 Latency / Throughput / 邏輯記憶體。
> 資料為**確定性合成資料**（SplitMix64），schema 完全符合規格；CSV 樣本已存至 `data/`。

- Data Loader：`ticks.csv` 10000 列、`account_transfers.csv` 10000 列 → RecordBatch 往返 ✅ OK。
- 說明：TC3/TC4 的 build 為一次性索引建構（不計入查詢門檻）；TC4 使用精確 FlatIndex（scalar），512 維大規模 ANN 需另建 HNSW/SIMD。
- Rayon 執行緒：8（可用 `GTV_HFT_THREADS` 環境變數調整，建議 4–8）。
- TC1 引擎：CUDA（`--features cuda`，RTX 4060）

| TC | 描述 | 規模 | Build | 查詢 Latency | Throughput (rows/s) | 記憶體 (MB) | 門檻 | 結果 |
|----|------|-----:|------:|-------------:|--------------------:|-----------:|------|------|
| TC1 | as-of join v3 (bucket + 8k chunk + 8t + branchless/prefetch), 500µs lag | 100000 | — | 1.82 ms | 54907796 | 3.1 | — | — |
| TC1 | as-of join v4 (payload decoupling + NT store), 500µs lag | 100000 | — | 2.71 ms | 36857677 | 3.1 | — | — |
| TC1 | as-of join fused (→ rel-spread 8MB out, no 16MB write), 500µs lag | 100000 | — | 1.21 ms | 82346479 | 3.1 | — | — |
| TC1 | as-of join CUDA merge (resident + fused 8MB out), 500µs lag | 100000 | 260.48 ms | 1.02 ms | 97834435 | 3.1 | — | — |
| TC1 | as-of join v3 (bucket + 8k chunk + 8t + branchless/prefetch), 500µs lag | 1000000 | — | 15.17 ms | 65927601 | 30.5 | < 5 ms | ❌ FAIL |
| TC1 | as-of join v4 (payload decoupling + NT store), 500µs lag | 1000000 | — | 15.24 ms | 65619463 | 30.5 | < 5 ms | ❌ FAIL |
| TC1 | as-of join fused (→ rel-spread 8MB out, no 16MB write), 500µs lag | 1000000 | — | 6.68 ms | 149602596 | 30.5 | < 5 ms | ❌ FAIL |
| TC1 | as-of join CUDA merge (resident + fused 8MB out), 500µs lag | 1000000 | 380.07 ms | 3.52 ms | 284365095 | 30.5 | < 5 ms | ✅ PASS |
| TC1 | as-of join v3 (bucket + 8k chunk + 8t + branchless/prefetch), 500µs lag | 5000000 | — | 74.00 ms | 67568947 | 152.6 | — | — |
| TC1 | as-of join v4 (payload decoupling + NT store), 500µs lag | 5000000 | — | 58.33 ms | 85721809 | 152.6 | — | — |
| TC1 | as-of join fused (→ rel-spread 8MB out, no 16MB write), 500µs lag | 5000000 | — | 42.17 ms | 118563856 | 152.6 | — | — |
| TC1 | as-of join CUDA merge (resident + fused 8MB out), 500µs lag | 5000000 | 384.48 ms | 49.79 ms | 100422024 | 152.6 | — | — |
| TC2 | OFI = e·ΔBid − f·ΔAsk, msum[100] | 100000 | — | 1.24 ms | 80372284 | 3.1 | — | — |
| TC2 | OFI = e·ΔBid − f·ΔAsk, msum[100] | 1000000 | — | 18.00 ms | 55559352 | 30.5 | < 2 ms | ❌ FAIL |
| TC2 | OFI = e·ΔBid − f·ΔAsk, msum[100] | 5000000 | — | 181.54 ms | 27541703 | 152.6 | — | — |
| TC3 | ring(3) + amount<0.1% filter; 20 matches | 100000 | 3.96 ms | 29.57 ms | 3381729 | 4.6 | — | — |
| TC3 | ring(3) + amount<0.1% filter; 100 matches | 500000 | 26.02 ms | 245.62 ms | 2035630 | 22.9 | < 10 ms | ❌ FAIL |
| TC4 | FlatIndex exact 512-dim, top-10 + ±100ms vol | 100000 | 326.0 µs | 79.28 ms | 1261420 | 195.3 | < 8 ms | ❌ FAIL |
| TC4 | FlatIndex exact 512-dim, top-10 + ±100ms vol | 1000000 | 3.47 ms | 1043.08 ms | 958700 | 1953.1 | < 8 ms | ❌ FAIL |
| TC5 | zone-map snapshot; 100 active orders | 100000 | 170.5 µs | 2.2 µs | 44782803403 | 1.5 | — | — |
| TC5 | zone-map snapshot; 100 active orders | 1000000 | 2.64 ms | 13.0 µs | 77160493827 | 15.3 | — | — |
| TC5 | zone-map snapshot; 100 active orders | 5000000 | 12.84 ms | 67.4 µs | 74170770783 | 76.3 | < 1 ms | ✅ PASS |

## 門檻達成摘要

- 指定門檻測試：9 項，通過 2 項，未通過 7 項。
- TC1（1000000）：15.17 ms > 5.00 ms
- TC1（1000000）：15.24 ms > 5.00 ms
- TC1（1000000）：6.68 ms > 5.00 ms
- TC1（1000000）：3.52 ms ≤ 5.00 ms
- TC2（1000000）：18.00 ms > 2.00 ms
- TC3（500000）：245.62 ms > 10.00 ms
- TC4（100000）：79.28 ms > 8.00 ms
- TC4（1000000）：1043.08 ms > 8.00 ms
- TC5（5000000）：67.4 µs ≤ 1.00 ms
