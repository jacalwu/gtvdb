# HFT 功能與效能驗證結果

> 依據 `HFT_TESTCASE.md` 五大案例，於 release 模式測量 Latency / Throughput / 邏輯記憶體。
> 資料為**確定性合成資料**（SplitMix64），schema 完全符合規格；CSV 樣本已存至 `data/`。

- Data Loader：`ticks.csv` 10000 列、`account_transfers.csv` 10000 列 → RecordBatch 往返 ✅ OK。
- 說明：TC3/TC4 的 build 為一次性索引建構（不計入查詢門檻）；TC4 使用精確 FlatIndex（scalar），512 維大規模 ANN 需另建 HNSW/SIMD。
- Rayon 執行緒：8（可用 `GTV_HFT_THREADS` 環境變數調整，建議 4–8）。
- TC1 引擎：純 CPU（v3 branchless / v4 payload-decoupling + NT-store）

| TC | 描述 | 規模 | Build | 查詢 Latency | Throughput (rows/s) | 記憶體 (MB) | 門檻 | 結果 |
|----|------|-----:|------:|-------------:|--------------------:|-----------:|------|------|
| TC1 | as-of join v3 (bucket + 8k chunk + 8t + branchless/prefetch), 500µs lag | 100000 | — | 1.96 ms | 51020408 | 3.1 | — | — |
| TC1 | as-of join v4 (payload decoupling + NT store), 500µs lag | 100000 | — | 1.90 ms | 52587101 | 3.1 | — | — |
| TC1 | as-of join v3 (bucket + 8k chunk + 8t + branchless/prefetch), 500µs lag | 1000000 | — | 15.99 ms | 62530198 | 30.5 | < 5 ms | ❌ FAIL |
| TC1 | as-of join v4 (payload decoupling + NT store), 500µs lag | 1000000 | — | 25.72 ms | 38877844 | 30.5 | < 5 ms | ❌ FAIL |
| TC1 | as-of join v3 (bucket + 8k chunk + 8t + branchless/prefetch), 500µs lag | 5000000 | — | 68.65 ms | 72828724 | 152.6 | — | — |
| TC1 | as-of join v4 (payload decoupling + NT store), 500µs lag | 5000000 | — | 80.03 ms | 62477676 | 152.6 | — | — |
| TC2 | OFI = e·ΔBid − f·ΔAsk, msum[100] | 100000 | — | 1.89 ms | 52941313 | 3.1 | — | — |
| TC2 | OFI = e·ΔBid − f·ΔAsk, msum[100] | 1000000 | — | 18.24 ms | 54833469 | 30.5 | < 2 ms | ❌ FAIL |
| TC2 | OFI = e·ΔBid − f·ΔAsk, msum[100] | 5000000 | — | 144.58 ms | 34582295 | 152.6 | — | — |
| TC3 | ring(3) + amount<0.1% filter; 20 matches | 100000 | 6.68 ms | 37.56 ms | 2662138 | 4.6 | — | — |
| TC3 | ring(3) + amount<0.1% filter; 100 matches | 500000 | 16.35 ms | 200.03 ms | 2499642 | 22.9 | < 10 ms | ❌ FAIL |
| TC4 | FlatIndex exact 512-dim, top-10 + ±100ms vol | 100000 | 474.0 µs | 84.45 ms | 1184129 | 195.3 | < 8 ms | ❌ FAIL |
| TC4 | FlatIndex exact 512-dim, top-10 + ±100ms vol | 1000000 | 8.60 ms | 919.34 ms | 1087737 | 1953.1 | < 8 ms | ❌ FAIL |
| TC5 | zone-map snapshot; 100 active orders | 100000 | 108.6 µs | 1.6 µs | 64432989691 | 1.5 | — | — |
| TC5 | zone-map snapshot; 100 active orders | 1000000 | 1.89 ms | 9.7 µs | 103594737387 | 15.3 | — | — |
| TC5 | zone-map snapshot; 100 active orders | 5000000 | 10.69 ms | 50.6 µs | 98876760006 | 76.3 | < 1 ms | ✅ PASS |

## 門檻達成摘要

- 指定門檻測試：7 項，通過 1 項，未通過 6 項。
- TC1（1000000）：15.99 ms > 5.00 ms
- TC1（1000000）：25.72 ms > 5.00 ms
- TC2（1000000）：18.24 ms > 2.00 ms
- TC3（500000）：200.03 ms > 10.00 ms
- TC4（100000）：84.45 ms > 8.00 ms
- TC4（1000000）：919.34 ms > 8.00 ms
- TC5（5000000）：50.6 µs ≤ 1.00 ms
