# HFT 功能與效能驗證結果

> 依據 `HFT_TESTCASE.md` 五大案例，於 release 模式測量 Latency / Throughput / 邏輯記憶體。
> 資料為**確定性合成資料**（SplitMix64），schema 完全符合規格；CSV 樣本已存至 `data/`。

- Data Loader：`ticks.csv` 10000 列、`account_transfers.csv` 10000 列 → RecordBatch 往返 ✅ OK。
- 說明：TC3/TC4 的 build 為一次性索引建構（不計入查詢門檻）；TC4 使用精確 FlatIndex（scalar），512 維大規模 ANN 需另建 HNSW/SIMD。

| TC | 描述 | 規模 | Build | 查詢 Latency | Throughput (rows/s) | 記憶體 (MB) | 門檻 | 結果 |
|----|------|-----:|------:|-------------:|--------------------:|-----------:|------|------|
| TC1 | as-of join (price + spread), 500µs lag | 100000 | — | 6.23 ms | 16044451 | 3.1 | — | — |
| TC1 | as-of join (price + spread), 500µs lag | 1000000 | — | 71.07 ms | 14070239 | 30.5 | < 5 ms | ❌ FAIL |
| TC1 | as-of join (price + spread), 500µs lag | 5000000 | — | 438.59 ms | 11400150 | 152.6 | — | — |
| TC2 | OFI = e·ΔBid − f·ΔAsk, msum[100] | 100000 | — | 1.71 ms | 58336999 | 3.1 | — | — |
| TC2 | OFI = e·ΔBid − f·ΔAsk, msum[100] | 1000000 | — | 20.45 ms | 48900025 | 30.5 | < 2 ms | ❌ FAIL |
| TC2 | OFI = e·ΔBid − f·ΔAsk, msum[100] | 5000000 | — | 167.91 ms | 29777877 | 152.6 | — | — |
| TC3 | ring(3) + amount<0.1% filter; 20 matches | 100000 | 2.97 ms | 30.05 ms | 3327467 | 4.6 | — | — |
| TC3 | ring(3) + amount<0.1% filter; 100 matches | 500000 | 34.70 ms | 142.39 ms | 3511423 | 22.9 | < 10 ms | ❌ FAIL |
| TC4 | FlatIndex exact 512-dim, top-10 + ±100ms vol | 100000 | 294.3 µs | 100.70 ms | 993079 | 195.3 | < 8 ms | ❌ FAIL |
| TC4 | FlatIndex exact 512-dim, top-10 + ±100ms vol | 1000000 | 2.55 ms | 1007.71 ms | 992345 | 1953.1 | < 8 ms | ❌ FAIL |
| TC5 | zone-map snapshot; 100 active orders | 100000 | 124.4 µs | 2.1 µs | 48623162227 | 1.5 | — | — |
| TC5 | zone-map snapshot; 100 active orders | 1000000 | 4.66 ms | 37.6 µs | 26622840147 | 15.3 | — | — |
| TC5 | zone-map snapshot; 100 active orders | 5000000 | 13.71 ms | 79.3 µs | 63026314236 | 76.3 | < 1 ms | ✅ PASS |

## 門檻達成摘要

- 指定門檻測試：6 項，通過 1 項，未通過 5 項。
- TC1（1000000）：71.07 ms > 5.00 ms
- TC2（1000000）：20.45 ms > 2.00 ms
- TC3（500000）：142.39 ms > 10.00 ms
- TC4（100000）：100.70 ms > 8.00 ms
- TC4（1000000）：1007.71 ms > 8.00 ms
- TC5（5000000）：79.3 µs ≤ 1.00 ms
