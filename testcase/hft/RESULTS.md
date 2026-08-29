# HFT 功能與效能驗證結果

> 依據 `HFT_TESTCASE.md` 五大案例，於 release 模式測量 Latency / Throughput / 邏輯記憶體。
> 資料為**確定性合成資料**（SplitMix64），schema 完全符合規格；CSV 樣本已存至 `data/`。

- Data Loader：`ticks.csv` 10000 列、`account_transfers.csv` 10000 列 → RecordBatch 往返 ✅ OK。
- 說明：TC3/TC4 的 build 為一次性索引建構（不計入查詢門檻）；TC4 使用精確 FlatIndex（scalar），512 維大規模 ANN 需另建 HNSW/SIMD。
- Rayon 執行緒：6（可用 `GTV_HFT_THREADS` 環境變數調整，建議 4–8）。

| TC | 描述 | 規模 | Build | 查詢 Latency | Throughput (rows/s) | 記憶體 (MB) | 門檻 | 結果 |
|----|------|-----:|------:|-------------:|--------------------:|-----------:|------|------|
| TC1 | as-of join v2 (time-bucket + 8k chunk + bounded threads), 500µs lag | 100000 | — | 2.64 ms | 37881801 | 3.1 | — | — |
| TC1 | as-of join v2 (time-bucket + 8k chunk + bounded threads), 500µs lag | 1000000 | — | 29.21 ms | 34235423 | 30.5 | < 5 ms | ❌ FAIL |
| TC1 | as-of join v2 (time-bucket + 8k chunk + bounded threads), 500µs lag | 5000000 | — | 94.11 ms | 53129660 | 152.6 | — | — |
| TC2 | OFI = e·ΔBid − f·ΔAsk, msum[100] | 100000 | — | 1.30 ms | 76780560 | 3.1 | — | — |
| TC2 | OFI = e·ΔBid − f·ΔAsk, msum[100] | 1000000 | — | 35.24 ms | 28376678 | 30.5 | < 2 ms | ❌ FAIL |
| TC2 | OFI = e·ΔBid − f·ΔAsk, msum[100] | 5000000 | — | 228.61 ms | 21871591 | 152.6 | — | — |
| TC3 | ring(3) + amount<0.1% filter; 20 matches | 100000 | 6.09 ms | 58.10 ms | 1721042 | 4.6 | — | — |
| TC3 | ring(3) + amount<0.1% filter; 100 matches | 500000 | 67.27 ms | 203.64 ms | 2455268 | 22.9 | < 10 ms | ❌ FAIL |
| TC4 | FlatIndex exact 512-dim, top-10 + ±100ms vol | 100000 | 232.3 µs | 118.62 ms | 843055 | 195.3 | < 8 ms | ❌ FAIL |
| TC4 | FlatIndex exact 512-dim, top-10 + ±100ms vol | 1000000 | 3.12 ms | 1760.37 ms | 568062 | 1953.1 | < 8 ms | ❌ FAIL |
| TC5 | zone-map snapshot; 100 active orders | 100000 | 201.7 µs | 1.9 µs | 52438384898 | 1.5 | — | — |
| TC5 | zone-map snapshot; 100 active orders | 1000000 | 1.66 ms | 10.0 µs | 99581756622 | 15.3 | — | — |
| TC5 | zone-map snapshot; 100 active orders | 5000000 | 32.96 ms | 55.9 µs | 89506283341 | 76.3 | < 1 ms | ✅ PASS |

## 門檻達成摘要

- 指定門檻測試：6 項，通過 1 項，未通過 5 項。
- TC1（1000000）：29.21 ms > 5.00 ms
- TC2（1000000）：35.24 ms > 2.00 ms
- TC3（500000）：203.64 ms > 10.00 ms
- TC4（100000）：118.62 ms > 8.00 ms
- TC4（1000000）：1760.37 ms > 8.00 ms
- TC5（5000000）：55.9 µs ≤ 1.00 ms
