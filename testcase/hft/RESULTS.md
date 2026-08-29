# HFT 功能與效能驗證結果

> 依據 `HFT_TESTCASE.md` 五大案例，於 release 模式測量 Latency / Throughput / 邏輯記憶體。
> 資料為**確定性合成資料**（SplitMix64），schema 完全符合規格；CSV 樣本已存至 `data/`。

- Data Loader：`ticks.csv` 10000 列、`account_transfers.csv` 10000 列 → RecordBatch 往返 ✅ OK。
- 說明：TC3/TC4 的 build 為一次性索引建構（不計入查詢門檻）；TC4 使用精確 FlatIndex（scalar），512 維大規模 ANN 需另建 HNSW/SIMD。
- Rayon 執行緒：8（可用 `GTV_HFT_THREADS` 環境變數調整，建議 4–8）。

| TC | 描述 | 規模 | Build | 查詢 Latency | Throughput (rows/s) | 記憶體 (MB) | 門檻 | 結果 |
|----|------|-----:|------:|-------------:|--------------------:|-----------:|------|------|
| TC1 | as-of join v3 (bucket + 8k chunk + 8t + branchless/prefetch), 500µs lag | 100000 | — | 1.25 ms | 79811581 | 3.1 | — | — |
| TC1 | as-of join v3 (bucket + 8k chunk + 8t + branchless/prefetch), 500µs lag | 1000000 | — | 11.46 ms | 87242640 | 30.5 | < 5 ms | ❌ FAIL |
| TC1 | as-of join v3 (bucket + 8k chunk + 8t + branchless/prefetch), 500µs lag | 5000000 | — | 45.41 ms | 110101156 | 152.6 | — | — |
| TC2 | OFI = e·ΔBid − f·ΔAsk, msum[100] | 100000 | — | 1.48 ms | 67607630 | 3.1 | — | — |
| TC2 | OFI = e·ΔBid − f·ΔAsk, msum[100] | 1000000 | — | 31.36 ms | 31889339 | 30.5 | < 2 ms | ❌ FAIL |
| TC2 | OFI = e·ΔBid − f·ΔAsk, msum[100] | 5000000 | — | 146.31 ms | 34173214 | 152.6 | — | — |
| TC3 | ring(3) + amount<0.1% filter; 20 matches | 100000 | 6.47 ms | 33.63 ms | 2973912 | 4.6 | — | — |
| TC3 | ring(3) + amount<0.1% filter; 100 matches | 500000 | 40.68 ms | 183.54 ms | 2724233 | 22.9 | < 10 ms | ❌ FAIL |
| TC4 | FlatIndex exact 512-dim, top-10 + ±100ms vol | 100000 | 1.35 ms | 121.05 ms | 826119 | 195.3 | < 8 ms | ❌ FAIL |
| TC4 | FlatIndex exact 512-dim, top-10 + ±100ms vol | 1000000 | 2.77 ms | 1050.80 ms | 951652 | 1953.1 | < 8 ms | ❌ FAIL |
| TC5 | zone-map snapshot; 100 active orders | 100000 | 105.8 µs | 1.4 µs | 72046109510 | 1.5 | — | — |
| TC5 | zone-map snapshot; 100 active orders | 1000000 | 1.73 ms | 9.2 µs | 108885017422 | 15.3 | — | — |
| TC5 | zone-map snapshot; 100 active orders | 5000000 | 7.86 ms | 50.3 µs | 99407531115 | 76.3 | < 1 ms | ✅ PASS |

## 門檻達成摘要

- 指定門檻測試：6 項，通過 1 項，未通過 5 項。
- TC1（1000000）：11.46 ms > 5.00 ms
- TC2（1000000）：31.36 ms > 2.00 ms
- TC3（500000）：183.54 ms > 10.00 ms
- TC4（100000）：121.05 ms > 8.00 ms
- TC4（1000000）：1050.80 ms > 8.00 ms
- TC5（5000000）：50.3 µs ≤ 1.00 ms
