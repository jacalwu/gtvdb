# HFT 功能與效能驗證結果

> 依據 `HFT_TESTCASE.md` 五大案例，於 release 模式測量 Latency / Throughput / 邏輯記憶體。
> 資料為**確定性合成資料**（SplitMix64），schema 完全符合規格；CSV 樣本已存至 `data/`。

- Data Loader：`ticks.csv` 10000 列、`account_transfers.csv` 10000 列 → RecordBatch 往返 ✅ OK。
- 說明：TC3/TC4 的 build 為一次性索引建構（不計入查詢門檻）；TC4 使用精確 FlatIndex（scalar），512 維大規模 ANN 需另建 HNSW/SIMD。

| TC | 描述 | 規模 | Build | 查詢 Latency | Throughput (rows/s) | 記憶體 (MB) | 門檻 | 結果 |
|----|------|-----:|------:|-------------:|--------------------:|-----------:|------|------|
| TC1 | parallel single-pass multi-col as-of join, 500µs lag | 100000 | — | 1.31 ms | 76620133 | 3.1 | — | — |
| TC1 | parallel single-pass multi-col as-of join, 500µs lag | 1000000 | — | 25.63 ms | 39021515 | 30.5 | < 5 ms | ❌ FAIL |
| TC1 | parallel single-pass multi-col as-of join, 500µs lag | 5000000 | — | 128.60 ms | 38881625 | 152.6 | — | — |
| TC2 | OFI = e·ΔBid − f·ΔAsk, msum[100] | 100000 | — | 3.15 ms | 31754853 | 3.1 | — | — |
| TC2 | OFI = e·ΔBid − f·ΔAsk, msum[100] | 1000000 | — | 54.09 ms | 18489085 | 30.5 | < 2 ms | ❌ FAIL |
| TC2 | OFI = e·ΔBid − f·ΔAsk, msum[100] | 5000000 | — | 179.00 ms | 27933242 | 152.6 | — | — |
| TC3 | ring(3) + amount<0.1% filter; 20 matches | 100000 | 9.28 ms | 37.70 ms | 2652865 | 4.6 | — | — |
| TC3 | ring(3) + amount<0.1% filter; 100 matches | 500000 | 87.31 ms | 351.94 ms | 1420691 | 22.9 | < 10 ms | ❌ FAIL |
| TC4 | FlatIndex exact 512-dim, top-10 + ±100ms vol | 100000 | 268.0 µs | 189.24 ms | 528442 | 195.3 | < 8 ms | ❌ FAIL |
| TC4 | FlatIndex exact 512-dim, top-10 + ±100ms vol | 1000000 | 3.62 ms | 1646.72 ms | 607268 | 1953.1 | < 8 ms | ❌ FAIL |
| TC5 | zone-map snapshot; 100 active orders | 100000 | 156.7 µs | 1.7 µs | 60350030175 | 1.5 | — | — |
| TC5 | zone-map snapshot; 100 active orders | 1000000 | 2.81 ms | 9.2 µs | 108813928183 | 15.3 | — | — |
| TC5 | zone-map snapshot; 100 active orders | 5000000 | 9.05 ms | 50.8 µs | 98516343861 | 76.3 | < 1 ms | ✅ PASS |

## 門檻達成摘要

- 指定門檻測試：6 項，通過 1 項，未通過 5 項。
- TC1（1000000）：25.63 ms > 5.00 ms
- TC2（1000000）：54.09 ms > 2.00 ms
- TC3（500000）：351.94 ms > 10.00 ms
- TC4（100000）：189.24 ms > 8.00 ms
- TC4（1000000）：1646.72 ms > 8.00 ms
- TC5（5000000）：50.8 µs ≤ 1.00 ms
