# HFT 功能與效能驗證結果

> 依據 `HFT_TESTCASE.md` 五大案例，於 release 模式測量 Latency / Throughput / 邏輯記憶體。
> 資料為**確定性合成資料**（SplitMix64），schema 完全符合規格；CSV 樣本已存至 `data/`。

- Data Loader：`ticks.csv` 10000 列、`account_transfers.csv` 10000 列 → RecordBatch 往返 ✅ OK。
- 說明：TC3/TC4 的 build 為一次性索引建構（不計入查詢門檻）；TC4 使用精確 FlatIndex（scalar），512 維大規模 ANN 需另建 HNSW/SIMD。
- Rayon 執行緒：8（可用 `GTV_HFT_THREADS` 環境變數調整，建議 4–8）。
- TC1 引擎：純 CPU（v3 branchless / v4 payload-decoupling + NT-store）

| TC | 描述 | 規模 | Build | 查詢 Latency | Throughput (rows/s) | 記憶體 (MB) | 門檻 | 結果 |
|----|------|-----:|------:|-------------:|--------------------:|-----------:|------|------|
| TC1 | as-of join v3 (bucket + 8k chunk + 8t + branchless/prefetch), 500µs lag | 100000 | — | 558.0 µs | 179198624 | 3.1 | — | — |
| TC1 | as-of join v4 (payload decoupling + NT store), 500µs lag | 100000 | — | 511.0 µs | 195683994 | 3.1 | — | — |
| TC1 | as-of join fused (→ rel-spread 8MB out, no 16MB write), 500µs lag | 100000 | — | 376.5 µs | 265609188 | 3.1 | — | — |
| TC1 | as-of join v3 (bucket + 8k chunk + 8t + branchless/prefetch), 500µs lag | 1000000 | — | 4.56 ms | 219465444 | 30.5 | < 5 ms | ✅ PASS |
| TC1 | as-of join v4 (payload decoupling + NT store), 500µs lag | 1000000 | — | 7.40 ms | 135193981 | 30.5 | < 5 ms | ❌ FAIL |
| TC1 | as-of join fused (→ rel-spread 8MB out, no 16MB write), 500µs lag | 1000000 | — | 2.55 ms | 392506424 | 30.5 | < 5 ms | ✅ PASS |
| TC1 | as-of join v3 (bucket + 8k chunk + 8t + branchless/prefetch), 500µs lag | 5000000 | — | 21.66 ms | 230888813 | 152.6 | — | — |
| TC1 | as-of join v4 (payload decoupling + NT store), 500µs lag | 5000000 | — | 26.39 ms | 189485603 | 152.6 | — | — |
| TC1 | as-of join fused (→ rel-spread 8MB out, no 16MB write), 500µs lag | 5000000 | — | 15.20 ms | 328962280 | 152.6 | — | — |
| TC2 | OFI = e·ΔBid − f·ΔAsk, msum[100] | 100000 | — | 642.3 µs | 155700911 | 3.1 | — | — |
| TC2 | OFI = e·ΔBid − f·ΔAsk, msum[100] | 1000000 | — | 6.61 ms | 151311981 | 30.5 | < 2 ms | ❌ FAIL |
| TC2 | OFI = e·ΔBid − f·ΔAsk, msum[100] | 5000000 | — | 57.45 ms | 87034330 | 152.6 | — | — |
| TC3 | ring(3) + amount<0.1% filter; 20 matches | 100000 | 2.83 ms | 12.73 ms | 7854041 | 4.6 | — | — |
| TC3 | ring(3) + amount<0.1% filter; 100 matches | 500000 | 10.61 ms | 63.10 ms | 7923591 | 22.9 | < 10 ms | ❌ FAIL |
| TC4 | FlatIndex exact 512-dim, top-10 + ±100ms vol | 100000 | 212.9 µs | 36.21 ms | 2761369 | 195.3 | < 8 ms | ❌ FAIL |
| TC4 | FlatIndex exact 512-dim, top-10 + ±100ms vol | 1000000 | 2.02 ms | 383.71 ms | 2606119 | 1953.1 | < 8 ms | ❌ FAIL |
| TC5 | zone-map snapshot; 100 active orders | 100000 | 58.8 µs | 0.7 µs | 141643059490 | 1.5 | — | — |
| TC5 | zone-map snapshot; 100 active orders | 1000000 | 1.07 ms | 4.5 µs | 222766763199 | 15.3 | — | — |
| TC5 | zone-map snapshot; 100 active orders | 5000000 | 5.14 ms | 24.9 µs | 201029269862 | 76.3 | < 1 ms | ✅ PASS |

## 門檻達成摘要

- 指定門檻測試：8 項，通過 3 項，未通過 5 項。
- TC1（1000000）：4.56 ms ≤ 5.00 ms
- TC1（1000000）：7.40 ms > 5.00 ms
- TC1（1000000）：2.55 ms ≤ 5.00 ms
- TC2（1000000）：6.61 ms > 2.00 ms
- TC3（500000）：63.10 ms > 10.00 ms
- TC4（100000）：36.21 ms > 8.00 ms
- TC4（1000000）：383.71 ms > 8.00 ms
- TC5（5000000）：24.9 µs ≤ 1.00 ms
