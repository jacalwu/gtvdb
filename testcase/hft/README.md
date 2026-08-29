# gtvdb HFT 功能與效能驗證（TC1–TC5）

依據 `HFT_TESTCASE.md` 的五大高頻交易測試案例，以 release 模式量測
Latency / Throughput / 邏輯記憶體，並將原始數據輸出到 [`RESULTS.md`](RESULTS.md)。

## 執行方式

```sh
cargo run --release -p gtv-cli --example hft_bench
```

執行後會：
1. 產生確定性合成資料（`ticks` / `account_transfers` 兩個 Arrow schema）並寫入 `data/` CSV 樣本；
2. 讀回 CSV → `RecordBatch` 驗證 Data Loader 往返一致；
3. 於 10 萬 / 100 萬 / 500 萬筆規模量測 TC1–TC5；
4. 覆寫 `RESULTS.md`（數據表 + 門檻達成摘要）。

`data/` 內 CSV 為可重生的樣本（由 benchmark 產生），已由 `testcase/.gitignore` 排除。

## 方法論

- **資料**：採用確定性合成資料（SplitMix64 種子 PRNG），schema 完全符合規格。
  未下載 Binance 真實 CSV —— 其 `AggTrades` schema 與規格定義的 `ticks` 八欄不相符，
  且單日解壓後約 150MB；規格清單亦允許「模擬/讀取」。相同演算法可套用到真實 CSV。
- **計時**：沿用 repo 現有 `Instant` micro-benchmark 慣例（`bench_asof` / `bench_temporal`），
  每個案例 warmup 後取多次平均。TC3/TC4 的索引建構（build）與查詢（query）分開計時，
  門檻只對查詢 latency 判定。
- **記憶體**：報表為「邏輯輸入佔用」（rows × bytes/row），非 peak RSS。
- **與清單的差異**：
  - 清單寫 `crates/gtvdb-core/benches/hft_benchmarks.rs`；實際 crate 名為 `gtv-core`，
    且 TC 邏輯跨越 gtv-core / gtv-array / gtv-index / gtv-pattern，其聯集落在 `gtv-cli`，
    故 benchmark 置於 `crates/gtv-cli/examples/hft_bench.rs`。
  - 清單要求 Criterion；本 repo 未引入 Criterion，採一致的 `Instant` 慣例。

## 結果摘要

| TC | 門檻 | 量測 | 結果 |
|----|------|------|------|
| TC1 跨資產 As-Of Join | < 5 ms @ 1M | 71.07 ms | ❌ |
| TC2 OFI 滾動 100 | < 2 ms @ 1M | 20.45 ms | ❌ |
| TC3 洗艙循環檢測 | < 10 ms @ 500k | 142.39 ms | ❌ |
| TC4 512 維 KNN + 時序過濾 | < 8 ms | 100.7 ms @ 100k | ❌ |
| TC5 點時間快照（Zone Map） | < 1 ms @ 5M | 79.3 µs | ✅ |

## 瓶頸診斷與後續方向

- **TC1（As-Of Join，14× 超標）**：目前 `asof_join_f64` 已具備單調雙指針 fast path（O(m+n)），
  但每次 join 產出 `Vec<Option<f64>>`（16 bytes/row）且需做 price + spread 兩次 join。
  改善方向：輸出改為 packed `f64` + 獨立 valid bitmask、單趟合併同時投影多欄、SIMD 化。
- **TC2（OFI，10× 超標）**：`msum` 已是 O(n) 滑動窗口，瓶頸在逐元素 OFI 純量迴圈。
  改善方向：用 Arrow 比較/算術 kernel 做 `ΔBidPrice`/`ΔAskPrice` 的 SIMD 差量，再向量乘加。
- **TC3（洗艙循環，14× 超標）**：`find` 對 50 萬個起點逐一 `find_from`，每個起點都分配
  `DfsState`（3 個 `Vec`）與 neighbor iterator，為 allocation-bound。
  改善方向：免分配 DFS（棧上狀態）、按節點出度剪枝、或對三角計數走專用 join。
- **TC4（向量 KNN，12× 超標）**：`FlatIndex::search_knn` 對全部 N 個向量算距離後
  `collect` + 全排序（O(N log N)），而非 top-K 堆（O(N log K)）；距離計算為 scalar。
  改善方向：top-K 部分選擇、SIMD 距離、或大規模改走 ANN（HNSW；目前 512 維大規模建構過慢）。
- **TC5（點時間快照，✅）**：Zone Map 剪枝已達 79µs @ 5M，符合 < 1ms 門檻，為本次唯一通過項。

## 檔案

- `RESULTS.md` — 由 benchmark 自動產生的完整數據表與門檻摘要。
- `data/ticks.csv`、`data/account_transfers.csv` — 10,000 列 schema 樣本（可重生）。
- `crates/gtv-cli/examples/hft_bench.rs` — 資料產生器、Data Loader 與 TC1–TC5 實作。
