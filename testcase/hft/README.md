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
  warmup 後取 **N 次最小值（min）**——對共享 WSL2 主機的排程器干擾最穩健，代表可達成的下限。
  TC3/TC4 的索引建構（build）與查詢（query）分開計時，門檻只對查詢 latency 判定。
  ⚠️ 絕對 latency 受主機負載影響有 ±2–3× 漂移；**通過/未通過的結論穩定**。
- **記憶體**：報表為「邏輯輸入佔用」（rows × bytes/row），非 peak RSS。
- **與清單的差異**：
  - 清單寫 `crates/gtvdb-core/benches/hft_benchmarks.rs`；實際 crate 名為 `gtv-core`，
    且 TC 邏輯跨越 gtv-core / gtv-array / gtv-index / gtv-pattern，其聯集落在 `gtv-cli`，
    故 benchmark 置於 `crates/gtv-cli/examples/hft_bench.rs`。
  - 清單要求 Criterion；本 repo 未引入 Criterion，採一致的 `Instant` 慣例。

## 結果摘要（tuning2.md 應用後）

| TC | 門檻 | 優化前 | 優化後 | 結果 |
|----|------|-------:|-------:|------|
| TC1 跨資產 As-Of Join | < 5 ms @ 1M | 71.07 ms | 25.63 ms | ❌ |
| TC2 OFI 滾動 100 | < 2 ms @ 1M | 20.45 ms | ~20–54 ms（未改動） | ❌ |
| TC3 洗艙循環檢測 | < 10 ms @ 500k | 142.39 ms | ~352 ms（未改動） | ❌ |
| TC4 512 維 KNN + 時序過濾 | < 8 ms | 100.7 ms @ 100k | ~189 ms（未改動） | ❌ |
| TC5 點時間快照（Zone Map） | < 1 ms @ 5M | 79.3 µs | 50.8 µs | ✅ |

## tuning2.md 應用內容（TC1）

`tuning2.md` 僅詳列 TC1 的重構方案（TC2–TC4 的章節在文件中為空白），已依其要點實作：

1. **單趟多欄投影（Single-pass Multi-column）**：一次雙指針掃描同時鎖定 price 與 spread，
   取代原先兩次獨立的 `asof_join_f64`。
2. **Rayon 區塊平行二分掃描**：左表切 64k 行 chunk，右表以 `partition_point` 二分定位起點，
   各 thread 免鎖 sweep。為此在 workspace 新增 `rayon = "1"` 依賴。
3. **緊密連續記憶體**：直接輸出 `Vec<f64>`（無效值填 `f64::NAN`），消除 `Option` 的 16 bytes/row 開銷。

結果：TC1 從 71.07 ms 降到 25.63 ms（~2.8×），並以 naive 參考實作驗證並行路徑正確性。

**觀察**：並行在單機上只帶來 ~1.6× 加速（22 workers），因為 as-of join 是**記憶體頻寬受限**
（memory-bound）而非 CPU-bound —— 22 條 thread 同時讀寫 48MB 後瓶頸落在記憶體控制器。
距離 < 5 ms 仍差 ~5×，需進一步 SIMD 化或更緊湊的資料佈局（見下）。

## 瓶頸診斷與後續方向

- **TC1（As-Of Join，~5× 超標）**：單趟多欄 + NAN 輸出已實作，剩餘瓶頸為記憶體頻寬與
  資料相依的 `while` 推進（branch）。後續方向：SIMD 同時比對多個 left、prefetch 右表、
  或對齊資料後以無分支 sweep 取代 `while`。
- **TC2（OFI，~10× 超標）**：`msum` 已是 O(n) 滑動窗口，瓶頸在逐元素 OFI 純量迴圈。
  方向：用 Arrow 比較/算術 kernel 做 `ΔBidPrice`/`ΔAskPrice` 的 SIMD 差量，再向量乘加。
- **TC3（洗艙循環，~35× 超標）**：`find` 對 50 萬個起點逐一 `find_from`，每個起點都分配
  `DfsState`（3 個 `Vec`）與 neighbor iterator，為 allocation-bound。
  方向：免分配 DFS（棧上狀態）、按節點出度剪枝、或對三角計數走專用 join。
- **TC4（向量 KNN，~20× 超標）**：`FlatIndex::search_knn` 對全部 N 個向量算距離後
  `collect` + 全排序（O(N log N)），而非 top-K 堆（O(N log K)）；距離計算為 scalar。
  方向：top-K 部分選擇、SIMD 距離、或大規模改走 ANN（HNSW；目前 512 維大規模建構過慢）。
- **TC5（點時間快照，✅）**：Zone Map 剪枝達 50µs @ 5M，符合 < 1 ms 門檻，為唯一通過項。

## 檔案

- `RESULTS.md` — 由 benchmark 自動產生的完整數據表與門檻摘要。
- `data/ticks.csv`、`data/account_transfers.csv` — 10,000 列 schema 樣本（可重生）。
- `crates/gtv-cli/examples/hft_bench.rs` — 資料產生器、Data Loader 與 TC1–TC5 實作（含 TC1 並行優化）。
- `Cargo.toml` / `crates/gtv-cli/Cargo.toml` — 新增 `rayon` workspace 依賴。
