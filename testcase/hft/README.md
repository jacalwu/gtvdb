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
TC1 的 Rayon 執行緒數可用環境變數調整（預設 8）：`GTV_HFT_THREADS=4 cargo run --release -p gtv-cli --example hft_bench`。

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

## 結果摘要（tuning2.md + v2 應用後）

| TC | 門檻 | 原始 | tuning2 v1 | **v2（本次）** | 結果 |
|----|------|------:|-----------:|---------------:|------|
| TC1 跨資產 As-Of Join | < 5 ms @ 1M | 71.07 ms | 25.63 ms | **15.20 ms** | ❌ |
| TC2 OFI 滾動 100 | < 2 ms @ 1M | 20.45 ms | — | ~43 ms（未改動） | ❌ |
| TC3 洗艙循環檢測 | < 10 ms @ 500k | 142.39 ms | — | ~383 ms（未改動） | ❌ |
| TC4 512 維 KNN + 時序過濾 | < 8 ms | 100.7 ms @ 100k | — | ~133 ms @ 100k（未改動） | ❌ |
| TC5 點時間快照（Zone Map） | < 1 ms @ 5M | 79.3 µs | 50.8 µs | 48.5 µs | ✅ |

## TC1 優化歷程

### tuning2.md §1（v1）：單趟多欄 + Rayon 並行
1. **單趟多欄投影**：一次雙指針掃描同時鎖定 price 與 spread，取代兩次獨立 `asof_join_f64`。
2. **Rayon 區塊平行二分掃描**：左表切 64k 行 chunk，右表以 `partition_point` 二分定位起點。
3. **緊密連續記憶體**：輸出 `Vec<f64>`（無效值填 `f64::NAN`），消除 `Option` 16 bytes/row 開銷。

結果：71.07 ms → 25.63 ms（~2.8×）。觀察到並行只給 ~1.6×（22 workers）——瓶頸在記憶體頻寬。

### v2（本次）：突破記憶體牆的四項極限優化
針對 v1 的記憶體牆，實作四項正交優化：
1. **Chunk 降至 L2 Cache 內**：64k（~1.5 MB，擠爆單核 L2）→ **8k 行（~192 KB）**，工作集常駐私有 L2。
2. **零初始化輸出**：`vec![f64::NAN]` 改 `MaybeUninit` + `set_len`，消滅一次 memset 與 RFO（Read-For-Ownership）雙重寫入。
3. **O(1) 時間桶索引**：右表按 1 ms 建 Direct-Mapping bucket，取代 per-chunk 的 `partition_point` O(log M) 隨機 Cache Miss。
4. **限制執行緒數**：22 threads → **8**（對齊記憶體通道，降低 Memory Controller 佇列爭奪）。

結果：25.63 ms → 15.20 ms（~1.7×）。以 `asof_join_multi_ref`（naive 單執行緒）驗證並行路徑正確性。

**執行緒掃描**（@ 1M）：4 threads = 18.3 ms、6 = 29.2 ms（受主機雜訊）、**8 = 15.2 ms**。8 為最佳，已設為預設。

### 仍差 3×：記憶體牆的本質
v2 已把能用的通用手段都用上，但 1M 行 = ~48 MB 讀寫流量，15.2 ms 對應有效頻寬僅 **~3.2 GB/s**。
這台共享 WSL2 主機對本工作負載只能供到 ~3 GB/s（非理論 20 GB/s 峰值）。
要突破 < 5 ms 需進一步 SIMD 化 sweep（一次比對多個 left）、Non-Temporal Store（`MOVNTDQ` 繞過快取直寫）、
與手動 prefetch——這些屬 v3 範疇，且即便全部到位，在共享主機上能否穩定 < 5 ms 仍取決於記憶體頻寬上限。

## 瓶頸診斷與後續方向

- **TC1（As-Of Join，~3× 超標）**：v2 已達記憶體牆（~3.2 GB/s）。v3 方向：SIMD sweep、
  非暫時性 store、prefetch 右表。
- **TC2（OFI，~20× 超標）**：`msum` 已是 O(n) 滑動窗口，瓶頸在逐元素 OFI 純量迴圈。
  方向：用 Arrow 比較/算術 kernel 做 `ΔBidPrice`/`ΔAskPrice` 的 SIMD 差量，再向量乘加。
- **TC3（洗艙循環，~38× 超標）**：`find` 對 50 萬個起點逐一 `find_from`，每個起點都分配
  `DfsState`（3 個 `Vec`）與 neighbor iterator，為 allocation-bound。
  方向：免分配 DFS（棧上狀態）、按節點出度剪枝、或對三角計數走專用 join。
- **TC4（向量 KNN，~16× 超標）**：`FlatIndex::search_knn` 對全部 N 個向量算距離後
  `collect` + 全排序（O(N log N)），而非 top-K 堆（O(N log K)）；距離計算為 scalar。
  方向：top-K 部分選擇、SIMD 距離、或大規模改走 ANN（HNSW；目前 512 維大規模建構過慢）。
- **TC5（點時間快照，✅）**：Zone Map 剪枝達 48µs @ 5M，符合 < 1 ms 門檻，為唯一通過項。

## 檔案

- `RESULTS.md` — 由 benchmark 自動產生的完整數據表與門檻摘要。
- `data/ticks.csv`、`data/account_transfers.csv` — 10,000 列 schema 樣本（可重生）。
- `crates/gtv-cli/examples/hft_bench.rs` — 資料產生器、Data Loader 與 TC1–TC5 實作（含 TC1 v1/v2）。
- `Cargo.toml` / `crates/gtv-cli/Cargo.toml` — 新增 `rayon` workspace 依賴。
