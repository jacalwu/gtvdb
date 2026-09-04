# TC1–TC5 效能測試 — 歷史紀錄與今日對照

> 更新日期：2026-09-04（HKT）
> 本文件彙整這台機器（WSL2，8 實體核心 / 15 GB RAM）在**今天之前**記錄的 TC1–TC5
> 量測結果，並與今天新跑的 REPL 計時（`testcase/run_tc_duration.sh`）做差異比較。

---

## 1. 歷史紀錄（2026-08-29 ~ 08-30）

### 1.1 資料來源

| 檔案 | 記錄日期 | 內容 |
|------|----------|------|
| `testcase/hft/RESULTS.md` | 08-29 23:19 | Rust `hft_bench` 完整原始輸出（`GTV_HFT_THREADS=1`） |
| `testcase/hft/KDB_RESULTS.md` | 08-29 23:19 | kdb+ (q) 對照結果 |
| `testcase/hft/TC1_TUNING.md` | 08-29 20:07 | TC1 各版本迭代歷程（71.07 ms → 2.5 ms） |
| `hft_tc2-tc4_results.md` | 08-29 22:13 | TC2–TC4 優化量測 |
| `PT_RESULT_V1.MD` | 08-30 11:32 | TC1–TC15 單執行緒效能總結（V1，committed） |

方法學（歷史，`hft_bench`）：

- **計時**：`Instant::now()` wall-clock，warmup + min-of-N（與 Rust / q 相同方法）。
- **資料**：SplitMix64 確定性合成資料，規模 100k / 1M / 5M（TC3 到 500k）。
- **執行緒**：`GTV_HFT_THREADS=1`（rayon 關閉；單執行緒內保留 AVX2/FMA SIMD）。
- **不計**：一次性索引建構（TC3/TC4 build）、終端列印、網路 I/O。

### 1.2 GTVDB（Rust）單執行緒 — 摘要（`PT_RESULT_V1.MD`）

| TC | 100k | 1M | 5M / 500k |
|----|-----:|----:|----------:|
| TC1 as-of join | 1.36 ms | 23.71 ms | 142.66 ms (5M) |
| TC2 OFI + msum[100] | 0.53 ms | 5.11 ms | 47.38 ms (5M) |
| TC3 wash-trade ring(3) | 1.41 ms | — | 7.13 ms (500k) |
| TC4 KNN 512-d 精確 | 25.32 ms | 176.05 ms | — |
| TC4 KNN IVF (f32, 無量化) | 2.90 ms | 17.83 ms | — |
| TC5 point-in-time（二分切片） | 0.1 µs | 0.3 µs | 0.1 µs (5M) |

### 1.3 完整變體（`testcase/hft/RESULTS.md`）

| TC | 規模 | 版本 | Latency |
|----|------|------|--------:|
| TC1 | 100k / 1M / 5M | v3 (bucket + branchless/prefetch) | 1.34 / 25.09 / 182.06 ms |
| TC1 | 100k / 1M / 5M | v4 (payload 解耦 + NT store) | 1.36 / 34.73 / 158.90 ms |
| TC1 | 100k / 1M / 5M | fused（零複製 → rel-spread） | 1.16 / 14.20 / 82.82 ms |
| TC2 | 100k / 1M / 5M | OFI 融合 rolling msum[100] | 611.4 µs / 5.74 ms / 53.94 ms |
| TC2-SIMD | 100k / 1M / 5M | AVX2 4-way f64 + O(1) msum | 986.6 µs / 12.65 ms / 102.45 ms |
| TC3 | 100k / 500k | CSR ring(3) + 金額剪枝 | 1.65 ms / 8.49 ms |
| TC4 | 100k / 1M | FlatIndex 精確（AVX2+FMA） | 29.17 ms / 248.86 ms |
| TC4 | 100k / 1M | IVF（nlist=1024, nprobe=32） | 2.85 ms / 25.52 ms |
| TC5 | 100k / 1M / 5M | binary slice O(log N) 零拷貝 | 0.1 / 0.3 / 0.1 µs |
| TC5-zone | 100k / 1M / 5M | zone-map O(N) mask | 1.5 / 9.2 / 50.9 µs |

### 1.4 kdb+ (q) 對照（`KDB_RESULTS.md`，`\s=0` 單執行緒）

| TC | 100k | 1M | 500k / 5M |
|----|-----:|----:|----------:|
| TC1 `aj` | 20.5 ms | 259.8 ms | 980 ms (5M) |
| TC2 OFI + msum[100] | 0.97 ms | 20.2 ms | 107.2 ms (5M) |
| TC3 `lj` triangle join | 6.3 ms | — | 51.6 ms (500k) |
| TC4 `mmu` 精確（chunked） | 381.8 ms | 3.71 s | — |
| TC5 `bin` | 0.93 µs | 0.89 µs | 0.88 µs (5M) |

> 已記錄結論：Rust 單執行緒在 TC1–TC4 比 kdb+ 快 2–21×；TC5 重構為二分切片後反超
> kdb+ `bin` 約 3–9×。TC1 里程碑為 fused 零複製（1M ~2.5 ms）與 CUDA merge-join
> （~3.5 ms），詳見 `testcase/hft/TC1_TUNING.md`。

---

## 2. 今日 REPL 計時（`run_tc_duration.sh`，2026-09-04 19:47）

方法學：驅動預先編譯好的 `target/release/gtv`，以 REPL 內建 `SET DURATION = ON`
計時（每條指令的計算時間，µs）；每項 warmup 1 次 + 量測 5 次，取 min（最佳）與 avg。
資料為 REPL 內建 demo 表（`prices` 6 列、`ticks` 6 列、`edges` 6 邊、`songs` 10 首）。

| TC | operator | REPL 指令 | min (µs) | avg (µs) |
|----|----------|-----------|---------:|---------:|
| tc1 | `aj` | `aj(0, 5, 15, 25);` | 0.888 | 1.915 |
| tc2 | `ofi` | `SELECT t, ofi(bid,ask,bid_sz,ask_sz,100) OVER (ORDER BY t) FROM ticks;` | 0.754 | 1.901 |
| tc3 | `wash` | `wash(500);` | 1.207 | 2.203 |
| tc4 | `knn` | `SELECT id FROM knn('songs','0.1,0.1',3);` | 566.981 | 641.096 |
| tc5 | `pit` | `pit(500);` | 2.269 | 8.272 |

> 附註：REPL 計時本身有抖動（今日多次執行 min 分別為 aj 0.887→3.277→0.888 µs、
> knn 509→430→567 µs），與歷史文件記載的「WSL2 共享主機 ±2× 噪聲」一致。

---

## 3. 差異比較（歷史 vs 今日）

### 3.1 方法學差異（先讀這節）

兩組數字**不是同一個量測對象**，直接比大小會誤導：

| 面向 | 歷史（`hft_bench`） | 今日（`run_tc_duration.sh`） |
|------|--------------------|------------------------------|
| 計時器 | `Instant::now()` wall-clock | REPL `SET DURATION`（單一指令計算時間） |
| 資料規模 | 100k / 1M / 5M 合成列 | demo 表（6–10 列 / 邊 / 首） |
| 量測內容 | 大規模批次延遲（記憶體頻寬牆） | 單一查詢的固定開銷 + 微小資料 |
| 代表性 | 吞吐量 / 可擴展性 | 熱路徑 kernel/SQL 啟動開銷 |

因此：
- 今日 REPL 數字（µs 級）反映的是「單條查詢的啟動 + 執行開銷」，資料量小到可忽略。
- 歷史數字（ms 級）反映的是「百萬列資料的搬移與計算」，是記憶體頻寬牆。
- 數量級差距（3–4 個數量級）來自**資料規模**，不是效能退化或改進。

### 3.2 數值對照（僅供參考）

| TC | 歷史 100k（µs） | 今日 REPL（µs） | 差距 | 可比性 |
|----|----------------:|----------------:|-----:|--------|
| TC1 | 1360 | 0.888 | −1359 | ❌ 不可比（100k vs 6 列） |
| TC2 | 530 | 0.754 | −529 | ❌ 不可比（100k vs 6 列） |
| TC3 | 1410 | 1.207 | −1409 | ❌ 不可比（100k vs 6 邊） |
| TC4 | 25320（精確）/ 2900（IVF） | 566.981 | −2433 ~ −24753 | ❌ 不可比（100k vs 10 首） |
| TC5 | 0.1 | 2.269 | +2.169 | ⚠️ 部分可比（皆為單次 O(log N) 快照查詢） |

### 3.3 解讀

1. **TC1–TC4：不可直接比**。歷史量測的是百萬列吞吐（TC1 1M = 23.71 ms ≈ 23.7 ns/列），
   今日 REPL 量測的是 demo 資料上的單查詢開銷（< 2 µs，被 kernel/SQL 啟動與列印主導）。
   要重現歷史數字仍需 `hft_bench`（`cargo run --release -p gtv-cli --example hft_bench`）。

2. **TC5：唯一可部分對照**。歷史的 0.1 µs 是純 `point_in_time_range` 二分切片（無 I/O）；
   今日 REPL `pit(500)` 的 2.269 µs 是完整 kernel 執行 + 結果格式化的動作時間，
   多出的 ~2.2 µs 即 REPL/kernel 路徑的固定開銷。兩者都證明「點時間快照」是微秒級、
   遠低於 < 1 ms 門檻。

3. **今日 REPL 計時的價值**：作為**快速回歸偵測**（不用重編譯，秒級跑完），用來盯住
   「同一條熱路徑查詢的開銷是否有跳變」，而非取代 `hft_bench` 的規模化基準。
   建議：以 `tc_duration_last.tsv` 為基線，觀察每次執行的 Δ%（例如今日 aj Δ = −72.9%、
   knn Δ = +31.9%，多屬主機噪聲範圍）。

---

## 4. 重現方式

```sh
# 歷史規模化基準（會重新編譯 release 範例）
GTV_HFT_THREADS=1 cargo run --release -p gtv-cli --example hft_bench
#   → 輸出覆寫 testcase/hft/RESULTS.md

# kdb+ 對照
/home/jacal/.kx/bin/q testcase/hft/hft_bench.q

# 今日 REPL 計時（不重新編譯，用預先建好的 gtv）
./testcase/run_tc_duration.sh
```
