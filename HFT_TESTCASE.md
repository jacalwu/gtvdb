# gtvdb HFT (High-Frequency Trading) 功能與效能驗證規格書

## 1. 免費高頻 Tick 數據集 (Dataset Sources)

# 高頻交易 (HFT) 免費 Tick Data 數據源與下載指南

本文件整理了 100% 免費、無隱藏收費且可用於量化交易、高頻數據處理與資料庫效能測試的 Tick 級數據源。

---

## 1. 免費 Command-Line 極速下載 (推薦測試首選)

### A. Binance Public Data (加密貨幣，100% 免費、無次數限制)
* **特點**：提供全市場歷史逐筆成交 (`AggTrades`) 與報價快照 (`BookTicker`)，檔案為 Zip 壓縮的 CSV。
* **精確度**：毫秒級 (ms)
* **下載示範 (Linux / macOS / WSL)**：

```bash
# 下載比特幣 (BTCUSDT) 單日逐筆成交 CSV (解壓後約 150MB，~200萬條數據)
curl -O [https://data.binance.vision/data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2024-01-01.zip](https://data.binance.vision/data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2024-01-01.zip)

# 解壓縮
unzip BTCUSDT-trades-2024-01-01.zip


Data Schema (BTCUSDT-trades-*.csv)：trade_id, price, qty, quote_qty, time, is_buyer_maker, is_best_matchB. Dukascopy (外匯 Tick，100% 免費、精準 Bid/Ask)特點：瑞士杜高斯貝銀行官方數據，涵蓋數十種貨幣對 (EURUSD, USDJPY 等) 歷史完整 Tick。精確度：毫秒級 (ms)下載示範 (需 Node.js 環境)：Bash# 一鍵下載 EURUSD 指定日期全天 Tick CSV
npx dukascopy-node -i eurusd -from 2024-01-01 -to 2024-01-02 -timeframe tick -format csv

Data Schema：timestamp, askPrice, bidPrice, askVolume, bidVolume2. 網頁直接下載 & 免費 Sample 數據源市場類型數據源名稱說明與網址精確度下載方式外匯HistDatahistdata.com提供 EUR/USD、GBP/USD 等多年的 Tick CSV。毫秒級網頁直接下載 ZIP美股/期貨FirstRate Datafirstratedata.com提供 AAPL、SPY、QQQ 等標的之免費 Tick 下載頁面。毫秒級免費 Sample 下載美股/期貨Databentodatabento.com註冊即送 $125 美元 額度，可免費下載原始 ITCH/MBP-1 數據。納秒級 (ns)Web 控制台 / Python API港股/美股Kaggle Datasetskaggle.com搜尋 NYSE TAQ Sample 或 HKEX L2 Data (含騰訊 0700.HK)。毫秒/微秒級免費帳號下載 CSV港股HKEX Open Data港交所官方 OMD-C 證券與衍生產品市場 L2/L3 歷史測試報盤數據。微秒級官網免費 Sample 下載3. 券商實時 Tick API (適合動態串流測試)若需要實時 (Real-time) 串流數據進行引擎測試：Interactive Brokers (盈透證券 API)：開戶後透過 Python SDK 使用 reqTickByTickData 獲取美股、港股、期貨的實時逐筆行情。Futu Open API (富途 API)：透過 Python SDK 直接訂閱港股 (如 0700.HK) 及美股 Level 2 逐筆成交與 Order Book 動態快照。4. 測試建議初階段壓測 (100萬 ~ 500萬筆)：建議直接下載 Binance單日 CSV，解壓即用，格式乾淨。高階功能測試 (納秒級對齊/Order Book)：建議申請 Databento 免費試用額度，獲取美股歷史 L2/L3 深度數據。

2. 網頁直接下載 & 免費 Sample 數據源市場類型數據源名稱說明與網址精確度下載方式外匯HistDatahistdata.com提供 EUR/USD、GBP/USD 等多年的 Tick CSV。毫秒級網頁直接下載 ZIP美股/期貨FirstRate Datafirstratedata.com提供 AAPL、SPY、QQQ 等標的之免費 Tick 下載頁面。毫秒級免費 Sample 下載美股/期貨Databentodatabento.com註冊即送 $125 美元 額度，可免費下載原始 ITCH/MBP-1 數據。納秒級 (ns)Web 控制台 / Python API港股/美股Kaggle Datasetskaggle.com搜尋 NYSE TAQ Sample 或 HKEX L2 Data (含騰訊 0700.HK)。毫秒/微秒級免費帳號下載 CSV港股HKEX Open Data港交所官方 OMD-C 證券與衍生產品市場 L2/L3 歷史測試報盤數據。微秒級官網免費 Sample 下載3. 券商實時 Tick API (適合動態串流測試)若需要實時 (Real-time) 串流數據進行引擎測試：Interactive Brokers (盈透證券 API)：開戶後透過 Python SDK 使用 reqTickByTickData 獲取美股、港股、期貨的實時逐筆行情。Futu Open API (富途 API)：透過 Python SDK 直接訂閱港股 (如 0700.HK) 及美股 Level 2 逐筆成交與 Order Book 動態快照。4. 測試建議初階段壓測 (100萬 ~ 500萬筆)：建議直接下載 Binance單日 CSV，解壓即用，格式乾淨。高階功能測試 (納秒級對齊/Order Book)：建議申請 Databento 免費試用額度，獲取美股歷史 L2/L3 深度數據。

---

測試數據下載到 data folder


## 2. 測試數據 Schema 定義 (Arrow RecordBatch)

### A. 行情 Tick 資料表 (`ticks`)
| 欄位名稱 | 數據類型 | 說明 |
|---|---|---|
| `symbol` | `Utf8` | 標的代碼 (例如：`0700.HK`, `AAPL`) |
| `timestamp` | `Timestamp(Nanosecond)` | 成交/報價納秒時間戳 |
| `price` | `Float64` | 最新成交價 |
| `volume` | `UInt64` | 成交量 |
| `bid_price_1` | `Float64` | 買一價 |
| `ask_price_1` | `Float64` | 賣一價 |
| `bid_size_1` | `UInt64` | 買一量 |
| `ask_size_1` | `UInt64` | 賣一量 |

### B. 帳戶關聯圖邊資料表 (`account_transfers`)
| 欄位名稱 | 數據類型 | 說明 |
|---|---|---|
| `src_account` | `UInt64` | 發起交易帳戶 ID |
| `dst_account` | `UInt64` | 接收交易帳戶 ID |
| `valid_from` | `Timestamp(Nanosecond)` | 關聯生效時間 |
| `valid_to` | `Timestamp(Nanosecond)` | 關聯失效時間 |
| `amount` | `Float64` | 資金轉移金額 |

---

## 3. 五大 HFT 測試案例 (Test Cases)

### Test Case 1: 跨資產 As-Of Temporal Join (Lead-Lag 套利特徵)
* **目標**：驗證 `gtvdb` 對於兩檔高度關聯標的（如 `0700.HK` 與 `3690.HK`，或 `SPY` 與 `AAPL`）在異步時間戳下的對齊速度。
* **查詢邏輯**：對 `0700.HK` 的每一筆 Tick，找出 `3690.HK` 在其過去 $T - 500\mu s$ 內最新的 `price` 與 `bid_ask_spread`。
* **通過標準**：100 萬筆數據 Temporal As-Of Join 耗時 **< 5 ms**。

### Test Case 2: 訂單流不平衡度 (OFI) 向量化計算 (Microstructure Feature)
* **目標**：驗證 `gtvdb-core` 使用 Arrow SIMD 運算高頻微觀結構特徵的能力。
* **計算公式**：
  $$\text{OFI}_t = e_t \cdot \Delta \text{BidPrice} - f_t \cdot \Delta \text{AskPrice}$$
  其中根據買賣價變化動態計算 Bid/Ask Size 增量。
* **通過標準**：1,000,000 筆 Tick 數據計算滾動 100-Tick OFI 耗時 **< 2 ms**。

### Test Case 3: 時序圖幌騙 (Spoofing) 與對倒 (Wash Trading) 檢測
* **目標**：驗證 Temporal-CSR 在微秒時間視窗內檢測異常鏈條的能力。
* **查詢邏輯**：在時間視窗 $\Delta T \le 10\text{ms}$ 內，尋找拓撲鏈 $A \rightarrow B \rightarrow C \rightarrow A$ 且資金轉移金額偏差 $< 0.1\%$ 的洗艙循環。
* **通過標準**：於 50 萬節點/邊的動態圖中，精準識別洗艙圖形，查詢 Latency **< 10 ms**。

### Test Case 4: 替換數據 (Alt-Data) 向量新聞與 Tick 行情時間對齊
* **目標**：驗證 Vector K-NN + Temporal 複合過濾。
* **查詢邏輯**：給定一篇新聞的 512 維 Embedding，透過向量索引尋找 Top-10 相似事件，並聯立檢索該新聞發布前後 $100\text{ms}$ 內對應股票的波動率變化。
* **通過標準**：向量檢索 + 時間切片過濾混合查詢 Latency **< 8 ms**。

### Test Case 5: 點時間 (Point-in-Time) 訂單簿快照重建
* **目標**：驗證 Zone Map 剪枝與零分配 Bitmask 在歷史特定時間點 $T_{snapshot}$ 提取全市場有效掛單快照的效能。
* **通過標準**：從 500 萬筆 Tick/Order 紀錄中檢索指定納秒時間點的快照，耗時 **< 1 ms**。

---

## 4. 任務執行清單 (Task Checklist for Claude)

- [ ] 在 `crates/gtvdb-core/benches/` 中建立 `hft_benchmarks.rs`。
- [ ] 撰寫模擬/讀取真實 Tick 數據 CSV 的 Data Loader（轉為 Arrow RecordBatch）。
- [ ] 實現 Test Case 1 至 Test Case 5 的測試代碼。
- [ ] 使用 Criterion 輸出 10 萬、100 萬與 500 萬筆數據規模下的 Latency、Throughput 與 Memory Footprint 報告。