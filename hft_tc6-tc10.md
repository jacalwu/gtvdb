# HFT 高頻交易測試案例規範：TC6 – TC10 (HFT_BENCHMARKS_TC6_TC10.md)

本文件定義高頻交易 (HFT) 系統與極速行情數據庫在 Pre-Trade 風控、Tick-to-Trade 報單鏈路、微觀結構因子計算與本地撮合等關鍵場景下的 Benchmark 規範與免費數據源下載指引。

---

## 1. 測試案例 (Test Cases) 規格

### TC6: Pre-Trade 亞微秒級風控檢查 (Pre-Trade Risk Check)
* **測試目標**：在訂單送出至交易所前，完成多重硬性風控檢查，防止胖手指與違規交易。
* **效能指標**：單筆風控判定 Latency **< 200 ns**，吞吐量 **> 5,000,000 orders/sec**。
* **檢查項目**：
  1. 胖手指防護 (Price Banding vs. Mid-price)
  2. 單筆最大下單數量與名義金額 (Max Qty / Notional Limit)
  3. 帳戶 Token Bucket 流速限制 (Rate Limiting)
  4. 自成交風險攔截 (Self-Match Prevention, SMP)
* **核心優化技術**：
  * 全 Bitmask 與無分支邏輯 (`CMOV` 指令)，消除 Branch Misprediction。
  * 數據結構嚴格對齊 64-byte Cache Line，完全常駐 CPU L1d Cache。
  * Zero-Allocation（零堆記憶體分配）。

---

### TC7: Tick-to-Trade 端到端極致延遲 (Tick-to-Trade Benchmark)
* **測試目標**：測量從網路卡 (NIC) 收到行情 UDP 封包，經解包、策略邏輯，至編碼並送出 FIX/Binary 報單封包的全鏈路時間。
* **效能指標**：Wire-to-Wire Latency **< 1.5 µs** (純 CPU) / **< 800 ns** (FPGA 加速卡)。
* **測試情境**：模擬連續 1,000,000 個 Market Data 封包驅動。
* **核心優化技術**：
  * Kernel Bypass (使用 DPDK 或 Solarflare EF_VI)。
  * CPU 核心綁定與輪詢模式 (Isolcpus + Busy Polling)。
  * SBE/FAST 協議零拷貝 (Zero-Copy) 解包與寫入。

---

### TC8: 即時 L2 訂單不平衡度 (OBI) 與 Micro-Price 計算
* **測試目標**：基於 L2 買賣 Top-10 檔位價量，動態計算微觀價格 (Micro-Price) 與訂單流不平衡因子 (OBI)。
* **效能指標**：10,000 個標的批量計算耗時 **< 1.0 µs**。
* **計算公式**：
  $$\text{Micro-Price} = \frac{P_{bid} \cdot Q_{ask} + P_{ask} \cdot Q_{bid}}{Q_{bid} + Q_{ask}}$$
  $$\text{OBI}_{L2} = \frac{\sum_{i=1}^{10} Q_{bid,i} - \sum_{i=1}^{10} Q_{ask,i}}{\sum_{i=1}^{10} Q_{bid,i} + \sum_{i=1}^{10} Q_{ask,i}}$$
* **核心優化技術**：
  * 使用 AVX2 / AVX-512 SIMD 同時處理 4 或 8 個檔位的 `f64` 乘加。
  * 浮點數倒數近似指令 (`_mm256_rcp_pd`) 替換高昂的 CPU 除法運算。

---

### TC9: 高維 Tick 級流式協方差矩陣 (Streaming Covariance Matrix)
* **測試目標**：針對 500 個標的（如 S&P 500 或 Crypto Top 500）的 Tick 級收益率，即時維護 $500 \times 500$ 流式協方差矩陣。
* **效能指標**：單次 Tick 觸發整體矩陣增量更新耗時 **< 2.0 ms**。
* **核心優化技術**：
  * **Welford 單趟 (One-pass) 在線演算法**：避免重複掃描歷史視窗。
  * **下對角矩陣 (Lower Triangular Matrix) 平坦化存儲**：將 $N \times N$ 壓至 $N(N+1)/2$ 陣列，大幅提升 L2/L3 Cache 命中率。
  * **OpenMP / Rayon 並行區塊更新**。

---

### TC10: 本地撮合引擎與隊列優先級模擬 (Local Matching Engine Simulator)
* **測試目標**：本地模擬交易所 Price-Time Priority (FIFO) 撮合邏輯，估算掛單排隊位置與即時成交率。
* **效能指標**：單筆限價單 (Limit Order) / 市價單 (Market Order) 撮合與狀態更新耗時 **< 1.0 µs**。
* **核心優化技術**：
  * **定長 Slab Allocator** 代替系統 `malloc/free`，管理訂單節點記憶體。
  * **IntMap / Array-based SkipList** 實現 $O(1)$ 檔位尋址與 $O(1)$ 雙向鏈表插入/刪除。

---

## 2. 免費高頻 (Tick / L2 / L3) 數據源與下載鏈接

執行上述 TC6–TC10 測試，需要高密度的 Tick / Depth / Orderbook 數據。以下提供免費且可直接下載的開源數據源：

### 1. 加密貨幣全量逐筆與深度數據 (Crypto Tick & L2 Depth)
* **Binance Public Data (官方免費)**
  * **數據內容**：包含全幣種 Tick 級交易 (`trades`)、L2 深度快照 (`depthSnapshots`) 與逐筆報價 (`bookTicker`)。
  * **特點**：CSV / Parquet 格式，每日更新，無頻寬限制。
  * **下載鏈接**：[https://data.binance.vision/](https://data.binance.vision/)
* **Tardis.dev Sample Datasets**
  * **數據內容**：提供 Deribit、BitMEX、Binance 等交易所的 **L3 逐筆訂單流 (MBO/MBP)** 範例數據。
  * **下載鏈接**：[https://tardis.dev/datasets-sample](https://tardis.dev/datasets-sample)

### 2. 傳統金融 Limit Order Book (L2 / L3 股市數據)
* **LOBSTER (Limit Order Book System - Educational)**
  * **數據內容**：學術界標準的 NASDAQ L3 Message 與 Orderbook 數據（包含 AAPL, MSFT, GOOG 等）。
  * **特點**：精確至納秒級 (Nanoseconds) 的逐筆新增、取消、執行與撮合事件。
  * **下載鏈接 (免費 Sample)**：[https://lobsterdata.com/info/DataSamples.php](https://lobsterdata.com/info/DataSamples.php)
* **Databento Sample Data**
  * **數據內容**：NASDAQ TotalView-ITCH (L3 / MBO) 與 CME 期貨深度數據。
  * **下載鏈接**：[https://databento.com/docs/samples](https://databento.com/docs/samples)

### 3. 合成數據生成器 (Synthetic Generator - 零依賴開發測試)
若無外部網路環境，可直接利用 Rust 編寫零分配的合成數據生成器：

```rust
// 產生 1,000,000 筆 L2 行情快照數據供 TC8 / TC10 測試
pub struct SyntheticMarketData {
    pub symbol_id: u32,
    pub bid_prices: [f64; 10],
    pub ask_prices: [f64; 10],
    pub bid_sizes: [f64; 10],
    pub ask_sizes: [f64; 10],
}

pub fn generate_bench_ticks(count: usize) -> Vec<SyntheticMarketData> {
    let mut rng: u64 = 0xdeadbeef;
    (0..count)
        .map(|i| {
            // LCG 快速偽隨機，避免 RNG 庫帶來的額外開銷
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            let base_price = 150.0 + ((rng % 1000) as f64) * 0.01;
            
            let mut ticks = SyntheticMarketData {
                symbol_id: (i % 500) as u32,
                bid_prices: [0.0; 10],
                ask_prices: [0.0; 10],
                bid_sizes: [0.0; 10],
                ask_sizes: [0.0; 10],
            };
            for level in 0..10 {
                ticks.bid_prices[level] = base_price - (level as f64 + 1.0) * 0.01;
                ticks.ask_prices[level] = base_price + (level as f64 + 1.0) * 0.01;
                ticks.bid_sizes[level] = ((rng % 50) + 1) as f64 * 100.0;
                ticks.ask_sizes[level] = ((rng % 50) + 1) as f64 * 100.0;
            }
            ticks
        })
        .collect()
}