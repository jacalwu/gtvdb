# gtvdb 量化金融功能增強開發路線圖與架構設計規範 (Claude Developer Guide)

> **目標**：本文件作為導向 Claude AI / 開發團隊進行 `gtvdb` 內核擴充與量化金融模組開發的規範手冊。
> 按照優先級與系統相依性，將開發計畫劃分為五大核心階段：**1. 磁碟儲存引擎 (HDB) -> 2. 矩陣計算與進階量化算子 -> 3. 時序圖 (Temporal-CSR) 金融風控 -> 4. 低延遲共享記憶體 IPC -> 5. 原生流式 Pub/Sub 引擎 (Tickerplant)**。

---

## 1. 階段一：磁碟儲存引擎與 kdb+ 風格 HDB 分區架構 (HDB-Style Storage Engine)

### 1.1 核心目標
為 `gtvdb` 建立超大數據集（大於記憶體容量）的按日/標的分區磁碟存儲機制，結合 Apache Arrow/Parquet 與記憶體映射 (`mmap`)，實現冷熱數據無縫查詢與亞毫秒級歷史 Backtest 切片。

### 1.2 目錄規範與物理儲存結構
預設 HDB 根目錄採用 `date/sym` 或 `date/table` 雙層分區結構：
```text
/var/lib/gtvdb/hdb/
├── sym_dictionary.parquet          # 全局 Symbol 枚舉表
├── 2026.08.28/
│   ├── trade/
│   │   ├── AAPL.parquet            # 按標的分區或按 chunk 列存
│   │   └── MSFT.parquet
│   └── quote/
└── 2026.08.29/
    └── trade/
```

### 1.3 核心 Rust Trait 與資料結構
```rust
use std::path::PathBuf;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::SchemaRef;

/// HDB 分區管理器接口
pub trait HdbPartitionManager: Send + Sync {
    /// 載入特定日期與股票代碼的物理數據集 (mmap / Parquet Column Chunk)
    fn load_partition(&self, date: &str, sym: &str) -> Result<Vec<RecordBatch>, GtvdbError>;
    
    /// 執行分區剪枝 (Partition Pruning)，根據 SQL 查詢條件過濾路徑
    fn prune_partitions(&self, start_date: &str, end_date: &str, syms: &[&str]) -> Vec<PathBuf>;
    
    /// 將 Hot Memory 數據洗刷 (Flush) 降級存入 HDB
    fn persist_hot_to_hdb(&self, date: &str, batch: &RecordBatch) -> Result<(), GtvdbError>;
}

/// Zero-Copy mmap 閱讀器封裝
pub struct MmapChunkReader {
    pub file_path: PathBuf,
    pub schema: SchemaRef,
    // 透過 memmap2 crate 提供記憶體映射
}
```

### 1.4 開發任務清單
1. **[HDB-1]** 實作 `MmapChunkReader`，結合 `arrow-parquet` 與 `memmap2`，實現磁碟 Parquet 檔案的高效零拷貝讀取。
2. **[HDB-2]** 於 DataFusion 查詢計劃器中寫入 **Partition Pruning Rule**，解析 SQL 的 `WHERE date >= '...' AND sym IN (...)` 並自動跳過非必要檔案。
3. **[HDB-3]** 實作 `hdb_flush` 背景任務，支援午夜落盤將 Memory Table 自動 Enum 化並寫入對應日期的 HDB 目錄。

---

## 2. 階段二：矩陣計算與進階量化算子 (Matrix & Advanced Quant Operators)

### 2.1 核心目標
基於 Arrow SIMD 陣列原生擴充量化金融矩陣算子、高頻 Orderbook 盤口重建以及期權與風控（Option Greeks & Intra-day VaR）計算。

### 2.2 擴充算子規格
1. **協方差矩陣 (Covariance Matrix)** & **PCA**
   * 函數簽名：`covariance_matrix(returns_matrix) -> Matrix`
   * 採用 SIMD (AVX-512/NEON) 加速收益率矩陣的點積與方差計算。
2. **L2/L3 盤口重建 (Orderbook Reconstruction)**
   * 函數簽名：`reconstruct_l2(mbo_stream) -> (L2_Bid_Ask_Snapshot)`
   * 輸入逐筆委託（MBO：Add/Cancel/Execute），輸出前 N 檔（Depth 10/20）買賣價格與數量。
3. **期權希臘字母 (Option Greeks)**
   * 函數簽名：`bs_greeks(option_type, S, K, T, r, sigma) -> (delta, gamma, vega, theta)`
   * 提供矢量化的 Black-Scholes 模型解析解算子。
4. **日內風險價值 (Intra-day VaR)**
   * 函數簽名：`var_historical(returns_vector, confidence_level)`

### 2.3 Rust 實作範例 (KernelPlan SIMD 優化)
```rust
use arrow::array::Float64Array;

/// SIMD 加速的高頻矢量化 Delta 計算
#[inline(always)]
pub fn calculate_black_scholes_delta_simd(
    s: &Float64Array, 
    k: &Float64Array, 
    t: &Float64Array, 
    r: f64, 
    v: &Float64Array
) -> Float64Array {
    // 使用 std::simd 或 ndarray 進行多通道矢量加速
    // 輸出與 Arrow 長度完全一致的 Float64Array
    todo!()
}
```

### 2.4 開發任務清單
1. **[QUANT-1]** 在 `KernelPlan` 註冊 `covariance_matrix` 與 `pca` UDF，支援多資產收益率相關性分析。
2. **[QUANT-2]** 實作 `reconstruct_l2` 狀態機算子，維護記憶體中的 BTree Orderbook 並實時導出 L2 盤口切片。
3. **[QUANT-3]** 寫入 `bs_greeks` 向量化算子，使 SQL 查詢可以直接計算海量期權持倉的 Delta/Gamma 風險敞口。

---

## 3. 階段三：時序圖 (Temporal-CSR) 金融風控應用 (Financial Risk Graph)

### 3.1 核心目標
利用 `gtvdb` 獨有的 **Temporal-CSR (Compressed Sparse Row)** 數據結構，將交易網絡與資金流向圖進行時間戳標註，實現洗倉（Anti-Wash Trading）與對手方違約傳導的毫秒級走訪。

### 3.2 關鍵圖算子與模式匹配
1. **洗倉交易檢測 (Anti-Wash Trading Detection)**
   * **Pattern**: 尋找時間視窗 $\Delta t$ 內，$A \xrightarrow{t_1} B \xrightarrow{t_2} C \xrightarrow{t_3} A$ 且交易量與金額高度吻合的閉環。
   * 算法：Temporal Path Traverser (邊上有時間約束 $t_1 \le t_2 \le t_3 \le t_1 + \Delta t$)。
2. **對手方風險傳導 (Counterparty Cascade Risk)**
   * 計算當特定金融機構 $V_0$ 違約時，沿著 Temporal Credit Graph 的連鎖反應與暴露敞口。

### 3.3 Temporal-CSR 結構定義
```rust
pub struct TemporalCsrGraph {
    /// 節點偏移量陣列 (CSR Row Offsets)
    pub row_offsets: Vec<usize>,
    /// 目標節點陣列 (CSR Column Indices)
    pub column_indices: Vec<u32>,
    /// 邊上的時間戳標註 (Nanoseconds timestamp)
    pub edge_timestamps: Vec<i64>,
    /// 邊上的交易屬性 (Volume, Price, Amount)
    pub edge_volumes: Vec<f64>,
}

impl TemporalCsrGraph {
    /// 檢測環形對倒交易 (Temporal Cycle Detection)
    pub fn find_wash_trade_cycles(
        &self, 
        max_depth: usize, 
        time_window_ns: i64
    ) -> Vec<Vec<u32>> {
        // 使用 DFS + 時間視窗剪枝走訪 CSR 矩陣
        todo!()
    }
}
```

### 3.4 開發任務清單
1. **[GRAPH-1]** 完善 `TemporalCsrGraph` 構建器，支援從 `RecordBatch` 中的 `(source_acc, target_acc, timestamp, amount)` 直接零拷貝生成 CSR。
2. **[GRAPH-2]** 在 SQL 中擴充 `WASH_TRADE_DETECT(table, window_seconds)` 內建函數。
3. **[GRAPH-3]** 實作對手方風險模擬 API，支援輸入違約節點並回傳傳導路徑與風險暴露總額。

---

## 4. 階段四：低延遲共享記憶體 IPC (Ultra-Low Latency Shm-IPC)

### 4.1 核心目標
構建 C/Rust 原生的共享記憶體 (Shared-Memory IPC) 通訊機制，繞過 POSIX TCP/IP 網絡棧，讓外部極速 Feedhandler 或 FPGA 抓包卡能以 **< 1 微秒** 的極低延遲將行情直接寫入 `gtvdb` 內存。

### 4.2 IPC 物理佈局 (POSIX Shm / memfd)
```text
+-----------------------------------------------------------------------+
|                       Shared Memory RingBuffer                        |
| +------------------+------------------+-----------------------------+ |
| | Header (Atomic)  | Slot 0 (64 bytes)| Slot 1 (64 bytes) ...       | |
| | Head / Tail Pointer| Lock-free Tick   | Lock-free Tick              | |
| +------------------+------------------+-----------------------------+ |
+-----------------------------------------------------------------------+
```

### 4.3 FFI 接口設計 (C/C++ Header Spec)
```c
// gtvdb_shm.h - C/C++ 互操作頭文件

typedef struct {
    int64_t timestamp_ns;
    char symbol[8];
    double price;
    uint32_t size;
    uint8_t side; // 0: Buy, 1: Sell
} gtvdb_tick_t;

// 初始化共享記憶體環形緩衝區
int gtvdb_shm_init(const char* shm_name, size_t buffer_size);

// 寫入單筆 Tick (無鎖極速寫入，微秒級)
int gtvdb_shm_push_tick(gtvdb_tick_t* tick);
```

### 4.4 開發任務清單
1. **[IPC-1]** 基於 Linux `memfd_create` / `shm_open` 實作跨進程 Lock-free RingBuffer。
2. **[IPC-2]** 提供 Rust 封裝 `ShmIngestor`，作為 `gtvdb` 內部的背景 Stream Source，將 Shm 數據直接轉為 Arrow `RecordBatch`。
3. **[IPC-3]** 撰寫 C/C++ FFI 綁定 (`gtvdb_shm.h`) 與基準測試工具，驗證端到端寫入延遲是否降至 1µs 以內。

---

## 5. 階段五：原生流式 Pub/Sub 引擎與 Tickerplant (Native Streaming Engine)

### 5.1 核心目標
將 `gtvdb` 的實時能力提升為 kdb+ Tickerplant 層級的流式數據中心，提供無鎖 WAL (Write-Ahead Log) 預寫日誌、實時 Pub/Sub 廣播與動態滾動視窗（如 1s / 1m K線實時合成）。

### 5.2 系統架構流轉圖
```text
[Shm / Network Feed] ──> [Tickerplant (WAL RingBuffer)]
                                 │
                 ┌───────────────┴───────────────┐
                 ▼                               ▼
       [Async Broadcast IPC]            [In-Memory Hot Engine]
                 │                               │
                 ▼                               ▼
         [Subscriber (GUI/Algo)]        [Dynamic CEP / 1m K-Line]
```

### 5.3 實時 CEP (Complex Event Processing) 視窗計算
```rust
pub struct RealtimeWindowAggregator {
    pub window_size_ns: i64,
    // 儲存目前視窗內的動態 OHLCV 狀態
}

impl RealtimeWindowAggregator {
    /// 當新 Tick 到達時增量更新 1 分鐘 K 線 (無需重算整個表)
    pub fn update_with_tick(&mut self, tick_px: f64, tick_vol: u64, ts: i64) -> Option<BarAggregate> {
        // 增量更新 Open, High, Low, Close, Volume
        todo!()
    }
}
```

### 5.4 開發任務清單
1. **[STREAM-1]** 實作高吞吐 Lock-free WAL，確保在極速行情下所有輸入 Tick 先落盤日誌再進入 memory。
2. **[STREAM-2]** 建立原生訂閱伺服器 (Pub/Sub Router)，支援客戶端透過 `.u.sub` 風格語法訂閱指定 `sym` 的行情流。
3. **[STREAM-3]** 實作 `RealtimeWindowAggregator`，支援實時合成 OHLCV K線與滾動 OFI (Order Flow Imbalance) 指標。

---

## 6. 開發與驗證指南 (Claude Action Items)

### 6.1 代碼風格與質量要求
* **記憶體安全與零拷貝**：優先使用 Arrow Slice 與 Reference，禁止在熱路徑（Hot-path）上進行深拷貝（Deep Copy）或頻繁分配堆記憶體 (`malloc` / `Box::new`)。
* **無鎖架構**：高頻模組（Shm-IPC、WAL、RingBuffer）必須採用 `std::sync::atomic` 原子操作，避免 `Mutex` 造成線程上下文切換。
* **無分支內核 (Branchless)**：算子實作盡量利用 SIMD 或掩碼運算，提高 CPU 內存預取與分支預測成功率。

### 6.2 效能測試標準 (Criterion Benchmarks)
每個增強模組必須附帶 `benches/` 基準測試代碼：
* **HDB 查詢延遲**：10 億條紀錄下分區剪枝與載入時間應 `< 1 ms`。
* **Quant 算子吞吐**：SIMD Delta/Covariance 每秒處理應 `> 5000` 萬個數據點。
* **Shm-IPC 端到端延遲**：寫入至 `gtvdb` 內存的 P99 延遲應 `< 800 ns`。
