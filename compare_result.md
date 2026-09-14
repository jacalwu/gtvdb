# gtvdb 與 DolphinDB / LanceDB / Vespa.ai / TensorDB / kdb+ 功能對比分析

> 本文對比 gtvdb 與五個主流「時序 / 向量 / 多模態」資料庫的功能定位，並分析
> gtvdb 的優勢與不足。屬分析文件，非精確效能競賽（各系統延遲與硬體/部署高度相關）。

---

## 1. 系統定位總覽

| | 定位 | 資料模型 | 語言 | 授權 | 部署 |
|---|---|---|---|---|---|
| **kdb+ (KX)** | 高頻時序（金融） | 欄式 + 內存表 | q（極簡向量語言） | 商業授權（貴） | 單機 + HDB 分區 |
| **DolphinDB** | 時序 + 流式 + 分佈式分析 | 欄式 + 表 | 類 SQL + 函數式 | 商業授權 | 單機 / 叢集 |
| **LanceDB** | 嵌入式多模態向量庫 | 欄式（Lance，Arrow 為底） | Python/Rust/TS + SQL(DataFusion) | Apache 2.0 | 嵌入式（無伺服器） |
| **Vespa.ai** | 大規模即時檢索/推薦 | 文件 + 向量 + 張量 | YQL + 配置 | Apache 2.0 | 分散式（JVM） |
| **TensorDB** | 張量/多模態資料庫（新興） | 張量 + 向量 + 欄式 | SQL/Python | 視具體實作 | 嵌入式/雲 |
| **gtvdb** | 單引擎「圖 + 時序 + 向量 + 欄式」 | 欄式 + Temporal-CSR + 向量索引 | Rust + SQL（DataFusion） | MIT OR Apache-2.0 | 嵌入式 + gRPC |

> 註：TensorDB 屬較新興且有多個同名/近似專案，這裡按其「張量原生 + 向量檢索 +
> 面向 ML/LLM」的普遍定位描述，具體能力隨實作而異。

---

## 2. 功能矩陣

| 能力 | kdb+ | DolphinDB | LanceDB | Vespa.ai | TensorDB | **gtvdb** |
|---|---|---|---|---|---|---|
| 欄式儲存（Arrow 零拷貝） | ✅（自訂） | ✅（自訂） | ✅ | ⚠️（JVM） | ✅ | ✅ |
| 時序 as-of join | ✅ `aj` | ✅ | ❌ | ❌ | ⚠️ | ✅ `aj`/`asof_join` |
| 滾動視窗（mavg/msum/deltas） | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ |
| 微結構/HFT 算子（OFI、micro-price、Lee-Ready、洗艙…） | ✅（自寫 q） | ⚠️（自寫） | ❌ | ❌ | ❌ | ✅ **內建 TC1–TC15** |
| 時序圖（時間旅行 + pattern） | ❌（無圖模型） | ❌ | ❌ | ❌ | ❌ | ✅ Temporal-CSR + ring/path/diamond |
| 向量檢索（精確 / ANN） | ❌（無原生） | ⚠️（有限） | ✅（IVF/HNSW） | ✅（HNSW） | ✅ | ✅ Flat/IVF/HNSW + bitmask |
| 全文檢索（BM25/混合） | ❌ | ⚠️ | ✅ FTS | ✅ 混合排名 | ⚠️ | ❌（缺口） |
| 混合檢索（向量+標量+全文+張量排名） | ❌ | ❌ | ⚠️ | ✅ **強項** | ⚠️ | ⚠️（向量+標量 mask） |
| 張量 / ML 原生 | ❌ | ⚠️（部分 ML） | ❌ | ✅ tensor ranking | ✅ | ❌（缺口） |
| 流式 Pub/Sub（tick） | ✅ tickerplant | ✅ | ❌ | ⚠️ | ❌ | ⚠️（`live` WebSocket 接入，非引擎級 pub/sub） |
| 完整 SQL | ⚠️（q 方言） | ✅ | ✅（DataFusion） | ⚠️（YQL） | ✅ | ✅ DataFusion + 雙模式 |
| 亞微秒熱路徑（預編譯） | ✅（q 直譯） | ⚠️ | ❌ | ❌ | ❌ | ✅ **KernelPlan** |
| 磁盤儲存引擎（分區/HDB） | ✅ HDB | ✅ | ✅（Lance 檔） | ✅ | ⚠️ | ⚠️（Parquet snapshot + 內存） |
| 分佈式 | ⚠️（HDB 分區） | ✅ | ❌（嵌入式） | ✅ | ⚠️ | ⚠️（P5 gRPC + Flight，較薄） |
| 開源 | ❌ | ❌ | ✅ | ✅ | ⚠️ | ✅ |

---

## 3. gtvdb 的優勢

1. **真正的「單引擎四合一」**：Graph + Temporal + Vector + Columnar 在同一個引擎、
   同一份 Arrow 記憶體上完成，無需像一般架構那樣拼 Neo4j + kdb+ + Milvus + ClickHouse。
   Temporal-CSR（時間旅行圖走訪 + pattern matching）這個組合在對比對象中幾乎是獨有。

2. **HFT 算子內建且經基準對照**：TC1–TC15 已做成 SQL 算子（簡稱/全名雙註冊），
   並與 kdb+ 做了單執行緒對照。其中：
   - TC5 點時間快照以 O(log N) 二分切片反超 kdb+ `bin`（~0.1µs vs ~0.88µs）；
   - TC1–TC4、TC6–TC10 在單執行緒下多數以 4–31× 領先 kdb+ 慣用寫法（AVX2/FMA、
     f32/u16 型別、零拷貝、無分支 kernel）。

3. **雙模式 SQL**：`hft` 模式（薄子集 + 簡稱 + 預編譯 KernelPlan，熱路徑零規劃）
   與 `full` 模式（完整 DataFusion：JOIN/GROUP BY/CTE/視窗）。兼顧「ns 級熱路徑」
   與「複雜分析」，是對比對象中少見的設計。

4. **Rust 工程特性**：記憶體安全、無 GC、單一二進位、低依賴、易嵌入（如 LanceDB
   的嵌入式路線），但同時保留了伺服器（gRPC/Flight）能力。

5. **活資料整合開箱即用**：`live`（LSE WebSocket 串流）、`fetch`/`read_tickdata`
   （REST 歷史抓取 + 自動分頁）、`bgload`（檔案後台導入）、`read_csv`/`read_parquet`。

6. **時序圖 + 洗艙檢測**：`wash_trade`/`pattern_match` 直接內建，kdb+/DolphinDB 需
   自寫 join，向量庫（LanceDB/Vespa/TensorDB）完全無此能力。

---

## 4. gtvdb 的不足

1. **成熟度與生態**：相較 kdb+/DolphinDB/Vespa，gtvdb 尚在早期，缺少：
   - 生產級分散式儲存引擎（資料分區、副本、HA、斷點恢復）；
   - 完整的查詢最佳化器與物化視圖/索引管理；
   - 大型社群、文檔、驅動與生態工具（ODBC/JDBC/各語言 SDK）。

2. **全文檢索與混合排名缺失**：無 BM25/全文索引，也無 Vespa 那樣的
   「全文 + 向量 + 標量 + 張量」統一 ranking 表達式。RAG/推薦場景需外接全文檢索。

3. **張量 / ML 原生能力弱**：無原生張量型別、無 tensor ranking、無內建訓練（DolphinDB
   有部分 ML、Vespa 有 tensor ranking、TensorDB 主打張量）。gtvdb 目前只有
   `covariance_matrix`/`pca`（規劃）等級別，LLM/ML 深度整合仍是「Analysis 模式」的
   待辦（M3）。

4. **分佈式較薄**：P5 是 gRPC 派送 + Arrow Flight 傳輸，但非 DolphinDB/Vespa 那種
   完整的分區並行查詢引擎（跨節點 join/聚合/索引分片尚未實現）。

5. **磁盤引擎薄弱**：僅 Parquet 讀寫 + SnapshotStore（時間旅行），無 kdb+ HDB /
   DolphinDB 那種按時間分區的列式磁盤儲存與漸進載入；大於記憶體的資料集無法高效處理。

6. **流式引擎非一等公民**：`live` 是「WebSocket 客戶端 + 週期刷新表」，非 kdb+
   tickerplant / DolphinDB 那樣的內建 Pub/Sub 流式計算引擎（滑動視窗、事件觸發、
   斷線重連的語意都較淺）。

7. **事務與並發**：以分析/查詢為主，無完整 ACID 事務、無多寫者 MVCC（LSM delta 只是
   原型）。

---

## 5. 定位與建議

| 場景 | 更合適的選擇 |
|---|---|
| 金融高頻（as-of/OFI/微結構/撮合/風控）單節點極低延遲 | **gtvdb** / kdb+ / DolphinDB |
| 需「時序 + 圖 + 向量」三者融合（如交易網絡反洗錢、時序知識圖譜） | **gtvdb（獨特）** |
| 大規模即時檢索/推薦（全文+向量+張量混合排名、高 QPS、分散式） | Vespa.ai |
| 嵌入式多模態向量 + SQL + 零拷貝（RAG、本地 AI 應用） | LanceDB / gtvdb |
| 張量/ML 原生工作負載 | TensorDB / Vespa |
| 成熟、金融級、需 HDB 歷史分區與完整流式 tick 架構 | kdb+ / DolphinDB |

**結論**：gtvdb 的核心差異化是「單引擎融合 Graph/Temporal/Vector/Columnar + 內建
HFT 算子 + 雙模式 SQL + Rust 低延遲」，在「時序圖 + 微結構 + 向量」交集上具獨特優勢；
主要短板是成熟度、全文/張量/ML 深度、以及分佈式與磁盤儲存引擎——這些也是從
「功能驗證」走向「生產可用」的關鍵補強方向。
