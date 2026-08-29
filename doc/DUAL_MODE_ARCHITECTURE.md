# gtvdb 雙模式架構設計：HFT 模式 vs Analysis 模式

> gtvdb 是單引擎資料庫（Graph + Temporal + Vector + Columnar），以類 SQL 對外。
> 但「亞微秒高頻」與「複雜分析」兩類負載的語法、執行路徑、延遲預算截然不同，
> 因此對外分兩種模式：**HFT 模式**（薄包裝、算子簡稱、極速）與
> **Analysis 模式**（完整 SQL、整合 LLM/Vector/ML/Graph/Risk）。

---

## 1. 雙模式總覽

| 維度 | HFT 模式 | Analysis 模式 |
|------|---------|--------------|
| 定位 | 高頻交易熱路徑（風控/撮合/行情特徵/時序對齊） | 研究、批處理、語意檢索、圖分析、風險歸因 |
| SQL 語法 | 受限子集（SELECT→算子→WHERE/ORDER BY/LIMIT，無 JOIN/GROUP BY/子查詢） | 完整 DataFusion SQL（JOIN/GROUP BY/視窗/CTE/子查詢） |
| 算子命名 | **簡稱**：`aj`/`ofi`/`obi`/`mp`/`knn`/`pit`/`wash`/`risk`/`match`/`cov`/`tt` | **全名**：`asof_join`/`order_flow_imbalance`/`point_in_time`/`wash_trade`/`covariance_matrix`… |
| 執行路徑 | 薄包裝 + 預編譯 KernelPlan（prepare 一次、重複執行零規劃） | DataFusion 完整邏輯→物理→優化器 |
| 延遲預算 | ns–µs（算子即 kernel；規劃成本攤銷後趨近 0） | ms–s（優化器/批次/網路 I/O） |
| 回傳 | 緊湊 Arrow 批次（零拷貝切片） | Arrow RecordBatch / Flight stream |
| 典型場景 | 訂單風控、Tick-to-Trade、L2 因子、撮合 | LLM 語意檢索、向量相似、回歸/協方差、圖走訪、風險歸因 |

**核心原則**：兩種模式**共用同一套 compiled kernel**（gtv-core/array/index/pattern），
差別只在「語法面」與「派送層」。kernel 只實作一次；HFT 模式給它一個最短派送路徑，
Analysis 模式給它完整 SQL 語意與優化器。

---

## 2. HFT 模式（薄包裝、簡稱、極速）

### 2.1 語法子集

```ebnf
stmt     := SELECT expr_list [ FROM source ] [ WHERE predicate ]
            [ ORDER BY col [ASC|DESC] ] [ LIMIT n ] ;
source   := table_name | op_call ;
op_call  := op_name '(' args ')' ;
```

- 不支援 JOIN、GROUP BY、HAVING、CTE、子查詢、DISTINCT、UNION。
- 支援 `WHERE` 的簡單比較／邏輯組合（走 kernel 無分支遮罩）。
- 算子只作用於**具名資源**（已註冊的表/索引/簿/序列），不產生隱式物化。

### 2.2 算子簡稱表（HFT 模式）

| 簡稱 | 全名（Analysis 模式） | 功能 | 底層 kernel |
|------|----------------------|------|-------------|
| `aj`  | `asof_join` | 表對表時序對齊（多欄+容差） | `asof_join_multi_l2_bucket` |
| `wj`  | `window_join` | 視窗聚合 join | window + asof 融合 |
| `msum` | `moving_sum` | 滾動和 | `gtv_array::window::msum` |
| `mavg` | `moving_avg` | 滾動平均 | `gtv_array::window::mavg` |
| `deltas` | `differences` | 前向差分（首元素 0） | `gtv_array::window::deltas` |
| `xbar` | `time_bucket` | 時間分桶 | 整數桶位 O(1) |
| `ofi`  | `order_flow_imbalance` | 訂單流不平衡（融合 + 滾動） | `tc2_compute_fused` |
| `obi`  | `order_book_imbalance` | L2 10 檔不平衡 | SIMD 累加 |
| `mp`   | `micro_price` | 微觀價格 | SIMD 乘加 + 倒數近似 |
| `knn`  | `vector_search` | 向量 Top-K（可選索引） | Flat/IVF/HNSW |
| `pit`  | `point_in_time` | 時點快照（O(log N) 零拷貝） | `point_in_time_range` |
| `wash` | `wash_trade_detect` | 洗艙環 A→B→C→A | CSR 三角 join + 金額剪枝 |
| `risk` | `risk_check` | 風控硬檢查（無分支） | `tc6_compute` |
| `match`| `match_orders` | 本地撮合（Price-Time FIFO） | Slab + IntMap matching |
| `cov`  | `covariance_matrix` | 500×500 流式協方差 | 下三角 rank-1 更新 |
| `tt`   | `time_travel` | 歷史快照讀取 | SnapshotStore |

### 2.3 執行路徑（關鍵）

HFT 模式**不走 DataFusion 完整優化器**（其每次查詢規劃有 µs 級開銷）。改為：

```
SQL(子集) ── 解析一次 ──> KernelPlan(預編譯) ── 重複執行 ──> Arrow 批次
                │                    │
                │ 註冊資源參照        │ 直接呼叫 compiled kernel
                └─ (表/索引/簿/序列)  └─ 零分配、零規劃
```

- `prepare(sql)` 只做一次：把算子名稱 + 參數綁定到已註冊的 kernel 函式指標。
- 執行迴圈：取輸入切片 → 呼叫 kernel → 寫入預分配輸出緩衝。**無 malloc、無
  logical plan、無優化器。**
- MVP 落地：可先以 DataFusion `PreparedStatement`（`SessionContext::prepare`
  後重複 `execute`）達成「規劃一次、執行多次」，熱路徑再替換為輕量
  `KernelPlan` executor。

### 2.4 延遲預算

| 階段 | 成本 |
|------|------|
| prepare（一次） | ~µs–ms（可接受，攤銷） |
| 執行派送 | ~10–100 ns（函式指標 + 切片） |
| kernel 本身 | 由算子決定（ns–µs，與直寫 Rust 相同） |

因此 HFT 模式的每事件延遲 ≈ **kernel 延遲 + ~100 ns 派送**，幾乎等同直寫 Rust。

---

## 3. Analysis 模式（完整 SQL、五大整合）

### 3.1 執行路徑

```
完整 SQL ──> DataFusion 邏輯規劃 ──> 優化器 ──> 物理計畫 ──> 批次執行 ──> RecordBatch/Flight
```

- 完整 `SessionContext`：JOIN/GROUP BY/視窗/CTE/子查詢/UNION。
- 全部算子以 **UDF / UDAF / UDWF / UDTF** 註冊（全名），供 SQL 自由組合。
- 支援 Parquet/CSV 外部表、time-travel、分散式 Flight。

### 3.2 五大整合

| 領域 | SQL 表面 | 底層 |
|------|---------|------|
| **LLM** | `embed(model, text)`、`chunk(text, n)`、`vector('…')`；與 `knn` 串接做 RAG | 內嵌/遠端 embedding + `gtv-index` |
| **Vector search** | `vector_search('emb', query, k [,label])`、hybrid：`WHERE` 過濾 + `knn` | Flat/IVF/HNSW + Arrow bitmask |
| **Machine learning** | `ols(y, x1..xn)`、`corr_matrix('rets')`、`covariance_matrix('rets', 500)`、`pca(...)`；UDAF/UDF | 下三角平坦化 + rayon + 線代 kernel |
| **Graph query** | `neighbors`/`khop`/`pattern_match`（ring/path/diamond）＋ 屬性 join | Temporal-CSR + `gtv-pattern` |
| **Risk 歸因** | `exposure(portfolio, factor_returns)`、`attribution(...)`、`var(...)` | 因子模型 + 協方差 + 分批 UDAF |

### 3.3 範例

```sql
-- LLM + Vector（RAG）
WITH q AS (SELECT embed('bge', '油價對航空股的影響') AS v)
SELECT id, distance
FROM vector_search('news_emb', (SELECT v FROM q), 10);

-- Graph query + 屬性
SELECT p.n1, p.n2, p.n3, n.value
FROM pattern_match('ring3', 150, 10) p
JOIN nodes n ON n.id = p.n1;

-- ML：滾動回歸 + 風險歸因
SELECT t, ols(asset_ret, mkt_ret) OVER (ORDER BY t ROWS 60 PRECEDING) AS beta
FROM returns;

-- Risk：因子協方差 + 組合歸因
SELECT * FROM covariance_matrix('factor_rets', 50);
SELECT attribution(portfolio, 'factor_exposures', 'factor_rets') AS r;
```

---

## 4. 實作方式：同一 kernel，兩個派送層

```
                 ┌───────────────────────────────────────┐
                 │        gtv-core / array / index /     │
                 │        pattern / storage（kernel 唯一）│
                 └───────────────┬───────────────────────┘
                                 │ 薄包裝（&[f64]↔ArrayRef）
          ┌──────────────────────┴───────────────────────┐
          │                                              │
   ┌──────▼──────┐                                ┌──────▼──────────┐
   │  HFT 模式   │                                │  Analysis 模式  │
   │ 子集 parser │                                │ DataFusion ctx  │
   │ KernelPlan  │                                │ UDF/UDAF/UDWF/  │
   │ 簡稱算子    │                                │ UDTF（全名）     │
   │ prepare/exec│                                │ 完整優化器      │
   └─────────────┘                                └─────────────────┘
```

- `gtv-engine` 新增 `HftSession`（薄 executor）與既有 `GtvContext`（DataFusion）。
- **kernel 只寫一次**，兩層各自做 `&[T]` 切片薄包裝，不複製演算法。
- 同名簡稱/全名映射在一張靜態表，HFT 模式收斂到 KernelPlan，Analysis 模式
  收斂到 `ScalarUDF`/`AggregateUDF`/`WindowUDF`/`TableFunctionImpl`。

---

## 5. TC1–TC10 雙模式寫法對照

| TC | HFT 模式（簡稱） | Analysis 模式（全名） |
|----|-----------------|----------------------|
| TC1 | `SELECT * FROM aj('a','b',500000)` | `SELECT * FROM asof_join('a','b',500000)` |
| TC2 | `SELECT t, ofi(bid,ask,bid_sz,ask_sz,100) OVER (ORDER BY t) FROM ticks` | 同左（全名 `order_flow_imbalance`） |
| TC3 | `SELECT * FROM wash('transfers',500,0.001)` | `SELECT * FROM wash_trade('transfers',500,0.001)` |
| TC4 | `SELECT id FROM knn('emb512','q',10)` | `SELECT * FROM vector_search('emb512', 'q', 10) k JOIN prices p ON k.id=p.sym` |
| TC5 | `SELECT * FROM pit('edges',150)` | `SELECT * FROM point_in_time('edges',150)` |
| TC6 | `SELECT count(*) FROM orders WHERE risk(price,qty,mid,smp)` | `SELECT count(*) FROM orders WHERE risk_ok(price,qty,mid,smp)` |
| TC7 | `SELECT * FROM ttrade('ticks')` | `SELECT * FROM tick_to_trade('ticks')` |
| TC8 | `SELECT sym, obi(bid_sz,ask_sz), mp(bid_px,ask_px,bid_sz,ask_sz) FROM book GROUP BY sym` | 同左（全名 `order_book_imbalance`/`micro_price`） |
| TC9 | `SELECT * FROM cov('rets',500)` | `SELECT * FROM covariance_matrix('rets',500)` |
| TC10 | `SELECT * FROM match('book1','orders')` | `SELECT * FROM match_orders('book1','orders')` |

> 註：HFT 模式的 GROUP BY（TC8）屬「受限批次聚合」，由簡稱算子內部完成，
> 不進入通用 GROUP BY 規劃。

---

## 6. 里程碑

1. **M0（現況）**：單一 `GtvContext`，`neighbors/asof_join/knn` + `mavg/msum/deltas`。
2. **M1（kernel 全接 Analysis 模式）**：把 TC1–TC10 算子以全名 UDF/UDAF/UDWF/UDTF
   註冊進 DataFusion，補 `.gtv` 腳本 + golden file。
3. **M2（HFT 模式 executor）**：子集 parser + `KernelPlan` prepare/exec + 簡稱表，
   熱路徑零規劃零分配。
4. **M3（五大整合）**：LLM `embed`、ML `ols/corr/pca`、圖 `pattern_match`、風險
   `attribution/var`，以及 Parquet/CSV 外部表與 Flight 輸出。
5. **M4（長期）**：原生 `ASOF JOIN` planner 下推、zone-map 下推、分散式 HFT 熱備。

---

## 7. 與既有文件的關係

- `doc/HFT_SQL_SURFACE_DESIGN.md` — 每個 TC 的 SQL 簽名、DataFusion 擴充點、
  效能分層（對應本文 Analysis 模式細節）。
- 本文為上層架構，定義**雙模式分界與派送策略**。
