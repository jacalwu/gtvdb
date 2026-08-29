# gtvdb HFT SQL 表面設計（TC1–TC10）

> 目標：gtvdb 是一個通用單引擎資料庫（Graph + Temporal + Vector + Columnar），
> 用戶應能以「類 SQL」方式實現 kdb+ 可做到的功能，而非直接寫 Rust。
> 本文先盤點現況，再定義 TC1–TC10 的 SQL 表面、擴充點與實作路線。

---

## 1. 現況盤點（已實現 vs 缺口）

### 1.1 已實現的 SQL 表面

gtv-cli 是一個互動式 SQL REPL（DataFusion `SessionContext`，包裝於
`gtv-engine::GtvContext`）。目前已註冊：

| 類別 | 名稱 | 型態 | 對應 primitive |
|------|------|------|----------------|
| Table function (UDTF) | `neighbors(src, valid_at)` | UDTF | `TemporalCSR::neighbors` |
| Table function (UDTF) | `asof_join(t0, t1, …)` | UDTF（左時間為純量參數） | `gtv_array::asof::asof_join_f64` |
| Table function (UDTF) | `knn(name, query, k [, label])` | UDTF | `KnnCollection::search`（精確暴力） |
| Window function (UDWF) | `mavg(x, n)` / `msum(x, n)` / `deltas(x)` | UDWF | `gtv_array::window` |
| 內建表 | `nodes` / `edges` / `prices` | MemTable | demo 資料 |

另有**非 SQL** 的 shell 命令：`knn`（HNSW）、`khop`、`pattern`（ring/path/diamond）、
`tt`（time-travel）、`delta`（LSM）、`udf`（wasm）、`save/load`、`remote`。

### 1.2 缺口（TC1–TC10 直接寫 Rust，未經 SQL）

| TC | 現況 | 缺口 |
|----|------|------|
| TC1 as-of join | `asof_join(t…)` 僅單一 value 欄、無容差、左時間為字面量 | 多欄（price+spread）、容差、表對表 |
| TC2 OFI | `msum/deltas` UDWF 存在 | 缺 `ticks` 表、缺融合 OFI 算子、`deltas[0]` 語意 |
| TC3 洗艙環 | `pattern` 為 shell 命令 | 無 SQL 暴露（ring(3) + 金額 <0.1% 過濾） |
| TC4 KNN | `knn()` 僅精確暴力 | 512 維大語料、HNSW/IVF、波動率 tail join |
| TC5 PIT 快照 | `WHERE valid_from<=T AND T<valid_to` 全掃描 | O(log N) `point_in_time_range` 未暴露 |
| TC6 風控 | 無 | scalar UDF（無分支 kernel） |
| TC7 tick-to-trade | 無 | UDTF（decode→logic→encode kernel） |
| TC8 OBI/micro-price | 無（可用標準 GROUP BY 但慢） | UDAF（compiled） |
| TC9 協方差 | 無 | 有狀態 accumulator / operator |
| TC10 撮合 | 無 | UDTF（matching engine） |

---

## 2. 核心設計原則

1. **SQL 為語意介面，compiled kernel 為效能內核**
   DataFusion 逐列執行有 ~100 ns–1 µs 的固定開銷。TC6（<200 ns/order）、
   TC7（<1.5 µs/packet）、TC10（<1 µs/order）這類「每事件延遲」目標，**無法靠
   逐列 SQL 達成**。正確分層是：SQL 運算子（UDF/UDAF/UDTF）內部直接呼叫
   gtv-core 的 compiled Rust kernel（無分支、SIMD、零拷貝），SQL 只負責調度與
   組裝。效能測試測的是 kernel，SQL 測試測的是可用性與結果正確性。

2. **集合式運算走宣告式 SQL，狀態式運算走 named operator**
   - 集合式（TC1/TC2/TC3/TC4/TC5/TC8）→ 標準 SQL + 自訂 table/window/aggregate。
   - 狀態式（TC7/TC9/TC10）→ 「註冊資源 + table function」：在 `GtvContext`
     註冊一個具名資源（book / covariance / series），UDTF 以名稱參照。

3. **沿用既有「註冊資源」模式**
   `GtvContext` 已用此模式註冊 `knn` collection 與 `asof_join` 右序列。擴充為：
   具名索引（KNN）、具名訂單簿（matching）、具名收益序列（covariance）。

---

## 3. TC → SQL 映射總表

| TC | 目標 SQL | 型態 | 內部 primitive |
|----|----------|------|----------------|
| TC1 | `SELECT * FROM asof_join('left','right',500000)` | UDTF（具名雙表） | `asof_join` bucket/雙指針 |
| TC2 | `SELECT t, ofi(bid,ask,bid_sz,ask_sz,100) OVER (ORDER BY t) FROM ticks` | UDWF | `gtv_array::window` 融合 OFI |
| TC3 | `SELECT * FROM wash_trade('transfers', T, 0.001)` | UDTF | CSR 三角 join + 金額剪枝 |
| TC4 | `SELECT * FROM knn('emb512', 'q', 10)` ＋ 波動率 join | UDTF（具名索引） | Flat/IVF/HNSW |
| TC5 | `SELECT * FROM point_in_time('edges', T)` | UDTF | `point_in_time_range`（O(log N)） |
| TC6 | `SELECT count(*) FROM orders WHERE risk_ok(price,qty,mid,smp)` | Scalar UDF | 無分支 kernel |
| TC7 | `SELECT * FROM tick_to_trade('ticks')` | UDTF | compiled decode→encode |
| TC8 | `SELECT sym, obi(bid_sz,ask_sz), micro(bid_px,ask_px,bid_sz,ask_sz) FROM book GROUP BY sym` | UDAF | compiled 10-level 累加 |
| TC9 | `SELECT * FROM covariance('rets', 500)` | UDTF（有狀態） | rank-1 下三角更新 |
| TC10 | `SELECT * FROM match_orders('orders')` | UDTF（具名簿） | Slab + IntMap matching engine |

---

## 4. 各 TC 設計細節

### 4.1 TC1 — as-of join（表對表、多欄、容差）

**現況**：`asof_join(t0, t1, …)` 左時間是字面量、右序列單欄、無容差。

**設計**：
```sql
-- 註冊兩個表後，以具名資源做 as-of join
SELECT * FROM asof_join('a', 'b', 500000);
-- 或進階（長期）：原生 ASOF JOIN 語法
SELECT * FROM a ASOF JOIN b
  ON a.sym = b.sym AND a.t >= b.t
  WITHIN 500000;
```
- **MVP**：`register_asof_join(name, right_times, right_price, right_spread, tol)`，
  UDTF `asof_join('left_table','right_name', tol)` 內部走
  `asof_join_multi_l2_bucket`（O(1) time-bucket + 雙指針 + 非時序寫入）。
- **長期**：DataFusion planner 擴充 `ASOF JOIN`（`UserDefinedLogicalNode`），
  讓 join 成為一等公民（可被 optimizer 重排、下推）。

### 4.2 TC2 — OFI（融合 window UDF）

**現況**：`msum/deltas` 已存在，但 `msum(deltas(bid)*… ,100)` 會遇到**巢狀 window
函數**限制，且 `deltas[0]` 語意與 OFI 的 `Δ[0]=0` 不符。

**設計**：新增單一融合 UDWF，避免巢狀：
```sql
SELECT t, ofi(bid, ask, bid_sz, ask_sz, 100) OVER (ORDER BY t) FROM ticks;
```
- 實作：`ofi` UDWF 在 `evaluate_all` 一次 pass 內完成
  `OFI_t = bid_sz·Δbid − ask_sz·Δask` → `msum[100]`，內部就是
  `hft_bench.rs::tc2_compute_fused` 的 kernel（分塊重疊 + 環形緩衝 + NT store）。
- 同時修正 `deltas` UDWF 使其首元素為 `0`（對齊 kdb `0f, 1_x - -1_x`）。

### 4.3 TC3 — 洗艙環路（pattern table function）

**現況**：`gtv_pattern::find`（ring/path/diamond）只經 shell `pattern` 命令。

**設計**：
```sql
-- 泛用 pattern
SELECT * FROM pattern_match('ring3', T, 10);
-- 專用洗艙（ring(3) + 金額偏差 < 0.1%）
SELECT * FROM wash_trade('transfers', T, 0.001, 1000000);
```
- 實作：`wash_trade` UDTF 包裝 `WashTradeDetector::detect`（CSR + 金額早停 +
  dst binary search + 零分配），回傳 `(a,b,c)` 列。
- 輸出為 table，可供後續 SQL 再 join 帳戶屬性。

### 4.4 TC4 — 512 維 KNN + 時序波動率

**現況**：`knn()` 僅 `KnnCollection` 精確暴力；無 HNSW/IVF 註冊、無大維度語料。

**設計**：
```sql
-- 註冊時選索引類型
--   register_vector_index('emb512', ids, data, dim, kind='ivf', nlist=1024, nprobe=32)
SELECT id, distance FROM knn('emb512', '0.1,0.2,…(512)', 10);
-- 波動率 tail：join 價格序列後做 ±100ms 視窗聚合
SELECT k.id, vol(p.price) OVER (PARTITION BY k.id ORDER BY p.t ROWS BETWEEN 100 PRECEDING AND 100 FOLLOWING)
FROM knn('emb512', '…', 10) k JOIN prices p ON k.id = p.sym;
```
- 實作：把 `FlatIndex` / `IvfIndex` / `HnswIndex` 統一為
  `enum VectorIndexKind`，註冊進 `GtvContext`；`knn()` 依 kind 分派。
- 查詢向量改採「具名 query vector」或 512 維字串（MVP 沿用字串，512 個逗號
  float，註明 CLI 層可再包一層 `vector('q')`）。
- 波動率沿用既有 `mavg`/自訂 UDWF `stddev`，以 ROWS frame 實作（目前 UDWF
  忽略 frame，需補 frame-aware 版本）。

### 4.5 TC5 — 點時間快照（O(log N) 零拷貝）

**現況**：`WHERE valid_from<=T AND T<valid_to` 為全掃描。

**設計**：
```sql
SELECT * FROM point_in_time('edges', T);   -- O(log N)，回傳連續 active 切片
```
- 實作：UDTF 呼叫 `gtv_core::temporal::point_in_time_range`（兩次
  `partition_point`），當 `valid_from`/`valid_to` 皆已排序時零拷貝回傳
  `[lo,hi)` 切片；未排序則退回落至 `temporal_mask_pruned`。
- 標準 SQL `WHERE` 保留為通用路徑。

### 4.6 TC6 — 風控（scalar UDF，無分支 kernel）

**設計**：
```sql
SELECT count(*) FROM orders
WHERE risk_ok(price, qty, mid_price, smp_flag);
```
- 實作：`risk_ok` ScalarUDF 內部為無分支 kernel：
  `pass = (|p−m|<=0.05m) & (q<=10000) & (p·q<=10e6) & !smp`，
  以 bitwise 而非 `if` 累加。**逐列 SQL 調度有固定開銷，故 TC6 的 <200 ns
  記錄仍由 kernel 量測；SQL 路徑驗證功能等價。**

### 4.7 TC7 — tick-to-trade（UDTF，compiled）

**設計**：
```sql
SELECT * FROM tick_to_trade('ticks');
-- 回傳 (sym, side, order_price, order_qty)
```
- 實作：UDTF 對整批 tick 跑 compiled kernel（decode 欄位 → mid/signal 策略 →
  encode 訂單），非逐列 DataFusion。此為 kdb 無法「純 SQL」表達的狀態流，屬
  operator 層。

### 4.8 TC8 — L2 OBI + micro-price（UDAF）

**設計**：
```sql
SELECT sym,
       obi(bid_sz, ask_sz) AS obi,
       micro_price(bid_px, ask_px, bid_sz, ask_sz) AS micro
FROM order_book
GROUP BY sym;
```
- 實作：兩個 AggregateUDF 在 accumulate 階段累加 `Σbid_sz/Σask_sz`（OBI）與
  best-level 乘加（micro-price），finalize 一次性除法。
- 寬表（10 檔 4 欄 × symbol）採「註冊列式資源 + UDTF」更貼近 SIMD 佈局；
  長表（`GROUP BY`）勝在 SQL 語意。兩者皆提供，效能記錄由 UDTF 路徑測。

### 4.9 TC9 — 流式協方差（有狀態 operator）

**設計**：
```sql
-- 註冊收益序列，維護 500×500 下三角協方差
SELECT * FROM covariance_matrix('rets', 500);   -- 回傳目前矩陣（下三角展開）
```
- 實作：`register_covariance(name, n)` 建立 accumulator；每次 `push_rets`（由
  tick_to_trade 或外部觸發）做 rank-1 更新。SQL 只讀取現狀。
- kernel：下三角平坦化 `N(N+1)/2` + rayon 區塊更新；單 tick <2 ms。

### 4.10 TC10 — 本地撮合（UDTF，具名簿）

**設計**：
```sql
-- 註冊訂單簿後，餵入訂單流，回傳成交
SELECT * FROM match_orders('book1', 'orders');
```
- 實作：`register_matching_engine(name, levels)`；`match_orders` UDTF 內部用
  Slab Allocator + IntMap 檔位定址 + 雙向鏈表（Price-Time FIFO），非 BTreeMap
  （現 `hft_bench_tc6_tc10` 僅 level-aggregate；正式版補 per-order FIFO 隊列）。

---

## 5. DataFusion 擴充點實作方式

| 擴充點 | trait | 用於 |
|--------|-------|------|
| Scalar UDF | `ScalarUDFImpl` | TC6 `risk_ok` |
| Aggregate UDF | `AggregateUDFImpl` | TC8 `obi`/`micro_price` |
| Window UDF | `WindowUDFImpl` | TC2 `ofi`、TC4 `vol/stddev` |
| Table function | `TableFunctionImpl` | TC1/3/4/5/7/10 |
| 具名資源註冊 | `GtvContext::register_*` | 右序列、向量索引、訂單簿、協方差 |
| Planner 擴充（長期） | `UserDefinedLogicalNode` / `TableProvider` | 原生 `ASOF JOIN`、zone-map 下推 |

所有運算子只做**薄包裝**：把 DataFusion 的 `ScalarValue`/`ArrayRef` 轉成
`&[f64]`/`&[i64]` 切片後，直接呼叫 `gtv-core/gtv-array/gtv-index/gtv-pattern`
的 compiled kernel；結果再包回 `RecordBatch`。**不複製演算法邏輯到 engine 層。**

---

## 6. 效能分層結論

| 層 | 延遲量級 | TC |
|----|---------|----|
| 宣告式 SQL（標準 WHERE/GROUP BY/JOIN） | 10 µs–ms（DataFusion 逐列/批開銷） | TC5 通用、TC8 長表 |
| 自訂 UDF/UDAF/UDTF（compiled kernel） | ns–µs（kernel 決定） | TC1–TC10 全部 |
| 原生 operator（matching/stateful） | ns（kernel 決定） | TC7/TC9/TC10 |

HFT 延遲記錄（TC6 <200ns、TC7 <1.5µs、TC10 <1µs）只在第 2/3 層可達；SQL 層
負責**組裝與正確性**，kernel 負責**極限延遲**。兩者共用同一運算子，互不矛盾。

---

## 7. 里程碑

1. **M0（現況）**：`neighbors/asof_join/knn` UDTF + `mavg/msum/deltas` UDWF。
2. **M1（集合式全覆蓋）**：TC1 多欄容差 asof、TC2 `ofi` UDWF、TC3
   `wash_trade`、TC5 `point_in_time`、TC8 UDAF。
3. **M2（向量索引接 SQL）**：註冊 `IvfIndex`/`HnswIndex`，`knn()` 分派；TC4
   波動率 join。
4. **M3（狀態式 operator）**：TC7 `tick_to_trade`、TC9 covariance、TC10
   matching engine、TC6 `risk_ok` scalar UDF。
5. **M4（長期）**：原生 `ASOF JOIN` planner 擴充、zone-map 下推、零拷貝
   Flight 輸出。

驗證：每個 TC 增補 `.gtv` 腳本（`testcase/scripts/`）＋ golden file，納入
`run_tests.sh`，與現有 TC-01…TC-11 一致。
