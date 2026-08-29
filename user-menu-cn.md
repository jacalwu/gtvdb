# gtvdb 用戶手冊

gtvdb 是單引擎記憶體資料庫，融合 **Graph + Temporal + Vector + Columnar** 四大能力。
透過互動式 SQL REPL（`gtv`）驅動，提供兩種會話模式：

- **`hft`**（預設）—— 低延遲薄子集，算子用簡稱，並有預編譯 `KernelPlan` 熱路徑
  （kdb+/q 風格簡寫）。
- **`full`** —— 完整 DataFusion SQL（JOIN / GROUP BY / 視窗 / CTE / 子查詢）。

---

## 1. 建置與啟動

```sh
cargo build --release -p gtv-cli --bin gtv
cargo run --release -p gtv-cli --bin gtv
# 或直接執行已編譯的二進位
./target/release/gtv
```

```text
gtv> help                      # 列出命令
gtv> tables                    # 顯示 demo 表
gtv> quit                      # 離開
```

內建 demo 表：

| 表名 | 欄位 |
|------|------|
| `nodes`  | id, value |
| `edges`  | src, dst, edge_type, valid_from, valid_to（Int64 奈秒） |
| `prices` | t, price |
| `ticks`  | t, bid, ask, bid_sz, ask_sz |
| `orders` | id, price, qty, mid, smp |
| `book`   | sym, level, bid_px, ask_px, bid_sz, ask_sz |
| `t`      | time, price, bid, ask, bid_size, ask_size, native_flag（as-of 對齊後的成交/盤口） |

---

## 2. 會話模式與計時

```sql
ALTER SESSION SET sqlmode = hft;    -- 預設：薄子集 + 簡稱
ALTER SESSION SET sqlmode = full;   -- 完整 DataFusion SQL
SET DURATION = ON;                  -- 每個 action 印出計算耗時（µs）
SET DURATION = OFF;
```

HFT 模式簡寫（免打完整 SELECT）：

```text
ticks                -- 裸表名   == SELECT * FROM ticks
pit(500)             -- 裸表函數 == SELECT * FROM pit(500)
```

---

## 3. 載入資料

### 3.1 從磁盤載入 CSV / Parquet

```text
gtv> loadcsv ticks /data/ticks.csv        # CSV -> session 表 `ticks`
gtv> load    ticks /data/ticks.parquet    # Parquet -> session 表 `ticks`
gtv> save    ticks /data/ticks.parquet    # 把任意已註冊表寫成 Parquet
```

CSV 以逗號分隔、含表頭；schema 自動推斷（`timestamp` 欄位讀為 `Int64` 奈秒）。
tick CSV 範例：

```text
symbol,timestamp,price,volume,bid_price_1,ask_price_1,bid_size_1,ask_size_1
0700.HK,0,100.5,100,100.4,100.6,10,20
3690.HK,500,200.1,80,200.0,200.2,30,40
```

### 3.2 指派到 session 變數（full 模式）

```sql
SELECT * FROM read_csv('/data/ticks.csv');
SELECT * FROM read_parquet('/data/ticks.parquet');
CREATE TABLE t AS SELECT symbol, price FROM read_csv('/data/ticks.csv');
```

### 3.3 後台導入（生產者執行緒）

```text
gtv> bgload ticks /data/ticks.csv 200        # 每 200 ms 重新導入
gtv> bgload ticks /data/ticks.parquet 200    # 依副檔名自動偵測 CSV / Parquet
gtv> SELECT count(*) FROM ticks;              # session 持續讀取最新資料
```

---

## 4. TC1–TC15 使用案例

每個 kernel 都同時註冊 **簡稱（hft）** 與 **全名（analysis）**，例如 `pit` /
`point_in_time`。

### TC1 — 跨資產 as-of join（多欄 + 容差）

```sql
SELECT * FROM aj(0, 5, 15, 25);            -- 簡稱（hft）
SELECT * FROM asof_join(0, 5, 15, 25);     -- 全名
-- 回傳 (t, price, spread)，每個左 t 對齊 500µs 內最新的右 t
```

### TC2 — 訂單流不平衡（滾動 100-tick，融合單趟）

```sql
SELECT t, ofi(bid, ask, bid_sz, ask_sz, 100) OVER (ORDER BY t) FROM ticks;
-- 全名：order_flow_imbalance(...)
```

### TC3 — 洗艙環路檢測（A→B→C→A）

```sql
SELECT * FROM wash(500);                   -- 簡稱（hft）
SELECT * FROM wash_trade(500);             -- 全名
-- 回傳 T=500 時活躍的 ring(3) 節點三元組 (a, b, c)
```

### TC4 — 向量 K-NN

```sql
SELECT id FROM knn('songs', '0.1,0.1', 3);          -- 簡稱
SELECT id FROM vector_search('songs', '0.1,0.1', 3); -- 全名
-- 回傳最相近的 3 首歌曲 id（8, 0, 1）
```

### TC5 — 點時間訂單簿快照（O(log N) 零拷貝）

```sql
SELECT * FROM pit(500);                    -- 簡稱（hft，KernelPlan 熱路徑）
SELECT * FROM point_in_time(500);          -- 全名
-- 回傳活躍區間 [valid_from <= 500 < valid_to]
```

### TC6 — 交易前風控檢查（無分支）

```sql
SELECT count(*) FROM orders WHERE risk(price, qty, mid, smp);    -- 簡稱
SELECT count(*) FROM orders WHERE risk_ok(price, qty, mid, smp); -- 全名
-- 4 項硬檢查：價格帶（5%）、最大數量（10k）、最大名義金額（10M）、自成交旗標
```

### TC7 — Tick-to-Trade 端到端（解碼 → 策略 → 編碼）

*規劃中（M3）—— 尚未開放為 SQL 算子。*

### TC8 — L2 訂單簿不平衡 + 微觀價格

```sql
-- full 模式（聚合 / 純量）
SELECT sym, obi(bid_sz, ask_sz) FROM book GROUP BY sym;              -- order_book_imbalance
SELECT sym, mp(bid_px, ask_px, bid_sz, ask_sz) FROM book WHERE level = 0;  -- micro_price
```

### TC9 — 流式 500×500 協方差矩陣

*規劃中（M3）—— 尚未開放為 SQL 算子。*

### TC10 — 本地撮合引擎（價格-時間優先）

*規劃中（M3）—— 尚未開放為 SQL 算子。*

### TC11 — Lee-Ready 演算法（報價規則 + 跳價規則備用）

```sql
SELECT time, lr(price, bid, ask) OVER (ORDER BY time) AS tc11 FROM t;   -- lee_ready
```

### TC12 — 跳價規則

```sql
SELECT time, tick(price) OVER (ORDER BY time) AS tc12 FROM t;           -- tick_rule
```

### TC13 — EMO（Ellis–O'Hara–Thomas）

```sql
SELECT time, emo(price, bid, ask) OVER (ORDER BY time) AS tc13 FROM t;
```

### TC14 — 一檔訂單流不平衡（Cont）

```sql
SELECT time, ofil(bid, bid_size, ask, ask_size) OVER (ORDER BY time) AS tc14 FROM t; -- ofi_l1
```

### TC15 — 交易所原生主動方旗標

```sql
SELECT time, agg(native_flag) AS tc15 FROM t;                          -- aggressor_flag
-- 'B'/'BUY' -> 1，'S'/'SELL' -> -1
```

### TC11–TC15 合併查詢

```sql
SELECT time,
       lr(price, bid, ask) OVER (ORDER BY time) AS tc11,
       tick(price) OVER (ORDER BY time) AS tc12,
       emo(price, bid, ask) OVER (ORDER BY time) AS tc13,
       ofil(bid, bid_size, ask, ask_size) OVER (ORDER BY time) AS tc14,
       agg(native_flag) AS tc15
FROM t;
```

---

## 5. 算子對照表

| TC | 簡稱（hft） | 全名（analysis） | 型態 |
|----|-------------|-----------------|------|
| TC1 | `aj` | `asof_join` | 表函數 |
| TC2 | `ofi` | `order_flow_imbalance` | 視窗 |
| TC3 | `wash` | `wash_trade` | 表函數 |
| TC4 | `knn` | `vector_search` | 表函數 |
| TC5 | `pit` | `point_in_time` | 表函數 |
| TC6 | `risk` | `risk_ok` | 純量 |
| TC8 | `obi` | `order_book_imbalance` | 聚合 |
| TC8 | `mp` | `micro_price` | 純量 |
| TC11 | `lr` | `lee_ready` | 視窗 |
| TC12 | `tick` | `tick_rule` | 視窗 |
| TC13 | `emo` | `emo` | 視窗 |
| TC14 | `ofil` | `ofi_l1` | 視窗 |
| TC15 | `agg` | `aggressor_flag` | 純量 |

另有內建：`mavg` / `msum` / `deltas`（視窗）、`neighbors`（表函數）、
`read_csv` / `read_parquet`（表函數）。

---

## 6. Shell 命令

```text
help | tables | quit
neighbors <node> [T]       k-hop <node> <k> [T]
mavg <n> | msum <n> | deltas
asof [t ...]               knn <node> [k] [--mask ids]
save <table> <path>        load <table> <path>
loadcsv <table> <path>     bgload <table> <path> [ms]
tt <table> <T>             pattern [T]        delta
udf [x ...]                remote <host:port> <sql>
```

---

## 7. 效能說明

- `hft` 模式下，`pit` / `wash` / `aj` / `ofi` / 裸表掃描會先編譯成 **KernelPlan**
  （依查詢文字快取），之後直接呼叫 compiled kernel——熱路徑零 DataFusion 規劃。
  `SET DURATION = ON` 顯示 µs 級計算耗時，例如 `pit(500)` 首次編譯後約 3 µs，
  對照 DataFusion 路徑約 1.5 ms。
- `tick_rule`/`lee_ready`/`emo` 使用顯式符號（`>0 → 1, <0 → -1, =0 → 0`）；
  Rust 的 `f64::signum` 對 `+0.0` 回傳 `1.0`，故刻意避用。

---

## 8. 備註

- TC7（tick-to-trade）、TC9（流式協方差）、TC10（撮合引擎）屬狀態式算子，規劃於
  M3 里程碑；目前在 `hft_bench_tc6_tc10` 中以 compiled kernel 做基準測試，尚未開放
  為 SQL 算子。
- `full` 模式是完整 DataFusion SQL；`hft` 模式是低延遲子集（不支援 JOIN / GROUP BY
  / CTE / 子查詢）。
