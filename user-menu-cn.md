# gtvdb 用戶手冊

gtvdb 是單引擎記憶體資料庫，融合 **Graph + Temporal + Vector + Columnar** 四大能力。
透過互動式 SQL REPL（`gtv`）驅動，提供兩種會話模式：

- **`hft`**（預設）—— 低延遲薄子集，算子用簡稱，並有預編譯 `KernelPlan` 熱路徑
  （kdb+/q 風格簡寫）。
- **`full`** —— 完整 DataFusion SQL（JOIN / GROUP BY / 視窗 / CTE / 子查詢）。

---

## 1. 建置與啟動

```sh
./rebuild.sh                    # 编译 release gtv（改 crates/ 后记得重跑）
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
gtv> LOAD CSV '/data/ticks.csv' INTO ticks   # doc-style 別名（同 loadcsv）
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

### 3.4 即時串流（London Strategic Edge WebSocket）

```sh
export LSE_API_KEY=lse_live_...              # London Strategic Edge live API key
gtv> live q BTC/USD ETH/USD SOL/USD          # 把即時報價 tick 串流進表 `q`
gtv> SELECT symbol, count(*) FROM q GROUP BY symbol;   # full 模式
```

經 `wss://ws.londonstrategicedge.com` 串流 `(symbol, price, bid, ask, ts)` tick
（auth `{action:"auth",api_key}`、訂閱 `{action:"subscribe",symbol}`）。免費 live
key 覆蓋 **加密貨幣**（`BTC/USD`、`ETH/USD`…）；股票/外匯可能需要更高 tier 或不同
symbol 格式。

### 3.5 抓取歷史 tick（London Strategic Edge REST API）

```text
gtv> fetch mco MCO 20000          # 抓取 MCO 歷史 tick 進表 `mco`
gtv> fetch tsla TSLA 100000       # (symbol, ts, price, bid, ask, volume)
gtv> SELECT count(*) FROM mco;
```

底層 `GET https://api.londonstrategicedge.com/tickdata?symbol=eq.<sym>`。優先使用
`LSE_API_KEY` 環境變數，未設定時用站方公開 key。注意 `lse_live_*` key 僅限 WebSocket，
在 REST API 會回 `Unauthorized`。自動分頁：API 單次上限 10000 列，`fetch`/`read_tickdata`
會用 `ts` keyset 游標翻頁直到 `limit`。

### 靜態數據快取（按天分目錄）

`yahoo`/`fetch`/`read_yahoo`/`read_tickdata` 是 **read-through 快取**：打 Web API 前先
檢查 `<GTV_DATA_DIR>/<source>/<date>/<symbol>.parquet`（預設 `data/static`），缺資料才
抓取一次並按天寫入 Parquet，之後每次執行完全離線。

```sh
export GTV_DATA_DIR=/data/gtv_static   # 可選，預設 ./data/static
gtv> yahoo hsi ^HSI --range 3mo        # 第一次：抓取 + 快取 64 天檔案
gtv> yahoo hsi ^HSI --range 3mo        # 第二次：命中快取，零網路
find $GTV_DATA_DIR/yahoo -name '*.parquet' | head   # 按天檔案
```

```sql
-- full 模式：同樣的抓取做成 SQL 表函數
SELECT * FROM read_tickdata('TSLA', 25000);
CREATE TABLE tsla AS SELECT * FROM read_tickdata('TSLA', 100000);
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

-- 完整簽名：knn(name, query, k [, label [, metric]])
SELECT id FROM knn('songs', '0.1,0.1', 3, 'pop');          -- 只喺 label='pop' 內搜
SELECT id FROM knn('tss', '0.1,0.1', 3, '*', 'cosine');    -- '*' = 不篩 label；metric=cosine
-- metric ∈ l2（預設）| cosine | dot；查詢 metric 必須同 collection 一致，
-- 不一致會回 MetricMismatch（唔會靜默用錯 metric）。
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

```sql
SELECT * FROM tick_to_trade('mco', 100);   -- 簡稱：ttrade('mco', 100)
-- 回傳 (side, price, qty)；side：0=買、1=賣、2=無
```

均值回歸交叉訊號（價格穿越尾部 `mavg[window]`）；例如對 250 萬列的
`stocks_MCO_tick.parquet` 一次編譯掃描產出 ~250 萬列。

### TC8 — L2 訂單簿不平衡 + 微觀價格

```sql
-- full 模式（聚合 / 純量）
SELECT sym, obi(bid_sz, ask_sz) FROM book GROUP BY sym;              -- order_book_imbalance
SELECT sym, mp(bid_px, ask_px, bid_sz, ask_sz) FROM book WHERE level = 0;  -- micro_price
```

### TC9 — 流式協方差矩陣

```sql
SELECT * FROM covariance_matrix('returns', 3);  -- 簡稱：cov('returns', 3)
-- 回傳 m×m 樣本協方差，格式 (i, j, cov)
```

讀取已註冊的 returns 表（欄位 `ret_0 .. ret_{m-1}`），回傳 `m × m` 樣本協方差。

### TC10 — 本地撮合引擎（價格-時間優先）

```sql
SELECT * FROM match_orders('orderstream');     -- 簡稱：match('orderstream')
-- 回傳 (side, price, qty) 成交事件
```

對已註冊訂單表（欄位 `side`、`is_mkt`、`price`、`qty`）做價格-時間優先撮合。

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
| TC7 | `ttrade` | `tick_to_trade` | 表函數 |
| TC8 | `obi` | `order_book_imbalance` | 聚合 |
| TC8 | `mp` | `micro_price` | 純量 |
| TC9 | `cov` | `covariance_matrix` | 表函數 |
| TC10 | `match` | `match_orders` | 表函數 |
| TC11 | `lr` | `lee_ready` | 視窗 |
| TC12 | `tick` | `tick_rule` | 視窗 |
| TC13 | `emo` | `emo` | 視窗 |
| TC14 | `ofil` | `ofi_l1` | 視窗 |
| TC15 | `agg` | `aggressor_flag` | 純量 |

另有內建：`mavg` / `msum` / `deltas`（視窗）、`neighbors`（表函數）、
`read_csv` / `read_parquet`（表函數）。

量化算子（Phase 2）：

| 簡稱 | 全名 | 型態 |
|------|------|------|
| — | `bs_price` / `bs_delta` / `bs_gamma` / `bs_vega` / `bs_theta` | 純量 |
| `var` | `var_historical` | 表函數 |
| — | `pca` | 表函數 |
| `l2` | `reconstruct_l2` | 表函數 |
| — | `xbar` | 純量（時間分桶） |
| — | `ohlc` | 表函數（tick → OHLCV K 線） |
| — | `zscore` / `momentum` | 視窗（截面 / 技術指標） |
| — | `signal` | 純量（z → buy/sell/hold） |
| `trunc` | — | 純量：Oracle 式時間分桶 `trunc(ts, 'DD')`（SS/MI/HH/DD/MM/YYYY）與數值截斷 `trunc(x[, digits])` |
| — | `read_yahoo` | 表函數（日線 OHLCV） |
| — | `cross_sectional_signal` | 表函數（fetch+momentum+zscore+signal+隔日報酬） |
| — | `relative_strength` | 表函數（標的 vs 指數 vs 同行） |

---

## 6. Shell 命令

```text
help | tables | providers | quit
neighbors <node> [T]       khop <node> <k> [T] [--max-edges N] [--max-frontier N] [--max-degree N]
mavg <n> | msum <n> | deltas
asof [t ...]               knn <node> [k] [--mask ids]
knn_from <name> <table> [dim] [metric]   # metric = l2（預設）| cosine | dot
embedding_register <name> <table>        # 註冊標準 embedding 表（schema 驗證 + 治理）
embedding_build <name> <root> <table> [type] [metric]   # 建構受治理向量索引（flat|ivf|hnsw）
save <table> <path>        load <table> <path>
loadcsv <table> <path>     bgload <table> <path> [ms]
LOAD CSV '<path>' INTO <table>          doc-style CSV 匯入（loadcsv 的別名）
live <table> <symbol...>   串流 LSE 即時 tick（需 LSE_API_KEY）
fetch <table> <symbol> [limit]  抓取 LSE 歷史 tick（REST API）
yahoo <table> <symbol...> [--range 1y]  抓 Yahoo 日線 OHLCV
md klines <provider> <table> <code...> [--period 1d] [--start] [--end] [--max] [--adjust]
md ticks  <provider> <table> <code...> [--max N]   行情 → 直接注册 session 表
hdb_save <table> <date> [root]  把表持久化到 HDB 分區
hdb_load <table> <date> <sym> [root]  讀取單個 HDB 分區
hdb_scan <table> <start> <end> [sym] [root]  掃描 HDB 日期區間
hdb_flush <table> [root] [secs]  背景 HDB 落盤（symbol 枚舉）
tt <table> <T>             pattern [T]        delta
udf [x ...]                remote <host:port> <sql>
metrics                                  # 引擎計數器 + SQL 延遲直方圖（Prometheus 文字）
```

行情/趨勢分析函數（provider 只是第一個參數，見 §7）：

```text
providers                            # 列出已注册的 provider
klines('futu','HK.00700','1d','2024-06-03','2024-06-07')   # 裸调=hft 简写
SELECT * FROM klines('yahoo','0700.HK','1d',...);           # 标准 SQL（任何模式）
fwd_proba('表',H,K)                 # 下一 H 交易日上涨/下跌概率
fwd_walk('表',H,K[,warmup])         # 严格因果 walk-forward 回放
fwd_regress('表',asof_ns,H,K)       # as-of 回归：方向+区间 vs 真实
align('表',freq_sec,'ffill'|'drop')            # 多標的對齊到規則網格
dq_report('表') | dq_check('表')                # 資料品質：NaN/重複/時間倒退/標的覆蓋
health_check('表'[,max_age_days[,min_rows]])    # 健康度：freshness vs 恆指交易日曆 等
strategy_stats('表')                            # 訊號診斷：hit/Brier/ECE/PSI（需 up + p_up 欄）
khop(src, k, valid_at[, max_hops, max_edges])   # 資源受限 k-hop BFS：visited bitmap + budget
                                                # 回傳 (hop, dst)；超出 budget 回 BudgetExceeded
crm_alloc('loan_exposure','collateral','guarantee','collateral_edges','guarantee_edges','BASE',T[, 'ead'])  # 每筆貸款的 CRM 覆蓋（§8）
crm_audit('loan_exposure','collateral','guarantee','collateral_edges','guarantee_edges','BASE',T[, 'ead'])  # 分配稽核紀錄（§8）
embedding_search(name, q, k [, tenant [, as_of]])  # 受治理向量檢索：provenance + 租戶 + 過期過濾（§12）
metrics                                        # 引擎計數器
```

---

## 7. 行情資料提供者與趨勢分析

### 7.1 統一 provider 框架

任何數據源（Futu OpenD、Yahoo Finance、自行註冊的 provider）實作同一個
`MarketProvider` trait 後按名字注册，**接口完全一致**——provider 只是函數的第一個參數：

```sql
-- 标准 SQL（任何模式）：
SELECT * FROM klines('futu', 'HK.00700', '1d', '2024-06-03', '2024-06-30');
SELECT * FROM ticks('futu', 'HK.00700', 100);       -- 当日逐笔（需盘中 + LV2）
-- REPL hft 模式可裸写：
klines('yahoo', '0700.HK', '1d', '2024-01-01', '2024-03-01')
```

- 統一 schema：`provider, symbol, ts(UTC 纳秒), open/high/low/close, volume, turnover, adjclose`（tick 另有 direction/sequence）
- 参数：`klines(provider, code, period[, start][, end][, max][, adjust])`；period `1m..1Y`，日期 `YYYY-MM-DD` 闭区间
- `futu`：本机 OpenD（`127.0.0.1:11111`）+ `futu-api`，代码 `HK.00700/US.AAPL`；`yahoo`：免 key，代码 `0700.HK/AAPL`，无 tick
- 引擎内置**缓存优先 + 增量补齐**（历史数据只拉一次）：目录 `GTV_MARKET_DIR`（默认 `data/market`），`GTV_MARKET_CACHE=0` 关闭

`md` 命令把行情注册成命名 session 表（供 `fwd_*`/SQL/HDB 使用）：

```text
gtv> md klines futu hk700 HK.00700 --period 1d --start 2024-01-01 --end 2024-12-31
```

### 7.2 未来 7–14 个交易日趋势信号（历史类比 kNN）

特征（内置 mom5/mom10/vol10/above_sma20，也可用 `feats` 自定列）→ z-score →
历史中找 K 个最相似交易日，`p_up/p_down` = 类比日未来 H 根实际涨/跌占比：

```text
gtv> md klines futu hk700 HK.00700 --period 1d --start 2020-01-01
SELECT * FROM fwd_proba('hk700', 10, 20);    -- 决策日=最后一根：p_up, p_down, hit_rate
```

- `fwd_walk('表', H, K)`：**严格因果**回放（每个历史点只用其之前的数据），逐条
  输出预测与真实结果，供校准（`stock_calib.sh`）
- `fwd_regress('表', asof_ns, H, K)`：把过去某日当“决策日”，输出方向概率 +
  预测价格区间（`pred_lo/pred_hi`）并与真实未来路径对比偏差

### 7.3 一键脚本

```bash
./stock_analysis.sh HK.00700          # SOURCE=futu 默认；exit 0 无信号 / 3 触发提醒（THRESHOLD 默认 0.75 可调）
./stock_calib.sh HK.00700             # walk-forward 校准：分桶 p vs 实际涨率 + 阈值建议
./stock_regress.sh HK.00700 2026-08-03 2026-07-01   # as-of 回归：方向命中/区间覆盖/收益偏差
# 数据源/参数：SOURCE=yahoo|parquet，HORIZON=10, K=20, THRESHOLD=0.8, START/END, FILE=（parquet 复用）
```

### 7.4 量化研究工具箱 & 持倉監控（function2.md — Phase A + C）

新增引擎函數（視窗函數需 `ALTER SESSION SET sqlmode = full`）：

```sql
-- 技術指標（因果 trailing-window，NaN 暖機），支援 PARTITION BY symbol：
--   ema(n), rsi(n), macd_dif/dea/hist(f,s,g), atr(n), boll_mid/up/lo(n,k), vwap(n)
SELECT t, rsi(close,14) OVER (PARTITION BY symbol ORDER BY t) FROM bars;
-- 多標對齊 + 截面因子
SELECT ts, symbol, close, zscore(close) OVER (PARTITION BY ts)
FROM align('multi', 86400000000000, 'ffill');    -- freq 單位 ns，fill = ffill | drop
-- 回測：單標（持倉狀態機 + 成本 + 停損/停利）與等權組合 + 再平衡
SELECT * FROM bt_report('sig', 20, 0.06, 0.10);  -- cost_bps, stop_loss, take_profit
SELECT * FROM pf_report('multisig', 20);
-- 資料品質 / 健康度 / 策略漂移
SELECT * FROM health_check('hk03668', 7, 200);
SELECT * FROM strategy_stats('decisions');       -- 需 up 與 p_up/cal_p_up_* 欄
-- 引擎指標：REPL 指令 `metrics`
```

持倉每日報告 scripts（`ZH=1` 印中文股名與表頭）：

```bash
./holdings_forecast.sh                  # direction(升/橫行/跌) + action(買/持/賣) + 模擬佐證 + 恆指情境
ZH=1 ./holdings_forecast.sh             # 中文版（HOLDING.txt 第二欄為中文股名）
EVENT_MODE=1 ./holdings_forecast.sh     # 事件風險層（門檻建議 0.80、跳空附錄）
./stock_sim.sh HK.00857                 # 紙上模擬：THR×成本網格、SIZING、多/空側平均
./holdings_sim.sh                       # 每股動作彙總（--run 補跑模擬）
```

每股 HORIZON/THRESHOLD 由 `HOLDING.cfg` 設定（例 `HK.00857 20 0.85`）。
規範/狀態：`function2.md`（Phase A+C）、`forecast-function.md`（已實現 / 不做清單+原因）、`design.md`。

详细文档：`doc/market-providers.md`、`stock_analysis.md`（含校准/回测结论与边界：Futu 无历史逐笔、Yahoo 分钟回溯窗口限制等）。

---

## 8. CRM 分配（信用風險緩釋）

對 `crm-allocation.md`（倉庫根目錄）的五張表執行 greedy CRM 分配：

* 一筆貸款可有多個抵押品 + 多個擔保人，反之亦然（多對多圖）；
* 抵押品價值先做 haircut 調整：
  `C × (1 − haircut − fx_haircut − maturity_mm)`（下限 0）；
* `specified`（合約鎖定）邊 **先** 分配；剩餘 `optimizable` 邊進入確定性
  greedy / priority 分配（來源與貸款按 priority 排序，相同時依 id 升序）；
* 擔保階段總在抵押品階段之後，因此擔保只覆蓋殘餘暴露：
  `CRMg = min(G, E − CRMc)`；
* 每次分配都會寫入 audit trail（可稽核 / 可重跑）。

載入五張 CSV（schema 與產生器見 `crm-allocation.md` §1–§3），然後分配：

```text
gtv> LOAD CSV '/home/jacal/gtvdb/testcase/loan_exposure.csv'     INTO loan_exposure
gtv> LOAD CSV '/home/jacal/gtvdb/testcase/collateral.csv'        INTO collateral
gtv> LOAD CSV '/home/jacal/gtvdb/testcase/guarantee.csv'         INTO guarantee
gtv> LOAD CSV '/home/jacal/gtvdb/testcase/collateral_edges.csv'  INTO collateral_edges
gtv> LOAD CSV '/home/jacal/gtvdb/testcase/guarantee_edges.csv'   INTO guarantee_edges

gtv> SELECT * FROM crm_alloc('loan_exposure','collateral','guarantee',
        'collateral_edges','guarantee_edges','BASE',-1);
```

參數：`crm_alloc(loan_tbl, coll_tbl, guar_tbl, coll_edges_tbl,
guar_edges_tbl, scenario, T [, exposure_col [, method]])`：

* `scenario` 過濾 `scenario_id` 列（`''` 或 `'*'` = 不區分情境）；
* `T` = as-of 時間點（ns）— 只保留 `valid_from <= T < valid_to` 的列；
  `-1` = 不做時間過濾；
* 可選 `exposure_col` = 要緩釋的貸款暴露欄（預設 `ead`，例如 `'pv'`）；
* 可選 `method` = `'greedy'`（預設）| `'haircut_efficiency'` | `'lp'`：
  - `greedy` — 抵押品依類型品質（CASH > BOND > EQUITY）、貸款依 `pd`
    （風險高者先覆蓋）；
  - `haircut_efficiency` — 抵押品依 haircut 調整後的**有效價值**
    （`C × (1 − Hc − Hfx − Hmm)`，大者先用）、貸款依**風險權重 RW**
    （`rw` 欄 → rating 對照表 → `pd`）；Phase 1 與分配規則相同，只改排序鍵；
  - `lp` — 精確 LP（需 `crm-lp` feature）。

輸出（每個在切片內的貸款一行）：
`loan_id, exposure, collateral_cover, guarantee_cover, net_exposure`。

完整 audit trail（相同輸入、相同確定性執行）：

```text
gtv> SELECT * FROM crm_audit('loan_exposure','collateral','guarantee',
        'collateral_edges','guarantee_edges','BASE',-1);
```

audit 欄位：`seq, stage (specified|greedy|lp), source_kind
(collateral|guarantee), source_id, loan_id, amount, source_remaining,
loan_remaining`。

一致性檢查 / 報表：

```sql
-- as-of / scenario 切片內的貸款數：
SELECT count(*) FROM crm_alloc('loan_exposure','collateral','guarantee',
        'collateral_edges','guarantee_edges','BASE',-1);
-- 暴露守恒：Σ(collateral_cover + guarantee_cover + net_exposure) = Σ ead
SELECT sum(collateral_cover + guarantee_cover + net_exposure) AS exposure_total,
       min(net_exposure) AS min_net
FROM crm_alloc('loan_exposure','collateral','guarantee',
        'collateral_edges','guarantee_edges','BASE',-1);
-- 覆蓋來源分佈：
SELECT stage, source_kind, count(*) AS allocs, sum(amount) AS amount
FROM crm_audit('loan_exposure','collateral','guarantee',
        'collateral_edges','guarantee_edges','BASE',-1)
GROUP BY stage, source_kind;
```

greedy vs LP（`method`）：Phase 1（specified）共用且完全相同；只有
optimizable 池的解法不同。

```text
cargo build -p gtv-cli --features gtv-engine/crm-lp     # 啟用 Phase 3（microlp）
gtv> SELECT * FROM crm_alloc('loan_exposure','collateral','guarantee',
        'collateral_edges','guarantee_edges','BASE',-1,'ead','greedy');  -- 啟發式
gtv> SELECT * FROM crm_alloc('loan_exposure','collateral','guarantee',
        'collateral_edges','guarantee_edges','BASE',-1,'ead','lp');      -- 精確 LP
-- 風險加權殘餘：LP 最小化 Σ pd × net_exposure（optimizable 池）
SELECT round(sum(r.net_exposure*l.pd),2) AS risk_weighted_net
FROM crm_alloc('loan_exposure','collateral','guarantee',
        'collateral_edges','guarantee_edges','BASE',-1,'ead','lp') r
JOIN loan_exposure l USING (loan_id);
```

行為說明：

* scenario / as-of 過濾逐表進行；端點被切片過濾掉的邊屬「無效」會被丟棄；
  引用源表中不存在的 id 則中止執行（資料錯誤）。
* 預設優先級：貸款用 `pd`（風險高者先覆蓋）、抵押品用 `type`
  （CASH > BOND > EQUITY）、擔保人用 `rating`（AAA > AA > A …）。
  任一張表加 `priority` 欄即可覆蓋該表預設值。
* 情境沒有資料（例如尚未載入 `STRESS`）回傳空結果 — 不是錯誤。
* `method='lp'` 對 optimizable 邊建立精確線性規劃
  （`x_e ≥ 0`、貸款需求 + 來源容量約束），**並遵守每條邊的合約上限**
  （`collateral_edges.ratio` 與 `guarantee_edges.amount`）；greedy Phase 2
  依參考語意刻意忽略這些上限，因此只有當 cap 生效時兩者結果才會不同。
  未開啟 feature 時 `method='lp'` 會回傳明確的重建提示。
* 純 Rust kernel（也可直接呼叫）：`gtv_array::crm::crm_alloc_greedy`
  （Phase 1+2）與 `gtv_array::crm_lp::crm_alloc_lp`
  （Phase 3，需 `crm-lp` feature；求解器走 `good_lp` — 預設 `microlp`，
  大模型可把 `good_lp` 的 feature 換成 `highs`）。

---

## 9. 效能說明

- `hft` 模式下，`pit` / `wash` / `aj` / `ofi` / 裸表掃描會先編譯成 **KernelPlan**
  （依查詢文字快取），之後直接呼叫 compiled kernel——熱路徑零 DataFusion 規劃。
  `SET DURATION = ON` 顯示 µs 級計算耗時，例如 `pit(500)` 首次編譯後約 3 µs，
  對照 DataFusion 路徑約 1.5 ms。
- `tick_rule`/`lee_ready`/`emo` 使用顯式符號（`>0 → 1, <0 → -1, =0 → 0`）；
  Rust 的 `f64::signum` 對 `+0.0` 回傳 `1.0`，故刻意避用。

---

## 10. 備註

- `full` 模式是完整 DataFusion SQL；`hft` 模式是低延遲子集（不支援 JOIN / GROUP BY
  / CTE / 子查詢）。
- 真實 tick 樣本（`stocks_{MCO,NVDA,TSLA}_tick.parquet`，各約 250 萬列，取自
  London Strategic Edge）位於 `testcase/hft/data/`：
  ```text
  gtv> load mco testcase/hft/data/stocks_MCO_tick.parquet
  gtv> SELECT count(*) FROM tick_to_trade('mco', 100);
  ```

---

## 11. 向量 metric 與圖走訪預算（prod_p1）

- **Distance metric**：`l2`（預設，平方歐氏）、`cosine`（`1 − cos`，索引會對
  rows 同 query 做 unit-normalize）、`dot`（內積，內部用負內積令「越小越近」統一）。
  索引建構時鎖定 metric 同 dimension；查詢 metric 不一致回 `MetricMismatch`，
  dimension 不一致回 `DimensionMismatch`。`dot` 唔係 metric（違反三角不等式），
  ANN recall 可能下降，建議改用 `cosine`。
- **`knn_from <name> <table> [dim] [metric]`**：由表註冊 vector collection，
  預設 L2；查詢時亦可以 `knn(name, q, k, '*', metric)` 指定（`'*'` = 不篩 label）。
- **`khop(src, k, valid_at[, max_hops, max_edges])`**：資源受限 k-hop BFS。
  每個節點最多訪問一次（visited bitmap），frontier 排序確定；超出
  `max_hops` / `max_edges` / frontier / rows / memory / deadline 會回
  `BudgetExceeded`，唔會 OOM。CLI 另支援 `--max-frontier`、`--max-degree`
  （高 degree guard，超過且無 predicate 時拒絕）。
- **TemporalCSR 自適應索引**：每個 source 嘅連續 edge run 做 binary search
  （`valid_from`）+ 每 64 條 edge 一個 zone map（`valid_to`），高 degree 節點
  查詢實測較線性掃描快 ~17×；低 degree 保持線性快路。
- **索引生命週期（`gtv-index-store`）**：`index_save <name> <root> <table> [type] [metric]`
  由表建立 `flat` / `ivf` / `hnsw` 索引並持久化為
  `<root>/<index_id>/v<n>/index.gtvidx`（manifest + payload + blake3 checksum）；
  `index_load <name> <root> [version]` 載入後可用 SQL
  `ann(name, query, k [, metric])` 查詢。版本以 `CURRENT` 原子切換，支援
  shadow build、atomic swap 同 rollback；container 或 payload checksum 不符會拒絕載入。

---

## 12. 受治理 Embedding 檢索（prod_p2 B2-4）

標準 embedding 表（`gtv-catalog::embedding_schema`）每一行都帶齊 provenance 同生命週期：

```text
entity_id, embedding: FixedSizeList<Float32>[dim],
model_id, model_version, tokenizer_version, dimension,
distance_metric, normalized, created_at,
effective_from, effective_to, source_hash,
feature_version, tenant_id, classification
```

- **`embedding_register <name> <table>`**：由 session 表註冊受治理 collection。
  註冊時驗證 `dimension` 同 `FixedSizeList` 長度一致、同一批只准單一
  model / version / dim / metric / normalized、`source_hash` 非空、
  `effective_from <= effective_to`；唔符會明確拒絕（唔會靜默建索引）。
- **`embedding_search(name, q, k [, tenant [, as_of]])`**：以 `q`（逗號分隔
  float，e.g. `'1.0,0.0'`）檢索，回傳
  `(id, distance, model_id, model_version, source_hash, feature_version)`。
  - `tenant`：只回該租戶嘅向量；`'*'` = 管理員跨租戶檢視；省略 = 不加租戶
    過濾。跨租戶**永遠唔會**互相命中。
  - `as_of`：只回 `effective_from <= as_of < effective_to`（`effective_to`
    為 null 代表長期有效）嘅向量，過期 embedding 唔會再被命中。
  - distance 報告方式同 `knn` / `ann` 一致（L2 → 歐氏距離）。
- **`embedding_build <name> <root> <table> [type] [metric]`**：先過同一套驗證
  + 過期/租戶過濾，再建 `flat` / `ivf` / `hnsw` 索引並持久化；spec 同資料
  dimension / model / metric 唔一致會被拒。manifest 會記錄
  `feature_version` 同 `normalized`，令檢索結果可追溯模型、來源 hash、特徵版本。
- **治理**：embedding 過期、替換、重建全部經同一條 validation + `filter_active`
  choke point；任何檢索結果都可列出 `model_id` / `version` / `source_hash` /
  `feature_version` / `tenant_id`。

---

## 13. 資料質量與發布閘門（prod_p2 B2-5）

規則以 JSON 聲明（inline、`@file` 或 `.json` 路徑），支援六類檢查：
completeeness（非空比例）、uniqueness（PK 唯一）、freshness（event_time 相對於
now 嘅 lag）、range（數值上下限）、referential（child → parent 完整性）、
reconciliation（source/target row count 同 amount sum）。

- **`dq_gate <table> <rules> [execution_id]`**：評估規則，逐條印出
  `rule / target / PASS|FAIL / observed / threshold / message`；任何一條 fail
  會回錯誤（block），並在 `GTV_HOME` 記錄決策（連 execution_id）。
- **`dq_override <table> <rule> <approver> <reason...>`**：豁免某條 fail 規則；
  寫入 append-only override ledger（含批准人、原因、時間、對象）。
- **`publish <table> <rules> [execution_id]`**：先過 gate；未達標且
  **無** override 會拒絕寫入 catalog，通過或已 override 才 commit snapshot。
- **`dq_audit`**：列出所有 gate 決策（pass / overridden / failures / exec_id）
  同 override（rule / approver / reason）。
- **SQL**：`SELECT * FROM dq_gate('table', '<rules_json>')` 回傳
  `(rule, target, passed, observed, threshold, message)`，可再用 SQL 彙總。

規則 JSON 例：

```json
[{"rule":"completeness","column":"price","min_ratio":0.99},
 {"rule":"uniqueness","columns":["sym"]},
 {"rule":"freshness","event_time_col":"ts","max_lag_ns":86400000000000},
 {"rule":"range","column":"price","min":0.0,"max":1000000.0},
 {"rule":"referential","child_col":"sym","parent":"syms","parent_col":"sym"},
 {"rule":"reconciliation","name":"src->tgt","source_rows":10,"target_rows":10,
  "tolerance":0.001,"source_sum":100.0,"target_sum":100.0}]
```

> Override 以 `rule` kind 為單位（同一個 target + rule 豁免該類全部 failure）。
> 現有 `dq_report` / `dq_check` / `health_check` 保留為純診斷，唔會 block。

---

## 14. IVF k-means 粗量化器（prod_p3 B3-4）

`IvfIndex` 嘅 coarse quantizer 由「均勻取樣」升級為 **k-means++ / Lloyd**：

- `KMeansConfig { nlist, max_iters(25), restarts(3), sample, seed, split_oversized,
  split_threshold_k }`；大 corpus 預設抽 50k 訓練；**相同 seed + 資料 → 完全相同 centroids**。
- **空 cell**：重指派「最遠點」令每個 cell 都非空；**oversized cell**
  （count > μ + kσ，`split_oversized = true`）會自動切分並重新 refine。
- `KMeansConfig::for_nlist(n)` 為保守預設（不切分、用全部資料）；
  `IvfIndex::with_metric` 用 k-means，`IvfIndex::with_uniform` 保留舊均勻取樣做
  baseline / fallback。
- 訓練 metadata（`kmeans` / `seed` / `restarts` / `iters` / `sample` / `inertia`）寫入
  `GIVFv2` payload；舊 `GIVFv1` 仍可載入（零格式回歸）。
- `CellStats` / `RetrainTrigger::PopulationImbalance` 可偵測 cell 人口失衡並提示 retrain。
- **`index_tune <table> [target_recall] [k]`**：對 `(nlist, nprobe)` 網格量度 Recall@K
  同掃描成本（probed rows），印出 recall/cost 曲線並揀出符合 target 嘅**最低成本**組合。
  table 需有 `id` + `v0..v{d-1}` 欄位（同 `index_save` 一致）。
- API：`gtv_index::{kmeans_train, tune_ivf, tune_ivf_curve, select_tuned}`。

---

## 15. Filter-aware ANN + 精確 rerank（prod_p3 B3-3）

`ann(...)` 新增兩個可選參數，將 metadata filter 同向量檢索結合，並控制執行策略：

```text
ann(name, query, k [, metric [, filter [, strategy]]])
```

- `filter` — 逗號分隔嘅 **id allow-list**（例如 `'1,7,42'`）；`'*'`、空或省略
  代表「無 filter」。id 對應 index 自身嘅 id（同 `knn_from` / `index_save` 同一 domain）。
- `strategy` — `auto`（預設）按 filter selectivity 分派；`exact` 強制精確 oracle
  （監管 / 高風險）。未知值會被拒絕。

planner 按 `selectivity = allowed / total` 揀五種策略之一：

| selectivity | Flat / HNSW | IVF |
|---|---|---|
| `< 1%` | `pre_filter_exact` | `pre_filter_exact` |
| `1–20%` | `oversampled_hnsw` | `filtered_ivf` |
| `>= 20%` | `oversampled_hnsw` | `post_filter_rerank` |
| `strategy='exact'` | `exact` | `exact` |

- **精確 rerank**：ANN 候選（按 `k / selectivity * safety` 放大，有上限）會用原始
  `f32` 向量重新計分，所以即使候選集係近似，回報嘅距離仍然精確。
- **`ann_explain(name, query, k [, metric [, filter [, strategy]]])`** 回傳所選 plan
  嘅一行 telemetry：

  `strategy, oversample, exact_rerank, total_count, allowed_count, selectivity,
   candidate_count, filtered_count, recall_estimate, filter_us, ann_us,
   rerank_us, reason`

  `recall_estimate` 會抽最多 16 條 corpus 向量對精確 oracle 量度（`Exact` ⇒ 1.0）。
- Rust API：`gtv_index::{plan_ann, execute_ann, estimate_recall, recall_curve,
  AnnConfig}`；`execute_ann` 回傳 `RerankResult { hits, approx_scores,
  exact_scores }` 同 `AnnTelemetry`（strategy、candidate / filtered count、
  latency 分解、reason）。

---

## 16. 雙時間軸模型（prod_p3 B3-2）

每個 fact 可以帶兩條互相獨立嘅時間軸：**business time**（幾時為真）同
**system time**（系統幾時知悉）。Temporal CSR 只索引 business time；system time
交由不可變嘅 table version 處理，更正只會 append 新版本，永不覆寫歷史。

- `gtv_core::BitemporalRange { business_from, business_to, system_from,
  system_to }`（半開區間；`i64::MAX` = 無限期），提供 `contains`、
  `business_overlaps`、`system_overlaps`、`overlaps`。
- `bitemporal_edge_schema()` 喺 edge table 上加 `business_valid_from/to`、
  `system_valid_from/to`、`event_time`、`ingest_time`、`business_date`，同時
  **保留 legacy `valid_from/to` 欄位**，所以舊查詢結果不變。
  `migrate_legacy_edges(batch, system_from)` 做升級（`business_* = valid_*`、
  `system_from` 由 caller 指定、`system_to = MAX`）。
- `gtv_storage::BitemporalStore` 保存 append-only 嘅 system version：
  - `record(table, system_from, batches)` append 一個版本（同一 timestamp 重播
    係 idempotent；舊版本永不改動）；
  - `as_of_system(table, system_ts)` 重演「嗰時系統所知」；
  - `as_of(table, business_ts, system_ts)` 兩軸合用；
  - `overlaps(table, key_column)` 標出矛盾版本。
- `gtv_catalog::FsCatalog::snapshot_as_of(table, system_ts)` 喺不可變 snapshot log
  上解析同一個 system-time cut。
- SQL（先用 `GtvContext::register_bitemporal_version` 註冊版本）：

  ```sql
  SELECT * FROM as_of('edges', 50, 1500);             -- business 50 @ system 1500
  SELECT * FROM as_of('edges', 50);                   -- business 50 @ 最新已知
  SELECT * FROM bitemporal_overlaps('edges', 'src');  -- 矛盾版本
  ```

  `as_of(table, business_ts [, system_ts])` 嘅 `system_ts` 預設為最新版本；當
  business 時點唔喺任何區間內，會回傳一個 schema 正確嘅空結果。

---

## 17. Streaming 攝取（prod_p3 B3-1）

`gtv-ingest` 係一個 source-agnostic、at-least-once 嘅 **micro-batch** 管線，建於
B2-1 atomic catalog commit 之上：

```text
poll ─▶ dedup(event_id) ─▶ watermark / late policy ─▶ encode Arrow
     ─▶ Sink（atomic catalog commit；offset 記入 snapshot.summary）
     ─▶ commit offset（offset store + 來源） ─▶ persist dedup
```

- **Envelope**：`Envelope { source, partition, offset, event_id, event_time,
  ingest_time, schema_version, payload }`。`event_id` 係穩定嘅 16-byte dedup key
  （來源冇 native id 時用 `Envelope::id_from_offset`）。
- **`SourceAdapter`**：`name / poll(max) / commit(offsets) / seek(offsets) /
  lag`。`FileReplayAdapter`（JSONL、單 partition）永遠可用；Kafka
  （`feature = "kafka"`）/ Pulsar（`feature = "pulsar"`）係預留 seam，而
  `decode_json_envelope` 係 protocol-agnostic 嘅 JSON decoder。
- **`OffsetStore`**：append-only `metadata/offsets.jsonl`，per
  `(source, partition)` 單調，重啟可續，容忍尾部被截斷嘅一行。
- **`DedupStore`**：有界、最近見過嘅 `event_id` 集合，原子持久化；重播即 no-op。
- **Watermark / `LatePolicy`**：`watermark = max_event_time − allowed_lateness`；
  `Recompute` 照發布 late event、`Dlq` 送去死信隊列、`Drop` 只計數。
- **`DeadLetterQueue`**：
  `deadletter/<source>/<date>/<partition>-<seq>.parquet`，含 `error_code` /
  `error_message` / `rejected_at`；`list` / `read` / `read_envelopes`（reprocess）。
- **`Pipeline<A, S>`**：`poll_once`（含 backpressure）、`publish_once`、
  `run_once`、`run_bounded`；`StreamMetrics` 提供 `events_polled / published /
  duplicate / late / dropped / dlq`、`end_to_end_lag_ns`、`event_time_lag_ns`、
  `offset_lag`。
- **`CatalogSink`**：原子發布 — 數據 batch 同 `source_offsets` 落入同一個
  snapshot。commit 前 crash 嘅 batch 對讀者不可見、offset 未 commit
  （at-least-once）；dedup + catalog idempotency key 令重播 effectively-once。

```rust
let sink = CatalogSink::open("catalog_home", "events")?;
let offsets = OffsetStore::open("catalog_home")?;
let dlq = DeadLetterQueue::new("catalog_home");
let dedup = DedupStore::open("catalog_home/metadata/dedup.txt", 1_000_000)?;
let mut p = Pipeline::new(
    FileReplayAdapter::open("events.jsonl", "feed")?,
    sink, offsets, dlq, dedup,
    StreamConfig::default(),
    WatermarkConfig { allowed_lateness_ns: 5_000_000_000, policy: LatePolicy::Dlq },
);
p.run_bounded(10_000)?;
```
