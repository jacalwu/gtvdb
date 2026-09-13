# gtvdb User Menu

gtvdb is a single-engine in-memory database combining **Graph + Temporal + Vector +
Columnar**. It is driven through an interactive SQL REPL (`gtv`) with two session
modes:

- **`hft`** (default) — a latency-first thin subset with abbreviated operators and a
  precompiled `KernelPlan` hot path (kdb+/q-style shorthand).
- **`full`** — complete DataFusion SQL (JOIN / GROUP BY / window / CTE / subquery).

---

## 1. Build & Launch

```sh
./rebuild.sh                    # build release gtv (rerun after any crates/ change)
cargo run --release -p gtv-cli --bin gtv
# or run the prebuilt binary
./target/release/gtv
```

```text
gtv> help                      # list commands
gtv> tables                    # show the demo tables
gtv> quit                      # exit
```

Pre-registered demo tables:

| table    | columns |
|----------|---------|
| `nodes`  | id, value |
| `edges`  | src, dst, edge_type, valid_from, valid_to (Int64 ns) |
| `prices` | t, price |
| `ticks`  | t, bid, ask, bid_sz, ask_sz |
| `orders` | id, price, qty, mid, smp |
| `book`   | sym, level, bid_px, ask_px, bid_sz, ask_sz |
| `t`      | time, price, bid, ask, bid_size, ask_size, native_flag (as-of-joined trade/quote) |

---

## 2. Session Modes & Timing

```sql
ALTER SESSION SET sqlmode = hft;    -- default: thin subset + short names
ALTER SESSION SET sqlmode = full;   -- full DataFusion SQL
SET DURATION = ON;                  -- print each action's compute time (µs)
SET DURATION = OFF;
```

HFT-mode shorthands (no full `SELECT` needed):

```text
ticks                -- bare table name  == SELECT * FROM ticks
pit(500)             -- bare table fn    == SELECT * FROM pit(500)
```

---

## 3. Loading Data

### 3.1 Load CSV / Parquet from disk

```text
gtv> loadcsv ticks /data/ticks.csv        # CSV -> session table `ticks`
gtv> LOAD CSV '/data/ticks.csv' INTO ticks   # doc-style alias (same as loadcsv)
gtv> load    ticks /data/ticks.parquet    # Parquet -> session table `ticks`
gtv> save    ticks /data/ticks.parquet    # write any registered table to Parquet
```

CSV is comma-separated with a header row; the schema is inferred (a `timestamp`
column is read as `Int64` nanoseconds). Example tick CSV:

```text
symbol,timestamp,price,volume,bid_price_1,ask_price_1,bid_size_1,ask_size_1
0700.HK,0,100.5,100,100.4,100.6,10,20
3690.HK,500,200.1,80,200.0,200.2,30,40
```

### 3.2 Load into a session variable (full mode)

```sql
SELECT * FROM read_csv('/data/ticks.csv');
SELECT * FROM read_parquet('/data/ticks.parquet');
CREATE TABLE t AS SELECT symbol, price FROM read_csv('/data/ticks.csv');
```

### 3.3 Background import (producer thread)

```text
gtv> bgload ticks /data/ticks.csv 200        # re-import every 200 ms
gtv> bgload ticks /data/ticks.parquet 200    # CSV vs Parquet auto-detected by extension
gtv> SELECT count(*) FROM ticks;              # session keeps reading the latest data
```

### 3.4 Live streaming (London Strategic Edge WebSocket)

```sh
export LSE_API_KEY=lse_live_...              # London Strategic Edge live API key
gtv> live q BTC/USD ETH/USD SOL/USD          # stream live quote ticks into table `q`
gtv> SELECT symbol, count(*) FROM q GROUP BY symbol;   # full mode
```

Streams `(symbol, price, bid, ask, ts)` ticks over `wss://ws.londonstrategicedge.com`
(auth `{action:"auth",api_key}`, subscribe `{action:"subscribe",symbol}`). The free
live key covers **crypto** symbols (`BTC/USD`, `ETH/USD`, …); stock/forex symbols may
require a higher tier or a different symbol format.

### 3.5 Fetch historical ticks (London Strategic Edge REST API)

```text
gtv> fetch mco MCO 20000          # pull historical ticks for MCO into `mco`
gtv> fetch tsla TSLA 100000       # (symbol, ts, price, bid, ask, volume)
gtv> SELECT count(*) FROM mco;
```

Backed by `GET https://api.londonstrategicedge.com/tickdata?symbol=eq.<sym>`.
Uses the `LSE_API_KEY` env var when set, otherwise the site's public key. Note the
`lse_live_*` key is WebSocket-only and returns `Unauthorized` on the REST API.
Pagination is automatic: the API caps each request at 10,000 rows, so `fetch`/
`read_tickdata` page with a `ts` keyset cursor up to `limit`.

### Static data cache (per-day directories)

`yahoo`/`fetch`/`read_yahoo`/`read_tickdata` are **read-through cached**: before
hitting the web API they check `<GTV_DATA_DIR>/<source>/<date>/<symbol>.parquet`
(default `data/static`), and on a miss they fetch once and write per-day Parquet
files, so subsequent runs are fully offline.

```sh
export GTV_DATA_DIR=/data/gtv_static   # optional; default ./data/static
gtv> yahoo hsi ^HSI --range 3mo        # 1st: fetch + cache 64 day files
gtv> yahoo hsi ^HSI --range 3mo        # 2nd: cache hit, no network
find $GTV_DATA_DIR/yahoo -name '*.parquet' | head   # per-day files
```

```sql
-- full mode: same fetch as a SQL table function
SELECT * FROM read_tickdata('TSLA', 25000);
CREATE TABLE tsla AS SELECT * FROM read_tickdata('TSLA', 100000);
```

---

## 4. TC1–TC15 Use Cases

Operator naming: each kernel is registered under a **short name (hft)** and a
**full name (analysis)**, e.g. `pit` / `point_in_time`.

### TC1 — Cross-asset as-of join (multi-column + tolerance)

```sql
SELECT * FROM aj(0, 5, 15, 25);            -- short (hft)
SELECT * FROM asof_join(0, 5, 15, 25);     -- full
-- returns (t, price, spread), matching each left t to the latest right t within 500us
```

### TC2 — Order-flow imbalance (rolling 100-tick, fused)

```sql
SELECT t, ofi(bid, ask, bid_sz, ask_sz, 100) OVER (ORDER BY t) FROM ticks;
-- full name: order_flow_imbalance(...)
```

### TC3 — Wash-trade cycle detection (A→B→C→A)

```sql
SELECT * FROM wash(500);                   -- short (hft)
SELECT * FROM wash_trade(500);             -- full
-- returns (a, b, c) ring(3) node triples active at T=500
```

### TC4 — Vector K-NN

```sql
SELECT id FROM knn('songs', '0.1,0.1', 3);          -- short
SELECT id FROM vector_search('songs', '0.1,0.1', 3); -- full
-- returns top-3 nearest song ids (8, 0, 1)

-- full signature: knn(name, query, k [, label [, metric]])
SELECT id FROM knn('songs', '0.1,0.1', 3, 'pop');          -- search only label='pop'
SELECT id FROM knn('tss', '0.1,0.1', 3, '*', 'cosine');    -- '*' = no label filter; metric=cosine
-- metric ∈ l2 (default) | cosine | dot; the query metric must match the
-- collection, otherwise a MetricMismatch is returned (never a silent wrong metric).
```

### TC5 — Point-in-time order-book snapshot (O(log N) zero-copy)

```sql
SELECT * FROM pit(500);                    -- short (hft, KernelPlan fast path)
SELECT * FROM point_in_time(500);          -- full
-- returns the active index range [valid_from <= 500 < valid_to]
```

### TC6 — Pre-trade risk check (branchless)

```sql
SELECT count(*) FROM orders WHERE risk(price, qty, mid, smp);   -- short
SELECT count(*) FROM orders WHERE risk_ok(price, qty, mid, smp); -- full
-- 4 hard checks: price band (5%), max qty (10k), max notional (10M), self-match flag
```

### TC7 — Tick-to-trade end-to-end (decode → strategy → encode)

```sql
SELECT * FROM tick_to_trade('mco', 100);   -- short: ttrade('mco', 100)
-- returns (side, price, qty); side: 0 = buy, 1 = sell, 2 = none
```

Mean-reversion crossover signal over the trailing `mavg[window]`; e.g. over the
2.5M-row `stocks_MCO_tick.parquet` it emits ~2.5M rows in one compiled pass.

### TC8 — L2 order-book imbalance + micro-price

```sql
-- full mode (aggregate / scalar)
SELECT sym, obi(bid_sz, ask_sz) FROM book GROUP BY sym;              -- order_book_imbalance
SELECT sym, mp(bid_px, ask_px, bid_sz, ask_sz) FROM book WHERE level = 0;  -- micro_price
```

### TC9 — Streaming covariance matrix

```sql
SELECT * FROM covariance_matrix('returns', 3);  -- short: cov('returns', 3)
-- returns the m×m sample covariance as (i, j, cov)
```

Takes a registered returns table with columns `ret_0 .. ret_{m-1}` and returns the
`m × m` sample covariance.

### TC10 — Local matching engine (price-time priority)

```sql
SELECT * FROM match_orders('orderstream');     -- short: match('orderstream')
-- returns (side, price, qty) fill events
```

Level-aggregate price-time matching over a registered order table with columns
`side`, `is_mkt`, `price`, `qty`.

### TC11 — Lee-Ready algorithm (quote rule + tick-rule fallback)

```sql
SELECT time, lr(price, bid, ask) OVER (ORDER BY time) AS tc11 FROM t;   -- lee_ready
```

### TC12 — Tick rule

```sql
SELECT time, tick(price) OVER (ORDER BY time) AS tc12 FROM t;           -- tick_rule
```

### TC13 — EMO (Ellis–O'Hara–Thomas)

```sql
SELECT time, emo(price, bid, ask) OVER (ORDER BY time) AS tc13 FROM t;
```

### TC14 — Level-1 order-flow imbalance (Cont)

```sql
SELECT time, ofil(bid, bid_size, ask, ask_size) OVER (ORDER BY time) AS tc14 FROM t; -- ofi_l1
```

### TC15 — Exchange aggressor flag

```sql
SELECT time, agg(native_flag) AS tc15 FROM t;                          -- aggressor_flag
-- 'B'/'BUY' -> 1, 'S'/'SELL' -> -1
```

### TC11–TC15 combined

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

## 5. Operator Reference

| TC | short (hft) | full (analysis) | type |
|----|-------------|-----------------|------|
| TC1 | `aj` | `asof_join` | table fn |
| TC2 | `ofi` | `order_flow_imbalance` | window |
| TC3 | `wash` | `wash_trade` | table fn |
| TC4 | `knn` | `vector_search` | table fn |
| TC5 | `pit` | `point_in_time` | table fn |
| TC6 | `risk` | `risk_ok` | scalar |
| TC7 | `ttrade` | `tick_to_trade` | table fn |
| TC8 | `obi` | `order_book_imbalance` | aggregate |
| TC8 | `mp` | `micro_price` | scalar |
| TC9 | `cov` | `covariance_matrix` | table fn |
| TC10 | `match` | `match_orders` | table fn |
| TC11 | `lr` | `lee_ready` | window |
| TC12 | `tick` | `tick_rule` | window |
| TC13 | `emo` | `emo` | window |
| TC14 | `ofil` | `ofi_l1` | window |
| TC15 | `agg` | `aggressor_flag` | scalar |

Plus built-ins: `mavg` / `msum` / `deltas` (window), `neighbors` (table fn),
`read_csv` / `read_parquet` (table fn).

Quant operators (phase 2):

| short | full | type |
|-------|------|------|
| — | `bs_price` / `bs_delta` / `bs_gamma` / `bs_vega` / `bs_theta` | scalar |
| `var` | `var_historical` | table fn |
| — | `pca` | table fn |
| `l2` | `reconstruct_l2` | table fn |
| — | `xbar` | scalar (time bucket) |
| — | `ohlc` | table fn (tick → OHLCV bars) |
| — | `zscore` / `momentum` | window (cross-sectional / technical) |
| — | `signal` | scalar (z → buy/sell/hold) |
| `trunc` | — | scalar: Oracle-style time bucket `trunc(ts, 'DD')` (SS/MI/HH/DD/MM/YYYY) and numeric trunc toward zero `trunc(x[, digits])` |
| — | `read_yahoo` | table fn (daily OHLCV) |
| — | `cross_sectional_signal` | table fn (fetch+momentum+zscore+signal+next-day ret) |
| — | `relative_strength` | table fn (target vs indices vs peers) |

---

## 6. Shell Commands

```text
help | tables | providers | quit
neighbors <node> [T]       khop <node> <k> [T] [--max-edges N] [--max-frontier N] [--max-degree N]
mavg <n> | msum <n> | deltas
asof [t ...]               knn <node> [k] [--mask ids]
knn_from <name> <table> [dim] [metric]   # metric = l2 (default) | cosine | dot
embedding_register <name> <table>        # register a standard (governed) embedding table
embedding_build <name> <root> <table> [type] [metric]   # governed index (flat|ivf|hnsw)
save <table> <path>        load <table> <path>
loadcsv <table> <path>     bgload <table> <path> [ms]
LOAD CSV '<path>' INTO <table>          doc-style CSV import (alias of loadcsv)
live <table> <symbol...>   stream LSE live ticks (needs LSE_API_KEY)
fetch <table> <symbol> [limit]  pull LSE historical ticks (REST API)
yahoo <table> <symbol...> [--range 1y]  pull daily OHLCV from Yahoo
md klines <provider> <table> <code...> [--period 1d] [--start] [--end] [--max] [--adjust]
md ticks  <provider> <table> <code...> [--max N]   market data -> named session table
hdb_save <table> <date> [root]  persist table to HDB partitions
hdb_load <table> <date> <sym> [root]  read one HDB partition
hdb_scan <table> <start> <end> [sym] [root]  scan HDB date range
hdb_flush <table> [root] [secs]  background HDB flush (sym-enumerated)
tt <table> <T>             pattern [T]        delta
udf [x ...]                remote <host:port> <sql>
metrics                                  # engine counters + SQL latency histogram + workload telemetry (Prometheus text)
workload                                 # per-class workload admission / isolation status (§19)
cbo [on|off|recall R]                    # multimodal cost-based optimizer status / config (§18)
scenario_load <table>                    # load versioned scenarios (§20)
hierarchy_load <kind> <table>            # load effective-dated hierarchy edges
refdata_load <table>                     # load effective-dated reference values
master_load <kind> <table>               # load master data for master_get(...)
crm_rating_load <table>                  # load CRM rating maps (map_name, key, value)
```

Market/trend functions (provider is just the first argument, see §7):

```text
providers                            # list registered providers
klines('futu','HK.00700','1d','2024-06-03','2024-06-07')   # bare call = hft shorthand
SELECT * FROM klines('yahoo','0700.HK','1d',...);           # standard SQL (any mode)
fwd_proba('table',H,K)              # P(up/down) over the next H trading days
fwd_walk('table',H,K[,warmup])      # strictly-causal walk-forward replay
fwd_regress('table',asof_ns,H,K)    # as-of regression: direction+band vs realised
align('table',freq_sec,'ffill'|'drop')      # multi-symbol alignment on a regular grid
backtest('table',cost_bps,stop,tp[,'SYM']) | bt_report(...)    # single-asset state-machine backtest
pf_backtest('table',cost_bps) | pf_report(...)                # equal-weight portfolio + rebalance
dq_report('table') | dq_check('table') | health_check('table')   # data quality & freshness
strategy_stats('table')              # hit / Brier / ECE / PSI over decision rows (up + p_up)
khop(src, k, valid_at[, max_hops, max_edges])   # resource-bounded k-hop BFS: visited bitmap + budget
                                                # returns (hop, dst); over-budget -> BudgetExceeded
crm_alloc('loan_exposure','collateral','guarantee','collateral_edges','guarantee_edges','BASE',T[, 'ead'])  # per-loan CRM cover (§8)
crm_audit('loan_exposure','collateral','guarantee','collateral_edges','guarantee_edges','BASE',T[, 'ead'])  # audit trail (§8)
embedding_search(name, q, k [, tenant [, as_of]])  # governed vector search: provenance + tenant + expiry (§12)
cbo_explain(name, query, k [, metric [, filter [, strategy]]])  # CBO plan choice + estimated cost (§18)
workload_status()                    # per-class admission / resource telemetry (§19)
resolve_scenario(name [, version])   # resolve inheritance / override with provenance (§20)
hierarchy_ancestors(kind, node, as_of)      # effective-dated ancestors (§20)
hierarchy_descendants(kind, node, as_of)    # effective-dated descendants
refdata_get(domain, key, as_of)      # effective-dated reference value
master_get(kind, id, as_of)          # master attributes (one row each)
crm_rating_map()                     # CRM rating / type maps (map_name, key, value)
metrics                              # engine counters (see above)
```

---

## 7. Market-Data Providers & Trend Analysis

### 7.1 Unified provider framework

Any source (Futu OpenD, Yahoo Finance, your own registered provider) implements
the same `MarketProvider` trait and is looked up by name — the interface is
identical and the provider is just the first function argument:

```sql
-- Standard SQL (any mode):
SELECT * FROM klines('futu', 'HK.00700', '1d', '2024-06-03', '2024-06-30');
SELECT * FROM ticks('futu', 'HK.00700', 100);       -- session ticks (intraday, LV2)
-- REPL hft mode also accepts the bare shorthand:
klines('yahoo', '0700.HK', '1d', '2024-01-01', '2024-03-01')
```

- Unified schema: `provider, symbol, ts (UTC epoch ns), open/high/low/close, volume, turnover, adjclose` (ticks add direction/sequence)
- Signature: `klines(provider, code, period[, start][, end][, max][, adjust])`; period `1m..1Y`, dates `YYYY-MM-DD` inclusive
- `futu`: local OpenD (`127.0.0.1:11111`) + `futu-api`, codes `HK.00700/US.AAPL`; `yahoo`: keyless, codes `0700.HK/AAPL`, no ticks
- Engine-level **cache-first + incremental fill** (static history fetched once):
  root `GTV_MARKET_DIR` (default `data/market`), `GTV_MARKET_CACHE=0` disables

The `md` command registers fetched market data as a named session table
(for `fwd_*` / SQL / HDB):

```text
gtv> md klines futu hk700 HK.00700 --period 1d --start 2024-01-01 --end 2024-12-31
```

### 7.2 7–14 trading-day trend signal (historical-analog kNN)

Built-in features (mom5/mom10/vol10/above_sma20 — or custom columns via `feats`)
are z-scored, then the K most similar past days are found;
`p_up/p_down` = share of those analog days whose next-H actual move was up/down:

```text
gtv> md klines futu hk700 HK.00700 --period 1d --start 2020-01-01
SELECT * FROM fwd_proba('hk700', 10, 20);    -- decision bar = last row: p_up, p_down, hit_rate
```

- `fwd_walk('table', H, K)` — strictly-causal replay (each bar only uses the
  past), one forecast/outcome row per evaluated bar; feeds `stock_calib.sh`
- `fwd_regress('table', asof_ns, H, K)` — treat a past day as “today”, output
the direction probabilities + predicted price band (`pred_lo/pred_hi`) and
compare with the realised future path

### 7.3 One-shot scripts

```bash
./stock_analysis.sh HK.00700          # SOURCE=futu default; exit 0 no signal / 3 alert (THRESHOLD default 0.75, adjustable)
./stock_calib.sh HK.00700             # walk-forward calibration: p buckets vs realised + threshold suggestion
./stock_regress.sh HK.00700 2026-08-03 2026-07-01   # as-of regression: hit/coverage/return error
# Source/params: SOURCE=yahoo|parquet, HORIZON=10, K=20, THRESHOLD=0.8, START/END, FILE=(parquet reuse)
```

### 7.4 Quant research toolkit & portfolio monitoring (function2.md — Phase A + C)

New engine functions (window functions need `ALTER SESSION SET sqlmode = full`):

```sql
-- technical indicators (causal trailing-window, NaN warm-up), PARTITION BY symbol allowed:
-- ema(n), rsi(n), macd_dif/dea/hist(f,s,g), atr(n), boll_mid/up/lo(n,k), vwap(n)
SELECT t, rsi(close,14) OVER (PARTITION BY symbol ORDER BY t) FROM bars;
-- multi-symbol alignment + cross-sectional factors
SELECT ts, symbol, close, zscore(close) OVER (PARTITION BY ts)
FROM align('multi', 86400000000000, 'ffill');        -- freq in ns, fill = ffill | drop
-- backtests: single-asset (position/cost/stop/tp) and equal-weight portfolio + rebalance
SELECT * FROM bt_report('sig', 20, 0.06, 0.10);     -- cost_bps, stop_loss, take_profit
SELECT * FROM pf_report('multisig', 20);
-- data quality / freshness / strategy drift
SELECT * FROM health_check('hk03668', 7, 200);      -- freshness vs market calendar
SELECT * FROM strategy_stats('decisions');          -- needs up + p_up/cal_p_up_* columns
-- engine metrics: REPL command `metrics`
```

Holdings daily-deck scripts (ZH=1 prints Chinese stock names/headers):

```bash
./holdings_forecast.sh                  # direction(up/flat/down) + action(BUY/HOLD/SELL) + sim evidence + HSI context
ZH=1 ./holdings_forecast.sh             # 中文版（HOLDING.txt 第二欄為中文股名）
EVENT_MODE=1 ./holdings_forecast.sh     # event risk overlay (threshold -> 0.80, gap appendix)
./stock_sim.sh HK.00857                 # paper-trade sim: THR×cost grid, SIZING, long/short side means
./holdings_sim.sh                       # per-stock action summary (--run refreshes sims)
```

Per-holding horizon/threshold via `HOLDING.cfg` (e.g. `HK.00857 20 0.85`).
Specs/status: `function2.md` (Phase A+C), `forecast-function.md` (implemented / not-done + reasons), `design.md`.

See `doc/market-providers.md` and `stock_analysis.md` for details and known
boundaries (Futu has no historical tick dumps, Yahoo intraday lookback caps).

---

## 8. CRM Allocation (credit-risk mitigation)

Greedy CRM (credit-risk-mitigation) allocator over the five tables of
`crm-allocation.md` (repo root):

* one loan may carry many collaterals **and** many guarantors, and vice versa
  (multi-to-multi graph);
* collateral capacity is haircut-adjusted up-front:
  `C × (1 − haircut − fx_haircut − maturity_mm)` (floored at 0);
* `specified` (contract-locked) edges are allocated **first**; remaining
  `optimizable` edges go to a deterministic greedy / priority allocator
  (sources & loans ranked by priority — ties broken by ascending id);
* guarantee phases always follow collateral phases, so a guarantee covers the
  residual exposure: `CRMg = min(G, E − CRMc)`;
* every allocation is recorded in an audit trail (auditable / replayable).

Load the five CSVs (schema + generator in `crm-allocation.md` §1–§3), then
allocate:

```text
gtv> LOAD CSV 'loan_exposure.csv'     INTO loan_exposure
gtv> LOAD CSV 'collateral.csv'        INTO collateral
gtv> LOAD CSV 'guarantee.csv'         INTO guarantee
gtv> LOAD CSV 'collateral_edges.csv'  INTO collateral_edges
gtv> LOAD CSV 'guarantee_edges.csv'   INTO guarantee_edges

gtv> SELECT * FROM crm_alloc('loan_exposure','collateral','guarantee',
        'collateral_edges','guarantee_edges','BASE',-1);
```

Arguments: `crm_alloc(loan_tbl, coll_tbl, guar_tbl, coll_edges_tbl,
guar_edges_tbl, scenario, T [, exposure_col [, method]])`:

* `scenario` filters `scenario_id` rows (`''` or `'*'` = any scenario);
* `T` = as-of instant in ns — only rows with `valid_from <= T < valid_to`
  participate; `-1` = no temporal filter;
* optional `exposure_col` = loan exposure to mitigate (default `ead`,
  e.g. `'pv'`);
* optional `method` = `'greedy'` (default) | `'haircut_efficiency'` |
  `'lp'`:
  - `greedy` — collateral by type quality (CASH > BOND > EQUITY), loans by
    `pd` (riskier first);
  - `haircut_efficiency` — collateral consumed by haircut-adjusted **effective
    value** (`C × (1 − Hc − Hfx − Hmm)`, largest first), loans by **risk
    weight** (`rw` column → rating map → `pd`); same Phase 1 + allocation rule,
    only the ordering keys change;
  - `lp` — exact LP (needs `crm-lp` feature).

Output (per loan in the slice):
`loan_id, exposure, collateral_cover, guarantee_cover, net_exposure`.

Full audit trail (same inputs, same deterministic run):

```text
gtv> SELECT * FROM crm_audit('loan_exposure','collateral','guarantee',
        'collateral_edges','guarantee_edges','BASE',-1);
```

Audit columns: `seq, stage (specified|greedy|lp), source_kind
(collateral|guarantee), source_id, loan_id, amount, source_remaining,
loan_remaining`.

Consistency checks / reports:

```sql
-- loans inside the as-of/scenario slice:
SELECT count(*) FROM crm_alloc('loan_exposure','collateral','guarantee',
        'collateral_edges','guarantee_edges','BASE',-1);
-- exposure conservation: Σ(collateral_cover + guarantee_cover + net_exposure) = Σ ead
SELECT sum(collateral_cover + guarantee_cover + net_exposure) AS exposure_total,
       min(net_exposure) AS min_net
FROM crm_alloc('loan_exposure','collateral','guarantee',
        'collateral_edges','guarantee_edges','BASE',-1);
-- where the cover came from:
SELECT stage, source_kind, count(*) AS allocs, sum(amount) AS amount
FROM crm_audit('loan_exposure','collateral','guarantee',
        'collateral_edges','guarantee_edges','BASE',-1)
GROUP BY stage, source_kind;
```

Greedy vs LP (`method`): Phase 1 (specified) is shared and byte-identical;
only the optimizable pool differs.

```text
cargo build -p gtv-cli --features gtv-engine/crm-lp     # enable Phase 3 (microlp)
gtv> SELECT * FROM crm_alloc('loan_exposure','collateral','guarantee',
        'collateral_edges','guarantee_edges','BASE',-1,'ead','greedy');  -- heuristic
gtv> SELECT * FROM crm_alloc('loan_exposure','collateral','guarantee',
        'collateral_edges','guarantee_edges','BASE',-1,'ead','lp');      -- exact LP
-- risk-weighted residual: LP minimises Σ pd × net_exposure over the optimizable pool
SELECT round(sum(r.net_exposure*l.pd),2) AS risk_weighted_net
FROM crm_alloc('loan_exposure','collateral','guarantee',
        'collateral_edges','guarantee_edges','BASE',-1,'ead','lp') r
JOIN loan_exposure l USING (loan_id);
```

Behaviour notes:

* Scenario / as-of filtering is applied per table. Edges whose endpoint was
  filtered out of the slice are inert and dropped; an edge that references an
  id that never existed in the source tables aborts the run (data error).
* Default priorities: loans by `pd` (riskier covered first), collaterals by
  `type` (CASH > BOND > EQUITY), guarantors by `rating` (AAA > AA > A …).
  Adding a `priority` column to any of the five tables overrides the default
  for that table.
* Running a scenario with no rows (e.g. `STRESS` before it is loaded) returns
  an empty result — not an error.
* `method='lp'` builds an exact linear program over the optimizable edges
  (`x_e ≥ 0`, loan-demand + source-capacity rows) **and honours the per-edge
  contractual caps** carried by `collateral_edges.ratio` and
  `guarantee_edges.amount`; the greedy phase 2 deliberately ignores those caps
  (reference semantics), so the two methods only differ when caps bind.
  Without the feature, `method='lp'` returns a clear rebuild hint.
* Kernels (pure Rust, callable directly): `gtv_array::crm::crm_alloc_greedy`
  (phases 1+2) and `gtv_array::crm_lp::crm_alloc_lp` (phase 3, `crm-lp`
  feature; solver backend via `good_lp` — `microlp` by default, swap the
  `good_lp` feature for `highs` on large models).

---

## 9. Performance Notes

- In `hft` mode, `pit` / `wash` / `aj` / `ofi` / bare table scans are compiled once
  into a **KernelPlan** (cached by query text) and executed directly on the
  compiled kernels — no DataFusion planning on the hot path.
  `SET DURATION = ON` shows the compute time in µs, e.g. `pit(500)` ~3 µs after the
  first compile vs ~1.5 ms through the DataFusion path.
- `tick_rule`/`lee_ready`/`emo` use an explicit sign (`>0 → 1, <0 → -1, =0 → 0`);
  Rust's `f64::signum` returns `1.0` for `+0.0` and is intentionally avoided.

---

## 10. Notes

- The `full` mode is the complete DataFusion SQL surface; the `hft` mode is the
  latency-first subset (no JOIN / GROUP BY / CTE / subquery).
- Real tick samples (`stocks_{MCO,NVDA,TSLA}_tick.parquet`, ~2.5M rows each, from
  London Strategic Edge) live under `testcase/hft/data/`:
  ```text
  gtv> load mco testcase/hft/data/stocks_MCO_tick.parquet
  gtv> SELECT count(*) FROM tick_to_trade('mco', 100);
  ```

---

## 11. Vector metrics & graph-traversal budgets (prod_p1)

- **Distance metric**: `l2` (default, squared Euclidean), `cosine` (`1 - cos`;
  the index unit-normalizes rows and the query), `dot` (inner product, stored as
  the negative inner product so "lower = closer" holds uniformly). The metric and
  dimension are fixed at build time; a mismatched query metric returns
  `MetricMismatch` and a mismatched dimension returns `DimensionMismatch`. `dot`
  is not a metric (it violates the triangle inequality), so ANN recall can drop —
  prefer `cosine`.
- **`knn_from <name> <table> [dim] [metric]`** registers a collection from a
  table (default L2); a query may also override with
  `knn(name, q, k, '*', metric)` where `'*'` means "no label filter".
- **`khop(src, k, valid_at[, max_hops, max_edges])`** is a resource-bounded k-hop
  BFS. Each node is visited at most once (visited bitmap) and frontiers are
  deterministically ordered; exceeding hops / edges / frontier / rows / memory /
  deadline returns `BudgetExceeded` instead of OOM. The CLI also supports
  `--max-frontier` and `--max-degree` (a high-degree guard that rejects a node
  above the limit unless an edge predicate is supplied).
- **Adaptive TemporalCSR**: each source's contiguous edge run is binary-searched
  on `valid_from` plus a per-64-edge zone map on `valid_to`, giving ~17x lower
  latency than a linear scan on high-degree nodes (low degree keeps the linear
  fast path).
- **Index lifecycle (`gtv-index-store`)**: `index_save <name> <root> <table> [type] [metric]`
  builds a `flat` / `ivf` / `hnsw` index from a table and persists it as
  `<root>/<index_id>/v<n>/index.gtvidx` (manifest + payload + blake3 checksum);
  `index_load <name> <root> [version]` loads it for the SQL
  `ann(name, query, k [, metric])` function. Versions swap atomically through
  `CURRENT`, supporting shadow builds, atomic swap and rollback; a container or
  payload checksum mismatch refuses to load.

---

## 12. Governed embedding search (prod_p2 B2-4)

A standard embedding table (`gtv-catalog::embedding_schema`) carries provenance
and lifecycle on every row:

```text
entity_id, embedding: FixedSizeList<Float32>[dim],
model_id, model_version, tokenizer_version, dimension,
distance_metric, normalized, created_at,
effective_from, effective_to, source_hash,
feature_version, tenant_id, classification
```

- **`embedding_register <name> <table>`** registers a governed collection from
  a session table. Registration validates that the `dimension` column matches
  the `FixedSizeList` length, that a batch uses a single model / version / dim /
  metric / normalized flag, that `source_hash` is non-empty and that
  `effective_from <= effective_to`; violations are rejected outright (no silent
  indexing).
- **`embedding_search(name, q, k [, tenant [, as_of]])`** searches with `q` (a
  comma-separated float vector, e.g. `'1.0,0.0'`) and returns
  `(id, distance, model_id, model_version, source_hash, feature_version)`.
  - `tenant` limits results to that tenant; `'*'` is an admin cross-tenant view;
    omitted means no tenant filter. Queries never cross tenants otherwise.
  - `as_of` keeps only rows with `effective_from <= as_of < effective_to` (a
    null `effective_to` is open-ended), so expired embeddings are never hit.
  - Distance is reported like `knn` / `ann` (L2 becomes Euclidean).
- **`embedding_build <name> <root> <table> [type] [metric]`** runs the same
  validation plus expiry/tenant filtering, then builds a `flat` / `ivf` / `hnsw`
  index and persists it; a spec that disagrees with the data on dimension / model
  / metric is refused. The manifest records `feature_version` and `normalized`,
  so every hit is traceable to its model, source hash and feature version.
- **Governance**: expiry, replacement and rebuild all go through the same
  validation + `filter_active` choke point; any retrieval can list `model_id` /
  `version` / `source_hash` / `feature_version` / `tenant_id`.

---

## 13. Data-quality & publish gate (prod_p2 B2-5)

Rules are declared as JSON (inline, `@file` or a `.json` path) and cover six
checks: completeness (non-null ratio), uniqueness (PK), freshness (event-time
lag vs now), range (numeric bounds), referential (child → parent integrity) and
reconciliation (source/target row count and amount sum).

- **`dq_gate <table> <rules> [execution_id]`** evaluates the rules and prints
  `rule / target / PASS|FAIL / observed / threshold / message` per rule. Any
  failure returns an error (blocks) and records the decision (with the
  execution id) under `GTV_HOME`.
- **`dq_override <table> <rule> <approver> <reason...>`** waives a failing rule
  and appends an override to the append-only ledger (approver, reason, time,
  target).
- **`publish <table> <rules> [execution_id]`** runs the gate first; without an
  override for every failing rule it refuses to write to the catalog, otherwise
  it commits the snapshot.
- **`dq_audit`** lists every gate decision (pass / overridden / failures /
  exec_id) and every override (rule / approver / reason).
- **SQL**: `SELECT * FROM dq_gate('table', '<rules_json>')` returns
  `(rule, target, passed, observed, threshold, message)`, further aggregatable in
  SQL.

Example rules JSON:

```json
[{"rule":"completeness","column":"price","min_ratio":0.99},
 {"rule":"uniqueness","columns":["sym"]},
 {"rule":"freshness","event_time_col":"ts","max_lag_ns":86400000000000},
 {"rule":"range","column":"price","min":0.0,"max":1000000.0},
 {"rule":"referential","child_col":"sym","parent":"syms","parent_col":"sym"},
 {"rule":"reconciliation","name":"src->tgt","source_rows":10,"target_rows":10,
  "tolerance":0.001,"source_sum":100.0,"target_sum":100.0}]
```

> Overrides are keyed by rule kind (one target + rule waives every failure of
> that kind). The existing `dq_report` / `dq_check` / `health_check` stay
> diagnostic and never block.

---

## 14. IVF k-means coarse quantizer (prod_p3 B3-4)

The `IvfIndex` coarse quantizer is upgraded from evenly-spaced sampling to
**k-means++ / Lloyd**:

- `KMeansConfig { nlist, max_iters(25), restarts(3), sample, seed, split_oversized,
  split_threshold_k }`; large corpora train on a 50k sample by default; **the same
  seed + data yields identical centroids**.
- **Empty cells** are reseeded to the farthest point so every cell is dense;
  **oversized cells** (count > μ + kσ, `split_oversized = true`) are split and
  refined automatically.
- `KMeansConfig::for_nlist(n)` is the conservative default (no split, full
  corpus); `IvfIndex::with_metric` uses k-means while `IvfIndex::with_uniform`
  keeps the old uniform sampling as a baseline / fallback.
- Training metadata (`kmeans` / `seed` / `restarts` / `iters` / `sample` /
  `inertia`) is persisted in the `GIVFv2` payload; legacy `GIVFv1` still loads
  (no format regression).
- `CellStats` / `RetrainTrigger::PopulationImbalance` detect unbalanced cell
  populations and flag a retrain.
- **`index_tune <table> [target_recall] [k]`** measures Recall@K and scan cost
  (probed rows) over an `(nlist, nprobe)` grid, prints the recall/cost curve and
  picks the **cheapest** combination meeting the target. The table needs
  `id` + `v0..v{d-1}` columns (same shape as `index_save`).
- API: `gtv_index::{kmeans_train, tune_ivf, tune_ivf_curve, select_tuned}`.

---

## 15. Filter-aware ANN + exact rerank (prod_p3 B3-3)

`ann(...)` takes two extra optional arguments that combine a metadata filter with
vector search and control the execution strategy:

```text
ann(name, query, k [, metric [, filter [, strategy]]])
```

- `filter` — a comma-separated **id allow-list** (e.g. `'1,7,42'`); `'*'`, empty
  or omitted means "no filter". Ids match the index's own ids (same domain as
  `knn_from` / `index_save`).
- `strategy` — `auto` (default) dispatches on filter selectivity; `exact` forces
  the exact oracle (regulatory / high-risk). Unknown values are rejected.

The planner picks one of five strategies from `selectivity = allowed / total`:

| selectivity | Flat / HNSW | IVF |
|---|---|---|
| `< 1%` | `pre_filter_exact` | `pre_filter_exact` |
| `1–20%` | `oversampled_hnsw` | `filtered_ivf` |
| `>= 20%` | `oversampled_hnsw` | `post_filter_rerank` |
| `strategy='exact'` | `exact` | `exact` |

- **Exact rerank**: ANN candidates (oversampled by `k / selectivity * safety`,
  capped) are re-scored against the original `f32` vectors, so the reported
  distance is exact even when the candidate set is approximate.
- **`ann_explain(name, query, k [, metric [, filter [, strategy]]])`** returns one
  row of telemetry for the chosen plan:

  `strategy, oversample, exact_rerank, total_count, allowed_count, selectivity,
   candidate_count, filtered_count, recall_estimate, filter_us, ann_us,
   rerank_us, reason`

  `recall_estimate` samples up to 16 corpus vectors against the exact oracle
  (`Exact` ⇒ 1.0).
- From Rust: `gtv_index::{plan_ann, execute_ann, estimate_recall, recall_curve,
  AnnConfig}`; `execute_ann` returns `RerankResult { hits, approx_scores,
  exact_scores }` plus `AnnTelemetry` (strategy, candidate / filtered counts,
  latency breakdown, reason).

---

## 16. Bitemporal time model (prod_p3 B3-2)

Every fact can carry two independent time axes: **business time** (when it is
true) and **system time** (when the system knew it). The temporal CSR keeps
indexing business time only; system time is versioned by immutable table
versions, so a correction appends a new version instead of overwriting history.

- `gtv_core::BitemporalRange { business_from, business_to, system_from,
  system_to }` (half-open; `i64::MAX` = open-ended) with `contains`,
  `business_overlaps`, `system_overlaps` and `overlaps`.
- `bitemporal_edge_schema()` extends the edge table with
  `business_valid_from/to`, `system_valid_from/to`, `event_time`, `ingest_time`
  and `business_date` while **keeping the legacy `valid_from/to` columns**, so
  old queries are unchanged. `migrate_legacy_edges(batch, system_from)` performs
  the upgrade (`business_* = valid_*`, given `system_from`, `system_to = MAX`).
- `gtv_storage::BitemporalStore` holds append-only system versions:
  - `record(table, system_from, batches)` appends a version (re-recording the
    same timestamp is idempotent; older versions are never mutated);
  - `as_of_system(table, system_ts)` replays "what the system knew at";
  - `as_of(table, business_ts, system_ts)` combines both axes;
  - `overlaps(table, key_column)` flags contradictory versions.
- `gtv_catalog::FsCatalog::snapshot_as_of(table, system_ts)` resolves the same
  system-time cut over the immutable snapshot log.
- SQL (register versions with `GtvContext::register_bitemporal_version`):

  ```sql
  SELECT * FROM as_of('edges', 50, 1500);             -- business 50 @ system cut 1500
  SELECT * FROM as_of('edges', 50);                   -- business 50 @ latest known
  SELECT * FROM bitemporal_overlaps('edges', 'src');  -- contradictory versions
  ```

  `as_of(table, business_ts [, system_ts])` defaults `system_ts` to the latest
  version and returns a schema-typed empty result when the business instant is
  outside every interval.

---

## 17. Streaming ingestion (prod_p3 B3-1)

`gtv-ingest` is a source-agnostic, at-least-once **micro-batch** pipeline on top
of the B2-1 atomic catalog commit:

```text
poll ─▶ dedup(event_id) ─▶ watermark / late policy ─▶ encode Arrow
     ─▶ Sink (atomic catalog commit; offsets in snapshot.summary)
     ─▶ commit offsets (offset store + source) ─▶ persist dedup
```

- **Envelope**: `Envelope { source, partition, offset, event_id, event_time,
  ingest_time, schema_version, payload }`. `event_id` is a stable 16-byte dedup
  key (`Envelope::id_from_offset` when the source has no native id).
- **`SourceAdapter`**: `name / poll(max) / commit(offsets) / seek(offsets) /
  lag`. `FileReplayAdapter` (JSONL, single partition) is always available;
  Kafka (`feature = "kafka"`) / Pulsar (`feature = "pulsar"`) are reserved
  seams, and `decode_json_envelope` is the protocol-agnostic JSON decoder.
- **`OffsetStore`**: append-only `metadata/offsets.jsonl`, monotonic per
  `(source, partition)`, survives restart, tolerates a truncated trailing line.
- **`DedupStore`**: bounded most-recently-seen `event_id` set, persisted
  atomically; replays are no-ops.
- **Watermark / `LatePolicy`**: `watermark = max_event_time −
  allowed_lateness`; `Recompute` publishes late events, `Dlq` routes them to the
  dead-letter queue, `Drop` counts them.
- **`DeadLetterQueue`**:
  `deadletter/<source>/<date>/<partition>-<seq>.parquet` with `error_code` /
  `error_message` / `rejected_at`; `list` / `read` / `read_envelopes`
  (reprocess).
- **`Pipeline<A, S>`**: `poll_once` (with backpressure), `publish_once`,
  `run_once`, `run_bounded`; `StreamMetrics` reports `events_polled /
  published / duplicate / late / dropped / dlq`, `end_to_end_lag_ns`,
  `event_time_lag_ns` and `offset_lag`.
- **`CatalogSink`**: atomic publish — the data batch and `source_offsets` land
  in the *same* snapshot. A crash before the commit leaves the batch invisible
  and the offset uncommitted (at-least-once); dedup + the catalog idempotency
  key make the replay effectively-once.

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

---

## 18. Multimodal cost-based optimizer (prod_p3 B3-5)

`cbo_explain(...)` combines three statistic families — **relational** (catalog
commit-time column stats), **graph** (TemporalCSR degree histogram / active
ratio) and **vector** (ANN corpus size, selectivity, IVF recall curve) — into a
single `QueryPlanChoice`: filter-first vs ANN-first, recommended index type,
temporal-bitmap-first, graph source pruning and exact rerank, plus an estimated
cost:

```text
cbo_explain(name, query, k [, metric [, filter [, strategy]]])
```

It returns one row:

`table, enabled, strategy, index_type, filter_first, temporal_bitmap_first,
prune_graph_sources, exact_rerank, estimated_rows, estimated_cost, selectivity,
corpus_size, max_degree, reason`

Decision rules (`CostModel` defaults):

| Condition | Decision |
|---|---|
| `corpus <= flat_max_rows` (10000) | recommend `flat`, filter-first |
| `selectivity < prefilter_threshold` (1%) | filter-first |
| `selectivity < filtered_threshold` (20%) | recommend `ivf` for large corpora |
| otherwise | recommend `hnsw`, ANN-first |
| temporal active ratio `< temporal_bitmap_threshold` (20%) | build the temporal bitmap first |
| graph `max_degree > high_degree_threshold` (1000) | prune source nodes first |
| strategy is not `Exact` / `PreFilterExact` | add exact rerank |
| IVF recall curve `< recall_target` (0.90) | fall back from `ivf` to `hnsw` |

- **Freshness**: after `publish` the CLI calls `refresh_table_stats_by_name`, so a
  plan always uses the latest committed stats, never stale numbers. `EXPLAIN`
  shows catalog-level statistics; `cbo_explain(...)` shows the CBO-level
  `strategy` / `index_type` / `estimated_cost`.
- **CLI**: `cbo` prints the cost-model parameters and registered stats;
  `cbo on|off` toggles it (when off it falls back to the fixed B3-3 strategy and
  the `reason` says `optimizer disabled`); `cbo recall <0.0-1.0>` tunes
  `recall_target`.
- **Rust API**: `gtv_engine::cbo::{CostModel, CboState, plan_multimodal,
  QueryPlanChoice, GraphStats, VectorStats, SelectivityStats}`;
  `GtvContext::{set_table_stats, refresh_table_stats_by_name, set_graph_stats,
  set_cost_model, cbo_state}`.

---

## 19. Workload management & isolation (prod_p3 B3-6)

Six workload classes, each with a resource group (higher priority wins) and
admission control:

| class | priority | max_concurrency |
|---|---:|---:|
| `interactive_aml` | 100 | 4 |
| `ingestion` | 50 | 2 |
| `risk_batch` | 40 | 2 |
| `alm_batch` | 30 | 1 |
| `ftp_batch` | 20 | 1 |
| `index_build` | 10 | 1 |

Global defaults: `global_max_active = 8`, `max_queue = 256`.

- **Admission**: `wait_admit(class, timeout)` returns `Admit { id, token }` /
  `Queue { position }` / `Reject { reason }`. If the global budget is full but
  the class is not, the request is queued; if the queue is also full it is
  rejected with an explicit error (never silently dropped).
- **Preemption**: when a request has higher priority than an active query, the
  lowest-priority victim is cancelled (only *strictly lower* priority is
  preempted); the victim's `CancelToken` (B1-3) is tripped and long-running
  operators cancel cooperatively. `preempt(id)` / `preempt_class` allow targeted
  or whole-class cancellation.
- **SQL**: `workload_status()` returns one row per class:
  `class, priority, cpu_quota, max_concurrency, memory_limit_bytes, io_limit_bps,
  active, admitted, queued, rejected, preempted, completed`.
- **CLI**: `workload` = `SELECT ... FROM workload_status() ORDER BY priority DESC`;
  `metrics` emits, besides engine / SQL histograms, Prometheus
  `gtv_workload_*{class=...}` (admitted / queued / rejected / preempted /
  completed / active) plus `gtv_spill_bytes` / `gtv_spill_active_files`
  (DataFusion spill).
- **API**: `GtvContext::sql_as(class, sql, timeout)` runs through admission;
  `workload()` returns the `Arc<WorkloadManager>`; `configure(ResourceGroup)`
  adjusts quotas.
- **Isolation measurement**: under permanent overload (8 ingestion / batch /
  index-build workers competing for 2 slots) interactive admission p99 is
  ≈ 8.6µs with 0 timeouts (see `doc/b3_mixed_load_slo.md`). Note this measures
  the **admission control plane** latency, not query execution; real CPU/memory
  enforcement in a single-process in-memory architecture requires the enterprise
  batch's compute-storage separation.

---

## 20. Enterprise SQL surface: scenario / hierarchy / reference (prod_p4 D1 + D6)

The enterprise batch (Milestone D/E) lives in **separate crates**; the engine
kernel never depends on them. The composition root (CLI / server) owns an
`EnterpriseRegistry` and registers these functions on the DataFusion session.
Load tables with the `*_load` commands, then query with SQL.

### 20.1 Load commands

```text
scenario_load <table>          # one row per shock; rows sharing (scenario_id, version) group
hierarchy_load <kind> <table>  # kind = legal_entity | organisation | product
refdata_load <table>           # domain / key / valid_from / valid_to / value
master_load <kind> <table>     # kind = account | customer | instrument | counterparty
```

**scenario columns**: `scenario_id, version, kind, factor, value` (required);
`parent_id, parent_version, source_cutoff, model_version, status,
dim_legal_entity, dim_portfolio, dim_product, dim_currency` (optional).
`kind` = `baseline | stress | adverse | reverse_stress`.

**hierarchy columns**: `parent, child, valid_from` (required), `valid_to`
(optional, default open-ended).

**reference columns**: `domain, key, valid_from, value` (required), `valid_to`
(optional).

**master columns**: `id, valid_from` (required), `valid_to` (optional); every
other column becomes a string attribute.

### 20.2 Query functions

```sql
-- resolve a scenario (inheritance + override); one row per shock with provenance
SELECT factor, value, source_scenario, source_version, chain
FROM resolve_scenario('stress', 1);        -- omit version for the latest

-- effective-dated hierarchies
SELECT related FROM hierarchy_ancestors('legal_entity', 'a1', 0);
SELECT related FROM hierarchy_descendants('legal_entity', 'root', 0);

-- effective-dated reference value (NULL when absent)
SELECT refdata_get('curve', 'USD.5Y', 50) AS rate;

-- master attributes (one row each; empty result when absent)
SELECT attr_key, attr_value FROM master_get('account', 'A1', 0);
```

`resolve_scenario` columns: `scenario_id, version, kind, chain, source_cutoff,
model_version, factor, legal_entity, portfolio, product, currency, value,
source_scenario, source_version`.

### 20.3 Boundary

- Domain crates `gtv-scenario` / `gtv-refdata` are **pure data layers** with no
  DataFusion dependency; `gtv-enterprise-sql` is the SQL adapter.
- Kernel crates (`gtv-core`, `gtv-engine`, `gtv-index`, `gtv-pattern`, …) must
  **never** depend on an enterprise crate (CI: `testcase/check_boundary.sh`).
  CLI / server are composition roots and are exempt.
