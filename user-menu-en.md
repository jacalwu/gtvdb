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
neighbors <node> [T]       k-hop <node> <k> [T]
mavg <n> | msum <n> | deltas
asof [t ...]               knn <node> [k] [--mask ids]
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
metrics                                  # engine counters + SQL latency histogram (Prometheus text)
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
crm_alloc('loan_exposure','collateral','guarantee','collateral_edges','guarantee_edges','BASE',T[, 'ead'])  # per-loan CRM cover (§8)
crm_audit('loan_exposure','collateral','guarantee','collateral_edges','guarantee_edges','BASE',T[, 'ead'])  # audit trail (§8)
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
