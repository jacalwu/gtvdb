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
cargo build --release -p gtv-cli --bin gtv
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
| — | `read_yahoo` | table fn (daily OHLCV) |
| — | `cross_sectional_signal` | table fn (fetch+momentum+zscore+signal+next-day ret) |
| — | `relative_strength` | table fn (target vs indices vs peers) |

---

## 6. Shell Commands

```text
help | tables | quit
neighbors <node> [T]       k-hop <node> <k> [T]
mavg <n> | msum <n> | deltas
asof [t ...]               knn <node> [k] [--mask ids]
save <table> <path>        load <table> <path>
loadcsv <table> <path>     bgload <table> <path> [ms]
live <table> <symbol...>   stream LSE live ticks (needs LSE_API_KEY)
fetch <table> <symbol> [limit]  pull LSE historical ticks (REST API)
yahoo <table> <symbol...> [--range 1y]  pull daily OHLCV from Yahoo
hdb_save <table> <date> [root]  persist table to HDB partitions
hdb_load <table> <date> <sym> [root]  read one HDB partition
hdb_scan <table> <start> <end> [sym] [root]  scan HDB date range
hdb_flush <table> [root] [secs]  background HDB flush (sym-enumerated)
tt <table> <T>             pattern [T]        delta
udf [x ...]                remote <host:port> <sql>
```

---

## 7. Performance Notes

- In `hft` mode, `pit` / `wash` / `aj` / `ofi` / bare table scans are compiled once
  into a **KernelPlan** (cached by query text) and executed directly on the
  compiled kernels — no DataFusion planning on the hot path.
  `SET DURATION = ON` shows the compute time in µs, e.g. `pit(500)` ~3 µs after the
  first compile vs ~1.5 ms through the DataFusion path.
- `tick_rule`/`lee_ready`/`emo` use an explicit sign (`>0 → 1, <0 → -1, =0 → 0`);
  Rust's `f64::signum` returns `1.0` for `+0.0` and is intentionally avoided.

---

## 8. Notes

- The `full` mode is the complete DataFusion SQL surface; the `hft` mode is the
  latency-first subset (no JOIN / GROUP BY / CTE / subquery).
- Real tick samples (`stocks_{MCO,NVDA,TSLA}_tick.parquet`, ~2.5M rows each, from
  London Strategic Edge) live under `testcase/hft/data/`:
  ```text
  gtv> load mco testcase/hft/data/stocks_MCO_tick.parquet
  gtv> SELECT count(*) FROM tick_to_trade('mco', 100);
  ```
