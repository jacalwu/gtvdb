//! Unified market-data provider framework.
//!
//! Every provider (Futu OpenD, Yahoo Finance, a user's own Python/HTTP feed,
//! ...) implements the single [`MarketProvider`] trait and is registered by
//! name into a process-wide [`ProviderRegistry`].  Callers — SQL table
//! functions, the REPL, or plain Rust — always go through the same entry
//! points, so the provider is just the first argument:
//!
//! ```text
//! SELECT * FROM klines('futu', 'HK.00700', '1d', '2024-06-01', '2024-06-30');
//! SELECT * FROM klines('yahoo', 'AAPL', '1d', '2024-01-01', '2024-12-31');
//! SELECT * FROM ticks('futu', 'HK.00700', 100);
//! ```
//!
//! Adding a new provider does **not** touch any caller: implement
//! [`MarketProvider`], then
//! `market::registry().register(Arc::new(MyProvider))` — the same
//! `klines(...)` / `ticks(...)` functions immediately resolve it by name.
//!
//! Data model — all providers emit the same two columnar schemas
//! ([`kline_schema`] / [`tick_schema`]), timestamped as **UTC epoch
//! nanoseconds** (`Int64`) so bars/ticks from different vendors can be
//! interleaved in the temporal engine without further conversion:
//!
//! * kline:  `provider, symbol, ts, open, high, low, close, volume,
//!            turnover, adjclose`
//! * tick:   `provider, symbol, ts, price, volume, turnover, direction,
//!            sequence`
//!
//! Capability notes:
//! * `futu`  — full historical K-lines (minute bars .. yearly, via OpenD
//!   `request_history_kline`) and *current-session* ticks (LV2 `TICKER`
//!   subscription; Futu does **not** serve historical trade-by-trade dumps —
//!   intraday ticks must be captured by subscribing while the market is open).
//! * `yahoo` — daily + intraday minute bars; no tick feed.

pub mod futu;
pub mod yahoo;

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

use anyhow::{anyhow, Result};
use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result as DfResult};

use crate::expr_util::{expr_to_i64, expr_to_string};

// ---------------------------------------------------------------------------
// Request types (provider-agnostic)
// ---------------------------------------------------------------------------

/// A historical K-line (OHLCV) request.
#[derive(Debug, Clone)]
pub struct KlineReq {
    /// Provider name used to build the request (informational).
    pub provider: String,
    /// Symbols, e.g. `HK.00700`, `US.AAPL` (Futu wire format) or `AAPL`,
    /// `0700.HK` (Yahoo wire format) — each provider documents its format.
    pub codes: Vec<String>,
    /// Bar period: `1m | 3m | 5m | 15m | 30m | 60m | 1d | 1w | 1M | 1Q | 1Y`.
    pub period: String,
    /// Inclusive start date `YYYY-MM-DD`. `None` = auto lookback window.
    pub start: Option<String>,
    /// Inclusive end date `YYYY-MM-DD`. `None` = today.
    pub end: Option<String>,
    /// Per-symbol bar cap / auto-lookback size (default 1000).
    pub max: usize,
    /// Price adjustment: `none | qfq | hfq` (Futu); ignored by Yahoo.
    pub adjust: String,
}

impl KlineReq {
    /// A single-symbol request with explicit date bounds.
    pub fn range(
        provider: &str,
        code: &str,
        period: &str,
        start: &str,
        end: &str,
    ) -> Self {
        Self {
            provider: provider.to_string(),
            codes: vec![code.to_string()],
            period: period.to_string(),
            start: (!start.is_empty()).then(|| start.to_string()),
            end: (!end.is_empty()).then(|| end.to_string()),
            max: 0,
            adjust: "qfq".to_string(),
        }
    }
}

/// A tick (individual trade print) request.
#[derive(Debug, Clone)]
pub struct TickReq {
    /// Provider name (informational).
    pub provider: String,
    /// Symbols, e.g. `HK.00700`.
    pub codes: Vec<String>,
    /// Maximum number of ticks per symbol (latest first semantics are
    /// provider-defined; Futu returns the most recent ticks).
    pub max: usize,
}

// ---------------------------------------------------------------------------
// Provider trait — the one interface every provider implements
// ---------------------------------------------------------------------------

/// Market data source. Implementations must be cheap to construct (no I/O in
/// the constructor) and `Send + Sync` (they may be called from the SQL
/// planning thread).
pub trait MarketProvider: Send + Sync {
    /// Canonical lowercase name, e.g. `futu`.
    fn name(&self) -> &str;

    /// Human description shown by the `providers` REPL command.
    fn describe(&self) -> String {
        format!("{} market data provider", self.name())
    }

    /// Fetch historical K-line bars, one or more RecordBatches all matching
    /// [`kline_schema`]. Rows should be sorted by (symbol, ts) ascending.
    fn fetch_klines(&self, req: &KlineReq) -> Result<Vec<RecordBatch>>;

    /// Fetch ticks, RecordBatches all matching [`tick_schema`].
    fn fetch_ticks(&self, req: &TickReq) -> Result<Vec<RecordBatch>>;
}

// ---------------------------------------------------------------------------
// Registry — runtime registration by name
// ---------------------------------------------------------------------------

/// A name → provider map. Providers are looked up at *call* time, so a
/// provider registered after the SQL table functions were set up is
/// immediately reachable through the same `klines(...)` / `ticks(...)`
/// interface.
#[derive(Default)]
pub struct ProviderRegistry {
    map: RwLock<HashMap<String, Arc<dyn MarketProvider>>>,
}

impl ProviderRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert (or replace) a provider under its `name()`. Name lookup is
    /// case-insensitive (stored lowercase).
    pub fn register(&self, provider: Arc<dyn MarketProvider>) {
        let mut map = self.map.write().expect("provider registry poisoned");
        map.insert(provider.name().to_lowercase(), provider);
    }

    /// Resolve a provider by name (case-insensitive).
    pub fn get(&self, name: &str) -> Option<Arc<dyn MarketProvider>> {
        let map = self.map.read().expect("provider registry poisoned");
        map.get(&name.to_lowercase()).cloned()
    }

    /// All registered provider names, sorted.
    pub fn names(&self) -> Vec<String> {
        let mut names: Vec<String> = {
            let map = self.map.read().expect("provider registry poisoned");
            map.keys().cloned().collect()
        };
        names.sort();
        names
    }

    /// `(name, description)` pairs for the `providers` command.
    pub fn list(&self) -> Vec<(String, String)> {
        let map = self.map.read().expect("provider registry poisoned");
        let mut v: Vec<(String, String)> = map
            .iter()
            .map(|(k, p)| (k.clone(), p.describe()))
            .collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v
    }
}

fn default_registry() -> &'static ProviderRegistry {
    static REG: OnceLock<ProviderRegistry> = OnceLock::new();
    REG.get_or_init(|| {
        let reg = ProviderRegistry::new();
        reg.register(Arc::new(futu::FutuProvider::new()));
        reg.register(Arc::new(yahoo::YahooProvider::new()));
        reg
    })
}

/// The process-wide provider registry, pre-loaded with the built-in `futu`
/// and `yahoo` providers. Register new providers with
/// `market::registry().register(...)`.
pub fn registry() -> &'static ProviderRegistry {
    default_registry()
}

// ---------------------------------------------------------------------------
// Unified Arrow schemas
// ---------------------------------------------------------------------------

pub fn kline_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("provider", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false), // epoch ns UTC
        Field::new("open", DataType::Float64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("low", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
        Field::new("volume", DataType::Float64, false),
        Field::new("turnover", DataType::Float64, false),
        Field::new("adjclose", DataType::Float64, false),
    ]))
}

pub fn tick_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("provider", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false), // epoch ns UTC
        Field::new("price", DataType::Float64, false),
        Field::new("volume", DataType::Float64, false),
        Field::new("turnover", DataType::Float64, false),
        Field::new("direction", DataType::Utf8, false), // B|S|N (buy/sell/neutral)
        Field::new("sequence", DataType::Int64, false),
    ]))
}

// ---------------------------------------------------------------------------
// Row types + columnar builders
// ---------------------------------------------------------------------------

/// One K-line bar in unified form.
#[derive(Debug, Clone)]
pub struct KlineRow {
    pub provider: String,
    pub symbol: String,
    pub ts_ns: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub turnover: f64,
    pub adjclose: f64,
}

/// One tick in unified form.
#[derive(Debug, Clone)]
pub struct TickRow {
    pub provider: String,
    pub symbol: String,
    pub ts_ns: i64,
    pub price: f64,
    pub volume: f64,
    pub turnover: f64,
    pub direction: String,
    pub sequence: i64,
}

pub fn kline_to_batch(rows: &[KlineRow]) -> RecordBatch {
    let cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.provider.as_str()).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.symbol.as_str()).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(rows.iter().map(|r| r.ts_ns).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(rows.iter().map(|r| r.open).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(rows.iter().map(|r| r.high).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(rows.iter().map(|r| r.low).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(rows.iter().map(|r| r.close).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(rows.iter().map(|r| r.volume).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.turnover).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.adjclose).collect::<Vec<_>>(),
        )),
    ];
    RecordBatch::try_new(kline_schema(), cols).expect("kline batch")
}

pub fn tick_to_batch(rows: &[TickRow]) -> RecordBatch {
    let cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.provider.as_str()).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.symbol.as_str()).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(rows.iter().map(|r| r.ts_ns).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(rows.iter().map(|r| r.price).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(rows.iter().map(|r| r.volume).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.turnover).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.direction.as_str()).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.sequence).collect::<Vec<_>>(),
        )),
    ];
    RecordBatch::try_new(tick_schema(), cols).expect("tick batch")
}

/// Coalesce a list of batches (all must share the unified kline schema).
pub fn merge_kline_batches(batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
    let mut rows: Vec<KlineRow> = Vec::new();
    for b in batches {
        rows.extend(kline_rows(&b)?);
    }
    // Group into chunks (up to 64k rows each) to bound record-batch sizes.
    let mut out = Vec::new();
    for chunk in rows.chunks(65536) {
        out.push(kline_to_batch(chunk));
    }
    if out.is_empty() {
        out.push(empty_kline_batch());
    }
    Ok(out)
}

pub fn kline_rows(b: &RecordBatch) -> Result<Vec<KlineRow>> {
    let n = b.num_rows();
    let provider = arrow::array::as_string_array(b.column(0));
    let symbol = arrow::array::as_string_array(b.column(1));
    let ts = arrow::array::as_primitive_array::<arrow::datatypes::Int64Type>(b.column(2));
    let open = arrow::array::as_primitive_array::<arrow::datatypes::Float64Type>(b.column(3));
    let high = arrow::array::as_primitive_array::<arrow::datatypes::Float64Type>(b.column(4));
    let low = arrow::array::as_primitive_array::<arrow::datatypes::Float64Type>(b.column(5));
    let close = arrow::array::as_primitive_array::<arrow::datatypes::Float64Type>(b.column(6));
    let volume = arrow::array::as_primitive_array::<arrow::datatypes::Float64Type>(b.column(7));
    let turnover = arrow::array::as_primitive_array::<arrow::datatypes::Float64Type>(b.column(8));
    let adjclose = arrow::array::as_primitive_array::<arrow::datatypes::Float64Type>(b.column(9));
    let mut rows = Vec::with_capacity(n);
    for i in 0..n {
        rows.push(KlineRow {
            provider: provider.value(i).to_string(),
            symbol: symbol.value(i).to_string(),
            ts_ns: ts.value(i),
            open: open.value(i),
            high: high.value(i),
            low: low.value(i),
            close: close.value(i),
            volume: volume.value(i),
            turnover: turnover.value(i),
            adjclose: adjclose.value(i),
        });
    }
    Ok(rows)
}

pub fn empty_kline_batch() -> RecordBatch {
    RecordBatch::new_empty(kline_schema())
}

// ---------------------------------------------------------------------------
// DataFusion table functions: klines(provider, code, ...) / ticks(provider, code, ...)
// ---------------------------------------------------------------------------

fn provider_of(args: &TableFunctionArgs, fname: &str) -> DfResult<(String, Arc<dyn MarketProvider>)> {
    let exprs = args.exprs();
    let name = expr_to_string(
        exprs
            .first()
            .ok_or_else(|| DataFusionError::Execution(format!("{fname}(provider, ...): missing provider argument")))?,
    )?;
    let provider = registry().get(&name).ok_or_else(|| {
        DataFusionError::Execution(format!(
            "{fname}: unknown provider `{name}` (registered: {})",
            registry().names().join(", ")
        ))
    })?;
    Ok((name, provider))
}

/// `klines(provider, code [, period] [, start] [, end] [, max] [, adjust])`.
///
/// `period` defaults to `1d`; `start`/`end` are inclusive `YYYY-MM-DD`
/// strings (empty = auto window sized by `max`, which defaults to 1000 bars);
/// `adjust` is `qfq` (default) | `hfq` | `none`.
#[derive(Debug, Default)]
pub struct KlinesTableFunction;

impl TableFunctionImpl for KlinesTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> DfResult<Arc<dyn TableProvider>> {
        let exprs = args.exprs();
        let (provider_name, provider) = provider_of(&args, "klines")?;
        let code = expr_to_string(exprs.get(1).ok_or_else(|| {
            DataFusionError::Execution("klines(provider, code, ...): missing code".into())
        })?)?;
        let get_str = |i: usize, default: &str| -> DfResult<String> {
            match exprs.get(i) {
                Some(e) => expr_to_string(e),
                None => Ok(default.to_string()),
            }
        };
        let period = get_str(2, "1d")?;
        let start = get_str(3, "")?;
        let end = get_str(4, "")?;
        let max = match exprs.get(5) {
            Some(e) => expr_to_i64(e)? as usize,
            // 0 = auto: no cap when a range is given; sized by lookback otherwise.
            None => 0,
        };
        let adjust = get_str(6, "qfq")?;
        let req = KlineReq {
            provider: provider_name,
            codes: vec![code],
            period,
            start: (!start.is_empty()).then(|| start.clone()),
            end: (!end.is_empty()).then(|| end.clone()),
            max,
            adjust,
        };
        let batches = provider
            .fetch_klines(&req)
            .map_err(|e| DataFusionError::Execution(format!("klines({}): {e:#}", req.codes[0])))?;
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        if rows == 0 {
            return Err(DataFusionError::Execution(format!(
                "klines: no data for `{}` period={} start={:?} end={:?}",
                req.codes[0], req.period, req.start, req.end
            )));
        }
        Ok(Arc::new(MemTable::try_new(kline_schema(), vec![batches])?))
    }
}

/// `ticks(provider, code [, max])` — most recent `max` ticks (default 500).
#[derive(Debug, Default)]
pub struct TicksTableFunction;

impl TableFunctionImpl for TicksTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> DfResult<Arc<dyn TableProvider>> {
        let exprs = args.exprs();
        let (provider_name, provider) = provider_of(&args, "ticks")?;
        let code = expr_to_string(exprs.get(1).ok_or_else(|| {
            DataFusionError::Execution("ticks(provider, code [, max]): missing code".into())
        })?)?;
        let max = match exprs.get(2) {
            Some(e) => expr_to_i64(e)? as usize,
            None => 500,
        };
        let req = TickReq {
            provider: provider_name,
            codes: vec![code],
            max,
        };
        let batches = provider
            .fetch_ticks(&req)
            .map_err(|e| DataFusionError::Execution(format!("ticks({}): {e:#}", req.codes[0])))?;
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        if rows == 0 {
            return Err(DataFusionError::Execution(format!(
                "ticks: no tick data for `{}` (market closed? no subscription?)",
                req.codes[0]
            )));
        }
        Ok(Arc::new(MemTable::try_new(tick_schema(), vec![batches])?))
    }
}

/// Register the provider-parameterised table functions on a SessionContext.
/// Safe to call multiple times (re-registration replaces).
pub fn register_udtfs(ctx: &datafusion::prelude::SessionContext) {
    ctx.register_udtf("klines", Arc::new(KlinesTableFunction));
    ctx.register_udtf("market_klines", Arc::new(KlinesTableFunction));
    ctx.register_udtf("ticks", Arc::new(TicksTableFunction));
    ctx.register_udtf("market_ticks", Arc::new(TicksTableFunction));
}

/// Run a request against a named provider (pure-Rust entry point, same code
/// path as the SQL functions).
pub fn fetch_klines(req: &KlineReq) -> Result<Vec<RecordBatch>> {
    let provider = registry()
        .get(&req.provider)
        .ok_or_else(|| anyhow!("unknown provider `{}`", req.provider))?;
    provider.fetch_klines(req)
}

pub fn fetch_ticks(req: &TickReq) -> Result<Vec<RecordBatch>> {
    let provider = registry()
        .get(&req.provider)
        .ok_or_else(|| anyhow!("unknown provider `{}`", req.provider))?;
    provider.fetch_ticks(req)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A synthetic provider registered at *runtime* through the exact same
    /// `registry().register(...)` path a user follows to add their own feed.
    /// It only needs to implement [`MarketProvider`] — callers (table
    /// functions, fetch_* helpers) are untouched, which is the point: the
    /// interface is provider-independent.
    struct SynthProvider;

    impl MarketProvider for SynthProvider {
        fn name(&self) -> &str {
            "synth"
        }
        fn describe(&self) -> String {
            "synth — deterministic test bars, no network".to_string()
        }
        fn fetch_klines(&self, req: &KlineReq) -> Result<Vec<RecordBatch>> {
            let mut rows = Vec::new();
            for code in &req.codes {
                // 5 sine-wave bars at daily offsets, ascending ts.
                for i in 0..5i64 {
                    let ts = 1_700_000_000_000_000_000 + i * 86_400_000_000_000;
                    let px = 100.0 + (i as f64) * 0.5;
                    rows.push(KlineRow {
                        provider: self.name().to_string(),
                        symbol: code.clone(),
                        ts_ns: ts,
                        open: px,
                        high: px + 1.0,
                        low: px - 1.0,
                        close: px,
                        volume: 1000.0,
                        turnover: px * 1000.0,
                        adjclose: f64::NAN,
                    });
                }
            }
            Ok(vec![kline_to_batch(&rows)])
        }
        fn fetch_ticks(&self, _req: &TickReq) -> Result<Vec<RecordBatch>> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn dynamic_registration_same_interface() {
        let reg = ProviderRegistry::new();
        reg.register(Arc::new(SynthProvider));

        // Same KlineReq type used by `klines('futu', ...)` etc.
        let req = KlineReq {
            provider: "synth".to_string(),
            codes: vec!["TEST.1".to_string()],
            period: "1d".to_string(),
            start: None,
            end: None,
            max: 0,
            adjust: "none".to_string(),
        };
        let batches = reg.get("synth").unwrap().fetch_klines(&req).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 5);
        assert_eq!(batches[0].schema(), kline_schema());

        // Provider lookup is case-insensitive.
        assert!(reg.get("SYNTH").is_some());
        assert!(reg.get("missing").is_none());
    }

    #[test]
    fn default_registry_has_builtins() {
        let names = registry().names();
        assert!(names.contains(&"futu".to_string()), "names={names:?}");
        assert!(names.contains(&"yahoo".to_string()), "names={names:?}");
    }
}
