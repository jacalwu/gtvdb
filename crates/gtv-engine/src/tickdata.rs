//! London Strategic Edge historical tick data (REST API) + `read_tickdata`
//! table function.
//!
//! `GET https://api.londonstrategicedge.com/tickdata?symbol=eq.<sym>&order=ts.asc&limit=N`
//! returns one row per tick (`symbol, ts, price, bid, ask, volume`). The API caps
//! a single request at 10,000 rows, so [`fetch_history`] pages with a
//! `ts=gt.<last_ts>` keyset cursor until `limit` is reached.

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use anyhow::{anyhow, Result};
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result as DfResult};

use crate::expr_util::{expr_to_i64, expr_to_string};

/// Public (anonymous) key embedded in the LSE web front-end — works for the
/// historical REST data. Override with the `LSE_API_KEY` env var.
pub const ANON_KEY: &str = "71f880e1d2ef471664f3b6c04c6dc1e618f94e51f68c87522bc6dcbc0ca173a5";

pub fn api_key() -> String {
    std::env::var("LSE_API_KEY").unwrap_or_else(|_| ANON_KEY.to_string())
}

/// Normalize a symbol to LSE's wire format: metals `XAUUSD` -> `XAU/USD`,
/// crypto `BTCUSD` -> `BTC/USD`, forex `EURUSD` -> `EUR/USD`, else uppercase.
pub fn normalize_symbol(s: &str) -> String {
    let t = s.trim().replace('/', "").to_uppercase();
    const METALS: &[&str] = &["XAU", "XAG", "XPT", "XPD"];
    const FOREX: &[&str] = &["EUR", "USD", "GBP", "JPY", "AUD", "NZD", "CAD", "CHF"];
    const CRYPTO: &[&str] = &[
        "BTC", "ETH", "LTC", "XRP", "SOL", "DOGE", "ADA", "DOT", "AVAX", "MATIC",
        "LINK", "UNI", "ATOM", "XLM", "ALGO", "FIL", "NEAR", "AAVE", "MKR", "COMP",
        "SNX", "YFI", "SUSHI", "CRV", "BAL", "INJ", "SUI", "SEI", "APT", "ARB", "OP",
    ];
    for m in METALS {
        if t.starts_with(m) && t.len() > m.len() {
            return format!("{m}/{}", &t[m.len()..]);
        }
    }
    for c in CRYPTO {
        if t.starts_with(c) && t.len() > c.len() {
            let rest = &t[c.len()..];
            if rest == "USD" || rest == "USDT" || rest == "BUSD" {
                return format!("{c}/{rest}");
            }
        }
    }
    if t.len() == 6 {
        let (a, b) = (&t[..3], &t[3..]);
        if FOREX.contains(&a) && FOREX.contains(&b) {
            return format!("{a}/{b}");
        }
    }
    t
}

/// One historical tick row.
#[derive(Debug, Clone)]
pub struct HistTick {
    pub symbol: String,
    pub ts: String,
    pub ts_us: i64,
    pub price: f64,
    pub bid: f64,
    pub ask: f64,
    pub volume: f64,
}

/// Parse an ISO-8601 timestamp string to epoch microseconds.
fn iso_to_us(s: &str) -> i64 {
    chrono::DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&chrono::Utc).timestamp_micros())
        .unwrap_or(0)
}

/// Historical tick-table schema.
pub fn hist_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("ts", DataType::Utf8, false),
        Field::new("ts_us", DataType::Int64, false),
        Field::new("price", DataType::Float64, false),
        Field::new("bid", DataType::Float64, false),
        Field::new("ask", DataType::Float64, false),
        Field::new("volume", DataType::Float64, false),
    ]))
}

/// Convert historical ticks to a single [`RecordBatch`].
pub fn hist_to_batch(ticks: &[HistTick]) -> RecordBatch {
    let sym: Vec<String> = ticks.iter().map(|t| t.symbol.clone()).collect();
    let ts: Vec<String> = ticks.iter().map(|t| t.ts.clone()).collect();
    let ts_us: Vec<i64> = ticks.iter().map(|t| t.ts_us).collect();
    let price: Vec<f64> = ticks.iter().map(|t| t.price).collect();
    let bid: Vec<f64> = ticks.iter().map(|t| t.bid).collect();
    let ask: Vec<f64> = ticks.iter().map(|t| t.ask).collect();
    let volume: Vec<f64> = ticks.iter().map(|t| t.volume).collect();
    RecordBatch::try_new(
        hist_schema(),
        vec![
            Arc::new(StringArray::from(sym)) as ArrayRef,
            Arc::new(StringArray::from(ts)) as ArrayRef,
            Arc::new(arrow::array::Int64Array::from(ts_us)) as ArrayRef,
            Arc::new(Float64Array::from(price)) as ArrayRef,
            Arc::new(Float64Array::from(bid)) as ArrayRef,
            Arc::new(Float64Array::from(ask)) as ArrayRef,
            Arc::new(Float64Array::from(volume)) as ArrayRef,
        ],
    )
    .expect("build hist batch")
}

/// Percent-encode a timestamp cursor for a PostgREST `ts=gt.<value>` filter
/// (`+` in the offset must become `%2B`).
fn pct_encode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Fetch up to `limit` historical ticks for `symbol`, paging past the API's
/// 10,000-row-per-request cap with a `ts` keyset cursor.
pub fn fetch_history(symbol: &str, limit: usize, key: &str) -> Result<Vec<HistTick>> {
    const PAGE: usize = 10_000;
    let sym = normalize_symbol(symbol);
    let mut ticks: Vec<HistTick> = Vec::new();
    let mut cursor: Option<String> = None;

    while ticks.len() < limit {
        let n = (limit - ticks.len()).min(PAGE);
        let mut url = format!(
            "https://api.londonstrategicedge.com/tickdata?symbol=eq.{sym}&order=ts.asc&limit={n}&select=symbol,ts,price,bid,ask,volume"
        );
        if let Some(c) = &cursor {
            url.push_str(&format!("&ts=gt.{}", pct_encode(c)));
        }
        let body = ureq::get(&url)
            .set("x-api-key", key)
            .call()
            .map_err(|e| anyhow!("lse fetch {sym}: {e}"))?
            .into_string()?;
        let arr: Vec<serde_json::Value> = serde_json::from_str(&body)?;
        if arr.is_empty() {
            break;
        }
        let got = arr.len();
        cursor = arr
            .last()
            .and_then(|v| v.get("ts"))
            .and_then(|x| x.as_str())
            .map(str::to_string);
        for v in arr {
            let ts = v.get("ts").and_then(|x| x.as_str()).unwrap_or("").to_string();
            let ts_us = iso_to_us(&ts);
            ticks.push(HistTick {
                symbol: v.get("symbol").and_then(|x| x.as_str()).unwrap_or("").to_string(),
                ts,
                ts_us,
                price: v.get("price").and_then(|x| x.as_f64()).unwrap_or(0.0),
                bid: v.get("bid").and_then(|x| x.as_f64()).unwrap_or(0.0),
                ask: v.get("ask").and_then(|x| x.as_f64()).unwrap_or(0.0),
                volume: v.get("volume").and_then(|x| x.as_f64()).unwrap_or(0.0),
            });
        }
        if got < n {
            break; // reached the end of available history
        }
    }
    Ok(ticks)
}

/// `read_tickdata(symbol, limit)` table function (full mode).
#[derive(Debug, Default)]
pub struct ReadTickdataTableFunction;

impl TableFunctionImpl for ReadTickdataTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> DfResult<Arc<dyn TableProvider>> {
        let exprs = args.exprs();
        let sym = expr_to_string(
            exprs.first().ok_or_else(|| {
                DataFusionError::Execution("read_tickdata(symbol, limit): missing symbol".into())
            })?,
        )?;
        let limit = expr_to_i64(
            exprs.get(1).ok_or_else(|| {
                DataFusionError::Execution("read_tickdata(symbol, limit): missing limit".into())
            })?,
        )?
        .max(1) as usize;

        let ticks = fetch_history(&sym, limit, &api_key())
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let batch = hist_to_batch(&ticks);
        Ok(Arc::new(MemTable::try_new(hist_schema(), vec![vec![batch]])?))
    }
}
