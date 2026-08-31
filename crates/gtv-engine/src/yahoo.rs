//! Yahoo Finance daily OHLCV download (free, no key).
//!
//! `https://query1.finance.yahoo.com/v8/finance/chart/<SYM>?range=<R>&interval=1d`
//! returns daily bars; days with no trade (null close) are skipped.

use std::sync::Arc;

use anyhow::{anyhow, Result};
use arrow::array::{as_primitive_array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef};
use chrono::{Duration, NaiveDate};
use gtv_storage::StaticCache;
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result as DfResult};
use serde_json::Value;

use crate::expr_util::expr_to_string;

/// One daily OHLCV bar.
#[derive(Debug, Clone)]
pub struct YahooDaily {
    pub symbol: String,
    pub ts: i64, // epoch seconds
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub adjclose: f64,
    pub volume: f64,
}

pub fn yahoo_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("open", DataType::Float64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("low", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
        Field::new("adjclose", DataType::Float64, false),
        Field::new("volume", DataType::Float64, false),
    ]))
}

pub fn yahoo_to_batch(rows: &[YahooDaily]) -> RecordBatch {
    let sym: Vec<String> = rows.iter().map(|r| r.symbol.clone()).collect();
    let ts: Vec<i64> = rows.iter().map(|r| r.ts).collect();
    let open: Vec<f64> = rows.iter().map(|r| r.open).collect();
    let high: Vec<f64> = rows.iter().map(|r| r.high).collect();
    let low: Vec<f64> = rows.iter().map(|r| r.low).collect();
    let close: Vec<f64> = rows.iter().map(|r| r.close).collect();
    let adjclose: Vec<f64> = rows.iter().map(|r| r.adjclose).collect();
    let volume: Vec<f64> = rows.iter().map(|r| r.volume).collect();
    RecordBatch::try_new(
        yahoo_schema(),
        vec![
            Arc::new(StringArray::from(sym)) as ArrayRef,
            Arc::new(Int64Array::from(ts)) as ArrayRef,
            Arc::new(Float64Array::from(open)) as ArrayRef,
            Arc::new(Float64Array::from(high)) as ArrayRef,
            Arc::new(Float64Array::from(low)) as ArrayRef,
            Arc::new(Float64Array::from(close)) as ArrayRef,
            Arc::new(Float64Array::from(adjclose)) as ArrayRef,
            Arc::new(Float64Array::from(volume)) as ArrayRef,
        ],
    )
    .expect("yahoo batch")
}

fn arr_of(v: &Value) -> Vec<Value> {
    v.as_array().cloned().unwrap_or_default()
}

fn f64_at(v: &[Value], i: usize) -> Option<f64> {
    v.get(i).and_then(|x| x.as_f64())
}

/// `read_yahoo(symbol, range)` table function — daily OHLCV via SQL.
#[derive(Debug, Default)]
pub struct ReadYahooTableFunction;

impl ReadYahooTableFunction {
    pub fn new() -> Self {
        Self
    }
}

impl TableFunctionImpl for ReadYahooTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> DfResult<Arc<dyn TableProvider>> {
        let exprs = args.exprs();
        let symbol = expr_to_string(
            exprs.first().ok_or_else(|| {
                DataFusionError::Execution("read_yahoo(symbol [, range]): missing symbol".into())
            })?,
        )?;
        let range = exprs
            .get(1)
            .map(expr_to_string)
            .transpose()?
            .unwrap_or_else(|| "1y".to_string());
        let rows = fetch_daily(&symbol, &range)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        if rows.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "no Yahoo data for `{symbol}` (range={range})"
            )));
        }
        let batch = yahoo_to_batch(&rows);
        Ok(Arc::new(MemTable::try_new(yahoo_schema(), vec![vec![batch]])?))
    }
}

/// Local cache root (`GTV_DATA_DIR`, default `data/static`).
fn data_dir() -> std::path::PathBuf {
    std::env::var("GTV_DATA_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("data/static"))
}

/// Read-through `fetch_daily`: check the per-day static cache first; only hit
/// Yahoo when the data is missing/stale, then write each day back to the cache.
pub fn fetch_daily(symbol: &str, range: &str) -> Result<Vec<YahooDaily>> {
    let cache = StaticCache::new(data_dir());
    let (start, end) = range_dates(range);
    let (start_s, end_s) = (
        start.format("%Y-%m-%d").to_string(),
        end.format("%Y-%m-%d").to_string(),
    );

    // Cache hit if the latest cached day is within 2 days of the range end.
    if let Some(latest) = cache.latest_date("yahoo", symbol) {
        if let Ok(l) = NaiveDate::parse_from_str(&latest, "%Y-%m-%d") {
            if l >= end - Duration::days(2) {
                let batches = cache.read_days("yahoo", symbol, &start_s, &end_s)?;
                let rows = batches_to_rows(&batches)?;
                if !rows.is_empty() {
                    return Ok(rows);
                }
            }
        }
    }

    // Miss -> fetch from Yahoo and populate the per-day cache.
    let rows = fetch_daily_web(symbol, range)?;
    if !rows.is_empty() {
        let mut by_day: std::collections::BTreeMap<String, Vec<YahooDaily>> =
            std::collections::BTreeMap::new();
        for r in &rows {
            by_day
                .entry(StaticCache::date_from_secs(r.ts))
                .or_default()
                .push(r.clone());
        }
        for (date, day_rows) in by_day {
            let batch = yahoo_to_batch(&day_rows);
            cache.write_day("yahoo", &date, symbol, &batch)?;
        }
    }
    Ok(rows)
}

/// Calendar date range implied by a Yahoo `range` string.
fn range_dates(range: &str) -> (NaiveDate, NaiveDate) {
    let end = chrono::Local::now().date_naive();
    let days = match range {
        "5d" => 5,
        "1mo" => 31,
        "3mo" => 92,
        "6mo" => 183,
        "1y" => 366,
        "2y" => 731,
        "5y" => 1827,
        "max" => 3650,
        _ => 366,
    };
    (end - Duration::days(days), end)
}

/// Convert cached yahoo-schema batches back to rows.
fn batches_to_rows(batches: &[RecordBatch]) -> Result<Vec<YahooDaily>> {
    let mut out = Vec::new();
    for b in batches {
        let ts = as_primitive_array::<Int64Type>(b.column_by_name("ts").unwrap());
        let open = as_primitive_array::<arrow::datatypes::Float64Type>(b.column_by_name("open").unwrap());
        let high = as_primitive_array::<arrow::datatypes::Float64Type>(b.column_by_name("high").unwrap());
        let low = as_primitive_array::<arrow::datatypes::Float64Type>(b.column_by_name("low").unwrap());
        let close = as_primitive_array::<arrow::datatypes::Float64Type>(b.column_by_name("close").unwrap());
        let adj = as_primitive_array::<arrow::datatypes::Float64Type>(b.column_by_name("adjclose").unwrap());
        let vol = as_primitive_array::<arrow::datatypes::Float64Type>(b.column_by_name("volume").unwrap());
        for i in 0..b.num_rows() {
            out.push(YahooDaily {
                symbol: "".into(), // patched below if a symbol column exists
                ts: ts.value(i),
                open: open.value(i),
                high: high.value(i),
                low: low.value(i),
                close: close.value(i),
                adjclose: adj.value(i),
                volume: vol.value(i),
            });
        }
    }
    // Restore symbols: each cached day file is single-symbol, so read the symbol
    // once per batch.
    let mut k = 0;
    for bb in batches {
        let sym = bb
            .column_by_name("symbol")
            .map(|c| arrow::array::as_string_array(c).value(0).to_string())
            .unwrap_or_default();
        for _ in 0..bb.num_rows() {
            if k < out.len() {
                out[k].symbol = sym.clone();
            }
            k += 1;
        }
    }
    Ok(out)
}

/// Fetch daily OHLCV for a symbol over a Yahoo `range` ("1mo".."max").
pub fn fetch_daily_web(symbol: &str, range: &str) -> Result<Vec<YahooDaily>> {
    let url = format!(
        "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range={range}&interval=1d"
    );
    let body = ureq::get(&url)
        .set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0")
        .call()
        .map_err(|e| anyhow!("yahoo {symbol}: {e}"))?
        .into_string()?;
    let v: Value = serde_json::from_str(&body)?;
    let Some(result) = v["chart"]["result"][0].as_object() else {
        return Ok(Vec::new()); // symbol not found / no data
    };
    let ts = arr_of(&result["timestamp"]);
    let quote = &result["indicators"]["quote"][0];
    let open = arr_of(&quote["open"]);
    let high = arr_of(&quote["high"]);
    let low = arr_of(&quote["low"]);
    let close = arr_of(&quote["close"]);
    let volume = arr_of(&quote["volume"]);
    let adjclose = arr_of(
        &result["indicators"]["adjclose"][0]
            .get("adjclose")
            .cloned()
            .unwrap_or_else(|| Value::Null),
    );

    let mut out = Vec::new();
    for i in 0..ts.len() {
        let t = ts[i].as_i64().unwrap_or(0);
        let Some(c) = f64_at(&close, i) else {
            continue; // no trade on this day
        };
        out.push(YahooDaily {
            symbol: symbol.to_string(),
            ts: t,
            open: f64_at(&open, i).unwrap_or(c),
            high: f64_at(&high, i).unwrap_or(c),
            low: f64_at(&low, i).unwrap_or(c),
            close: c,
            adjclose: f64_at(&adjclose, i).unwrap_or(c),
            volume: f64_at(&volume, i).unwrap_or(0.0),
        });
    }
    Ok(out)
}
