//! Yahoo Finance daily OHLCV download (free, no key).
//!
//! `https://query1.finance.yahoo.com/v8/finance/chart/<SYM>?range=<R>&interval=1d`
//! returns daily bars; days with no trade (null close) are skipped.

use std::sync::Arc;

use anyhow::{anyhow, Result};
use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
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

/// Fetch daily OHLCV for a symbol over a Yahoo `range` ("1mo".."max").
pub fn fetch_daily(symbol: &str, range: &str) -> Result<Vec<YahooDaily>> {
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
