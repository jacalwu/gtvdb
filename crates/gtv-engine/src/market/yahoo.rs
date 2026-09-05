//! Yahoo Finance provider (free, no API key) for the unified `market`
//! framework.
//!
//! Backed by the public chart endpoint
//! `https://query1.finance.yahoo.com/v8/finance/chart/<SYM>?interval=<iv>&period1=..&period2=..`.
//! Yahoo identifies symbols differently from Futu (`AAPL`, `0700.HK`,
//! `BTC-USD`, ...). K-lines only — Yahoo has no tick feed here.
//!
//! Interval mapping: `1m/5m/15m/30m/60m -> same`, `1d -> 1d`, `1w -> 1wk`,
//! `1M -> 1mo`. Yahoo additionally caps how far back intraday ranges go
//! (≈30 d for 1m, 60 d for 5m/15m, …); requested ranges beyond the cap simply
//! return the available suffix. `adjclose` is populated for daily-and-coarser
//! bars (`events=history`), NaN for intraday.

use anyhow::{anyhow, bail, Context, Result};
use arrow::array::RecordBatch;
use chrono::NaiveDate;

use super::{kline_to_batch, KlineReq, KlineRow, MarketProvider, TickReq};

const BASE: &str = "https://query1.finance.yahoo.com/v8/finance/chart/";

/// Yahoo interval string for a unified period, if supported.
fn yahoo_interval(period: &str) -> Option<&'static str> {
    match period.to_lowercase().as_str() {
        "1m" => Some("1m"),
        "5m" => Some("5m"),
        "15m" => Some("15m"),
        "30m" => Some("30m"),
        "60m" => Some("60m"),
        "1h" => Some("60m"),
        "1d" => Some("1d"),
        "1w" => Some("1wk"),
        "1M" => Some("1mo"),
        _ => None,
    }
}

fn interval_secs(period: &str) -> i64 {
    match period.to_lowercase().as_str() {
        "1m" => 60,
        "5m" => 300,
        "15m" => 900,
        "30m" => 1800,
        "60m" | "1h" => 3600,
        "1d" => 86_400,
        "1w" => 604_800,
        "1M" => 2_592_000,
        _ => 86_400,
    }
}

/// Default lookback when the caller did not pin `start`.
fn lookback_days(period: &str, n_bars: usize) -> i64 {
    let bars = (n_bars.max(1)) as i64;
    // Sub-daily bars occur only in trading hours; widen the calendar window.
    let intraday = matches!(
        period.to_lowercase().as_str(),
        "1m" | "5m" | "15m" | "30m" | "60m" | "1h"
    );
    let stretch = if intraday { 3 } else { 1 };
    (interval_secs(period) * bars * stretch).max(2 * 86_400) / 86_400
}

/// GET with alternate-host + retry/backoff. Yahoo's public v8 endpoint is
/// aggressively rate-limited from some IPs ("Edge: Too Many Requests"); a
/// couple of retries over `query1`/`query2` smooths out bursts.
fn http_get_retry(url: &str) -> Result<String> {
    let mut last_err: Option<anyhow::Error> = None;
    for host in ["query1.finance.yahoo.com", "query2.finance.yahoo.com"] {
        for attempt in 0..2 {
            let u = url.replace("query1.finance.yahoo.com", host);
            match ureq::get(&u)
                .timeout(std::time::Duration::from_secs(30))
                .call()
            {
                Ok(r) => match r.into_string() {
                    Ok(body) => {
                        if body.contains("Too Many Requests") {
                            last_err =
                                Some(anyhow!("yahoo rate limit ({}), retrying", host));
                        } else {
                            return Ok(body);
                        }
                    }
                    Err(e) => last_err = Some(anyhow!("read yahoo response: {e}")),
                },
                Err(ureq::Error::Status(code, _)) => {
                    last_err = Some(anyhow!("yahoo HTTP {code}"));
                    if code != 429 && code != 403 {
                        break; // real error -> next host
                    }
                }
                Err(e) => last_err = Some(anyhow!("yahoo transport error: {e}")),
            }
            std::thread::sleep(std::time::Duration::from_millis(600 * (attempt + 1)));
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow!("yahoo request failed")))
}

/// Parse `YYYY-MM-DD` to epoch seconds (today when unparseable).
fn secs_of(ymd: &str) -> i64 {
    NaiveDate::parse_from_str(ymd, "%Y-%m-%d")
        .map(|d| d.and_hms_opt(0, 0, 0).expect("midnight").and_utc().timestamp())
        .unwrap_or_else(|_| chrono::Utc::now().timestamp())
}

/// Fetch one symbol/range from Yahoo and return unified kline rows.
fn fetch_rows(code: &str, period: &str, start: Option<&str>, end: Option<&str>, max: usize) -> Result<Vec<KlineRow>> {
    let iv = yahoo_interval(period)
        .ok_or_else(|| anyhow!("yahoo: unsupported period `{period}` (use 1m..60m,1h,1d,1w,1M)"))?;
    let p2 = secs_of(end.unwrap_or(""));
    // Auto lookback targets ~1000 bars when the caller gave neither start nor
    // an explicit count; bounded ranges bypass the window logic entirely.
    let auto_bars = if max > 0 { max } else { 1000 };
    let p1 = match start {
        Some(s) => secs_of(s),
        None => p2 - lookback_days(period, auto_bars) * 86_400,
    };
    if p1 >= p2 {
        bail!("yahoo: empty range start={p1} end={p2}");
    }

    let url = format!("{BASE}{code}?interval={iv}&period1={p1}&period2={p2}&events=history");
    let body = http_get_retry(&url).with_context(|| format!("yahoo GET {code} interval={iv}"))?;
    let v: serde_json::Value = serde_json::from_str(&body).context("yahoo JSON")?;

    if let Some(err) = v.pointer("/chart/error").and_then(|e| e.as_str()) {
        bail!("yahoo error for `{code}`: {err}");
    }
    let result = v
        .pointer("/chart/result/0")
        .ok_or_else(|| anyhow!("yahoo: empty result for `{code}` (unknown symbol?)"))?;
    let ts = result
        .pointer("/timestamp")
        .and_then(|t| t.as_array())
        .ok_or_else(|| anyhow!("yahoo: no timestamp array for `{code}`"))?;
    let quote = result.pointer("/indicators/quote/0").cloned().unwrap_or_default();
    let open = quote.get("open").and_then(|a| a.as_array());
    let high = quote.get("high").and_then(|a| a.as_array());
    let low = quote.get("low").and_then(|a| a.as_array());
    let close = quote.get("close").and_then(|a| a.as_array());
    let volume = quote.get("volume").and_then(|a| a.as_array());
    let adjclose = result
        .pointer("/indicators/adjclose/0/adjclose")
        .and_then(|a| a.as_array());

    let f = |a: Option<&Vec<serde_json::Value>>, i: usize| -> f64 {
        a.and_then(|v| v.get(i))
            .and_then(|x| x.as_f64())
            .unwrap_or(f64::NAN)
    };

    let mut rows = Vec::new();
    for (i, t) in ts.iter().enumerate() {
        let Some(sec) = t.as_i64() else { continue };
        let c = f(close, i);
        if !c.is_finite() {
            continue; // untraded slot (holiday / pre-listing)
        }
        rows.push(KlineRow {
            provider: "yahoo".to_string(),
            symbol: code.to_string(),
            ts_ns: sec * 1_000_000_000,
            open: f(open, i),
            high: f(high, i),
            low: f(low, i),
            close: c,
            volume: f(volume, i),
            turnover: f64::NAN,
            adjclose: f(adjclose, i),
        });
    }
    Ok(rows)
}

/// A `yahoo` provider instance (stateless).
pub struct YahooProvider {
    name: String,
}

impl YahooProvider {
    pub fn new() -> Self {
        Self {
            name: "yahoo".to_string(),
        }
    }
}

impl Default for YahooProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl MarketProvider for YahooProvider {
    fn name(&self) -> &str {
        &self.name
    }

    fn describe(&self) -> String {
        "yahoo — Yahoo Finance daily/intraday bars (free, no key; no ticks)".to_string()
    }

    fn fetch_klines(&self, req: &KlineReq) -> Result<Vec<RecordBatch>> {
        let cache = super::cache::MarketCache::new();
        let mut batches = Vec::new();
        for code in &req.codes {
            let code_own = code.clone();
            let period = req.period.clone();
            let start = req.start.clone();
            let end = req.end.clone();
            let max = req.max;
            let rows = super::cache::cached_klines(
                &cache,
                "yahoo",
                code,
                &period,
                "none", // Yahoo serves unadjusted close + adjclose column
                start,
                end,
                max,
                move |norm: &KlineReq| {
                    fetch_rows(&code_own, &norm.period, norm.start.as_deref(), norm.end.as_deref(), norm.max)
                        .with_context(|| format!("yahoo klines `{code_own}` period={}", norm.period))
                },
            )?;
            if !rows.is_empty() {
                batches.push(kline_to_batch(&rows));
            }
        }
        Ok(batches)
    }

    fn fetch_ticks(&self, _req: &TickReq) -> Result<Vec<RecordBatch>> {
        bail!("yahoo: no tick feed; try provider `futu` (intraday ticks) or a tick-recorded gtv table")
    }
}
