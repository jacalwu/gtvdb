//! Local read-through cache for static historical K-lines.
//!
//! Historical bars are (almost) static: only the recent tail changes, and
//! provider requests cost time/quota (Futu OpenD history-K quota & pacing,
//! Yahoo rate limits). [`cached_klines`] makes `klines(...)`/`md` cache-aware
//! inside the engine, so callers need no protocol change:
//!
//! * one parquet file per `(provider, symbol, period, adjust)` key plus a
//!   small JSON meta with the covered `[start_ns, end_ns)` window;
//! * full **coverage hit** → served from disk, provider untouched;
//! * **head/tail gaps** → only the missing segments are fetched and merged
//!   (ascending, dedup by `ts_ns`), then the store is updated;
//! * **adjusted prices** (`qfq`/`hfq`) are repriced by corporate actions, so
//!   a cache older than `ADJUST_TTL_DAYS` (5) is discarded and refetched;
//! * **intraday bars whose range includes today** bypass the cache entirely
//!   (today's prints are still forming) but still refresh the store for the
//!   static past part.
//!
//! Root directory: `GTV_MARKET_DIR` (default `data/market`); disable with
//! `GTV_MARKET_CACHE=0`.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use chrono::{NaiveDate, TimeZone, Utc};
use serde::{Deserialize, Serialize};

use super::{kline_rows, kline_to_batch, KlineReq, KlineRow};

const DAY_NS: i64 = 86_400_000_000_000;
const ADJUST_TTL_DAYS: i64 = 5;

#[derive(Debug, Clone, Copy)]
pub struct CacheMeta {
    pub covered_start_ns: i64,
    pub covered_end_ns: i64, // exclusive
    pub stored_at_unix: i64,
}

#[derive(Debug, Clone)]
pub struct MarketCache {
    root: PathBuf,
    enabled: bool,
}

fn subdaily(period: &str) -> bool {
    matches!(
        period.to_lowercase().as_str(),
        "1m" | "3m" | "5m" | "15m" | "30m" | "60m"
    )
}

fn period_secs(period: &str) -> i64 {
    match period.to_lowercase().as_str() {
        "1m" => 60,
        "3m" => 180,
        "5m" => 300,
        "15m" => 900,
        "30m" => 1800,
        "60m" => 3600,
        "1d" => DAY_NS / 1_000_000_000,
        "1w" => 7 * DAY_NS / 1_000_000_000,
        "1M" => 30 * DAY_NS / 1_000_000_000,
        "1Q" => 91 * DAY_NS / 1_000_000_000,
        "1Y" => 365 * DAY_NS / 1_000_000_000,
        _ => DAY_NS / 1_000_000_000,
    }
}

/// `YYYY-MM-DD` -> ns at UTC midnight.
pub fn date_ns(s: &str) -> i64 {
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .map(|d| d.and_hms_opt(0, 0, 0).expect("midnight").and_utc().timestamp_nanos_opt().unwrap_or(0))
        .unwrap_or(0)
}

/// ns -> `YYYY-MM-DD` (UTC).
fn ns_date(ns: i64) -> String {
    Utc.timestamp_opt(ns.div_euclid(1_000_000_000), 0)
        .single()
        .map(|dt| dt.format("%Y-%m-%d").to_string())
        .unwrap_or_default()
}

fn sanitize(code: &str) -> String {
    code.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect::<String>()
        .to_lowercase()
}

fn today_str() -> String {
    Utc::now().format("%Y-%m-%d").to_string()
}

impl MarketCache {
    pub fn new() -> Self {
        let enabled = std::env::var("GTV_MARKET_CACHE")
            .map(|v| v != "0")
            .unwrap_or(true);
        let root = std::env::var("GTV_MARKET_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("data/market"));
        Self { root, enabled }
    }

    fn key_paths(&self, provider: &str, symbol: &str, period: &str, adjust: &str) -> (PathBuf, PathBuf) {
        let dir = self
            .root
            .join(sanitize(provider))
            .join(sanitize(symbol));
        let stem = format!("{}__{}", sanitize(period), sanitize(adjust));
        (dir.join(format!("{stem}.parquet")), dir.join(format!("{stem}.json")))
    }

    fn read_meta(path: &Path) -> Option<CacheMeta> {
        let s = fs::read_to_string(path).ok()?;
        #[derive(Deserialize)]
        struct M {
            covered_start_ns: i64,
            covered_end_ns: i64,
            stored_at_unix: i64,
        }
        let m: M = serde_json::from_str(&s).ok()?;
        Some(CacheMeta {
            covered_start_ns: m.covered_start_ns,
            covered_end_ns: m.covered_end_ns,
            stored_at_unix: m.stored_at_unix,
        })
    }

    fn write_meta(path: &Path, meta: &CacheMeta) -> Result<()> {
        #[derive(Serialize)]
        struct M {
            covered_start_ns: i64,
            covered_end_ns: i64,
            stored_at_unix: i64,
        }
        let s = serde_json::to_string(&M {
            covered_start_ns: meta.covered_start_ns,
            covered_end_ns: meta.covered_end_ns,
            stored_at_unix: meta.stored_at_unix,
        })?;
        fs::write(path, s).context("write cache meta")
    }

    fn load(&self, provider: &str, symbol: &str, period: &str, adjust: &str) -> (Vec<KlineRow>, Option<CacheMeta>) {
        let (pf, mf) = self.key_paths(provider, symbol, period, adjust);
        let meta = Self::read_meta(&mf);
        let rows = match gtv_storage::read_batches(pf.to_str().unwrap_or("")) {
            Ok(batches) => batches
                .iter()
                .flat_map(|b| kline_rows(b).unwrap_or_default())
                .collect::<Vec<_>>(),
            Err(_) => Vec::new(),
        };
        (rows, meta)
    }

    fn store(
        &self,
        provider: &str,
        symbol: &str,
        period: &str,
        adjust: &str,
        rows: &[KlineRow],
        meta: &CacheMeta,
    ) -> Result<()> {
        let (pf, mf) = self.key_paths(provider, symbol, period, adjust);
        if let Some(dir) = pf.parent() {
            fs::create_dir_all(dir).context("create market cache dir")?;
        }
        let tmp = pf.with_extension("parquet.tmp");
        let batch = kline_to_batch(rows);
        gtv_storage::write_batch(tmp.to_str().unwrap_or(""), &batch).context("write market cache")?;
        fs::rename(&tmp, &pf).context("commit market cache")?;
        Self::write_meta(&mf, meta)?;
        Ok(())
    }

    /// Effective `[start_ns, end_ns)` for a request, honouring `max`-based
    /// auto lookback when `start` is absent. `max_hint` = the caller's max
    /// (0 = auto/default 1000).
    fn request_window(req: &KlineReq, max_hint: usize) -> (i64, i64, i64) {
        let end_s = req.end.clone().unwrap_or_else(today_str);
        let end_excl = date_ns(&end_s) + DAY_NS;
        let n_bars = if max_hint > 0 { max_hint } else { 1000 } as i64;
        let start_ns = match &req.start {
            Some(s) => date_ns(s),
            None => {
                let intraday = subdaily(&req.period);
                let stretch = if intraday { 3 } else { 1 };
                // calendar days ≈ period_sec * bars * stretch / sec-per-day
                let days = (period_secs(&req.period) * n_bars * stretch / 86_400).max(2);
                let days = days.min(if intraday { 8 * 366 } else { 20 * 366 });
                end_excl - days * DAY_NS
            }
        };
        (start_ns, end_excl, n_bars)
    }
}

/// Cache-aware K-line fetch. `raw` performs a real provider fetch for exactly
/// the window given in the (normalised) request and must return rows ascending.
pub fn cached_klines<F>(
    cache: &MarketCache,
    provider: &str,
    code: &str,
    period: &str,
    adjust: &str,
    start: Option<String>,
    end: Option<String>,
    max: usize,
    raw: F,
) -> Result<Vec<KlineRow>>
where
    F: Fn(&KlineReq) -> Result<Vec<KlineRow>>,
{
    // Normalise to a concrete window (auto lookback when no start given).
    let norm_req = KlineReq {
        provider: provider.to_string(),
        codes: vec![code.to_string()],
        period: period.to_string(),
        start,
        end,
        max: 0, // explicit window -> fetch everything in range
        adjust: adjust.to_string(),
    };
    let (start_ns, end_excl, n_bars) = MarketCache::request_window(&norm_req, max);
    // Explicit-window request used for any real provider fetch, so the engine
    // and the provider agree on what range was obtained.
    let range_req = KlineReq {
        start: Some(ns_date(start_ns)),
        end: Some(ns_date(end_excl - 1)),
        ..norm_req.clone()
    };
    let fetch_full = || raw(&range_req).map_err(Into::into);

    let end_s = norm_req.end.clone().unwrap_or_else(today_str);
    // Today's intraday bars are still forming: bypass caching entirely.
    if !cache.enabled || (subdaily(period) && end_s == today_str()) {
        return fetch_full();
    }

    let (mut rows, meta) = cache.load(provider, code, period, adjust);

    // Adjusted prices get repriced by splits/dividends -> refresh the whole
    // store once it is older than the TTL.
    let stale = matches!(adjust.to_lowercase().as_str(), "qfq" | "hfq")
        && meta
            .map(|m| Utc::now().timestamp() - m.stored_at_unix > ADJUST_TTL_DAYS * 86_400)
            .unwrap_or(false);

    let cover_hit = !stale
        && meta
            .map(|m| m.covered_start_ns <= start_ns && m.covered_end_ns >= end_excl)
            .unwrap_or(false);

    if cover_hit {
        let out: Vec<KlineRow> = rows
            .into_iter()
            .filter(|r| r.ts_ns >= start_ns && r.ts_ns < end_excl)
            .collect();
        return Ok(trim_tail(out, n_bars));
    }

    // Stale or empty store -> replace wholesale.
    let mut fetched: Vec<KlineRow> = if rows.is_empty() || stale {
        if !rows.is_empty() {
            rows.clear();
        }
        fetch_full()?
    } else {
        Vec::new()
    };
    // Otherwise backfill only the missing head/tail segments.
    if !stale && !rows.is_empty() {
        if let Some(f) = rows.iter().map(|r| r.ts_ns).min() {
            if start_ns < f {
                let head = fetch_segment(provider, code, period, adjust, start_ns, f, &raw)?;
                fetched.extend(head);
            }
        }
        if let Some(l) = rows.iter().map(|r| r.ts_ns).max() {
            if end_excl > l + DAY_NS {
                let tail = fetch_segment(provider, code, period, adjust, l + DAY_NS, end_excl, &raw)?;
                fetched.extend(tail);
            }
        }
    }

    // Merge: dedup by ts, ascending.
    rows.extend(fetched);
    rows.sort_by_key(|r| r.ts_ns);
    rows.dedup_by(|a, b| a.ts_ns == b.ts_ns);

    if !rows.is_empty() {
        let new_first = rows.iter().map(|r| r.ts_ns).min().unwrap_or(start_ns);
        let new_last = rows.iter().map(|r| r.ts_ns).max().unwrap_or(end_excl - 1);
        let new_meta = CacheMeta {
            covered_start_ns: new_first.min(start_ns),
            covered_end_ns: (new_last + DAY_NS).max(end_excl),
            stored_at_unix: Utc::now().timestamp(),
        };
        cache.store(provider, code, period, adjust, &rows, &new_meta)?;
    }

    let out: Vec<KlineRow> = rows
        .into_iter()
        .filter(|r| r.ts_ns >= start_ns && r.ts_ns < end_excl)
        .collect();
    Ok(trim_tail(out, n_bars))
}

/// Fetch one contiguous calendar segment `[s, e)` (e exclusive) and sanity it.
fn fetch_segment<F>(
    provider: &str,
    code: &str,
    period: &str,
    adjust: &str,
    s_ns: i64,
    e_ns: i64,
    raw: &F,
) -> Result<Vec<KlineRow>>
where
    F: Fn(&KlineReq) -> Result<Vec<KlineRow>>,
{
    if e_ns <= s_ns {
        return Ok(Vec::new());
    }
    let start_s = ns_date(s_ns);
    let end_s = ns_date(e_ns - 1);
    let req = KlineReq {
        provider: provider.to_string(),
        codes: vec![code.to_string()],
        period: period.to_string(),
        start: Some(start_s.clone()),
        end: Some(end_s.clone()),
        max: 0,
        adjust: adjust.to_string(),
    };
    raw(&req).map_err(|e| anyhow!("cache backfill [{start_s},{end_s}]: {e:#}"))
}

/// When the caller relied on `max` for an auto window, keep only the last
/// `max` bars of the returned slice.
fn trim_tail(mut rows: Vec<KlineRow>, n_bars: i64) -> Vec<KlineRow> {
    if n_bars > 0 && rows.len() > n_bars as usize {
        let cut = rows.len() - n_bars as usize;
        rows.drain(..cut);
    }
    rows
}

impl Default for MarketCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Deterministic fake provider: one bar per calendar day in range.
    fn fake_raw<'a>(
        calls: &'a AtomicUsize,
        code: &str,
    ) -> impl Fn(&KlineReq) -> Result<Vec<KlineRow>> + 'a {
        let code = code.to_string();
        move |req: &KlineReq| {
            calls.fetch_add(1, Ordering::SeqCst);
            let mut rows = Vec::new();
            let s = req.start.clone().unwrap_or_else(|| "2024-01-01".into());
            let e = req.end.clone().unwrap_or_else(|| "2024-01-31".into());
            let mut t = date_ns(&s);
            let end_excl = date_ns(&e) + DAY_NS;
            while t < end_excl {
                rows.push(KlineRow {
                    provider: "fake".into(),
                    symbol: code.clone(),
                    ts_ns: t,
                    open: 1.0, high: 2.0, low: 0.5, close: 1.5,
                    volume: 1.0, turnover: 0.0, adjclose: 1.5,
                });
                t += DAY_NS;
            }
            Ok(rows)
        }
    }

    fn with_tmp_cache(f: impl FnOnce()) {
        let dir = format!("/tmp/gtv_mkt_test_{}_{}", std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos());
        std::env::set_var("GTV_MARKET_DIR", &dir);
        std::env::set_var("GTV_MARKET_CACHE", "1");
        f();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn cache_hit_reuses_and_tail_is_incremental() {
        with_tmp_cache(|| {
            let calls = AtomicUsize::new(0);
            let cache = MarketCache::new();
            let code = "HK.00001".to_string();
            let raw = fake_raw(&calls, &code);

            // 1) first fetch fills the whole window
            let r1 = cached_klines(
                &cache, "fake", &code, "1d", "none",
                Some("2024-06-01".into()), Some("2024-06-30".into()), 0, &raw,
            )
            .unwrap();
            assert_eq!(r1.len(), 30);
            assert_eq!(calls.load(Ordering::SeqCst), 1);

            // 2) same window again -> pure cache hit, provider untouched
            let r2 = cached_klines(
                &cache, "fake", &code, "1d", "none",
                Some("2024-06-01".into()), Some("2024-06-30".into()), 0, &raw,
            )
            .unwrap();
            assert_eq!(r2.len(), 30);
            assert_eq!(r2[0].ts_ns, r1[0].ts_ns);
            assert_eq!(calls.load(Ordering::SeqCst), 1, "no refetch on covered window");

            // 3) extend the end -> only the new tail is fetched (one call)
            let r3 = cached_klines(
                &cache, "fake", &code, "1d", "none",
                Some("2024-06-01".into()), Some("2024-07-10".into()), 0, &raw,
            )
            .unwrap();
            assert_eq!(r3.len(), 40);
            assert_eq!(calls.load(Ordering::SeqCst), 2, "tail-only incremental fetch");

            // 4) widen the head too -> one more call for the older segment
            let r4 = cached_klines(
                &cache, "fake", &code, "1d", "none",
                Some("2024-05-15".into()), Some("2024-07-10".into()), 0, &raw,
            )
            .unwrap();
            assert_eq!(r4.len(), 57);
            assert_eq!(calls.load(Ordering::SeqCst), 3, "head+tail merge");
        });
    }

    #[test]
    fn auto_window_max_trims_to_last_n() {
        with_tmp_cache(|| {
            let calls = AtomicUsize::new(0);
            let cache = MarketCache::new();
            let raw = fake_raw(&calls, "X");
            // no start -> auto window; max=5 keeps last 5 bars of the request
            let rows = cached_klines(
                &cache, "fake", "X", "1d", "none",
                None, Some("2024-01-10".into()), 5, &raw,
            )
            .unwrap();
            assert_eq!(rows.len(), 5);
            assert!(calls.load(Ordering::SeqCst) >= 1);
        });
    }
}
