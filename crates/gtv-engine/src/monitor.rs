//! Phase C observability: engine metrics + data-quality / health / strategy
//! diagnostics over registry tables (function2.md §2).
//!
//! * engine metrics — process counters + SQL latency histogram, printed as
//!   Prometheus text by the REPL `metrics` command.
//! * `dq_report('t')` / `dq_check('t')`   — data quality (§2.3)
//! * `health_check('t'[, max_age_days[, min_rows]])` — daily health (§2.4)
//! * `strategy_stats('t')` — hit rate / Brier / ECE / PSI drift (§2.2)

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use arrow::array::{as_primitive_array, as_string_array, Array, ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Float64Type, Int64Type, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl};
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Expr;
use datafusion::scalar::ScalarValue;

use crate::expr_util::expr_to_string;
use crate::hft_exec::HftRegistry;

// ---------------------------------------------------------------------------
// Engine metrics (§2.1 — process level; OS counters are out of scope, see doc)
// ---------------------------------------------------------------------------

pub static QUERIES: AtomicU64 = AtomicU64::new(0);
pub static QUERY_ERRORS: AtomicU64 = AtomicU64::new(0);
/// latency buckets in seconds
pub const LAT_BUCKETS: [f64; 7] = [0.0001, 0.001, 0.01, 0.1, 1.0, 10.0, 100.0];
pub static LATENCY: [AtomicU64; 7] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];

/// Record one query: bump the counter and the matching latency buckets.
pub fn record_query(elapsed_secs: f64) {
    QUERIES.fetch_add(1, Ordering::Relaxed);
    for (b, c) in LAT_BUCKETS.iter().zip(LATENCY.iter()) {
        if elapsed_secs <= *b {
            c.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub fn record_error() {
    QUERY_ERRORS.fetch_add(1, Ordering::Relaxed);
}

/// Prometheus text exposition of the engine metrics.
pub fn prometheus_text() -> String {
    let mut s = String::new();
    s.push_str("# HELP gtv_queries_total SQL statements executed\n");
    s.push_str("# TYPE gtv_queries_total counter\n");
    s.push_str(&format!("gtv_queries_total {}\n", QUERIES.load(Ordering::Relaxed)));
    s.push_str("# HELP gtv_query_errors_total SQL statements that failed\n");
    s.push_str("# TYPE gtv_query_errors_total counter\n");
    s.push_str(&format!(
        "gtv_query_errors_total {}\n",
        QUERY_ERRORS.load(Ordering::Relaxed)
    ));
    s.push_str("# HELP gtv_query_latency_seconds SQL latency histogram\n");
    s.push_str("# TYPE gtv_query_latency_seconds histogram\n");
    for (i, b) in LAT_BUCKETS.iter().enumerate() {
        let le = if i == LAT_BUCKETS.len() - 1 { "+Inf".to_string() } else { format!("{b}") };
        s.push_str(&format!(
            "gtv_query_latency_seconds_bucket{{le=\"{le}\"}} {}\n",
            LATENCY[i].load(Ordering::Relaxed)
        ));
    }
    s
}

// ---------------------------------------------------------------------------
// Pure analyzers (unit-testable)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct DqStats {
    pub rows: i64,
    pub n_symbols: i64,
    pub nan_inf: i64,
    pub dup_ts_sym: i64,
    pub non_monotonic: i64,
    pub min_rows_sym: i64,
    pub max_rows_sym: i64,
}

/// Scan `(ts, symbol)` pairs in row order. `nan_inf` is supplied by the caller
/// (float columns scanned separately).
pub fn dq_analyze(ts: &[i64], sym: &[&str], nan_inf: i64) -> DqStats {
    let mut seen = std::collections::HashSet::new();
    let mut dup = 0i64;
    let mut non_mono = 0i64;
    let mut last: HashMap<&str, i64> = HashMap::new();
    for (i, &t) in ts.iter().enumerate() {
        let s = sym.get(i).copied().unwrap_or("");
        if !seen.insert((t, s)) {
            dup += 1;
        }
        if let Some(&prev) = last.get(s) {
            if t < prev {
                non_mono += 1;
            }
        }
        last.insert(s, t);
    }
    let counts: Vec<usize> = {
        let mut m: HashMap<&str, usize> = HashMap::new();
        for s in sym {
            *m.entry(s).or_default() += 1;
        }
        m.into_values().collect()
    };
    DqStats {
        rows: ts.len() as i64,
        n_symbols: counts.len() as i64,
        nan_inf,
        dup_ts_sym: dup,
        non_monotonic: non_mono,
        min_rows_sym: counts.iter().copied().min().unwrap_or(0) as i64,
        max_rows_sym: counts.iter().copied().max().unwrap_or(0) as i64,
    }
}

#[derive(Debug, Clone, Default)]
pub struct StratStats {
    pub n: i64,
    pub hit: f64,
    pub brier: f64,
    pub ece10: f64,
    pub psi: f64,
    pub hit_first80: f64,
    pub hit_last20: f64,
    pub strong070: i64,
    pub strong070_win: f64,
}

/// Strategy metrics over decision rows (up in {0,1} as f64, p in [0,1]).
pub fn strategy_stats(up: &[f64], p: &[f64]) -> StratStats {
    let n = up.len();
    if n == 0 {
        return StratStats::default();
    }
    let hit_fn = |slice: &[(f64, f64)]| -> f64 {
        if slice.is_empty() {
            return f64::NAN;
        }
        slice.iter().filter(|(u, pp)| (*pp > 0.5) == (*u > 0.5)).count() as f64 / slice.len() as f64
    };
    let pairs: Vec<(f64, f64)> = up.iter().copied().zip(p.iter().copied()).collect();
    let hit = hit_fn(&pairs);
    let brier = pairs.iter().map(|(u, pp)| (pp - u) * (pp - u)).sum::<f64>() / n as f64;
    let mut ece = 0.0f64;
    for b in 0..10 {
        let lo = b as f64 / 10.0;
        let hi = (b + 1) as f64 / 10.0;
        let grp: Vec<(f64, f64)> = pairs
            .iter()
            .copied()
            .filter(|(_, pp)| *pp >= lo && (*pp < hi || (b == 9 && *pp <= 1.0)))
            .collect();
        if !grp.is_empty() {
            let emp = grp.iter().filter(|(u, _)| *u > 0.5).count() as f64 / grp.len() as f64;
            let mp = grp.iter().map(|(_, pp)| pp).sum::<f64>() / grp.len() as f64;
            ece += (mp - emp).abs() * grp.len() as f64;
        }
    }
    ece /= n as f64;
    // PSI: compare p_up distribution, first 80% vs last 20%
    let split = (n as f64 * 0.8) as usize;
    let psi = if split >= 10 && n - split >= 10 {
        let mut psi_v = 0.0f64;
        for b in 0..10 {
            let lo = b as f64 / 10.0;
            let hi = (b + 1) as f64 / 10.0;
            let cnt = |rows: &[(f64, f64)]| {
                rows.iter()
                    .filter(|(_, pp)| *pp >= lo && (*pp < hi || (b == 9 && *pp <= 1.0)))
                    .count() as f64
                    + 0.5 // smoothing to avoid log(0)
            };
            let e = cnt(&pairs[..split]) / split as f64;
            let a = cnt(&pairs[split..]) / (n - split) as f64;
            psi_v += (a - e) * (a / e).ln();
        }
        psi_v
    } else {
        f64::NAN
    };
    let strong: Vec<(f64, f64)> = pairs
        .iter()
        .copied()
        .filter(|(_, pp)| pp.max(1.0 - pp) >= 0.70)
        .collect();
    StratStats {
        n: n as i64,
        hit,
        brier,
        ece10: ece,
        psi,
        hit_first80: hit_fn(&pairs[..split.max(1).min(n)]),
        hit_last20: hit_fn(&pairs[split.min(n)..]),
        strong070: strong.len() as i64,
        strong070_win: if strong.is_empty() {
            f64::NAN
        } else {
            strong.iter().filter(|(u, _)| *u > 0.5).count() as f64 / strong.len() as f64
        },
    }
}

// ---------------------------------------------------------------------------
// Shared table helpers
// ---------------------------------------------------------------------------

fn f64_vals(a: &ArrayRef) -> &[f64] {
    as_primitive_array::<Float64Type>(a.as_ref()).values().as_ref()
}

fn fetch_table(
    registry: &Arc<RwLock<HftRegistry>>,
    name: &str,
) -> Result<Vec<RecordBatch>> {
    let reg = registry
        .read()
        .map_err(|_| DataFusionError::Execution("hft registry poisoned".into()))?;
    let out: Vec<RecordBatch> = reg
        .tables
        .get(name)
        .ok_or_else(|| DataFusionError::Execution(format!("unknown table `{name}`")))?
        .as_ref()
        .clone();
    drop(reg);
    Ok(out)
}

fn lit_f64(e: &Expr) -> Option<f64> {
    if let Expr::Literal(sv, _) = e {
        match sv {
            ScalarValue::Float64(Some(v)) => Some(*v),
            ScalarValue::Int64(Some(v)) => Some(*v as f64),
            ScalarValue::Utf8(Some(s)) => s.parse().ok(),
            _ => None,
        }
    } else {
        None
    }
}

/// Gather (ts, symbol) rows, symbol = '' when absent.
fn ts_symbols(batches: &[RecordBatch]) -> Result<(Vec<i64>, Vec<String>)> {
    let tname = ["ts_us", "ts", "t", "time"]
        .iter()
        .find(|n| {
            batches.first().and_then(|b| b.column_by_name(n)).is_some_and(|a| {
                matches!(a.data_type(), DataType::Int64 | DataType::Int32)
            })
        })
        .ok_or_else(|| DataFusionError::Execution("table has no Int64 ts column".into()))?;
    let mut ts = Vec::new();
    let mut syms = Vec::new();
    let has_sym = batches.first().and_then(|b| b.column_by_name("symbol")).is_some();
    for b in batches {
        let t = b.column_by_name(tname).unwrap();
        if matches!(t.data_type(), DataType::Int64) {
            ts.extend_from_slice(as_primitive_array::<Int64Type>(t).values());
        } else {
            for i in 0..t.len() {
                ts.push(as_primitive_array::<Int64Type>(t).value(i));
            }
        }
        if has_sym {
            let s = as_string_array(b.column_by_name("symbol").unwrap());
            for i in 0..s.len() {
                syms.push(s.value(i).to_string());
            }
        } else {
            syms.extend(std::iter::repeat(String::new()).take(t.len()));
        }
    }
    Ok((ts, syms))
}

fn count_nan_inf(batches: &[RecordBatch]) -> i64 {
    let mut n = 0i64;
    for b in batches {
        for c in b.columns() {
            if let DataType::Float64 = c.data_type() {
                for v in f64_vals(c) {
                    if !v.is_finite() {
                        n += 1;
                    }
                }
            }
        }
    }
    n
}

fn now_ns() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as i64).unwrap_or(0)
}

// ---------------------------------------------------------------------------
// dq_report / dq_check
// ---------------------------------------------------------------------------

fn run_dq(batches: &[RecordBatch]) -> Result<DqStats> {
    let (ts, syms) = ts_symbols(batches)?;
    let sym_refs: Vec<&str> = syms.iter().map(String::as_str).collect();
    Ok(dq_analyze(&ts, &sym_refs, count_nan_inf(batches)))
}

#[derive(Debug)]
pub struct DqReportTableFunction {
    registry: Arc<RwLock<HftRegistry>>,
}

impl DqReportTableFunction {
    pub fn new(registry: Arc<RwLock<HftRegistry>>) -> Self {
        Self { registry }
    }
}

impl TableFunctionImpl for DqReportTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn datafusion::datasource::TableProvider>> {
        let name = expr_to_string(
            args.exprs()
                .first()
                .ok_or_else(|| DataFusionError::Execution("dq_report(name): missing name".into()))?,
        )?;
        let batches = fetch_table(&self.registry, &name)?;
        let s = run_dq(&batches)?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("table", DataType::Utf8, false),
            Field::new("rows", DataType::Int64, false),
            Field::new("n_symbols", DataType::Int64, false),
            Field::new("nan_inf", DataType::Int64, false),
            Field::new("dup_ts_sym", DataType::Int64, false),
            Field::new("non_monotonic", DataType::Int64, false),
            Field::new("min_rows_sym", DataType::Int64, false),
            Field::new("max_rows_sym", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![name.clone()])) as ArrayRef,
                Arc::new(Int64Array::from(vec![s.rows])) as ArrayRef,
                Arc::new(Int64Array::from(vec![s.n_symbols])) as ArrayRef,
                Arc::new(Int64Array::from(vec![s.nan_inf])) as ArrayRef,
                Arc::new(Int64Array::from(vec![s.dup_ts_sym])) as ArrayRef,
                Arc::new(Int64Array::from(vec![s.non_monotonic])) as ArrayRef,
                Arc::new(Int64Array::from(vec![s.min_rows_sym])) as ArrayRef,
                Arc::new(Int64Array::from(vec![s.max_rows_sym])) as ArrayRef,
            ],
        )?;
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}

#[derive(Debug)]
pub struct DqCheckTableFunction {
    registry: Arc<RwLock<HftRegistry>>,
}

impl DqCheckTableFunction {
    pub fn new(registry: Arc<RwLock<HftRegistry>>) -> Self {
        Self { registry }
    }
}

impl TableFunctionImpl for DqCheckTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn datafusion::datasource::TableProvider>> {
        let name = expr_to_string(
            args.exprs()
                .first()
                .ok_or_else(|| DataFusionError::Execution("dq_check(name): missing name".into()))?,
        )?;
        let batches = fetch_table(&self.registry, &name)?;
        let s = run_dq(&batches)?;
        let issues: Vec<(&str, i64)> = vec![
            ("nan_or_inf", s.nan_inf),
            ("duplicate_ts_symbol", s.dup_ts_sym),
            ("non_monotonic_ts", s.non_monotonic),
            ("low_symbol_rows", (s.max_rows_sym - s.min_rows_sym).max(0)),
        ]
        .into_iter()
        .filter(|(_, n)| *n > 0)
        .collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new("issue", DataType::Utf8, false),
            Field::new("count", DataType::Int64, false),
        ]));
        let mut isc = Vec::new();
        let mut cnt = Vec::new();
        for (i, c) in issues {
            isc.push(i.to_string());
            cnt.push(c);
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(isc)) as ArrayRef,
                Arc::new(Int64Array::from(cnt)) as ArrayRef,
            ],
        )?;
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}

// ---------------------------------------------------------------------------
// health_check(name[, max_age_days[, min_rows]])
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct HealthCheckTableFunction {
    registry: Arc<RwLock<HftRegistry>>,
}

impl HealthCheckTableFunction {
    pub fn new(registry: Arc<RwLock<HftRegistry>>) -> Self {
        Self { registry }
    }
}

impl TableFunctionImpl for HealthCheckTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn datafusion::datasource::TableProvider>> {
        let exprs = args.exprs();
        let name = expr_to_string(
            exprs.first().ok_or_else(|| {
                DataFusionError::Execution("health_check(name[,max_age_days[,min_rows]]): missing name".into())
            })?,
        )?;
        let max_age = exprs.get(1).and_then(lit_f64).unwrap_or(7.0).max(0.0);
        let min_rows = exprs.get(2).and_then(lit_f64).unwrap_or(50.0).max(1.0) as i64;
        let batches = fetch_table(&self.registry, &name)?;
        let s = run_dq(&batches)?;
        let (ts, _) = ts_symbols(&batches)?;
        let last = ts.iter().copied().max();
        let age_days = last
            .map(|t| (now_ns() - t) as f64 / 86_400_000_000_000.0)
            .unwrap_or(f64::INFINITY);
        let mut checks: Vec<(String, &'static str, String)> = Vec::new();
        let fresh = age_days.is_finite() && age_days <= max_age;
        checks.push((
            "freshness".into(),
            if fresh { "ok" } else { "warn" },
            format!("last bar {} days ago (limit {max_age})", age_days.round()),
        ));
        checks.push((
            "row_count".into(),
            if s.rows >= min_rows { "ok" } else { "warn" },
            format!("{} rows (min {min_rows})", s.rows),
        ));
        checks.push((
            "no_nan".into(),
            if s.nan_inf == 0 { "ok" } else { "warn" },
            format!("{} non-finite float values", s.nan_inf),
        ));
        checks.push((
            "no_duplicates".into(),
            if s.dup_ts_sym == 0 { "ok" } else { "warn" },
            format!("{} duplicate (ts,symbol)", s.dup_ts_sym),
        ));
        checks.push((
            "ts_sorted".into(),
            if s.non_monotonic == 0 { "ok" } else { "warn" },
            format!("{} backwards jumps", s.non_monotonic),
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("check", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, false),
            Field::new("detail", DataType::Utf8, false),
        ]));
        let (mut c, mut st, mut de): (Vec<String>, Vec<String>, Vec<String>) =
            (Vec::new(), Vec::new(), Vec::new());
        for (a, b, d) in checks {
            c.push(a);
            st.push(b.to_string());
            de.push(d);
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(c)) as ArrayRef,
                Arc::new(StringArray::from(st)) as ArrayRef,
                Arc::new(StringArray::from(de)) as ArrayRef,
            ],
        )?;
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}

// ---------------------------------------------------------------------------
// strategy_stats(name)
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct StrategyStatsTableFunction {
    registry: Arc<RwLock<HftRegistry>>,
}

impl StrategyStatsTableFunction {
    pub fn new(registry: Arc<RwLock<HftRegistry>>) -> Self {
        Self { registry }
    }
}

impl TableFunctionImpl for StrategyStatsTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn datafusion::datasource::TableProvider>> {
        let name = expr_to_string(
            args.exprs()
                .first()
                .ok_or_else(|| DataFusionError::Execution("strategy_stats(name): missing name".into()))?,
        )?;
        let batches = fetch_table(&self.registry, &name)?;
        // outcome column: up (or next_day_return-like), probability: cal/raw
        let upname = ["up", "next_day_return", "actual"]
            .iter()
            .find(|n| batches.first().and_then(|b| b.column_by_name(n)).is_some())
            .copied()
            .ok_or_else(|| DataFusionError::Execution("strategy_stats: no up/actual column".into()))?;
        let pname = ["cal_p_up_platt", "p_up", "cal_p_up_iso", "prob"]
            .iter()
            .find(|n| batches.first().and_then(|b| b.column_by_name(n)).is_some())
            .copied()
            .ok_or_else(|| DataFusionError::Execution("strategy_stats: no p_up/cal column".into()))?;
        let mut up = Vec::new();
        let mut p = Vec::new();
        for b in &batches {
            let a = b.column_by_name(upname).unwrap();
            let pa = b.column_by_name(pname).unwrap();
            for i in 0..a.len() {
                let u = match a.data_type() {
                    DataType::Float64 => f64_vals(a)[i],
                    DataType::Int64 | DataType::Int32 => as_primitive_array::<Int64Type>(a).value(i) as f64,
                    _ => f64::NAN,
                };
                let pp = match pa.data_type() {
                    DataType::Float64 => f64_vals(pa)[i],
                    _ => f64::NAN,
                };
                if u.is_finite() && pp.is_finite() {
                    up.push(u);
                    p.push(pp);
                }
            }
        }
        let s = strategy_stats(&up, &p);
        let schema = Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, false),
            Field::new("hit_rate", DataType::Float64, false),
            Field::new("brier", DataType::Float64, false),
            Field::new("ece10", DataType::Float64, false),
            Field::new("psi_pup", DataType::Float64, true),
            Field::new("hit_first80", DataType::Float64, true),
            Field::new("hit_last20", DataType::Float64, true),
            Field::new("strong070_n", DataType::Int64, false),
            Field::new("strong070_win", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![s.n])) as ArrayRef,
                Arc::new(Float64Array::from(vec![s.hit])) as ArrayRef,
                Arc::new(Float64Array::from(vec![s.brier])) as ArrayRef,
                Arc::new(Float64Array::from(vec![s.ece10])) as ArrayRef,
                Arc::new(Float64Array::from(vec![s.psi])) as ArrayRef,
                Arc::new(Float64Array::from(vec![s.hit_first80])) as ArrayRef,
                Arc::new(Float64Array::from(vec![s.hit_last20])) as ArrayRef,
                Arc::new(Int64Array::from(vec![s.strong070])) as ArrayRef,
                Arc::new(Float64Array::from(vec![s.strong070_win])) as ArrayRef,
            ],
        )?;
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dq_detects_duplicates_and_backjumps() {
        let ts = [1i64, 2, 2, 4, 3, 5];
        let sym = ["A", "A", "B", "A", "A", "A"];
        let s = dq_analyze(&ts, &sym, 0);
        assert_eq!(s.dup_ts_sym, 0, "dups are across symbols at ts=2?");
        assert!(s.non_monotonic >= 1, "A: 4 -> 3 backjump");
    }

    #[test]
    fn dq_counts_duplicate_pairs() {
        let ts = [1i64, 1, 1, 2];
        let sym = ["A", "A", "B", "A"];
        let s = dq_analyze(&ts, &sym, 3);
        assert_eq!(s.dup_ts_sym, 1, "one duplicate (1,A)");
        assert_eq!(s.nan_inf, 3);
        assert_eq!(s.n_symbols, 2);
    }

    #[test]
    fn strat_metrics_on_perfect_calibration() {
        let up = [1.0, 0.0, 1.0, 0.0];
        let p = [1.0, 0.0, 1.0, 0.0];
        let s = strategy_stats(&up, &p);
        assert_eq!(s.hit, 1.0);
        assert!(s.brier < 1e-12);
        assert!(s.ece10 < 1e-12);
    }
}
