//! `fwd_proba(name, horizon, k [, feats])` — historical-analog trend
//! probability.
//!
//! Given a registered table of **daily bars** (any Int64 time column
//! `t`/`ts`/`time`/`ts_us`, a `close` or `price` Float64 column), returns a
//! single decision row:
//!
//! ```text
//! t, close, p_up, p_down, n_analogs, hit_rate, n_labeled
//! ```
//!
//! Semantics (see doc/stock_analysis.md):
//! * Rows must be ascending by time. Label for row `i` = `close[i+H] > close[i]`
//!   (needs `i+H < n`, so the last `H` rows are label-less; the *decision* row
//!   is the very last row).
//! * Features: when `feats` lists column names they must exist as Float64
//!   columns; when omitted (or empty) the built-in feature set is used —
//!   `mom5`, `mom10`, `vol10`, `above_sma20` computed from `close`.
//! * Features are z-normalised over the labelled rows only; the decision row is
//!   scored with those statistics.
//! * `p_up`/`p_down` = fraction of the `min(k, n_labeled)` nearest labelled
//!   rows (Euclidean in feature space) with positive / negative forward return.
//! * `hit_rate` = leave-one-out accuracy of the same rule over the labelled
//!   history.

use std::sync::{Arc, RwLock};

use arrow::array::{as_primitive_array, ArrayRef, Float64Array, Int64Array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Float64Type, Int64Type, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl};
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result};

use crate::expr_util::{expr_to_i64, expr_to_string};
use crate::hft_exec::HftRegistry;

/// The `fwd_proba` table function over the compiled-kernel table registry.
#[derive(Debug)]
pub struct FwdProbaTableFunction {
    registry: Arc<RwLock<HftRegistry>>,
}

impl FwdProbaTableFunction {
    pub fn new(registry: Arc<RwLock<HftRegistry>>) -> Self {
        Self { registry }
    }
}

fn output_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("t", DataType::Int64, false),
        Field::new("close", DataType::Float64, false),
        Field::new("p_up", DataType::Float64, false),
        Field::new("p_down", DataType::Float64, false),
        Field::new("n_analogs", DataType::Int64, false),
        Field::new("hit_rate", DataType::Float64, false),
        Field::new("n_labeled", DataType::Int64, false),
    ]))
}

/// Pull one column across all batches as `Vec<i64>` (casting if needed).
fn col_i64(batches: &[RecordBatch], name: &str) -> Result<Vec<i64>> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column_by_name(name)
            .ok_or_else(|| DataFusionError::Execution(format!("`{name}` column missing")))?;
        let casted = cast(arr, &DataType::Int64)
            .map_err(|e| DataFusionError::Execution(format!("`{name}` as Int64: {e}")))?;
        out.extend_from_slice(as_primitive_array::<Int64Type>(casted.as_ref()).values());
    }
    Ok(out)
}

/// Pull one column across all batches as `Vec<f64>` (casting if needed).
fn col_f64(batches: &[RecordBatch], name: &str) -> Result<Vec<f64>> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column_by_name(name)
            .ok_or_else(|| DataFusionError::Execution(format!("`{name}` column missing")))?;
        let casted = cast(arr, &DataType::Float64)
            .map_err(|e| DataFusionError::Execution(format!("`{name}` as Float64: {e}")))?;
        out.extend_from_slice(as_primitive_array::<Float64Type>(casted.as_ref()).values());
    }
    Ok(out)
}

/// First schema column matching any candidate name.
fn pick<'a>(batches: &[RecordBatch], names: &[&'a str]) -> Option<&'a str> {
    let probe = batches.first()?;
    names
        .iter()
        .copied()
        .find(|n| probe.schema().field_with_name(n).is_ok())
}

fn pick_i64(batches: &[RecordBatch], names: &[&str], what: &str) -> Result<Vec<i64>> {
    pick(batches, names)
        .map(|n| col_i64(batches, n))
        .unwrap_or_else(|| {
            Err(DataFusionError::Execution(format!(
                "fwd_proba: no {what} column in {names:?}"
            )))
        })
}

fn pick_f64(batches: &[RecordBatch], names: &[&str], what: &str) -> Result<Vec<f64>> {
    pick(batches, names)
        .map(|n| col_f64(batches, n))
        .unwrap_or_else(|| {
            Err(DataFusionError::Execution(format!(
                "fwd_proba: no {what} column in {names:?}"
            )))
        })
}

/// Built-in feature set computed from daily `close` (rows sorted ascending):
/// `[mom5, mom10, vol10, above_sma20]`.
fn auto_features(closes: &[f64]) -> Vec<Vec<f64>> {
    let n = closes.len();
    let mut out = vec![vec![0.0f64; 4]; n];
    // mom5 / mom10 / above-sma20 per bar (windowed sums, no index loops).
    for (i, row) in out.iter_mut().enumerate() {
        let a5 = i.saturating_sub(5);
        let a10 = i.saturating_sub(10);
        row[0] = closes[i] / closes[a5] - 1.0; // mom5
        row[1] = closes[i] / closes[a10] - 1.0; // mom10
        let lo = i.saturating_sub(19);
        let s: f64 = closes[lo..=i].iter().sum();
        row[3] = closes[i] / (s / (i - lo + 1) as f64) - 1.0; // above sma20
    }
    // vol10: stddev of trailing daily returns (windows over consecutive pairs).
    for (i, row) in out.iter_mut().enumerate().skip(1) {
        let lo = i.saturating_sub(9).max(1);
        let seg = &closes[(lo - 1)..=i];
        let (mut sum, mut ss) = (0.0f64, 0.0f64);
        for w in seg.windows(2) {
            let r = w[1] / w[0] - 1.0;
            sum += r;
            ss += r * r;
        }
        let c = (seg.len() - 1) as f64;
        let mean = sum / c;
        row[2] = (ss / c - mean * mean).max(0.0).sqrt();
    }
    out
}

/// z-normalise rows using statistics of the labelled prefix `[0, m)`.
fn zscore_with(feats: &[Vec<f64>], m: usize) -> (Vec<Vec<f64>>, Vec<f64>) {
    let d = feats.first().map_or(0, |r| r.len());
    let mut mean = vec![0.0f64; d];
    for r in &feats[..m] {
        for j in 0..d {
            mean[j] += r[j];
        }
    }
    if m > 0 {
        for v in mean.iter_mut() {
            *v /= m as f64;
        }
    }
    let mut std = vec![0.0f64; d];
    for r in &feats[..m] {
        for j in 0..d {
            let z = r[j] - mean[j];
            std[j] += z * z;
        }
    }
    for v in std.iter_mut() {
        *v = (*v / (m.max(1) as f64)).sqrt();
        if *v == 0.0 {
            *v = 1.0; // constant feature: no discriminative power
        }
    }
    let norm = |r: &[f64]| -> Vec<f64> { (0..d).map(|j| (r[j] - mean[j]) / std[j]).collect() };
    let scaled: Vec<Vec<f64>> = feats.iter().map(|r| norm(r)).collect();
    (scaled, norm(&feats[feats.len() - 1]))
}

fn sq_dist(a: &[f64], b: &[f64]) -> f64 {
    a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
}

/// `(p_up, p_down, n_used, hit_rate)` over the labelled prefix `[0, m)`.
fn analog(scaled: &[Vec<f64>], labels: &[bool], query: &[f64], k: usize) -> (f64, f64, usize, f64) {
    let m = labels.len();

    // Leave-one-out hit rate over the labelled history.
    let mut correct = 0usize;
    for i in 0..m {
        let mut others: Vec<(f64, usize)> = (0..m)
            .filter(|&j| j != i)
            .map(|j| (sq_dist(&scaled[i], &scaled[j]), j))
            .collect();
        others.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        let take = others.len().min(k);
        let ups = others[..take].iter().filter(|(_, j)| labels[*j]).count();
        if (ups as f64 >= take as f64 / 2.0) == labels[i] {
            correct += 1;
        }
    }
    let hit_rate = if m > 0 { correct as f64 / m as f64 } else { 0.0 };

    // Decision-row prediction (neighbours only from the labelled set).
    let mut dists: Vec<(f64, usize)> = (0..m)
        .map(|j| (sq_dist(query, &scaled[j]), j))
        .collect();
    dists.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    let n_used = dists.len().min(k);
    let ups = dists[..n_used].iter().filter(|(_, j)| labels[*j]).count();
    let downs = dists[..n_used].iter().filter(|(_, j)| !labels[*j]).count();
    (
        ups as f64 / n_used as f64,
        downs as f64 / n_used as f64,
        n_used,
        hit_rate,
    )
}

fn compute(
    times: Vec<i64>,
    closes: Vec<f64>,
    feats: Vec<Vec<f64>>,
    horizon: usize,
    k: usize,
) -> Result<RecordBatch> {
    let n = closes.len();
    if n < horizon + 2 {
        return Err(DataFusionError::Execution(format!(
            "fwd_proba: need > {horizon}+1 bars (got {n})"
        )));
    }
    let m = n - horizon; // labelled rows [0, m)
    let labels: Vec<bool> = (0..m).map(|i| closes[i + horizon] > closes[i]).collect();
    let (scaled, query) = zscore_with(&feats, m);
    let (p_up, p_down, n_used, hit_rate) = analog(&scaled, &labels, &query, k.max(1));
    let batch = RecordBatch::try_new(
        output_schema(),
        vec![
            Arc::new(Int64Array::from(vec![times[n - 1]])) as ArrayRef,
            Arc::new(Float64Array::from(vec![closes[n - 1]])) as ArrayRef,
            Arc::new(Float64Array::from(vec![p_up])) as ArrayRef,
            Arc::new(Float64Array::from(vec![p_down])) as ArrayRef,
            Arc::new(Int64Array::from(vec![n_used as i64])) as ArrayRef,
            Arc::new(Float64Array::from(vec![hit_rate])) as ArrayRef,
            Arc::new(Int64Array::from(vec![m as i64])) as ArrayRef,
        ],
    )
    .map_err(|e| DataFusionError::Execution(format!("fwd_proba output: {e}")))?;
    Ok(batch)
}

impl TableFunctionImpl for FwdProbaTableFunction {
    fn call_with_args(
        &self,
        args: TableFunctionArgs,
    ) -> Result<Arc<dyn datafusion::datasource::TableProvider>> {
        let exprs = args.exprs();
        let name = expr_to_string(exprs.first().ok_or_else(|| {
            DataFusionError::Execution("fwd_proba(name, horizon, k [, feats]): missing name".into())
        })?)?;
        let horizon = expr_to_i64(exprs.get(1).ok_or_else(|| {
            DataFusionError::Execution("fwd_proba(name, horizon, k [, feats]): missing horizon".into())
        })?)?
        .max(1) as usize;
        let k = expr_to_i64(exprs.get(2).ok_or_else(|| {
            DataFusionError::Execution("fwd_proba(name, horizon, k [, feats]): missing k".into())
        })?)?
        .max(1) as usize;

        let reg = self.registry.read().map_err(|_| {
            DataFusionError::Execution("hft registry poisoned".into())
        })?;
        let batches = reg.tables.get(&name).ok_or_else(|| {
            DataFusionError::Execution(format!("fwd_proba: unknown table `{name}`"))
        })?;
        let batches: Vec<RecordBatch> = batches.iter().cloned().collect();
        drop(reg);

        let times = pick_i64(&batches, &["t", "ts", "time", "ts_us"], "time")?;
        let closes = pick_f64(&batches, &["close", "price", "adjclose"], "close/price")?;
        if times.len() != closes.len() || times.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "fwd_proba: table `{name}` is empty or columns are ragged"
            )));
        }

        let feats = match exprs.get(3) {
            Some(fe) => {
                let csv = expr_to_string(fe)?;
                let names: Vec<String> = csv
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                if names.is_empty() {
                    auto_features(&closes)
                } else {
                    let mut feats = vec![Vec::with_capacity(names.len()); closes.len()];
                    for fn_ in &names {
                        let col = col_f64(&batches, fn_)?;
                        if col.len() != closes.len() {
                            return Err(DataFusionError::Execution(format!(
                                "fwd_proba: column `{fn_}` length {} != rows {}",
                                col.len(),
                                closes.len()
                            )));
                        }
                        for (i, v) in col.into_iter().enumerate() {
                            feats[i].push(v);
                        }
                    }
                    feats
                }
            }
            None => auto_features(&closes),
        };

        let out = compute(times, closes, feats, horizon, k)?;
        Ok(Arc::new(MemTable::try_new(out.schema(), vec![vec![out]])?))
    }
}

// ---------------------------------------------------------------------------
// fwd_walk(name, horizon, k [, warmup] [, feats]) — walk-forward calibration
// ---------------------------------------------------------------------------

// Strictly-causal walk-forward twin of [`FwdProbaTableFunction`]: for every
// bar `p` in `[warmup, m)` it predicts using **only** labelled rows before
// `p` (neighbours + z-normalisation statistics are recomputed from `[0,p)`),
// then records the realised outcome. The result is the honest backtest of the
// historical-analog rule — feed the rows to a calibration/bucketing layer
// (see `stock_calib.sh`) to obtain hit-rate by confidence bucket and a
// threshold suggestion.

fn walk_output_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("t", DataType::Int64, false),
        Field::new("close", DataType::Float64, false),
        Field::new("p_up", DataType::Float64, false),
        Field::new("p_down", DataType::Float64, false),
        Field::new("up", DataType::Int64, false), // realised: close[t+H] > close[t]
        Field::new("n_analogs", DataType::Int64, false),
    ]))
}

/// z-normalisation statistics (mean, std) of feature rows `[0, p)`.
fn z_stats(feats: &[Vec<f64>]) -> (Vec<f64>, Vec<f64>) {
    let d = feats.first().map_or(0, |r| r.len());
    let n = feats.len() as f64;
    let mut mean = vec![0.0f64; d];
    for r in feats {
        for (j, v) in r.iter().enumerate() {
            mean[j] += v;
        }
    }
    if n > 0.0 {
        for v in mean.iter_mut() {
            *v /= n;
        }
    }
    let mut std = vec![0.0f64; d];
    for r in feats {
        for (j, v) in r.iter().enumerate() {
            let z = v - mean[j];
            std[j] += z * z;
        }
    }
    for v in std.iter_mut() {
        *v = (*v / n.max(1.0)).sqrt();
        if *v == 0.0 {
            *v = 1.0;
        }
    }
    (mean, std)
}

fn z_apply(feats: &[Vec<f64>], mean: &[f64], std: &[f64]) -> Vec<Vec<f64>> {
    feats
        .iter()
        .map(|r| {
            r.iter()
                .zip(mean.iter().zip(std.iter()))
                .map(|(v, (m, s))| (v - m) / s)
                .collect()
        })
        .collect()
}

/// Walk-forward prediction rows: `(t, close, p_up, p_down, up, n_analogs)`.
fn walk_compute(
    times: Vec<i64>,
    closes: Vec<f64>,
    feats: Vec<Vec<f64>>,
    horizon: usize,
    k: usize,
    warmup: usize,
) -> Result<RecordBatch> {
    let n = closes.len();
    if n < horizon + 2 {
        return Err(DataFusionError::Execution(format!(
            "fwd_walk: need > {horizon}+1 bars (got {n})"
        )));
    }
    let m = n - horizon; // labelled rows [0, m)
    let labels: Vec<bool> = (0..m).map(|i| closes[i + horizon] > closes[i]).collect();
    let warm = warmup.clamp(1, m.saturating_sub(1));
    let k = k.max(1);

    let mut t = Vec::new();
    let mut close = Vec::new();
    let mut p_up = Vec::new();
    let mut p_down = Vec::new();
    let mut up = Vec::new();
    let mut n_analogs = Vec::new();

    for p in warm..m {
        // Training set = labelled rows strictly before p, recomputed each step
        // so the exercise is fully causal (no future leakage).
        let (mean, std) = z_stats(&feats[..p]);
        let scaled = z_apply(&feats[..p], &mean, &std);
        let q = {
            let mut v = Vec::with_capacity(feats[p].len());
            for (x, (m_, s)) in feats[p].iter().zip(mean.iter().zip(std.iter())) {
                v.push((x - m_) / s);
            }
            v
        };
        let mut dists: Vec<(f64, usize)> = (0..p)
            .map(|j| (sq_dist(&scaled[j], &q), j))
            .collect();
        dists.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        let used = dists.len().min(k);
        let ups = dists[..used].iter().filter(|(_, j)| labels[*j]).count();
        let pu = ups as f64 / used as f64;
        let pd = (used - ups) as f64 / used as f64;
        t.push(times[p]);
        close.push(closes[p]);
        p_up.push(pu);
        p_down.push(pd);
        up.push(labels[p] as i64);
        n_analogs.push(used as i64);
    }

    if t.is_empty() {
        return Err(DataFusionError::Execution(format!(
            "fwd_walk: warmup {warm} leaves no rows to evaluate (m={m})"
        )));
    }
    RecordBatch::try_new(
        walk_output_schema(),
        vec![
            Arc::new(Int64Array::from(t)) as ArrayRef,
            Arc::new(Float64Array::from(close)) as ArrayRef,
            Arc::new(Float64Array::from(p_up)) as ArrayRef,
            Arc::new(Float64Array::from(p_down)) as ArrayRef,
            Arc::new(Int64Array::from(up)) as ArrayRef,
            Arc::new(Int64Array::from(n_analogs)) as ArrayRef,
        ],
    )
    .map_err(|e| DataFusionError::Execution(format!("fwd_walk output: {e}")))
}

/// `fwd_walk(name, horizon, k [, warmup] [, feats])` table function.
#[derive(Debug)]
pub struct FwdWalkTableFunction {
    registry: Arc<RwLock<HftRegistry>>,
}

impl FwdWalkTableFunction {
    pub fn new(registry: Arc<RwLock<HftRegistry>>) -> Self {
        Self { registry }
    }
}

impl TableFunctionImpl for FwdWalkTableFunction {
    fn call_with_args(
        &self,
        args: TableFunctionArgs,
    ) -> Result<Arc<dyn datafusion::datasource::TableProvider>> {
        let exprs = args.exprs();
        let name = expr_to_string(exprs.first().ok_or_else(|| {
            DataFusionError::Execution("fwd_walk(name, horizon, k [, warmup] [, feats]): missing name".into())
        })?)?;
        let horizon = expr_to_i64(exprs.get(1).ok_or_else(|| {
            DataFusionError::Execution("fwd_walk: missing horizon".into())
        })?)?
        .max(1) as usize;
        let k = expr_to_i64(exprs.get(2).ok_or_else(|| {
            DataFusionError::Execution("fwd_walk: missing k".into())
        })?)?
        .max(1) as usize;

        let reg = self.registry.read().map_err(|_| {
            DataFusionError::Execution("hft registry poisoned".into())
        })?;
        let batches = reg.tables.get(&name).ok_or_else(|| {
            DataFusionError::Execution(format!("fwd_walk: unknown table `{name}`"))
        })?;
        let batches: Vec<RecordBatch> = batches.iter().cloned().collect();
        drop(reg);

        let times = pick_i64(&batches, &["t", "ts", "time", "ts_us"], "time")?;
        let closes = pick_f64(&batches, &["close", "price", "adjclose"], "close/price")?;
        if times.len() != closes.len() || times.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "fwd_walk: table `{name}` is empty or columns are ragged"
            )));
        }
        let warmup = match exprs.get(3) {
            Some(e) => expr_to_i64(e)?.max(1) as usize,
            None => (k.max(2) * 10).max(20), // default warm-up ≈ 20 (or k*10) past bars
        };
        let feats = match exprs.get(4) {
            Some(fe) => {
                let csv = expr_to_string(fe)?;
                let names: Vec<String> = csv
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                if names.is_empty() {
                    auto_features(&closes)
                } else {
                    let mut feats = vec![Vec::with_capacity(names.len()); closes.len()];
                    for fn_ in &names {
                        let col = col_f64(&batches, fn_)?;
                        if col.len() != closes.len() {
                            return Err(DataFusionError::Execution(format!(
                                "fwd_walk: column `{fn_}` length {} != rows {}",
                                col.len(),
                                closes.len()
                            )));
                        }
                        for (i, v) in col.into_iter().enumerate() {
                            feats[i].push(v);
                        }
                    }
                    feats
                }
            }
            None => auto_features(&closes),
        };

        let out = walk_compute(times, closes, feats, horizon, k, warmup)?;
        Ok(Arc::new(MemTable::try_new(out.schema(), vec![vec![out]])?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rising_series_predicts_up_auto_features() {
        // Steadily rising closes: the decision bar's mom/sma features should
        // match mostly up-labelled neighbours.
        let n = 120;
        let horizon = 3usize;
        let times: Vec<i64> = (0..n as i64).collect();
        let closes: Vec<f64> = (0..n).map(|i| 100.0 + i as f64).collect();
        let feats = auto_features(&closes);
        let batch = compute(times, closes, feats, horizon, 5).unwrap();
        let cols = batch.columns();
        let pu = as_primitive_array::<Float64Type>(cols[2].as_ref()).value(0);
        let pd = as_primitive_array::<Float64Type>(cols[3].as_ref()).value(0);
        assert!(pu > pd, "p_up={pu} should exceed p_down={pd} on a rising series");
        assert!(pu >= 0.5);
        assert_eq!(cols[4].as_ref().len(), 1);
    }


    #[test]
    fn walk_on_rising_series_is_fully_up_and_causal() {
        // Strictly rising closes: every walk-forward bar has only up-labelled
        // history, so p_up == 1 and every prediction is correct.
        let n = 120;
        let horizon = 3usize;
        let times: Vec<i64> = (0..n as i64).collect();
        let closes: Vec<f64> = (0..n).map(|i| 100.0 + i as f64).collect();
        let feats = auto_features(&closes);
        let batch = walk_compute(times, closes, feats, horizon, 5, 30).unwrap();
        let cols = batch.columns();
        let pu = as_primitive_array::<Float64Type>(cols[2].as_ref());
        let up = as_primitive_array::<Int64Type>(cols[4].as_ref());
        assert_eq!(up.len(), 120 - horizon - 30);
        for i in 0..up.len() {
            assert_eq!(pu.value(i), 1.0, "row {i}: p_up must be 1 on pure uptrend");
            assert_eq!(up.value(i), 1);
        }
    }

    #[test]
    fn too_few_bars_is_an_error() {
        let err = compute(vec![0, 1], vec![1.0, 2.0], vec![vec![0.0], vec![0.1]], 5, 3)
            .unwrap_err();
        assert!(err.to_string().contains("need > 5+1 bars"), "{err}");
    }
}
