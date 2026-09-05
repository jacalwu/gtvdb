//! `fwd_proba(name, horizon, k, feats)` — historical-analog trend probability.
//!
//! Given a registered table of **daily bars** with a monotonically increasing
//! `t` (Int64) column, a `close` (Float64) column and one Float64 column per
//! comma-separated entry of `feats` (already-computed technical features), this
//! table function returns a single row:
//!
//! ```text
//! t, close, p_up, p_down, n_analogs, hit_rate, n_labeled
//! ```
//!
//! Semantics (see doc/stock_analysis.md):
//! * Label for row `i` = `sign(close[i+H] − close[i])` (needs `i+H` < n, so the
//!   last `H` rows are label-less; the *decision* row is the very last row).
//! * Features are z-normalised over the labelled rows only; the decision row is
//!   scored with those statistics.
//! * `p_up`/`p_down` = fraction of the `min(k, n_labeled)` nearest labelled
//!   historical rows (Euclidean in feature space) whose forward return was
//!   positive / negative.
//! * `hit_rate` = leave-one-out accuracy of the same rule over the labelled
//!   history (direction predicted by neighbour majority vs actual label).
//!
//! The model is explainable and requires no training: as history grows the
//! analog base grows with it.

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
        out.extend_from_slice(
            as_primitive_array::<Int64Type>(casted.as_ref()).values(),
        );
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
        out.extend_from_slice(
            as_primitive_array::<Float64Type>(casted.as_ref()).values(),
        );
    }
    Ok(out)
}

/// z-normalise columns (`feats` row-major, n×d) using stats of the *labelled*
/// rows `[0, m)`; the decision vector is the last row.
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
    let norm = |r: &[f64]| -> Vec<f64> {
        (0..d).map(|j| (r[j] - mean[j]) / std[j]).collect()
    };
    let scaled: Vec<Vec<f64>> = feats.iter().map(|r| norm(r)).collect();
    (scaled, norm(&feats[feats.len() - 1]))
}

fn sq_dist(a: &[f64], b: &[f64]) -> f64 {
    a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
}

/// Analog probability over labelled history.
///
/// `labels[i]` = true when row `i` (of the labelled set) went up over the
/// horizon. Returns `(p_up, p_down, n_used, hit_rate)` where `hit_rate` is
/// leave-one-out accuracy across the labelled set.
fn analog(
    scaled: &[Vec<f64>],
    labels: &[bool],
    query: &[f64],
    k: usize,
) -> (f64, f64, usize, f64) {
    let m = labels.len();
    // `scaled` has one row per bar (labelled prefix [0, m) + unlabelled tail,
    // including the decision row); neighbours are only drawn from [0, m).

    // Leave-one-out hit rate.
    let mut correct = 0usize;
    for i in 0..m {
        let mut others: Vec<(f64, usize)> = (0..m)
            .filter(|&j| j != i)
            .map(|j| (sq_dist(&scaled[i], &scaled[j]), j))
            .collect();
        others.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        let take = others.len().min(k);
        let ups = others[..take].iter().filter(|(_, j)| labels[*j]).count();
        let pred_up = ups as f64 >= take as f64 / 2.0;
        if pred_up == labels[i] {
            correct += 1;
        }
    }
    let hit_rate = if m > 0 { correct as f64 / m as f64 } else { 0.0 };

    // Decision-row prediction.
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
    let schema = output_schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
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
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn datafusion::datasource::TableProvider>> {
        let exprs = args.exprs();
        let name = expr_to_string(
            exprs.first().ok_or_else(|| {
                DataFusionError::Execution("fwd_proba(name, horizon, k, feats): missing name".into())
            })?,
        )?;
        let horizon = expr_to_i64(
            exprs.get(1).ok_or_else(|| {
                DataFusionError::Execution("fwd_proba(name, horizon, k, feats): missing horizon".into())
            })?,
        )?
        .max(1) as usize;
        let k = expr_to_i64(
            exprs.get(2).ok_or_else(|| {
                DataFusionError::Execution("fwd_proba(name, horizon, k, feats): missing k".into())
            })?,
        )?
        .max(1) as usize;
        let feats_csv = expr_to_string(
            exprs.get(3).ok_or_else(|| {
                DataFusionError::Execution("fwd_proba(name, horizon, k, feats): missing feats".into())
            })?,
        )?;
        let feat_names: Vec<String> = feats_csv
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if feat_names.is_empty() {
            return Err(DataFusionError::Execution(
                "fwd_proba: feats must list at least one column".into(),
            ));
        }

        let reg = self.registry.read().map_err(|_| {
            DataFusionError::Execution("hft registry poisoned".into())
        })?;
        let batches = reg.tables.get(&name).ok_or_else(|| {
            DataFusionError::Execution(format!("fwd_proba: unknown table `{name}`"))
        })?;
        let batches: Vec<RecordBatch> = batches.iter().cloned().collect();
        drop(reg);

        let times = col_i64(&batches, "t")?;
        let closes = col_f64(&batches, "close")?;
        let mut feats = Vec::with_capacity(times.len());
        for fn_ in &feat_names {
            let col = col_f64(&batches, fn_)?;
            if col.len() != times.len() {
                return Err(DataFusionError::Execution(format!(
                    "fwd_proba: column `{fn_}` length {} != rows {}",
                    col.len(),
                    times.len()
                )));
            }
            // transpose: feature column j -> row vector element j
            if feats.is_empty() {
                feats = vec![Vec::with_capacity(feat_names.len()); times.len()];
            }
            for (i, v) in col.into_iter().enumerate() {
                feats[i].push(v);
            }
        }
        if times.len() != closes.len() || times.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "fwd_proba: table `{name}` is empty or ragged"
            )));
        }

        let out = compute(times, closes, feats, horizon, k)?;
        Ok(Arc::new(MemTable::try_new(out.schema(), vec![vec![out]])?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rising_series_predicts_up() {
        // A steadily rising close with a momentum feature: the decision row
        // (last) should find mostly up-labelled neighbours.
        let n = 60;
        let horizon = 3usize;
        let times: Vec<i64> = (0..n as i64).collect();
        let closes: Vec<f64> = (0..n).map(|i| i as f64 * 1.0).collect();
        // feature: close[t-2] momentum, roughly aligned with the trend.
        let feats: Vec<Vec<f64>> = (0..n)
            .map(|i| {
                let m2 = if i >= 2 { closes[i] - closes[i - 2] } else { 0.0 };
                vec![m2]
            })
            .collect();
        let batch = compute(times, closes, feats, horizon, 5).unwrap();
        let cols = batch.columns();
        let pu = as_primitive_array::<Float64Type>(cols[2].as_ref()).value(0);
        let pd = as_primitive_array::<Float64Type>(cols[3].as_ref()).value(0);
        assert!(pu > pd, "p_up={pu} should exceed p_down={pd} on a rising series");
        assert!(pu >= 0.5);
    }
}
