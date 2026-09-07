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

use arrow::array::{as_primitive_array, ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Float64Type, Int64Type, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl};
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Expr;
use datafusion::scalar::ScalarValue;

use crate::expr_util::{expr_to_i64, expr_to_string};
use crate::hft_exec::HftRegistry;

// ---------------------------------------------------------------------------
// Label & model-selection seams (design.md D1 / D2).
// ---------------------------------------------------------------------------

/// Binary label semantics for "up over the next `horizon` bars" (design.md D1).
///
/// Up = strictly positive H-bar move: `close[t+H] > close[t]`. A *flat*
/// outcome (equal closes) is NOT up and folds into the down bucket — matching
/// the historical-analog pipeline and doc/stock_analysis.md (「平盤計入非漲」).
/// This is the single source of truth used by `compute`, `walk_compute` and
/// `regress_compute`.
///
/// The three-class extension (sideways band ±ε) is **deferred to Phase B**
/// (design.md §11 D1). When it lands, the flat-band half-width should scale
/// with the horizon (ε(H=5)≈0.5% , ε(H=10)≈1%) and its definition lives here.
#[inline]
fn is_up(close_now: f64, close_fwd: f64) -> bool {
    close_fwd > close_now
}

/// Which prediction backend scores the decision rows (design.md D2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScorerKind {
    /// Historical-analog kNN — the production scorer today.
    Analog,
    /// In-engine gradient-boosted trees — reserved for design.md M2 (not built).
    Gbdt,
}

impl ScorerKind {
    fn parse(tok: &str) -> Option<ScorerKind> {
        match tok.trim() {
            "analog" => Some(ScorerKind::Analog),
            "gbdt" => Some(ScorerKind::Gbdt),
            _ => None,
        }
    }

    /// Stable tag emitted in the additive output `model` column so every row
    /// stays attributable to its scoring backend.
    fn tag(self) -> &'static str {
        match self {
            ScorerKind::Analog => "analog",
            ScorerKind::Gbdt => "gbdt",
        }
    }
}

fn expr_is_string_literal(e: &Expr) -> bool {
    matches!(
        e,
        Expr::Literal(ScalarValue::Utf8(Some(_)) | ScalarValue::LargeUtf8(Some(_)), _)
    )
}

/// Split an optional trailing `model` token (`'analog'` / `'gbdt'`) off the
/// table-function arguments. The token must be the **last** positional arg and
/// a bare string from the known set; anything else (a feats CSV, a number, …)
/// leaves the list untouched, so trailing features keep their meaning.
/// Returns `(model, args_to_keep)`.
fn split_model_token(exprs: &[Expr]) -> (ScorerKind, usize) {
    if let Some(last) = exprs.last() {
        if expr_is_string_literal(last) {
            if let Ok(s) = expr_to_string(last) {
                if let Some(k) = ScorerKind::parse(&s) {
                    return (k, exprs.len() - 1);
                }
            }
        }
    }
    (ScorerKind::Analog, exprs.len())
}

/// Scorer availability gate: only backends that exist today may be selected.
fn ensure_available(model: ScorerKind) -> Result<()> {
    match model {
        ScorerKind::Analog => Ok(()),
        ScorerKind::Gbdt => Err(DataFusionError::Execution(
            "model='gbdt' is reserved for design.md M2 (in-engine GBDT not built yet); \
             use model='analog'"
                .into(),
        )),
    }
}

/// Append the additive `model` tag column (Utf8) to an engine output batch.
/// Schema extension is backwards-compatible: consumers address columns by name.
fn append_model_col(batch: RecordBatch, model: ScorerKind) -> Result<RecordBatch> {
    let rows = batch.num_rows();
    let mut fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(Field::new("model", DataType::Utf8, false));
    let mut cols: Vec<ArrayRef> = batch.columns().to_vec();
    cols.push(Arc::new(StringArray::from(vec![model.tag(); rows])) as ArrayRef);
    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)
        .map_err(|e| DataFusionError::Execution(format!("append model column: {e}")))
}

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
    let labels: Vec<bool> = (0..m).map(|i| is_up(closes[i], closes[i + horizon])).collect();
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

fn lit_f64(e: &Expr) -> Option<f64> {
    if let Expr::Literal(sv, _) = e {
        match sv {
            ScalarValue::Float64(Some(v)) => Some(*v),
            ScalarValue::Int64(Some(v)) => Some(*v as f64),
            ScalarValue::Utf8(Some(s)) => s.parse::<f64>().ok(),
            _ => None,
        }
    } else {
        None
    }
}

fn walk_output_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("t", DataType::Int64, false),
        Field::new("close", DataType::Float64, false),
        Field::new("p_up", DataType::Float64, false),
        Field::new("p_down", DataType::Float64, false),
        Field::new("up", DataType::Int64, false), // realised: close[t+H] > close[t]
        Field::new("n_analogs", DataType::Int64, false),
        // --- diagnostics columns ---
        Field::new("actual_ret", DataType::Float64, false), // realised H-bar return
        Field::new("q05", DataType::Float64, false), // neighbour forward H-ret quantiles
        Field::new("q10", DataType::Float64, false),
        Field::new("q25", DataType::Float64, false),
        Field::new("q50", DataType::Float64, false),
        Field::new("q75", DataType::Float64, false),
        Field::new("q90", DataType::Float64, false),
        Field::new("q95", DataType::Float64, false),
        Field::new("bar_ret", DataType::Float64, false), // decision-bar daily return
        Field::new("vol20", DataType::Float64, false),   // trailing-20d vol at decision day
        // calibrated probabilities (NaN in the fitting window); empty unless
        // fwd_walk is called with a calib fraction (e.g. ,30 -> 30%)
        Field::new("cal_p_up_iso", DataType::Float64, false),
        Field::new("cal_p_up_platt", DataType::Float64, false),
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

/// Trailing `win`-day realised volatility (std of daily close-to-close
/// returns ending at `end`, inclusive), 0.0 when too few samples.
fn trailing_vol(closes: &[f64], end: usize, win: usize) -> f64 {
    let lo = end.saturating_sub(win).max(1);
    if end < 1 || end - lo + 1 < 3 {
        return 0.0;
    }
    let mut sum = 0.0;
    let mut ss = 0.0;
    let mut cnt = 0usize;
    for k in lo..=end {
        if k >= 1 {
            let r = closes[k] / closes[k - 1] - 1.0;
            sum += r;
            ss += r * r;
            cnt += 1;
        }
    }
    if cnt < 2 {
        return 0.0;
    }
    let mean = sum / cnt as f64;
    (ss / cnt as f64 - mean * mean).max(0.0).sqrt()
}

// ---------------------------------------------------------------------------
// Probability calibration (isotonic PAV + Platt), used by fwd_walk
// ---------------------------------------------------------------------------

/// Isotonic regression (pool-adjacent-violators) over (score, label) pairs.
/// Returns a right-continuous step function: `apply_iso(q, &xs, &ys)`.
fn isotonic_fit(scores: &[f64], labels: &[f64]) -> (Vec<f64>, Vec<f64>) {
    let mut pairs: Vec<(f64, f64)> = scores.iter().copied().zip(labels.iter().copied()).collect();
    pairs.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    // stack of (run_sum, run_count, run_end_score)
    let mut stack: Vec<(f64, f64, f64)> = Vec::new();
    for (x, y) in pairs {
        stack.push((y, 1.0, x));
        while stack.len() >= 2 {
            let n = stack.len();
            let (s2, c2, _) = stack[n - 1];
            let (s1, c1, _) = stack[n - 2];
            if s2 / c2 >= s1 / c1 - 1e-12 {
                break;
            }
            let (sn, cn) = (s1 + s2, c1 + c2);
            let x_end = stack.pop().unwrap().2;
            stack.pop();
            stack.push((sn, cn, x_end));
        }
    }
    // build step edges: for each block, its mean applies for x in (prev_edge, block.last]
    let mut edges: Vec<(f64, f64)> = Vec::new();
    for (s, c, x_end) in &stack {
        edges.push((*x_end, s / c));
    }
    let xs: Vec<f64> = edges.iter().map(|(x, _)| *x).collect();
    let ys: Vec<f64> = edges.iter().map(|(_, y)| *y).collect();
    (xs, ys)
}

/// Query the isotonic step function at `q` (clamped outside its range).
fn iso_apply(q: f64, xs: &[f64], ys: &[f64]) -> f64 {
    if xs.is_empty() {
        return q;
    }
    if q <= xs[0] {
        return ys[0];
    }
    if q >= xs[xs.len() - 1] {
        return *ys.last().unwrap();
    }
    // first edge whose x >= q -> that block's mean
    match xs.iter().position(|&x| x >= q) {
        Some(i) => ys[i],
        None => *ys.last().unwrap(),
    }
}

/// Platt scaling: p' = sigmoid(a * f + b), fit by Newton steps on log-loss.
fn platt_fit(scores: &[f64], labels: &[f64]) -> (f64, f64) {
    // label smoothing (Lin, Wu 2007) avoids over-confident extremes
    let (npos, nneg) = (
        labels.iter().filter(|&&y| y > 0.0).count() as f64,
        labels.iter().filter(|&&y| y <= 0.0).count() as f64,
    );
    let (targets, x): (Vec<f64>, Vec<f64>) = labels
        .iter()
        .zip(scores.iter())
        .map(|(&y, &f)| {
            let t = if y > 0.0 { (npos + 1.0) / (npos + 2.0) } else { 1.0 / (nneg + 2.0) };
            (t, f)
        })
        .unzip();
    let (mut a, mut b) = (1.0f64, 0.0f64);
    for _ in 0..100 {
        let (mut g0, mut g1, mut h00, mut h01, mut h11) = (0.0, 0.0, 0.0, 0.0, 0.0);
        for (f, t) in x.iter().zip(targets.iter()) {
            let p = 1.0 / (1.0 + (-(a * f + b)).exp());
            let e = p - t; // gradient of log-loss in (a,b)
            g0 += e * f;
            g1 += e;
            let w = p * (1.0 - p);
            h00 += w * f * f;
            h01 += w * f;
            h11 += w;
        }
        let det = h00 * h11 - h01 * h01;
        if det.abs() < 1e-12 {
            break;
        }
        let (da, db) = ((g0 * h11 - g1 * h01) / det, (g1 * h00 - g0 * h01) / det);
        a -= da;
        b -= db;
        if da * da + db * db < 1e-10 {
            break;
        }
    }
    (a, b)
}

fn platt_apply(q: f64, a: f64, b: f64) -> f64 {
    1.0 / (1.0 + (-(a * q + b)).exp())
}

/// Strictly-causal walk-forward rows: `(t, close, p_up, p_down, up, n_analogs,
/// actual_ret, q05..q95, bar_ret, vol20)`.
///
/// Decision bar `i` is scored using **only** labelled rows `j` with
/// `j + H <= i` — i.e. neighbours whose outcome was already observable at the
/// decision close (no look-ahead). Feature z-statistics likewise come from
/// that same past-only labelled set. `q*` are quantiles of those neighbours'
/// realised H-bar forward returns; `actual_ret` is the realised H-bar return
/// of the decision bar itself; `bar_ret`/`vol20` support regime diagnostics.
fn walk_compute(
    times: Vec<i64>,
    closes: Vec<f64>,
    feats: Vec<Vec<f64>>,
    horizon: usize,
    k: usize,
    warmup: usize,
    cal_frac: f64,
) -> Result<RecordBatch> {
    let n = closes.len();
    if n < horizon + 2 {
        return Err(DataFusionError::Execution(format!(
            "fwd_walk: need > {horizon}+1 bars (got {n})"
        )));
    }
    let m = n - horizon; // decision/outcome rows [0, m): bar i has outcome close[i+H]
    let k = k.max(1);

    // Warmup adapts to the available history so short series still evaluate
    // (leave >= 20 decision rows); explicit large values are clamped.
    let max_warm = (m as i64 - horizon as i64 - 19).max(1) as usize; // keep >= 20 eval rows
    let warm = warmup.min(max_warm).max(1);
    let start = horizon.saturating_add(warm).saturating_sub(1).max(horizon + 1);
    if start >= m {
        return Err(DataFusionError::Execution(format!(
            "fwd_walk: history too short (n={n}, horizon={horizon}) for warmup {warmup} (need ~{} bars+)",
            horizon * 2 + 40
        )));
    }

    let mut t = Vec::new();
    let mut close = Vec::new();
    let mut p_up = Vec::new();
    let mut p_down = Vec::new();
    let mut up = Vec::new();
    let mut n_analogs = Vec::new();
    let mut actual_ret = Vec::new();
    let mut q_cols = vec![Vec::new(); 7];
    let mut bar_ret = Vec::new();
    let mut vol20 = Vec::new();
    let mut idx = Vec::new();

    for i in start..m {
        idx.push(i as i64);
        let lab_n = i - horizon + 1; // labelled training rows j in [0, lab_n), j+H <= i
        let (mean, std) = z_stats(&feats[..lab_n]);
        let scaled = z_apply(&feats[..lab_n], &mean, &std);
        let q = {
            let mut v = Vec::with_capacity(feats[i].len());
            for (x, (mu, s)) in feats[i].iter().zip(mean.iter().zip(std.iter())) {
                v.push((x - mu) / s);
            }
            v
        };
        let mut dists: Vec<(f64, usize)> = (0..lab_n)
            .map(|j| (sq_dist(&scaled[j], &q), j))
            .collect();
        dists.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        let used = dists.len().min(k);
        let chosen = &dists[..used];

        let mut hrets: Vec<f64> = chosen
            .iter()
            .map(|(_, j)| {
                let base = closes[*j];
                if base > 0.0 { closes[j + horizon] / base - 1.0 } else { f64::NAN }
            })
            .collect();
        hrets.retain(|x| x.is_finite());
        hrets.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let ups = chosen
            .iter()
            .filter(|(_, j)| is_up(closes[*j], closes[j + horizon]))
            .count();
        let pu = ups as f64 / used as f64;
        let pd = (used - ups) as f64 / used as f64;
        let ps = [0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95];

        let act = closes[i + horizon] / closes[i] - 1.0;
        t.push(times[i]);
        close.push(closes[i]);
        p_up.push(pu);
        p_down.push(pd);
        up.push(if is_up(closes[i], closes[i + horizon]) { 1 } else { 0 });
        n_analogs.push(used as i64);
        actual_ret.push(act);
        for (col, pr) in q_cols.iter_mut().zip(ps.iter()) {
            col.push(if hrets.is_empty() { f64::NAN } else { pct(&hrets, *pr) });
        }
        bar_ret.push(if i >= 1 { closes[i] / closes[i - 1] - 1.0 } else { 0.0 });
        vol20.push(trailing_vol(&closes, i, 20));
    }

    // --- causal probability calibration ---
    // Fit on the earliest cal_frac of walk decisions (outcomes already
    // resolved by the time later decisions fire) and apply only to decisions
    // whose bar index is past the last training outcome (gap = horizon).
    let eval_len = up.len();
    let mut cal_iso = vec![f64::NAN; eval_len];
    let mut cal_platt = vec![f64::NAN; eval_len];
    if cal_frac > 0.0 && eval_len >= 60 {
        let train_n = (((eval_len as f64) * cal_frac.clamp(0.1, 0.5)) as usize).max(20);
        let train_n = train_n.min(eval_len.saturating_sub(30));
        if train_n >= 20 {
            let labelsf: Vec<f64> = up[..train_n].iter().map(|&u| if u > 0 { 1.0 } else { 0.0 }).collect();
            let (iso_x, iso_y) = isotonic_fit(&p_up[..train_n], &labelsf);
            let (pa, pb) = platt_fit(&p_up[..train_n], &labelsf);
            let last_train_idx = idx[train_n - 1];
            for r in train_n..eval_len {
                if idx[r] >= last_train_idx + horizon as i64 {
                    cal_iso[r] = iso_apply(p_up[r], &iso_x, &iso_y);
                    cal_platt[r] = platt_apply(p_up[r], pa, pb);
                }
            }
        }
    }

    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(t)),
        Arc::new(Float64Array::from(close)),
        Arc::new(Float64Array::from(p_up)),
        Arc::new(Float64Array::from(p_down)),
        Arc::new(Int64Array::from(up)),
        Arc::new(Int64Array::from(n_analogs)),
        Arc::new(Float64Array::from(actual_ret)),
        Arc::new(Float64Array::from(std::mem::take(&mut q_cols[0]))),
        Arc::new(Float64Array::from(std::mem::take(&mut q_cols[1]))),
        Arc::new(Float64Array::from(std::mem::take(&mut q_cols[2]))),
        Arc::new(Float64Array::from(std::mem::take(&mut q_cols[3]))),
        Arc::new(Float64Array::from(std::mem::take(&mut q_cols[4]))),
        Arc::new(Float64Array::from(std::mem::take(&mut q_cols[5]))),
        Arc::new(Float64Array::from(std::mem::take(&mut q_cols[6]))),
        Arc::new(Float64Array::from(bar_ret)),
        Arc::new(Float64Array::from(vol20)),
        Arc::new(Float64Array::from(cal_iso)),
        Arc::new(Float64Array::from(cal_platt)),
    ];
    RecordBatch::try_new(walk_output_schema(), cols)
        .map_err(|e| DataFusionError::Execution(format!("fwd_walk output: {e}")))
}

/// `fwd_walk(name, horizon, k [, warmup] [, cal_frac] [, feats] [, 'model'])` table function.
///
/// Optional trailing `model` token (design.md D2): `'analog'` (default) or
/// `'gbdt'` (reserved — in-engine GBDT lands in design.md M2; selecting it
/// today is a clear error). The token must be the last positional argument.
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
        let (model, keep) = split_model_token(exprs);
        ensure_available(model)?;
        let exprs = &exprs[..keep];
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
        // Optional positional args:
        //   arg3: numeric in (0,1) -> calibration fraction (warmup auto);
        //         numeric >= 1 -> warmup (default ~k*10); string -> feats CSV
        //   arg4: numeric in (0,1) -> calibration fraction (overrides arg3)
        //         string (if arg3 was numeric) -> feats CSV
        //   arg5: feats CSV when arg4 was the calibration fraction
        //   trailing: bare string token in {analog, gbdt} -> model (default
        //     analog; stripped before the parsing above — see split_model_token)
        let warm_default = (k.max(2) * 10).max(20);
        let mut warmup = warm_default;
        let mut cal_frac = 0.0f64;
        let mut feats_expr: Option<&Expr> = None;
        if let Some(e) = exprs.get(3) {
            if let Some(v) = lit_f64(e) {
                if (0.0..1.0).contains(&v) {
                    cal_frac = v.clamp(0.05, 0.9);
                } else {
                    warmup = (v as i64).max(1) as usize;
                }
            } else {
                feats_expr = Some(e);
            }
        }
        if let Some(e) = exprs.get(4) {
            if let Some(v) = lit_f64(e) {
                if (0.0..1.0).contains(&v) {
                    cal_frac = v.clamp(0.05, 0.9);
                } else {
                    warmup = (v as i64).max(1) as usize;
                }
            } else if feats_expr.is_none() {
                feats_expr = Some(e);
            }
        }
        if feats_expr.is_none() {
            feats_expr = exprs.get(5);
        }
        let feats = match feats_expr {
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

        let out = append_model_col(
            walk_compute(times, closes, feats, horizon, k, warmup, cal_frac)?,
            model,
        )?;
        Ok(Arc::new(MemTable::try_new(out.schema(), vec![vec![out]])?))
    }
}

// ---------------------------------------------------------------------------
// fwd_regress(name, asof_ns, horizon, k [, feats]) — as-of date regression test
// ---------------------------------------------------------------------------

const DAY_NS: i64 = 86_400_000_000_000;

fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let idx = ((p * (sorted.len() - 1) as f64).round() as usize).min(sorted.len() - 1);
    sorted[idx]
}

fn regress_output_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("t", DataType::Int64, false),            // decision bar ts
        Field::new("close", DataType::Float64, false),      // decision bar close
        Field::new("p_up", DataType::Float64, false),
        Field::new("p_down", DataType::Float64, false),
        Field::new("n_analogs", DataType::Int64, false),
        // predicted forward range (ratios: +0.05 == +5%) over next H bars
        Field::new("pred_lo", DataType::Float64, false),
        Field::new("pred_hi", DataType::Float64, false),
        Field::new("pred_ret", DataType::Float64, false),
        // realised path after the decision bar
        Field::new("act_lo", DataType::Float64, false),
        Field::new("act_hi", DataType::Float64, false),
        Field::new("act_ret", DataType::Float64, false),
        Field::new("up", DataType::Int64, false),           // 1 up / 0 down / -1 future missing
        Field::new("n_future", DataType::Int64, false),     // realised bars available (< H if partial)
    ]))
}

/// As-of regression: treat `asof` (a calendar day, ns) as the decision date.
/// Only bars on/before that day are used to score the last one; the forecast
/// (direction + range band) is then compared with the realised next-H path.
fn regress_compute(
    times: Vec<i64>,
    closes: Vec<f64>,
    feats: Vec<Vec<f64>>,
    asof_ns: i64,
    horizon: usize,
    k: usize,
) -> Result<RecordBatch> {
    let n = closes.len();
    if n < 2 {
        return Err(DataFusionError::Execution(
            "fwd_regress: table is empty".into(),
        ));
    }
    // prefix = bars within the as-of calendar day (ts < asof + 1 day, so both
    // Futu's UTC-midnight labels and Yahoo's exchange-midnight labels count).
    let cut = asof_ns.saturating_add(DAY_NS);
    let e = match times.iter().position(|&t| t >= cut) {
        Some(i) => i.saturating_sub(1),
        None => n - 1,
    };
    if times[e] >= cut {
        return Err(DataFusionError::Execution(format!(
            "fwd_regress: asof {} is before the first bar",
            asof_ns
        )));
    }
    let p = e + 1; // prefix length
    if p < horizon + 2 {
        return Err(DataFusionError::Execution(format!(
            "fwd_regress: asof leaves only {p} bars (need > {}), too early",
            horizon + 1
        )));
    }
    let m = p - horizon; // labelled rows within prefix
    let labels: Vec<bool> = (0..m).map(|i| is_up(closes[i], closes[i + horizon])).collect();
    let k = k.max(1);

    // z-normalise over the labelled prefix and score the decision bar (= last
    // bar of the prefix), mirroring fwd_proba but with no post-asof data.
    let (mean, std) = z_stats(&feats[..m]);
    let scaled = z_apply(&feats[..m], &mean, &std);
    let q: Vec<f64> = feats[p - 1]
        .iter()
        .zip(mean.iter().zip(std.iter()))
        .map(|(v, (mu, s))| (v - mu) / s)
        .collect();
    let mut dists: Vec<(f64, usize)> = (0..m)
        .map(|j| (sq_dist(&scaled[j], &q), j))
        .collect();
    dists.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    let used = dists.len().min(k);
    let ups = dists[..used].iter().filter(|(_, j)| labels[*j]).count();
    let p_up = ups as f64 / used as f64;
    let p_down = (used - ups) as f64 / used as f64;

    // Forward range forecast from the same neighbours: each analog day's
    // realised path over the following H bars, relative to its own close.
    let mut lows = Vec::with_capacity(used);
    let mut highs = Vec::with_capacity(used);
    let mut hrets = Vec::with_capacity(used);
    for (_, j) in dists[..used].iter() {
        let base = closes[*j];
        if base <= 0.0 {
            continue;
        }
        let mut lo = f64::INFINITY;
        let mut hi = f64::NEG_INFINITY;
        for h in 1..=horizon {
            let r = closes[j + h] / base - 1.0;
            lo = lo.min(r);
            hi = hi.max(r);
        }
        lows.push(lo);
        highs.push(hi);
        hrets.push(closes[j + horizon] / base - 1.0);
    }
    lows.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    highs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    hrets.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let pred_lo = if lows.is_empty() { f64::NAN } else { pct(&lows, 0.10) };
    let pred_hi = if highs.is_empty() { f64::NAN } else { pct(&highs, 0.90) };
    let pred_ret = if hrets.is_empty() {
        f64::NAN
    } else {
        hrets.iter().sum::<f64>() / hrets.len() as f64
    };

    // Realised path after the decision bar.
    let n_future = (n - 1 - e).min(horizon) as i64;
    let (act_lo, act_hi, act_ret, up) = if n_future > 0 {
        let base = closes[e];
        let mut lo = f64::INFINITY;
        let mut hi = f64::NEG_INFINITY;
        for h in 1..=(n_future as usize) {
            let r = closes[e + h] / base - 1.0;
            lo = lo.min(r);
            hi = hi.max(r);
        }
        let full = n_future as usize == horizon;
        let ret = closes[e + n_future as usize] / base - 1.0;
        let up = if full {
            if is_up(base, closes[e + n_future as usize]) { 1 } else { 0 }
        } else {
            -1
        };
        (lo, hi, ret, up)
    } else {
        (f64::NAN, f64::NAN, f64::NAN, -1)
    };

    RecordBatch::try_new(
        regress_output_schema(),
        vec![
            Arc::new(Int64Array::from(vec![times[e]])) as ArrayRef,
            Arc::new(Float64Array::from(vec![closes[e]])) as ArrayRef,
            Arc::new(Float64Array::from(vec![p_up])) as ArrayRef,
            Arc::new(Float64Array::from(vec![p_down])) as ArrayRef,
            Arc::new(Int64Array::from(vec![used as i64])) as ArrayRef,
            Arc::new(Float64Array::from(vec![pred_lo])) as ArrayRef,
            Arc::new(Float64Array::from(vec![pred_hi])) as ArrayRef,
            Arc::new(Float64Array::from(vec![pred_ret])) as ArrayRef,
            Arc::new(Float64Array::from(vec![act_lo])) as ArrayRef,
            Arc::new(Float64Array::from(vec![act_hi])) as ArrayRef,
            Arc::new(Float64Array::from(vec![act_ret])) as ArrayRef,
            Arc::new(Int64Array::from(vec![up])) as ArrayRef,
            Arc::new(Int64Array::from(vec![n_future])) as ArrayRef,
        ],
    )
    .map_err(|e| DataFusionError::Execution(format!("fwd_regress output: {e}")))
}

/// `fwd_regress(name, asof_ns, horizon, k [, feats] [, 'model'])` table function — as-of
/// regression test (one decision bar per call). `asof_ns` is an epoch-ns
/// calendar-day anchor; bars on/before that day are the training prefix.
/// Optional trailing `model` token as in `fwd_walk` (design.md D2).
#[derive(Debug)]
pub struct FwdRegressTableFunction {
    registry: Arc<RwLock<HftRegistry>>,
}

impl FwdRegressTableFunction {
    pub fn new(registry: Arc<RwLock<HftRegistry>>) -> Self {
        Self { registry }
    }
}

impl TableFunctionImpl for FwdRegressTableFunction {
    fn call_with_args(
        &self,
        args: TableFunctionArgs,
    ) -> Result<Arc<dyn datafusion::datasource::TableProvider>> {
        let exprs = args.exprs();
        let (model, keep) = split_model_token(exprs);
        ensure_available(model)?;
        let exprs = &exprs[..keep];
        let name = expr_to_string(exprs.first().ok_or_else(|| {
            DataFusionError::Execution(
                "fwd_regress(name, asof_ns, horizon, k [, feats]): missing name".into(),
            )
        })?)?;
        let asof_ns = expr_to_i64(exprs.get(1).ok_or_else(|| {
            DataFusionError::Execution("fwd_regress: missing asof_ns".into())
        })?)?;
        let horizon = expr_to_i64(exprs.get(2).ok_or_else(|| {
            DataFusionError::Execution("fwd_regress: missing horizon".into())
        })?)?
        .max(1) as usize;
        let k = expr_to_i64(exprs.get(3).ok_or_else(|| {
            DataFusionError::Execution("fwd_regress: missing k".into())
        })?)?
        .max(1) as usize;

        let reg = self.registry.read().map_err(|_| {
            DataFusionError::Execution("hft registry poisoned".into())
        })?;
        let batches = reg.tables.get(&name).ok_or_else(|| {
            DataFusionError::Execution(format!("fwd_regress: unknown table `{name}`"))
        })?;
        let batches: Vec<RecordBatch> = batches.iter().cloned().collect();
        drop(reg);

        let times = pick_i64(&batches, &["t", "ts", "time", "ts_us"], "time")?;
        let closes = pick_f64(&batches, &["close", "price", "adjclose"], "close/price")?;
        if times.len() != closes.len() || times.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "fwd_regress: table `{name}` is empty or columns are ragged"
            )));
        }
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
                                "fwd_regress: column `{fn_}` length {} != rows {}",
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

        let out = append_model_col(regress_compute(times, closes, feats, asof_ns, horizon, k)?, model)?;
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
        let batch = walk_compute(times, closes, feats, horizon, 5, 30, 0.0).unwrap();
        let cols = batch.columns();
        let pu = as_primitive_array::<Float64Type>(cols[2].as_ref());
        let up = as_primitive_array::<Int64Type>(cols[4].as_ref());
        // rows: m - (H + warm - 1) with m = n - H
        let expected = 120usize - 2 * horizon - 29; // 120-3-(3+30-1)
        assert_eq!(up.len(), expected, "eval row count");
        for i in 0..up.len() {
            assert_eq!(pu.value(i), 1.0, "row {i}: p_up must be 1 on pure uptrend");
            assert_eq!(up.value(i), 1);
            let ar = as_primitive_array::<Float64Type>(cols[6].as_ref()).value(i);
            assert!(ar > 0.0, "actual_ret positive");
            let q50 = as_primitive_array::<Float64Type>(cols[10].as_ref()).value(i);
            assert!(q50 > 0.0, "neighbour median fwd-ret positive");
        }
    }


    #[test]
    fn regress_rising_series_asof_matches_actual() {
        // Strictly rising closes, as-of in the middle: forecast must be up
        // (p_up == 1) and the realised path must sit inside the band.
        let n = 120;
        let horizon = 3usize;
        let base = 1_700_000_000_000_000_000i64;
        let times: Vec<i64> = (0..n as i64).map(|i| base + i * DAY_NS).collect();
        let closes: Vec<f64> = (0..n).map(|i| 100.0 + i as f64).collect();
        let feats = auto_features(&closes);
        // asof = bar 90 -> 90 bars in prefix, next 3 bars available as future.
        let batch = regress_compute(times, closes, feats, base + 90 * DAY_NS, horizon, 5).unwrap();
        let cols = batch.columns();
        let f = |i: usize| as_primitive_array::<Float64Type>(cols[i].as_ref()).value(0);
        let u = as_primitive_array::<Int64Type>(cols[11].as_ref()).value(0);
        let nf = as_primitive_array::<Int64Type>(cols[12].as_ref()).value(0);
        assert_eq!(u, 1, "pure uptrend must realise up");
        assert_eq!(nf, 3);
        assert_eq!(f(2), 1.0); // p_up == 1
        // cols: 5 pred_lo, 6 pred_hi, 7 pred_ret, 8 act_lo, 9 act_hi, 10 act_ret
        assert!(f(7) > 0.0, "pred_ret positive on uptrend");
        assert!(f(5) > 0.0 && f(6) > 0.0 && f(8) > 0.0 && f(9) > 0.0);
        // Analog forecasts lag a deterministic drift by a couple of ticks
        // (neighbour baselines sit a few bars earlier); allow small tolerance.
        let tol = 0.02;
        assert!(f(8) >= f(5) - tol && f(9) <= f(6) + tol, "actual ~inside band");
        assert!((f(10) - f(7)).abs() < 1e-3, "predicted H-ret ~ actual on pure drift");
    }

    #[test]
    fn regress_without_future_reports_up_minus_one() {
        let n = 12;
        let horizon = 10usize;
        let base = 1_700_000_000_000_000_000i64;
        let times: Vec<i64> = (0..n as i64).map(|i| base + i * DAY_NS).collect();
        let closes: Vec<f64> = (0..n).map(|i| 100.0 + i as f64).collect();
        let feats = auto_features(&closes);
        let batch = regress_compute(times, closes, feats, base + 11 * DAY_NS, horizon, 5).unwrap(); // no future bars
        let cols = batch.columns();
        let u = as_primitive_array::<Int64Type>(cols[11].as_ref()).value(0);
        let nf = as_primitive_array::<Int64Type>(cols[12].as_ref()).value(0);
        assert_eq!(u, -1);
        assert_eq!(nf, 0);
    }

    #[test]
    fn too_few_bars_is_an_error() {
        let err = compute(vec![0, 1], vec![1.0, 2.0], vec![vec![0.0], vec![0.1]], 5, 3)
            .unwrap_err();
        assert!(err.to_string().contains("need > 5+1 bars"), "{err}");
    }

    #[test]
    fn calibration_is_monotone_and_in_range() {
        // isotonic & platt both map scores monotonically into [0,1]
        let n = 200usize;
        let scores: Vec<f64> = (0..n).map(|i| i as f64 / (n - 1) as f64).collect();
        // labels: increasing truth with noise -> reasonable curve
        let labels: Vec<f64> = scores.iter().map(|&s| (s * 0.6 + 0.15)).collect();
        let (xs, ys) = isotonic_fit(&scores, &labels);
        let mut prev = -1.0f64;
        for &q in &scores {
            let v = iso_apply(q, &xs, &ys);
            assert!((0.0..=1.0).contains(&v), "iso out of range {v}");
            assert!(v + 1e-9 >= prev, "iso must be non-decreasing: {prev}->{v}");
            prev = v;
        }
        let (a, b) = platt_fit(&scores, &labels);
        prev = -1.0f64;
        for &q in &scores {
            let v = platt_apply(q, a, b);
            assert!(v > 0.0 && v < 1.0, "platt out of (0,1): {v}");
            assert!(v + 1e-9 >= prev, "platt must be non-decreasing");
            prev = v;
        }
    }

    #[test]
    fn walk_cal_columns_fill_only_causal_region() {
        // A gentle trending series with wobble so p_up isn't degenerate.
        let n = 500usize;
        let base = 1_700_000_000_000_000_000i64;
        let times: Vec<i64> = (0..n as i64).map(|i| base + i * DAY_NS).collect();
        let closes: Vec<f64> = (0..n)
            .map(|i| {
                let t = i as f64;
                100.0 + t * 0.08 + 4.0 * (t / 23.0).sin()
            })
            .collect();
        let feats = auto_features(&closes);
        let horizon = 5usize;
        let batch = walk_compute(times, closes, feats, horizon, 10, 40, 0.4).unwrap();
        let cols = batch.columns();
        assert_eq!(cols.len(), 18);
        let iso = as_primitive_array::<Float64Type>(cols[16].as_ref());
        let platt = as_primitive_array::<Float64Type>(cols[17].as_ref());
        let mut finite = 0usize;
        for i in 0..iso.len() {
            let iv = iso.value(i);
            let pv = platt.value(i);
            if iv.is_finite() {
                assert!(pv.is_finite(), "iso finite but platt NaN at {i}");
                assert!((0.0..=1.0).contains(&iv) && (0.0..=1.0).contains(&pv));
                finite += 1;
            }
        }
        assert!(finite > 0, "calibrated region must be populated");
        assert!(finite < iso.len() as usize, "fitting window must stay NaN");
    }

    #[test]
    fn flat_close_counts_as_not_up() {
        // D1: label semantics — a flat H-bar move (equal closes) is *not* up,
        // so an all-flat series has p_up == 0 (flat folds into the down
        // bucket, matching stock_analysis.md 「平盤計入非漲」).
        let n = 40usize;
        let horizon = 3usize;
        let times: Vec<i64> = (0..n as i64).collect();
        let closes: Vec<f64> = vec![100.0; n];
        let feats = vec![vec![0.0f64]; n]; // constant feature -> no NaN warm-up
        let batch = compute(times, closes, feats, horizon, 5).unwrap();
        let cols = batch.columns();
        let pu = as_primitive_array::<Float64Type>(cols[2].as_ref()).value(0);
        let pd = as_primitive_array::<Float64Type>(cols[3].as_ref()).value(0);
        assert_eq!(pu, 0.0, "flat labels must never count as up");
        assert_eq!(pd, 1.0);
        assert!(!is_up(100.0, 100.0), "flat == not up");
        assert!(is_up(100.0, 100.0001));
    }

    #[test]
    fn trailing_model_token_and_availability() {
        // D2: optional trailing 'model' token on fwd_walk / fwd_regress.
        use datafusion::logical_expr::Expr as E;
        use datafusion::scalar::ScalarValue as SV;
        let strl = |s: &str| E::Literal(SV::Utf8(Some(s.to_string())), None);
        let intl = |i: i64| E::Literal(SV::Int64(Some(i)), None);
        // explicit 'analog' -> trimmed, default model
        let e = vec![strl("tbl"), intl(10), intl(20), strl("analog")];
        let (m, keep) = split_model_token(&e);
        assert_eq!(m, ScorerKind::Analog);
        assert_eq!(keep, 3);
        // no token -> default analog, nothing trimmed
        let (m2, keep2) = split_model_token(&[strl("tbl"), intl(10), intl(20)]);
        assert_eq!((m2, keep2), (ScorerKind::Analog, 3));
        // warmup + trailing 'gbdt' -> token consumed, warmup kept
        let e3 = vec![strl("tbl"), intl(10), intl(20), intl(1), strl("gbdt")];
        let (m3, keep3) = split_model_token(&e3);
        assert_eq!((m3, keep3), (ScorerKind::Gbdt, 4));
        // unknown bare string stays a feats CSV, not a model token
        let e4 = vec![strl("tbl"), intl(10), intl(20), strl("mom5,mom10")];
        let (m4, keep4) = split_model_token(&e4);
        assert_eq!((m4, keep4), (ScorerKind::Analog, 4));
        // gbdt is reserved until design.md M2
        assert!(ensure_available(ScorerKind::Analog).is_ok());
        let err = ensure_available(ScorerKind::Gbdt).unwrap_err().to_string();
        assert!(err.contains("M2"), "{err}");
    }

    #[test]
    fn walk_output_is_tagged_with_model() {
        // D2: engine output carries an additive `model` column (analog tag).
        let n = 120usize;
        let horizon = 3usize;
        let times: Vec<i64> = (0..n as i64).collect();
        let closes: Vec<f64> = (0..n).map(|i| 100.0 + i as f64).collect();
        let feats = auto_features(&closes);
        let batch = walk_compute(times, closes, feats, horizon, 5, 30, 0.0).unwrap();
        let tagged = append_model_col(batch, ScorerKind::Analog).unwrap();
        assert_eq!(tagged.num_columns(), 19, "18 walk cols + model tag");
        let any = arrow::array::Array::as_any(tagged.column(18).as_ref());
        let model_col = any
            .downcast_ref::<arrow::array::StringArray>()
            .expect("model column is Utf8");
        for i in 0..tagged.num_rows() {
            assert_eq!(model_col.value(i), "analog");
        }
    }
}
