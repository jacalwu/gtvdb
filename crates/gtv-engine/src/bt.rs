//! Single-asset backtest table functions (function2.md Phase A §1.4, phase 1).
//!
//! ```sql
//! SELECT * FROM backtest('sig_tbl'[, cost_bps[, stop_loss[, take_profit]][, 'SYM']]);
//! SELECT * FROM bt_report('sig_tbl'[, cost_bps[, stop_loss[, take_profit]][, 'SYM']]);
//! ```
//! The registered table needs a ts column (ts_us/ts/t/time), a price column
//! (close/price/adjclose) and a signal column (`signal`/`sig`/`pos`) holding
//! -1/0/1 numbers (or 'buy'/'sell'/'hold' strings). Optional trailing `'SYM'`
//! filters to one symbol. Position state machine + stop/take-profit/cost live
//! in `gtv_array::backtest`.

use std::fmt::Debug;
use std::sync::{Arc, RwLock};

use arrow::array::{as_primitive_array, as_string_array, Array, ArrayRef, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Float64Type, Int64Type, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl};
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Expr;
use datafusion::scalar::ScalarValue;

use crate::expr_util::expr_to_string;
use crate::hft_exec::HftRegistry;
use gtv_array::backtest::BtParams;

fn expr_to_f64(e: &Expr) -> Result<f64> {
    match e {
        Expr::Literal(sv, _) => match sv {
            ScalarValue::Float64(Some(v)) => Ok(*v),
            ScalarValue::Int64(Some(v)) => Ok(*v as f64),
            ScalarValue::Utf8(Some(s)) => s
                .parse::<f64>()
                .map_err(|_| DataFusionError::Execution(format!("expected number, got {s}"))),
            other => Err(DataFusionError::Execution(format!(
                "expected a numeric literal, got {other:?}"
            ))),
        },
        _ => Err(DataFusionError::Execution(
            "backtest arguments must be literals".into(),
        )),
    }
}

fn f64_values(array: &ArrayRef) -> &[f64] {
    as_primitive_array::<Float64Type>(array.as_ref()).values().as_ref()
}

fn col_i64(batches: &[RecordBatch], name: &str) -> Option<Vec<i64>> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b.column_by_name(name)?;
        if !matches!(arr.data_type(), DataType::Int64) {
            return None;
        }
        out.extend_from_slice(as_primitive_array::<Int64Type>(arr.as_ref()).values());
    }
    Some(out)
}

fn col_f64(batches: &[RecordBatch], name: &str) -> Option<Vec<f64>> {
    let arr = batches.first()?.column_by_name(name)?;
    if !matches!(arr.data_type(), DataType::Float64) {
        return None;
    }
    let mut out = Vec::new();
    for b in batches {
        out.extend_from_slice(f64_values(b.column_by_name(name).unwrap()));
    }
    Some(out)
}

/// Parse the signal column: numeric (>0 long / <0 short / 0 flat) or
/// string ('buy'/'sell'/'hold').
fn parse_signal(batches: &[RecordBatch], sym_filter: Option<&str>) -> Result<Vec<i8>> {
    let names = ["signal", "sig", "pos"];
    let mut used_col = None;
    for n in names {
        if batches.first().and_then(|b| b.column_by_name(n)).is_some() {
            used_col = Some(n);
            break;
        }
    }
    let col = used_col.ok_or_else(|| {
        DataFusionError::Execution("backtest: table has no signal/sig/pos column".into())
    })?;
    let mut sig = Vec::new();
    for b in batches {
        let a = b.column_by_name(col).unwrap();
        match a.data_type() {
            DataType::Float64 => {
                for v in f64_values(a) {
                    sig.push(if *v > 0.0 { 1 } else if *v < 0.0 { -1 } else { 0 });
                }
            }
            DataType::Int64 | DataType::Int32 => {
                for i in 0..a.len() {
                    let v = as_primitive_array::<Int64Type>(a).value(i);
                    sig.push(if v > 0 { 1 } else if v < 0 { -1 } else { 0 });
                }
            }
            DataType::Utf8 => {
                let sa = as_string_array(a);
                for i in 0..sa.len() {
                    sig.push(match sa.value(i).to_ascii_lowercase().as_str() {
                        "buy" | "long" | "1" => 1,
                        "sell" | "short" | "-1" => -1,
                        _ => 0,
                    });
                }
            }
            other => {
                return Err(DataFusionError::Execution(format!(
                    "backtest: unsupported signal type {other}"
                )))
            }
        }
    }
    if let Some(f) = sym_filter {
        // keep rows whose symbol == f (row order preserved per batch)
        let syms: Vec<String> = {
            let mut v = Vec::new();
            for b in batches {
                let a = as_string_array(b.column_by_name("symbol").ok_or_else(|| {
                    DataFusionError::Execution("backtest: 'SYM' filter but no symbol column".into())
                })?);
                for i in 0..a.len() {
                    v.push(a.value(i).to_string());
                }
            }
            v
        };
        for (i, sym) in syms.iter().enumerate() {
            if sym != f {
                sig[i] = 0; // mask rows of other symbols (keep length)
            }
        }
    }
    Ok(sig)
}

fn read_inputs(
    batches: &[RecordBatch],
    sym_filter: Option<&str>,
    table: &str,
) -> Result<(Vec<i64>, Vec<f64>, Vec<i8>)> {
    let ts = ["ts_us", "ts", "t", "time"]
        .iter()
        .find_map(|n| col_i64(batches, n))
        .ok_or_else(|| {
            DataFusionError::Execution(format!("backtest: table `{table}` has no Int64 ts col"))
        })?;
    let px = ["close", "price", "adjclose"]
        .iter()
        .find_map(|n| col_f64(batches, n))
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "backtest: table `{table}` has no close/price/adjclose Float64 col"
            ))
        })?;
    let sig = parse_signal(batches, sym_filter)?;
    if ts.len() != px.len() || ts.len() != sig.len() {
        return Err(DataFusionError::Execution(format!(
            "backtest: ragged columns in `{table}`"
        )));
    }
    // single-row index order already aligned by batches (rows are not sorted);
    // sort jointly by ts.
    let mut idx: Vec<usize> = (0..ts.len()).collect();
    idx.sort_by_key(|&i| ts[i]);
    let ts: Vec<i64> = idx.iter().map(|&i| ts[i]).collect();
    let px: Vec<f64> = idx.iter().map(|&i| px[i]).collect();
    let sig: Vec<i8> = idx.iter().map(|&i| sig[i]).collect();
    Ok((ts, px, sig))
}

fn parse_common(
    exprs: &[Expr],
    _table: &str,
) -> Result<(String, BtParams, Option<String>)> {
    let name = expr_to_string(
        exprs.first()
            .ok_or_else(|| DataFusionError::Execution(format!("backtest(name,..): missing name")))?,
    )?;
    let mut params = BtParams::default();
    let mut sym: Option<String> = None;
    for (i, e) in exprs.iter().enumerate().skip(1) {
        if matches!(e, Expr::Literal(ScalarValue::Utf8(Some(_)), _)) {
            sym = Some(expr_to_string(e)?);
        } else {
            let v = expr_to_f64(e)?.max(0.0);
            match i {
                1 => params.cost_bps = v,
                2 => params.stop_loss = v,
                3 => params.take_profit = v,
                _ => {}
            }
        }
    }
    Ok((name, params, sym))
}

#[derive(Debug)]
pub struct BacktestTableFunction {
    registry: Arc<RwLock<HftRegistry>>,
}

impl BacktestTableFunction {
    pub fn new(registry: Arc<RwLock<HftRegistry>>) -> Self {
        Self { registry }
    }
}

impl TableFunctionImpl for BacktestTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn datafusion::datasource::TableProvider>> {
        let exprs = args.exprs();
        let (name, params, sym) = parse_common(exprs, "backtest")?;
        let reg = self.registry.read().map_err(|_| {
            DataFusionError::Execution("hft registry poisoned".into())
        })?;
        let batches = reg.tables.get(&name).cloned().ok_or_else(|| {
            DataFusionError::Execution(format!("backtest: unknown table `{name}`"))
        })?;
        drop(reg);
        let (ts, px, sig) = read_inputs(&batches, sym.as_deref(), &name)?;
        let fills = gtv_array::backtest::simulate(&ts, &px, &sig, &params);

        let schema = Arc::new(Schema::new(vec![
            Field::new("entry_ts", DataType::Int64, false),
            Field::new("exit_ts", DataType::Int64, false),
            Field::new("side", DataType::Utf8, false),
            Field::new("entry_px", DataType::Float64, false),
            Field::new("exit_px", DataType::Float64, false),
            Field::new("gross_ret", DataType::Float64, false),
            Field::new("net_ret", DataType::Float64, false),
            Field::new("cost", DataType::Float64, false),
            Field::new("reason", DataType::Utf8, false),
        ]));
        let mut ets = Vec::with_capacity(fills.len());
        let mut xts = Vec::with_capacity(fills.len());
        let mut side_s = Vec::with_capacity(fills.len());
        let mut epx = Vec::with_capacity(fills.len());
        let mut xpx = Vec::with_capacity(fills.len());
        let mut gr = Vec::with_capacity(fills.len());
        let mut nr = Vec::with_capacity(fills.len());
        let mut cost = Vec::with_capacity(fills.len());
        let mut reason = Vec::with_capacity(fills.len());
        for f in &fills {
            ets.push(f.entry_ts);
            xts.push(f.exit_ts);
            side_s.push(if f.side > 0 { "long" } else { "short" });
            epx.push(f.entry_px);
            xpx.push(f.exit_px);
            gr.push(f.gross);
            nr.push(f.net);
            cost.push(f.cost);
            reason.push(f.reason.label());
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ets)) as ArrayRef,
                Arc::new(Int64Array::from(xts)) as ArrayRef,
                Arc::new(arrow::array::StringArray::from(side_s)) as ArrayRef,
                Arc::new(Float64Array::from(epx)) as ArrayRef,
                Arc::new(Float64Array::from(xpx)) as ArrayRef,
                Arc::new(Float64Array::from(gr)) as ArrayRef,
                Arc::new(Float64Array::from(nr)) as ArrayRef,
                Arc::new(Float64Array::from(cost)) as ArrayRef,
                Arc::new(arrow::array::StringArray::from(reason)) as ArrayRef,
            ],
        )?;
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}

#[derive(Debug)]
pub struct BtReportTableFunction {
    registry: Arc<RwLock<HftRegistry>>,
}

impl BtReportTableFunction {
    pub fn new(registry: Arc<RwLock<HftRegistry>>) -> Self {
        Self { registry }
    }
}

impl TableFunctionImpl for BtReportTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn datafusion::datasource::TableProvider>> {
        let exprs = args.exprs();
        let (name, params, sym) = parse_common(exprs, "bt_report")?;
        let reg = self.registry.read().map_err(|_| {
            DataFusionError::Execution("hft registry poisoned".into())
        })?;
        let batches = reg.tables.get(&name).cloned().ok_or_else(|| {
            DataFusionError::Execution(format!("bt_report: unknown table `{name}`"))
        })?;
        drop(reg);
        let (ts, px, sig) = read_inputs(&batches, sym.as_deref(), &name)?;
        let fills = gtv_array::backtest::simulate(&ts, &px, &sig, &params);
        let r = gtv_array::backtest::report(&fills);
        let schema = Arc::new(Schema::new(vec![
            Field::new("n_trades", DataType::Int64, false),
            Field::new("win_rate", DataType::Float64, false),
            Field::new("total_ret", DataType::Float64, false),
            Field::new("ann_ret", DataType::Float64, true),
            Field::new("mean_net", DataType::Float64, false),
            Field::new("sharpe", DataType::Float64, true),
            Field::new("maxdd", DataType::Float64, false),
            Field::new("cost_total", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![r.n_trades])) as ArrayRef,
                Arc::new(Float64Array::from(vec![r.win_rate])) as ArrayRef,
                Arc::new(Float64Array::from(vec![r.total_ret])) as ArrayRef,
                Arc::new(Float64Array::from(vec![r.ann_ret])) as ArrayRef,
                Arc::new(Float64Array::from(vec![r.mean_net])) as ArrayRef,
                Arc::new(Float64Array::from(vec![r.sharpe])) as ArrayRef,
                Arc::new(Float64Array::from(vec![r.maxdd])) as ArrayRef,
                Arc::new(Float64Array::from(vec![r.cost_total])) as ArrayRef,
            ],
        )?;
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}

// ---------------------------------------------------------------------------
// Portfolio backtest table functions (function2.md §1.4 phase 2)
// ---------------------------------------------------------------------------

fn collect_columns(batches: &[RecordBatch]) -> Result<(Vec<i64>, Vec<f64>, Vec<i8>, Vec<String>)> {
    let ts_name = ["ts_us", "ts", "t", "time"]
        .iter()
        .find(|n| col_i64(batches, n).is_some())
        .copied()
        .unwrap_or("ts");
    let mut ts = Vec::new();
    for b in batches {
        if let Some(v) = col_i64(std::slice::from_ref(b), ts_name) {
            ts.extend(v);
        }
    }
    let px = ["close", "price", "adjclose"]
        .iter()
        .find(|n| col_f64(batches, n).is_some())
        .copied()
        .ok_or_else(|| DataFusionError::Execution("pf: no close/price/adjclose Float64 col".into()))?;
    let mut pxv = Vec::new();
    for b in batches {
        let arr = b.column_by_name(px).unwrap();
        if matches!(arr.data_type(), DataType::Float64) {
            pxv.extend_from_slice(f64_values(arr));
        } else if matches!(arr.data_type(), DataType::Int64) {
            for i in 0..arr.len() {
                pxv.push(as_primitive_array::<Int64Type>(arr).value(i) as f64);
            }
        } else {
            return Err(DataFusionError::Execution(format!(
                "pf: column `{px}` type {:?} unsupported",
                arr.data_type()
            )));
        }
    }
    let sig = parse_signal(batches, None)?;
    let syms: Vec<String> = if batches.first().and_then(|b| b.column_by_name("symbol")).is_some() {
        let mut v = Vec::new();
        for b in batches {
            let sa = as_string_array(b.column_by_name("symbol").unwrap());
            for i in 0..sa.len() {
                v.push(sa.value(i).to_string());
            }
        }
        v
    } else {
        vec![String::new(); ts.len()]
    };
    if ts.len() != pxv.len() || ts.len() != sig.len() || ts.len() != syms.len() {
        return Err(DataFusionError::Execution(
            "pf: ragged columns across batches".into(),
        ));
    }
    Ok((ts, pxv, sig, syms))
}

fn group_series(
    batches: &[RecordBatch],
    table: &str,
) -> Result<Vec<gtv_array::backtest::PfSeries>> {
    let (ts, px, sig, syms) = collect_columns(batches)?;
    let mut groups: std::collections::BTreeMap<String, Vec<usize>> =
        std::collections::BTreeMap::new();
    for i in 0..ts.len() {
        groups.entry(syms[i].clone()).or_default().push(i);
    }
    let mut out = Vec::new();
    for (sym, idxs) in groups {
        let mut sub: Vec<(i64, f64, i8)> = idxs.iter().map(|&i| (ts[i], px[i], sig[i])).collect();
        sub.sort_by_key(|r| r.0);
        let mut s = gtv_array::backtest::PfSeries { ts: vec![], close: vec![], sig: vec![] };
        for (t, c, g) in sub {
            s.ts.push(t);
            s.close.push(c);
            s.sig.push(g);
        }
        if !s.ts.is_empty() {
            let _ = sym;
            out.push(s);
        }
    }
    if out.is_empty() {
        return Err(DataFusionError::Execution(format!(
            "pf: table `{table}` is empty"
        )));
    }
    Ok(out)
}

#[derive(Debug)]
pub struct PfBacktestTableFunction {
    registry: Arc<RwLock<HftRegistry>>,
}

impl PfBacktestTableFunction {
    pub fn new(registry: Arc<RwLock<HftRegistry>>) -> Self {
        Self { registry }
    }
}

impl TableFunctionImpl for PfBacktestTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn datafusion::datasource::TableProvider>> {
        let exprs = args.exprs();
        let name = expr_to_string(
            exprs.first()
                .ok_or_else(|| DataFusionError::Execution("pf_backtest(name[, cost_bps]): missing name".into()))?,
        )?;
        let cost_bps = exprs.get(1).map(expr_to_f64).transpose()?.unwrap_or(0.0).max(0.0);
        let reg = self.registry.read().map_err(|_| {
            DataFusionError::Execution("hft registry poisoned".into())
        })?;
        let batches = reg.tables.get(&name).cloned().ok_or_else(|| {
            DataFusionError::Execution(format!("pf_backtest: unknown table `{name}`"))
        })?;
        drop(reg);
        let series = group_series(&batches, &name)?;
        let days = gtv_array::backtest::portfolio_sim(&series, cost_bps);
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("nav", DataType::Float64, false),
            Field::new("ret", DataType::Float64, false),
            Field::new("n_active", DataType::Int64, false),
            Field::new("turnover", DataType::Float64, false),
        ]));
        let mut t = Vec::with_capacity(days.len());
        let mut nav = Vec::with_capacity(days.len());
        let mut ret = Vec::with_capacity(days.len());
        let mut na = Vec::with_capacity(days.len());
        let mut to = Vec::with_capacity(days.len());
        for d in &days {
            t.push(d.ts);
            nav.push(d.nav);
            ret.push(d.ret);
            na.push(d.n_active as i64);
            to.push(d.turnover);
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(t)) as ArrayRef,
                Arc::new(Float64Array::from(nav)) as ArrayRef,
                Arc::new(Float64Array::from(ret)) as ArrayRef,
                Arc::new(Int64Array::from(na)) as ArrayRef,
                Arc::new(Float64Array::from(to)) as ArrayRef,
            ],
        )?;
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}

#[derive(Debug)]
pub struct PfReportTableFunction {
    registry: Arc<RwLock<HftRegistry>>,
}

impl PfReportTableFunction {
    pub fn new(registry: Arc<RwLock<HftRegistry>>) -> Self {
        Self { registry }
    }
}

impl TableFunctionImpl for PfReportTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn datafusion::datasource::TableProvider>> {
        let exprs = args.exprs();
        let name = expr_to_string(
            exprs.first()
                .ok_or_else(|| DataFusionError::Execution("pf_report(name[, cost_bps]): missing name".into()))?,
        )?;
        let cost_bps = exprs.get(1).map(expr_to_f64).transpose()?.unwrap_or(0.0).max(0.0);
        let reg = self.registry.read().map_err(|_| {
            DataFusionError::Execution("hft registry poisoned".into())
        })?;
        let batches = reg.tables.get(&name).cloned().ok_or_else(|| {
            DataFusionError::Execution(format!("pf_report: unknown table `{name}`"))
        })?;
        drop(reg);
        let series = group_series(&batches, &name)?;
        let days = gtv_array::backtest::portfolio_sim(&series, cost_bps);
        let r = gtv_array::backtest::pf_report(&days);
        let schema = Arc::new(Schema::new(vec![
            Field::new("n_bars", DataType::Int64, false),
            Field::new("final_nav", DataType::Float64, false),
            Field::new("total_ret", DataType::Float64, false),
            Field::new("ann_ret", DataType::Float64, true),
            Field::new("vol_daily", DataType::Float64, false),
            Field::new("sharpe_daily", DataType::Float64, true),
            Field::new("maxdd", DataType::Float64, false),
            Field::new("turnover_total", DataType::Float64, false),
            Field::new("avg_active", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![r.n_bars])) as ArrayRef,
                Arc::new(Float64Array::from(vec![r.final_nav])) as ArrayRef,
                Arc::new(Float64Array::from(vec![r.total_ret])) as ArrayRef,
                Arc::new(Float64Array::from(vec![r.ann_ret])) as ArrayRef,
                Arc::new(Float64Array::from(vec![r.vol_daily])) as ArrayRef,
                Arc::new(Float64Array::from(vec![r.sharpe_daily])) as ArrayRef,
                Arc::new(Float64Array::from(vec![r.maxdd])) as ArrayRef,
                Arc::new(Float64Array::from(vec![r.turnover_total])) as ArrayRef,
                Arc::new(Float64Array::from(vec![r.avg_active])) as ArrayRef,
            ],
        )?;
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}

#[cfg(test)]
mod tests {
    use gtv_array::backtest::{simulate, BtParams};

    fn series(step: f64) -> Vec<f64> {
        (0..60).map(|i| 100.0 + i as f64 * step).collect()
    }
    fn ts(len: usize) -> Vec<i64> {
        (0..len as i64).map(|i| i * 86_400_000_000_000).collect()
    }

    #[test]
    fn long_with_stops_never_reenters_without_flat() {
        // churn guard: after a stop, no immediate re-entry until a flat bar.
        let px = (0..40)
            .map(|i| if i < 8 { 100.0 + i as f64 } else { 100.0 - (i - 8) as f64 * 8.0 })
            .collect::<Vec<f64>>();
        let t = ts(px.len());
        let mut sig = vec![0i8; px.len()];
        for i in 1..px.len() {
            sig[i] = 1;
        }
        let p = BtParams { cost_bps: 0.0, stop_loss: 0.1, take_profit: 0.0 };
        let fills = simulate(&t, &px, &sig, &p);
        assert!(!fills.is_empty());
        assert_eq!(fills[0].reason, gtv_array::backtest::ExitReason::StopLoss);
        // after the stop we stay flat until the end (never a flat bar) -> 1 trade
        assert_eq!(fills.len(), 1, "no re-entry churn without flat signal");
    }

    #[test]
    fn flip_and_flat_then_retrend_reenters() {
        let px = series(1.0);
        let t = ts(px.len());
        let mut sig = vec![0i8; px.len()];
        for i in 10..=19 {
            sig[i] = 1;
        }
        for i in 20..=24 {
            sig[i] = 0; // flat -> allows a clean re-entry
        }
        for i in 25..=31 {
            sig[i] = 1; // second long leg
        }
        let fills = simulate(&t, &px, &sig, &BtParams::default());
        assert_eq!(fills.len(), 2, "exit on flat, re-enter on next rising edge");
        assert!(fills[0].exit_ts < fills[1].entry_ts);
    }
}
