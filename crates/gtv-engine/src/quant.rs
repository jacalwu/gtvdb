//! Quant operators (roadmap phase 2) as DataFusion UDFs:
//! Black-Scholes price/Greeks (scalar), historical VaR / PCA / L2 reconstruction
//! (table functions over registered tables).

use std::sync::{Arc, RwLock};

use arrow::array::{as_primitive_array, as_string_array, Array, ArrayRef, Float64Array, Int32Array, Int64Array, UInt64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type, Int64Type, Schema, SchemaRef, UInt64Type};
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
use datafusion::logical_expr::{ColumnarValue, Expr, PartitionEvaluator, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility, WindowUDF, WindowUDFImpl};
use datafusion::scalar::ScalarValue;

use crate::expr_util::{expr_to_i64, expr_to_string};
use crate::hft_exec::HftRegistry;
use gtv_array::quant::MboMsg;

fn f64_values(array: &ArrayRef) -> &[f64] {
    as_primitive_array::<Float64Type>(array.as_ref()).values().as_ref()
}

/// Extract a float literal (accepting Int64/Float64/Float32).
fn expr_to_f64(expr: &Expr) -> DfResult<f64> {
    match expr {
        Expr::Literal(sv, _) => match sv {
            ScalarValue::Float64(Some(v)) => Ok(*v),
            ScalarValue::Float32(Some(v)) => Ok(*v as f64),
            ScalarValue::Int64(Some(v)) => Ok(*v as f64),
            other => Err(DataFusionError::Execution(format!(
                "expected a numeric literal, got {other:?}"
            ))),
        },
        _ => Err(DataFusionError::Execution("arguments must be literals".into())),
    }
}

// ---------------------------------------------------------------------------
// Black-Scholes scalar UDFs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Greek {
    Price,
    Delta,
    Gamma,
    Vega,
    Theta,
    Rho,
}

impl Greek {
    fn apply(self, opt: &str, s: f64, k: f64, t: f64, r: f64, v: f64) -> f64 {
        match self {
            Greek::Price => gtv_array::quant::bs_price(opt, s, k, t, r, v),
            Greek::Delta => gtv_array::quant::bs_delta(opt, s, k, t, r, v),
            Greek::Gamma => gtv_array::quant::bs_gamma(opt, s, k, t, r, v),
            Greek::Vega => gtv_array::quant::bs_vega(opt, s, k, t, r, v),
            Greek::Theta => gtv_array::quant::bs_theta(opt, s, k, t, r, v),
            Greek::Rho => gtv_array::quant::bs_rho(opt, s, k, t, r, v),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct BsUdf {
    name: &'static str,
    signature: Signature,
    greek: Greek,
}

impl BsUdf {
    fn new(name: &'static str, greek: Greek) -> Self {
        Self {
            name,
            signature: Signature::exact(
                vec![
                    DataType::Utf8,
                    DataType::Float64,
                    DataType::Float64,
                    DataType::Float64,
                    DataType::Float64,
                    DataType::Float64,
                ],
                Volatility::Immutable,
            ),
            greek,
        }
    }
}

impl ScalarUDFImpl for BsUdf {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let opt = as_string_array(&arrays[0]);
        let s = f64_values(&arrays[1]);
        let k = f64_values(&arrays[2]);
        let t = f64_values(&arrays[3]);
        let r = f64_values(&arrays[4]);
        let v = f64_values(&arrays[5]);
        let out: Float64Array = (0..s.len())
            .map(|i| self.greek.apply(opt.value(i), s[i], k[i], t[i], r[i], v[i]))
            .collect();
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

pub fn bs_udfs() -> Vec<ScalarUDF> {
    vec![
        ScalarUDF::from(BsUdf::new("bs_price", Greek::Price)),
        ScalarUDF::from(BsUdf::new("bs_delta", Greek::Delta)),
        ScalarUDF::from(BsUdf::new("bs_gamma", Greek::Gamma)),
        ScalarUDF::from(BsUdf::new("bs_vega", Greek::Vega)),
        ScalarUDF::from(BsUdf::new("bs_theta", Greek::Theta)),
        ScalarUDF::from(BsUdf::new("bs_rho", Greek::Rho)),
    ]
}

// ---------------------------------------------------------------------------
// xbar(ts, bucket) — scalar UDF (timestamp bucketing)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct XbarUdf {
    signature: Signature,
}

impl XbarUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Int64, DataType::Int64],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for XbarUdf {
    fn name(&self) -> &str {
        "xbar"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let ts = as_primitive_array::<Int64Type>(&arrays[0]).values();
        let bucket = as_primitive_array::<Int64Type>(&arrays[1]).values();
        let out: Int64Array = (0..ts.len())
            .map(|i| {
                let b = bucket.get(i).copied().unwrap_or(1).max(1);
                (ts[i] / b) * b
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

pub fn xbar_udf() -> ScalarUDF {
    ScalarUDF::from(XbarUdf::new())
}

// ---------------------------------------------------------------------------
// var_historical(name, confidence) — table function
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct VarHistoricalTableFunction {
    registry: Arc<RwLock<HftRegistry>>,
}

impl VarHistoricalTableFunction {
    pub fn new(registry: Arc<RwLock<HftRegistry>>) -> Self {
        Self { registry }
    }
}

impl TableFunctionImpl for VarHistoricalTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> DfResult<Arc<dyn TableProvider>> {
        let exprs = args.exprs();
        let name = expr_to_string(
            exprs.first().ok_or_else(|| {
                DataFusionError::Execution("var_historical(name, confidence): missing name".into())
            })?,
        )?;
        let mut confidence = expr_to_f64(
            exprs.get(1).ok_or_else(|| {
                DataFusionError::Execution(
                    "var_historical(name, confidence): missing confidence".into(),
                )
            })?,
        )?;
        if confidence > 1.0 {
            confidence /= 100.0; // accept 95 as 0.95
        }
        let reg = self.registry.read().map_err(|_| {
            DataFusionError::Execution("hft registry poisoned".into())
        })?;
        let batches = reg.tables.get(&name).ok_or_else(|| {
            DataFusionError::Execution(format!("unknown table `{name}`"))
        })?;
        let mut returns = Vec::new();
        for b in batches.as_ref() {
            let arr = b
                .column_by_name("returns")
                .or_else(|| b.column_by_name("ret"))
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "table `{name}` has no `returns`/`ret` column"
                    ))
                })?;
            returns.extend_from_slice(f64_values(arr));
        }
        let var = gtv_array::quant::var_historical(&returns, confidence);
        let schema = Arc::new(Schema::new(vec![Field::new("var", DataType::Float64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Float64Array::from(vec![var])) as ArrayRef],
        )?;
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}

// ---------------------------------------------------------------------------
// pca(name, n_components) — table function (eigenvalues + explained variance)
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct PcaTableFunction {
    registry: Arc<RwLock<HftRegistry>>,
}

impl PcaTableFunction {
    pub fn new(registry: Arc<RwLock<HftRegistry>>) -> Self {
        Self { registry }
    }
}

impl TableFunctionImpl for PcaTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> DfResult<Arc<dyn TableProvider>> {
        let exprs = args.exprs();
        let name = expr_to_string(
            exprs.first().ok_or_else(|| {
                DataFusionError::Execution("pca(name, n): missing name".into())
            })?,
        )?;
        let m = expr_to_i64(
            exprs.get(1)
                .ok_or_else(|| DataFusionError::Execution("pca(name, n): missing n".into()))?,
        )?
        .max(1) as usize;

        let reg = self.registry.read().map_err(|_| {
            DataFusionError::Execution("hft registry poisoned".into())
        })?;
        let batches = reg.tables.get(&name).ok_or_else(|| {
            DataFusionError::Execution(format!("unknown table `{name}`"))
        })?;

        // Gather m return columns into a row-major [n][m] flat matrix.
        let mut cols = Vec::with_capacity(m);
        for c in 0..m {
            let mut v = Vec::new();
            for b in batches.as_ref() {
                let arr = b.column_by_name(&format!("ret_{c}")).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "table `{name}` has no `ret_{c}` column"
                    ))
                })?;
                v.extend_from_slice(f64_values(arr));
            }
            cols.push(v);
        }
        let n = cols.first().map(|c| c.len()).unwrap_or(0);
        let mut flat = Vec::with_capacity(n * m);
        for r in 0..n {
            for c in 0..m {
                flat.push(cols[c][r]);
            }
        }
        let cov = gtv_array::hft_ops::covariance_matrix(&flat, m);
        let (vals, _vecs) = gtv_array::quant::jacobi_eigen(&cov, m, 100);
        let total: f64 = vals.iter().sum::<f64>().max(1e-12);

        let mut comp = Vec::new();
        let mut eigenvalue = Vec::new();
        let mut ratio = Vec::new();
        for (i, &v) in vals.iter().enumerate() {
            comp.push(i as i32);
            eigenvalue.push(v);
            ratio.push(v / total);
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("component", DataType::Int32, false),
            Field::new("eigenvalue", DataType::Float64, false),
            Field::new("ratio", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(comp)) as ArrayRef,
                Arc::new(Float64Array::from(eigenvalue)) as ArrayRef,
                Arc::new(Float64Array::from(ratio)) as ArrayRef,
            ],
        )?;
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}

// ---------------------------------------------------------------------------
// reconstruct_l2(name, depth) — table function over an MBO stream
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct ReconstructL2TableFunction {
    registry: Arc<RwLock<HftRegistry>>,
}

impl ReconstructL2TableFunction {
    pub fn new(registry: Arc<RwLock<HftRegistry>>) -> Self {
        Self { registry }
    }
}

fn extract_num_col(batches: &[RecordBatch], col: &str) -> DfResult<Vec<f64>> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b.column_by_name(col).ok_or_else(|| {
            DataFusionError::Execution(format!("missing `{col}` column"))
        })?;
        match arr.data_type() {
            DataType::Float64 => out.extend_from_slice(f64_values(arr)),
            DataType::Int64 => out.extend(
                as_primitive_array::<Int64Type>(arr.as_ref())
                    .values()
                    .iter()
                    .map(|&v| v as f64),
            ),
            DataType::UInt64 => out.extend(
                as_primitive_array::<UInt64Type>(arr.as_ref())
                    .values()
                    .iter()
                    .map(|&v| v as f64),
            ),
            DataType::Utf8 => {
                // action as "add"/"cancel"/"execute"
                let s = as_string_array(arr);
                out.extend((0..s.len()).map(|i| match s.value(i) {
                    "add" | "A" => 0.0,
                    "cancel" | "C" => 1.0,
                    "execute" | "E" => 2.0,
                    _ => 0.0,
                }));
            }
            _ => {
                return Err(DataFusionError::Execution(format!(
                    "unsupported type for `{col}`"
                )))
            }
        }
    }
    Ok(out)
}

impl TableFunctionImpl for ReconstructL2TableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> DfResult<Arc<dyn TableProvider>> {
        let exprs = args.exprs();
        let name = expr_to_string(
            exprs.first().ok_or_else(|| {
                DataFusionError::Execution("reconstruct_l2(name, depth): missing name".into())
            })?,
        )?;
        let depth = expr_to_i64(
            exprs.get(1).ok_or_else(|| {
                DataFusionError::Execution("reconstruct_l2(name, depth): missing depth".into())
            })?,
        )?
        .max(1) as usize;

        let reg = self.registry.read().map_err(|_| {
            DataFusionError::Execution("hft registry poisoned".into())
        })?;
        let batches = reg.tables.get(&name).ok_or_else(|| {
            DataFusionError::Execution(format!("unknown table `{name}`"))
        })?;

        let oid = {
            let mut v = Vec::new();
            for b in batches.as_ref() {
                let arr = b.column_by_name("order_id").or_else(|| b.column_by_name("id")).ok_or_else(|| {
                    DataFusionError::Execution(format!("table `{name}` has no `order_id` column"))
                })?;
                v.extend(
                    as_primitive_array::<UInt64Type>(arr.as_ref())
                        .values()
                        .iter()
                        .copied(),
                );
            }
            v
        };
        let side = extract_num_col(batches, "side")?;
        let price = extract_num_col(batches, "price")?;
        let qty = extract_num_col(batches, "qty")?;
        let action = extract_num_col(batches, "action")?;

        let msgs: Vec<MboMsg> = (0..oid.len())
            .map(|i| MboMsg {
                order_id: oid[i],
                side: side[i] as u8,
                price: price[i],
                qty: qty[i] as u64,
                action: action[i] as u8,
            })
            .collect();
        let (bids, asks) = gtv_array::quant::reconstruct_l2(&msgs, depth);

        let mut side_out = Vec::new();
        let mut price_out = Vec::new();
        let mut qty_out = Vec::new();
        for (p, q) in bids {
            side_out.push(0i32);
            price_out.push(p);
            qty_out.push(q);
        }
        for (p, q) in asks {
            side_out.push(1i32);
            price_out.push(p);
            qty_out.push(q);
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("side", DataType::Int32, false),
            Field::new("price", DataType::Float64, false),
            Field::new("qty", DataType::UInt64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(side_out)) as ArrayRef,
                Arc::new(Float64Array::from(price_out)) as ArrayRef,
                Arc::new(UInt64Array::from(qty_out)) as ArrayRef,
            ],
        )?;
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}

// ---------------------------------------------------------------------------
// ohlc(name, bucket) — table function: tick -> OHLCV bars
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct OhlcTableFunction {
    registry: Arc<RwLock<HftRegistry>>,
}

impl OhlcTableFunction {
    pub fn new(registry: Arc<RwLock<HftRegistry>>) -> Self {
        Self { registry }
    }
}

fn extract_i64_col(batches: &[RecordBatch], col: &str) -> DfResult<Vec<i64>> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b.column_by_name(col).ok_or_else(|| {
            DataFusionError::Execution(format!("missing `{col}` column"))
        })?;
        if !matches!(arr.data_type(), DataType::Int64) {
            return Err(DataFusionError::Execution(format!(
                "column `{col}` is not Int64"
            )));
        }
        out.extend_from_slice(as_primitive_array::<Int64Type>(arr.as_ref()).values());
    }
    Ok(out)
}

fn extract_string_col(batches: &[RecordBatch], col: &str) -> DfResult<Vec<String>> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b.column_by_name(col).ok_or_else(|| {
            DataFusionError::Execution(format!("missing `{col}` column"))
        })?;
        let s = as_string_array(arr);
        out.extend((0..s.len()).map(|i| s.value(i).to_string()));
    }
    Ok(out)
}

impl TableFunctionImpl for OhlcTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> DfResult<Arc<dyn TableProvider>> {
        let exprs = args.exprs();
        let name = expr_to_string(
            exprs.first()
                .ok_or_else(|| DataFusionError::Execution("ohlc(name, bucket): missing name".into()))?,
        )?;
        let bucket = expr_to_i64(
            exprs.get(1)
                .ok_or_else(|| DataFusionError::Execution("ohlc(name, bucket): missing bucket".into()))?,
        )?
        .max(1);

        let reg = self.registry.read().map_err(|_| {
            DataFusionError::Execution("hft registry poisoned".into())
        })?;
        let batches = reg.tables.get(&name).ok_or_else(|| {
            DataFusionError::Execution(format!("unknown table `{name}`"))
        })?;

        let ts = extract_i64_col(batches, "ts_us")
            .or_else(|_| extract_i64_col(batches, "t"))
            .or_else(|_| extract_i64_col(batches, "time"))
            .or_else(|_| extract_i64_col(batches, "ts"))
            .map_err(|_| {
                DataFusionError::Execution(format!("table `{name}` has no Int64 ts column"))
            })?;
        let price = {
            let mut v = Vec::new();
            for b in batches.as_ref() {
                let arr = b.column_by_name("price").ok_or_else(|| {
                    DataFusionError::Execution(format!("table `{name}` has no `price` column"))
                })?;
                v.extend_from_slice(f64_values(arr));
            }
            v
        };
        let volume = batches
            .first()
            .and_then(|b| b.column_by_name("volume").map(|_| ()))
            .map(|_| {
                let mut v = Vec::new();
                for b in batches.as_ref() {
                    v.extend_from_slice(f64_values(b.column_by_name("volume").unwrap()));
                }
                v
            })
            .unwrap_or_else(|| vec![1.0; price.len()]);

        let has_sym = batches
            .first()
            .and_then(|b| b.column_by_name("symbol").map(|c| matches!(c.data_type(), DataType::Utf8)))
            .unwrap_or(false);

        let mut symbol_out: Vec<String> = Vec::new();
        let mut bar_out: Vec<i64> = Vec::new();
        let mut o_out: Vec<f64> = Vec::new();
        let mut h_out: Vec<f64> = Vec::new();
        let mut l_out: Vec<f64> = Vec::new();
        let mut c_out: Vec<f64> = Vec::new();
        let mut v_out: Vec<f64> = Vec::new();

        if has_sym {
            let syms = extract_string_col(batches, "symbol")?;
            let mut groups: std::collections::BTreeMap<String, Vec<usize>> =
                std::collections::BTreeMap::new();
            for i in 0..ts.len() {
                groups.entry(syms[i].clone()).or_default().push(i);
            }
            for (sym, idxs) in groups {
                let (sub_ts, sub_px, sub_vol): (Vec<i64>, Vec<f64>, Vec<f64>) = idxs
                    .iter()
                    .map(|&i| (ts[i], price[i], volume[i]))
                    .collect::<Vec<_>>()
                    .into_iter()
                    .fold((vec![], vec![], vec![]), |(mut a, mut b, mut c), (t, p, v)| {
                        a.push(t);
                        b.push(p);
                        c.push(v);
                        (a, b, c)
                    });
                let (bar, o, h, l, c, v) = gtv_array::quant::ohlc_single(&sub_ts, &sub_px, &sub_vol, bucket);
                for k in 0..bar.len() {
                    symbol_out.push(sym.clone());
                    bar_out.push(bar[k]);
                    o_out.push(o[k]);
                    h_out.push(h[k]);
                    l_out.push(l[k]);
                    c_out.push(c[k]);
                    v_out.push(v[k]);
                }
            }
        } else {
            let (bar, o, h, l, c, v) = gtv_array::quant::ohlc_single(&ts, &price, &volume, bucket);
            for k in 0..bar.len() {
                bar_out.push(bar[k]);
                o_out.push(o[k]);
                h_out.push(h[k]);
                l_out.push(l[k]);
                c_out.push(c[k]);
                v_out.push(v[k]);
            }
        }

        let mut fields = vec![
            Field::new("bar", DataType::Int64, false),
            Field::new("open", DataType::Float64, false),
            Field::new("high", DataType::Float64, false),
            Field::new("low", DataType::Float64, false),
            Field::new("close", DataType::Float64, false),
            Field::new("volume", DataType::Float64, false),
        ];
        let mut cols: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(bar_out)) as ArrayRef,
            Arc::new(Float64Array::from(o_out)) as ArrayRef,
            Arc::new(Float64Array::from(h_out)) as ArrayRef,
            Arc::new(Float64Array::from(l_out)) as ArrayRef,
            Arc::new(Float64Array::from(c_out)) as ArrayRef,
            Arc::new(Float64Array::from(v_out)) as ArrayRef,
        ];
        if has_sym {
            fields.insert(0, Field::new("symbol", DataType::Utf8, false));
            cols.insert(0, Arc::new(arrow::array::StringArray::from(symbol_out)) as ArrayRef);
        }
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), cols)?;
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}

// ---------------------------------------------------------------------------
// align(name, freq_sec, fill) — multi-symbol time alignment onto a regular grid
// ---------------------------------------------------------------------------

/// One symbol's aligned output rows (grid ts, close, volume, obs-count).
fn align_series(
    ts: &[i64],
    close: &[f64],
    volume: &[f64],
    freq: i64,
    ffill: bool,
) -> Vec<(i64, f64, f64, i64)> {
    let mut out = Vec::new();
    if ts.is_empty() {
        return out;
    }
    let first_b = ts[0] / freq * freq;
    let last_b = ts[ts.len() - 1] / freq * freq;
    let mut i = 0usize;
    let mut carry: Option<(f64, f64)> = None; // last (close, volume) seen
    let mut b = first_b;
    while b <= last_b {
        let next = b + freq;
        let mut lc: f64 = f64::NAN;
        let mut lv: f64 = f64::NAN;
        let mut n = 0i64;
        while i < ts.len() && ts[i] < next {
            lc = close[i];
            lv = volume[i];
            n += 1;
            i += 1;
        }
        if n > 0 {
            carry = Some((lc, lv));
            out.push((b, lc, lv, n));
        } else if ffill {
            if let Some((c, v)) = carry {
                out.push((b, c, v, 0));
            }
        }
        b = next;
    }
    out
}

#[derive(Debug)]
pub struct AlignTableFunction {
    registry: Arc<RwLock<HftRegistry>>,
}

impl AlignTableFunction {
    pub fn new(registry: Arc<RwLock<HftRegistry>>) -> Self {
        Self { registry }
    }
}

impl TableFunctionImpl for AlignTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> DfResult<Arc<dyn TableProvider>> {
        let exprs = args.exprs();
        let name = expr_to_string(
            exprs.first()
                .ok_or_else(|| DataFusionError::Execution("align(name, freq_sec, fill): missing name".into()))?,
        )?;
        let freq = expr_to_i64(
            exprs.get(1)
                .ok_or_else(|| DataFusionError::Execution("align: missing freq_sec (60/3600/86400…)".into()))?,
        )?
        .max(1);
        let fill = exprs
            .get(2)
            .map(expr_to_string)
            .transpose()?
            .unwrap_or_else(|| "ffill".to_string());
        let ffill = match fill.as_str() {
            "ffill" | "fill" | "forward" => true,
            "drop" | "none" => false,
            other => {
                return Err(DataFusionError::Execution(format!(
                    "align: fill must be 'ffill' or 'drop', got '{other}'"
                )))
            }
        };

        let reg = self.registry.read().map_err(|_| {
            DataFusionError::Execution("hft registry poisoned".into())
        })?;
        let batches = reg.tables.get(&name).ok_or_else(|| {
            DataFusionError::Execution(format!("unknown table `{name}`"))
        })?;
        let batches = batches.clone();
        drop(reg);

        let ts = extract_i64_col(&batches, "ts")
            .or_else(|_| extract_i64_col(&batches, "ts_us"))
            .or_else(|_| extract_i64_col(&batches, "t"))
            .or_else(|_| extract_i64_col(&batches, "time"))
            .map_err(|_| {
                DataFusionError::Execution(format!("table `{name}` has no Int64 ts column"))
            })?;
        let close = extract_f64_col_any(&batches, &["close", "price", "adjclose"], &name)?;
        let volume = if batches.first().and_then(|b| b.column_by_name("volume")).is_some() {
            let mut v = Vec::new();
            for b in batches.iter() {
                v.extend(f64_col_values(b.column_by_name("volume").unwrap())?);
            }
            v
        } else {
            vec![1.0; close.len()]
        };
        let syms = if batches
            .first()
            .and_then(|b| b.column_by_name("symbol").map(|c| matches!(c.data_type(), DataType::Utf8)))
            .unwrap_or(false)
        {
            extract_string_col(&batches, "symbol")?
        } else {
            vec![String::new(); ts.len()]
        };
        if ts.len() != close.len() || ts.len() != syms.len() {
            return Err(DataFusionError::Execution(format!(
                "align: ragged columns in `{name}`"
            )));
        }

        // group rows by symbol, sort by ts, align each onto the freq grid
        let mut groups: std::collections::BTreeMap<String, Vec<usize>> =
            std::collections::BTreeMap::new();
        for i in 0..ts.len() {
            groups.entry(syms[i].clone()).or_default().push(i);
        }
        let mut rows: Vec<(i64, String, f64, f64, i64)> = Vec::new();
        for (sym, idxs) in groups {
            let mut sub: Vec<(i64, f64, f64)> = idxs
                .iter()
                .map(|&i| (ts[i], close[i], volume[i]))
                .collect();
            sub.sort_by_key(|r| r.0);
            let (st, sc, sv): (Vec<i64>, Vec<f64>, Vec<f64>) =
                sub.into_iter().map(|(a, b, c)| (a, b, c)).fold(
                    (vec![], vec![], vec![]),
                    |(mut a, mut b, mut c), (t, p, v)| {
                        a.push(t);
                        b.push(p);
                        c.push(v);
                        (a, b, c)
                    },
                );
            for (b, c, v, n) in align_series(&st, &sc, &sv, freq, ffill) {
                rows.push((b, sym.clone(), c, v, n));
            }
        }
        rows.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("symbol", DataType::Utf8, false),
            Field::new("close", DataType::Float64, false),
            Field::new("volume", DataType::Float64, false),
            Field::new("n_obs", DataType::Int64, false),
        ]));
        let mut ts_out = Vec::with_capacity(rows.len());
        let mut sym_out = Vec::with_capacity(rows.len());
        let mut c_out = Vec::with_capacity(rows.len());
        let mut v_out = Vec::with_capacity(rows.len());
        let mut n_out = Vec::with_capacity(rows.len());
        for (b, s, c, v, n) in rows {
            ts_out.push(b);
            sym_out.push(s);
            c_out.push(c);
            v_out.push(v);
            n_out.push(n);
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ts_out)) as ArrayRef,
                Arc::new(arrow::array::StringArray::from(sym_out)) as ArrayRef,
                Arc::new(Float64Array::from(c_out)) as ArrayRef,
                Arc::new(Float64Array::from(v_out)) as ArrayRef,
                Arc::new(Int64Array::from(n_out)) as ArrayRef,
            ],
        )?;
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}

fn extract_f64_col_any(batches: &[RecordBatch], cands: &[&str], name: &str) -> DfResult<Vec<f64>> {
    for cand in cands {
        if batches.first().and_then(|b| b.column_by_name(cand)).is_some() {
            let mut v = Vec::new();
            for b in batches {
                v.extend(f64_col_values(b.column_by_name(cand).unwrap())?);
            }
            return Ok(v);
        }
    }
    Err(DataFusionError::Execution(format!(
        "table `{name}` has no close/price/adjclose Float64 column"
    )))
}

/// Read a numeric column as f64 (casts Int32/Int64/Float32 -> Float64).
fn f64_col_values(array: &arrow::array::ArrayRef) -> DfResult<Vec<f64>> {
    if matches!(array.data_type(), DataType::Float64) {
        return Ok(f64_values(array).to_vec());
    }
    if !matches!(
        array.data_type(),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
            | DataType::Float32
    ) {
        return Err(DataFusionError::Execution(format!(
            "column `{}` is not numeric",
            array.data_type()
        )));
    }
    let casted = arrow::compute::cast(array.as_ref(), &DataType::Float64)?;
    Ok(as_primitive_array::<Float64Type>(casted.as_ref()).values().to_vec())
}

// ---------------------------------------------------------------------------
// zscore / momentum — cross-sectional & technical window UDFs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum WindowOp {
    Zscore,
    Momentum,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct QuantWindowUdf {
    name: &'static str,
    signature: Signature,
    op: WindowOp,
}

impl QuantWindowUdf {
    fn new(name: &'static str, op: WindowOp) -> Self {
        let args = match op {
            WindowOp::Zscore => vec![DataType::Float64],
            WindowOp::Momentum => vec![DataType::Float64, DataType::Int64],
        };
        Self {
            name,
            signature: Signature::exact(args, Volatility::Immutable),
            op,
        }
    }
}

impl WindowUDFImpl for QuantWindowUdf {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(&self, _args: PartitionEvaluatorArgs) -> DfResult<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(QuantWindowEvaluator { op: self.op }))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> DfResult<FieldRef> {
        Ok(Field::new(field_args.name(), DataType::Float64, true).into())
    }
}

#[derive(Debug)]
struct QuantWindowEvaluator {
    op: WindowOp,
}

impl PartitionEvaluator for QuantWindowEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], _num_rows: usize) -> DfResult<ArrayRef> {
        if values.is_empty() {
            return Ok(Arc::new(Float64Array::from(Vec::<f64>::new())));
        }
        let x = f64_values(&values[0]);
        let out: Vec<f64> = match self.op {
            WindowOp::Zscore => gtv_array::quant::zscore(x),
            WindowOp::Momentum => {
                let n = values
                    .get(1)
                    .and_then(|a| ScalarValue::try_from_array(a, 0).ok())
                    .and_then(|s| s.cast_to(&DataType::Int64).ok())
                    .and_then(|s| match s {
                        ScalarValue::Int64(Some(n)) => Some(n.max(1) as usize),
                        _ => None,
                    })
                    .unwrap_or(1);
                gtv_array::quant::momentum(x, n)
            }
        };
        Ok(Arc::new(Float64Array::from(out)))
    }
}

pub fn quant_window_udfs() -> Vec<WindowUDF> {
    vec![
        WindowUDF::from(QuantWindowUdf::new("zscore", WindowOp::Zscore)),
        WindowUDF::from(QuantWindowUdf::new("momentum", WindowOp::Momentum)),
    ]
}

// ---------------------------------------------------------------------------
// signal(z, buy_thr, sell_thr) — scalar UDF: cross-sectional buy/sell/hold
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SignalUdf {
    signature: Signature,
}

impl SignalUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Float64, DataType::Float64, DataType::Float64],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SignalUdf {
    fn name(&self) -> &str {
        "signal"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let z = f64_values(&arrays[0]);
        let buy = f64_values(&arrays[1]);
        let sell = f64_values(&arrays[2]);
        let out: Vec<String> = (0..z.len())
            .map(|i| {
                let b = buy.get(i).copied().unwrap_or(0.5);
                let s = sell.get(i).copied().unwrap_or(-0.5);
                if z[i] >= b {
                    "buy".to_string()
                } else if z[i] <= s {
                    "sell".to_string()
                } else {
                    "hold".to_string()
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(arrow::array::StringArray::from(out))))
    }
}

pub fn signal_udf() -> ScalarUDF {
    ScalarUDF::from(SignalUdf::new())
}

// ---------------------------------------------------------------------------
// cross_sectional_signal(symbols, momentum_n, buy_thr, sell_thr) — one-shot
// multi-symbol signal pipeline with next-day return for backtest validation
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct CrossSectionalSignalTableFunction;

impl CrossSectionalSignalTableFunction {
    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("close", DataType::Float64, false),
            Field::new("momentum", DataType::Float64, false),
            Field::new("zscore", DataType::Float64, false),
            Field::new("signal", DataType::Utf8, false),
            Field::new("next_day_return", DataType::Float64, true),
        ]))
    }
}

impl TableFunctionImpl for CrossSectionalSignalTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> DfResult<Arc<dyn TableProvider>> {
        let exprs = args.exprs();
        let symbols_str = expr_to_string(
            exprs.first().ok_or_else(|| {
                DataFusionError::Execution(
                    "cross_sectional_signal(symbols, n, buy_thr, sell_thr): missing symbols".into(),
                )
            })?,
        )?;
        let n = expr_to_i64(
            exprs.get(1).ok_or_else(|| {
                DataFusionError::Execution(
                    "cross_sectional_signal(symbols, n, buy_thr, sell_thr): missing n".into(),
                )
            })?,
        )?
        .max(1) as usize;
        let buy_thr = expr_to_f64(
            exprs.get(2).ok_or_else(|| {
                DataFusionError::Execution(
                    "cross_sectional_signal(symbols, n, buy_thr, sell_thr): missing buy_thr"
                        .into(),
                )
            })?,
        )?;
        let sell_thr = expr_to_f64(
            exprs.get(3).ok_or_else(|| {
                DataFusionError::Execution(
                    "cross_sectional_signal(symbols, n, buy_thr, sell_thr): missing sell_thr"
                        .into(),
                )
            })?,
        )?;

        let symbols: Vec<String> = symbols_str
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if symbols.is_empty() {
            return Err(DataFusionError::Execution("no symbols given".into()));
        }

        // Per-symbol: fetch daily, momentum, next-day return.
        struct Row {
            symbol: String,
            ts: i64,
            close: f64,
            mom: f64,
            next_ret: Option<f64>,
        }
        let mut rows: Vec<Row> = Vec::new();
        for sym in &symbols {
            let daily = crate::yahoo::fetch_daily(sym, "1y")
                .map_err(|e| DataFusionError::Execution(e.to_string()))?;
            if daily.is_empty() {
                continue;
            }
            let close: Vec<f64> = daily.iter().map(|d| d.close).collect();
            let mom = gtv_array::quant::momentum(&close, n);
            for i in 0..daily.len() {
                let next_ret = if i + 1 < close.len() && close[i] != 0.0 {
                    Some(close[i + 1] / close[i] - 1.0)
                } else {
                    None
                };
                rows.push(Row {
                    symbol: sym.clone(),
                    ts: daily[i].ts,
                    close: daily[i].close,
                    mom: mom[i],
                    next_ret,
                });
            }
        }
        if rows.is_empty() {
            return Err(DataFusionError::Execution("no Yahoo data for symbols".into()));
        }

        // Cross-sectional z-score of momentum per trading day.
        let mut groups: std::collections::BTreeMap<i64, Vec<usize>> =
            std::collections::BTreeMap::new();
        for (i, r) in rows.iter().enumerate() {
            groups.entry(r.ts).or_default().push(i);
        }
        let mut z = vec![0.0f64; rows.len()];
        for idxs in groups.values() {
            let m: Vec<f64> = idxs.iter().map(|&i| rows[i].mom).collect();
            let zs = gtv_array::quant::zscore(&m);
            for (k, &i) in idxs.iter().enumerate() {
                z[i] = zs[k];
            }
        }

        let mut sym_out = Vec::new();
        let mut ts_out = Vec::new();
        let mut close_out = Vec::new();
        let mut mom_out = Vec::new();
        let mut z_out = Vec::new();
        let mut sig_out = Vec::new();
        let mut ret_out: Vec<Option<f64>> = Vec::new();
        for (i, r) in rows.iter().enumerate() {
            sym_out.push(r.symbol.clone());
            ts_out.push(r.ts);
            close_out.push(r.close);
            mom_out.push(r.mom);
            z_out.push(z[i]);
            sig_out.push(if z[i] >= buy_thr {
                "buy".to_string()
            } else if z[i] <= sell_thr {
                "sell".to_string()
            } else {
                "hold".to_string()
            });
            ret_out.push(r.next_ret);
        }

        let schema = Self::schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::StringArray::from(sym_out)) as ArrayRef,
                Arc::new(Int64Array::from(ts_out)) as ArrayRef,
                Arc::new(Float64Array::from(close_out)) as ArrayRef,
                Arc::new(Float64Array::from(mom_out)) as ArrayRef,
                Arc::new(Float64Array::from(z_out)) as ArrayRef,
                Arc::new(arrow::array::StringArray::from(sig_out)) as ArrayRef,
                Arc::new(Float64Array::from(ret_out)) as ArrayRef,
            ],
        )?;
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}

// ---------------------------------------------------------------------------
// relative_strength(target, indices, peers, n) — target vs indices vs peers
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct RelativeStrengthTableFunction;

impl RelativeStrengthTableFunction {
    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("close", DataType::Float64, false),
            Field::new("target_mom", DataType::Float64, false),
            Field::new("target_z", DataType::Float64, false),
            Field::new("idx_mean_mom", DataType::Float64, false),
            Field::new("peer_mean_mom", DataType::Float64, false),
            Field::new("excess_vs_idx", DataType::Float64, false),
            Field::new("excess_vs_peer", DataType::Float64, false),
            Field::new("signal", DataType::Utf8, false),
            Field::new("next_day_return", DataType::Float64, true),
        ]))
    }
}

impl TableFunctionImpl for RelativeStrengthTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> DfResult<Arc<dyn TableProvider>> {
        let exprs = args.exprs();
        let target = expr_to_string(
            exprs.first().ok_or_else(|| {
                DataFusionError::Execution("relative_strength(target, indices, peers, n): missing target".into())
            })?,
        )?;
        let indices_str = expr_to_string(
            exprs.get(1).ok_or_else(|| {
                DataFusionError::Execution("relative_strength(target, indices, peers, n): missing indices".into())
            })?,
        )?;
        let peers_str = expr_to_string(
            exprs.get(2).ok_or_else(|| {
                DataFusionError::Execution("relative_strength(target, indices, peers, n): missing peers".into())
            })?,
        )?;
        let n = expr_to_i64(
            exprs.get(3).ok_or_else(|| {
                DataFusionError::Execution("relative_strength(target, indices, peers, n): missing n".into())
            })?,
        )?
        .max(1) as usize;

        let indices: Vec<String> = indices_str
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        let peers: Vec<String> = peers_str
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        // Fetch all series: symbol -> (ts -> close)
        let mut series: std::collections::BTreeMap<String, Vec<(i64, f64)>> =
            std::collections::BTreeMap::new();
        let mut fetch = |sym: &str| -> DfResult<()> {
            let daily = crate::yahoo::fetch_daily(sym, "1y")
                .map_err(|e| DataFusionError::Execution(e.to_string()))?;
            let v: Vec<(i64, f64)> = daily.iter().map(|d| (d.ts, d.close)).collect();
            series.insert(sym.to_string(), v);
            Ok(())
        };
        fetch(&target)?;
        for i in &indices {
            fetch(i)?;
        }
        for p in &peers {
            fetch(p)?;
        }

        // momentum per symbol: symbol -> (ts -> mom)
        let mut mom_map: std::collections::BTreeMap<String, std::collections::HashMap<i64, f64>> =
            std::collections::BTreeMap::new();
        for (sym, rows) in &series {
            let close: Vec<f64> = rows.iter().map(|&(_, c)| c).collect();
            let mom = gtv_array::quant::momentum(&close, n);
            let mut m = std::collections::HashMap::new();
            for (k, &(ts, _)) in rows.iter().enumerate() {
                m.insert(ts, mom[k]);
            }
            mom_map.insert(sym.clone(), m);
        }
        // Forward-fill: if a benchmark has no bar at the exact ts, use the most
        // recent prior day's momentum (so index/peer calendars align with the
        // target's trading days).
        let mom_ff = |m: &std::collections::HashMap<i64, f64>, ts: i64| -> f64 {
            if let Some(&v) = m.get(&ts) {
                return v;
            }
            let mut best: Option<(i64, f64)> = None;
            for (&k, &v) in m {
                if k <= ts && best.map_or(true, |(bk, _)| k > bk) {
                    best = Some((k, v));
                }
            }
            best.map(|(_, v)| v).unwrap_or(0.0)
        };
        let get_mom = |sym: &str, ts: i64| -> f64 {
            mom_map.get(sym).map(|m| mom_ff(m, ts)).unwrap_or(0.0)
        };

        // Cross-sectional z of target within (target + peers) per day.
        let mut peer_ts: std::collections::BTreeSet<i64> = std::collections::BTreeSet::new();
        for p in &peers {
            if let Some(m) = mom_map.get(p) {
                peer_ts.extend(m.keys().copied());
            }
        }
        let mut target_z_map: std::collections::HashMap<i64, f64> =
            std::collections::HashMap::new();
        for &ts in &peer_ts {
            let mut vals = vec![get_mom(&target, ts)];
            for p in &peers {
                vals.push(get_mom(p, ts));
            }
            let zs = gtv_array::quant::zscore(&vals);
            target_z_map.insert(ts, zs[0]);
        }

        // Iterate the target's own days.
        let mut ts_out = Vec::new();
        let mut close_out = Vec::new();
        let mut tm_out = Vec::new();
        let mut tz_out = Vec::new();
        let mut im_out = Vec::new();
        let mut pm_out = Vec::new();
        let mut exi_out = Vec::new();
        let mut exp_out = Vec::new();
        let mut sig_out = Vec::new();
        let mut ret_out: Vec<Option<f64>> = Vec::new();
        let target_closes: Vec<f64> = series[&target].iter().map(|&(_, c)| c).collect();

        for (i, &(ts, close)) in series[&target].iter().enumerate() {
            let tm = get_mom(&target, ts);
            let idx_mean = if indices.is_empty() {
                0.0
            } else {
                indices.iter().map(|i| get_mom(i, ts)).sum::<f64>() / indices.len() as f64
            };
            let peer_mean = if peers.is_empty() {
                0.0
            } else {
                peers.iter().map(|p| get_mom(p, ts)).sum::<f64>() / peers.len() as f64
            };
            let tz = target_z_map.get(&ts).copied().unwrap_or(0.0);
            let sig = if tm > idx_mean && tm > peer_mean {
                "buy"
            } else if tm < idx_mean && tm < peer_mean {
                "sell"
            } else {
                "hold"
            };
            ts_out.push(ts);
            close_out.push(close);
            tm_out.push(tm);
            tz_out.push(tz);
            im_out.push(idx_mean);
            pm_out.push(peer_mean);
            exi_out.push(tm - idx_mean);
            exp_out.push(tm - peer_mean);
            let next_ret = if i + 1 < target_closes.len() && target_closes[i] != 0.0 {
                Some(target_closes[i + 1] / target_closes[i] - 1.0)
            } else {
                None
            };
            sig_out.push(sig.to_string());
            ret_out.push(next_ret);
        }

        let schema = Self::schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ts_out)) as ArrayRef,
                Arc::new(Float64Array::from(close_out)) as ArrayRef,
                Arc::new(Float64Array::from(tm_out)) as ArrayRef,
                Arc::new(Float64Array::from(tz_out)) as ArrayRef,
                Arc::new(Float64Array::from(im_out)) as ArrayRef,
                Arc::new(Float64Array::from(pm_out)) as ArrayRef,
                Arc::new(Float64Array::from(exi_out)) as ArrayRef,
                Arc::new(Float64Array::from(exp_out)) as ArrayRef,
                Arc::new(arrow::array::StringArray::from(sig_out)) as ArrayRef,
                Arc::new(Float64Array::from(ret_out)) as ArrayRef,
            ],
        )?;
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn align_ffill_fills_gaps_and_drop_skips() {
        const D: i64 = 86_400_000_000_000; // 1 day in ns
        // contiguous series A
        let ts = [0i64, D, 2 * D, 3 * D];
        let close = [10.0, 11.0, 12.0, 13.0];
        let vol = [100.0, 110.0, 120.0, 130.0];
        let ff = align_series(&ts, &close, &vol, D, true);
        assert_eq!(ff.len(), 4, "ffill keeps every grid step");
        assert!((ff[3].1 - 13.0).abs() < 1e-12);
        // B: trades day0 and day2 only (missing day1)
        let tsb = [0i64, 2 * D];
        let cb = [5.0, 7.0];
        let vb = [50.0, 70.0];
        let ff_b = align_series(&tsb, &cb, &vb, D, true);
        assert_eq!(ff_b.len(), 3, "gap on the daily grid is filled");
        assert_eq!(ff_b[0].0, 0);
        assert!((ff_b[1].1 - 5.0).abs() < 1e-12, "carried close 5.0");
        assert_eq!(ff_b[1].3, 0, "filled row n_obs = 0");
        assert!((ff_b[2].1 - 7.0).abs() < 1e-12);
        let drop_b = align_series(&tsb, &cb, &vb, D, false);
        assert_eq!(drop_b.len(), 2, "drop: no synthetic rows");
        assert_eq!(drop_b[0].0, 0);
        assert_eq!(drop_b[1].0, 2 * D);
    }

    #[test]
    fn align_multi_bucket_takes_last_obs() {
        const S: i64 = 1_000_000_000; // 1s in ns
        let ts = [0i64, 2 * S, 5 * S]; // two obs in bucket [0,1s) if freq=1s? 5s lands elsewhere
        let close = [1.0, 2.0, 3.0];
        let vol = [10.0, 20.0, 30.0];
        // freq = 5s -> buckets: 0 and 5
        let out = align_series(&ts, &close, &vol, 5 * S, false);
        assert_eq!(out.len(), 2);
        assert!((out[0].1 - 2.0).abs() < 1e-12, "bucket [0,5s) takes last obs 2.0");
        assert_eq!(out[0].3, 2, "two obs in bucket");
        assert!((out[1].1 - 3.0).abs() < 1e-12);
    }
}
