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
}

impl Greek {
    fn apply(self, opt: &str, s: f64, k: f64, t: f64, r: f64, v: f64) -> f64 {
        match self {
            Greek::Price => gtv_array::quant::bs_price(opt, s, k, t, r, v),
            Greek::Delta => gtv_array::quant::bs_delta(opt, s, k, t, r, v),
            Greek::Gamma => gtv_array::quant::bs_gamma(opt, s, k, t, r, v),
            Greek::Vega => gtv_array::quant::bs_vega(opt, s, k, t, r, v),
            Greek::Theta => gtv_array::quant::bs_theta(opt, s, k, t, r, v),
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
