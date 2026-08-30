//! Quant operators (roadmap phase 2) as DataFusion UDFs:
//! Black-Scholes price/Greeks (scalar), historical VaR / PCA / L2 reconstruction
//! (table functions over registered tables).

use std::sync::{Arc, RwLock};

use arrow::array::{as_primitive_array, as_string_array, Array, ArrayRef, Float64Array, Int32Array, UInt64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Float64Type, Int64Type, Schema, UInt64Type};
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::{ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
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
