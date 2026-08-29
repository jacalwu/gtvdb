//! HFT operators exposed as DataFusion UDFs (M1).
//!
//! These are thin wrappers: each operator downcasts the Arrow arrays to raw
//! slices and calls the compiled `gtv_array` / `gtv-core` kernel directly, then
//! wraps the result back into an Arrow array. Full names and HFT abbreviations
//! are both registered (e.g. `order_flow_imbalance` and `ofi`).

use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::{as_primitive_array, ArrayRef, BooleanArray, Float64Array};
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafusion::error::Result;
use datafusion::logical_expr::function::{AccumulatorArgs, PartitionEvaluatorArgs, StateFieldsArgs, WindowUDFFieldArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, ColumnarValue, PartitionEvaluator,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility, WindowUDF,
    WindowUDFImpl,
};
use datafusion::scalar::ScalarValue;

/// Downcast an `ArrayRef` to a `Float64Array` and return its raw values.
fn f64_values(array: &ArrayRef) -> &[f64] {
    as_primitive_array::<Float64Type>(array.as_ref()).values().as_ref()
}

// ---------------------------------------------------------------------------
// TC2 — order flow imbalance (`order_flow_imbalance` / `ofi`), window UDF
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct OfiUdf {
    name: &'static str,
    signature: Signature,
}

impl OfiUdf {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            signature: Signature::exact(
                vec![
                    DataType::Float64,
                    DataType::Float64,
                    DataType::Float64,
                    DataType::Float64,
                    DataType::Int64,
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl WindowUDFImpl for OfiUdf {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(&self, _args: PartitionEvaluatorArgs) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(OfiEvaluator))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<FieldRef> {
        Ok(Field::new(field_args.name(), DataType::Float64, true).into())
    }
}

#[derive(Debug)]
struct OfiEvaluator;

impl PartitionEvaluator for OfiEvaluator {
    /// `values[0..4]` = bid, ask, bid_sz, ask_sz; `values[4]` = constant window.
    fn evaluate_all(&mut self, values: &[ArrayRef], _num_rows: usize) -> Result<ArrayRef> {
        if values.len() < 4 {
            return Ok(Arc::new(Float64Array::from(Vec::<f64>::new())));
        }
        let bid = f64_values(&values[0]);
        let ask = f64_values(&values[1]);
        let bid_sz = f64_values(&values[2]);
        let ask_sz = f64_values(&values[3]);
        let window = values
            .get(4)
            .and_then(|a| ScalarValue::try_from_array(a, 0).ok())
            .and_then(|s| s.cast_to(&DataType::Int64).ok())
            .and_then(|s| match s {
                ScalarValue::Int64(Some(n)) => Some(n.max(1) as usize),
                _ => None,
            })
            .unwrap_or(1);
        Ok(Arc::new(Float64Array::from(gtv_array::window::ofi_rolling(
            bid, ask, bid_sz, ask_sz, window,
        ))))
    }
}

// ---------------------------------------------------------------------------
// TC6 — risk check (`risk_ok` / `risk`), scalar UDF (branchless)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RiskOkUdf {
    name: &'static str,
    signature: Signature,
}

impl RiskOkUdf {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            signature: Signature::exact(
                vec![
                    DataType::Float64,
                    DataType::Float64,
                    DataType::Float64,
                    DataType::Float64,
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RiskOkUdf {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let price = f64_values(&arrays[0]);
        let qty = f64_values(&arrays[1]);
        let mid = f64_values(&arrays[2]);
        let smp = f64_values(&arrays[3]);
        let out: BooleanArray = (0..price.len())
            .map(|i| {
                let (p, q, m, s) = (price[i], qty[i], mid[i], smp[i]);
                (p - m).abs() <= 0.05 * m && q <= 10_000.0 && p * q <= 10e6 && s == 0.0
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

// ---------------------------------------------------------------------------
// TC8 — micro price (`micro_price` / `mp`), scalar UDF
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MicroPriceUdf {
    name: &'static str,
    signature: Signature,
}

impl MicroPriceUdf {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            signature: Signature::exact(
                vec![
                    DataType::Float64,
                    DataType::Float64,
                    DataType::Float64,
                    DataType::Float64,
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for MicroPriceUdf {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let bp = f64_values(&arrays[0]);
        let ap = f64_values(&arrays[1]);
        let bs = f64_values(&arrays[2]);
        let asz = f64_values(&arrays[3]);
        let out: Float64Array = (0..bp.len())
            .map(|i| {
                let (b, a, bq, aq) = (bp[i], ap[i], bs[i], asz[i]);
                let d = bq + aq;
                if d == 0.0 {
                    0.0
                } else {
                    (b * aq + a * bq) / d
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

// ---------------------------------------------------------------------------
// TC8 — order book imbalance (`order_book_imbalance` / `obi`), aggregate UDF
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ObiUdf {
    name: &'static str,
    signature: Signature,
}

impl ObiUdf {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            signature: Signature::exact(
                vec![DataType::Float64, DataType::Float64],
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for ObiUdf {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn accumulator(&self, _args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ObiAccumulator::default()))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Field::new("sum_bid", DataType::Float64, true).into(),
            Field::new("sum_ask", DataType::Float64, true).into(),
        ])
    }
}

#[derive(Debug, Default)]
struct ObiAccumulator {
    sum_bid: f64,
    sum_ask: f64,
}

impl Accumulator for ObiAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let b = f64_values(&values[0]);
        let a = f64_values(&values[1]);
        self.sum_bid += b.iter().sum::<f64>();
        self.sum_ask += a.iter().sum::<f64>();
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let b = f64_values(&states[0]);
        let a = f64_values(&states[1]);
        self.sum_bid += b.iter().sum::<f64>();
        self.sum_ask += a.iter().sum::<f64>();
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let d = self.sum_bid + self.sum_ask;
        let obi = if d == 0.0 {
            0.0
        } else {
            (self.sum_bid - self.sum_ask) / d
        };
        Ok(ScalarValue::Float64(Some(obi)))
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Float64(Some(self.sum_bid)),
            ScalarValue::Float64(Some(self.sum_ask)),
        ])
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

// ---------------------------------------------------------------------------
// Registration helpers
// ---------------------------------------------------------------------------

/// Window UDFs: `order_flow_imbalance` + abbreviation `ofi`.
pub fn hft_window_udfs() -> Vec<WindowUDF> {
    vec![
        WindowUDF::from(OfiUdf::new("order_flow_imbalance")),
        WindowUDF::from(OfiUdf::new("ofi")),
    ]
}

/// Scalar UDFs: `risk_ok`/`risk`, `micro_price`/`mp`.
pub fn hft_scalar_udfs() -> Vec<ScalarUDF> {
    vec![
        ScalarUDF::from(RiskOkUdf::new("risk_ok")),
        ScalarUDF::from(RiskOkUdf::new("risk")),
        ScalarUDF::from(MicroPriceUdf::new("micro_price")),
        ScalarUDF::from(MicroPriceUdf::new("mp")),
    ]
}

/// Aggregate UDFs: `order_book_imbalance`/`obi`.
pub fn hft_aggregate_udfs() -> Vec<AggregateUDF> {
    vec![
        AggregateUDF::from(ObiUdf::new("order_book_imbalance")),
        AggregateUDF::from(ObiUdf::new("obi")),
    ]
}
