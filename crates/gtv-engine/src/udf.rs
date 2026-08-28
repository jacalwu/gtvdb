//! kdb+-style window functions (`mavg`, `msum`, `deltas`) as DataFusion
//! user-defined window functions (UDWFs), so they can be called from SQL
//! with the same semantics as the native `gtv_array` primitives.

use std::sync::Arc;

use arrow::array::{as_primitive_array, ArrayRef, Float64Array};
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafusion::error::Result;
use datafusion::logical_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
use datafusion::logical_expr::{
    PartitionEvaluator, Signature, Volatility, WindowUDF, WindowUDFImpl,
};
use datafusion::scalar::ScalarValue;

/// Which gtv_array rolling primitive this UDWF dispatches to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum RollingOp {
    Mavg,
    Msum,
    Deltas,
}

/// A window function over a `Float64` column.
///
/// `mavg(x, n)` and `msum(x, n)` take a trailing window size `n`; `deltas(x)`
/// takes a single argument. The window size is read from the constant second
/// argument at evaluation time, so a single UDWF instance serves every `n`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RollingUdf {
    name: &'static str,
    signature: Signature,
    op: RollingOp,
}

impl RollingUdf {
    fn new(name: &'static str, op: RollingOp) -> Self {
        let signature = match op {
            RollingOp::Mavg | RollingOp::Msum => Signature::exact(
                vec![DataType::Float64, DataType::Int64],
                Volatility::Immutable,
            ),
            RollingOp::Deltas => {
                Signature::exact(vec![DataType::Float64], Volatility::Immutable)
            }
        };
        Self {
            name,
            signature,
            op,
        }
    }
}

impl WindowUDFImpl for RollingUdf {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(&self, _args: PartitionEvaluatorArgs) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(RollingEvaluator { op: self.op }))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<FieldRef> {
        Ok(Field::new(field_args.name(), DataType::Float64, true).into())
    }
}

/// Stateful evaluator: computes the whole output column in one pass over the
/// (already sorted) partition, ignoring any `OVER` window frame.
#[derive(Debug)]
struct RollingEvaluator {
    op: RollingOp,
}

impl PartitionEvaluator for RollingEvaluator {
    /// `values[0]` is the value column; `values[1]` (for mavg/msum) is the
    /// constant window size `n`; `values[2..]` are the `ORDER BY` expressions.
    fn evaluate_all(&mut self, values: &[ArrayRef], _num_rows: usize) -> Result<ArrayRef> {
        if values.is_empty() {
            return Ok(Arc::new(Float64Array::from(Vec::<f64>::new())));
        }
        let vals = extract_f64(&values[0]);
        let out = match self.op {
            RollingOp::Mavg => {
                let n = window_size(values)?;
                gtv_array::window::mavg(&vals, n)
            }
            RollingOp::Msum => {
                let n = window_size(values)?;
                gtv_array::window::msum(&vals, n)
            }
            RollingOp::Deltas => gtv_array::window::deltas(&vals),
        };
        Ok(Arc::new(Float64Array::from(out)))
    }
}

/// Extract the trailing window size `n` from the constant second argument.
fn window_size(values: &[ArrayRef]) -> Result<usize> {
    let Some(arg) = values.get(1) else {
        return Ok(1);
    };
    match ScalarValue::try_from_array(arg, 0)?.cast_to(&DataType::Int64)? {
        ScalarValue::Int64(Some(n)) => Ok(n.max(1) as usize),
        _ => Ok(1),
    }
}

/// Downcast an `ArrayRef` to a `Float64Array` and copy out its values.
fn extract_f64(array: &ArrayRef) -> Vec<f64> {
    as_primitive_array::<Float64Type>(array.as_ref())
        .values()
        .as_ref()
        .to_vec()
}

/// Build the three gtv window UDWFs.
pub fn window_udfs() -> Vec<WindowUDF> {
    vec![
        WindowUDF::from(RollingUdf::new("mavg", RollingOp::Mavg)),
        WindowUDF::from(RollingUdf::new("msum", RollingOp::Msum)),
        WindowUDF::from(RollingUdf::new("deltas", RollingOp::Deltas)),
    ]
}
