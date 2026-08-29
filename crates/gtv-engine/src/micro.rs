//! TC11–TC15 market micro-structure operators as DataFusion UDFs.
//!
//! Direction operators (`tick_rule`, `lee_ready`, `emo`, `aggressor_flag`)
//! return `Int32` (`1`/`-1`/`0`); `ofi_l1` returns `Float64`. Direction
//! operators with a lag are window UDFs (`OVER (ORDER BY t)`); the aggressor
//! flag is a scalar UDF. Full names and HFT abbreviations are both registered.

use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::{as_primitive_array, as_string_array, ArrayRef, Float64Array, Int32Array};
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafusion::error::Result;
use datafusion::logical_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
use datafusion::logical_expr::{
    ColumnarValue, PartitionEvaluator, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility, WindowUDF, WindowUDFImpl,
};

fn f64_values(array: &ArrayRef) -> &[f64] {
    as_primitive_array::<Float64Type>(array.as_ref()).values().as_ref()
}

// ---------------------------------------------------------------------------
// Window UDFs: tick_rule / lee_ready / emo / ofi_l1
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum MicroOp {
    TickRule,
    LeeReady,
    Emo,
    OfiL1,
}

impl MicroOp {
    fn arity(self) -> usize {
        match self {
            MicroOp::TickRule => 1,
            MicroOp::LeeReady | MicroOp::Emo => 3,
            MicroOp::OfiL1 => 4,
        }
    }

    fn out_type(self) -> DataType {
        match self {
            MicroOp::OfiL1 => DataType::Float64,
            _ => DataType::Int32,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MicroWindowUdf {
    name: &'static str,
    signature: Signature,
    op: MicroOp,
}

impl MicroWindowUdf {
    fn new(name: &'static str, op: MicroOp) -> Self {
        let arg_types = vec![DataType::Float64; op.arity()];
        Self {
            name,
            signature: Signature::exact(arg_types, Volatility::Immutable),
            op,
        }
    }
}

impl WindowUDFImpl for MicroWindowUdf {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(&self, _args: PartitionEvaluatorArgs) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(MicroEvaluator { op: self.op }))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<FieldRef> {
        Ok(Field::new(field_args.name(), self.op.out_type(), true).into())
    }
}

#[derive(Debug)]
struct MicroEvaluator {
    op: MicroOp,
}

impl PartitionEvaluator for MicroEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], _num_rows: usize) -> Result<ArrayRef> {
        if values.len() < self.op.arity() {
            return Ok(Arc::new(Int32Array::from(Vec::<i32>::new())));
        }
        match self.op {
            MicroOp::TickRule => {
                let out = gtv_array::micro::tick_rule(f64_values(&values[0]));
                Ok(Arc::new(Int32Array::from(out)))
            }
            MicroOp::LeeReady => {
                let out = gtv_array::micro::lee_ready(
                    f64_values(&values[0]),
                    f64_values(&values[1]),
                    f64_values(&values[2]),
                );
                Ok(Arc::new(Int32Array::from(out)))
            }
            MicroOp::Emo => {
                let out = gtv_array::micro::emo(
                    f64_values(&values[0]),
                    f64_values(&values[1]),
                    f64_values(&values[2]),
                );
                Ok(Arc::new(Int32Array::from(out)))
            }
            MicroOp::OfiL1 => {
                let out = gtv_array::micro::ofi_l1(
                    f64_values(&values[0]),
                    f64_values(&values[1]),
                    f64_values(&values[2]),
                    f64_values(&values[3]),
                );
                Ok(Arc::new(Float64Array::from(out)))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Scalar UDF: aggressor_flag
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AggressorUdf {
    name: &'static str,
    signature: Signature,
}

impl AggressorUdf {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for AggressorUdf {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let flags = as_string_array(&arrays[0]);
        let out: Int32Array = flags
            .iter()
            .map(|f| match f {
                Some("B") | Some("BUY") => 1,
                Some("S") | Some("SELL") => -1,
                _ => 0,
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

// ---------------------------------------------------------------------------
// Registration helpers
// ---------------------------------------------------------------------------

pub fn micro_window_udfs() -> Vec<WindowUDF> {
    vec![
        WindowUDF::from(MicroWindowUdf::new("tick_rule", MicroOp::TickRule)),
        WindowUDF::from(MicroWindowUdf::new("tick", MicroOp::TickRule)),
        WindowUDF::from(MicroWindowUdf::new("lee_ready", MicroOp::LeeReady)),
        WindowUDF::from(MicroWindowUdf::new("lr", MicroOp::LeeReady)),
        WindowUDF::from(MicroWindowUdf::new("emo", MicroOp::Emo)),
        WindowUDF::from(MicroWindowUdf::new("ofi_l1", MicroOp::OfiL1)),
        WindowUDF::from(MicroWindowUdf::new("ofil", MicroOp::OfiL1)),
    ]
}

pub fn micro_scalar_udfs() -> Vec<ScalarUDF> {
    vec![
        ScalarUDF::from(AggressorUdf::new("aggressor_flag")),
        ScalarUDF::from(AggressorUdf::new("agg")),
    ]
}
