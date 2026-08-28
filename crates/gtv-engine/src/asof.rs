//! kdb+-style `asof` join exposed to SQL as a DataFusion table function.
//!
//! `asof_join(t0, t1, ...)` matches each left time against a right-side series
//! captured at registration time, returning `(t, value)` rows where `value` is
//! the last right value whose time is `<= t` (or `NULL` before the first).

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::Result;

use crate::expr_util::expr_to_i64;

/// Table function over a snapshot of a right-side time series.
#[derive(Debug)]
pub struct AsofJoinTableFunction {
    right_times: Vec<i64>,
    right_values: Vec<f64>,
}

impl AsofJoinTableFunction {
    pub fn new(right_times: Vec<i64>, right_values: Vec<f64>) -> Self {
        debug_assert_eq!(
            right_times.len(),
            right_values.len(),
            "right_times and right_values must have equal length"
        );
        Self {
            right_times,
            right_values,
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("t", DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
        ]))
    }
}

impl TableFunctionImpl for AsofJoinTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let left: Vec<i64> = args
            .exprs()
            .iter()
            .map(expr_to_i64)
            .collect::<Result<_>>()?;

        let matched = gtv_array::asof::asof_join_f64(&left, &self.right_times, &self.right_values);

        let values: Float64Array = matched.iter().copied().collect();
        let batch = RecordBatch::try_new(
            Self::schema(),
            vec![
                Arc::new(Int64Array::from(left)) as ArrayRef,
                Arc::new(values) as ArrayRef,
            ],
        )?;

        Ok(Arc::new(MemTable::try_new(Self::schema(), vec![vec![batch]])?))
    }
}
