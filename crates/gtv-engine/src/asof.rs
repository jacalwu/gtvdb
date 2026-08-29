//! kdb+-style `asof` join exposed to SQL as a DataFusion table function.
//!
//! `asof_join(t0, t1, ...)` matches each left time against a right-side series
//! captured at registration time. Two shapes are supported:
//!   * single-column: `(t, value)`
//!   * multi-column + tolerance: `(t, price, spread)` where a match is dropped
//!     when `left_t - matched_right_t > tolerance`.

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
    right_price: Vec<f64>,
    /// When `Some`, the table function returns `(t, price, spread)` instead of
    /// `(t, value)` and applies the tolerance window.
    right_spread: Option<Vec<f64>>,
    tolerance_ns: Option<i64>,
}

impl AsofJoinTableFunction {
    /// Single-column as-of join (backward compatible): `(t, value)`.
    pub fn new(right_times: Vec<i64>, right_values: Vec<f64>) -> Self {
        debug_assert_eq!(right_times.len(), right_values.len());
        Self {
            right_times,
            right_price: right_values,
            right_spread: None,
            tolerance_ns: None,
        }
    }

    /// Multi-column as-of join with tolerance: `(t, price, spread)`.
    pub fn new_multi(
        right_times: Vec<i64>,
        right_price: Vec<f64>,
        right_spread: Vec<f64>,
        tolerance_ns: i64,
    ) -> Self {
        debug_assert_eq!(right_times.len(), right_price.len());
        debug_assert_eq!(right_times.len(), right_spread.len());
        Self {
            right_times,
            right_price,
            right_spread: Some(right_spread),
            tolerance_ns: Some(tolerance_ns),
        }
    }

    fn schema(&self) -> SchemaRef {
        let mut fields = vec![
            Field::new("t", DataType::Int64, false),
            Field::new(
                if self.right_spread.is_some() { "price" } else { "value" },
                DataType::Float64,
                true,
            ),
        ];
        if self.right_spread.is_some() {
            fields.push(Field::new("spread", DataType::Float64, true));
        }
        Arc::new(Schema::new(fields))
    }
}

impl TableFunctionImpl for AsofJoinTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let left: Vec<i64> = args
            .exprs()
            .iter()
            .map(expr_to_i64)
            .collect::<Result<_>>()?;

        let schema = self.schema();
        let n = left.len();
        let mut price = Vec::with_capacity(n);
        let mut spread: Vec<Option<f64>> = Vec::with_capacity(n);

        for &lt in &left {
            // Binary search for the greatest right time <= lt.
            let j = self.right_times.partition_point(|&rt| rt <= lt);
            if j == 0 {
                price.push(None);
                if self.right_spread.is_some() {
                    spread.push(None);
                }
                continue;
            }
            let matched_t = self.right_times[j - 1];
            let within_tol = self
                .tolerance_ns
                .map_or(true, |tol| lt - matched_t <= tol);
            if within_tol {
                price.push(Some(self.right_price[j - 1]));
                if let Some(s) = &self.right_spread {
                    spread.push(Some(s[j - 1]));
                }
            } else {
                price.push(None);
                if self.right_spread.is_some() {
                    spread.push(None);
                }
            }
        }

        let mut cols: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(left)) as ArrayRef,
            Arc::new(Float64Array::from(price)) as ArrayRef,
        ];
        if let Some(_) = &self.right_spread {
            cols.push(Arc::new(Float64Array::from(spread)) as ArrayRef);
        }

        let batch = RecordBatch::try_new(schema.clone(), cols)?;
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}
