//! HFT table functions (M1): `point_in_time` and `wash_trade`.
//!
//! Thin wrappers over `gtv_core::temporal::point_in_time_range` (O(log N),
//! zero-copy slicing) and `gtv_pattern::find` (temporal ring detection).

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result};
use gtv_core::temporal::point_in_time_range;
use gtv_core::TemporalCSR;
use gtv_pattern::{find, Pattern};

use crate::expr_util::expr_to_i64;

/// `point_in_time(T)` over temporally-sorted rows: returns the active
/// `[valid_from <= T < valid_to)` index range via two `partition_point` binary
/// searches — O(log N), no mask materialization, zero writes.
#[derive(Debug)]
pub struct PointInTimeTableFunction {
    valid_from: Vec<i64>,
    valid_to: Vec<i64>,
}

impl PointInTimeTableFunction {
    pub fn new(valid_from: Vec<i64>, valid_to: Vec<i64>) -> Self {
        debug_assert_eq!(valid_from.len(), valid_to.len());
        Self {
            valid_from,
            valid_to,
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("idx", DataType::Int64, false),
            Field::new("valid_from", DataType::Int64, false),
            Field::new("valid_to", DataType::Int64, false),
        ]))
    }
}

impl TableFunctionImpl for PointInTimeTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let t = expr_to_i64(
            args.exprs()
                .first()
                .ok_or_else(|| DataFusionError::Execution("point_in_time(T): missing T".into()))?,
        )?;

        let range = point_in_time_range(&self.valid_from, &self.valid_to, t);
        let idx: Vec<i64> = (range.start..range.end).map(|i| i as i64).collect();
        let vf = self.valid_from[range.clone()].to_vec();
        let vt = self.valid_to[range].to_vec();

        let batch = RecordBatch::try_new(
            Self::schema(),
            vec![
                Arc::new(Int64Array::from(idx)) as ArrayRef,
                Arc::new(Int64Array::from(vf)) as ArrayRef,
                Arc::new(Int64Array::from(vt)) as ArrayRef,
            ],
        )?;
        Ok(Arc::new(MemTable::try_new(Self::schema(), vec![vec![batch]])?))
    }
}

/// `wash_trade(T)` over a temporal transfer graph: detects every `A->B->C->A`
/// ring active at `T` with strictly increasing event times, returning
/// `(a, b, c)` node triples.
#[derive(Debug)]
pub struct WashTradeTableFunction {
    csr: TemporalCSR,
}

impl WashTradeTableFunction {
    pub fn new(csr: &TemporalCSR) -> Self {
        Self { csr: csr.clone() }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt64, false),
            Field::new("b", DataType::UInt64, false),
            Field::new("c", DataType::UInt64, false),
        ]))
    }
}

impl TableFunctionImpl for WashTradeTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let t = expr_to_i64(
            args.exprs()
                .first()
                .ok_or_else(|| DataFusionError::Execution("wash_trade(T): missing T".into()))?,
        )?;

        let matches = find(&self.csr, &Pattern::ring(3), t, 10_000)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        let mut a = Vec::new();
        let mut b = Vec::new();
        let mut c = Vec::new();
        for m in &matches {
            a.push(m.nodes[0]);
            b.push(m.nodes[1]);
            c.push(m.nodes[2]);
        }

        let batch = RecordBatch::try_new(
            Self::schema(),
            vec![
                Arc::new(UInt64Array::from(a)) as ArrayRef,
                Arc::new(UInt64Array::from(b)) as ArrayRef,
                Arc::new(UInt64Array::from(c)) as ArrayRef,
            ],
        )?;
        Ok(Arc::new(MemTable::try_new(Self::schema(), vec![vec![batch]])?))
    }
}
