//! Graph traversal exposed to SQL as a DataFusion table function.
//!
//! `neighbors(src, valid_at)` returns the temporal neighbors of `src` that are
//! active at `valid_at` (nanoseconds), with the same column shape as the
//! registered `edges` table (temporal columns as `Int64` nanoseconds).

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, UInt16Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result};
use gtv_core::TemporalCSR;

use crate::expr_util::{expr_to_i64, expr_to_u64};

/// Table function over a snapshot of the temporal CSR.
#[derive(Debug)]
pub struct NeighborsTableFunction {
    csr: Arc<TemporalCSR>,
}

impl NeighborsTableFunction {
    pub fn new(csr: &TemporalCSR) -> Self {
        Self {
            csr: Arc::new(csr.clone()),
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("src", DataType::UInt64, false),
            Field::new("dst", DataType::UInt64, false),
            Field::new("edge_type", DataType::UInt16, false),
            Field::new("valid_from", DataType::Int64, false),
            Field::new("valid_to", DataType::Int64, false),
        ]))
    }
}

impl TableFunctionImpl for NeighborsTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let exprs = args.exprs();
        let src = expr_to_u64(
            exprs
                .first()
                .ok_or_else(|| DataFusionError::Execution("neighbors(src, valid_at): missing `src`".into()))?,
        )?;
        let valid_at = expr_to_i64(
            exprs
                .get(1)
                .ok_or_else(|| DataFusionError::Execution("neighbors(src, valid_at): missing `valid_at`".into()))?,
        )?;

        let mut src_out = Vec::new();
        let mut dst_out = Vec::new();
        let mut et_out = Vec::new();
        let mut vf_out = Vec::new();
        let mut vt_out = Vec::new();
        let neighbors = self
            .csr
            .neighbors(src, valid_at)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        for nb in neighbors {
            src_out.push(src);
            dst_out.push(nb.dst);
            et_out.push(nb.edge_type);
            vf_out.push(nb.valid_from);
            vt_out.push(nb.valid_to);
        }

        let batch = RecordBatch::try_new(
            Self::schema(),
            vec![
                Arc::new(UInt64Array::from(src_out)) as ArrayRef,
                Arc::new(UInt64Array::from(dst_out)) as ArrayRef,
                Arc::new(UInt16Array::from(et_out)) as ArrayRef,
                Arc::new(Int64Array::from(vf_out)) as ArrayRef,
                Arc::new(Int64Array::from(vt_out)) as ArrayRef,
            ],
        )?;

        Ok(Arc::new(MemTable::try_new(Self::schema(), vec![vec![batch]])?))
    }
}
