//! File-loading table functions: `read_csv(path)` and `read_parquet(path)`.
//!
//! Both materialize a file as an in-memory table expression that can be queried
//! directly or assigned to a session variable with
//! `CREATE TABLE t AS SELECT * FROM read_csv('path')` (or `read_parquet`).

use std::sync::Arc;

use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result};

use crate::expr_util::expr_to_string;

#[derive(Debug, Default)]
pub struct ReadCsvTableFunction;

impl ReadCsvTableFunction {
    pub fn new() -> Self {
        Self
    }
}

impl TableFunctionImpl for ReadCsvTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let path = expr_to_string(
            args.exprs()
                .first()
                .ok_or_else(|| DataFusionError::Execution("read_csv(path): missing path".into()))?,
        )?;
        let batches = gtv_storage::read_csv(&path)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let first = batches
            .first()
            .ok_or_else(|| DataFusionError::Execution(format!("empty csv `{path}`")))?;
        Ok(Arc::new(MemTable::try_new(first.schema(), vec![batches])?))
    }
}

/// `read_parquet(path)` table function — load a Parquet file as an in-memory
/// table (the Parquet counterpart of [`ReadCsvTableFunction`]).
#[derive(Debug, Default)]
pub struct ReadParquetTableFunction;

impl ReadParquetTableFunction {
    pub fn new() -> Self {
        Self
    }
}

impl TableFunctionImpl for ReadParquetTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let path = expr_to_string(
            args.exprs()
                .first()
                .ok_or_else(|| {
                    DataFusionError::Execution("read_parquet(path): missing path".into())
                })?,
        )?;
        let batches = gtv_storage::read_batches(&path)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let first = batches
            .first()
            .ok_or_else(|| DataFusionError::Execution(format!("empty parquet `{path}`")))?;
        Ok(Arc::new(MemTable::try_new(first.schema(), vec![batches])?))
    }
}
