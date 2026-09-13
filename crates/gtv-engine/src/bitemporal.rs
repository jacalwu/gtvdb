//! Bitemporal SQL surface (prod_p3 B3-2).
//!
//! `as_of(table, business_ts [, system_ts])` answers a two-axis query against a
//! bitemporal table registered in the session: it first picks the newest system
//! version known at `system_ts` (default: now / latest) and then keeps the rows
//! whose business interval contains `business_ts`. `bitemporal_overlaps(table,
//! key_column)` surfaces contradictory versions for governance.

use std::sync::{Arc, RwLock};

use arrow::array::{Int64Array, RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result};
use gtv_storage::BitemporalStore;

use crate::expr_util::{expr_to_i64, expr_to_string};

/// Shared handle to the session's bitemporal store.
pub type BitemporalRegistry = Arc<RwLock<BitemporalStore>>;

fn poisoned(what: &str) -> DataFusionError {
    DataFusionError::Execution(format!("{what}: bitemporal registry poisoned"))
}

/// `as_of(table, business_ts [, system_ts])`.
#[derive(Debug)]
pub struct BitemporalAsOfTableFunction {
    store: BitemporalRegistry,
}

impl BitemporalAsOfTableFunction {
    pub fn new(store: BitemporalRegistry) -> Self {
        Self { store }
    }
}

impl TableFunctionImpl for BitemporalAsOfTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let exprs = args.exprs();
        let usage = "as_of(table, business_ts [, system_ts])";
        let table = expr_to_string(
            exprs
                .first()
                .ok_or_else(|| DataFusionError::Execution(format!("{usage}: missing `table`")))?,
        )?;
        let business_ts = expr_to_i64(exprs.get(1).ok_or_else(|| {
            DataFusionError::Execution(format!("{usage}: missing `business_ts`"))
        })?)?;
        let system_ts = exprs
            .get(2)
            .map(expr_to_i64)
            .transpose()?
            .unwrap_or(i64::MAX);

        let store = self
            .store
            .read()
            .map_err(|_| poisoned("as_of"))?;
        let batches = store
            .as_of(&table, business_ts, system_ts)
            .map_err(|e| DataFusionError::Execution(format!("as_of: {e}")))?;
        let schema = batches
            .first()
            .map(|b| b.schema())
            .or_else(|| store.schema(&table))
            .ok_or_else(|| {
                DataFusionError::Execution(format!("as_of: unknown bitemporal table `{table}`"))
            })?;
        if batches.is_empty() {
            let empty = RecordBatch::new_empty(schema.clone());
            return Ok(Arc::new(MemTable::try_new(schema, vec![vec![empty]])?));
        }
        Ok(Arc::new(MemTable::try_new(schema, vec![batches])?))
    }
}

/// `bitemporal_overlaps(table, key_column)` — contradictory versions.
#[derive(Debug)]
pub struct BitemporalOverlapsTableFunction {
    store: BitemporalRegistry,
}

impl BitemporalOverlapsTableFunction {
    pub fn new(store: BitemporalRegistry) -> Self {
        Self { store }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::UInt64, false),
            Field::new("first", DataType::UInt64, false),
            Field::new("second", DataType::UInt64, false),
            Field::new("business_from", DataType::Int64, false),
            Field::new("business_to", DataType::Int64, false),
        ]))
    }
}

impl TableFunctionImpl for BitemporalOverlapsTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let exprs = args.exprs();
        let usage = "bitemporal_overlaps(table, key_column)";
        let table = expr_to_string(
            exprs
                .first()
                .ok_or_else(|| DataFusionError::Execution(format!("{usage}: missing `table`")))?,
        )?;
        let key = expr_to_string(exprs.get(1).ok_or_else(|| {
            DataFusionError::Execution(format!("{usage}: missing `key_column`"))
        })?)?;

        let store = self
            .store
            .read()
            .map_err(|_| poisoned("bitemporal_overlaps"))?;
        let found = store
            .overlaps(&table, &key)
            .map_err(|e| DataFusionError::Execution(format!("bitemporal_overlaps: {e}")))?;
        let schema = Self::schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(
                    found.iter().map(|o| o.key).collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(
                    found.iter().map(|o| o.first as u64).collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(
                    found.iter().map(|o| o.second as u64).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    found.iter().map(|o| o.business_from).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    found.iter().map(|o| o.business_to).collect::<Vec<_>>(),
                )),
            ],
        )?;
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}
