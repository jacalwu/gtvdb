//! `ann(name, query, k [, metric])` — vector search over registered persisted
//! indexes ([`gtv_index::AnyIndex`], loaded via [`gtv_index_store`]).
//!
//! This is the SQL surface for index-lifecycle-managed indexes (B2-2): unlike
//! `knn`, the collection may be a Flat / IVF / HNSW index loaded from a
//! `.gtvidx` snapshot rather than an in-memory brute-force collection.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use arrow::array::{ArrayRef, Float64Array, RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result};
use gtv_core::{Metric, VectorIndex};
use gtv_index::AnyIndex;

use crate::expr_util::{expr_to_i64, expr_to_string};

/// Shared registry of persisted indexes, keyed by name.
pub type IndexRegistry = Arc<RwLock<HashMap<String, AnyIndex>>>;

/// The `ann` table function over the persisted-index registry.
#[derive(Debug)]
pub struct AnnTableFunction {
    indexes: IndexRegistry,
}

impl AnnTableFunction {
    pub fn new(indexes: IndexRegistry) -> Self {
        Self { indexes }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("distance", DataType::Float64, false),
        ]))
    }
}

/// Report a distance the same way the `knn` UDTF does (L2 → Euclidean).
fn report_distance(metric: Metric, raw: f32) -> f64 {
    match metric {
        Metric::L2 => (raw.max(0.0) as f64).sqrt(),
        _ => raw as f64,
    }
}

impl TableFunctionImpl for AnnTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let exprs = args.exprs();
        let name = expr_to_string(exprs.first().ok_or_else(|| {
            DataFusionError::Execution("ann(name, query, k [, metric]): missing `name`".into())
        })?)?;
        let query = expr_to_string(exprs.get(1).ok_or_else(|| {
            DataFusionError::Execution("ann(name, query, k [, metric]): missing `query`".into())
        })?)?;
        let k = expr_to_i64(exprs.get(2).ok_or_else(|| {
            DataFusionError::Execution("ann(name, query, k [, metric]): missing `k`".into())
        })?)? as usize;
        let requested_metric = exprs.get(3).map(expr_to_string).transpose()?;

        let query: Vec<f32> = query
            .split(',')
            .map(|t| {
                t.trim()
                    .parse::<f32>()
                    .map_err(|_| DataFusionError::Execution(format!("ann: bad query `{t}`")))
            })
            .collect::<Result<_>>()?;

        let indexes = self
            .indexes
            .read()
            .map_err(|_| DataFusionError::Execution("ann: registry poisoned".into()))?;
        let index = indexes.get(&name).ok_or_else(|| {
            DataFusionError::Execution(format!("ann: unknown index `{name}`"))
        })?;

        if let Some(m) = requested_metric.as_deref() {
            let metric = Metric::parse(m).ok_or_else(|| {
                DataFusionError::Execution(format!("ann: unknown metric `{m}` (l2|cosine|dot)"))
            })?;
            if metric != index.metric() {
                return Err(DataFusionError::Execution(format!(
                    "ann: metric mismatch — index is {}, query asked for {}",
                    index.metric().as_str(),
                    metric.as_str()
                )));
            }
        }

        let hits = index
            .search(&query, k, None)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let metric = index.metric();
        let ids: Vec<u64> = hits.iter().map(|h| h.id).collect();
        let dists: Vec<f64> = hits
            .iter()
            .map(|h| report_distance(metric, h.distance))
            .collect();

        let batch = RecordBatch::try_new(
            Self::schema(),
            vec![
                Arc::new(UInt64Array::from(ids)) as ArrayRef,
                Arc::new(Float64Array::from(dists)) as ArrayRef,
            ],
        )?;
        Ok(Arc::new(MemTable::try_new(Self::schema(), vec![vec![batch]])?))
    }
}
