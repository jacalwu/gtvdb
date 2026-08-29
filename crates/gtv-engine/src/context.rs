//! A DataFusion [`SessionContext`] wrapper for the gtv engine.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use gtv_core::TemporalCSR;

use crate::knn::{KnnCollection, KnnTableFunction};

/// A DataFusion `SessionContext` that gtv tables and UDFs are registered into.
#[derive(Clone)]
pub struct GtvContext {
    ctx: SessionContext,
    knn_collections: Arc<RwLock<HashMap<String, KnnCollection>>>,
}

impl GtvContext {
    pub fn new() -> Self {
        let ctx = SessionContext::new();
        for udwf in crate::udf::window_udfs() {
            ctx.register_udwf(udwf);
        }
        let knn_collections = Arc::new(RwLock::new(HashMap::new()));
        ctx.register_udtf("knn", Arc::new(KnnTableFunction::new(knn_collections.clone())));
        Self {
            ctx,
            knn_collections,
        }
    }

    pub fn session(&self) -> &SessionContext {
        &self.ctx
    }

    /// Register an in-memory table from a schema and its record batches.
    pub fn register_batches(
        &self,
        name: &str,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<()> {
        let table = MemTable::try_new(schema, vec![batches])?;
        self.ctx.register_table(name, Arc::new(table))?;
        Ok(())
    }

    /// Run a SQL query and collect all result batches.
    pub async fn sql(&self, query: &str) -> Result<Vec<RecordBatch>> {
        let df = self.ctx.sql(query).await?;
        df.collect().await
    }

    /// Register the `neighbors(src, valid_at)` graph table function.
    pub fn register_neighbors(&self, csr: &TemporalCSR) {
        self.ctx.register_udtf(
            "neighbors",
            Arc::new(crate::graph::NeighborsTableFunction::new(csr)),
        );
    }

    /// Register the `asof_join(t0, t1, ...)` table function against a
    /// right-side time series (times + values, equally long).
    pub fn register_asof_join(&self, right_times: Vec<i64>, right_values: Vec<f64>) {
        self.ctx.register_udtf(
            "asof_join",
            Arc::new(crate::asof::AsofJoinTableFunction::new(
                right_times,
                right_values,
            )),
        );
    }

    /// Register a named vector collection for `knn(name, query, k [, label])`.
    pub fn register_knn(
        &self,
        name: &str,
        ids: Vec<u64>,
        vectors: Vec<Vec<f32>>,
        labels: Option<Vec<String>>,
    ) -> Result<()> {
        let collection = KnnCollection::new(ids, vectors, labels)
            .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
        self.knn_collections
            .write()
            .map_err(|_| {
                datafusion::error::DataFusionError::Execution(
                    "knn collection registry poisoned".into(),
                )
            })?
            .insert(name.to_string(), collection);
        Ok(())
    }
}

impl Default for GtvContext {
    fn default() -> Self {
        Self::new()
    }
}
