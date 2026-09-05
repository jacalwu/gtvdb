//! A DataFusion [`SessionContext`] wrapper for the gtv engine.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use gtv_core::TemporalCSR;

use crate::hft_exec::{compile_hft, AsofResource, HftRegistry, KernelPlan, PitResource};
use crate::knn::{KnnCollection, KnnTableFunction};

/// A DataFusion `SessionContext` that gtv tables and UDFs are registered into.
#[derive(Clone)]
pub struct GtvContext {
    ctx: SessionContext,
    knn_collections: Arc<RwLock<HashMap<String, KnnCollection>>>,
    hft_reg: Arc<RwLock<HftRegistry>>,
}

impl GtvContext {
    pub fn new() -> Self {
        let ctx = SessionContext::new();
        for udwf in crate::udf::window_udfs() {
            ctx.register_udwf(udwf);
        }
        for udwf in crate::hft::hft_window_udfs() {
            ctx.register_udwf(udwf);
        }
        for udf in crate::hft::hft_scalar_udfs() {
            ctx.register_udf(udf);
        }
        for udaf in crate::hft::hft_aggregate_udfs() {
            ctx.register_udaf(udaf);
        }
        for udf in crate::quant::bs_udfs() {
            ctx.register_udf(udf);
        }
        ctx.register_udf(crate::quant::xbar_udf());
        ctx.register_udf(crate::quant::signal_udf());
        ctx.register_udf(crate::datetime::trunc_udf());
        for udwf in crate::micro::micro_window_udfs() {
            ctx.register_udwf(udwf);
        }
        for udwf in crate::quant::quant_window_udfs() {
            ctx.register_udwf(udwf);
        }
        for udf in crate::micro::micro_scalar_udfs() {
            ctx.register_udf(udf);
        }
        let knn_collections = Arc::new(RwLock::new(HashMap::new()));
        ctx.register_udtf("knn", Arc::new(KnnTableFunction::new(knn_collections.clone())));
        ctx.register_udtf("vector_search", Arc::new(KnnTableFunction::new(knn_collections.clone())));
        ctx.register_udtf("read_csv", Arc::new(crate::csv::ReadCsvTableFunction::new()));
        ctx.register_udtf("read_parquet", Arc::new(crate::csv::ReadParquetTableFunction::new()));
        ctx.register_udtf("read_yahoo", Arc::new(crate::yahoo::ReadYahooTableFunction::new()));
        ctx.register_udtf("cross_sectional_signal", Arc::new(crate::quant::CrossSectionalSignalTableFunction));
        ctx.register_udtf("relative_strength", Arc::new(crate::quant::RelativeStrengthTableFunction));
        ctx.register_udtf("read_tickdata", Arc::new(crate::tickdata::ReadTickdataTableFunction));
        let hft_reg = Arc::new(RwLock::new(HftRegistry::default()));
        {
            let tt = Arc::new(crate::hft_tf::TickToTradeTableFunction::new(hft_reg.clone()));
            ctx.register_udtf("tick_to_trade", tt.clone());
            ctx.register_udtf("ttrade", tt);
            let mo = Arc::new(crate::hft_tf::MatchOrdersTableFunction::new(hft_reg.clone()));
            ctx.register_udtf("match_orders", mo.clone());
            ctx.register_udtf("match", mo);
            let cv = Arc::new(crate::hft_tf::CovarianceTableFunction::new(hft_reg.clone()));
            ctx.register_udtf("covariance_matrix", cv.clone());
            ctx.register_udtf("cov", cv);
            let var = Arc::new(crate::quant::VarHistoricalTableFunction::new(hft_reg.clone()));
            ctx.register_udtf("var_historical", var.clone());
            ctx.register_udtf("var", var);
            let pca = Arc::new(crate::quant::PcaTableFunction::new(hft_reg.clone()));
            ctx.register_udtf("pca", pca);
            let l2 = Arc::new(crate::quant::ReconstructL2TableFunction::new(hft_reg.clone()));
            ctx.register_udtf("reconstruct_l2", l2.clone());
            ctx.register_udtf("l2", l2);
            let ohlc = Arc::new(crate::quant::OhlcTableFunction::new(hft_reg.clone()));
            ctx.register_udtf("ohlc", ohlc);
            ctx.register_udtf(
                "fwd_proba",
                Arc::new(crate::analytics::FwdProbaTableFunction::new(hft_reg.clone())),
            );
        }
        Self {
            ctx,
            knn_collections,
            hft_reg,
        }
    }

    /// Compile a HFT-subset SQL string into a precompiled [`KernelPlan`], or
    /// `None` when it falls outside the fast-path subset.
    pub fn compile_hft(&self, sql: &str) -> Option<KernelPlan> {
        let reg = self.hft_reg.read().ok()?;
        compile_hft(sql, &reg)
    }

    pub fn session(&self) -> &SessionContext {
        &self.ctx
    }

    /// Register an in-memory table from a schema and its record batches.
    /// Replaces any existing table of the same name (idempotent re-register).
    pub fn register_batches(
        &self,
        name: &str,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<()> {
        let _ = self.ctx.deregister_table(name);
        let table = MemTable::try_new(schema.clone(), vec![batches.clone()])?;
        self.ctx.register_table(name, Arc::new(table))?;
        if let Ok(mut reg) = self.hft_reg.write() {
            reg.tables.insert(name.to_string(), Arc::new(batches));
        }
        Ok(())
    }

    /// Remove a registered table (no-op when absent).
    ///
    /// Clears both the DataFusion catalog entry and the compiled-kernel table
    /// registry (`hft_reg`), so a later `drop table` / `DROP TABLE` fully
    /// releases the underlying record batches.
    pub fn deregister_table(&self, name: &str) {
        let _ = self.ctx.deregister_table(name);
        if let Ok(mut reg) = self.hft_reg.write() {
            reg.tables.remove(name);
        }
    }

    /// Whether `name` resolves to a registered table (kernel registry or the
    /// DataFusion catalog). Querying with `LIMIT 0` resolves the name during
    /// planning, so `Err` means the table does not exist.
    pub async fn has_table(&self, name: &str) -> bool {
        if self
            .hft_reg
            .read()
            .map(|r| r.tables.contains_key(name))
            .unwrap_or(false)
        {
            return true;
        }
        self.ctx.sql(&format!("SELECT * FROM {name} LIMIT 0")).await.is_ok()
    }

    /// Row count of a registered table without materializing a query, or
    /// `None` when the table is not in the kernel registry (e.g. created via
    /// native DataFusion CTAS). Cheap — used by background flush tasks to
    /// decide whether a hot table changed since the last checkpoint.
    pub fn table_rows(&self, name: &str) -> Option<usize> {
        let reg = self.hft_reg.read().ok()?;
        reg.tables
            .get(name)
            .map(|batches| batches.iter().map(|b| b.num_rows()).sum())
    }

    /// Load a CSV file from disk and register it as `name` (method 1:
    /// traditional disk load).
    pub fn register_csv(&self, path: &str, name: &str) -> Result<()> {
        let batches = gtv_storage::read_csv(path)
            .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
        let first = batches
            .first()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(format!("empty csv `{path}`"))
            })?;
        self.register_batches(name, first.schema(), batches)
    }

    /// Load a Parquet file from disk and register it as `name`.
    pub fn register_parquet(&self, path: &str, name: &str) -> Result<()> {
        let batches = gtv_storage::read_batches(path)
            .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
        let first = batches
            .first()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(format!("empty parquet `{path}`"))
            })?;
        self.register_batches(name, first.schema(), batches)
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

    /// Register a multi-column + tolerance `asof_join`/`aj` against a right-side
    /// series (times + price + spread).
    pub fn register_asof_join_multi(
        &self,
        right_times: Vec<i64>,
        right_price: Vec<f64>,
        right_spread: Vec<f64>,
        tolerance_ns: i64,
    ) {
        let tf = Arc::new(crate::asof::AsofJoinTableFunction::new_multi(
            right_times.clone(),
            right_price.clone(),
            right_spread.clone(),
            tolerance_ns,
        ));
        self.ctx.register_udtf("asof_join", tf.clone());
        self.ctx.register_udtf("aj", tf);
        let res = Arc::new(AsofResource {
            times: Arc::new(right_times),
            price: Arc::new(right_price),
            spread: Arc::new(right_spread),
            tolerance: tolerance_ns,
        });
        if let Ok(mut reg) = self.hft_reg.write() {
            reg.asof.insert("aj".to_string(), res);
        }
    }

    /// Register `point_in_time`/`pit` over temporally-sorted rows.
    pub fn register_point_in_time(&self, valid_from: Vec<i64>, valid_to: Vec<i64>) {
        let tf = Arc::new(crate::hft_tf::PointInTimeTableFunction::new(
            valid_from.clone(),
            valid_to.clone(),
        ));
        self.ctx.register_udtf("point_in_time", tf.clone());
        self.ctx.register_udtf("pit", tf);
        let res = Arc::new(PitResource {
            valid_from: Arc::new(valid_from),
            valid_to: Arc::new(valid_to),
        });
        if let Ok(mut reg) = self.hft_reg.write() {
            reg.pit.insert("pit".to_string(), res);
        }
    }

    /// Register `wash_trade`/`wash` over a temporal transfer graph.
    pub fn register_wash_trade(&self, csr: &TemporalCSR) {
        let tf = Arc::new(crate::hft_tf::WashTradeTableFunction::new(csr));
        self.ctx.register_udtf("wash_trade", tf.clone());
        self.ctx.register_udtf("wash", tf);
        if let Ok(mut reg) = self.hft_reg.write() {
            reg.wash.insert("wash".to_string(), Arc::new(csr.clone()));
        }
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
