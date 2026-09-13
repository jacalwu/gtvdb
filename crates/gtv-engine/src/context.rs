//! A DataFusion [`SessionContext`] wrapper for the gtv engine.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::error::Result;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use gtv_catalog::{ExecutionRecord, IndexRef, ModelRef, TableRef};
use gtv_core::TemporalCSR;
use gtv_index::AnyIndex;

use crate::hft_exec::{compile_hft, AsofResource, HftRegistry, KernelPlan, PitResource};
use crate::knn::{KnnCollection, KnnTableFunction};
use crate::embedding::{EmbeddingCollection, EmbeddingRegistry, EmbeddingSearchTableFunction};

/// Options attached to a lineage-enabled execution.
#[derive(Debug, Clone, Default)]
pub struct ExecutionOptions {
    pub model_versions: Vec<ModelRef>,
    pub index_snapshots: Vec<IndexRef>,
    pub scenario_version: Option<String>,
    pub business_cutoff: Option<i64>,
    pub runtime_params: serde_json::Value,
}

/// Directory DataFusion uses for sort/join spill files (B3-6 §7.5).
///
/// `GTV_SPILL_DIR` overrides the default of `<system temp>/gtv-spill`.
/// Production deployments should point this at the catalog volume so spill
/// bytes are accounted for next to the data they came from.
pub fn spill_dir() -> PathBuf {
    std::env::var_os("GTV_SPILL_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("gtv-spill"))
}

/// blake3 (hex) of the Arrow IPC encoding of the output batches.
pub(crate) fn batches_checksum(batches: &[RecordBatch]) -> String {
    let Some(first) = batches.first() else {
        return blake3::hash(b"").to_hex().to_string();
    };
    let mut buf = Vec::new();
    if let Ok(mut w) = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &first.schema()) {
        for b in batches {
            let _ = w.write(b);
        }
        let _ = w.finish();
    }
    blake3::hash(&buf).to_hex().to_string()
}

/// A DataFusion `SessionContext` that gtv tables and UDFs are registered into.
#[derive(Clone)]
pub struct GtvContext {
    ctx: SessionContext,
    knn_collections: Arc<RwLock<HashMap<String, KnnCollection>>>,
    any_indexes: crate::ann::IndexRegistry,
    /// Governed embedding collections (B2-4) for `embedding_search`.
    embedding_collections: EmbeddingRegistry,
    /// Bitemporal system-time versions for `as_of` (B3-2).
    bitemporal: crate::bitemporal::BitemporalRegistry,
    /// Multimodal cost-based optimizer state (B3-5).
    cbo: crate::cbo::CboRegistry,
    /// Workload admission / isolation manager (B3-6).
    workload: Arc<crate::workload::WorkloadManager>,
    /// Configurable CRM rating / type maps (prod_p4 audit P0).
    crm_maps: Arc<RwLock<crate::crm::CrmRatingMaps>>,
    /// Registered tables mapped to the catalog snapshot they were loaded from.
    table_sources: Arc<RwLock<HashMap<String, TableRef>>>,
    hft_reg: Arc<RwLock<HftRegistry>>,
}

impl GtvContext {
    pub fn new() -> Self {
        // B3-5: EXPLAIN should surface the estimated statistics that the
        // `StatsTable` provider exposes (num_rows / min / max / null_count).
        let mut config = SessionConfig::new();
        config.options_mut().explain.show_statistics = true;

        // B3-6 spill-to-disk: large sorts / joins land under the configured
        // spill directory (default `<tmp>/gtv-spill`) instead of the process
        // temp dir, so operations can account for and cap spill usage.
        let spill = spill_dir();
        let _ = std::fs::create_dir_all(&spill);
        let disk = DiskManagerBuilder::default()
            .with_mode(DiskManagerMode::Directories(vec![spill]));
        let runtime = RuntimeEnvBuilder::new()
            .with_disk_manager_builder(disk)
            .build()
            .expect("gtv: build DataFusion runtime env");
        let ctx = SessionContext::new_with_config_rt(config, Arc::new(runtime));
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
        for udwf in crate::indicator::indicator_window_udfs() {
            ctx.register_udwf(udwf);
        }
        for udf in crate::micro::micro_scalar_udfs() {
            ctx.register_udf(udf);
        }
        let knn_collections = Arc::new(RwLock::new(HashMap::new()));
        ctx.register_udtf("knn", Arc::new(KnnTableFunction::new(knn_collections.clone())));
        ctx.register_udtf("vector_search", Arc::new(KnnTableFunction::new(knn_collections.clone())));
        let any_indexes: crate::ann::IndexRegistry = Arc::new(RwLock::new(HashMap::new()));
        ctx.register_udtf("ann", Arc::new(crate::ann::AnnTableFunction::new(any_indexes.clone())));
        ctx.register_udtf("ann_search", Arc::new(crate::ann::AnnTableFunction::new(any_indexes.clone())));
        ctx.register_udtf(
            "ann_explain",
            Arc::new(crate::ann::AnnExplainTableFunction::new(any_indexes.clone())),
        );
        let embedding_collections: EmbeddingRegistry = Arc::new(RwLock::new(HashMap::new()));
        ctx.register_udtf(
            "embedding_search",
            Arc::new(EmbeddingSearchTableFunction::new(embedding_collections.clone())),
        );
        let bitemporal: crate::bitemporal::BitemporalRegistry =
            Arc::new(RwLock::new(gtv_storage::BitemporalStore::new()));
        let as_of = Arc::new(crate::bitemporal::BitemporalAsOfTableFunction::new(
            bitemporal.clone(),
        ));
        ctx.register_udtf("as_of", as_of.clone());
        ctx.register_udtf("bitemporal_as_of", as_of);
        ctx.register_udtf(
            "bitemporal_overlaps",
            Arc::new(crate::bitemporal::BitemporalOverlapsTableFunction::new(
                bitemporal.clone(),
            )),
        );
        let cbo: crate::cbo::CboRegistry = Arc::new(RwLock::new(crate::cbo::CboState::default()));
        ctx.register_udtf(
            "cbo_explain",
            Arc::new(crate::cbo::CboExplainTableFunction::new(
                any_indexes.clone(),
                cbo.clone(),
            )),
        );
        let workload = Arc::new(crate::workload::WorkloadManager::default());
        ctx.register_udtf(
            "workload_status",
            Arc::new(crate::workload::WorkloadStatusTableFunction::new(
                workload.clone(),
            )),
        );
        ctx.register_udtf("read_csv", Arc::new(crate::csv::ReadCsvTableFunction::new()));
        ctx.register_udtf("read_parquet", Arc::new(crate::csv::ReadParquetTableFunction::new()));
        ctx.register_udtf("read_yahoo", Arc::new(crate::yahoo::ReadYahooTableFunction::new()));
        ctx.register_udtf("cross_sectional_signal", Arc::new(crate::quant::CrossSectionalSignalTableFunction));
        ctx.register_udtf("relative_strength", Arc::new(crate::quant::RelativeStrengthTableFunction));
        ctx.register_udtf("read_tickdata", Arc::new(crate::tickdata::ReadTickdataTableFunction));
        crate::market::register_udtfs(&ctx);
        let hft_reg = Arc::new(RwLock::new(HftRegistry::default()));
        // Configurable CRM rating maps — regulatory defaults, overridable from
        // a configuration table via `GtvContext::set_crm_rating_maps`.
        let crm_maps: Arc<RwLock<crate::crm::CrmRatingMaps>> =
            Arc::new(RwLock::new(crate::crm::CrmRatingMaps::regulatory_defaults()));
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
                "align",
                Arc::new(crate::quant::AlignTableFunction::new(hft_reg.clone())),
            );
            let crm_alloc = Arc::new(crate::crm::CrmAllocTableFunction::new(
                hft_reg.clone(),
                crm_maps.clone(),
            ));
            ctx.register_udtf("crm_alloc", crm_alloc);
            ctx.register_udtf(
                "crm_audit",
                Arc::new(crate::crm::CrmAuditTableFunction::new(
                    hft_reg.clone(),
                    crm_maps.clone(),
                )),
            );
            ctx.register_udtf(
                "crm_rating_map",
                Arc::new(crate::crm::CrmRatingMapTableFunction::new(crm_maps.clone())),
            );
            ctx.register_udtf(
                "backtest",
                Arc::new(crate::bt::BacktestTableFunction::new(hft_reg.clone())),
            );
            ctx.register_udtf(
                "bt_report",
                Arc::new(crate::bt::BtReportTableFunction::new(hft_reg.clone())),
            );
            ctx.register_udtf(
                "pf_backtest",
                Arc::new(crate::bt::PfBacktestTableFunction::new(hft_reg.clone())),
            );
            ctx.register_udtf(
                "pf_report",
                Arc::new(crate::bt::PfReportTableFunction::new(hft_reg.clone())),
            );
            ctx.register_udtf(
                "dq_report",
                Arc::new(crate::monitor::DqReportTableFunction::new(hft_reg.clone())),
            );
            ctx.register_udtf(
                "dq_check",
                Arc::new(crate::monitor::DqCheckTableFunction::new(hft_reg.clone())),
            );
            ctx.register_udtf(
                "dq_gate",
                Arc::new(crate::dq::DqGateTableFunction::new(hft_reg.clone())),
            );
            ctx.register_udtf(
                "health_check",
                Arc::new(crate::monitor::HealthCheckTableFunction::new(hft_reg.clone())),
            );
            ctx.register_udtf(
                "strategy_stats",
                Arc::new(crate::monitor::StrategyStatsTableFunction::new(hft_reg.clone())),
            );
            ctx.register_udtf(
                "fwd_proba",
                Arc::new(crate::analytics::FwdProbaTableFunction::new(hft_reg.clone())),
            );
            ctx.register_udtf(
                "fwd_walk",
                Arc::new(crate::analytics::FwdWalkTableFunction::new(hft_reg.clone())),
            );
            ctx.register_udtf(
                "fwd_regress",
                Arc::new(crate::analytics::FwdRegressTableFunction::new(hft_reg.clone())),
            );
        }
        Self {
            ctx,
            knn_collections,
            any_indexes,
            embedding_collections,
            bitemporal,
            cbo,
            workload,
            crm_maps,
            table_sources: Arc::new(RwLock::new(HashMap::new())),
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
        // Wrap with the B3-5 statistics provider so EXPLAIN / DataFusion's own
        // optimizers see catalog stats (num_rows / min / max / null_count).
        let provider = crate::cbo::StatsTable::new(
            name,
            schema.clone(),
            vec![batches.clone()],
            self.cbo.clone(),
        );
        self.ctx.register_table(name, Arc::new(provider))?;
        // Seed session statistics from the actual batches so EXPLAIN shows
        // min/max/null bounds even before a catalog refresh (B3-5).
        if let Ok(mut cbo) = self.cbo.write() {
            cbo.tables.insert(
                name.to_string(),
                gtv_catalog::TableStats::from_batches(name, &batches),
            );
        }
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

    /// Cloned record batches for a registered session table, or `None` when the
    /// table is not in the kernel registry. Used by the DQ gate.
    pub fn table_batches(&self, name: &str) -> Option<Vec<RecordBatch>> {
        let reg = self.hft_reg.read().ok()?;
        reg.tables.get(name).map(|b| b.as_ref().clone())
    }

    /// Evaluate data-quality `rules` over session table `table`, resolving
    /// referential parents from other session tables. Returns the gate decision
    /// plus the per-rule evidence. Errors only when the target table is missing.
    pub fn evaluate_dq(
        &self,
        table: &str,
        rules: &[gtv_catalog::DqRule],
    ) -> Result<(gtv_catalog::GateDecision, Vec<crate::dq::RuleOutcome>)> {
        let batches = self.table_batches(table).ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!("unknown table `{table}`"))
        })?;
        let reg = self.hft_reg.clone();
        let lookup = move |parent: &str, col: &str| crate::dq::parent_lookup(&reg, parent, col);
        Ok(crate::dq::evaluate(&batches, rules, &lookup))
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
        self.ctx.register_udtf(
            "khop",
            Arc::new(crate::graph::KhopTableFunction::new(csr)),
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
        self.register_knn_metric(name, ids, vectors, labels, gtv_core::Metric::L2)
    }

    /// Register a named vector collection with an explicit distance metric.
    pub fn register_knn_metric(
        &self,
        name: &str,
        ids: Vec<u64>,
        vectors: Vec<Vec<f32>>,
        labels: Option<Vec<String>>,
        metric: gtv_core::Metric,
    ) -> Result<()> {
        let collection = KnnCollection::with_metric(ids, vectors, labels, metric)
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

    /// Register a persisted (loaded) index for `ann(name, query, k [, metric])`.
    pub fn register_any_index(&self, name: &str, index: AnyIndex) {
        if let Ok(mut map) = self.any_indexes.write() {
            map.insert(name.to_string(), index);
        }
    }

    /// Register a governed embedding batch (catalog standard schema) for
    /// `embedding_search(name, query, k [, tenant [, as_of]])`. The batch is
    /// validated (dimension / single model-metric / provenance) first.
    pub fn register_embedding(&self, name: &str, batch: &RecordBatch) -> Result<()> {
        let collection = EmbeddingCollection::from_batch(batch)?;
        self.register_embedding_collection(name, collection);
        Ok(())
    }

    /// Register an already-built [`EmbeddingCollection`].
    pub fn register_embedding_collection(&self, name: &str, collection: EmbeddingCollection) {
        if let Ok(mut map) = self.embedding_collections.write() {
            map.insert(name.to_string(), collection);
        }
    }

    /// Append a system-time version of `table` for the bitemporal `as_of`
    /// surface. Corrections append a new `system_from`; older versions stay
    /// queryable.
    pub fn register_bitemporal_version(
        &self,
        table: &str,
        system_from: i64,
        batches: Vec<RecordBatch>,
    ) -> Result<()> {
        self.bitemporal
            .write()
            .map_err(|_| {
                datafusion::error::DataFusionError::Execution(
                    "bitemporal registry poisoned".into(),
                )
            })?
            .record(table, system_from, batches)
            .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))
    }

    /// Shared bitemporal store (for the CLI / catalog wiring).
    pub fn bitemporal_store(&self) -> crate::bitemporal::BitemporalRegistry {
        self.bitemporal.clone()
    }

    /// Shared multimodal cost-based optimizer state (B3-5).
    pub fn cbo_state(&self) -> crate::cbo::CboRegistry {
        self.cbo.clone()
    }

    /// Register relational statistics for `name` (used by `cbo_explain`).
    pub fn set_table_stats(&self, name: &str, stats: gtv_catalog::TableStats) -> Result<()> {
        self.cbo
            .write()
            .map_err(|_| {
                datafusion::error::DataFusionError::Execution("cbo registry poisoned".into())
            })?
            .tables
            .insert(name.to_string(), stats);
        Ok(())
    }

    /// Refresh relational statistics from the catalog (never stale: reads the
    /// current committed version).
    pub fn refresh_table_stats(
        &self,
        name: &str,
        catalog: &gtv_catalog::FsCatalog,
        table: gtv_catalog::TableId,
    ) -> Result<()> {
        let stats = catalog
            .table_stats(table)
            .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
        self.set_table_stats(name, stats)
    }

    /// Refresh statistics for `name` from the catalog by table name (B3-5).
    ///
    /// Convenience wrapper that resolves the [`gtv_catalog::TableId`] first,
    /// used by the CLI after `publish`.
    pub fn refresh_table_stats_by_name(
        &self,
        name: &str,
        catalog: &gtv_catalog::FsCatalog,
    ) -> Result<()> {
        let stats = catalog
            .table_stats_by_name(name)
            .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
        self.set_table_stats(name, stats)
    }

    /// Register graph shape statistics for `name`.
    pub fn set_graph_stats(&self, name: &str, stats: crate::cbo::GraphStats) -> Result<()> {
        self.cbo
            .write()
            .map_err(|_| {
                datafusion::error::DataFusionError::Execution("cbo registry poisoned".into())
            })?
            .graphs
            .insert(name.to_string(), stats);
        Ok(())
    }

    /// Replace the cost model (weights / enable flag).
    pub fn set_cost_model(&self, model: crate::cbo::CostModel) -> Result<()> {
        self.cbo
            .write()
            .map_err(|_| {
                datafusion::error::DataFusionError::Execution("cbo registry poisoned".into())
            })?
            .model = model;
        Ok(())
    }

    /// Shared workload manager (B3-6).
    pub fn workload(&self) -> Arc<crate::workload::WorkloadManager> {
        self.workload.clone()
    }

    /// Replace the CRM rating / type maps (prod_p4 audit P0). Loading from a
    /// configuration table keeps the regulatory defaults for unspecified keys.
    pub fn set_crm_rating_maps(
        &self,
        maps: crate::crm::CrmRatingMaps,
    ) -> Result<()> {
        *self
            .crm_maps
            .write()
            .map_err(|_| {
                datafusion::error::DataFusionError::Execution(
                    "crm rating maps poisoned".into(),
                )
            })? = maps;
        Ok(())
    }

    /// Shared handle to the CRM rating maps.
    pub fn crm_rating_maps(&self) -> Arc<RwLock<crate::crm::CrmRatingMaps>> {
        self.crm_maps.clone()
    }

    /// Execute `query` under a workload class, with admission control.
    ///
    /// This is the B3-6 integration point: the class's resource group decides
    /// whether the query is admitted, queued or rejected; a preemption signal
    /// arriving before collection aborts the query. Operators that observe the
    /// returned [`gtv_core::CancelToken`] (graph traversal, index build) can be
    /// cancelled cooperatively mid-flight via
    /// [`crate::workload::WorkloadManager::preempt`].
    pub async fn sql_as(
        &self,
        class: crate::workload::WorkloadClass,
        query: &str,
        timeout: std::time::Duration,
    ) -> std::result::Result<Vec<RecordBatch>, crate::workload::WorkloadError> {
        use crate::workload::{Admission, WorkloadError};
        let (id, token) = match self.workload.wait_admit(class, timeout)? {
            Admission::Admit { id, token } => (id, token),
            Admission::Queue { .. } => {
                return Err(WorkloadError::Rejected("unexpected queued admission".into()))
            }
            Admission::Reject { reason } => return Err(WorkloadError::Rejected(reason)),
        };
        if token.load(std::sync::atomic::Ordering::Relaxed) {
            self.workload.release(id);
            return Err(WorkloadError::Preempted);
        }
        let out = self
            .sql(query)
            .await
            .map_err(|e| WorkloadError::Rejected(e.to_string()));
        self.workload.release(id);
        out
    }

    /// Bytes currently used by DataFusion spill files (B3-6 §7.5).
    pub fn spill_bytes(&self) -> u64 {
        self.ctx.runtime_env().disk_manager.used_disk_space()
    }

    /// Prometheus text: process query metrics, per-class workload telemetry and
    /// spill usage.
    pub fn prometheus(&self) -> String {
        let mut s = crate::monitor::prometheus_text();
        s.push_str(&self.workload.prometheus());
        s.push_str("# TYPE gtv_spill_bytes gauge\n");
        s.push_str(&format!("gtv_spill_bytes {}\n", self.spill_bytes()));
        let progress = self.ctx.runtime_env().disk_manager.spilling_progress();
        s.push_str("# TYPE gtv_spill_active_files gauge\n");
        s.push_str(&format!(
            "gtv_spill_active_files {}\n",
            progress.active_files_count
        ));
        s
    }

    /// Record which catalog snapshot a registered table was loaded from, so a
    /// lineage record can pin it.
    pub fn set_table_source(&self, name: &str, source: TableRef) {
        if let Ok(mut map) = self.table_sources.write() {
            map.insert(name.to_string(), source);
        }
    }

    /// The catalog snapshot a registered table came from, if known.
    pub fn table_source(&self, name: &str) -> Option<TableRef> {
        self.table_sources.read().ok()?.get(name).cloned()
    }

    /// Execute `sql`, returning the output batches and a complete
    /// [`ExecutionRecord`] (source snapshots, model/index versions, output
    /// checksum). The caller persists the record (e.g. via the catalog).
    pub async fn execute_with_lineage(
        &self,
        sql: &str,
        opts: ExecutionOptions,
    ) -> Result<(Vec<RecordBatch>, ExecutionRecord)> {
        let mut record = ExecutionRecord::begin(sql, env!("CARGO_PKG_VERSION"));

        let df = self.ctx.sql(sql).await?;
        let plan = df.logical_plan().clone();
        let mut names: Vec<String> = Vec::new();
        let _ = plan.apply(&mut |node: &LogicalPlan| {
            if let LogicalPlan::TableScan(scan) = node {
                names.push(scan.table_name.table().to_string());
            }
            Ok(TreeNodeRecursion::Continue)
        })?;

        let batches = df.collect().await?;
        let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        let checksum = batches_checksum(&batches);

        names.sort();
        names.dedup();
        if let Ok(sources) = self.table_sources.read() {
            record.source_tables = names
                .iter()
                .filter_map(|n| sources.get(n).cloned())
                .collect();
        }
        record.model_versions = opts.model_versions;
        record.index_snapshots = opts.index_snapshots;
        record.udf_versions = crate::lineage::extract_udfs(&plan);
        record.scenario_version = opts.scenario_version;
        record.business_cutoff = opts.business_cutoff;
        record.runtime_params = opts.runtime_params;
        record.finish(checksum, rows);
        Ok((batches, record))
    }
}

impl Default for GtvContext {
    fn default() -> Self {
        Self::new()
    }
}
