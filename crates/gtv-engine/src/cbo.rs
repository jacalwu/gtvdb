//! B3-5 multimodal cost-based optimizer.
//!
//! Combines three statistic families — relational ([`TableStats`], from
//! `gtv-catalog` commit-time column stats), graph ([`GraphStats`], from the
//! temporal CSR) and vector ([`VectorStats`] / [`SelectivityStats`], from the
//! ANN index) — into a single [`QueryPlanChoice`]: filter-first vs ANN-first,
//! recommended index type, temporal-bitmap-first, graph source pruning and
//! exact rerank, plus an estimated cost.
//!
//! The optimizer is exposed to SQL as `cbo_explain(...)`; setting
//! [`CostModel::enabled`] to `false` falls back to the fixed B3-3 strategy.

use std::collections::HashMap;
use std::fmt::{self, Formatter};
use std::sync::{Arc, RwLock};

use arrow::array::BooleanArray;
use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableFunctionArgs, TableFunctionImpl};
use datafusion::common::stats::{ColumnStatistics, Precision, Statistics};
use datafusion::common::ScalarValue;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::{DataSource, DataSourceExec};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::TableType;
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
};
use gtv_catalog::{Scalar, TableStats};
use gtv_core::{TemporalCSR, VectorIndex};
use gtv_index::{allowed_count, plan_ann, AnnConfig, AnnStrategy, AnyIndex, IndexType};

use crate::ann::{build_mask, parse_ann_args, IndexRegistry};

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------

/// Graph shape statistics from the temporal CSR.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct GraphStats {
    pub node_count: u64,
    pub edge_count: u64,
    pub max_degree: u64,
    /// `hist[k]` = nodes with `floor(log2(degree)) == k`.
    pub degree_histogram: Vec<u64>,
    /// Fraction of edges active at the planning instant (1.0 = all).
    pub temporal_active_ratio: f64,
}

impl GraphStats {
    pub fn from_csr(csr: &TemporalCSR) -> Self {
        let s = csr.stats();
        Self {
            node_count: s.node_count as u64,
            edge_count: s.edge_count as u64,
            max_degree: s.max_degree as u64,
            degree_histogram: s.degree_histogram,
            temporal_active_ratio: 1.0,
        }
    }

    pub fn with_active_ratio(mut self, ratio: f64) -> Self {
        self.temporal_active_ratio = ratio.clamp(0.0, 1.0);
        self
    }

    pub fn mean_degree(&self) -> f64 {
        if self.node_count == 0 {
            0.0
        } else {
            self.edge_count as f64 / self.node_count as f64
        }
    }
}

/// One point of an ANN recall/cost curve (B3-4 `tune_ivf_curve` output).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RecallLatencyPoint {
    /// IVF `nprobe` (or HNSW `ef`) for this point.
    pub nprobe: usize,
    /// Measured Recall@K against the exact oracle.
    pub recall: f64,
    /// Mean candidate vectors scanned per query (cost proxy).
    pub probed_rows: f64,
}

/// Vector corpus / index statistics derived live from an [`AnyIndex`].
#[derive(Debug, Clone, PartialEq)]
pub struct VectorStats {
    pub corpus_size: u64,
    pub dim: usize,
    pub index_type: IndexType,
    pub nlist: usize,
    pub nprobe: usize,
    pub cell_distribution: Vec<u32>,
    /// Measured recall/cost curve (empty until populated from B3-4 tuning).
    pub recall_latency_curve: Vec<RecallLatencyPoint>,
}

impl Default for VectorStats {
    fn default() -> Self {
        Self {
            corpus_size: 0,
            dim: 0,
            index_type: IndexType::Flat,
            nlist: 0,
            nprobe: 0,
            cell_distribution: Vec::new(),
            recall_latency_curve: Vec::new(),
        }
    }
}

impl VectorStats {
    pub fn from_index(index: &AnyIndex) -> Self {
        let (nlist, nprobe, cell_distribution) = match index {
            AnyIndex::Ivf(ivf) => (ivf.nlist(), ivf.nprobe(), ivf.cell_stats().counts),
            _ => (0, 0, Vec::new()),
        };
        Self {
            corpus_size: index.len() as u64,
            dim: index.dim(),
            index_type: index.index_type(),
            nlist,
            nprobe,
            cell_distribution,
            recall_latency_curve: Vec::new(),
        }
    }

    /// Attach a B3-4 recall/cost curve so the cost model can check whether the
    /// recommended IVF `nprobe` actually meets the recall target.
    pub fn with_recall_curve(mut self, curve: Vec<RecallLatencyPoint>) -> Self {
        self.recall_latency_curve = curve;
        self
    }

    /// IVF probe fraction used by the cost model.
    pub fn probe_fraction(&self) -> f64 {
        if self.nlist == 0 {
            1.0
        } else {
            (self.nprobe.max(1) as f64 / self.nlist as f64).clamp(0.0, 1.0)
        }
    }

    /// Estimated Recall@K for the current `nprobe`, or `None` when no curve is
    /// available (treat as unknown / trust the index).
    pub fn estimated_recall(&self) -> Option<f64> {
        if self.recall_latency_curve.is_empty() {
            return None;
        }
        // Nearest `nprobe` point (the curve is small and monotonically tuned).
        self.recall_latency_curve
            .iter()
            .min_by_key(|p| p.nprobe.abs_diff(self.nprobe.max(1)))
            .map(|p| p.recall)
    }
}

/// Filter selectivity for the current query.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SelectivityStats {
    pub allowed: u64,
    pub total: u64,
}

impl SelectivityStats {
    pub fn from_mask(index: &AnyIndex, mask: Option<&BooleanArray>) -> Self {
        Self {
            allowed: allowed_count(index.len(), mask) as u64,
            total: index.len() as u64,
        }
    }

    pub fn selectivity(&self) -> f64 {
        if self.total == 0 {
            1.0
        } else {
            self.allowed as f64 / self.total as f64
        }
    }
}

// ---------------------------------------------------------------------------
// DataFusion statistics provider
// ---------------------------------------------------------------------------

/// Map a catalog [`Scalar`] bound onto a typed DataFusion [`ScalarValue`],
/// guided by the column's Arrow type so `Int64` vs `Int32` etc. stay correct.
fn scalar_to_value(s: &Scalar, dt: &DataType) -> Option<ScalarValue> {
    use DataType::*;
    match (s, dt) {
        (Scalar::Bool(b), Boolean) => Some(ScalarValue::Boolean(Some(*b))),
        (Scalar::Int(i), Int8) => Some(ScalarValue::Int8(Some(*i as i8))),
        (Scalar::Int(i), Int16) => Some(ScalarValue::Int16(Some(*i as i16))),
        (Scalar::Int(i), Int32) => Some(ScalarValue::Int32(Some(*i as i32))),
        (Scalar::Int(i), Int64) => Some(ScalarValue::Int64(Some(*i))),
        (Scalar::Int(i), Date32) => Some(ScalarValue::Date32(Some(*i as i32))),
        (Scalar::Int(i), Date64) => Some(ScalarValue::Date64(Some(*i))),
        (Scalar::Int(i), Timestamp(TimeUnit::Nanosecond, tz)) => {
            Some(ScalarValue::TimestampNanosecond(Some(*i), tz.clone()))
        }
        (Scalar::Int(i), Timestamp(TimeUnit::Microsecond, tz)) => {
            Some(ScalarValue::TimestampMicrosecond(Some(*i), tz.clone()))
        }
        (Scalar::Int(i), Timestamp(TimeUnit::Millisecond, tz)) => {
            Some(ScalarValue::TimestampMillisecond(Some(*i), tz.clone()))
        }
        (Scalar::Int(i), Timestamp(TimeUnit::Second, tz)) => {
            Some(ScalarValue::TimestampSecond(Some(*i), tz.clone()))
        }
        (Scalar::UInt(u), UInt8) => Some(ScalarValue::UInt8(Some(*u as u8))),
        (Scalar::UInt(u), UInt16) => Some(ScalarValue::UInt16(Some(*u as u16))),
        (Scalar::UInt(u), UInt32) => Some(ScalarValue::UInt32(Some(*u as u32))),
        (Scalar::UInt(u), UInt64) => Some(ScalarValue::UInt64(Some(*u))),
        (Scalar::Float(f), Float32) => Some(ScalarValue::Float32(Some(*f as f32))),
        (Scalar::Float(f), Float64) => Some(ScalarValue::Float64(Some(*f))),
        (Scalar::Str(s), Utf8) => Some(ScalarValue::Utf8(Some(s.clone()))),
        (Scalar::Str(s), LargeUtf8) => Some(ScalarValue::LargeUtf8(Some(s.clone()))),
        _ => None,
    }
}

/// A [`TableProvider`] over in-memory batches that serves catalog [`TableStats`]
/// to DataFusion's planner and `EXPLAIN`.
///
/// This is the B3-5 "custom statistics provider" (design §6.3). Unlike a
/// [`MemTable`](datafusion::datasource::MemTable), whose scan recomputes
/// statistics from the actual batches, this provider attaches the *catalog*
/// estimates (row count, min/max, null count, distinct count) to the scan node,
/// so `EXPLAIN` reports the numbers Risk / AML actually planned against and
/// DataFusion's own optimizers can use them. Stats are read from the shared
/// [`CboRegistry`] at planning time, so a catalog commit +
/// `refresh_table_stats` is reflected immediately (never stale).
#[derive(Debug)]
pub struct StatsTable {
    name: String,
    schema: SchemaRef,
    partitions: Vec<Vec<RecordBatch>>,
    cbo: CboRegistry,
}

impl StatsTable {
    pub fn new(
        name: impl Into<String>,
        schema: SchemaRef,
        partitions: Vec<Vec<RecordBatch>>,
        cbo: CboRegistry,
    ) -> Self {
        Self {
            name: name.into(),
            schema,
            partitions,
            cbo,
        }
    }

    /// Build DataFusion statistics from the registered [`TableStats`], or
    /// `None` when no stats are registered for this table.
    pub fn statistics_estimate(&self) -> Option<Statistics> {
        let ts = {
            let cbo = self.cbo.read().ok()?;
            cbo.tables.get(&self.name)?.clone()
        };
        let mut columns = Vec::with_capacity(self.schema.fields().len());
        for field in self.schema.fields() {
            match ts.column(field.name()) {
                Some(cs) => {
                    let bound = |s: &Option<Scalar>| match s {
                        Some(v) => scalar_to_value(v, field.data_type())
                            .map(Precision::Exact)
                            .unwrap_or(Precision::Absent),
                        None => Precision::Absent,
                    };
                    columns.push(ColumnStatistics {
                        null_count: Precision::Exact(cs.null_count as usize),
                        max_value: bound(&cs.max),
                        min_value: bound(&cs.min),
                        sum_value: Precision::Absent,
                        distinct_count: cs
                            .distinct_est
                            .map(|d| Precision::Inexact(d as usize))
                            .unwrap_or(Precision::Absent),
                        byte_size: Precision::Absent,
                    });
                }
                None => columns.push(ColumnStatistics::new_unknown()),
            }
        }
        Some(Statistics {
            num_rows: Precision::Exact(ts.row_count as usize),
            total_byte_size: Precision::Absent,
            column_statistics: columns,
        })
    }
}

/// A [`DataSource`] that delegates IO to an inner source but reports the
/// catalog-derived [`Statistics`] (B3-5). This is what makes the provider's
/// estimates visible in `EXPLAIN` even though the underlying
/// [`MemorySourceConfig`] would otherwise recompute them from the batches.
#[derive(Debug)]
struct StatsDataSource {
    inner: Arc<dyn DataSource>,
    stats: Arc<Statistics>,
}

impl DataSource for StatsDataSource {
    fn open(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.inner.open(partition, context)
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        self.inner.fmt_as(t, f)
    }

    fn output_partitioning(&self) -> Partitioning {
        self.inner.output_partitioning()
    }

    fn eq_properties(&self) -> EquivalenceProperties {
        self.inner.eq_properties()
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Arc<Statistics>> {
        Ok(self.stats.clone())
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn DataSource>> {
        let inner = self.inner.with_fetch(limit)?;
        let stats = self
            .stats
            .as_ref()
            .clone()
            .with_fetch(limit, 0, 1)
            .ok()?;
        Some(Arc::new(StatsDataSource {
            inner,
            stats: Arc::new(stats),
        }))
    }

    fn fetch(&self) -> Option<usize> {
        self.inner.fetch()
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(
            &Arc<dyn PhysicalExpr>,
        ) -> Result<datafusion::common::tree_node::TreeNodeRecursion>,
    ) -> Result<datafusion::common::tree_node::TreeNodeRecursion> {
        self.inner.apply_expressions(f)
    }

    /// Projection pushdown would change the output schema while the catalog
    /// stats still describe the full schema, so decline the swap and let the
    /// projection stay above the scan.
    fn try_swapping_with_projection(
        &self,
        _projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn DataSource>>> {
        Ok(None)
    }
}

#[async_trait::async_trait]
impl TableProvider for StatsTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn statistics(&self) -> Option<Statistics> {
        self.statistics_estimate()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[datafusion::logical_expr::Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut source = MemorySourceConfig::try_new(
            &self.partitions,
            self.schema.clone(),
            projection.cloned(),
        )?;
        source = source.with_show_sizes(state.config_options().explain.show_sizes);
        let exec = match self.statistics_estimate() {
            Some(stats) => {
                // `scan` may push a projection into the source, narrowing the
                // output schema. DataFusion matches statistics entries to the
                // scan's *output* schema positionally, so project the
                // full-table catalog stats the same way to keep them aligned
                // (otherwise FilterExec's interval analysis goes out of
                // bounds).
                let stats = Arc::new(project_statistics(stats, projection));
                DataSourceExec::from_data_source(StatsDataSource {
                    inner: Arc::new(source),
                    stats,
                })
            }
            None => DataSourceExec::from_data_source(source),
        };
        Ok(exec)
    }
}

/// Project a full-table [`Statistics`] down to the source's output columns.
///
/// `None` (no projection pushdown) returns the statistics unchanged. The row
/// count and byte size are schema-independent and are preserved.
fn project_statistics(stats: Statistics, projection: Option<&Vec<usize>>) -> Statistics {
    let Some(projection) = projection else {
        return stats;
    };
    let column_statistics = projection
        .iter()
        .filter_map(|&idx| stats.column_statistics.get(idx).cloned())
        .collect();
    Statistics {
        num_rows: stats.num_rows,
        total_byte_size: stats.total_byte_size,
        column_statistics,
    }
}

// ---------------------------------------------------------------------------
// Cost model
// ---------------------------------------------------------------------------

/// Tunable weights. `enabled = false` reverts to the fixed B3-3 strategy.
#[derive(Debug, Clone, PartialEq)]
pub struct CostModel {
    pub enabled: bool,
    /// Corpora at or below this many vectors prefer an exact flat scan.
    pub flat_max_rows: u64,
    /// Selectivity below this prefers filtering before the ANN.
    pub prefilter_threshold: f64,
    /// Selectivity below this prefers IVF / filtered ANN.
    pub filtered_threshold: f64,
    pub flat_scan: f64,
    pub ivf_scan: f64,
    pub hnsw_scan: f64,
    pub per_row_filter: f64,
    pub rerank: f64,
    /// Temporal active ratio below this builds the bitmap first.
    pub temporal_bitmap_threshold: f64,
    /// Graph queries with a higher max degree prune source nodes first.
    pub high_degree_threshold: u64,
    /// When a recall curve is known, reject an IVF recommendation whose
    /// estimated recall falls below this and fall back to HNSW.
    pub recall_target: f64,
}

impl Default for CostModel {
    fn default() -> Self {
        Self {
            enabled: true,
            flat_max_rows: 10_000,
            prefilter_threshold: 0.01,
            filtered_threshold: 0.20,
            flat_scan: 1.0,
            ivf_scan: 0.05,
            hnsw_scan: 1.0,
            per_row_filter: 0.5,
            rerank: 2.0,
            temporal_bitmap_threshold: 0.20,
            high_degree_threshold: 1_000,
            recall_target: 0.90,
        }
    }
}

/// The optimizer's decision for one multimodal query.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryPlanChoice {
    pub strategy: AnnStrategy,
    /// Index type the cost model recommends (may differ from the live index).
    pub index_type: IndexType,
    pub filter_first: bool,
    pub temporal_bitmap_first: bool,
    pub prune_graph_sources: bool,
    pub exact_rerank: bool,
    pub estimated_rows: u64,
    pub estimated_cost: f64,
    pub selectivity: f64,
    pub reason: String,
}

/// Inputs for one planning decision (all optional except selectivity / k).
#[derive(Debug, Clone, Copy, Default)]
pub struct MultimodalQuery<'a> {
    pub table: Option<&'a TableStats>,
    pub graph: Option<&'a GraphStats>,
    pub vector: Option<&'a VectorStats>,
    pub selectivity: f64,
    pub k: usize,
    /// The query has a temporal predicate.
    pub has_temporal: bool,
    /// The query touches the graph.
    pub is_graph_query: bool,
}

/// Choose the execution strategy + estimate its cost.
pub fn plan_multimodal(
    q: &MultimodalQuery,
    ann_cfg: &AnnConfig,
    model: &CostModel,
) -> QueryPlanChoice {
    let sel = q.selectivity.clamp(0.0, 1.0);
    let corpus = q
        .vector
        .map(|v| v.corpus_size)
        .or_else(|| q.table.map(|t| t.row_count))
        .unwrap_or(0);
    let live_type = q.vector.map(|v| v.index_type).unwrap_or(IndexType::Flat);

    // Recommended index type from the corpus size and selectivity.
    let mut recommended = if corpus <= model.flat_max_rows {
        IndexType::Flat
    } else if sel < model.filtered_threshold {
        IndexType::Ivf
    } else {
        IndexType::Hnsw
    };
    // B3-4 integration: an IVF plan only survives if the measured recall curve
    // says the current `nprobe` can hit the target; otherwise prefer HNSW.
    if recommended == IndexType::Ivf {
        if let Some(recall) = q.vector.and_then(|v| v.estimated_recall()) {
            if recall + 1e-9 < model.recall_target {
                recommended = IndexType::Hnsw;
            }
        }
    }
    let index_type = if model.enabled { recommended } else { live_type };

    let filter_first = if model.enabled {
        sel < model.prefilter_threshold || corpus <= model.flat_max_rows
    } else {
        sel < ann_cfg.prefilter_threshold
    };

    let active_ratio = q
        .graph
        .map(|g| g.temporal_active_ratio)
        .or_else(|| q.table.map(|t| t.temporal_active_ratio(0)))
        .unwrap_or(1.0);
    let temporal_bitmap_first = q.has_temporal && active_ratio < model.temporal_bitmap_threshold;

    let max_degree = q.graph.map(|g| g.max_degree).unwrap_or(0);
    let prune_graph_sources = q.is_graph_query && max_degree > model.high_degree_threshold;

    let strategy = plan_ann(index_type, sel, q.k, ann_cfg).strategy;
    let exact_rerank = !matches!(
        strategy,
        AnnStrategy::Exact | AnnStrategy::PreFilterExact
    );

    let allowed = (corpus as f64 * sel).round() as u64;
    let filter_cost = corpus as f64 * model.per_row_filter;
    let ann_cost = match index_type {
        IndexType::Flat => corpus as f64 * model.flat_scan,
        IndexType::Ivf => {
            let probe = q.vector.map(|v| v.probe_fraction()).unwrap_or(0.1);
            corpus as f64 * model.ivf_scan * probe.max(1e-3)
        }
        IndexType::Hnsw => {
            let ef = (1.0 / sel.max(0.01)).min(64.0);
            (corpus as f64).max(1.0).ln() * model.hnsw_scan * ef
        }
    };
    let rerank_cost = q.k as f64 * model.rerank;
    let graph_prune_cost = if prune_graph_sources {
        max_degree as f64 * 0.1
    } else {
        0.0
    };
    let bitmap_cost = if temporal_bitmap_first {
        corpus as f64 * 0.05
    } else {
        0.0
    };
    let estimated_cost = if filter_first {
        filter_cost + allowed as f64 * model.flat_scan + graph_prune_cost + bitmap_cost
    } else {
        ann_cost + rerank_cost + graph_prune_cost + bitmap_cost
    };

    let reason = if !model.enabled {
        format!("optimizer disabled — fixed {} strategy {strategy:?}", live_type.as_str())
    } else {
        format!(
            "corpus={corpus}, selectivity={sel:.4} -> {} / {:?}{}{}{}{}",
            index_type.as_str(),
            strategy,
            if filter_first { ", filter-first" } else { ", ann-first" },
            if temporal_bitmap_first { ", temporal-bitmap" } else { "" },
            if prune_graph_sources { ", prune-sources" } else { "" },
            match q.vector.and_then(|v| v.estimated_recall()) {
                Some(r) => format!(", recall={r:.3}"),
                None => String::new(),
            },
        )
    };

    QueryPlanChoice {
        strategy,
        index_type,
        filter_first,
        temporal_bitmap_first,
        prune_graph_sources,
        exact_rerank,
        estimated_rows: allowed,
        estimated_cost,
        selectivity: sel,
        reason,
    }
}

// ---------------------------------------------------------------------------
// Engine state + SQL surface
// ---------------------------------------------------------------------------

/// Shared optimizer state (cost model + registered stats).
#[derive(Debug, Clone)]
pub struct CboState {
    pub model: CostModel,
    pub tables: HashMap<String, TableStats>,
    pub graphs: HashMap<String, GraphStats>,
}

impl Default for CboState {
    fn default() -> Self {
        Self {
            model: CostModel::default(),
            tables: HashMap::new(),
            graphs: HashMap::new(),
        }
    }
}

/// Shared handle to the optimizer state.
pub type CboRegistry = Arc<RwLock<CboState>>;

/// `cbo_explain(name, query, k [, metric [, filter [, strategy]]])` — one row
/// describing the chosen multimodal plan and its estimated cost.
#[derive(Debug)]
pub struct CboExplainTableFunction {
    indexes: IndexRegistry,
    cbo: CboRegistry,
}

impl CboExplainTableFunction {
    pub fn new(indexes: IndexRegistry, cbo: CboRegistry) -> Self {
        Self { indexes, cbo }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("table", DataType::Utf8, false),
            Field::new("enabled", DataType::Boolean, false),
            Field::new("strategy", DataType::Utf8, false),
            Field::new("index_type", DataType::Utf8, false),
            Field::new("filter_first", DataType::Boolean, false),
            Field::new("temporal_bitmap_first", DataType::Boolean, false),
            Field::new("prune_graph_sources", DataType::Boolean, false),
            Field::new("exact_rerank", DataType::Boolean, false),
            Field::new("estimated_rows", DataType::UInt64, false),
            Field::new("estimated_cost", DataType::Float64, false),
            Field::new("selectivity", DataType::Float64, false),
            Field::new("corpus_size", DataType::UInt64, false),
            Field::new("max_degree", DataType::Int64, false),
            Field::new("reason", DataType::Utf8, false),
        ]))
    }
}

impl TableFunctionImpl for CboExplainTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let exprs = args.exprs();
        let parsed = parse_ann_args(
            exprs,
            "cbo_explain(name, query, k [, metric [, filter [, strategy]]])",
        )?;
        let name = parsed.name.clone();

        let indexes = self
            .indexes
            .read()
            .map_err(|_| DataFusionError::Execution("cbo_explain: registry poisoned".into()))?;
        let index = indexes.get(&name).ok_or_else(|| {
            DataFusionError::Execution(format!("cbo_explain: unknown index `{name}`"))
        })?;
        let mask = build_mask(index, parsed.allowed.as_ref());
        let vector = VectorStats::from_index(index);
        let sel = SelectivityStats::from_mask(index, mask.as_ref());

        let cbo = self
            .cbo
            .read()
            .map_err(|_| DataFusionError::Execution("cbo_explain: optimizer poisoned".into()))?;
        let table = cbo.tables.get(&name);
        let graph = cbo.graphs.get(&name);

        let q = MultimodalQuery {
            table,
            graph,
            vector: Some(&vector),
            selectivity: sel.selectivity(),
            k: parsed.k,
            has_temporal: table
                .map(|t| t.column("event_time").is_some() || t.column("valid_from").is_some())
                .unwrap_or(false),
            is_graph_query: graph.is_some(),
        };
        let choice = plan_multimodal(&q, &parsed.cfg, &cbo.model);

        let schema = Self::schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![name.as_str()])) as ArrayRef,
                Arc::new(arrow::array::BooleanArray::from(vec![cbo.model.enabled])) as ArrayRef,
                Arc::new(StringArray::from(vec![choice.strategy.as_str()])) as ArrayRef,
                Arc::new(StringArray::from(vec![choice.index_type.as_str()])) as ArrayRef,
                Arc::new(arrow::array::BooleanArray::from(vec![choice.filter_first])) as ArrayRef,
                Arc::new(arrow::array::BooleanArray::from(vec![
                    choice.temporal_bitmap_first
                ])) as ArrayRef,
                Arc::new(arrow::array::BooleanArray::from(vec![
                    choice.prune_graph_sources
                ])) as ArrayRef,
                Arc::new(arrow::array::BooleanArray::from(vec![choice.exact_rerank])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![choice.estimated_rows])) as ArrayRef,
                Arc::new(Float64Array::from(vec![choice.estimated_cost])) as ArrayRef,
                Arc::new(Float64Array::from(vec![choice.selectivity])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![vector.corpus_size])) as ArrayRef,
                Arc::new(Int64Array::from(vec![graph
                    .map(|g| g.max_degree as i64)
                    .unwrap_or(-1)])) as ArrayRef,
                Arc::new(StringArray::from(vec![choice.reason.clone()])) as ArrayRef,
            ],
        )?;
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gtv_index::IvfIndex;

    fn vector(index_type: IndexType, corpus: u64, nlist: usize, nprobe: usize) -> VectorStats {
        VectorStats {
            corpus_size: corpus,
            dim: 8,
            index_type,
            nlist,
            nprobe,
            cell_distribution: vec![1; nlist],
            recall_latency_curve: Vec::new(),
        }
    }

    fn plan(
        vector: &VectorStats,
        graph: Option<&GraphStats>,
        sel: f64,
        k: usize,
        has_temporal: bool,
    ) -> QueryPlanChoice {
        let q = MultimodalQuery {
            table: None,
            graph,
            vector: Some(vector),
            selectivity: sel,
            k,
            has_temporal,
            is_graph_query: graph.is_some(),
        };
        plan_multimodal(&q, &AnnConfig::default(), &CostModel::default())
    }

    #[test]
    fn tiny_corpus_prefers_flat_and_filter_first() {
        let v = vector(IndexType::Flat, 500, 0, 0);
        let choice = plan(&v, None, 0.5, 10, false);
        assert_eq!(choice.index_type, IndexType::Flat);
        assert!(choice.filter_first);
        assert!(matches!(
            choice.strategy,
            AnnStrategy::PreFilterExact | AnnStrategy::Exact
        ));
        assert!(choice.estimated_cost > 0.0);
    }

    #[test]
    fn selective_filter_large_corpus_is_filter_first_ivf() {
        let v = vector(IndexType::Hnsw, 1_000_000, 0, 0);
        let choice = plan(&v, None, 0.001, 10, false);
        assert_eq!(choice.index_type, IndexType::Ivf);
        assert!(choice.filter_first);
        assert_eq!(choice.strategy, AnnStrategy::PreFilterExact);
        assert!(choice.estimated_rows <= 10_000);
    }

    #[test]
    fn high_selectivity_large_corpus_prefers_hnsw() {
        let v = vector(IndexType::Flat, 1_000_000, 0, 0);
        let choice = plan(&v, None, 0.8, 10, false);
        assert_eq!(choice.index_type, IndexType::Hnsw);
        assert!(!choice.filter_first);
        assert_eq!(choice.strategy, AnnStrategy::OversampledHnsw);
        assert!(choice.exact_rerank);
    }

    #[test]
    fn temporal_and_graph_hints_are_applied() {
        let v = vector(IndexType::Hnsw, 1_000_000, 0, 0);
        let g = GraphStats {
            node_count: 1_000,
            edge_count: 500_000,
            max_degree: 50_000,
            degree_histogram: vec![1, 2, 3],
            temporal_active_ratio: 0.05,
        };
        let choice = plan(&v, Some(&g), 0.3, 10, true);
        assert!(choice.temporal_bitmap_first);
        assert!(choice.prune_graph_sources);
        assert!(choice.estimated_cost > 0.0);
    }

    #[test]
    fn disabled_optimizer_falls_back_to_live_index() {
        let v = vector(IndexType::Hnsw, 1_000_000, 0, 0);
        let q = MultimodalQuery {
            table: None,
            graph: None,
            vector: Some(&v),
            selectivity: 0.5,
            k: 10,
            has_temporal: false,
            is_graph_query: false,
        };
        let mut model = CostModel::default();
        model.enabled = false;
        let choice = plan_multimodal(&q, &AnnConfig::default(), &model);
        assert_eq!(choice.index_type, IndexType::Hnsw);
        assert!(choice.reason.contains("optimizer disabled"));
    }

    #[test]
    fn adaptive_choice_is_cheaper_than_the_fixed_strategy() {
        // A million-row corpus stored as a Flat index: without the CBO the
        // engine would full-scan it, while the optimizer recommends HNSW.
        let v = vector(IndexType::Flat, 1_000_000, 0, 0);
        let q = MultimodalQuery {
            table: None,
            graph: None,
            vector: Some(&v),
            selectivity: 0.5,
            k: 10,
            has_temporal: false,
            is_graph_query: false,
        };
        let adaptive = plan_multimodal(&q, &AnnConfig::default(), &CostModel::default());
        let mut fixed_model = CostModel::default();
        fixed_model.enabled = false;
        let fixed = plan_multimodal(&q, &AnnConfig::default(), &fixed_model);
        assert_eq!(adaptive.index_type, IndexType::Hnsw);
        assert_eq!(fixed.index_type, IndexType::Flat);
        assert!(
            adaptive.estimated_cost < fixed.estimated_cost,
            "adaptive {} !< fixed {}",
            adaptive.estimated_cost,
            fixed.estimated_cost
        );
    }

    #[test]
    fn ivf_cell_stats_feed_vector_stats() {
        let ids: Vec<u64> = (0..64).collect();
        let data: Vec<f32> = (0..64).map(|i| i as f32).collect();
        let ivf = IvfIndex::with_metric(ids, data, 1, 8, 2, gtv_core::Metric::L2).unwrap();
        let index = AnyIndex::Ivf(ivf);
        let vs = VectorStats::from_index(&index);
        assert_eq!(vs.corpus_size, 64);
        assert_eq!(vs.nlist, 8);
        assert_eq!(vs.cell_distribution.len(), 8);
        assert_eq!(vs.index_type, IndexType::Ivf);
    }

    #[test]
    fn recall_curve_downgrades_unreliable_ivf_to_hnsw() {
        // A tuned curve showing nprobe=5 only reaches 0.5 recall must stop the
        // optimizer from recommending IVF for a recall-sensitive query.
        let v = vector(IndexType::Ivf, 1_000_000, 100, 5).with_recall_curve(vec![
            RecallLatencyPoint {
                nprobe: 5,
                recall: 0.5,
                probed_rows: 1_000.0,
            },
        ]);
        let choice = plan(&v, None, 0.05, 10, false);
        assert_eq!(choice.index_type, IndexType::Hnsw);
        assert!(choice.reason.contains("recall=0.500"), "{}", choice.reason);

        // The same plan stays IVF when the curve meets the target.
        let good = vector(IndexType::Ivf, 1_000_000, 100, 5).with_recall_curve(vec![
            RecallLatencyPoint {
                nprobe: 5,
                recall: 0.99,
                probed_rows: 1_000.0,
            },
        ]);
        assert_eq!(plan(&good, None, 0.05, 10, false).index_type, IndexType::Ivf);
    }
}
