//! `ann(name, query, k [, metric])` — vector search over registered persisted
//! indexes ([`gtv_index::AnyIndex`], loaded via [`gtv_index_store`]).
//!
//! This is the SQL surface for index-lifecycle-managed indexes (B2-2): unlike
//! `knn`, the collection may be a Flat / IVF / HNSW index loaded from a
//! `.gtvidx` snapshot rather than an in-memory brute-force collection.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use arrow::array::{ArrayRef, BooleanArray, Float64Array, RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result};
use gtv_core::{Metric, VectorIndex};
use gtv_index::{
    estimate_recall, execute_ann, plan_ann, AnnConfig, AnyIndex,
};

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

/// Parse a comma-separated query vector.
fn parse_query(query: &str) -> Result<Vec<f32>> {
    query
        .split(',')
        .map(|t| {
            t.trim()
                .parse::<f32>()
                .map_err(|_| DataFusionError::Execution(format!("ann: bad query `{t}`")))
        })
        .collect()
}

/// Parse the filter argument. `None`/`*`/empty means no filter; otherwise a
/// comma-separated id allow-list.
fn parse_filter(filter: Option<&str>) -> Option<HashSet<u64>> {
    let f = filter?.trim();
    if f.is_empty() || f == "*" {
        return None;
    }
    Some(
        f.split(',')
            .filter_map(|t| t.trim().parse::<u64>().ok())
            .collect(),
    )
}

/// Build the positional `BooleanArray` mask for an id allow-list.
pub(crate) fn build_mask(index: &AnyIndex, allowed: Option<&HashSet<u64>>) -> Option<BooleanArray> {
    let allowed = allowed?;
    let ids = index.ids();
    Some(BooleanArray::from(
        ids.iter().map(|id| allowed.contains(id)).collect::<Vec<_>>(),
    ))
}

/// Resolve the effective `AnnConfig` (strategy hint) for a query.
fn config_for(strategy_hint: Option<&str>) -> Result<AnnConfig> {
    let mut cfg = AnnConfig::default();
    if let Some(hint) = strategy_hint {
        match hint.trim().to_ascii_lowercase().as_str() {
            "" | "auto" | "adaptive" => {}
            "exact" | "force_exact" => cfg.force_exact = true,
            other => {
                return Err(DataFusionError::Execution(format!(
                    "ann: unknown strategy `{other}` (auto|exact)"
                )))
            }
        }
    }
    Ok(cfg)
}

/// The argument bundle shared by `ann`, `ann_explain` and `cbo_explain`.
pub(crate) struct AnnArgs {
    pub(crate) name: String,
    pub(crate) query: Vec<f32>,
    pub(crate) k: usize,
    pub(crate) requested_metric: Option<String>,
    pub(crate) allowed: Option<HashSet<u64>>,
    pub(crate) cfg: AnnConfig,
}

pub(crate) fn parse_ann_args(
    exprs: &[datafusion::logical_expr::Expr],
    usage: &str,
) -> Result<AnnArgs> {
    let name = expr_to_string(
        exprs
            .first()
            .ok_or_else(|| DataFusionError::Execution(format!("{usage}: missing `name`")))?,
    )?;
    let query = parse_query(&expr_to_string(exprs.get(1).ok_or_else(|| {
        DataFusionError::Execution(format!("{usage}: missing `query`"))
    })?)?)?;
    let k = expr_to_i64(exprs.get(2).ok_or_else(|| {
        DataFusionError::Execution(format!("{usage}: missing `k`"))
    })?)? as usize;
    let requested_metric = exprs.get(3).map(expr_to_string).transpose()?;
    let filter = exprs.get(4).map(expr_to_string).transpose()?;
    let strategy = exprs.get(5).map(expr_to_string).transpose()?;
    Ok(AnnArgs {
        name,
        query,
        k,
        requested_metric,
        allowed: parse_filter(filter.as_deref()),
        cfg: config_for(strategy.as_deref())?,
    })
}

/// Reject a caller metric that disagrees with the index metric.
fn check_metric(index: &AnyIndex, requested: Option<&str>, who: &str) -> Result<()> {
    if let Some(m) = requested {
        let metric = Metric::parse(m).ok_or_else(|| {
            DataFusionError::Execution(format!("{who}: unknown metric `{m}` (l2|cosine|dot)"))
        })?;
        if metric != index.metric() {
            return Err(DataFusionError::Execution(format!(
                "{who}: metric mismatch — index is {}, query asked for {}",
                index.metric().as_str(),
                metric.as_str()
            )));
        }
    }
    Ok(())
}

impl TableFunctionImpl for AnnTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let exprs = args.exprs();
        let parsed = parse_ann_args(exprs, "ann(name, query, k [, metric [, filter [, strategy]]])")?;
        let AnnArgs {
            name,
            query,
            k,
            requested_metric,
            allowed,
            cfg,
        } = parsed;

        let indexes = self
            .indexes
            .read()
            .map_err(|_| DataFusionError::Execution("ann: registry poisoned".into()))?;
        let index = indexes.get(&name).ok_or_else(|| {
            DataFusionError::Execution(format!("ann: unknown index `{name}`"))
        })?;
        check_metric(index, requested_metric.as_deref(), "ann")?;

        if query.len() != index.dim() {
            return Err(DataFusionError::Execution(format!(
                "ann: query dim {} != index dim {}",
                query.len(),
                index.dim()
            )));
        }

        let mask = build_mask(index, allowed.as_ref());
        let total = index.len().max(1);
        let selectivity = gtv_index::allowed_count(index.len(), mask.as_ref()) as f64 / total as f64;
        let plan = plan_ann(index.index_type(), selectivity, k, &cfg);
        let metric = index.metric();
        let (result, _tel) = execute_ann(index, &query, k, mask.as_ref(), &plan)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let ids: Vec<u64> = result.hits.iter().map(|h| h.id).collect();
        let dists: Vec<f64> = result
            .hits
            .iter()
            .map(|h| report_distance(metric, h.exact))
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

/// `ann_explain(name, query, k [, metric [, filter [, strategy]]])` — one row of
/// B3-3 telemetry for the chosen strategy (plus a sampled recall estimate).
#[derive(Debug)]
pub struct AnnExplainTableFunction {
    indexes: IndexRegistry,
}

impl AnnExplainTableFunction {
    pub fn new(indexes: IndexRegistry) -> Self {
        Self { indexes }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("strategy", DataType::Utf8, false),
            Field::new("oversample", DataType::Int64, false),
            Field::new("exact_rerank", DataType::Boolean, false),
            Field::new("total_count", DataType::UInt64, false),
            Field::new("allowed_count", DataType::UInt64, false),
            Field::new("selectivity", DataType::Float64, false),
            Field::new("candidate_count", DataType::UInt64, false),
            Field::new("filtered_count", DataType::UInt64, false),
            Field::new("recall_estimate", DataType::Float64, true),
            Field::new("filter_us", DataType::UInt64, false),
            Field::new("ann_us", DataType::UInt64, false),
            Field::new("rerank_us", DataType::UInt64, false),
            Field::new("reason", DataType::Utf8, false),
        ]))
    }
}

impl TableFunctionImpl for AnnExplainTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let exprs = args.exprs();
        let parsed = parse_ann_args(
            exprs,
            "ann_explain(name, query, k [, metric [, filter [, strategy]]])",
        )?;
        let AnnArgs {
            name,
            query,
            k,
            requested_metric,
            allowed,
            cfg,
        } = parsed;

        let indexes = self
            .indexes
            .read()
            .map_err(|_| DataFusionError::Execution("ann_explain: registry poisoned".into()))?;
        let index = indexes.get(&name).ok_or_else(|| {
            DataFusionError::Execution(format!("ann_explain: unknown index `{name}`"))
        })?;
        check_metric(index, requested_metric.as_deref(), "ann_explain")?;
        let mask = build_mask(index, allowed.as_ref());
        let total = index.len();
        let allowed_count = gtv_index::allowed_count(total, mask.as_ref());
        let selectivity = allowed_count as f64 / total.max(1) as f64;
        let plan = plan_ann(index.index_type(), selectivity, k, &cfg);
        let (_, tel) = execute_ann(index, &query, k, mask.as_ref(), &plan)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        // Cheap self-recall: sample up to 16 corpus vectors as queries.
        let ids = index.ids();
        let step = (ids.len() / 16).max(1);
        let queries: Vec<Vec<f32>> = ids
            .iter()
            .step_by(step)
            .take(16)
            .filter_map(|id| index.vector_for_id(*id).map(|v| v.to_vec()))
            .collect();
        let recall = estimate_recall(index, &queries, k, mask.as_ref(), &plan)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        let schema = Self::schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![tel.strategy.as_str().to_string()])) as ArrayRef,
                Arc::new(arrow::array::Int64Array::from(vec![tel.oversample as i64])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![plan.exact_rerank])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![total as u64])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![allowed_count as u64])) as ArrayRef,
                Arc::new(Float64Array::from(vec![selectivity])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![tel.candidate_count])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![tel.filtered_count])) as ArrayRef,
                Arc::new(Float64Array::from(vec![Some(recall)])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![tel.latency.filter_us])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![tel.latency.ann_us])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![tel.latency.rerank_us])) as ArrayRef,
                Arc::new(StringArray::from(vec![tel.reason.clone()])) as ArrayRef,
            ],
        )?;
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}
