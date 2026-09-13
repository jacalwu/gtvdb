//! Governed embedding search (B2-4).
//!
//! [`EmbeddingCollection`] is built from a standard embedding batch
//! (`gtv-catalog` schema). Unlike the bare `knn` collection it keeps per-vector
//! provenance and carries the `effective_from/to` window and `tenant_id`, so
//! every hit can report its model/source/feature version and searches never
//! cross tenants or return expired vectors.
//!
//! Exposed to SQL as `embedding_search(name, query, k [, tenant [, as_of]])`
//! returning `(id, distance, model_id, model_version, source_hash,
//! feature_version)`.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use arrow::array::{
    Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result};
use gtv_catalog::{EmbeddingGovernance, EmbeddingProvenance};
use gtv_core::Metric;

use crate::expr_util::{expr_to_i64, expr_to_string};

/// Shared registry of governed embedding collections, keyed by name.
pub type EmbeddingRegistry = Arc<RwLock<HashMap<String, EmbeddingCollection>>>;

/// One retrieval hit with its provenance.
#[derive(Debug, Clone, PartialEq)]
pub struct EmbeddingHit {
    pub id: u64,
    pub distance: f64,
    pub provenance: EmbeddingProvenance,
}

/// A governed set of embedding vectors plus per-row lifecycle metadata.
#[derive(Debug, Clone)]
pub struct EmbeddingCollection {
    ids: Vec<u64>,
    vectors: Vec<Vec<f32>>,
    provenance: Vec<EmbeddingProvenance>,
    effective_from: Vec<i64>,
    effective_to: Vec<Option<i64>>,
    metric: Metric,
    governance: EmbeddingGovernance,
}

fn i64_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Int64Array> {
    batch
        .column_by_name(name)
        .and_then(|a| a.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| DataFusionError::Execution(format!("embedding: missing Int64 column `{name}`")))
}

impl EmbeddingCollection {
    /// Validate `batch` (catalog schema) and build a governed collection.
    /// The metric is taken from the batch's `distance_metric` column.
    pub fn from_batch(batch: &RecordBatch) -> Result<Self> {
        let (ids, vectors, provenance) = gtv_catalog::read_embeddings(batch)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let governance = gtv_catalog::validate_embedding_batch(batch)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let metric = Metric::parse(&governance.distance_metric).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "embedding: unknown metric `{}`",
                governance.distance_metric
            ))
        })?;

        let from = i64_column(batch, "effective_from")?;
        let to = i64_column(batch, "effective_to")?;
        let effective_from = (0..batch.num_rows()).map(|i| from.value(i)).collect();
        let effective_to = (0..batch.num_rows())
            .map(|i| if to.is_null(i) { None } else { Some(to.value(i)) })
            .collect();

        Ok(Self {
            ids,
            vectors,
            provenance,
            effective_from,
            effective_to,
            metric,
            governance,
        })
    }

    /// The metric every query against this collection must use.
    pub fn metric(&self) -> Metric {
        self.metric
    }

    /// The governance shared by the collection's vectors.
    pub fn governance(&self) -> &EmbeddingGovernance {
        &self.governance
    }

    pub fn len(&self) -> usize {
        self.ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    fn dim(&self) -> usize {
        self.vectors.first().map(Vec::len).unwrap_or(0)
    }

    /// Nearest neighbours at `as_of`, restricted to `tenant` when given.
    /// Rows whose `effective_to` has passed are never returned.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        tenant: Option<&str>,
        as_of: i64,
    ) -> Vec<EmbeddingHit> {
        let mut scored: Vec<(f32, usize)> = self
            .vectors
            .iter()
            .enumerate()
            .filter(|(i, _)| {
                let active = self.effective_from[*i] <= as_of
                    && self
                        .effective_to[*i]
                        .is_none_or(|to| as_of < to);
                let tenant_ok =
                    tenant.is_none_or(|t| self.provenance[*i].tenant_id == t);
                active && tenant_ok
            })
            .map(|(i, v)| (self.metric.distance(query, v), i))
            .collect();
        scored.sort_by(|a, b| {
            a.0.partial_cmp(&b.0)
                .unwrap_or(Ordering::Equal)
                .then_with(|| self.ids[a.1].cmp(&self.ids[b.1]))
        });
        scored.truncate(k);
        scored
            .into_iter()
            .map(|(d, i)| EmbeddingHit {
                id: self.ids[i],
                distance: report_distance(self.metric, d),
                provenance: self.provenance[i].clone(),
            })
            .collect()
    }
}

/// Report a distance the same way `knn`/`ann` do (L2 → Euclidean).
fn report_distance(metric: Metric, raw: f32) -> f64 {
    match metric {
        Metric::L2 => (raw.max(0.0) as f64).sqrt(),
        _ => raw as f64,
    }
}

/// `embedding_search` table function over governed collections.
#[derive(Debug)]
pub struct EmbeddingSearchTableFunction {
    collections: EmbeddingRegistry,
}

impl EmbeddingSearchTableFunction {
    pub fn new(collections: EmbeddingRegistry) -> Self {
        Self { collections }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("distance", DataType::Float64, false),
            Field::new("model_id", DataType::Utf8, false),
            Field::new("model_version", DataType::Utf8, false),
            Field::new("source_hash", DataType::Utf8, false),
            Field::new("feature_version", DataType::Utf8, false),
        ]))
    }
}

impl TableFunctionImpl for EmbeddingSearchTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let exprs = args.exprs();
        let name = expr_to_string(exprs.first().ok_or_else(|| {
            DataFusionError::Execution(
                "embedding_search(name, query, k [, tenant [, as_of]]): missing `name`".into(),
            )
        })?)?;
        let query = expr_to_string(exprs.get(1).ok_or_else(|| {
            DataFusionError::Execution(
                "embedding_search(name, query, k [, tenant [, as_of]]): missing `query`".into(),
            )
        })?)?;
        let k = expr_to_i64(exprs.get(2).ok_or_else(|| {
            DataFusionError::Execution(
                "embedding_search(name, query, k [, tenant [, as_of]]): missing `k`".into(),
            )
        })?)? as usize;
        let tenant = exprs.get(3).map(expr_to_string).transpose()?;
        let as_of = exprs
            .get(4)
            .map(expr_to_i64)
            .transpose()?
            .unwrap_or_else(gtv_catalog::schema::now_ns);

        // `'*'` = admin view across tenants; omitted = no tenant filter.
        let tenant = match tenant.as_deref() {
            Some("*") | None => None,
            Some(t) => Some(t),
        };

        let query: Vec<f32> = query
            .split(',')
            .map(|t| {
                t.trim().parse::<f32>().map_err(|_| {
                    DataFusionError::Execution(format!("embedding_search: bad query `{t}`"))
                })
            })
            .collect::<Result<_>>()?;

        let collections = self
            .collections
            .read()
            .map_err(|_| DataFusionError::Execution("embedding: registry poisoned".into()))?;
        let collection = collections.get(&name).ok_or_else(|| {
            DataFusionError::Execution(format!("embedding_search: unknown collection `{name}`"))
        })?;
        if query.len() != collection.dim() {
            return Err(DataFusionError::Execution(format!(
                "embedding_search: query dim {} != collection dim {}",
                query.len(),
                collection.dim()
            )));
        }

        let hits = collection.search(&query, k, tenant, as_of);
        let ids: Vec<u64> = hits.iter().map(|h| h.id).collect();
        let dists: Vec<f64> = hits.iter().map(|h| h.distance).collect();
        let model_ids: Vec<&str> = hits.iter().map(|h| h.provenance.model_id.as_str()).collect();
        let model_versions: Vec<&str> = hits
            .iter()
            .map(|h| h.provenance.model_version.as_str())
            .collect();
        let hashes: Vec<&str> = hits.iter().map(|h| h.provenance.source_hash.as_str()).collect();
        let features: Vec<&str> = hits
            .iter()
            .map(|h| h.provenance.feature_version.as_str())
            .collect();

        let batch = RecordBatch::try_new(
            Self::schema(),
            vec![
                Arc::new(UInt64Array::from(ids)) as ArrayRef,
                Arc::new(Float64Array::from(dists)) as ArrayRef,
                Arc::new(StringArray::from(model_ids)) as ArrayRef,
                Arc::new(StringArray::from(model_versions)) as ArrayRef,
                Arc::new(StringArray::from(hashes)) as ArrayRef,
                Arc::new(StringArray::from(features)) as ArrayRef,
            ],
        )?;
        Ok(Arc::new(MemTable::try_new(Self::schema(), vec![vec![batch]])?))
    }
}
