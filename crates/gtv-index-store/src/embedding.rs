//! Embedding → index integration (B2-4).
//!
//! An [`EmbeddingIndexSpec`] pins the governance an index was built under
//! (model, version, dimension, metric, normalization). Building from a standard
//! embedding batch goes through the catalog validation + active/tenant filter,
//! then refuses to proceed when the batch disagrees with the spec — this is the
//! "mixed model/dim/metric cannot be indexed" guard.

use arrow::record_batch::RecordBatch;
use gtv_catalog::{EmbeddingGovernance, SnapshotId, TableId};
use gtv_core::Metric;
use gtv_index::{AnyIndex, BuildOptions};

use crate::store::{IndexMeta, IndexStore, IndexVersion};
use crate::{IndexManifest, IndexStoreError};

/// Governance + build parameters for an embedding index.
#[derive(Debug, Clone)]
pub struct EmbeddingIndexSpec {
    pub name: String,
    pub table: Option<TableId>,
    pub corpus_snapshot_id: Option<SnapshotId>,
    pub model_id: String,
    pub model_version: String,
    pub feature_version: String,
    pub metric: Metric,
    pub dim: u32,
    pub normalized: bool,
    pub build_options: BuildOptions,
}

/// Why an embedding batch could not be turned into an index.
#[derive(Debug, thiserror::Error)]
pub enum EmbeddingIndexError {
    #[error("embedding dimension {data} != index spec dimension {spec}")]
    Dimension { spec: u32, data: u32 },

    #[error("embedding model `{data}` != index spec model `{spec}`")]
    Model { spec: String, data: String },

    #[error("embedding metric `{data}` != index spec metric `{spec}`")]
    Metric { spec: String, data: String },

    #[error("no active embeddings for the requested tenant/as-of window")]
    Empty,

    #[error(transparent)]
    Governance(#[from] gtv_catalog::CatalogError),

    #[error(transparent)]
    Index(#[from] gtv_core::GtvError),

    #[error(transparent)]
    Store(#[from] IndexStoreError),
}

/// Validate a batch against `spec` and build an index over the rows active at
/// `as_of` (restricted to `tenant` when given).
pub fn build_index_from_embeddings(
    spec: &EmbeddingIndexSpec,
    batch: &RecordBatch,
    as_of: i64,
    tenant: Option<&str>,
) -> Result<(AnyIndex, EmbeddingGovernance), EmbeddingIndexError> {
    let active = gtv_catalog::filter_active(batch, as_of, tenant)?;
    if active.num_rows() == 0 {
        return Err(EmbeddingIndexError::Empty);
    }
    let governance = gtv_catalog::validate_embedding_batch(&active)?;

    if governance.dimension != spec.dim {
        return Err(EmbeddingIndexError::Dimension {
            spec: spec.dim,
            data: governance.dimension,
        });
    }
    if governance.model_id != spec.model_id || governance.model_version != spec.model_version {
        return Err(EmbeddingIndexError::Model {
            spec: format!("{}@{}", spec.model_id, spec.model_version),
            data: format!("{}@{}", governance.model_id, governance.model_version),
        });
    }
    if governance.distance_metric != spec.metric.as_str() {
        return Err(EmbeddingIndexError::Metric {
            spec: spec.metric.as_str().to_string(),
            data: governance.distance_metric,
        });
    }

    let (ids, vectors, _) = gtv_catalog::read_embeddings(&active)?;
    let index = AnyIndex::build(ids, vectors, spec.metric, &spec.build_options)?;
    Ok((index, governance))
}

/// Build, persist and activate an embedding index, returning its version and
/// the manifest (which now carries the embedding governance).
pub fn build_and_save(
    store: &IndexStore,
    spec: &EmbeddingIndexSpec,
    batch: &RecordBatch,
    as_of: i64,
    tenant: Option<&str>,
) -> Result<(IndexVersion, IndexManifest), EmbeddingIndexError> {
    let (index, governance) = build_index_from_embeddings(spec, batch, as_of, tenant)?;
    let meta = IndexMeta {
        table_id: spec.table,
        corpus_snapshot_id: spec.corpus_snapshot_id,
        source_file_ids: Vec::new(),
        model_id: governance.model_id.clone(),
        model_version: governance.model_version.clone(),
        embedding_model: governance.model_id.clone(),
        feature_version: spec.feature_version.clone(),
        normalized: spec.normalized,
        tombstone_count: 0,
    };
    let version = store.save(&spec.name, &index, &spec.build_options, &meta)?;
    let manifest = store.manifest(&spec.name, Some(version.version))?;
    Ok((version, manifest))
}
