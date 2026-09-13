//! Standard embedding schema and governance (B2-4).
//!
//! Embeddings are not just `Vec<Vec<f32>>`: every vector carries the model,
//! dimension, metric and provenance that produced it. This module defines the
//! canonical Arrow schema, validates a batch against it (dimension consistency,
//! a single model/metric per index, non-empty source hashes, sane effective
//! windows) and extracts vectors plus provenance for the search layer.
//!
//! Lifecycle governance lives here too: [`filter_active`] drops rows whose
//! `effective_to` has passed and enforces `tenant_id` isolation before the
//! vectors ever reach an index.

use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, FixedSizeListArray, Float32Array, Int64Array, RecordBatch, StringArray,
    UInt32Array, UInt64Array,
};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::error::{CatalogError, Result};

/// Name of the embedding vector column.
pub const EMBEDDING_COLUMN: &str = "embedding";

/// Distance metrics accepted by [`validate_embedding_batch`].
pub const METRICS: [&str; 3] = ["l2", "cosine", "dot"];

fn msg(m: impl Into<String>) -> CatalogError {
    CatalogError::Msg(m.into())
}

fn column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    let arr = batch
        .column_by_name(name)
        .ok_or_else(|| msg(format!("embedding: missing column `{name}`")))?;
    arr.as_any().downcast_ref::<T>().ok_or_else(|| {
        msg(format!(
            "embedding: column `{name}` has type {}, expected {}",
            arr.data_type(),
            std::any::type_name::<T>()
        ))
    })
}

/// Canonical embedding schema for vectors of `dim` floats.
pub fn embedding_schema(dim: u32) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("entity_id", DataType::UInt64, false),
        Field::new(
            EMBEDDING_COLUMN,
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dim as i32,
            ),
            false,
        ),
        Field::new("model_id", DataType::Utf8, false),
        Field::new("model_version", DataType::Utf8, false),
        Field::new("tokenizer_version", DataType::Utf8, true),
        Field::new("dimension", DataType::UInt32, false),
        Field::new("distance_metric", DataType::Utf8, false),
        Field::new("normalized", DataType::Boolean, false),
        Field::new("created_at", DataType::Int64, false),
        Field::new("effective_from", DataType::Int64, false),
        Field::new("effective_to", DataType::Int64, true),
        Field::new("source_hash", DataType::Utf8, false),
        Field::new("feature_version", DataType::Utf8, false),
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("classification", DataType::Utf8, true),
    ]))
}

/// The dimension of a schema's embedding column, if it is a `FixedSizeList`.
pub fn schema_dimension(schema: &Schema) -> Option<u32> {
    match schema.field_with_name(EMBEDDING_COLUMN).ok()?.data_type() {
        DataType::FixedSizeList(_, len) => u32::try_from(*len).ok(),
        _ => None,
    }
}

/// Uniform governance fields shared by every row of a validated batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingGovernance {
    pub model_id: String,
    pub model_version: String,
    pub dimension: u32,
    pub distance_metric: String,
    pub normalized: bool,
    pub feature_version: String,
}

/// Per-row provenance returned with a retrieval hit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingProvenance {
    pub entity_id: u64,
    pub model_id: String,
    pub model_version: String,
    pub source_hash: String,
    pub feature_version: String,
    pub tenant_id: String,
}

/// Validate a batch against the canonical embedding contract.
///
/// Rejects: `FixedSizeList` length != `dimension` column, mixed
/// model/version/dimension/metric/normalized across rows, empty `source_hash`,
/// unknown metrics, and `effective_from > effective_to`.
pub fn validate_embedding_batch(batch: &RecordBatch) -> Result<EmbeddingGovernance> {
    let n = batch.num_rows();
    if n == 0 {
        return Err(msg("embedding: empty batch"));
    }

    let list = column::<FixedSizeListArray>(batch, EMBEDDING_COLUMN)?;
    let dims = column::<UInt32Array>(batch, "dimension")?;
    let model_id = column::<StringArray>(batch, "model_id")?;
    let model_version = column::<StringArray>(batch, "model_version")?;
    let metric = column::<StringArray>(batch, "distance_metric")?;
    let normalized = column::<arrow::array::BooleanArray>(batch, "normalized")?;
    let feature_version = column::<StringArray>(batch, "feature_version")?;
    let source_hash = column::<StringArray>(batch, "source_hash")?;
    let from = column::<Int64Array>(batch, "effective_from")?;
    let to = column::<Int64Array>(batch, "effective_to")?;

    let schema_dim = match list.data_type() {
        DataType::FixedSizeList(_, len) => *len,
        other => {
            return Err(msg(format!(
                "embedding: `{EMBEDDING_COLUMN}` must be FixedSizeList, got {other}"
            )))
        }
    };

    for i in 0..n {
        if list.is_null(i) {
            return Err(msg(format!("embedding: null vector at row {i}")));
        }
        let row_dim = dims.value(i);
        if row_dim as i32 != schema_dim {
            return Err(msg(format!(
                "embedding: row {i} dimension {row_dim} != FixedSizeList length {schema_dim}"
            )));
        }
        if model_id.value(i) != model_id.value(0) {
            return Err(msg(format!(
                "embedding: mixed model_id — row {i} is `{}`, first row is `{}`",
                model_id.value(i),
                model_id.value(0)
            )));
        }
        if model_version.value(i) != model_version.value(0) {
            return Err(msg(format!(
                "embedding: mixed model_version — row {i} is `{}`, first is `{}`",
                model_version.value(i),
                model_version.value(0)
            )));
        }
        if dims.value(i) != dims.value(0) {
            return Err(msg(format!(
                "embedding: mixed dimension — row {i} is {}, first is {}",
                dims.value(i),
                dims.value(0)
            )));
        }
        if metric.value(i) != metric.value(0) {
            return Err(msg(format!(
                "embedding: mixed distance_metric — row {i} is `{}`, first is `{}`",
                metric.value(i),
                metric.value(0)
            )));
        }
        if normalized.value(i) != normalized.value(0) {
            return Err(msg(format!(
                "embedding: mixed normalized flag at row {i}"
            )));
        }
        if feature_version.value(i) != feature_version.value(0) {
            return Err(msg(format!(
                "embedding: mixed feature_version — row {i} is `{}`, first is `{}`",
                feature_version.value(i),
                feature_version.value(0)
            )));
        }
        if source_hash.value(i).is_empty() {
            return Err(msg(format!("embedding: empty source_hash at row {i}")));
        }
        if !to.is_null(i) && from.value(i) > to.value(i) {
            return Err(msg(format!(
                "embedding: effective_from {} > effective_to {} at row {i}",
                from.value(i),
                to.value(i)
            )));
        }
    }

    let first_metric = metric.value(0).to_string();
    if !METRICS.contains(&first_metric.as_str()) {
        return Err(msg(format!(
            "embedding: unknown distance_metric `{first_metric}` (expected l2|cosine|dot)"
        )));
    }

    Ok(EmbeddingGovernance {
        model_id: model_id.value(0).to_string(),
        model_version: model_version.value(0).to_string(),
        dimension: dims.value(0),
        distance_metric: first_metric,
        normalized: normalized.value(0),
        feature_version: feature_version.value(0).to_string(),
    })
}

/// Extract `(entity_ids, vectors, provenance)` from a validated batch.
pub fn read_embeddings(
    batch: &RecordBatch,
) -> Result<(Vec<u64>, Vec<Vec<f32>>, Vec<EmbeddingProvenance>)> {
    let governance = validate_embedding_batch(batch)?;
    let ids = column::<UInt64Array>(batch, "entity_id")?;
    let list = column::<FixedSizeListArray>(batch, EMBEDDING_COLUMN)?;
    let dim = list.value_length() as usize;
    let model_id = column::<StringArray>(batch, "model_id")?;
    let model_version = column::<StringArray>(batch, "model_version")?;
    let source_hash = column::<StringArray>(batch, "source_hash")?;
    let feature_version = column::<StringArray>(batch, "feature_version")?;
    let tenant = column::<StringArray>(batch, "tenant_id")?;

    let mut out_ids = Vec::with_capacity(batch.num_rows());
    let mut out_vecs = Vec::with_capacity(batch.num_rows());
    let mut out_prov = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        let cell = list.value(i);
        let flat = cell
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| msg("embedding: FixedSizeList child is not Float32"))?;
        out_vecs.push((0..dim).map(|d| flat.value(d)).collect());
        out_ids.push(ids.value(i));
        out_prov.push(EmbeddingProvenance {
            entity_id: ids.value(i),
            model_id: model_id.value(i).to_string(),
            model_version: model_version.value(i).to_string(),
            source_hash: source_hash.value(i).to_string(),
            feature_version: feature_version.value(i).to_string(),
            tenant_id: tenant.value(i).to_string(),
        });
    }
    debug_assert_eq!(governance.dimension as usize, dim);
    Ok((out_ids, out_vecs, out_prov))
}

/// Keep only rows active at `as_of` (`effective_from <= as_of < effective_to`,
/// open-ended when `effective_to` is null) and, when `tenant` is given, owned
/// by that tenant. This is the single choke point for expiry + tenant isolation.
pub fn filter_active(batch: &RecordBatch, as_of: i64, tenant: Option<&str>) -> Result<RecordBatch> {
    let from = column::<Int64Array>(batch, "effective_from")?;
    let to = column::<Int64Array>(batch, "effective_to")?;
    let tenants = column::<StringArray>(batch, "tenant_id")?;

    let mut keep = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        let active = from.value(i) <= as_of && (to.is_null(i) || as_of < to.value(i));
        let tenant_ok = tenant.is_none_or(|t| tenants.value(i) == t);
        keep.push(active && tenant_ok);
    }
    Ok(filter_record_batch(batch, &BooleanArray::from(keep))?)
}

/// Convenience: the ids of rows active at `as_of` for `tenant`.
pub fn active_entity_ids(batch: &RecordBatch, as_of: i64, tenant: Option<&str>) -> Result<Vec<u64>> {
    let filtered = filter_active(batch, as_of, tenant)?;
    let ids = column::<UInt64Array>(&filtered, "entity_id")?;
    Ok((0..filtered.num_rows()).map(|i| ids.value(i)).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_has_fixed_size_list() {
        let s = embedding_schema(4);
        assert_eq!(schema_dimension(&s), Some(4));
        assert!(s.field_with_name("tenant_id").is_ok());
    }
}
