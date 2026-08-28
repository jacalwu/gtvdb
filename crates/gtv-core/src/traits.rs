//! Core extension traits for the engine.

use arrow::array::{BooleanArray, UInt64Array};
use arrow::record_batch::RecordBatch;

use crate::error::Result;

/// Index over a temporal graph: fetch the neighbors of `src_nodes` that are
/// active at a single point in time `valid_at`.
pub trait TemporalGraphIndex: Send + Sync {
    fn fetch_temporal_neighbors(
        &self,
        src_nodes: &UInt64Array,
        valid_at: i64,
    ) -> Result<RecordBatch>;
}

/// Pluggable approximate nearest-neighbor index.
pub trait VectorIndex: Send + Sync {
    fn search_knn(
        &self,
        query: &[f32],
        k: usize,
        filter_mask: Option<&BooleanArray>,
    ) -> Result<UInt64Array>;
}

/// A user-defined temporal operator that maps a RecordBatch to another.
pub trait CustomTemporalOperator: Send + Sync {
    fn name(&self) -> &str;
    fn eval_batch(&self, input: &RecordBatch) -> Result<RecordBatch>;
}
