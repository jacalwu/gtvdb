//! Core extension traits for the engine.

use arrow::array::{BooleanArray, UInt64Array};
use arrow::record_batch::RecordBatch;

use crate::error::Result;
use crate::metric::Metric;

/// Index over a temporal graph: fetch the neighbors of `src_nodes` that are
/// active at a single point in time `valid_at`.
pub trait TemporalGraphIndex: Send + Sync {
    fn fetch_temporal_neighbors(
        &self,
        src_nodes: &UInt64Array,
        valid_at: i64,
    ) -> Result<RecordBatch>;
}

/// One ranked nearest-neighbor hit.
///
/// `distance` follows the index's [`Metric`] and is always "lower = closer"
/// (inner-product indexes return the negative inner product). The field is
/// `f32` to match the vector corpus precision.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VectorHit {
    pub id: u64,
    pub distance: f32,
}

/// Pluggable nearest-neighbor index.
///
/// The metric is fixed at build time; callers must not mix metrics within one
/// index. Implementations that normalize their rows (Cosine) also normalize the
/// query, so `search` is self-consistent.
pub trait VectorIndex: Send + Sync {
    /// The metric this index was built with.
    fn metric(&self) -> Metric;

    /// Vector dimension of the indexed corpus.
    fn dim(&self) -> usize;

    /// Ranked hits, nearest first. Exact or approximate depending on the
    /// implementation.
    fn search(
        &self,
        query: &[f32],
        k: usize,
        filter_mask: Option<&BooleanArray>,
    ) -> Result<Vec<VectorHit>>;

    /// Backward-compatible convenience returning ids only.
    fn search_knn(
        &self,
        query: &[f32],
        k: usize,
        filter_mask: Option<&BooleanArray>,
    ) -> Result<UInt64Array> {
        let hits = self.search(query, k, filter_mask)?;
        Ok(UInt64Array::from(
            hits.into_iter().map(|h| h.id).collect::<Vec<_>>(),
        ))
    }
}

/// A user-defined temporal operator that maps a RecordBatch to another.
pub trait CustomTemporalOperator: Send + Sync {
    fn name(&self) -> &str;
    fn eval_batch(&self, input: &RecordBatch) -> Result<RecordBatch>;
}
