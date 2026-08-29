//! gtv-delta: LSM-style dynamic delta buffer + compaction over a temporal
//! graph snapshot.
//!
//! The immutable [`TemporalGraph`] snapshot is the "SSTable" tier; the
//! [`DeltaBuffer`] is the mutable in-memory "memtable" of edge insertions and
//! deletions. [`compact`] folds the delta into a fresh snapshot; [`LsmStore`]
//! bundles both and can run that compaction on a background thread.

mod delta;
mod store;

pub use delta::{DeltaBuffer, DeltaEdge};
pub use store::LsmStore;

use gtv_core::{Result, TemporalGraph};

/// Fold a [`DeltaBuffer`] into a snapshot, returning a new [`TemporalGraph`]
/// that keeps the same node table but a rebuilt edge table + CSR.
pub fn compact(graph: &TemporalGraph, delta: &DeltaBuffer) -> Result<TemporalGraph> {
    let new_edges = delta.apply(graph.edges())?;
    TemporalGraph::new(graph.nodes().clone(), new_edges)
}
