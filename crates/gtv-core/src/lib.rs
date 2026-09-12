//! gtv-core: in-memory data structures and extension traits for the
//! Temporal-Columnar Graph-Vector engine.
//!
//! Phase 1 provides the immutable in-memory Temporal-CSR index, Arrow-backed
//! node/edge tables, and the three core extension traits.

pub mod chunk;
pub mod csr;
pub mod error;
pub mod graph;
pub mod metric;
pub mod table;
pub mod temporal;
pub mod traits;
pub mod traversal;

pub use chunk::TemporalEdgeChunk;
pub use csr::{Neighbor, NeighborStrategy, Neighbors, TemporalCSR, TemporalCsrStats};
pub use error::{GtvError, Result};
pub use graph::TemporalGraph;
pub use metric::{DistanceMetric, Metric};
pub use table::{EdgeTable, NodeTable};
pub use traits::{CustomTemporalOperator, TemporalGraphIndex, VectorHit, VectorIndex};
pub use traversal::{
    BudgetTracker, CancelToken, EdgePredicate, KhopResult, TraversalBudget, TraversalStats,
    VisitedSet,
};
