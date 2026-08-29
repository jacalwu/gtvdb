//! gtv-core: in-memory data structures and extension traits for the
//! Temporal-Columnar Graph-Vector engine.
//!
//! Phase 1 provides the immutable in-memory Temporal-CSR index, Arrow-backed
//! node/edge tables, and the three core extension traits.

pub mod chunk;
pub mod csr;
pub mod error;
pub mod graph;
pub mod table;
pub mod temporal;
pub mod traits;

pub use chunk::TemporalEdgeChunk;
pub use csr::TemporalCSR;
pub use error::{GtvError, Result};
pub use graph::TemporalGraph;
pub use table::{EdgeTable, NodeTable};
pub use traits::{CustomTemporalOperator, TemporalGraphIndex, VectorIndex};
