//! gtv-index: pluggable nearest-neighbor indexes implementing the
//! [`VectorIndex`](gtv_core::VectorIndex) trait, with Arrow bitmask
//! (temporal filter) pruning.
//!
//! - [`FlatIndex`] — exact brute-force K-NN (reference implementation).
//! - [`HnswIndex`] — approximate Hierarchical Navigable Small World graph.

pub mod flat;
pub mod hnsw;

pub use flat::FlatIndex;
pub use hnsw::HnswIndex;
