//! gtv-index: pluggable nearest-neighbor indexes implementing the
//! [`VectorIndex`](gtv_core::VectorIndex) trait, with Arrow bitmask
//! (temporal filter) pruning.
//!
//! - [`FlatIndex`] — exact brute-force K-NN (reference implementation).
//! - [`IvfIndex`] — inverted-file index (coarse partition + exact `f32` probe scan).
//! - [`HnswIndex`] — approximate Hierarchical Navigable Small World graph.

pub mod bytes;
pub mod flat;
pub mod hnsw;
pub mod index;
pub mod ivf;

pub use flat::FlatIndex;
pub use hnsw::HnswIndex;
pub use index::{AnyIndex, BuildOptions, IndexType, PersistableIndex};
pub use ivf::IvfIndex;
