//! gtv-index: pluggable nearest-neighbor indexes implementing the
//! [`VectorIndex`](gtv_core::VectorIndex) trait, with Arrow bitmask
//! (temporal filter) pruning.
//!
//! - [`FlatIndex`] — exact brute-force K-NN (reference implementation).
//! - [`IvfIndex`] — inverted-file index (coarse partition + exact `f32` probe scan).
//! - [`HnswIndex`] — approximate Hierarchical Navigable Small World graph.

pub mod ann;
pub mod bytes;
pub mod flat;
pub mod hnsw;
pub mod index;
pub mod ivf;

pub use ann::{
    allowed_count, estimate_recall, execute_ann, plan_ann, recall_curve, AnnConfig, AnnPlan,
    AnnStrategy, AnnTelemetry, LatencyBreakdown, RerankResult, RerankedHit,
};
pub use flat::FlatIndex;
pub use hnsw::HnswIndex;
pub use index::{AnyIndex, BuildOptions, IndexType, PersistableIndex};
pub use ivf::{
    kmeans_train, select_tuned, tune_ivf, tune_ivf_curve, CellStats, IvfIndex, IvfTrainingMeta,
    KMeansConfig, RetrainTrigger, TuneConfig, TuneResult,
};
