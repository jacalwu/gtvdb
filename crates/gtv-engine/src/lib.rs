//! gtv-engine: DataFusion integration for the gtv engine.
//!
//! Phase 2 registers the gtv array primitives and graph traversal as
//! DataFusion UDFs / table functions so they can be driven from SQL.

pub mod context;
pub mod graph;
pub mod udf;

pub use context::GtvContext;
