//! gtv-engine: DataFusion integration for the gtv engine.
//!
//! Phase 2 registers the gtv array primitives and graph traversal as
//! DataFusion UDFs / table functions so they can be driven from SQL.

pub mod asof;
pub mod context;
pub mod csv;
pub mod graph;
pub mod hft;
pub mod hft_exec;
pub mod hft_tf;
pub mod knn;
pub mod micro;
pub mod udf;

mod expr_util;

pub use context::GtvContext;
