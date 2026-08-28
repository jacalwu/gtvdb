//! kdb+-style vectorized array primitives over Arrow columns.
//!
//! The pure math lives on raw numeric slices for the in-memory MVP; Arrow-typed
//! wrappers are provided so the crate can be surfaced as DataFusion UDFs in
//! Phase 2.

pub mod asof;
pub mod window;
