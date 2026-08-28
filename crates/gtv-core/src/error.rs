//! Error type shared across the gtv engine crates.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum GtvError {
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("schema error: {0}")]
    Schema(String),

    #[error("node id out of range: {0}")]
    NodeOutOfRange(u64),
}

pub type Result<T> = std::result::Result<T, GtvError>;
