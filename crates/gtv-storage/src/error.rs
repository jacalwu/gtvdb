//! Storage-layer error type (kept separate so gtv-core stays free of parquet).

use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("{0}")]
    Msg(String),
}

pub type Result<T> = std::result::Result<T, StorageError>;
