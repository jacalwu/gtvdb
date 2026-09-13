//! Error type for the streaming ingestion layer.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum IngestError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("catalog error: {0}")]
    Catalog(#[from] gtv_catalog::CatalogError),

    #[error("storage error: {0}")]
    Storage(#[from] gtv_storage::StorageError),

    #[error("source `{name}` partition {partition} is at offset {offset} but the adapter only has {available} events")]
    OffsetOutOfRange {
        name: String,
        partition: i32,
        offset: i64,
        available: usize,
    },

    #[error("source `{0}` not found")]
    UnknownSource(String),

    #[error("{0}")]
    Msg(String),
}

pub type Result<T> = std::result::Result<T, IngestError>;
