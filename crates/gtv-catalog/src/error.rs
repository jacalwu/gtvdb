//! Catalog error type.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("storage error: {0}")]
    Storage(#[from] gtv_storage::StorageError),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("table not found: {0}")]
    TableNotFound(String),

    #[error("snapshot not found: {0}")]
    SnapshotNotFound(String),

    #[error("table already exists: {0}")]
    TableExists(String),

    #[error("schema incompatible: {0}")]
    SchemaIncompatible(String),

    #[error("corrupt catalog: {0}")]
    Corrupt(String),

    #[error("{0}")]
    Msg(String),
}

pub type Result<T> = std::result::Result<T, CatalogError>;
