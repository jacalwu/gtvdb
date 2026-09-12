//! Snapshot manifests: immutable data-file metadata and the table versions that
//! reference them.

use serde::{Deserialize, Serialize};

use crate::id::{CommitId, DataFileId, SnapshotId, TableId};
use crate::partition::PartitionValue;
use crate::schema::SchemaVersion;

/// A serde-friendly scalar for column min/max statistics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Scalar {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    Str(String),
}

/// Per-column statistics for a data file.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnStat {
    pub name: String,
    pub null_count: u64,
    pub min: Option<Scalar>,
    pub max: Option<Scalar>,
    #[serde(default)]
    pub distinct_est: Option<u64>,
}

/// The upstream position a file's rows came from (streaming / CDC).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceOffset {
    pub source: String,
    pub partition: i32,
    pub offset: i64,
}

/// On-disk encoding of a data file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileFormat {
    Parquet,
    /// External CSV reference (registered, never written by the catalog).
    Csv,
}

/// One immutable columnar data file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataFile {
    pub file_id: DataFileId,
    pub path: String,
    pub format: FileFormat,
    /// `true` when the catalog wrote the file; `false` for an external
    /// CSV/Parquet reference (checksum/row_count are then best-effort).
    #[serde(default = "default_managed")]
    pub managed: bool,
    pub row_count: u64,
    pub size_bytes: u64,
    pub column_stats: Vec<ColumnStat>,
    /// Event-time bounding box (ns); `i64::MIN/MAX` when unknown.
    pub event_time_min: i64,
    pub event_time_max: i64,
    pub schema_version: SchemaVersion,
    #[serde(default)]
    pub partition: Vec<PartitionValue>,
    /// Hex-encoded blake3 of the file bytes.
    pub checksum: String,
    #[serde(default)]
    pub source_offsets: Vec<SourceOffset>,
    pub commit_id: CommitId,
}

/// The kind of change a snapshot represents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommitOp {
    Append,
    Overwrite,
    Delete,
}

fn default_managed() -> bool {
    true
}

/// A committed, immutable table version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub snapshot_id: SnapshotId,
    pub parent: Option<SnapshotId>,
    pub table_id: TableId,
    pub schema_version: SchemaVersion,
    pub spec_version: u32,
    /// File ids referenced by this snapshot (resolved through the file index).
    pub files: Vec<DataFileId>,
    pub op: CommitOp,
    /// Free-form commit metadata (e.g. `idempotency_key`, source offsets).
    #[serde(default)]
    pub summary: serde_json::Value,
    pub created_at: i64,
}
