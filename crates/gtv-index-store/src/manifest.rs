//! Index manifest: everything needed to identify, verify and rebuild an index.

use gtv_catalog::{DataFileId, IndexId, SnapshotId, TableId};
use gtv_index::{BuildOptions, IndexType};
use serde::{Deserialize, Serialize};

/// Metadata recorded alongside an index payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexManifest {
    pub index_id: IndexId,
    pub name: String,
    /// Authoritative table this index was built from (if any).
    #[serde(default)]
    pub table_id: Option<TableId>,
    /// Corpus snapshot the index was built from.
    #[serde(default)]
    pub corpus_snapshot_id: Option<SnapshotId>,
    #[serde(default)]
    pub source_file_ids: Vec<DataFileId>,
    pub index_type: IndexType,
    #[serde(default)]
    pub model_id: String,
    #[serde(default)]
    pub model_version: String,
    #[serde(default)]
    pub embedding_model: String,
    pub dim: u32,
    /// `l2` / `cosine` / `dot`.
    pub metric: String,
    pub build_options: BuildOptions,
    pub build_ts: i64,
    /// blake3 (hex) of the index payload.
    pub payload_checksum: String,
    pub engine_version: String,
    pub row_count: u64,
    #[serde(default)]
    pub tombstone_count: u64,
}
