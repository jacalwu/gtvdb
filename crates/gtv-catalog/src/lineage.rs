//! Execution lineage: the evidence package recorded for every query (B2-3).
//!
//! A [`ExecutionRecord`] pins everything needed to explain or replay a result:
//! the query text/hash, engine version, the source table snapshots it read, the
//! model / index / scenario versions it used, and the output checksum. Records
//! are append-only and looked up by [`ExecutionId`].

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::id::{IndexId, SnapshotId, TableId};
use crate::manifest::SourceOffset;

/// A unique execution id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ExecutionId(pub Uuid);

impl ExecutionId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for ExecutionId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ExecutionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for ExecutionId {
    type Err = uuid::Error;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(Self(Uuid::parse_str(s)?))
    }
}

/// A table version read by an execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableRef {
    pub table_id: TableId,
    pub snapshot_id: SnapshotId,
    pub schema_version: u32,
    pub table_name: String,
}

/// A vector index version used by an execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexRef {
    pub index_id: IndexId,
    pub version: u32,
    pub metric: String,
    pub dim: u32,
}

/// A model version referenced by an execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelRef {
    pub model_id: String,
    pub version: String,
}

/// A user-defined function version used by an execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UdfRef {
    pub name: String,
    pub version: String,
    /// blake3 (hex) fingerprint of the function's identity for this engine.
    pub hash: String,
    /// `true` when the function may return different output for the same
    /// input across executions (e.g. `random()` / `now()`). Replay must refuse
    /// or be explicitly forced for records that reference one.
    #[serde(default)]
    pub nondeterministic: bool,
}

impl UdfRef {
    /// Build a UDF reference, deriving its fingerprint from name + version.
    pub fn new(name: &str, version: &str, nondeterministic: bool) -> Self {
        let hash = blake3::hash(format!("{name}@{version}").as_bytes())
            .to_hex()
            .to_string();
        Self {
            name: name.to_string(),
            version: version.to_string(),
            hash,
            nondeterministic,
        }
    }
}

/// The recorded evidence for one query execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionRecord {
    pub execution_id: ExecutionId,
    pub query_text: String,
    /// blake3 (hex) of `query_text`.
    pub query_hash: String,
    pub engine_version: String,
    #[serde(default)]
    pub source_tables: Vec<TableRef>,
    #[serde(default)]
    pub source_offsets: Vec<SourceOffset>,
    #[serde(default)]
    pub model_versions: Vec<ModelRef>,
    #[serde(default)]
    pub index_snapshots: Vec<IndexRef>,
    #[serde(default)]
    pub scenario_version: Option<String>,
    #[serde(default)]
    pub business_cutoff: Option<i64>,
    #[serde(default)]
    pub udf_versions: Vec<UdfRef>,
    #[serde(default)]
    pub runtime_params: serde_json::Value,
    /// blake3 (hex) of the Arrow IPC encoding of the output batches.
    pub output_checksum: String,
    pub output_rows: u64,
    pub started_at: i64,
    pub finished_at: i64,
}

impl ExecutionRecord {
    /// Start a record (checksum/rows filled by [`ExecutionRecord::finish`]).
    pub fn begin(query_text: &str, engine_version: &str) -> Self {
        Self {
            execution_id: ExecutionId::new(),
            query_text: query_text.to_string(),
            query_hash: query_hash(query_text),
            engine_version: engine_version.to_string(),
            source_tables: Vec::new(),
            source_offsets: Vec::new(),
            model_versions: Vec::new(),
            index_snapshots: Vec::new(),
            scenario_version: None,
            business_cutoff: None,
            udf_versions: Vec::new(),
            runtime_params: serde_json::Value::Null,
            output_checksum: String::new(),
            output_rows: 0,
            started_at: crate::schema::now_ns(),
            finished_at: 0,
        }
    }

    /// Complete the record with the output evidence.
    pub fn finish(&mut self, output_checksum: String, output_rows: u64) {
        self.output_checksum = output_checksum;
        self.output_rows = output_rows;
        self.finished_at = crate::schema::now_ns();
    }
}

/// blake3 (hex) of a query string.
pub fn query_hash(query: &str) -> String {
    blake3::hash(query.as_bytes()).to_hex().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn begin_finish_round_trip() {
        let mut rec = ExecutionRecord::begin("SELECT 1", "0.1.0");
        assert_eq!(rec.query_hash, query_hash("SELECT 1"));
        assert_eq!(rec.output_rows, 0);
        rec.finish("abc".into(), 3);
        assert_eq!(rec.output_checksum, "abc");
        assert_eq!(rec.output_rows, 3);
        assert!(rec.finished_at >= rec.started_at);
    }

    #[test]
    fn serde_round_trip() {
        let mut rec = ExecutionRecord::begin("SELECT 1", "0.1.0");
        rec.finish("abc".into(), 1);
        let json = serde_json::to_string(&rec).unwrap();
        let back: ExecutionRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(back.execution_id, rec.execution_id);
        assert_eq!(back.output_checksum, "abc");
    }
}
