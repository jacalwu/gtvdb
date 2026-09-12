//! Partition spec: how a table's rows are laid out on disk.
//!
//! The spec is versioned. `date/table/symbol`-style specs created too many small
//! files (roadmap P0.2), so callers can choose `Identity`, `DateTrunc` or
//! `HashBucket` transforms; the catalog stores the spec metadata and the
//! partition values supplied at commit time.

use serde::{Deserialize, Serialize};

/// A single partitioning transform.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Transform {
    /// Use the column value directly.
    Identity,
    /// Truncate a timestamp to `unit` (`day` / `hour`).
    DateTrunc { unit: String },
    /// `murmur3(column) % buckets`.
    HashBucket { buckets: u32 },
}

/// One partition column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionColumn {
    pub source: String,
    pub transform: Transform,
    pub name: String,
}

/// A versioned partition spec.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionSpec {
    pub version: u32,
    pub columns: Vec<PartitionColumn>,
}

impl PartitionSpec {
    /// The default unpartitioned spec (a single logical partition).
    pub fn single() -> Self {
        Self {
            version: 1,
            columns: Vec::new(),
        }
    }

    /// True when the table is not partitioned.
    pub fn is_single(&self) -> bool {
        self.columns.is_empty()
    }
}

impl Default for PartitionSpec {
    fn default() -> Self {
        Self::single()
    }
}

/// A concrete partition value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PartitionValue {
    Null,
    Str(String),
    Int(i64),
}

impl PartitionValue {
    /// Filesystem-safe directory component.
    pub fn dir_component(&self) -> String {
        match self {
            PartitionValue::Null => "__null__".to_string(),
            PartitionValue::Int(i) => i.to_string(),
            PartitionValue::Str(s) => {
                let cleaned: String = s
                    .chars()
                    .map(|c| {
                        if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | ':') {
                            c
                        } else {
                            '_'
                        }
                    })
                    .collect();
                if cleaned.is_empty() {
                    "__empty__".to_string()
                } else {
                    cleaned
                }
            }
        }
    }
}

/// Join partition values into a relative directory path.
pub fn partition_dir(values: &[PartitionValue]) -> String {
    if values.is_empty() {
        "__single__".to_string()
    } else {
        values
            .iter()
            .map(PartitionValue::dir_component)
            .collect::<Vec<_>>()
            .join("/")
    }
}
