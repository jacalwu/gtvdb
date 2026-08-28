//! In-memory time-travel store: named tables, each a sequence of full-table
//! snapshots keyed by a nanosecond timestamp. `as_of(T)` returns the newest
//! snapshot with `timestamp <= T`.

use std::collections::HashMap;

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::error::{Result, StorageError};

/// One point-in-time snapshot of a table.
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub timestamp: i64,
    pub batches: Vec<RecordBatch>,
}

/// A multi-versioned collection of tables supporting point-in-time reads.
#[derive(Debug, Default, Clone)]
pub struct SnapshotStore {
    tables: HashMap<String, TableVersions>,
}

#[derive(Debug, Clone)]
struct TableVersions {
    schema: SchemaRef,
    snapshots: Vec<Snapshot>,
}

impl SnapshotStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a full-table snapshot at `timestamp`.
    pub fn insert(&mut self, table: &str, timestamp: i64, batches: Vec<RecordBatch>) -> Result<()> {
        let Some(first) = batches.first() else {
            return Err(StorageError::Msg(format!(
                "snapshot for `{table}` has no batches"
            )));
        };
        let schema = first.schema();

        let versions = self.tables.entry(table.to_string()).or_insert_with(|| TableVersions {
            schema: schema.clone(),
            snapshots: Vec::new(),
        });
        if versions.schema.as_ref() != schema.as_ref() {
            return Err(StorageError::Msg(format!(
                "schema mismatch for `{table}` at T={timestamp}"
            )));
        }

        versions.snapshots.push(Snapshot { timestamp, batches });
        versions
            .snapshots
            .sort_by_key(|s| s.timestamp);
        Ok(())
    }

    /// Newest snapshot at or before `timestamp`.
    pub fn as_of(&self, table: &str, timestamp: i64) -> Result<Vec<RecordBatch>> {
        let versions = self
            .tables
            .get(table)
            .ok_or_else(|| StorageError::Msg(format!("unknown table `{table}`")))?;
        versions
            .snapshots
            .iter()
            .rev()
            .find(|s| s.timestamp <= timestamp)
            .map(|s| s.batches.clone())
            .ok_or_else(|| {
                StorageError::Msg(format!(
                    "no snapshot of `{table}` at or before T={timestamp}"
                ))
            })
    }

    /// Newest snapshot overall.
    pub fn latest(&self, table: &str) -> Result<Vec<RecordBatch>> {
        let versions = self
            .tables
            .get(table)
            .ok_or_else(|| StorageError::Msg(format!("unknown table `{table}`")))?;
        versions
            .snapshots
            .last()
            .map(|s| s.batches.clone())
            .ok_or_else(|| StorageError::Msg(format!("no snapshot of `{table}`")))
    }

    /// All snapshot timestamps of a table, ascending.
    pub fn timestamps(&self, table: &str) -> Result<Vec<i64>> {
        let versions = self
            .tables
            .get(table)
            .ok_or_else(|| StorageError::Msg(format!("unknown table `{table}`")))?;
        Ok(versions.snapshots.iter().map(|s| s.timestamp).collect())
    }

    pub fn table_names(&self) -> impl Iterator<Item = &str> {
        self.tables.keys().map(|s| s.as_str())
    }

    pub fn schema(&self, table: &str) -> Option<SchemaRef> {
        self.tables.get(table).map(|v| v.schema.clone())
    }

    pub fn schema_owned(&self, table: &str) -> Option<Schema> {
        self.tables
            .get(table)
            .map(|v| v.schema.as_ref().clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use arrow::array::{Int64Array, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};

    fn batch(id: i64) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![id]))],
        )
        .unwrap()
    }

    #[test]
    fn as_of_picks_latest_not_after() {
        let mut store = SnapshotStore::new();
        store.insert("t", 0, vec![batch(0)]).unwrap();
        store.insert("t", 100, vec![batch(100)]).unwrap();
        store.insert("t", 200, vec![batch(200)]).unwrap();

        let got = store.as_of("t", 150).unwrap();
        assert_eq!(
            got[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            100
        );
    }

    #[test]
    fn as_of_before_first_errors() {
        let mut store = SnapshotStore::new();
        store.insert("t", 100, vec![batch(100)]).unwrap();
        assert!(store.as_of("t", 50).is_err());
    }

    #[test]
    fn schema_mismatch_rejected() {
        let mut store = SnapshotStore::new();
        store.insert("t", 0, vec![batch(0)]).unwrap();
        let other = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)])),
            vec![Arc::new(UInt64Array::from(vec![1u64]))],
        )
        .unwrap();
        assert!(store.insert("t", 1, vec![other]).is_err());
    }
}
