//! Publish sinks for the micro-batch pipeline.
//!
//! [`CatalogSink`] performs the B2-1 atomic commit: the data batch and the
//! source offsets land in the *same* snapshot (offsets both on the data file and
//! in the snapshot summary), so a reader never sees a batch whose offsets are
//! not recorded, and vice versa.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;
use gtv_catalog::{
    CommitOp, CommitOptions, FsCatalog, NewFile, PartitionSpec, SourceOffset, TableId,
};

use crate::envelope::envelope_schema;
use crate::error::Result;
use crate::source::PartitionOffset;

/// Result of durably publishing one micro-batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishOutcome {
    pub snapshot: String,
    pub rows: usize,
}

/// Destination for a published micro-batch.
pub trait Sink {
    /// Durable publish. `idempotency_key` makes a replayed batch resolve to the
    /// same snapshot instead of duplicating data.
    fn publish(
        &self,
        batch: &RecordBatch,
        offsets: &[PartitionOffset],
        idempotency_key: Option<&str>,
    ) -> Result<PublishOutcome>;
}

/// Atomically commit micro-batches into a catalog table.
#[derive(Debug, Clone)]
pub struct CatalogSink {
    catalog: FsCatalog,
    table: TableId,
    table_name: String,
}

impl CatalogSink {
    /// Build from an open catalog and table id.
    pub fn new(catalog: FsCatalog, table: TableId, table_name: impl Into<String>) -> Self {
        Self {
            catalog,
            table,
            table_name: table_name.into(),
        }
    }

    /// Open the catalog at `root` and create `table` (envelope schema) if needed.
    pub fn open(root: impl Into<PathBuf>, table: &str) -> Result<Self> {
        let catalog = FsCatalog::open(root)?;
        let table_id = match catalog.table(table) {
            Ok(meta) => meta.table_id,
            Err(gtv_catalog::CatalogError::TableNotFound(_)) => catalog.create_table(
                table,
                envelope_schema(),
                PartitionSpec::single(),
            )?,
            Err(e) => return Err(e.into()),
        };
        Ok(Self::new(catalog, table_id, table))
    }

    pub fn table_id(&self) -> TableId {
        self.table
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn catalog(&self) -> &FsCatalog {
        &self.catalog
    }
}

impl Sink for CatalogSink {
    fn publish(
        &self,
        batch: &RecordBatch,
        offsets: &[PartitionOffset],
        idempotency_key: Option<&str>,
    ) -> Result<PublishOutcome> {
        let source_offsets: Vec<SourceOffset> = offsets.iter().map(Into::into).collect();
        let opts = CommitOptions {
            idempotency_key: idempotency_key.map(str::to_string),
            source_offsets,
            summary: serde_json::json!({ "streaming": true }),
            ..CommitOptions::default()
        };
        let snapshot = self.catalog.commit(
            self.table,
            CommitOp::Append,
            vec![NewFile::unpartitioned(batch.clone())],
            &opts,
        )?;
        Ok(PublishOutcome {
            snapshot: snapshot.to_string(),
            rows: batch.num_rows(),
        })
    }
}

/// In-memory sink for fast unit tests (records every published batch).
#[derive(Debug, Clone, Default)]
pub struct MemorySink {
    batches: Arc<Mutex<Vec<RecordBatch>>>,
    keys: Arc<Mutex<Vec<Option<String>>>>,
}

impl MemorySink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn batches(&self) -> Vec<RecordBatch> {
        self.batches.lock().unwrap().clone()
    }

    pub fn keys(&self) -> Vec<Option<String>> {
        self.keys.lock().unwrap().clone()
    }

    pub fn rows(&self) -> usize {
        self.batches
            .lock()
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum()
    }
}

impl Sink for MemorySink {
    fn publish(
        &self,
        batch: &RecordBatch,
        _offsets: &[PartitionOffset],
        idempotency_key: Option<&str>,
    ) -> Result<PublishOutcome> {
        self.batches.lock().unwrap().push(batch.clone());
        self.keys
            .lock()
            .unwrap()
            .push(idempotency_key.map(str::to_string));
        Ok(PublishOutcome {
            snapshot: format!("mem-{}", self.batches.lock().unwrap().len()),
            rows: batch.num_rows(),
        })
    }
}

/// A sink that always fails — used to prove "incomplete batch is invisible".
#[derive(Debug, Clone, Copy, Default)]
pub struct FailingSink;

impl Sink for FailingSink {
    fn publish(
        &self,
        _batch: &RecordBatch,
        _offsets: &[PartitionOffset],
        _idempotency_key: Option<&str>,
    ) -> Result<PublishOutcome> {
        Err(crate::error::IngestError::Msg(
            "sink unavailable (injected failure)".into(),
        ))
    }
}
