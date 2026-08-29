//! CSV → Arrow loader with schema inference.

use std::fs::File;
use std::sync::Arc;

use arrow::csv::{infer_schema_from_files, ReaderBuilder};
use arrow::record_batch::RecordBatch;

use crate::error::Result;

/// Read a CSV file into [`RecordBatch`]es, inferring the schema from the first
/// `max_read_records` rows (header row expected). The `timestamp` column is
/// inferred as `Int64` (nanoseconds), matching the kdb convention of raw
/// timestamp counts.
pub fn read_csv(path: &str) -> Result<Vec<RecordBatch>> {
    let schema = infer_schema_from_files(&[path.to_string()], b',', Some(200), true)?;
    let file = File::open(path)?;
    let builder = ReaderBuilder::new(Arc::new(schema))
        .with_header(true)
        .with_batch_size(65_536);
    let mut reader = builder.build(file)?;
    let mut batches = Vec::new();
    for batch in reader.by_ref() {
        batches.push(batch?);
    }
    Ok(batches)
}
