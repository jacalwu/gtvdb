//! Thin Arrow ↔ Parquet round-trip helpers.

use std::fs::File;

use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;

use crate::error::Result;

/// Write a single [`RecordBatch`] to `path` as a Parquet file.
pub fn write_batch(path: &str, batch: &RecordBatch) -> Result<()> {
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
    writer.write(batch)?;
    writer.close()?;
    Ok(())
}

/// Read all row groups of a Parquet file into [`RecordBatch`]es.
pub fn read_batches(path: &str) -> Result<Vec<RecordBatch>> {
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch?);
    }
    Ok(batches)
}
