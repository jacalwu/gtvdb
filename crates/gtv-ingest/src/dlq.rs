//! Dead-letter queue for rejected events.
//!
//! Files land at `<root>/deadletter/<source>/<date>/<partition>-<seq>.parquet`
//! with the envelope fields plus `error_code` / `error_message` / `rejected_at`
//! so every rejection is auditable and can be reprocessed through the same
//! pipeline.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

use crate::envelope::{now_ns, Envelope};
use crate::error::Result;

static SEQ: AtomicU64 = AtomicU64::new(0);

/// `YYYY-MM-DD` of an event time (ns); falls back to epoch on bad input.
pub fn dlq_date(event_time_ns: i64) -> String {
    let secs = event_time_ns.div_euclid(1_000_000_000);
    chrono::DateTime::from_timestamp(secs, 0)
        .unwrap_or_default()
        .format("%Y-%m-%d")
        .to_string()
}

/// Dead-letter queue writer / reader.
#[derive(Debug, Clone)]
pub struct DeadLetterQueue {
    root: PathBuf,
}

/// DLQ schema: envelope + rejection metadata.
pub fn dlq_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("source", DataType::Utf8, false),
        Field::new("partition", DataType::Int32, false),
        Field::new("offset", DataType::Int64, false),
        Field::new("event_id", DataType::Binary, false),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("payload", DataType::Binary, false),
        Field::new("error_code", DataType::Utf8, false),
        Field::new("error_message", DataType::Utf8, false),
        Field::new(
            "rejected_at",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ]))
}

impl DeadLetterQueue {
    /// `root` is the base directory (DLQ files go under `root/deadletter`).
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Directory holding one source/day's rejects.
    pub fn dir(&self, source: &str, date: &str) -> PathBuf {
        self.root.join("deadletter").join(source).join(date)
    }

    fn encode(
        &self,
        envs: &[Envelope],
        error_code: &str,
        error_message: &str,
    ) -> Result<RecordBatch> {
        let schema = dlq_schema();
        let n = envs.len();
        let cols: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(
                envs.iter().map(|e| e.source.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                envs.iter().map(|e| e.partition).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                envs.iter().map(|e| e.offset).collect::<Vec<_>>(),
            )),
            Arc::new(BinaryArray::from_iter_values(
                envs.iter().map(|e| e.event_id),
            )),
            Arc::new(TimestampNanosecondArray::from(
                envs.iter().map(|e| e.event_time).collect::<Vec<_>>(),
            )),
            Arc::new(BinaryArray::from_iter_values(
                envs.iter().map(|e| e.payload.as_ref()),
            )),
            Arc::new(StringArray::from(vec![error_code; n])),
            Arc::new(StringArray::from(vec![error_message; n])),
            Arc::new(TimestampNanosecondArray::from(vec![now_ns(); n])),
        ];
        Ok(RecordBatch::try_new(schema, cols)?)
    }

    /// Write `envs` to the DLQ, returning the parquet path.
    pub fn write(
        &self,
        source: &str,
        date: &str,
        partition: i32,
        envs: &[Envelope],
        error_code: &str,
        error_message: &str,
    ) -> Result<PathBuf> {
        let dir = self.dir(source, date);
        fs::create_dir_all(&dir)?;
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let path = dir.join(format!("{partition}-{seq}-{}.parquet", now_ns()));
        let batch = self.encode(envs, error_code, error_message)?;
        gtv_storage::write_batch(&path.to_string_lossy(), &batch)?;
        Ok(path)
    }

    /// All DLQ parquet paths for a source.
    pub fn list(&self, source: &str) -> Result<Vec<PathBuf>> {
        let dir = self.root.join("deadletter").join(source);
        let mut out = Vec::new();
        if !dir.exists() {
            return Ok(out);
        }
        for date in fs::read_dir(&dir)? {
            let date = date?.path();
            if !date.is_dir() {
                continue;
            }
            for f in fs::read_dir(&date)? {
                let f = f?.path();
                if f.extension().and_then(|e| e.to_str()) == Some("parquet") {
                    out.push(f);
                }
            }
        }
        out.sort();
        Ok(out)
    }

    /// Read a DLQ parquet file back into batches.
    pub fn read(&self, path: impl AsRef<Path>) -> Result<Vec<RecordBatch>> {
        Ok(gtv_storage::read_batches(&path.as_ref().to_string_lossy())?)
    }

    /// Recover the rejected events as [`Envelope`]s so they can be fed back
    /// through the pipeline after a fix (the `reprocess` path).
    pub fn read_envelopes(&self, path: impl AsRef<Path>) -> Result<Vec<Envelope>> {
        let batches = self.read(path)?;
        let mut out = Vec::new();
        for b in &batches {
            let source = b
                .column_by_name("source")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let partition = b
                .column_by_name("partition")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let offset = b
                .column_by_name("offset")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let event_id = b
                .column_by_name("event_id")
                .unwrap()
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();
            let event_time = b
                .column_by_name("event_time")
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            let payload = b
                .column_by_name("payload")
                .unwrap()
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();
            for i in 0..b.num_rows() {
                let mut id = [0u8; 16];
                id.copy_from_slice(event_id.value(i));
                out.push(Envelope::with_ingest_time(
                    source.value(i),
                    partition.value(i),
                    offset.value(i),
                    id,
                    event_time.value(i),
                    now_ns(),
                    1,
                    bytes::Bytes::copy_from_slice(payload.value(i)),
                ));
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::Envelope;
    use bytes::Bytes;

    fn env(n: i64) -> Envelope {
        Envelope::with_ingest_time(
            "feed",
            0,
            n,
            Envelope::id_from_offset("feed", 0, n),
            1_700_000_000_000_000_000 + n,
            0,
            1,
            Bytes::from_static(b"bad"),
        )
    }

    #[test]
    fn write_list_and_read_round_trip() {
        let root = std::env::temp_dir().join(format!(
            "gtv_dlq_{}_{}",
            std::process::id(),
            now_ns()
        ));
        let dlq = DeadLetterQueue::new(&root);
        let date = dlq_date(1_700_000_000_000_000_000);
        assert_eq!(date, "2023-11-14");
        let path = dlq
            .write("feed", &date, 0, &[env(1), env(2)], "LATE", "below watermark")
            .unwrap();
        assert!(path.exists());
        let listed = dlq.list("feed").unwrap();
        assert_eq!(listed.len(), 1);
        let batches = dlq.read(&listed[0]).unwrap();
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[0].schema(), dlq_schema());
        // Rejected events can be recovered for reprocessing.
        let recovered = dlq.read_envelopes(&listed[0]).unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].offset, 1);
        assert_eq!(recovered[1].payload.as_ref(), b"bad");
        let _ = fs::remove_dir_all(&root);
    }
}
