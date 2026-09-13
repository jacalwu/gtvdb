//! File replay adapter — always on, for tests, backfills and disaster replay.
//!
//! Reads newline-delimited JSON (one [`JsonEnvelope`] per line) as a single
//! logical partition. The line index is the offset, so replaying the same file
//! always yields the same `(partition, offset)` → the same derived `event_id`.

use std::fs;
use std::path::{Path, PathBuf};

use bytes::Bytes;

use crate::envelope::{now_ns, parse_event_id, Envelope, JsonEnvelope};
use crate::error::{IngestError, Result};
use crate::source::{PartitionLag, PartitionOffset, SourceAdapter};

/// Single-partition replay adapter over a JSONL file (or in-memory lines).
#[derive(Debug, Clone)]
pub struct FileReplayAdapter {
    source: String,
    path: Option<PathBuf>,
    partition: i32,
    records: Vec<JsonEnvelope>,
    /// Last committed offset (inclusive); `-1` = nothing committed yet.
    committed: i64,
    /// Read cursor: last offset handed out by `poll` (may be ahead of
    /// `committed` while a batch is in flight). `seek` rewinds it.
    cursor: i64,
}

impl FileReplayAdapter {
    /// Open `path` as source `source` (partition 0).
    pub fn open(path: impl AsRef<Path>, source: impl Into<String>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let text = fs::read_to_string(&path)?;
        Self::from_str(source, &text).map(|mut a| {
            a.path = Some(path);
            a
        })
    }

    /// Build from an in-memory JSONL body (tests / embedded fixtures).
    pub fn from_str(source: impl Into<String>, text: &str) -> Result<Self> {
        let mut records = Vec::new();
        for (lineno, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let rec: JsonEnvelope = serde_json::from_str(line).map_err(|e| {
                IngestError::Msg(format!("line {}: invalid JSON envelope: {e}", lineno + 1))
            })?;
            records.push(rec);
        }
        Ok(Self {
            source: source.into(),
            path: None,
            partition: 0,
            records,
            committed: -1,
            cursor: -1,
        })
    }

    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// Total number of events in the file.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Reset the read cursor to the start (replay the whole file again).
    pub fn rewind(&mut self) {
        self.committed = -1;
        self.cursor = -1;
    }

    fn envelope_at(&self, idx: usize) -> Result<Envelope> {
        let rec = &self.records[idx];
        let offset = idx as i64;
        let event_id = match rec.event_id.as_deref() {
            Some(hex) => parse_event_id(hex)?,
            None => Envelope::id_from_offset(&self.source, self.partition, offset),
        };
        Ok(Envelope::with_ingest_time(
            self.source.clone(),
            self.partition,
            offset,
            event_id,
            rec.event_time,
            now_ns(),
            rec.schema_version,
            Bytes::from(rec.payload.clone()),
        ))
    }
}

impl SourceAdapter for FileReplayAdapter {
    fn name(&self) -> &str {
        &self.source
    }

    fn poll(&mut self, max: usize) -> Result<Vec<Envelope>> {
        let start = (self.cursor + 1).max(0) as usize;
        let end = (start + max).min(self.records.len());
        let out: Result<Vec<Envelope>> = (start..end).map(|i| self.envelope_at(i)).collect();
        if end > start {
            self.cursor = end as i64 - 1;
        }
        out
    }

    fn commit(&mut self, offsets: &[PartitionOffset]) -> Result<()> {
        for o in offsets {
            if o.source == self.source && o.partition == self.partition {
                self.committed = self.committed.max(o.offset);
                self.cursor = self.cursor.max(o.offset);
            }
        }
        Ok(())
    }

    fn seek(&mut self, offsets: &[PartitionOffset]) -> Result<()> {
        // Resync the read cursor to the commit point first; an empty slice then
        // means "rewind to whatever is already committed" (used to recover a
        // failed publish).
        self.cursor = self.committed;
        for o in offsets {
            if o.source == self.source && o.partition == self.partition {
                if o.offset >= self.records.len() as i64 {
                    return Err(IngestError::OffsetOutOfRange {
                        name: self.source.clone(),
                        partition: self.partition,
                        offset: o.offset,
                        available: self.records.len(),
                    });
                }
                self.committed = o.offset;
                self.cursor = o.offset;
            }
        }
        Ok(())
    }

    fn lag(&self) -> Vec<PartitionLag> {
        vec![PartitionLag {
            partition: self.partition,
            committed: self.committed,
            high: self.records.len() as i64 - 1,
        }]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body() -> &'static str {
        "{\"event_time\":100,\"payload\":\"a\"}\n\
         {\"event_time\":101,\"payload\":\"b\"}\n\
         \n\
         {\"event_time\":102,\"payload\":\"c\"}\n"
    }

    #[test]
    fn poll_advances_cursor_and_commit_drives_lag() {
        let mut a = FileReplayAdapter::from_str("f", body()).unwrap();
        assert_eq!(a.len(), 3);
        assert_eq!(a.lag()[0].lag(), 3); // high=2, committed=-1
        let first = a.poll(2).unwrap();
        assert_eq!(first.len(), 2);
        assert_eq!((first[0].offset, first[1].offset), (0, 1));
        // The read cursor advanced, so the next poll does not repeat them.
        let second = a.poll(2).unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].offset, 2);
        // Lag is measured against the *committed* offset, not the cursor.
        assert_eq!(a.lag()[0].lag(), 3);
        a.commit(&[PartitionOffset::new("f", 0, 2)]).unwrap();
        assert_eq!(a.lag()[0].lag(), 0);
        // seek rewinds the cursor back to the committed position.
        a.seek(&[PartitionOffset::new("f", 0, 1)]).unwrap();
        assert_eq!(a.poll(10).unwrap()[0].offset, 2);
    }

    #[test]
    fn rewind_replays_from_scratch() {
        let mut a = FileReplayAdapter::from_str("f", body()).unwrap();
        a.commit(&[PartitionOffset::new("f", 0, 2)]).unwrap();
        assert!(a.poll(10).unwrap().is_empty());
        a.rewind();
        assert_eq!(a.poll(10).unwrap().len(), 3);
    }

    #[test]
    fn explicit_event_id_is_honoured() {
        let text = "{\"event_time\":1,\"payload\":\"x\",\"event_id\":\"000102030405060708090a0b0c0d0e0f\"}";
        let mut a = FileReplayAdapter::from_str("f", text).unwrap();
        let e = a.poll(1).unwrap();
        assert_eq!(e[0].event_id[0], 0x00);
        assert_eq!(e[0].event_id[15], 0x0f);
    }

    #[test]
    fn seek_rejects_out_of_range() {
        let mut a = FileReplayAdapter::from_str("f", body()).unwrap();
        let err = a.seek(&[PartitionOffset::new("f", 0, 99)]).unwrap_err();
        assert!(matches!(err, IngestError::OffsetOutOfRange { .. }));
    }
}
