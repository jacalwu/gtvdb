//! Durable committed-offset store.
//!
//! Append-only JSONL: each successful micro-batch append is one line holding
//! the offsets that batch covered. On open the store replays the log and keeps
//! the **maximum** offset per `(source, partition)`, so offsets never move
//! backwards across restarts.
//!
//! The catalog snapshot summary remains the atomic source of truth (offsets are
//! written in the same commit as the data); this store is the fast local resume
//! point and can be rebuilt from the snapshot log.

use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::error::Result;
use crate::source::PartitionOffset;

/// Append-only committed offset log.
#[derive(Debug, Clone)]
pub struct OffsetStore {
    path: PathBuf,
    committed: HashMap<(String, i32), i64>,
}

impl OffsetStore {
    /// Open (or create) the store under `dir` as `metadata/offsets.jsonl`.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref().join("metadata");
        fs::create_dir_all(&dir)?;
        let path = dir.join("offsets.jsonl");
        let mut store = Self {
            path,
            committed: HashMap::new(),
        };
        store.load()?;
        Ok(store)
    }

    /// Path of the offset log.
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn load(&mut self) -> Result<()> {
        let text = match fs::read_to_string(&self.path) {
            Ok(t) => t,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            // A crash can leave a truncated trailing line; skip anything that
            // does not parse rather than refusing to start.
            let Ok(offsets) = serde_json::from_str::<Vec<PartitionOffset>>(line) else {
                continue;
            };
            for o in offsets {
                self.bump(&o);
            }
        }
        Ok(())
    }

    fn bump(&mut self, o: &PartitionOffset) {
        let e = self
            .committed
            .entry((o.source.clone(), o.partition))
            .or_insert(o.offset);
        *e = (*e).max(o.offset);
    }

    /// Last committed offset for a partition, if any.
    pub fn committed(&self, source: &str, partition: i32) -> Option<i64> {
        self.committed
            .get(&(source.to_string(), partition))
            .copied()
    }

    /// Every committed offset, sorted by `(source, partition)`.
    pub fn all(&self) -> Vec<PartitionOffset> {
        let mut out: Vec<PartitionOffset> = self
            .committed
            .iter()
            .map(|((s, p), o)| PartitionOffset::new(s.clone(), *p, *o))
            .collect();
        out.sort_by(|a, b| a.source.cmp(&b.source).then(a.partition.cmp(&b.partition)));
        out
    }

    /// Append `offsets` to the log and update the in-memory view.
    pub fn commit(&mut self, offsets: &[PartitionOffset]) -> Result<()> {
        if offsets.is_empty() {
            return Ok(());
        }
        let line = serde_json::to_vec(offsets)?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        file.write_all(&line)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        for o in offsets {
            self.bump(o);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "gtv_offsets_{tag}_{}_{}",
            std::process::id(),
            crate::envelope::now_ns()
        ));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn commit_survives_reopen_and_is_monotonic() {
        let dir = tmp("reopen");
        {
            let mut s = OffsetStore::open(&dir).unwrap();
            s.commit(&[PartitionOffset::new("kafka", 0, 10)]).unwrap();
            s.commit(&[PartitionOffset::new("kafka", 0, 25)]).unwrap();
            assert_eq!(s.committed("kafka", 0), Some(25));
        }
        let mut s = OffsetStore::open(&dir).unwrap();
        assert_eq!(s.committed("kafka", 0), Some(25));
        // A late/duplicate commit must not move the offset backwards.
        s.commit(&[PartitionOffset::new("kafka", 0, 5)]).unwrap();
        assert_eq!(s.committed("kafka", 0), Some(25));
        assert_eq!(s.all().len(), 1);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn truncated_trailing_line_is_ignored() {
        let dir = tmp("trunc");
        let mut s = OffsetStore::open(&dir).unwrap();
        s.commit(&[PartitionOffset::new("f", 0, 3)]).unwrap();
        // Simulate a crash half-way through the next append.
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(s.path())
            .unwrap();
        f.write_all(b"[{\"source\":\"f\",\"partition\":0,\"off").unwrap();
        drop(f);
        let s2 = OffsetStore::open(&dir).unwrap();
        assert_eq!(s2.committed("f", 0), Some(3));
        let _ = fs::remove_dir_all(&dir);
    }
}
