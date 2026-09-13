//! Event-id dedup store.
//!
//! Streaming semantics are **at-least-once + dedup**: a crash between the data
//! commit and the source offset commit replays a batch, and the dedup store
//! makes that replay a no-op. This implementation is an exact, bounded,
//! most-recently-seen set (deterministic and cheap for the batch sizes the
//! single-node engine handles); a bloom/roaring variant can slot in behind the
//! same API when the window grows beyond memory.

use std::collections::{HashSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};

use crate::envelope::{event_id_hex, parse_event_id, Envelope};
use crate::error::Result;

/// Bounded, persistable set of seen `event_id`s.
#[derive(Debug, Clone)]
pub struct DedupStore {
    capacity: usize,
    set: HashSet<[u8; 16]>,
    order: VecDeque<[u8; 16]>,
    path: Option<PathBuf>,
}

impl DedupStore {
    /// In-memory store keeping the most recent `capacity` ids.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            set: HashSet::new(),
            order: VecDeque::new(),
            path: None,
        }
    }

    /// Open a persisted store at `path` (hex id per line).
    pub fn open(path: impl AsRef<Path>, capacity: usize) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut store = Self::new(capacity);
        if let Ok(text) = fs::read_to_string(&path) {
            for line in text.lines() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                if let Ok(id) = parse_event_id(line) {
                    store.insert(id);
                }
            }
        }
        store.path = Some(path);
        Ok(store)
    }

    pub fn len(&self) -> usize {
        self.set.len()
    }

    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    pub fn contains(&self, id: &[u8; 16]) -> bool {
        self.set.contains(id)
    }

    /// Insert an id, returning `true` when it was **new** (not a duplicate).
    pub fn insert(&mut self, id: [u8; 16]) -> bool {
        if !self.set.insert(id) {
            return false;
        }
        self.order.push_back(id);
        while self.order.len() > self.capacity {
            if let Some(old) = self.order.pop_front() {
                self.set.remove(&old);
            }
        }
        true
    }

    /// Split envelopes into `(fresh, duplicate_count)`.
    pub fn filter(&mut self, envs: Vec<Envelope>) -> (Vec<Envelope>, usize) {
        let mut fresh = Vec::with_capacity(envs.len());
        let mut dups = 0usize;
        for e in envs {
            if self.insert(e.event_id) {
                fresh.push(e);
            } else {
                dups += 1;
            }
        }
        (fresh, dups)
    }

    /// Atomically persist the current window, when backed by a path.
    pub fn persist(&self) -> Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        let mut body = String::with_capacity(self.order.len() * 33);
        for id in &self.order {
            body.push_str(&event_id_hex(id));
            body.push('\n');
        }
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        gtv_storage::write_atomic(path, body.as_bytes())?;
        Ok(())
    }

    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::Envelope;
    use bytes::Bytes;

    fn env(n: u64) -> Envelope {
        let key = format!("e{n}");
        Envelope::with_ingest_time(
            "s",
            0,
            n as i64,
            Envelope::id_from_key(&key),
            n as i64,
            0,
            1,
            Bytes::from_static(b"x"),
        )
    }

    #[test]
    fn duplicates_are_filtered() {
        let mut d = DedupStore::new(100);
        let (fresh, dups) = d.filter(vec![env(1), env(2), env(1), env(3), env(2)]);
        assert_eq!(fresh.len(), 3);
        assert_eq!(dups, 2);
        assert_eq!(d.len(), 3);
    }

    #[test]
    fn window_eviction_allows_reprocessing() {
        let mut d = DedupStore::new(2);
        assert!(d.insert(Envelope::id_from_key("a")));
        assert!(d.insert(Envelope::id_from_key("b")));
        assert!(!d.insert(Envelope::id_from_key("a")));
        // Inserting "c" evicts "a" (oldest).
        assert!(d.insert(Envelope::id_from_key("c")));
        assert_eq!(d.len(), 2);
        assert!(d.insert(Envelope::id_from_key("a")));
    }

    #[test]
    fn persist_round_trips() {
        let dir = std::env::temp_dir().join(format!(
            "gtv_dedup_{}_{}",
            std::process::id(),
            crate::envelope::now_ns()
        ));
        let path = dir.join("dedup.txt");
        {
            let mut d = DedupStore::open(&path, 100).unwrap();
            d.filter(vec![env(1), env(2), env(3)]);
            d.persist().unwrap();
        }
        let d = DedupStore::open(&path, 100).unwrap();
        assert_eq!(d.len(), 3);
        assert!(d.contains(&Envelope::id_from_key("e1")));
        let _ = fs::remove_dir_all(&dir);
    }
}
