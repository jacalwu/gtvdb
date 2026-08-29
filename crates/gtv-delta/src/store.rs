//! An [`LsmStore`] bundling an immutable snapshot with a mutable delta, plus an
//! optional background compaction thread.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use gtv_core::{EdgeTable, Result, TemporalGraph};

use crate::delta::DeltaBuffer;
use crate::delta::DeltaEdge;

/// A snapshot + delta pair.
///
/// Writes go into the mutable delta and are visible immediately via
/// [`LsmStore::merged_edges`]; a background thread (or an explicit
/// [`LsmStore::compact_now`]) folds the delta into the snapshot.
pub struct LsmStore {
    snapshot: Arc<Mutex<TemporalGraph>>,
    delta: Arc<Mutex<DeltaBuffer>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl LsmStore {
    pub fn new(snapshot: TemporalGraph) -> Self {
        Self {
            snapshot: Arc::new(Mutex::new(snapshot)),
            delta: Arc::new(Mutex::new(DeltaBuffer::new())),
            stop: Arc::new(AtomicBool::new(false)),
            handle: None,
        }
    }

    /// Queue an edge insertion into the delta buffer.
    pub fn insert(&self, edge: DeltaEdge) {
        self.delta.lock().expect("delta poisoned").insert(edge);
    }

    /// Queue an edge deletion into the delta buffer.
    pub fn delete(&self, edge: DeltaEdge) {
        self.delta.lock().expect("delta poisoned").delete(edge);
    }

    /// Number of pending delta operations.
    pub fn pending(&self) -> usize {
        self.delta.lock().expect("delta poisoned").len()
    }

    /// The logical edge table: snapshot edges with pending deltas applied.
    pub fn merged_edges(&self) -> Result<EdgeTable> {
        let snapshot = self.snapshot.lock().expect("snapshot poisoned");
        let delta = self.delta.lock().expect("delta poisoned");
        delta.apply(snapshot.edges())
    }

    /// Fold the delta into the snapshot synchronously, then clear it.
    pub fn compact_now(&self) -> Result<()> {
        let mut snapshot = self.snapshot.lock().expect("snapshot poisoned");
        let mut delta = self.delta.lock().expect("delta poisoned");
        if delta.is_empty() {
            return Ok(());
        }
        let new_edges = delta.apply(snapshot.edges())?;
        *snapshot = TemporalGraph::new(snapshot.nodes().clone(), new_edges)?;
        delta.clear();
        Ok(())
    }

    /// Start a background thread that compacts every `interval`.
    pub fn spawn_background(&mut self, interval: Duration) {
        if self.handle.is_some() {
            return;
        }
        let snapshot = Arc::clone(&self.snapshot);
        let delta = Arc::clone(&self.delta);
        let stop = Arc::clone(&self.stop);
        self.handle = Some(std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                std::thread::sleep(interval);
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                let mut snap = snapshot.lock().expect("snapshot poisoned");
                let mut del = delta.lock().expect("delta poisoned");
                if del.is_empty() {
                    continue;
                }
                if let Ok(new_edges) = del.apply(snap.edges()) {
                    if let Ok(new_snap) = TemporalGraph::new(snap.nodes().clone(), new_edges) {
                        *snap = new_snap;
                        del.clear();
                    }
                }
            }
        }));
    }

    /// Signal the background thread to stop and join it.
    pub fn stop_background(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for LsmStore {
    fn drop(&mut self) {
        self.stop_background();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gtv_core::{EdgeTable, NodeTable};
    use arrow::array::{Float64Array, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn graph() -> TemporalGraph {
        let nodes = NodeTable::new(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::UInt64, false),
                    Field::new("value", DataType::Float64, false),
                ])),
                vec![
                    Arc::new(UInt64Array::from(vec![0u64, 1, 2])) as _,
                    Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])) as _,
                ],
            )
            .unwrap(),
        )
        .unwrap();
        let edges = EdgeTable::from_vecs(vec![0], vec![1], vec![1u16], vec![0], vec![100]).unwrap();
        TemporalGraph::new(nodes, edges).unwrap()
    }

    fn edge(src: u64, dst: u64, from: i64, to: i64) -> DeltaEdge {
        DeltaEdge {
            src,
            dst,
            edge_type: 1,
            valid_from: from,
            valid_to: to,
        }
    }

    #[test]
    fn writes_visible_before_compaction() {
        let store = LsmStore::new(graph());
        store.insert(edge(1, 2, 0, 100));
        assert_eq!(store.merged_edges().unwrap().len(), 2);
    }

    #[test]
    fn compaction_folds_delta_and_clears() {
        let store = LsmStore::new(graph());
        store.insert(edge(1, 2, 0, 100));
        store.compact_now().unwrap();
        assert_eq!(store.pending(), 0);
        // After compaction, the merged view and the snapshot agree.
        assert_eq!(store.merged_edges().unwrap().len(), 2);
    }

    #[test]
    fn background_thread_compacts() {
        let mut store = LsmStore::new(graph());
        store.insert(edge(1, 2, 0, 100));
        store.spawn_background(Duration::from_millis(10));
        // Wait for the background thread to fold the delta in.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while store.pending() > 0 && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        store.stop_background();
        assert_eq!(store.pending(), 0);
        assert_eq!(store.merged_edges().unwrap().len(), 2);
    }
}
