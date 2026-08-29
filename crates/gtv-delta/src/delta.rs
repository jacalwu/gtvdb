//! Mutable edge delta buffer.

use std::collections::{BTreeMap, BTreeSet};

use gtv_core::{EdgeTable, Result};

/// A single edge in delta form, mirroring the `EdgeTable` columns.
///
/// `valid_from`/`valid_to` are nanosecond timestamps; the interval is
/// half-open `[valid_from, valid_to)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DeltaEdge {
    pub src: u64,
    pub dst: u64,
    pub edge_type: u16,
    pub valid_from: i64,
    pub valid_to: i64,
}

/// Mutable in-memory delta buffer: pending edge insertions and deletions.
///
/// Deletions are *exact-match tombstones* — they remove an edge whose five
/// columns match exactly (no interval splitting). This is the MVP semantic;
/// a full bitemporal model would split `[valid_from, valid_to)` at the
/// deletion instant instead.
#[derive(Debug, Clone, Default)]
pub struct DeltaBuffer {
    inserts: BTreeSet<DeltaEdge>,
    deletes: BTreeSet<DeltaEdge>,
}

impl DeltaBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Queue an edge insertion.
    pub fn insert(&mut self, edge: DeltaEdge) {
        self.deletes.remove(&edge);
        self.inserts.insert(edge);
    }

    /// Queue an edge deletion (exact-match tombstone).
    pub fn delete(&mut self, edge: DeltaEdge) {
        self.inserts.remove(&edge);
        self.deletes.insert(edge);
    }

    pub fn len(&self) -> usize {
        self.inserts.len() + self.deletes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inserts.is_empty() && self.deletes.is_empty()
    }

    pub fn pending_inserts(&self) -> impl Iterator<Item = &DeltaEdge> {
        self.inserts.iter()
    }

    pub fn pending_deletes(&self) -> impl Iterator<Item = &DeltaEdge> {
        self.deletes.iter()
    }

    /// Apply pending deltas to a snapshot edge table, returning a new table.
    pub fn apply(&self, edges: &EdgeTable) -> Result<EdgeTable> {
        let n = edges.len();
        let mut keep: BTreeMap<DeltaEdge, ()> = BTreeMap::new();
        for i in 0..n {
            let e = DeltaEdge {
                src: edges.src().value(i),
                dst: edges.dst().value(i),
                edge_type: edges.edge_type().value(i),
                valid_from: edges.valid_from().value(i),
                valid_to: edges.valid_to().value(i),
            };
            // Tombstone wins: an edge present in `deletes` is dropped.
            if self.deletes.contains(&e) {
                continue;
            }
            keep.insert(e, ());
        }
        // Insertions land on top of the surviving snapshot edges.
        for &e in &self.inserts {
            keep.insert(e, ());
        }

        let mut ordered: Vec<DeltaEdge> = keep.into_keys().collect();
        // Match the CSR's canonical ordering: (src, valid_from, valid_to, dst).
        ordered.sort_unstable_by_key(|e| (e.src, e.valid_from, e.valid_to, e.dst));

        EdgeTable::from_vecs(
            ordered.iter().map(|e| e.src).collect(),
            ordered.iter().map(|e| e.dst).collect(),
            ordered.iter().map(|e| e.edge_type).collect(),
            ordered.iter().map(|e| e.valid_from).collect(),
            ordered.iter().map(|e| e.valid_to).collect(),
        )
    }

    /// Clear all pending deltas (call after a successful compaction).
    pub fn clear(&mut self) {
        self.inserts.clear();
        self.deletes.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gtv_core::TemporalGraph;
    use gtv_core::NodeTable;
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
                    Arc::new(UInt64Array::from(vec![0u64, 1, 2, 3])) as _,
                    Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])) as _,
                ],
            )
            .unwrap(),
        )
        .unwrap();
        let edges = EdgeTable::from_vecs(
            vec![0, 0, 1],
            vec![1, 2, 3],
            vec![1u16, 1, 2],
            vec![0, 50, 0],
            vec![100, 200, 100],
        )
        .unwrap();
        TemporalGraph::new(nodes, edges).unwrap()
    }

    fn edge(src: u64, dst: u64, ty: u16, from: i64, to: i64) -> DeltaEdge {
        DeltaEdge {
            src,
            dst,
            edge_type: ty,
            valid_from: from,
            valid_to: to,
        }
    }

    #[test]
    fn insert_is_visible_after_apply() {
        let g = graph();
        let mut delta = DeltaBuffer::new();
        delta.insert(edge(2, 0, 9, 0, 500));

        let new_edges = delta.apply(g.edges()).unwrap();
        assert_eq!(new_edges.len(), 4);
        let csr = new_edges.to_csr(g.node_count()).unwrap();
        assert_eq!(csr.neighbors(2, 0).unwrap().map(|n| n.dst).collect::<Vec<_>>(), vec![0]);
    }

    #[test]
    fn delete_removes_exact_match() {
        let g = graph();
        let mut delta = DeltaBuffer::new();
        delta.delete(edge(0, 2, 1, 50, 200));

        let new_edges = delta.apply(g.edges()).unwrap();
        assert_eq!(new_edges.len(), 2);
        let csr = new_edges.to_csr(g.node_count()).unwrap();
        // node 0's edge to dst 2 is gone; only 0 -> 1 remains.
        assert_eq!(csr.neighbors(0, 150).unwrap().count(), 0);
        assert_eq!(csr.neighbors(0, 0).unwrap().map(|n| n.dst).collect::<Vec<_>>(), vec![1]);
    }

    #[test]
    fn compact_rebuilds_graph() {
        let g = graph();
        let mut delta = DeltaBuffer::new();
        delta.insert(edge(3, 0, 5, 0, 1000));
        let g2 = crate::compact(&g, &delta).unwrap();
        assert_eq!(g2.edge_count(), 4);
        assert_eq!(g2.csr().neighbors(3, 0).unwrap().map(|n| n.dst).collect::<Vec<_>>(), vec![0]);
    }

    #[test]
    fn delete_then_insert_wins() {
        let mut delta = DeltaBuffer::new();
        let e = edge(0, 1, 1, 0, 100);
        delta.delete(e);
        delta.insert(e); // re-insert cancels the tombstone
        assert!(delta.pending_deletes().next().is_none());
        assert_eq!(delta.pending_inserts().count(), 1);
    }
}
