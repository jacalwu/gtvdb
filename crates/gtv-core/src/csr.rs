//! Immutable in-memory Temporal-CSR index.

use std::sync::Arc;

use arrow::array::{ArrayRef, TimestampNanosecondArray, UInt16Array, UInt64Array};
use arrow::record_batch::RecordBatch;

use crate::error::{GtvError, Result};
use crate::table::edge_schema;
use crate::traits::TemporalGraphIndex;

/// A single neighbor edge resolved at a point in time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Neighbor {
    pub dst: u64,
    pub edge_type: u16,
    pub valid_from: i64,
    pub valid_to: i64,
}

#[derive(Debug, Clone, Copy)]
struct EdgeRow {
    src: u64,
    dst: u64,
    valid_from: i64,
    valid_to: i64,
    edge_type: u16,
}

/// Immutable in-memory Temporal-CSR.
///
/// Edges are sorted by `(src, valid_from, valid_to, dst)`; `offsets` partitions
/// the parallel edge arrays into contiguous runs per source node, so a neighbor
/// lookup scans only the run belonging to the queried source node.
#[derive(Debug, Clone)]
pub struct TemporalCSR {
    node_count: usize,
    offsets: Vec<u32>,
    dst: Vec<u64>,
    valid_from: Vec<i64>,
    valid_to: Vec<i64>,
    edge_type: Vec<u16>,
}

impl TemporalCSR {
    /// Build the index from parallel edge column arrays.
    pub fn from_arrays(
        src: &UInt64Array,
        dst: &UInt64Array,
        valid_from: &TimestampNanosecondArray,
        valid_to: &TimestampNanosecondArray,
        edge_type: &UInt16Array,
        node_count: usize,
    ) -> Result<Self> {
        let n = src.len();
        if dst.len() != n
            || valid_from.len() != n
            || valid_to.len() != n
            || edge_type.len() != n
        {
            return Err(GtvError::InvalidArgument(
                "edge arrays have mismatched lengths".into(),
            ));
        }

        let src_v = src.values().as_ref();
        let dst_v = dst.values().as_ref();
        let vf_v = valid_from.values().as_ref();
        let vt_v = valid_to.values().as_ref();
        let et_v = edge_type.values().as_ref();

        let mut rows: Vec<EdgeRow> = (0..n)
            .map(|i| EdgeRow {
                src: src_v[i],
                dst: dst_v[i],
                valid_from: vf_v[i],
                valid_to: vt_v[i],
                edge_type: et_v[i],
            })
            .collect();

        if let Some(bad) = rows.iter().map(|r| r.src).find(|s| *s >= node_count as u64) {
            return Err(GtvError::NodeOutOfRange(bad));
        }

        rows.sort_unstable_by_key(|r| (r.src, r.valid_from, r.valid_to, r.dst));

        let mut offsets = vec![0u32; node_count + 1];
        for r in &rows {
            offsets[r.src as usize + 1] += 1;
        }
        for i in 0..node_count {
            offsets[i + 1] += offsets[i];
        }

        let mut dst_vec = Vec::with_capacity(n);
        let mut vf_vec = Vec::with_capacity(n);
        let mut vt_vec = Vec::with_capacity(n);
        let mut et_vec = Vec::with_capacity(n);
        for r in &rows {
            dst_vec.push(r.dst);
            vf_vec.push(r.valid_from);
            vt_vec.push(r.valid_to);
            et_vec.push(r.edge_type);
        }

        Ok(Self {
            node_count,
            offsets,
            dst: dst_vec,
            valid_from: vf_vec,
            valid_to: vt_vec,
            edge_type: et_vec,
        })
    }

    pub fn node_count(&self) -> usize {
        self.node_count
    }

    pub fn edge_count(&self) -> usize {
        self.dst.len()
    }

    #[inline]
    fn edge_range(&self, src: u64) -> Result<(usize, usize)> {
        if src >= self.node_count as u64 {
            return Err(GtvError::NodeOutOfRange(src));
        }
        let s = src as usize;
        Ok((self.offsets[s] as usize, self.offsets[s + 1] as usize))
    }

    /// Neighbors of `src` active at `valid_at`.
    ///
    /// TODO(P1): the per-node run is scanned linearly; sort each run by
    /// `valid_from` and binary-search for the active slice in a later pass.
    pub fn neighbors(&self, src: u64, valid_at: i64) -> Result<impl Iterator<Item = Neighbor> + '_> {
        let (start, end) = self.edge_range(src)?;
        Ok((start..end)
            .filter(move |&e| self.valid_from[e] <= valid_at && valid_at < self.valid_to[e])
            .map(move |e| Neighbor {
                dst: self.dst[e],
                edge_type: self.edge_type[e],
                valid_from: self.valid_from[e],
                valid_to: self.valid_to[e],
            }))
    }

    /// Fetch neighbors for a batch of source nodes as a `RecordBatch` with the
    /// canonical edge schema.
    pub fn neighbors_record_batch(
        &self,
        src_nodes: &UInt64Array,
        valid_at: i64,
    ) -> Result<RecordBatch> {
        let mut src_out: Vec<u64> = Vec::new();
        let mut dst_out: Vec<u64> = Vec::new();
        let mut et_out: Vec<u16> = Vec::new();
        let mut vf_out: Vec<i64> = Vec::new();
        let mut vt_out: Vec<i64> = Vec::new();

        for &s in src_nodes.values().as_ref() {
            for nb in self.neighbors(s, valid_at)? {
                src_out.push(s);
                dst_out.push(nb.dst);
                et_out.push(nb.edge_type);
                vf_out.push(nb.valid_from);
                vt_out.push(nb.valid_to);
            }
        }

        let batch = RecordBatch::try_new(
            edge_schema(),
            vec![
                Arc::new(UInt64Array::from(src_out)) as ArrayRef,
                Arc::new(UInt64Array::from(dst_out)) as ArrayRef,
                Arc::new(UInt16Array::from(et_out)) as ArrayRef,
                Arc::new(TimestampNanosecondArray::from(vf_out)) as ArrayRef,
                Arc::new(TimestampNanosecondArray::from(vt_out)) as ArrayRef,
            ],
        )?;
        Ok(batch)
    }

    /// k-hop traversal: one frontier per hop (1..=k), each a deduplicated,
    /// sorted array of reached nodes at snapshot time `valid_at`.
    pub fn khop(&self, seeds: &UInt64Array, k: usize, valid_at: i64) -> Result<Vec<UInt64Array>> {
        let mut frontiers = Vec::with_capacity(k);
        let mut current: Vec<u64> = seeds.values().as_ref().to_vec();
        for _ in 0..k {
            let mut next: Vec<u64> = Vec::new();
            for &s in &current {
                for nb in self.neighbors(s, valid_at)? {
                    next.push(nb.dst);
                }
            }
            next.sort_unstable();
            next.dedup();
            frontiers.push(UInt64Array::from(next.clone()));
            current = next;
        }
        Ok(frontiers)
    }
}

impl TemporalGraphIndex for TemporalCSR {
    fn fetch_temporal_neighbors(
        &self,
        src_nodes: &UInt64Array,
        valid_at: i64,
    ) -> Result<RecordBatch> {
        self.neighbors_record_batch(src_nodes, valid_at)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> TemporalCSR {
        TemporalCSR::from_arrays(
            &UInt64Array::from(vec![0u64, 0, 1, 1, 2, 3]),
            &UInt64Array::from(vec![1u64, 2, 3, 4, 5, 5]),
            &TimestampNanosecondArray::from(vec![0i64, 50, 0, 100, 0, 150]),
            &TimestampNanosecondArray::from(vec![100i64, 200, 100, 300, 300, 400]),
            &UInt16Array::from(vec![1u16, 1, 2, 2, 1, 3]),
            6,
        )
        .unwrap()
    }

    #[test]
    fn neighbors_sliced_by_time() {
        let csr = sample();
        let at_t0: Vec<u64> = csr.neighbors(0, 0).unwrap().map(|n| n.dst).collect();
        assert_eq!(at_t0, vec![1]);

        let at_t150: Vec<u64> = csr.neighbors(0, 150).unwrap().map(|n| n.dst).collect();
        assert_eq!(at_t150, vec![2]);
    }

    #[test]
    fn half_open_interval_semantics() {
        let csr = sample();
        // valid_to is exclusive: at exactly valid_to the edge is gone.
        assert_eq!(csr.neighbors(3, 399).unwrap().count(), 1); // 3 -> 5 active
        assert_eq!(csr.neighbors(3, 400).unwrap().count(), 0); // 3 -> 5 expired
        // node 0's first edge (0->1, [0,100)) expires at T=100 while 0->2 persists.
        let nbs: Vec<u64> = csr.neighbors(0, 100).unwrap().map(|n| n.dst).collect();
        assert_eq!(nbs, vec![2]);
    }

    #[test]
    fn khop_frontiers() {
        let csr = sample();
        let f = csr.khop(&UInt64Array::from(vec![0u64]), 2, 0).unwrap();
        assert_eq!(f.len(), 2);
        assert_eq!(f[0].values().as_ref(), &[1u64]);
        assert_eq!(f[1].values().as_ref(), &[3u64]);
    }

    #[test]
    fn node_out_of_range_is_error() {
        let csr = sample();
        assert!(csr.neighbors(99, 0).is_err());
    }
}
