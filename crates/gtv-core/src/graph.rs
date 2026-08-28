//! High-level temporal graph: node table + edge table + Temporal-CSR index.

use arrow::array::UInt64Array;

use crate::csr::TemporalCSR;
use crate::error::Result;
use crate::table::{EdgeTable, NodeTable};

/// A temporal graph bundling a node table, an edge table, and a [`TemporalCSR`]
/// index built from the edges.
#[derive(Debug, Clone)]
pub struct TemporalGraph {
    nodes: NodeTable,
    edges: EdgeTable,
    csr: TemporalCSR,
}

impl TemporalGraph {
    pub fn new(nodes: NodeTable, edges: EdgeTable) -> Result<Self> {
        let node_count = nodes.len();
        let csr = edges.to_csr(node_count)?;
        Ok(Self { nodes, edges, csr })
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    pub fn nodes(&self) -> &NodeTable {
        &self.nodes
    }

    pub fn edges(&self) -> &EdgeTable {
        &self.edges
    }

    pub fn csr(&self) -> &TemporalCSR {
        &self.csr
    }

    /// k-hop traversal from `seeds` at snapshot time `valid_at`.
    pub fn khop(&self, seeds: &UInt64Array, k: usize, valid_at: i64) -> Result<Vec<UInt64Array>> {
        self.csr.khop(seeds, k, valid_at)
    }
}
