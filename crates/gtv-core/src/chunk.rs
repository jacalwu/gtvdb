//! Columnar chunk of temporal graph edges.

use arrow::array::{TimestampNanosecondArray, UInt16Array, UInt64Array};

use crate::csr::TemporalCSR;
use crate::error::{GtvError, Result};

/// Columnar chunk of temporal graph edges.
///
/// All arrays must share the same length. `valid_from`/`valid_to` are nanosecond
/// timestamps; an edge is active at time `T` iff `valid_from <= T < valid_to`.
#[derive(Debug, Clone)]
pub struct TemporalEdgeChunk {
    pub src_nodes: UInt64Array,
    pub dst_nodes: UInt64Array,
    pub valid_from: TimestampNanosecondArray,
    pub valid_to: TimestampNanosecondArray,
    pub edge_type: UInt16Array,
}

impl TemporalEdgeChunk {
    pub fn len(&self) -> usize {
        self.src_nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.src_nodes.is_empty()
    }

    /// Validate that all columns share the same length.
    pub fn validate(&self) -> Result<()> {
        let n = self.src_nodes.len();
        if self.dst_nodes.len() != n
            || self.valid_from.len() != n
            || self.valid_to.len() != n
            || self.edge_type.len() != n
        {
            return Err(GtvError::InvalidArgument(format!(
                "TemporalEdgeChunk columns have mismatched lengths: src={}, dst={}, valid_from={}, valid_to={}, edge_type={}",
                self.src_nodes.len(),
                self.dst_nodes.len(),
                self.valid_from.len(),
                self.valid_to.len(),
                self.edge_type.len()
            )));
        }
        Ok(())
    }

    /// Build a [`TemporalCSR`] index from this chunk.
    pub fn to_csr(&self, node_count: usize) -> Result<TemporalCSR> {
        self.validate()?;
        TemporalCSR::from_arrays(
            &self.src_nodes,
            &self.dst_nodes,
            &self.valid_from,
            &self.valid_to,
            &self.edge_type,
            node_count,
        )
    }
}
