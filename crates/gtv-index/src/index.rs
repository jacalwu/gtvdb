//! Index-type dispatch and the persistable-index trait.

use arrow::array::BooleanArray;
use gtv_core::{GtvError, Metric, Result, VectorHit, VectorIndex};
use serde::{Deserialize, Serialize};

use crate::{FlatIndex, HnswIndex, IvfIndex};

/// Which concrete index implementation is stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexType {
    Flat,
    Ivf,
    Hnsw,
}

impl IndexType {
    pub fn as_str(&self) -> &'static str {
        match self {
            IndexType::Flat => "flat",
            IndexType::Ivf => "ivf",
            IndexType::Hnsw => "hnsw",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "flat" => Some(IndexType::Flat),
            "ivf" => Some(IndexType::Ivf),
            "hnsw" => Some(IndexType::Hnsw),
            _ => None,
        }
    }

    pub fn code(&self) -> u8 {
        match self {
            IndexType::Flat => 0,
            IndexType::Ivf => 1,
            IndexType::Hnsw => 2,
        }
    }

    pub fn from_code(c: u8) -> Result<Self> {
        match c {
            0 => Ok(IndexType::Flat),
            1 => Ok(IndexType::Ivf),
            2 => Ok(IndexType::Hnsw),
            other => Err(GtvError::InvalidArgument(format!(
                "index: unknown type code {other}"
            ))),
        }
    }
}

/// Build parameters, serialized into the index manifest.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BuildOptions {
    Flat,
    Ivf { nlist: usize, nprobe: usize },
    Hnsw {
        m: usize,
        ef_construction: usize,
        ef_search: usize,
    },
}

impl BuildOptions {
    pub fn index_type(&self) -> IndexType {
        match self {
            BuildOptions::Flat => IndexType::Flat,
            BuildOptions::Ivf { .. } => IndexType::Ivf,
            BuildOptions::Hnsw { .. } => IndexType::Hnsw,
        }
    }
}

/// A vector index that can be serialized to / from a byte payload.
pub trait PersistableIndex: VectorIndex {
    fn index_type(&self) -> IndexType;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn to_bytes(&self) -> Vec<u8>;
}

impl PersistableIndex for FlatIndex {
    fn index_type(&self) -> IndexType {
        IndexType::Flat
    }
    fn len(&self) -> usize {
        FlatIndex::len(self)
    }
    fn to_bytes(&self) -> Vec<u8> {
        FlatIndex::to_bytes(self)
    }
}

impl PersistableIndex for IvfIndex {
    fn index_type(&self) -> IndexType {
        IndexType::Ivf
    }
    fn len(&self) -> usize {
        IvfIndex::len(self)
    }
    fn to_bytes(&self) -> Vec<u8> {
        IvfIndex::to_bytes(self)
    }
}

impl PersistableIndex for HnswIndex {
    fn index_type(&self) -> IndexType {
        IndexType::Hnsw
    }
    fn len(&self) -> usize {
        HnswIndex::len(self)
    }
    fn to_bytes(&self) -> Vec<u8> {
        HnswIndex::to_bytes(self)
    }
}

/// A dynamically dispatched index (any of the three implementations).
#[derive(Debug, Clone)]
pub enum AnyIndex {
    Flat(FlatIndex),
    Ivf(IvfIndex),
    Hnsw(HnswIndex),
}

impl AnyIndex {
    pub fn index_type(&self) -> IndexType {
        match self {
            AnyIndex::Flat(_) => IndexType::Flat,
            AnyIndex::Ivf(_) => IndexType::Ivf,
            AnyIndex::Hnsw(_) => IndexType::Hnsw,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            AnyIndex::Flat(i) => i.len(),
            AnyIndex::Ivf(i) => i.len(),
            AnyIndex::Hnsw(i) => i.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn as_flat(&self) -> Option<&FlatIndex> {
        match self {
            AnyIndex::Flat(i) => Some(i),
            _ => None,
        }
    }
    pub fn as_ivf(&self) -> Option<&IvfIndex> {
        match self {
            AnyIndex::Ivf(i) => Some(i),
            _ => None,
        }
    }
    pub fn as_hnsw(&self) -> Option<&HnswIndex> {
        match self {
            AnyIndex::Hnsw(i) => Some(i),
            _ => None,
        }
    }

    /// The stored vector for `id`, if present (exact-rerank accessor).
    pub fn vector_for_id(&self, id: u64) -> Option<&[f32]> {
        match self {
            AnyIndex::Flat(i) => i.vector_for_id(id),
            AnyIndex::Ivf(i) => i.vector_for_id(id),
            AnyIndex::Hnsw(i) => i.vector_for_id(id),
        }
    }

    /// The index's ids in the *filter-mask* order (the order of the ids the
    /// index was built from). For Flat this is insertion order; for IVF it undoes
    /// the cell reordering so a `BooleanArray` mask lines up with the internal
    /// `orig_pos` domain.
    pub fn ids(&self) -> Vec<u64> {
        match self {
            AnyIndex::Flat(i) => i.ids().to_vec(),
            AnyIndex::Ivf(i) => i.mask_order_ids(),
            AnyIndex::Hnsw(i) => i.ids().to_vec(),
        }
    }

    /// Exact top-K (brute force / full probe) under an optional mask. This is
    /// the oracle path used by the B3-3 `Exact` / `PreFilterExact` strategies.
    pub fn exact_search(
        &self,
        query: &[f32],
        k: usize,
        mask: Option<&arrow::array::BooleanArray>,
    ) -> Result<Vec<VectorHit>> {
        match self {
            AnyIndex::Flat(i) => i.search(query, k, mask),
            AnyIndex::Ivf(i) => i.exact_search(query, k, mask),
            AnyIndex::Hnsw(i) => i.brute_search(query, k, mask),
        }
    }

    /// Serialize the concrete index payload (no container header).
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            AnyIndex::Flat(i) => i.to_bytes(),
            AnyIndex::Ivf(i) => i.to_bytes(),
            AnyIndex::Hnsw(i) => i.to_bytes(),
        }
    }

    /// Deserialize a concrete index payload of the given type.
    pub fn from_bytes(index_type: IndexType, bytes: &[u8]) -> Result<Self> {
        Ok(match index_type {
            IndexType::Flat => AnyIndex::Flat(FlatIndex::from_bytes(bytes)?),
            IndexType::Ivf => AnyIndex::Ivf(IvfIndex::from_bytes(bytes)?),
            IndexType::Hnsw => AnyIndex::Hnsw(HnswIndex::from_bytes(bytes)?),
        })
    }

    /// Build a fresh index from a corpus.
    pub fn build(
        ids: Vec<u64>,
        vectors: Vec<Vec<f32>>,
        metric: Metric,
        options: &BuildOptions,
    ) -> Result<Self> {
        match options {
            BuildOptions::Flat => Ok(AnyIndex::Flat(FlatIndex::with_metric(
                ids, vectors, metric,
            )?)),
            BuildOptions::Ivf { nlist, nprobe } => {
                let dim = vectors.first().map(Vec::len).unwrap_or(0);
                let mut data = Vec::with_capacity(vectors.len() * dim);
                for v in &vectors {
                    data.extend_from_slice(v);
                }
                Ok(AnyIndex::Ivf(IvfIndex::with_metric(
                    ids, data, dim, *nlist, *nprobe, metric,
                )?))
            }
            BuildOptions::Hnsw {
                m,
                ef_construction,
                ef_search,
            } => Ok(AnyIndex::Hnsw(HnswIndex::build_with_metric(
                ids,
                vectors,
                *m,
                *ef_construction,
                *ef_search,
                metric,
            )?)),
        }
    }
}

impl VectorIndex for AnyIndex {
    fn metric(&self) -> Metric {
        match self {
            AnyIndex::Flat(i) => i.metric(),
            AnyIndex::Ivf(i) => i.metric(),
            AnyIndex::Hnsw(i) => i.metric(),
        }
    }

    fn dim(&self) -> usize {
        match self {
            AnyIndex::Flat(i) => i.dim(),
            AnyIndex::Ivf(i) => i.dim(),
            AnyIndex::Hnsw(i) => i.dim(),
        }
    }

    fn search(
        &self,
        query: &[f32],
        k: usize,
        filter_mask: Option<&BooleanArray>,
    ) -> Result<Vec<VectorHit>> {
        match self {
            AnyIndex::Flat(i) => i.search(query, k, filter_mask),
            AnyIndex::Ivf(i) => i.search(query, k, filter_mask),
            AnyIndex::Hnsw(i) => i.search(query, k, filter_mask),
        }
    }
}
