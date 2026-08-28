//! Exact brute-force K-NN index (reference implementation for tests).

use arrow::array::{BooleanArray, UInt64Array};
use gtv_core::{GtvError, Result, VectorIndex};

/// Exact K-NN via a linear scan over every vector, optionally restricted to
/// the nodes allowed by a [`BooleanArray`] bitmask.
///
/// The bitmask is indexed by the *position* of the vector in the index
/// (0..n); it is the caller's job to build a mask aligned with that layout.
#[derive(Debug, Clone)]
pub struct FlatIndex {
    ids: Vec<u64>,
    vectors: Vec<Vec<f32>>,
    dim: usize,
}

impl FlatIndex {
    pub fn new(ids: Vec<u64>, vectors: Vec<Vec<f32>>) -> Result<Self> {
        if ids.len() != vectors.len() {
            return Err(GtvError::InvalidArgument(
                "ids and vectors length mismatch".into(),
            ));
        }
        let Some(first) = vectors.first() else {
            return Err(GtvError::InvalidArgument("empty index".into()));
        };
        let dim = first.len();
        if dim == 0 {
            return Err(GtvError::InvalidArgument("zero-dimension vectors".into()));
        }
        for v in &vectors {
            if v.len() != dim {
                return Err(GtvError::InvalidArgument(
                    "inconsistent vector dimensions".into(),
                ));
            }
        }
        Ok(Self { ids, vectors, dim })
    }

    pub fn len(&self) -> usize {
        self.vectors.len()
    }

    pub fn is_empty(&self) -> bool {
        self.vectors.is_empty()
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    fn squared_l2(a: &[f32], b: &[f32]) -> f32 {
        a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
    }
}

impl VectorIndex for FlatIndex {
    fn search_knn(
        &self,
        query: &[f32],
        k: usize,
        filter_mask: Option<&BooleanArray>,
    ) -> Result<UInt64Array> {
        if query.len() != self.dim {
            return Err(GtvError::InvalidArgument(format!(
                "query dim {} != index dim {}",
                query.len(),
                self.dim
            )));
        }
        if let Some(mask) = filter_mask {
            if mask.len() != self.vectors.len() {
                return Err(GtvError::InvalidArgument(
                    "filter mask length mismatch".into(),
                ));
            }
        }
        if k == 0 {
            return Ok(UInt64Array::from(Vec::<u64>::new()));
        }

        let mut scored: Vec<(f32, u64)> = (0..self.vectors.len())
            .filter(|&i| filter_mask.map_or(true, |m| m.value(i)))
            .map(|i| (Self::squared_l2(query, &self.vectors[i]), self.ids[i]))
            .collect();
        scored.sort_by(|a, b| {
            a.0.partial_cmp(&b.0)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.1.cmp(&b.1))
        });
        scored.truncate(k);

        Ok(UInt64Array::from(
            scored.into_iter().map(|(_, id)| id).collect::<Vec<u64>>(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::BooleanArray;

    fn idx() -> FlatIndex {
        // 3 nodes, 2 dims: (0,0), (1,1), (5,5)
        FlatIndex::new(
            vec![0, 1, 2],
            vec![
                vec![0.0, 0.0],
                vec![1.0, 1.0],
                vec![5.0, 5.0],
            ],
        )
        .unwrap()
    }

    #[test]
    fn knn_orders_by_distance() {
        let index = idx();
        let got = index.search_knn(&[0.1, 0.1], 3, None).unwrap();
        assert_eq!(got.values().as_ref(), &[0, 1, 2]);
    }

    #[test]
    fn knn_respects_bitmask() {
        let index = idx();
        let mask = BooleanArray::from(vec![false, true, true]);
        let got = index.search_knn(&[5.0, 5.0], 3, Some(&mask)).unwrap();
        assert_eq!(got.values().as_ref(), &[2, 1]);
    }

    #[test]
    fn knn_truncates_to_k() {
        let index = idx();
        let got = index.search_knn(&[0.0, 0.0], 2, None).unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got.values().as_ref(), &[0, 1]);
    }

    #[test]
    fn rejects_dimension_mismatch() {
        let index = idx();
        assert!(index.search_knn(&[1.0], 2, None).is_err());
    }
}
