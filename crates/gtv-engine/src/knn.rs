//! Exact K-NN over named vector collections, exposed to SQL as a table function.
//!
//! `knn(name, query, k [, label])` runs a brute-force L2 nearest-neighbor search
//! over a collection registered ahead of time (via [`GtvContext::register_knn`]),
//! returning `(id, distance)` rows ordered nearest-first (ties broken by
//! ascending id, matching the reference oracle in `testcase/data/gen_data.py`).
//!
//! The optional 4th argument filters by a per-vector metadata label — this is
//! the gtvdb counterpart of KDB.AI's `metadata_filtering` (e.g. genre over
//! song embeddings).

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use arrow::array::{ArrayRef, Float64Array, RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result};

use crate::expr_util::{expr_to_i64, expr_to_string};

/// A named, immutable set of vectors (plus optional metadata labels) to search.
#[derive(Debug, Clone)]
pub struct KnnCollection {
    ids: Vec<u64>,
    vectors: Vec<Vec<f32>>,
    /// Per-vector metadata label (e.g. genre), aligned with `vectors`.
    labels: Option<Vec<String>>,
}

impl KnnCollection {
    pub fn new(
        ids: Vec<u64>,
        vectors: Vec<Vec<f32>>,
        labels: Option<Vec<String>>,
    ) -> Result<Self> {
        if ids.len() != vectors.len() {
            return Err(DataFusionError::Execution(
                "knn collection: ids/vectors length mismatch".into(),
            ));
        }
        if let Some(labels) = &labels {
            if labels.len() != vectors.len() {
                return Err(DataFusionError::Execution(
                    "knn collection: labels/vectors length mismatch".into(),
                ));
            }
        }
        let Some(dim) = vectors.first().map(Vec::len) else {
            return Err(DataFusionError::Execution(
                "knn collection: empty vector set".into(),
            ));
        };
        if dim == 0 {
            return Err(DataFusionError::Execution(
                "knn collection: zero-dimension vectors".into(),
            ));
        }
        if vectors.iter().any(|v| v.len() != dim) {
            return Err(DataFusionError::Execution(
                "knn collection: inconsistent vector dimensions".into(),
            ));
        }
        Ok(Self {
            ids,
            vectors,
            labels,
        })
    }

    fn dim(&self) -> usize {
        self.vectors.first().map(Vec::len).unwrap_or(0)
    }

    /// Exact L2 K-NN, optionally restricted to vectors whose label equals
    /// `label`. Returns `(id, distance)` pairs nearest-first.
    fn search(&self, query: &[f32], k: usize, label: Option<&str>) -> Vec<(u64, f64)> {
        let mut scored: Vec<(f32, u64)> = self
            .vectors
            .iter()
            .enumerate()
            .filter(|(i, _)| match (&self.labels, label) {
                (Some(labels), Some(want)) => labels[*i] == want,
                _ => true,
            })
            .map(|(i, v)| (squared_l2(query, v), self.ids[i]))
            .collect();
        scored.sort_by(|a, b| {
            a.0.partial_cmp(&b.0)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.1.cmp(&b.1))
        });
        scored.truncate(k);
        scored
            .into_iter()
            .map(|(d2, id)| (id, (d2 as f64).sqrt()))
            .collect()
    }
}

fn squared_l2(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
}

/// The `knn` table function over a shared, mutable set of collections.
#[derive(Debug)]
pub struct KnnTableFunction {
    collections: Arc<RwLock<HashMap<String, KnnCollection>>>,
}

impl KnnTableFunction {
    pub fn new(collections: Arc<RwLock<HashMap<String, KnnCollection>>>) -> Self {
        Self { collections }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("distance", DataType::Float64, false),
        ]))
    }
}

impl TableFunctionImpl for KnnTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let exprs = args.exprs();
        let name = expr_to_string(
            exprs
                .first()
                .ok_or_else(|| DataFusionError::Execution("knn(name, query, k [, label]): missing `name`".into()))?,
        )?;
        let query = expr_to_string(
            exprs
                .get(1)
                .ok_or_else(|| DataFusionError::Execution("knn(name, query, k [, label]): missing `query`".into()))?,
        )?;
        let k = expr_to_i64(
            exprs
                .get(2)
                .ok_or_else(|| DataFusionError::Execution("knn(name, query, k [, label]): missing `k`".into()))?,
        )? as usize;
        let label = exprs.get(3).map(expr_to_string).transpose()?;

        let query: Vec<f32> = query
            .split(',')
            .map(|t| {
                t.trim().parse::<f32>().map_err(|_| {
                    DataFusionError::Execution(format!("knn: invalid query component `{t}`"))
                })
            })
            .collect::<Result<_>>()?;

        let collections = self.collections.read().map_err(|_| {
            DataFusionError::Execution("knn: collection registry poisoned".into())
        })?;
        let collection = collections.get(&name).ok_or_else(|| {
            DataFusionError::Execution(format!("knn: unknown collection `{name}`"))
        })?;

        if query.len() != collection.dim() {
            return Err(DataFusionError::Execution(format!(
                "knn: query dim {} != collection dim {}",
                query.len(),
                collection.dim()
            )));
        }

        let rows = collection.search(&query, k, label.as_deref());
        let ids: Vec<u64> = rows.iter().map(|(id, _)| *id).collect();
        let dists: Vec<f64> = rows.iter().map(|(_, d)| *d).collect();

        let batch = RecordBatch::try_new(
            Self::schema(),
            vec![
                Arc::new(UInt64Array::from(ids)) as ArrayRef,
                Arc::new(Float64Array::from(dists)) as ArrayRef,
            ],
        )?;

        Ok(Arc::new(MemTable::try_new(
            Self::schema(),
            vec![vec![batch]],
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn songs() -> KnnCollection {
        // 10 songs, 2-dim embeddings, with genre labels (mirrors gen_data.py).
        let ids: Vec<u64> = (0..10).collect();
        let vectors = vec![
            vec![0.0, 0.0], // 0 pop
            vec![0.5, 0.5], // 1 pop
            vec![5.0, 5.0], // 2 rock
            vec![5.2, 5.1], // 3 rock
            vec![1.0, 1.0], // 4 jazz
            vec![1.1, 1.0], // 5 jazz
            vec![9.0, 9.0], // 6 classical
            vec![9.1, 9.0], // 7 classical
            vec![0.2, 0.1], // 8 pop
            vec![4.9, 5.0], // 9 rock
        ];
        let labels = vec![
            "pop", "pop", "rock", "rock", "jazz", "jazz", "classical", "classical", "pop",
            "rock",
        ]
        .into_iter()
        .map(str::to_string)
        .collect();
        KnnCollection::new(ids, vectors, Some(labels)).unwrap()
    }

    #[test]
    fn songs_knn_matches_oracle() {
        let c = songs();
        // TC-11 oracle: knn(q=[0.1,0.1], k=3, no filter) = [8, 0, 1]
        let got = c.search(&[0.1, 0.1], 3, None);
        assert_eq!(got.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![8, 0, 1]);
    }

    #[test]
    fn songs_knn_pop_filter_matches_oracle() {
        let c = songs();
        // TC-11 oracle: mask=pop = [8, 0, 1]
        let got = c.search(&[0.1, 0.1], 3, Some("pop"));
        assert_eq!(got.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![8, 0, 1]);
    }

    #[test]
    fn songs_knn_rock_filter_matches_oracle() {
        let c = songs();
        // TC-11 oracle: q=[5.0,5.0], mask=rock = [2, 9, 3]
        let got = c.search(&[5.0, 5.0], 3, Some("rock"));
        assert_eq!(got.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![2, 9, 3]);
    }

    #[test]
    fn tss_knn_matches_oracle() {
        // TC-07 oracle: knn(q=noisy_uptrend, k=4) = [0, 2, 1, 3]
        let vectors = vec![
            vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0], // 0 uptrend
            vec![5.0, 4.0, 2.0, 2.0, 4.0, 5.0], // 1 dip
            vec![3.0, 3.0, 3.0, 3.0, 3.0, 3.0], // 2 flat
            vec![6.0, 5.0, 4.0, 3.0, 2.0, 1.0], // 3 downtrend
        ];
        let c = KnnCollection::new((0..4).collect(), vectors, None).unwrap();
        let q = [1.1, 2.0, 3.1, 4.0, 5.1, 6.0];
        let got = c.search(&q, 4, None);
        assert_eq!(got.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![0, 2, 1, 3]);
        let top1 = c.search(&q, 1, None);
        assert_eq!(top1.iter().map(|(id, _)| *id).collect::<Vec<_>>(), vec![0]);
    }
}
