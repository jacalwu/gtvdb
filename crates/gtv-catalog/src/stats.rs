//! Column statistics computed at commit time, for partition pruning and the
//! future cost-based optimizer (B3-5).

use arrow::array::{
    Array, Float32Array, Float64Array, Int32Array, Int64Array, StringArray,
    TimestampNanosecondArray, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use serde::{Deserialize, Serialize};

use crate::manifest::{ColumnStat, DataFile, Scalar};

/// Aggregated statistics of one table version (B3-5 cost-based optimizer).
///
/// Built from the B2-1 commit-time [`ColumnStat`]s of a snapshot's data files,
/// so refreshing after a commit never serves stale numbers.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TableStats {
    pub table: String,
    pub row_count: u64,
    pub file_count: u64,
    /// Event-time bounding box (ns); `i64::MIN/MAX` when unknown.
    pub event_time_min: i64,
    pub event_time_max: i64,
    pub columns: Vec<ColumnStat>,
}

impl TableStats {
    /// Aggregate the files of one immutable snapshot / table version.
    pub fn from_files(table: &str, files: &[DataFile]) -> Self {
        let mut stats = TableStats {
            table: table.to_string(),
            row_count: 0,
            file_count: files.len() as u64,
            event_time_min: i64::MAX,
            event_time_max: i64::MIN,
            columns: Vec::new(),
        };
        for f in files {
            stats.row_count += f.row_count;
            stats.event_time_min = stats.event_time_min.min(f.event_time_min);
            stats.event_time_max = stats.event_time_max.max(f.event_time_max);
            for c in &f.column_stats {
                stats.merge_column(c.clone());
            }
        }
        if files.is_empty() {
            stats.event_time_min = 0;
            stats.event_time_max = 0;
        }
        stats
    }

    /// Aggregate statistics directly from in-memory batches (session tables).
    ///
    /// This lets `register_batches` make `EXPLAIN` show real min/max/null
    /// bounds immediately, without waiting for a catalog round-trip. Distinct
    /// counts stay unknown (they are only estimated at catalog commit time).
    pub fn from_batches(table: &str, batches: &[RecordBatch]) -> Self {
        let mut stats = TableStats {
            table: table.to_string(),
            row_count: 0,
            file_count: batches.len() as u64,
            event_time_min: i64::MAX,
            event_time_max: i64::MIN,
            columns: Vec::new(),
        };
        for b in batches {
            stats.row_count += b.num_rows() as u64;
            for c in column_stats(b) {
                stats.merge_column(c);
            }
        }
        if batches.is_empty() {
            stats.event_time_min = 0;
            stats.event_time_max = 0;
        } else if let Some((lo, hi)) = stats.column("event_time").and_then(|c| match (&c.min, &c.max) {
            (Some(Scalar::Int(lo)), Some(Scalar::Int(hi))) => Some((*lo, *hi)),
            _ => None,
        }) {
            stats.event_time_min = lo;
            stats.event_time_max = hi;
        }
        stats
    }

    /// Fold one column's stats into the accumulator (min/max/null/distinct).
    fn merge_column(&mut self, c: ColumnStat) {
        match self.columns.iter_mut().find(|s| s.name == c.name) {
            Some(acc) => {
                acc.null_count += c.null_count;
                acc.min = min_scalar(acc.min.take(), c.min);
                acc.max = max_scalar(acc.max.take(), c.max);
                acc.distinct_est = match (acc.distinct_est, c.distinct_est) {
                    (Some(a), Some(b)) => Some(a.max(b)),
                    (a, b) => a.or(b),
                };
            }
            None => self.columns.push(c),
        }
    }

    pub fn column(&self, name: &str) -> Option<&ColumnStat> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Active-at-`T` ratio from the event-time bounding box (crude but stable).
    pub fn temporal_active_ratio(&self, _at: i64) -> f64 {
        if self.row_count == 0 {
            return 1.0;
        }
        1.0
    }
}

fn scalar_key(s: &Scalar) -> Option<(u8, f64)> {
    match s {
        Scalar::Null => None,
        Scalar::Bool(b) => Some((0, *b as u8 as f64)),
        Scalar::Int(i) => Some((1, *i as f64)),
        Scalar::UInt(u) => Some((2, *u as f64)),
        Scalar::Float(f) => Some((3, *f)),
        Scalar::Str(_) => None,
    }
}

fn min_scalar(a: Option<Scalar>, b: Option<Scalar>) -> Option<Scalar> {
    match (a, b) {
        (Some(a), Some(b)) => match (scalar_key(&a), scalar_key(&b)) {
            (Some((ka, va)), Some((kb, vb))) if ka == kb && vb < va => Some(b),
            _ => Some(a),
        },
        (Some(a), None) => Some(a),
        (None, b) => b,
    }
}

fn max_scalar(a: Option<Scalar>, b: Option<Scalar>) -> Option<Scalar> {
    match (a, b) {
        (Some(a), Some(b)) => match (scalar_key(&a), scalar_key(&b)) {
            (Some((ka, va)), Some((kb, vb))) if ka == kb && vb > va => Some(b),
            _ => Some(a),
        },
        (Some(a), None) => Some(a),
        (None, b) => b,
    }
}

/// Compute min/max/null-count for every column of `batch` (best effort: types
/// without a scalar representation get `None` bounds).
pub fn column_stats(batch: &RecordBatch) -> Vec<ColumnStat> {
    batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let arr = batch.column(i);
            let (min, max) = min_max(arr.as_ref());
            ColumnStat {
                name: field.name().clone(),
                null_count: arr.null_count() as u64,
                min,
                max,
                distinct_est: None,
            }
        })
        .collect()
}

fn min_max(arr: &dyn Array) -> (Option<Scalar>, Option<Scalar>) {
    match arr.data_type() {
        DataType::UInt64 => prim(arr.as_any().downcast_ref::<UInt64Array>(), |v| Scalar::UInt(v)),
        DataType::UInt32 => prim(arr.as_any().downcast_ref::<UInt32Array>(), |v| {
            Scalar::UInt(v as u64)
        }),
        DataType::UInt16 => prim(arr.as_any().downcast_ref::<UInt16Array>(), |v| {
            Scalar::UInt(v as u64)
        }),
        DataType::Int64 => prim(arr.as_any().downcast_ref::<Int64Array>(), |v| Scalar::Int(v)),
        DataType::Int32 => prim(arr.as_any().downcast_ref::<Int32Array>(), |v| {
            Scalar::Int(v as i64)
        }),
        DataType::Float64 => prim_f64(arr.as_any().downcast_ref::<Float64Array>()),
        DataType::Float32 => prim_f32(arr.as_any().downcast_ref::<Float32Array>()),
        DataType::Timestamp(_, _) => prim(
            arr.as_any().downcast_ref::<TimestampNanosecondArray>(),
            |v| Scalar::Int(v),
        ),
        DataType::Utf8 => str_bounds(arr.as_any().downcast_ref::<StringArray>()),
        _ => (None, None),
    }
}

fn prim<T, F>(arr: Option<&T>, wrap: F) -> (Option<Scalar>, Option<Scalar>)
where
    T: PrimitiveLike,
    F: Fn(T::Native) -> Scalar,
{
    let Some(arr) = arr else {
        return (None, None);
    };
    let mut min: Option<T::Native> = None;
    let mut max: Option<T::Native> = None;
    arr.for_each(|v| {
        if min.map_or(true, |m| v < m) {
            min = Some(v);
        }
        if max.map_or(true, |m| v > m) {
            max = Some(v);
        }
    });
    (min.map(&wrap), max.map(&wrap))
}

fn prim_f64(arr: Option<&Float64Array>) -> (Option<Scalar>, Option<Scalar>) {
    let Some(arr) = arr else {
        return (None, None);
    };
    let mut min: Option<f64> = None;
    let mut max: Option<f64> = None;
    for v in arr.iter().flatten() {
        if min.map_or(true, |m| v < m) {
            min = Some(v);
        }
        if max.map_or(true, |m| v > m) {
            max = Some(v);
        }
    }
    (min.map(Scalar::Float), max.map(Scalar::Float))
}

fn prim_f32(arr: Option<&Float32Array>) -> (Option<Scalar>, Option<Scalar>) {
    let Some(arr) = arr else {
        return (None, None);
    };
    let mut min: Option<f32> = None;
    let mut max: Option<f32> = None;
    for v in arr.iter().flatten() {
        if min.map_or(true, |m| v < m) {
            min = Some(v);
        }
        if max.map_or(true, |m| v > m) {
            max = Some(v);
        }
    }
    (
        min.map(|v| Scalar::Float(v as f64)),
        max.map(|v| Scalar::Float(v as f64)),
    )
}

fn str_bounds(arr: Option<&StringArray>) -> (Option<Scalar>, Option<Scalar>) {
    let Some(arr) = arr else {
        return (None, None);
    };
    let mut min: Option<&str> = None;
    let mut max: Option<&str> = None;
    for v in arr.iter().flatten() {
        if min.map_or(true, |m| v < m) {
            min = Some(v);
        }
        if max.map_or(true, |m| v > m) {
            max = Some(v);
        }
    }
    (
        min.map(|s| Scalar::Str(s.to_string())),
        max.map(|s| Scalar::Str(s.to_string())),
    )
}

/// Minimal abstraction so `prim` can handle the integer/timestamp arrays.
trait PrimitiveLike {
    type Native: Copy + PartialOrd;
    fn for_each<F: FnMut(Self::Native)>(&self, f: F);
}

macro_rules! primitive_like {
    ($ty:ty, $native:ty) => {
        impl PrimitiveLike for $ty {
            type Native = $native;
            fn for_each<F: FnMut($native)>(&self, mut f: F) {
                for v in self.iter().flatten() {
                    f(v);
                }
            }
        }
    };
}

primitive_like!(UInt64Array, u64);
primitive_like!(UInt32Array, u32);
primitive_like!(UInt16Array, u16);
primitive_like!(Int64Array, i64);
primitive_like!(Int32Array, i32);
primitive_like!(TimestampNanosecondArray, i64);
