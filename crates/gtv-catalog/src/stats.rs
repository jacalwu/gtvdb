//! Column statistics computed at commit time, for partition pruning and the
//! future cost-based optimizer (B3-5).

use arrow::array::{
    Array, Float32Array, Float64Array, Int32Array, Int64Array, StringArray,
    TimestampNanosecondArray, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use crate::manifest::{ColumnStat, Scalar};

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
