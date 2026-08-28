//! As-of join (`kdb aj`): align timestamps against a sorted reference series.

use arrow::array::{Float64Array, Int64Array};

/// For each `left_times[i]`, return `right_values[j]` where `right_times[j]` is
/// the greatest time `<= left_times[i]`. `right_times` must be ascending; left
/// times may be in any order. Left times with no earlier right value map to `None`.
pub fn asof_join_f64(left_times: &[i64], right_times: &[i64], right_values: &[f64]) -> Vec<Option<f64>> {
    assert_eq!(
        right_times.len(),
        right_values.len(),
        "right_times and right_values must have equal length"
    );
    left_times
        .iter()
        .map(|&lt| {
            let j = right_times.partition_point(|&rt| rt <= lt);
            if j == 0 {
                None
            } else {
                Some(right_values[j - 1])
            }
        })
        .collect()
}

/// i64 counterpart of [`asof_join_f64`].
pub fn asof_join_i64(left_times: &[i64], right_times: &[i64], right_values: &[i64]) -> Vec<Option<i64>> {
    assert_eq!(
        right_times.len(),
        right_values.len(),
        "right_times and right_values must have equal length"
    );
    left_times
        .iter()
        .map(|&lt| {
            let j = right_times.partition_point(|&rt| rt <= lt);
            if j == 0 {
                None
            } else {
                Some(right_values[j - 1])
            }
        })
        .collect()
}

/// Arrow-typed as-of join for float values.
pub fn asof_join(
    left_times: &Int64Array,
    right_times: &Int64Array,
    right_values: &Float64Array,
) -> Float64Array {
    Float64Array::from(asof_join_f64(
        left_times.values().as_ref(),
        right_times.values().as_ref(),
        right_values.values().as_ref(),
    ))
}

/// Arrow-typed as-of join for integer values.
pub fn asof_join_i64_array(
    left_times: &Int64Array,
    right_times: &Int64Array,
    right_values: &Int64Array,
) -> Int64Array {
    Int64Array::from(asof_join_i64(
        left_times.values().as_ref(),
        right_times.values().as_ref(),
        right_values.values().as_ref(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn asof_basic() {
        let rt = vec![0i64, 10, 20, 30];
        let rv = vec![1.0, 2.0, 3.0, 4.0];
        let lt = vec![5i64, 10, 15, 25, 40];
        let got = asof_join_f64(&lt, &rt, &rv);
        assert_eq!(
            got,
            vec![Some(1.0), Some(2.0), Some(2.0), Some(3.0), Some(4.0)]
        );
    }

    #[test]
    fn asof_before_first_is_none() {
        let rt = vec![10i64, 20];
        let rv = vec![1.0, 2.0];
        assert_eq!(asof_join_f64(&[5], &rt, &rv), vec![None]);
    }

    #[test]
    fn asof_arrow_wrapper() {
        let lt = Int64Array::from(vec![5i64, 10]);
        let rt = Int64Array::from(vec![0i64, 10, 20]);
        let rv = Float64Array::from(vec![1.0, 2.0, 3.0]);
        let out = asof_join(&lt, &rt, &rv);
        assert_eq!(out.len(), 2);
        assert_eq!(out.value(0), 1.0);
        assert_eq!(out.value(1), 2.0);
    }
}
