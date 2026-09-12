//! As-of join (`kdb aj`): align timestamps against a sorted reference series.

use arrow::array::{Float64Array, Int64Array};

/// Monotonic two-pointer merge. `left_times` and `right_times` must both be
/// non-decreasing. No sortedness scan — the caller is responsible.
///
/// Caches the last matching right value so the hot loop does not re-index
/// `j - 1` or branch on `j == 0` after each advance.
fn asof_join_sorted_inner<T: Copy>(
    left_times: &[i64],
    right_times: &[i64],
    right_values: &[T],
) -> Vec<Option<T>> {
    let mut out = Vec::with_capacity(left_times.len());
    let mut j = 0usize;
    let mut last = None;
    let right_len = right_times.len();

    for &lt in left_times {
        while j < right_len && right_times[j] <= lt {
            last = Some(right_values[j]);
            j += 1;
        }
        out.push(last);
    }
    out
}

/// Order-independent as-of join: argsort the left times, one monotonic merge,
/// then restore input order. `right_times` must be non-decreasing.
///
/// This is O(n log n + n + m) and is preferred over n independent binary
/// searches when left is large (the DataFrame-engine `merge_asof` strategy).
fn asof_join_unsorted_inner<T: Copy>(
    left_times: &[i64],
    right_times: &[i64],
    right_values: &[T],
) -> Vec<Option<T>> {
    let n = left_times.len();
    if n == 0 {
        return Vec::new();
    }

    let mut order: Vec<usize> = (0..n).collect();
    order.sort_unstable_by_key(|&i| left_times[i]);

    let mut out = vec![None; n];
    let mut j = 0usize;
    let mut last = None;
    let right_len = right_times.len();

    for &i in &order {
        let lt = left_times[i];
        while j < right_len && right_times[j] <= lt {
            last = Some(right_values[j]);
            j += 1;
        }
        out[i] = last;
    }
    out
}

fn assert_right_aligned<T>(right_times: &[i64], right_values: &[T]) {
    assert_eq!(
        right_times.len(),
        right_values.len(),
        "right_times and right_values must have equal length"
    );
}

/// Sorted-left as-of join: `left_times` must be non-decreasing.
///
/// Use this when the caller already knows the left clock is ordered (kdb `aj`,
/// HDB scans) so the implementation can skip both a sortedness probe and an
/// argsort.
pub fn asof_join_sorted_f64(
    left_times: &[i64],
    right_times: &[i64],
    right_values: &[f64],
) -> Vec<Option<f64>> {
    assert_right_aligned(right_times, right_values);
    asof_join_sorted_inner(left_times, right_times, right_values)
}

/// i64 counterpart of [`asof_join_sorted_f64`].
pub fn asof_join_sorted_i64(
    left_times: &[i64],
    right_times: &[i64],
    right_values: &[i64],
) -> Vec<Option<i64>> {
    assert_right_aligned(right_times, right_values);
    asof_join_sorted_inner(left_times, right_times, right_values)
}

/// For each `left_times[i]`, return `right_values[j]` where `right_times[j]` is
/// the greatest time `<= left_times[i]`. `right_times` must be ascending; left
/// times may be in any order. Left times with no earlier right value map to `None`.
///
/// Left order is restored after an argsort + merge; there is no O(n) sortedness
/// probe. If `left_times` is already known non-decreasing, call
/// [`asof_join_sorted_f64`] instead.
pub fn asof_join_f64(
    left_times: &[i64],
    right_times: &[i64],
    right_values: &[f64],
) -> Vec<Option<f64>> {
    assert_right_aligned(right_times, right_values);
    asof_join_unsorted_inner(left_times, right_times, right_values)
}

/// i64 counterpart of [`asof_join_f64`].
pub fn asof_join_i64(
    left_times: &[i64],
    right_times: &[i64],
    right_values: &[i64],
) -> Vec<Option<i64>> {
    assert_right_aligned(right_times, right_values);
    asof_join_unsorted_inner(left_times, right_times, right_values)
}

/// Arrow-typed as-of join for float values. Left times may be unordered.
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

/// Arrow-typed as-of join for integer values. Left times may be unordered.
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
        let got = asof_join_sorted_f64(&lt, &rt, &rv);
        assert_eq!(
            got,
            vec![Some(1.0), Some(2.0), Some(2.0), Some(3.0), Some(4.0)]
        );
        assert_eq!(got, asof_join_f64(&lt, &rt, &rv));
    }

    #[test]
    fn asof_before_first_is_none() {
        let rt = vec![10i64, 20];
        let rv = vec![1.0, 2.0];
        assert_eq!(asof_join_f64(&[5], &rt, &rv), vec![None]);
        assert_eq!(asof_join_sorted_f64(&[5], &rt, &rv), vec![None]);
    }

    #[test]
    fn asof_empty_sides() {
        let rt = vec![0i64, 10];
        let rv = vec![1.0, 2.0];
        assert!(asof_join_f64(&[], &rt, &rv).is_empty());
        assert!(asof_join_sorted_f64(&[], &rt, &rv).is_empty());
        assert_eq!(asof_join_f64(&[1, 2], &[], &[]), vec![None, None]);
        assert_eq!(asof_join_sorted_f64(&[1, 2], &[], &[]), vec![None, None]);
    }

    #[test]
    fn asof_duplicate_left_times() {
        let rt = vec![0i64, 10, 20];
        let rv = vec![1.0, 2.0, 3.0];
        let lt = vec![10i64, 10, 10];
        let got = asof_join_sorted_f64(&lt, &rt, &rv);
        assert_eq!(got, vec![Some(2.0), Some(2.0), Some(2.0)]);
    }

    #[test]
    fn asof_unsorted_left_matches_sorted() {
        let rt = vec![0i64, 10, 20, 30];
        let rv = vec![1.0, 2.0, 3.0, 4.0];
        let lt = vec![25i64, 5, 40, 10, 15];
        let got = asof_join_f64(&lt, &rt, &rv);
        assert_eq!(
            got,
            vec![Some(3.0), Some(1.0), Some(4.0), Some(2.0), Some(2.0)]
        );
        let mut sorted_lt = lt.clone();
        sorted_lt.sort_unstable();
        let sorted_got = asof_join_sorted_f64(&sorted_lt, &rt, &rv);
        assert_eq!(
            sorted_got,
            vec![Some(1.0), Some(2.0), Some(2.0), Some(3.0), Some(4.0)]
        );
    }

    #[test]
    fn asof_i64_sorted_and_unsorted() {
        let rt = vec![0i64, 10, 20];
        let rv = vec![7i64, 8, 9];
        assert_eq!(
            asof_join_sorted_i64(&[5, 20], &rt, &rv),
            vec![Some(7), Some(9)]
        );
        assert_eq!(
            asof_join_i64(&[20, 5], &rt, &rv),
            vec![Some(9), Some(7)]
        );
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
