//! As-of join (`kdb aj`): align timestamps against a sorted reference series.

use arrow::array::{Float64Array, Int64Array};

/// As-of-join core: for each `left_times[i]`, return `right_values[j]` where
/// `right_times[j]` is the greatest time `<= left_times[i]`.
///
/// `right_times` must be ascending. When `left_times` is also ascending (the
/// common kdb `aj` case) a monotonic two-pointer merge runs in O(m+n) with a
/// single sequential pass over both sides; otherwise each left time is resolved
/// by binary search in O(m log n). The ascending fast path is detected with a
/// short-circuiting O(m) scan (early-exits at the first inversion).
fn asof_join_inner<T: Copy>(
    left_times: &[i64],
    right_times: &[i64],
    right_values: &[T],
) -> Vec<Option<T>> {
    let mut out = Vec::with_capacity(left_times.len());

    if left_times.windows(2).all(|w| w[0] <= w[1]) {
        // Fast path: both sides sorted -> single monotonic merge, cache-friendly.
        let mut j = 0usize;
        for &lt in left_times {
            while j < right_times.len() && right_times[j] <= lt {
                j += 1;
            }
            out.push(if j == 0 { None } else { Some(right_values[j - 1]) });
        }
        return out;
    }

    // General path: order-independent binary search per left time.
    for &lt in left_times {
        let j = right_times.partition_point(|&rt| rt <= lt);
        out.push(if j == 0 { None } else { Some(right_values[j - 1]) });
    }
    out
}

/// For each `left_times[i]`, return `right_values[j]` where `right_times[j]` is
/// the greatest time `<= left_times[i]`. `right_times` must be ascending; left
/// times may be in any order. Left times with no earlier right value map to `None`.
pub fn asof_join_f64(left_times: &[i64], right_times: &[i64], right_values: &[f64]) -> Vec<Option<f64>> {
    assert_eq!(
        right_times.len(),
        right_values.len(),
        "right_times and right_values must have equal length"
    );
    asof_join_inner(left_times, right_times, right_values)
}

/// i64 counterpart of [`asof_join_f64`].
pub fn asof_join_i64(left_times: &[i64], right_times: &[i64], right_values: &[i64]) -> Vec<Option<i64>> {
    assert_eq!(
        right_times.len(),
        right_values.len(),
        "right_times and right_values must have equal length"
    );
    asof_join_inner(left_times, right_times, right_values)
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
    fn asof_unsorted_left_matches_sorted() {
        // Unsorted left must fall back to binary search and still align
        // per-element (each left time maps to the same right value regardless
        // of its position in the input).
        let rt = vec![0i64, 10, 20, 30];
        let rv = vec![1.0, 2.0, 3.0, 4.0];
        let lt = vec![25i64, 5, 40, 10, 15];
        let got = asof_join_f64(&lt, &rt, &rv);
        assert_eq!(
            got,
            vec![Some(3.0), Some(1.0), Some(4.0), Some(2.0), Some(2.0)]
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
