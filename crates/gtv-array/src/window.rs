//! kdb+-style rolling-window aggregations: `mavg`, `msum`, `deltas`.

use arrow::array::Float64Array;

/// kdb `mavg[n] x`: trailing moving average over a window of `n`.
///
/// The first `n-1` elements are averaged over the available prefix (cumulative
/// average), matching kdb semantics.
pub fn mavg(values: &[f64], n: usize) -> Vec<f64> {
    assert!(n >= 1, "window size must be >= 1");
    let mut out = Vec::with_capacity(values.len());
    let mut window_sum = 0.0f64;
    for (i, &v) in values.iter().enumerate() {
        window_sum += v;
        if i >= n {
            window_sum -= values[i - n];
        }
        out.push(window_sum / (i + 1).min(n) as f64);
    }
    out
}

/// kdb `msum[n] x`: trailing moving sum over a window of `n`.
pub fn msum(values: &[f64], n: usize) -> Vec<f64> {
    assert!(n >= 1, "window size must be >= 1");
    let mut out = Vec::with_capacity(values.len());
    let mut window_sum = 0.0f64;
    for (i, &v) in values.iter().enumerate() {
        window_sum += v;
        if i >= n {
            window_sum -= values[i - n];
        }
        out.push(window_sum);
    }
    out
}

/// kdb `deltas x`: `x[i] - x[i-1]`, with `out[0] == x[0]`.
pub fn deltas(values: &[f64]) -> Vec<f64> {
    let mut out = Vec::with_capacity(values.len());
    for (i, &v) in values.iter().enumerate() {
        out.push(if i == 0 { v } else { v - values[i - 1] });
    }
    out
}

/// i64 moving sum.
pub fn msum_i64(values: &[i64], n: usize) -> Vec<i64> {
    assert!(n >= 1, "window size must be >= 1");
    let mut out = Vec::with_capacity(values.len());
    let mut window_sum = 0i64;
    for (i, &v) in values.iter().enumerate() {
        window_sum += v;
        if i >= n {
            window_sum -= values[i - n];
        }
        out.push(window_sum);
    }
    out
}

/// i64 deltas.
pub fn deltas_i64(values: &[i64]) -> Vec<i64> {
    let mut out = Vec::with_capacity(values.len());
    for (i, &v) in values.iter().enumerate() {
        out.push(if i == 0 { v } else { v - values[i - 1] });
    }
    out
}

/// Arrow-typed moving average.
pub fn mavg_array(arr: &Float64Array, n: usize) -> Float64Array {
    Float64Array::from(mavg(arr.values().as_ref(), n))
}

/// Arrow-typed moving sum.
pub fn msum_array(arr: &Float64Array, n: usize) -> Float64Array {
    Float64Array::from(msum(arr.values().as_ref(), n))
}

/// Arrow-typed deltas.
pub fn deltas_array(arr: &Float64Array) -> Float64Array {
    Float64Array::from(deltas(arr.values().as_ref()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mavg_matches_kdb() {
        assert_eq!(mavg(&[1.0, 2.0, 3.0, 4.0, 5.0], 3), vec![1.0, 1.5, 2.0, 3.0, 4.0]);
    }

    #[test]
    fn msum_matches_kdb() {
        assert_eq!(msum(&[1.0, 2.0, 3.0, 4.0, 5.0], 3), vec![1.0, 3.0, 6.0, 9.0, 12.0]);
    }

    #[test]
    fn deltas_matches_kdb() {
        assert_eq!(deltas(&[1.0, 2.0, 4.0, 7.0]), vec![1.0, 1.0, 2.0, 3.0]);
    }

    #[test]
    fn window_larger_than_input() {
        assert_eq!(mavg(&[1.0, 2.0, 3.0], 10), vec![1.0, 1.5, 2.0]);
    }
}
