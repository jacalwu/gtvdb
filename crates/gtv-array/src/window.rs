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

/// Fused order-flow imbalance + rolling sum: `OFI_t = bid_sz·Δbid − ask_sz·Δask`
/// (with `Δ[0] = 0`), then `msum[window]`. Single pass — the intermediate `ofi`
/// array is never materialized (memory0copy.md: operator fusion).
pub fn ofi_rolling(
    bid: &[f64],
    ask: &[f64],
    bid_sz: &[f64],
    ask_sz: &[f64],
    window: usize,
) -> Vec<f64> {
    assert!(window >= 1, "window size must be >= 1");
    assert_eq!(bid.len(), ask.len());
    assert_eq!(bid.len(), bid_sz.len());
    assert_eq!(bid.len(), ask_sz.len());
    let n = bid.len();
    let mut out = Vec::with_capacity(n);
    if n == 0 {
        return out;
    }
    let mut ring = vec![0.0f64; window];
    let mut win = 0.0f64;
    let mut pos = 0usize;
    let mut prev_bid = bid[0];
    let mut prev_ask = ask[0];
    for i in 0..n {
        let d_bid = if i == 0 { 0.0 } else { bid[i] - prev_bid };
        let d_ask = if i == 0 { 0.0 } else { ask[i] - prev_ask };
        prev_bid = bid[i];
        prev_ask = ask[i];
        let o = bid_sz[i] * d_bid - ask_sz[i] * d_ask;
        win += o;
        if pos >= window {
            win -= ring[pos % window];
        }
        ring[pos % window] = o;
        pos += 1;
        out.push(win);
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

    #[test]
    fn ofi_rolling_matches_two_pass() {
        let bid = vec![100.0, 100.1, 100.3, 100.2];
        let ask = vec![100.2, 100.3, 100.5, 100.4];
        let bsz = vec![10.0, 20.0, 30.0, 40.0];
        let asz = vec![5.0, 15.0, 25.0, 35.0];
        // Reference: materialize OFI then msum[2].
        let ofi: Vec<f64> = (0..bid.len())
            .map(|i| {
                let db = if i == 0 { 0.0 } else { bid[i] - bid[i - 1] };
                let da = if i == 0 { 0.0 } else { ask[i] - ask[i - 1] };
                bsz[i] * db - asz[i] * da
            })
            .collect();
        assert_eq!(ofi_rolling(&bid, &ask, &bsz, &asz, 2), msum(&ofi, 2));
    }
}
