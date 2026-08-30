//! Quant kernels (roadmap phase 2): Black-Scholes price & Greeks, historical
//! VaR, L2 order-book reconstruction and a symmetric-matrix Jacobi
//! eigendecomposition for PCA.

use std::collections::{BTreeMap, HashMap};

// ---------------------------------------------------------------------------
// Normal distribution helpers (Abramowitz–Stegun 7.1.26 erf approximation)
// ---------------------------------------------------------------------------

#[inline]
fn erf(x: f64) -> f64 {
    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let x = x.abs();
    let t = 1.0 / (1.0 + 0.3275911 * x);
    let y = 1.0
        - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t
            + 0.254829592)
            * t
            * (-x * x).exp();
    sign * y
}

#[inline]
fn norm_cdf(x: f64) -> f64 {
    0.5 * (1.0 + erf(x / std::f64::consts::SQRT_2))
}

#[inline]
fn norm_pdf(x: f64) -> f64 {
    (-0.5 * x * x).exp() / (2.0 * std::f64::consts::PI).sqrt()
}

/// `true` for a call, `false` for a put (case-insensitive `c`/`p` first char).
#[inline]
fn is_call(option_type: &str) -> bool {
    option_type
        .chars()
        .next()
        .map(|c| c.eq_ignore_ascii_case(&'c'))
        .unwrap_or(true)
}

/// Black-Scholes `d1`.
#[inline]
fn bs_d1(s: f64, k: f64, t: f64, r: f64, v: f64) -> f64 {
    let vol_sqrt_t = v * t.sqrt();
    ((s / k).ln() + (r + 0.5 * v * v) * t) / vol_sqrt_t
}

/// Black-Scholes option price (call or put).
pub fn bs_price(option_type: &str, s: f64, k: f64, t: f64, r: f64, v: f64) -> f64 {
    if t <= 0.0 || v <= 0.0 {
        let intrinsic = if is_call(option_type) {
            (s - k).max(0.0)
        } else {
            (k - s).max(0.0)
        };
        return intrinsic;
    }
    let d1 = bs_d1(s, k, t, r, v);
    let d2 = d1 - v * t.sqrt();
    let df = (-r * t).exp();
    if is_call(option_type) {
        s * norm_cdf(d1) - k * df * norm_cdf(d2)
    } else {
        k * df * norm_cdf(-d2) - s * norm_cdf(-d1)
    }
}

/// Black-Scholes delta.
pub fn bs_delta(option_type: &str, s: f64, k: f64, t: f64, r: f64, v: f64) -> f64 {
    if t <= 0.0 || v <= 0.0 {
        return if is_call(option_type) {
            if s > k { 1.0 } else { 0.0 }
        } else if s < k {
            -1.0
        } else {
            0.0
        };
    }
    let d1 = bs_d1(s, k, t, r, v);
    if is_call(option_type) {
        norm_cdf(d1)
    } else {
        norm_cdf(d1) - 1.0
    }
}

/// Black-Scholes gamma (identical for call/put).
pub fn bs_gamma(_option_type: &str, s: f64, k: f64, t: f64, r: f64, v: f64) -> f64 {
    if t <= 0.0 || v <= 0.0 || s <= 0.0 {
        return 0.0;
    }
    let d1 = bs_d1(s, k, t, r, v);
    norm_pdf(d1) / (s * v * t.sqrt())
}

/// Black-Scholes vega (per 1.0 vol; identical for call/put).
pub fn bs_vega(_option_type: &str, s: f64, k: f64, t: f64, r: f64, v: f64) -> f64 {
    if t <= 0.0 || v <= 0.0 {
        return 0.0;
    }
    let d1 = bs_d1(s, k, t, r, v);
    s * norm_pdf(d1) * t.sqrt()
}

/// Black-Scholes theta (per year).
pub fn bs_theta(option_type: &str, s: f64, k: f64, t: f64, r: f64, v: f64) -> f64 {
    if t <= 0.0 || v <= 0.0 {
        return 0.0;
    }
    let d1 = bs_d1(s, k, t, r, v);
    let d2 = d1 - v * t.sqrt();
    let df = (-r * t).exp();
    let term = -(s * norm_pdf(d1) * v) / (2.0 * t.sqrt());
    if is_call(option_type) {
        term - r * k * df * norm_cdf(d2)
    } else {
        term + r * k * df * norm_cdf(-d2)
    }
}

// ---------------------------------------------------------------------------
// Historical Value-at-Risk
// ---------------------------------------------------------------------------

/// Historical VaR: the `1 - confidence` quantile of the loss distribution
/// (negative of the sorted-return quantile). `confidence` e.g. 0.95 / 0.99.
pub fn var_historical(returns: &[f64], confidence: f64) -> f64 {
    if returns.is_empty() {
        return 0.0;
    }
    let mut r = returns.to_vec();
    r.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let q = (1.0 - confidence).clamp(0.0, 1.0);
    let idx = ((r.len() - 1) as f64 * q).round() as usize;
    -r[idx.min(r.len() - 1)]
}

// ---------------------------------------------------------------------------
// L2 order-book reconstruction (MBO: add / cancel / execute)
// ---------------------------------------------------------------------------

/// One market-by-order message. `side`: `0` = buy (bid), `1` = sell (ask);
/// `action`: `0` = add, `1` = cancel, `2` = execute.
#[derive(Debug, Clone, Copy)]
pub struct MboMsg {
    pub order_id: u64,
    pub side: u8,
    pub price: f64,
    pub qty: u64,
    pub action: u8,
}

/// Reconstruct the aggregated L2 book from an MBO stream, returning the top
/// `depth` bid levels (best first) and ask levels (best first) as `(price, qty)`.
pub fn reconstruct_l2(
    msgs: &[MboMsg],
    depth: usize,
) -> (Vec<(f64, u64)>, Vec<(f64, u64)>) {
    let mut orders: HashMap<u64, (u8, u64, u64)> = HashMap::new(); // id -> (side, ticks, qty)
    let mut bids: BTreeMap<u64, u64> = BTreeMap::new(); // ticks -> agg qty
    let mut asks: BTreeMap<u64, u64> = BTreeMap::new();

    for m in msgs {
        let ticks = (m.price * 100.0).round() as u64;
        match m.action {
            0 => {
                orders.insert(m.order_id, (m.side, ticks, m.qty));
                let book = if m.side == 0 { &mut bids } else { &mut asks };
                *book.entry(ticks).or_insert(0) += m.qty;
            }
            1 => {
                if let Some((side, t, q)) = orders.remove(&m.order_id) {
                    let book = if side == 0 { &mut bids } else { &mut asks };
                    if let Some(agg) = book.get_mut(&t) {
                        *agg = agg.saturating_sub(q);
                        if *agg == 0 {
                            book.remove(&t);
                        }
                    }
                }
            }
            2 => {
                if let Some((side, ticks, q)) = orders.get_mut(&m.order_id) {
                    let fill = m.qty.min(*q);
                    *q -= fill;
                    let book = if *side == 0 { &mut bids } else { &mut asks };
                    if let Some(agg) = book.get_mut(ticks) {
                        *agg = agg.saturating_sub(fill);
                        if *agg == 0 {
                            book.remove(ticks);
                        }
                    }
                    if *q == 0 {
                        orders.remove(&m.order_id);
                    }
                }
            }
            _ => {}
        }
    }

    let bid_out: Vec<(f64, u64)> = bids
        .iter()
        .rev()
        .take(depth)
        .map(|(p, q)| (*p as f64 / 100.0, *q))
        .collect();
    let ask_out: Vec<(f64, u64)> = asks
        .iter()
        .take(depth)
        .map(|(p, q)| (*p as f64 / 100.0, *q))
        .collect();
    (bid_out, ask_out)
}

// ---------------------------------------------------------------------------
// Time bucketing + OHLCV bar aggregation (shared with roadmap Phase 5)
// ---------------------------------------------------------------------------

/// Round each timestamp down to the bucket boundary (`bucket` in the same unit
/// as `ts`, e.g. ns/µs/ms).
pub fn xbar(ts: &[i64], bucket: i64) -> Vec<i64> {
    let bucket = bucket.max(1);
    ts.iter().map(|&t| (t / bucket) * bucket).collect()
}

/// Aggregate a single-symbol tick series into OHLCV bars keyed by bucket start.
/// `ts` must be in chronological order (tick data is). Returns
/// `(bar_ts, open, high, low, close, volume)`.
pub fn ohlc_single(
    ts: &[i64],
    price: &[f64],
    volume: &[f64],
    bucket: i64,
) -> (Vec<i64>, Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>) {
    let bucket = bucket.max(1);
    let mut groups: BTreeMap<i64, Vec<usize>> = BTreeMap::new();
    for i in 0..ts.len() {
        groups
            .entry((ts[i] / bucket) * bucket)
            .or_default()
            .push(i);
    }
    let mut bar = Vec::with_capacity(groups.len());
    let mut open = Vec::with_capacity(groups.len());
    let mut high = Vec::with_capacity(groups.len());
    let mut low = Vec::with_capacity(groups.len());
    let mut close = Vec::with_capacity(groups.len());
    let mut vol = Vec::with_capacity(groups.len());
    for (b, idxs) in groups {
        let (mut h, mut l, mut v) = (f64::NEG_INFINITY, f64::INFINITY, 0.0);
        for &i in &idxs {
            h = h.max(price[i]);
            l = l.min(price[i]);
            v += volume[i];
        }
        bar.push(b);
        open.push(price[idxs[0]]);
        high.push(h);
        low.push(l);
        close.push(price[idxs[idxs.len() - 1]]);
        vol.push(v);
    }
    (bar, open, high, low, close, vol)
}

// ---------------------------------------------------------------------------
// Jacobi eigendecomposition (symmetric matrix) for PCA
// ---------------------------------------------------------------------------

/// Jacobi eigenvalue decomposition of a symmetric `m × m` matrix (flattened
/// row-major). Returns `(eigenvalues, eigenvectors)` where the eigenvectors are
/// the columns of the `m × m` matrix, both sorted by descending eigenvalue.
pub fn jacobi_eigen(cov: &[f64], m: usize, max_iter: usize) -> (Vec<f64>, Vec<f64>) {
    let mut a = cov.to_vec();
    let mut v = vec![0.0f64; m * m];
    for i in 0..m {
        v[i * m + i] = 1.0;
    }

    for _ in 0..max_iter {
        // largest off-diagonal
        let (mut p, mut q, mut max_off) = (0usize, 1usize, 0.0f64);
        for i in 0..m {
            for j in (i + 1)..m {
                let x = a[i * m + j].abs();
                if x > max_off {
                    max_off = x;
                    p = i;
                    q = j;
                }
            }
        }
        if max_off < 1e-12 {
            break;
        }
        let app = a[p * m + p];
        let aqq = a[q * m + q];
        let apq = a[p * m + q];
        let theta = 0.5 * (aqq - app).atan2(2.0 * apq);
        let c = theta.cos();
        let s = theta.sin();
        // apply rotation A = J^T A J
        for i in 0..m {
            if i != p && i != q {
                let aip = a[i * m + p];
                let aiq = a[i * m + q];
                a[i * m + p] = c * aip - s * aiq;
                a[p * m + i] = a[i * m + p];
                a[i * m + q] = s * aip + c * aiq;
                a[q * m + i] = a[i * m + q];
            }
        }
        a[p * m + p] = c * c * app - 2.0 * s * c * apq + s * s * aqq;
        a[q * m + q] = s * s * app + 2.0 * s * c * apq + c * c * aqq;
        a[p * m + q] = 0.0;
        a[q * m + p] = 0.0;
        // apply rotation V = V J
        for i in 0..m {
            let vip = v[i * m + p];
            let viq = v[i * m + q];
            v[i * m + p] = c * vip - s * viq;
            v[i * m + q] = s * vip + c * viq;
        }
    }

    // eigenvalues on the diagonal; sort descending
    let mut eigen: Vec<(f64, usize)> = (0..m).map(|i| (a[i * m + i].max(0.0), i)).collect();
    eigen.sort_by(|x, y| y.0.partial_cmp(&x.0).unwrap_or(std::cmp::Ordering::Equal));
    let mut vals = vec![0.0f64; m];
    let mut vecs = vec![0.0f64; m * m];
    for (k, &(val, col)) in eigen.iter().enumerate() {
        vals[k] = val;
        for i in 0..m {
            vecs[i * m + k] = v[i * m + col];
        }
    }
    (vals, vecs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bs_call_price_matches_known() {
        // S=100, K=100, T=1, r=0.05, v=0.2 -> call ~10.4506, put ~5.5735
        let c = bs_price("call", 100.0, 100.0, 1.0, 0.05, 0.2);
        let p = bs_price("put", 100.0, 100.0, 1.0, 0.05, 0.2);
        assert!((c - 10.4506).abs() < 0.01, "call={c}");
        assert!((p - 5.5735).abs() < 0.01, "put={p}");
    }

    #[test]
    fn bs_put_call_parity() {
        // C - P = S - K e^{-rT}
        let s = 120.0;
        let k = 110.0;
        let t = 0.5;
        let r = 0.03;
        let v = 0.25;
        let c = bs_price("c", s, k, t, r, v);
        let p = bs_price("p", s, k, t, r, v);
        assert!((c - p - (s - k * (-r * t).exp())).abs() < 1e-6);
    }

    #[test]
    fn var_is_negative_of_quantile() {
        // returns -0.10, -0.05, 0.00, 0.05, 0.10; 80% VaR = -(-0.05) = 0.05
        let r = [-0.10, -0.05, 0.00, 0.05, 0.10];
        assert!((var_historical(&r, 0.8) - 0.05).abs() < 1e-9);
    }

    #[test]
    fn reconstruct_l2_builds_book() {
        let msgs = [
            MboMsg { order_id: 1, side: 1, price: 100.0, qty: 10, action: 0 },
            MboMsg { order_id: 2, side: 1, price: 100.0, qty: 5, action: 0 },
            MboMsg { order_id: 3, side: 0, price: 99.0, qty: 7, action: 0 },
            MboMsg { order_id: 1, side: 1, price: 100.0, qty: 0, action: 1 },
        ];
        let (bids, asks) = reconstruct_l2(&msgs, 5);
        assert_eq!(asks, vec![(100.0, 5)]);
        assert_eq!(bids, vec![(99.0, 7)]);
    }

    #[test]
    fn jacobi_diagonalizes_identity() {
        let cov = [1.0, 0.0, 0.0, 1.0]; // 2x2 identity
        let (vals, _) = jacobi_eigen(&cov, 2, 50);
        assert!((vals[0] - 1.0).abs() < 1e-9);
        assert!((vals[1] - 1.0).abs() < 1e-9);
    }

    #[test]
    fn xbar_buckets_down() {
        let ts = [0, 59, 60, 119, 120];
        assert_eq!(xbar(&ts, 60), vec![0, 0, 60, 60, 120]);
    }

    #[test]
    fn ohlc_aggregates_bars() {
        // two 1-second bars: [0, 0.5] and [1.0, 1.5]
        let ts = [0, 500_000_000, 1_000_000_000, 1_500_000_000];
        let px = [10.0, 11.0, 9.0, 8.0];
        let vol = [1.0, 2.0, 3.0, 4.0];
        let (bar, o, h, l, c, v) = ohlc_single(&ts, &px, &vol, 1_000_000_000);
        assert_eq!(bar, vec![0, 1_000_000_000]);
        assert_eq!(o, vec![10.0, 9.0]);
        assert_eq!(h, vec![11.0, 9.0]);
        assert_eq!(l, vec![10.0, 8.0]);
        assert_eq!(c, vec![11.0, 8.0]);
        assert_eq!(v, vec![3.0, 7.0]);
    }
}
