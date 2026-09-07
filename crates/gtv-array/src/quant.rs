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

/// Black-Scholes rho (per 1.0 rate; call > 0, put < 0).
pub fn bs_rho(option_type: &str, s: f64, k: f64, t: f64, r: f64, v: f64) -> f64 {
    if t <= 0.0 || v <= 0.0 {
        return 0.0;
    }
    let d1 = bs_d1(s, k, t, r, v);
    let d2 = d1 - v * t.sqrt();
    let df = (-r * t).exp();
    if is_call(option_type) {
        k * t * df * norm_cdf(d2)
    } else {
        -k * t * df * norm_cdf(-d2)
    }
}

// ---------------------------------------------------------------------------
// Technical indicators (causal / trailing-window kernels).
// NaN marks the warm-up prefix where the window is not yet full, matching
// kdb+ rolling operators (mavg/msum) when called via OVER (ORDER BY ts).
// ---------------------------------------------------------------------------

/// Exponential moving average: seed = SMA of the first `n` points at index
/// `n-1`, then `e = alpha*x + (1-alpha)*e` with `alpha = 2/(n+1)`.
pub fn ema(x: &[f64], n: usize) -> Vec<f64> {
    let mut out = vec![f64::NAN; x.len()];
    if n == 0 {
        return out;
    }
    let alpha = 2.0 / (n as f64 + 1.0);
    if x.len() >= n {
        let s: f64 = x[..n].iter().sum();
        let mut e = s / n as f64;
        out[n - 1] = e;
        for i in n..x.len() {
            e = alpha * x[i] + (1.0 - alpha) * e;
            out[i] = e;
        }
    }
    out
}

/// RSI(n) with Wilder smoothing (valid from index `n`, needs n returns).
pub fn rsi(x: &[f64], n: usize) -> Vec<f64> {
    let mut out = vec![f64::NAN; x.len()];
    if n == 0 || x.len() <= n {
        return out;
    }
    let mut ag = 0.0f64;
    let mut al = 0.0f64;
    for i in 1..=n {
        let d = x[i] - x[i - 1];
        if d > 0.0 {
            ag += d;
        } else {
            al -= d;
        }
    }
    ag /= n as f64;
    al /= n as f64;
    let rsi_of = |ag: f64, al: f64| -> f64 {
        if al <= 0.0 {
            if ag > 0.0 {
                100.0
            } else {
                50.0
            }
        } else {
            (100.0 - 100.0 / (1.0 + ag / al)).max(0.0).min(100.0)
        }
    };
    out[n] = rsi_of(ag, al);
    for i in (n + 1)..x.len() {
        let d = x[i] - x[i - 1];
        ag = (ag * (n as f64 - 1.0) + d.max(0.0)) / n as f64;
        al = (al * (n as f64 - 1.0) + (-d).max(0.0)) / n as f64;
        out[i] = rsi_of(ag, al);
    }
    out
}

/// MACD family: `(dif, dea, hist)` for (fast, slow, signal) e.g. (12,26,9).
/// `dea` = EMA(signal) of `dif`; output valid once both are warm.
pub fn macd(x: &[f64], fast: usize, slow: usize, sig: usize) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let len = x.len();
    let (fast, slow) = if fast >= slow { (slow.max(1), fast) } else { (fast, slow) };
    let sig = sig.max(1);
    let f = ema(x, fast);
    let s = ema(x, slow);
    let mut dif = vec![f64::NAN; len];
    let start = slow - 1; // both emas finite from here
    for i in start..len {
        dif[i] = f[i] - s[i];
    }
    let mut dea = vec![f64::NAN; len];
    if len >= start + sig {
        let alpha = 2.0 / (sig as f64 + 1.0);
        let m: f64 = dif[start..start + sig].iter().sum::<f64>() / sig as f64;
        dea[start + sig - 1] = m;
        for i in (start + sig)..len {
            let v = alpha * dif[i] + (1.0 - alpha) * dea[i - 1];
            dea[i] = v;
        }
    }
    let hist: Vec<f64> = (0..len).map(|i| if dea[i].is_nan() { f64::NAN } else { dif[i] - dea[i] }).collect();
    (dif, dea, hist)
}

#[inline]
fn true_range(h: &[f64], l: &[f64], c: &[f64], i: usize) -> f64 {
    if i == 0 {
        return h[0] - l[0];
    }
    let hl = h[i] - l[i];
    let hc = (h[i] - c[i - 1]).abs();
    let lc = (l[i] - c[i - 1]).abs();
    hl.max(hc).max(lc)
}

/// ATR(n): Wilder-smoothed true range (valid from index `n`).
pub fn atr(h: &[f64], l: &[f64], c: &[f64], n: usize) -> Vec<f64> {
    let mut out = vec![f64::NAN; h.len()];
    if n == 0 || h.len() <= n {
        return out;
    }
    let mut a = (1..=n).map(|i| true_range(h, l, c, i)).sum::<f64>() / n as f64;
    out[n] = a;
    for i in (n + 1)..h.len() {
        a = (a * (n as f64 - 1.0) + true_range(h, l, c, i)) / n as f64;
        out[i] = a;
    }
    out
}

/// Bollinger bands: `(mid, upper, lower)` over the trailing `n` window with
/// `k` population std-dev (valid from index `n-1`).
pub fn bollinger(c: &[f64], n: usize, k: f64) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let len = c.len();
    let (mut mid, mut up, mut lo) = (vec![f64::NAN; len], vec![f64::NAN; len], vec![f64::NAN; len]);
    if n == 0 {
        return (mid, up, lo);
    }
    for i in (n - 1)..len {
        let w = &c[i + 1 - n..=i];
        let m = w.iter().sum::<f64>() / n as f64;
        let var = w.iter().map(|v| (v - m) * (v - m)).sum::<f64>() / n as f64;
        let sd = var.sqrt();
        mid[i] = m;
        up[i] = m + k * sd;
        lo[i] = m - k * sd;
    }
    (mid, up, lo)
}

/// Rolling VWAP over the trailing `n` bars: sum(typical*vol)/sum(vol).
pub fn vwap(h: &[f64], l: &[f64], c: &[f64], v: &[f64], n: usize) -> Vec<f64> {
    let len = h.len();
    let mut out = vec![f64::NAN; len];
    if n == 0 {
        return out;
    }
    for i in (n - 1)..len {
        let (mut num, mut den) = (0.0f64, 0.0f64);
        for j in (i + 1 - n)..=i {
            let tp = (h[j] + l[j] + c[j]) / 3.0;
            num += tp * v[j];
            den += v[j];
        }
        if den > 0.0 {
            out[i] = num / den;
        }
    }
    out
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

/// Cross-sectional z-score within a partition: `(x - mean) / stddev`
/// (population stddev; zero when the partition is constant).
pub fn zscore(x: &[f64]) -> Vec<f64> {
    let n = x.len();
    if n == 0 {
        return Vec::new();
    }
    let mean = x.iter().sum::<f64>() / n as f64;
    let var = x.iter().map(|v| (v - mean) * (v - mean)).sum::<f64>() / n as f64;
    let std = var.sqrt();
    if std == 0.0 {
        return vec![0.0; n];
    }
    x.iter().map(|v| (v - mean) / std).collect()
}

/// n-period return: `x[i] / x[i-n] - 1` (0 for the first `n` rows).
pub fn momentum(x: &[f64], n: usize) -> Vec<f64> {
    let n = n.max(1);
    (0..x.len())
        .map(|i| {
            if i < n || x[i - n] == 0.0 {
                0.0
            } else {
                x[i] / x[i - n] - 1.0
            }
        })
        .collect()
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
    fn zscore_standardizes() {
        let x = [1.0, 2.0, 3.0];
        let z = zscore(&x);
        // mean=2, std=sqrt(2/3) -> z = [-1.2247, 0, 1.2247]
        assert!((z[1]).abs() < 1e-9);
        assert!((z[2] - z[2]).abs() < 1e-9);
        assert!((z[0] + z[2]).abs() < 1e-9);
    }

    #[test]
    fn momentum_is_n_period_return() {
        let x = [100.0, 101.0, 102.0, 104.0];
        let m = momentum(&x, 2);
        assert_eq!(m, vec![0.0, 0.0, 102.0 / 100.0 - 1.0, 104.0 / 101.0 - 1.0]);
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

// ---------------------------------------------------------------------------
// function3.md Wave A — trend/vol/volume/pattern/regime indicator kernels.
// All are causal trailing-window; NaN marks the warm-up prefix.
// ---------------------------------------------------------------------------

#[inline]
fn wsum(x: &[f64], lo: usize, hi: usize) -> f64 {
    x[lo..=hi].iter().sum()
}

/// EMA slope: ema(x,n)[i] - ema(x,n)[i-1].
pub fn ema_slope(x: &[f64], n: usize) -> Vec<f64> {
    let e = ema(x, n);
    let mut out = vec![f64::NAN; x.len()];
    for i in 1..x.len() {
        if !e[i].is_nan() && !e[i - 1].is_nan() {
            out[i] = e[i] - e[i - 1];
        }
    }
    out
}

/// EMA acceleration: second difference of ema.
pub fn ema_accel(x: &[f64], n: usize) -> Vec<f64> {
    let s = ema_slope(x, n);
    let mut out = vec![f64::NAN; x.len()];
    for i in 2..x.len() {
        if !s[i].is_nan() && !s[i - 1].is_nan() {
            out[i] = s[i] - s[i - 1];
        }
    }
    out
}

/// Bollinger bandwidth (up-lo)/mid, k=2.
pub fn boll_width(c: &[f64], n: usize) -> Vec<f64> {
    let (mid, up, lo) = bollinger(c, n, 2.0);
    let mut out = vec![f64::NAN; c.len()];
    for i in 0..c.len() {
        if mid[i].is_finite() && mid[i].abs() > 1e-12 {
            out[i] = (up[i] - lo[i]) / mid[i];
        }
    }
    out
}

/// Bollinger mid slope.
pub fn boll_mid_slope(c: &[f64], n: usize) -> Vec<f64> {
    let (mid, _, _) = bollinger(c, n, 2.0);
    let mut out = vec![f64::NAN; c.len()];
    for i in 1..c.len() {
        if mid[i].is_finite() && mid[i - 1].is_finite() {
            out[i] = mid[i] - mid[i - 1];
        }
    }
    out
}

/// MACD DIF-DEA distance (signal 9) — equals the standard histogram.
pub fn macd_distance(x: &[f64], fast: usize, slow: usize) -> Vec<f64> {
    macd(x, fast, slow, 9).2
}

/// MACD DIF slope (first difference of DIF).
pub fn macd_slope(x: &[f64], fast: usize, slow: usize) -> Vec<f64> {
    let dif = macd(x, fast, slow, 9).0;
    let mut out = vec![f64::NAN; x.len()];
    for i in 1..x.len() {
        if dif[i].is_finite() && dif[i - 1].is_finite() {
            out[i] = dif[i] - dif[i - 1];
        }
    }
    out
}

/// ATR ratio atr(short)/atr(long) — >1 means short-term vol expanding.
pub fn atr_ratio(h: &[f64], l: &[f64], c: &[f64], short: usize, long: usize) -> Vec<f64> {
    let a = atr(h, l, c, short);
    let b = atr(h, l, c, long);
    let mut out = vec![f64::NAN; h.len()];
    for i in 0..h.len() {
        if a[i].is_finite() && b[i].is_finite() && b[i].abs() > 1e-12 {
            out[i] = a[i] / b[i];
        }
    }
    out
}

/// Annualised historical volatility over the trailing n simple returns.
pub fn hv(c: &[f64], n: usize, ann: usize) -> Vec<f64> {
    let mut out = vec![f64::NAN; c.len()];
    if n == 0 {
        return out;
    }
    let ann = (ann.max(1)) as f64;
    for i in n..c.len() {
        let lo = i + 1 - n;
        let mut sum = 0.0;
        let mut ss = 0.0;
        for j in lo..=i {
            if j >= 1 {
                let r = c[j] / c[j - 1] - 1.0;
                sum += r;
                ss += r * r;
            }
        }
        let nn = (i - lo + 1) as f64;
        let mean = sum / nn;
        let var = (ss / nn - mean * mean).max(0.0);
        out[i] = var.sqrt() * ann.sqrt();
    }
    out
}

/// HV ratio hv(short)/hv(long) - 1.
pub fn hv_ratio(c: &[f64], short: usize, long: usize, ann: usize) -> Vec<f64> {
    let s = hv(c, short, ann);
    let l = hv(c, long, ann);
    let mut out = vec![f64::NAN; c.len()];
    for i in 0..c.len() {
        if s[i].is_finite() && l[i].is_finite() && l[i].abs() > 1e-12 {
            out[i] = s[i] / l[i] - 1.0;
        }
    }
    out
}

/// Volume spike: v / sma(v,n) - 1 (valid from index n-1).
pub fn vol_spike(v: &[f64], n: usize) -> Vec<f64> {
    let mut out = vec![f64::NAN; v.len()];
    if n == 0 {
        return out;
    }
    for i in (n - 1)..v.len() {
        let sma = wsum(v, i + 1 - n, i) / n as f64;
        if sma > 0.0 {
            out[i] = v[i] / sma - 1.0;
        }
    }
    out
}

/// On-balance volume (cumulative sign of close moves times volume).
pub fn obv(c: &[f64], v: &[f64]) -> Vec<f64> {
    let mut out = vec![0.0; c.len()];
    for i in 1..c.len() {
        let s = if c[i] > c[i - 1] { 1.0 } else if c[i] < c[i - 1] { -1.0 } else { 0.0 };
        out[i] = out[i - 1] + s * v[i];
    }
    out
}

/// OBV n-bar slope: obv[i] - obv[i-n].
pub fn obv_slope(c: &[f64], v: &[f64], n: usize) -> Vec<f64> {
    let b = obv(c, v);
    let mut out = vec![f64::NAN; c.len()];
    for i in n..c.len() {
        out[i] = b[i] - b[i - n];
    }
    out
}

/// VWAP deviation: close/vwap(n) - 1.
pub fn vwap_dev(h: &[f64], l: &[f64], c: &[f64], v: &[f64], n: usize) -> Vec<f64> {
    let w = vwap(h, l, c, v, n);
    let mut out = vec![f64::NAN; c.len()];
    for i in 0..c.len() {
        if w[i].is_finite() && w[i].abs() > 1e-12 {
            out[i] = c[i] / w[i] - 1.0;
        }
    }
    out
}

/// Strict new n-day high (today's high > max of prior n highs) -> 1/0.
pub fn hh(h: &[f64], n: usize) -> Vec<f64> {
    let mut out = vec![f64::NAN; h.len()];
    if n == 0 {
        return out;
    }
    for i in n..h.len() {
        let mx = h[i - n..i].iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        out[i] = if h[i] > mx { 1.0 } else { 0.0 };
    }
    out
}

/// Strict new n-day low -> 1/0.
pub fn ll(l: &[f64], n: usize) -> Vec<f64> {
    let mut out = vec![f64::NAN; l.len()];
    if n == 0 {
        return out;
    }
    for i in n..l.len() {
        let mn = l[i - n..i].iter().cloned().fold(f64::INFINITY, f64::min);
        out[i] = if l[i] < mn { 1.0 } else { 0.0 };
    }
    out
}

/// Candle pattern intensities in [0,1] of the bar range: green-body / red-body /
/// lower shadow / upper shadow strength.
pub fn candle_green(o: &[f64], h: &[f64], l: &[f64], c: &[f64]) -> Vec<f64> {
    candle_part(o, h, l, c, 0)
}
pub fn candle_red(o: &[f64], h: &[f64], l: &[f64], c: &[f64]) -> Vec<f64> {
    candle_part(o, h, l, c, 1)
}
pub fn candle_lower_shadow(o: &[f64], h: &[f64], l: &[f64], c: &[f64]) -> Vec<f64> {
    candle_part(o, h, l, c, 2)
}
pub fn candle_upper_shadow(o: &[f64], h: &[f64], l: &[f64], c: &[f64]) -> Vec<f64> {
    candle_part(o, h, l, c, 3)
}
fn candle_part(o: &[f64], h: &[f64], l: &[f64], c: &[f64], kind: usize) -> Vec<f64> {
    let n = o.len();
    let mut out = vec![0.0; n];
    for i in 0..n {
        let rng = (h[i] - l[i]).max(0.0);
        if rng <= 0.0 {
            continue;
        }
        let v = match kind {
            0 => if c[i] > o[i] { (c[i] - o[i]) / rng } else { 0.0 }, // green body share
            1 => if o[i] > c[i] { (o[i] - c[i]) / rng } else { 0.0 }, // red body share
            2 => (o[i].min(c[i]) - l[i]) / rng,                       // lower shadow
            _ => (h[i] - o[i].max(c[i])) / rng,                       // upper shadow
        };
        out[i] = v.clamp(0.0, 1.0);
    }
    out
}

/// Trend regime: ema(c,f) - ema(c,s) (>0 uptrend).
pub fn regime_trend(c: &[f64], f: usize, s: usize) -> Vec<f64> {
    let ef = ema(c, f);
    let es = ema(c, s);
    let mut out = vec![f64::NAN; c.len()];
    for i in 0..c.len() {
        if ef[i].is_finite() && es[i].is_finite() {
            out[i] = ef[i] - es[i];
        }
    }
    out
}

/// Vol regime: hv_ratio(short,long) — >0 expanding.
pub fn regime_vol(c: &[f64], short: usize, long: usize, ann: usize) -> Vec<f64> {
    hv_ratio(c, short, long, ann)
}

/// Event regime: 1 when volume spikes (>50% above its n-SMA) or the bar gaps
/// more than 1.5×ATR(n) from the prior close.
pub fn regime_event(o: &[f64], h: &[f64], l: &[f64], c: &[f64], v: &[f64], n: usize) -> Vec<f64> {
    let spike = vol_spike(v, n);
    let a = atr(h, l, c, n);
    let mut out = vec![f64::NAN; c.len()];
    for i in 0..c.len() {
        if !spike[i].is_finite() {
            continue;
        }
        let big_gap = if i >= 1 && c[i - 1].abs() > 1e-12 && a[i].is_finite() {
            (o[i] / c[i - 1] - 1.0).abs() >= 1.5 * (a[i] / c[i - 1])
        } else {
            false
        };
        out[i] = if spike[i] >= 0.5 || big_gap { 1.0 } else { 0.0 };
    }
    out
}

/// Opening gap vs prior close: open[i]/close[i-1] - 1 (NaN on the first bar).
pub fn gap(o: &[f64], c: &[f64]) -> Vec<f64> {
    let mut out = vec![f64::NAN; o.len()];
    for i in 1..o.len() {
        if c[i - 1].abs() > 1e-12 {
            out[i] = o[i] / c[i - 1] - 1.0;
        }
    }
    out
}

// ---------------------------------------------------------------------------
// function3.md Wave B — cross-sectional rank & rolling beta kernels.
// ---------------------------------------------------------------------------

/// Cross-sectional percentile rank (ascending, ties averaged) in [0,1].
pub fn xrank(x: &[f64]) -> Vec<f64> {
    let n = x.len();
    let mut out = vec![f64::NAN; n];
    if n == 0 {
        return out;
    }
    let mut order: Vec<usize> = (0..n).collect();
    order.sort_by(|&a, &b| x[a].partial_cmp(&x[b]).unwrap_or(std::cmp::Ordering::Equal));
    // tie-aware average ranks
    let mut i = 0usize;
    while i < n {
        let mut j = i + 1;
        while j < n && x[order[j]] == x[order[i]] {
            j += 1;
        }
        let avg = ((i + j - 1) as f64) / 2.0; // 0-based average rank
        let pct = if n > 1 { avg / (n - 1) as f64 } else { 0.5 };
        for k in order[i..j].iter() {
            out[*k] = pct;
        }
        i = j;
    }
    out
}

/// Rolling beta of x on y over the trailing n returns: cov / var(y).
pub fn rolling_beta(x: &[f64], y: &[f64], n: usize) -> Vec<f64> {
    let len = x.len().min(y.len());
    let mut out = vec![f64::NAN; len];
    if n < 2 {
        return out;
    }
    for i in n..len {
        let lo = i + 1 - n;
        let mut xs = Vec::with_capacity(n);
        let mut ys = Vec::with_capacity(n);
        let mut cnt = 0usize;
        for j in lo..=i {
            if j >= 1 && x[j - 1].abs() > 1e-12 && y[j - 1].abs() > 1e-12 {
                xs.push(x[j] / x[j - 1] - 1.0);
                ys.push(y[j] / y[j - 1] - 1.0);
                cnt += 1;
            }
        }
        if cnt < 2 {
            continue;
        }
        let sx = xs.iter().sum::<f64>();
        let sy = ys.iter().sum::<f64>();
        let mxx = sx / cnt as f64;
        let myy = sy / cnt as f64;
        let mut cov = 0.0;
        let mut vary = 0.0;
        for k in 0..cnt {
            cov += (xs[k] - mxx) * (ys[k] - myy);
            vary += (ys[k] - myy) * (ys[k] - myy);
        }
        if vary.abs() > 1e-12 {
            out[i] = cov / vary;
        }
    }
    out
}
