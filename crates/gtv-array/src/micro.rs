//! Market micro-structure classification kernels (TC11–TC15).
//!
//! These mirror the kdb+/q vectorized implementations in `hft-tc11-tc15.md`:
//! tick rule, Lee-Ready, EMO, level-1 order-flow imbalance and the exchange
//! aggressor flag. All return `i32` direction codes (`1` = buy, `-1` = sell,
//! `0` = unclassified) except OFI which returns `f64`.

/// Sign of `v` as `-1`/`0`/`1`. (Unlike `f64::signum`, zero maps to `0` — Rust's
/// `signum` returns `1.0` for `+0.0`, which breaks the tick rule's "no change".)
#[inline]
fn sign(v: f64) -> i32 {
    if v > 0.0 {
        1
    } else if v < 0.0 {
        -1
    } else {
        0
    }
}

/// TC12 — tick rule: `signum(px[i] - px[i-1])`, first row = 0, and a zero
/// change carries the previous non-zero direction forward (`fills` in q).
pub fn tick_rule(px: &[f64]) -> Vec<i32> {
    let mut out = Vec::with_capacity(px.len());
    let mut last = 0i32;
    for i in 0..px.len() {
        let d = if i == 0 {
            0
        } else {
            sign(px[i] - px[i - 1])
        };
        if d != 0 {
            last = d;
        }
        out.push(last);
    }
    out
}

/// TC11 — Lee-Ready: quote rule (sign of `px - mid`), falling back to the tick
/// rule when the trade is exactly at the mid.
pub fn lee_ready(px: &[f64], bid: &[f64], ask: &[f64]) -> Vec<i32> {
    let tick = tick_rule(px);
    let mut out = Vec::with_capacity(px.len());
    for i in 0..px.len() {
        let mid = 0.5 * (bid[i] + ask[i]);
        let q = sign(px[i] - mid);
        out.push(if q != 0 { q } else { tick[i] });
    }
    out
}

/// TC13 — EMO (Ellis–O'Hara–Thomas): `+1` at the ask, `-1` at the bid,
/// otherwise the tick rule.
pub fn emo(px: &[f64], bid: &[f64], ask: &[f64]) -> Vec<i32> {
    let tick = tick_rule(px);
    let mut out = Vec::with_capacity(px.len());
    for i in 0..px.len() {
        let e = if px[i] == ask[i] {
            1
        } else if px[i] == bid[i] {
            -1
        } else {
            0
        };
        out.push(if e != 0 { e } else { tick[i] });
    }
    out
}

/// TC14 — level-1 order-flow imbalance (Cont et al. "half" convention):
/// `dB = +bid_sz` on bid up / `0` on bid down / `Δbid_sz` on unchanged,
/// `dA = 0` on ask up / `+ask_sz` on ask down / `Δask_sz` on unchanged,
/// `OFI = dB - dA`. First row has no lag → `0.0`.
pub fn ofi_l1(bid_px: &[f64], bid_sz: &[f64], ask_px: &[f64], ask_sz: &[f64]) -> Vec<f64> {
    let n = bid_px.len();
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        if i == 0 {
            out.push(0.0);
            continue;
        }
        let dbid = bid_px[i] - bid_px[i - 1];
        let dask = ask_px[i] - ask_px[i - 1];
        let db = if dbid > 0.0 {
            bid_sz[i]
        } else if dbid < 0.0 {
            0.0
        } else {
            bid_sz[i] - bid_sz[i - 1]
        };
        let da = if dask > 0.0 {
            0.0
        } else if dask < 0.0 {
            ask_sz[i]
        } else {
            ask_sz[i] - ask_sz[i - 1]
        };
        out.push(db - da);
    }
    out
}

/// TC15 — exchange native aggressor flag: `B`/`BUY` → `1`, `S`/`SELL` → `-1`.
pub fn aggressor_flag(flags: &[&str]) -> Vec<i32> {
    flags
        .iter()
        .map(|f| match *f {
            "B" | "BUY" => 1,
            "S" | "SELL" => -1,
            _ => 0,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_rule_forward_fills_zero_changes() {
        // 180.50 -> 180.55 -> 180.55 -> 180.45 -> 180.45 -> 180.50
        let px = [180.50, 180.55, 180.55, 180.45, 180.45, 180.50];
        assert_eq!(tick_rule(&px), vec![0, 1, 1, -1, -1, 1]);
    }

    #[test]
    fn lee_ready_and_emo_match_doc_example() {
        let px = [180.50, 180.55, 180.55, 180.45, 180.45, 180.50];
        let bid = [180.40, 180.50, 180.50, 180.40, 180.40, 180.45];
        let ask = [180.60, 180.60, 180.55, 180.50, 180.50, 180.55];
        assert_eq!(lee_ready(&px, &bid, &ask), vec![0, 1, 1, -1, -1, 1]);
        assert_eq!(emo(&px, &bid, &ask), vec![0, 1, 1, -1, -1, 1]);
    }

    #[test]
    fn ofi_l1_matches_q_formula() {
        // Aligned quote: (bid, ask, bid_sz, ask_sz)
        let bid = [180.40, 180.50, 180.50, 180.40, 180.40, 180.45];
        let ask = [180.60, 180.60, 180.55, 180.50, 180.50, 180.55];
        let bsz = [100.0, 200.0, 150.0, 300.0, 200.0, 250.0];
        let asz = [200.0, 150.0, 100.0, 100.0, 150.0, 100.0];
        assert_eq!(
            ofi_l1(&bid, &bsz, &ask, &asz),
            vec![0.0, 250.0, -150.0, -100.0, -150.0, 250.0]
        );
    }

    #[test]
    fn aggressor_flag_parses_both_forms() {
        assert_eq!(aggressor_flag(&["BUY", "SELL", "B", "S", "X"]), vec![1, -1, 1, -1, 0]);
    }
}
