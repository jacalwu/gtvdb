//! TC7 / TC9 / TC10 compiled kernels: tick-to-trade, streaming covariance and
//! a price-time matching engine. Pure slice math, exposed to SQL as table
//! functions and to the benchmark as direct calls.

use std::collections::BTreeMap;

use crate::window::mavg;

/// TC7 — tick-to-trade: a mean-reversion crossover signal generates an order on
/// each crossing of the trailing `mavg[window]`.
///
/// Returns three aligned vectors: `side` (`0` = buy, `1` = sell, `2` = none),
/// `price` (order price = current price) and `qty` (fixed lot, 0 when no order).
pub fn tick_to_trade(price: &[f64], window: usize) -> (Vec<u8>, Vec<f64>, Vec<u64>) {
    let n = price.len();
    let mut side = Vec::with_capacity(n);
    let mut opx = Vec::with_capacity(n);
    let mut oqty = Vec::with_capacity(n);
    if n == 0 {
        return (side, opx, oqty);
    }
    let ma = mavg(price, window.max(1));
    let mut prev_above = price[0] >= ma[0];
    for i in 0..n {
        let above = price[i] >= ma[i];
        // Buy when price crosses above the average, sell when it crosses below.
        let s = if above != prev_above {
            if above {
                0u8
            } else {
                1u8
            }
        } else {
            2u8
        };
        side.push(s);
        opx.push(price[i]);
        oqty.push(if s == 2 { 0 } else { 100 });
        prev_above = above;
    }
    (side, opx, oqty)
}

/// TC10 — price-time priority matching over a level-aggregate book (each price
/// level holds an aggregate quantity; FIFO within a level is elided).
///
/// `side` (`0` buy / `1` sell), `is_mkt` (`1` market / `0` limit), `price`
/// (limit price), `qty`. Market orders walk the opposite book from the best
/// price; limit orders rest. Returns one `(side, price, qty)` tuple per fill.
pub fn match_orders(
    side: &[u8],
    is_mkt: &[u8],
    price: &[f64],
    qty: &[u64],
) -> (Vec<u8>, Vec<f64>, Vec<u64>) {
    let mut book_b: BTreeMap<u64, u64> = BTreeMap::new();
    let mut book_a: BTreeMap<u64, u64> = BTreeMap::new();
    let mut f_side = Vec::new();
    let mut f_price = Vec::new();
    let mut f_qty = Vec::new();

    for i in 0..side.len() {
        let s = side[i];
        let m = is_mkt[i];
        let p = (price[i] * 100.0).round() as u64; // integer ticks (cents)
        let mut rem = qty[i];

        if m == 1 {
            if s == 0 {
                // market buy -> hit best asks (lowest price)
                while rem > 0 && !book_a.is_empty() {
                    let best = *book_a.keys().next().unwrap();
                    let avail = book_a[&best];
                    let f = rem.min(avail);
                    rem -= f;
                    f_side.push(0);
                    f_price.push(best as f64 / 100.0);
                    f_qty.push(f);
                    if avail == f {
                        book_a.remove(&best);
                    } else {
                        book_a.insert(best, avail - f);
                    }
                }
            } else {
                // market sell -> hit best bids (highest price)
                while rem > 0 && !book_b.is_empty() {
                    let best = *book_b.keys().next_back().unwrap();
                    let avail = book_b[&best];
                    let f = rem.min(avail);
                    rem -= f;
                    f_side.push(1);
                    f_price.push(best as f64 / 100.0);
                    f_qty.push(f);
                    if avail == f {
                        book_b.remove(&best);
                    } else {
                        book_b.insert(best, avail - f);
                    }
                }
            }
        } else {
            let book = if s == 0 { &mut book_b } else { &mut book_a };
            *book.entry(p).or_insert(0) += rem;
        }
    }
    (f_side, f_price, f_qty)
}

/// TC9 — sample covariance matrix over `m` series of `n` observations.
///
/// `returns` is row-major `[n rows][m cols]` flattened; the result is the
/// `m × m` symmetric covariance matrix flattened row-major.
pub fn covariance_matrix(returns: &[f64], m: usize) -> Vec<f64> {
    assert!(m > 0 && returns.len() % m == 0, "returns.len() must be a multiple of m");
    let n = returns.len() / m;
    let mut cov = vec![0.0f64; m * m];
    if n < 2 {
        return cov;
    }
    // Column means.
    let mut means = vec![0.0f64; m];
    for r in 0..n {
        for c in 0..m {
            means[c] += returns[r * m + c];
        }
    }
    for c in 0..m {
        means[c] /= n as f64;
    }
    // (n-1)-normalized cross-products.
    for r in 0..n {
        for i in 0..m {
            let di = returns[r * m + i] - means[i];
            for j in 0..m {
                cov[i * m + j] += di * (returns[r * m + j] - means[j]);
            }
        }
    }
    let denom = (n - 1) as f64;
    for v in cov.iter_mut() {
        *v /= denom;
    }
    cov
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_to_trade_emits_crossovers() {
        // mavg[2] = [100, 100.5, 100.5, 100]: price crosses below at idx 2
        // (sell) and back above at idx 3 (buy).
        let px = [100.0, 101.0, 100.0, 100.0];
        let (side, _, qty) = tick_to_trade(&px, 2);
        assert_eq!(side, vec![2, 2, 1, 0]);
        assert_eq!(qty, vec![0, 0, 100, 100]);
    }

    #[test]
    fn match_orders_fills_market() {
        // one limit sell at 100 (qty 3), then a market buy of 5 -> fill 3.
        let side = vec![1u8, 0];
        let is_mkt = vec![0u8, 1];
        let price = vec![100.0, 100.0];
        let qty = vec![3u64, 5];
        let (fs, fp, fq) = match_orders(&side, &is_mkt, &price, &qty);
        assert_eq!(fp, vec![100.0]);
        assert_eq!(fq, vec![3]);
        assert_eq!(fs, vec![0]);
    }

    #[test]
    fn covariance_is_symmetric_and_positive() {
        // 2 series, 3 obs: [1,4], [2,5], [3,6] -> cov = [[1,1],[1,1]]
        let r = [1.0, 4.0, 2.0, 5.0, 3.0, 6.0];
        let c = covariance_matrix(&r, 2);
        assert!((c[0] - 1.0).abs() < 1e-12);
        assert!((c[1] - 1.0).abs() < 1e-12);
        assert!((c[2] - 1.0).abs() < 1e-12);
        assert!((c[3] - 1.0).abs() < 1e-12);
    }
}
