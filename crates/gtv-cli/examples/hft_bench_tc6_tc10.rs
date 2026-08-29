//! HFT TC6–TC10 benchmark (mirrors hft_tc6-tc10.md).
//!
//! Single-threaded, deterministic synthetic data, min-of-N timing — the Rust
//! counterpart to `testcase/hft/hft_bench_tc6_tc10.q` so the two can be compared
//! under the same (single-core) resource budget.
//!
//! Run with:
//!   cargo run --release -p gtv-cli --example hft_bench_tc6_tc10

use std::collections::BTreeMap;
use std::hint::black_box;
use std::time::Instant;

// ---------------------------------------------------------------------------
// Deterministic PRNG (SplitMix64)
// ---------------------------------------------------------------------------

struct SplitMix64(u64);

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        SplitMix64(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

// ---------------------------------------------------------------------------
// TC6 — pre-trade risk check (branchless hard checks)
// ---------------------------------------------------------------------------

#[inline(always)]
fn tc6_compute(
    price: &[f64],
    qty: &[u64],
    smp: &[u8],
    sym: &[u32],
    mid: &[f64],
) -> u64 {
    let mut pass = 0u64;
    for i in 0..price.len() {
        let m = mid[sym[i] as usize];
        let band = (price[i] - m).abs() <= 0.05 * m;
        let maxq = qty[i] <= 10_000;
        let maxn = price[i] * qty[i] as f64 <= 10e6;
        let smp_ok = smp[i] == 0;
        // branchless: accumulate the boolean without a conditional jump.
        pass += (band & maxq & maxn & smp_ok) as u64;
    }
    pass
}

// ---------------------------------------------------------------------------
// TC7 — tick-to-trade (decode -> strategy -> encode)
// ---------------------------------------------------------------------------

#[inline(always)]
fn tc7_compute(bid: &[f64], ask: &[f64]) -> u64 {
    let mut out = 0u64;
    for i in 0..bid.len() {
        let mid = (bid[i] + ask[i]) * 0.5;
        let side = if mid < 150.5 {
            0u8
        } else if mid > 150.5 {
            1u8
        } else {
            2u8
        };
        let _opx = if side == 0 { bid[i] } else { ask[i] };
        let oqty = if side == 2 { 0u64 } else { 100u64 };
        out += (side < 2) as u64 + (oqty > 0) as u64;
    }
    out
}

// ---------------------------------------------------------------------------
// TC8 — L2 OBI + micro-price (10 levels, SIMD-friendly)
// ---------------------------------------------------------------------------

#[inline(always)]
fn tc8_compute(bp: &[f64], ap: &[f64], bs: &[f64], as_: &[f64], nsyms: usize) -> f64 {
    let mut sum = 0.0;
    for s in 0..nsyms {
        let o = s * 10;
        let mut bsum = 0.0;
        let mut asum = 0.0;
        for l in 0..10 {
            bsum += bs[o + l];
            asum += as_[o + l];
        }
        let obi = (bsum - asum) / (bsum + asum);
        let (b0, a0, bs0, as0) = (bp[o], ap[o], bs[o], as_[o]);
        let micro = (b0 * as0 + a0 * bs0) / (bs0 + as0);
        sum += obi + micro;
    }
    sum
}

// ---------------------------------------------------------------------------
// TC9 — 500x500 streaming covariance (rank-1 update)
// ---------------------------------------------------------------------------

#[inline(always)]
fn tc9_update(c: &mut [f64], r: &[f64], n: usize) {
    for i in 0..n {
        let ri = r[i];
        let row = i * n;
        for j in 0..n {
            c[row + j] = 0.999 * c[row + j] + ri * r[j];
        }
    }
}

// ---------------------------------------------------------------------------
// TC10 — local matching engine (price-time, level-aggregate book)
// ---------------------------------------------------------------------------

#[inline(always)]
fn tc10_compute(side: &[u8], is_mkt: &[u8], price: &[f64], qty: &[u64]) -> u64 {
    let mut book_b: BTreeMap<u64, u64> = BTreeMap::new();
    let mut book_a: BTreeMap<u64, u64> = BTreeMap::new();
    let mut fills = 0u64;

    for i in 0..side.len() {
        let s = side[i];
        let m = is_mkt[i];
        let p = (price[i] * 100.0).round() as u64; // integer ticks
        let mut rem = qty[i];

        if m == 1 {
            if s == 0 {
                // market buy -> hit best asks (lowest price)
                while rem > 0 && !book_a.is_empty() {
                    let best = *book_a.keys().next().unwrap();
                    let avail = book_a[&best];
                    let f = rem.min(avail);
                    rem -= f;
                    fills += 1;
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
                    fills += 1;
                    if avail == f {
                        book_b.remove(&best);
                    } else {
                        book_b.insert(best, avail - f);
                    }
                }
            }
        } else {
            // limit -> rest in book
            let book = if s == 0 { &mut book_b } else { &mut book_a };
            *book.entry(p).or_insert(0) += rem;
        }
    }
    fills
}

// ---------------------------------------------------------------------------
// Timing + report
// ---------------------------------------------------------------------------

fn bench_us<F: FnMut()>(iters: usize, mut f: F) -> f64 {
    f();
    let mut best = f64::INFINITY;
    for _ in 0..iters {
        let t = Instant::now();
        black_box(f());
        best = best.min(t.elapsed().as_secs_f64() * 1e6);
    }
    best
}

struct Row {
    tc: &'static str,
    scale: usize,
    us: f64,
    unit: &'static str,
    threshold: Option<f64>, // in ns per event (None = no threshold)
}

impl Row {
    fn per_ns(&self) -> f64 {
        self.us * 1_000.0 / self.scale as f64
    }
}

fn main() {
    let mut rows: Vec<Row> = Vec::new();

    // ---- TC6 ----
    {
        let n = 5_000_000usize;
        let nsyms = 500usize;
        let mut rng = SplitMix64::new(606);
        let price: Vec<f64> = (0..n).map(|_| 150.0 + (rng.next_u64() % 1000) as f64 / 100.0).collect();
        let qty: Vec<u64> = (0..n).map(|_| 1 + rng.next_u64() % 50_000).collect();
        let smp: Vec<u8> = (0..n).map(|_| (rng.next_u64() & 1) as u8).collect();
        let sym: Vec<u32> = (0..n).map(|_| (rng.next_u64() % nsyms as u64) as u32).collect();
        let mid: Vec<f64> = (0..nsyms).map(|_| 150.0 + (rng.next_u64() % 1000) as f64 / 100.0).collect();
        let us = bench_us(5, || {
            black_box(tc6_compute(&price, &qty, &smp, &sym, &mid));
        });
        rows.push(Row { tc: "TC6", scale: n, us, unit: "ns/order", threshold: Some(200.0) });
    }

    // ---- TC7 ----
    {
        let n = 1_000_000usize;
        let mut rng = SplitMix64::new(707);
        let bid: Vec<f64> = (0..n).map(|_| 150.0 + (rng.next_u64() % 1000) as f64 / 100.0).collect();
        let ask: Vec<f64> = bid.iter().map(|&b| b + 0.01 + (rng.next_u64() % 5) as f64 / 100.0).collect();
        let us = bench_us(5, || {
            black_box(tc7_compute(&bid, &ask));
        });
        rows.push(Row { tc: "TC7", scale: n, us, unit: "ns/packet", threshold: Some(1500.0) });
    }

    // ---- TC8 ----
    for &nsyms in &[10_000usize, 100_000, 1_000_000] {
        let mut rng = SplitMix64::new(808);
        let total = nsyms * 10;
        let base: Vec<f64> = (0..nsyms).map(|_| 150.0 + (rng.next_u64() % 1000) as f64 / 100.0).collect();
        let mut bp = vec![0.0f64; total];
        let mut ap = vec![0.0f64; total];
        let mut bs = vec![0.0f64; total];
        let mut as_ = vec![0.0f64; total];
        for s in 0..nsyms {
            let o = s * 10;
            for l in 0..10 {
                let lv = (l as f64 + 1.0) * 0.01;
                bp[o + l] = base[s] - lv;
                ap[o + l] = base[s] + lv;
                bs[o + l] = rng.next_f64();
                as_[o + l] = rng.next_f64();
            }
        }
        let us = bench_us(if nsyms >= 1_000_000 { 3 } else { 10 }, || {
            black_box(tc8_compute(&bp, &ap, &bs, &as_, nsyms));
        });
        // Threshold < 1 us TOTAL for the 10k-symbol batch -> 0.1 ns/symbol.
        let thr = if nsyms == 10_000 { Some(0.1) } else { None };
        rows.push(Row { tc: "TC8", scale: nsyms, us, unit: "ns/symbol", threshold: thr });
    }

    // ---- TC9 ----
    {
        let n = 500usize;
        let k = 100usize;
        let mut rng = SplitMix64::new(909);
        let rs: Vec<Vec<f64>> = (0..k).map(|_| (0..n).map(|_| rng.next_f64()).collect()).collect();
        let mut c = vec![0.0f64; n * n];
        let us = bench_us(3, || {
            for rv in &rs {
                tc9_update(&mut c, rv, n);
            }
            black_box(c[n * n / 2]);
        });
        // per-tick
        let per_tick_us = us / k as f64;
        rows.push(Row { tc: "TC9", scale: k, us, unit: "ns/tick", threshold: Some(2_000_000.0) });
        black_box(per_tick_us);
    }

    // ---- TC10 ----
    {
        let n = 200_000usize;
        let mut rng = SplitMix64::new(1010);
        let side: Vec<u8> = (0..n).map(|_| (rng.next_u64() & 1) as u8).collect();
        let is_mkt: Vec<u8> = (0..n).map(|_| (rng.next_u64() % 10 == 0) as u8).collect();
        let price: Vec<f64> = (0..n).map(|_| 150.0 + (rng.next_u64() % 1000) as f64 / 100.0).collect();
        let qty: Vec<u64> = (0..n).map(|_| 1 + rng.next_u64() % 100).collect();
        let us = bench_us(3, || {
            black_box(tc10_compute(&side, &is_mkt, &price, &qty));
        });
        rows.push(Row { tc: "TC10", scale: n, us, unit: "ns/order", threshold: Some(1000.0) });
    }

    // ---- report ----
    println!();
    println!("| TC | scale | total | per-event | throughput | threshold | result |");
    println!("|----|------:|------:|----------:|-----------:|----------:|--------|");
    for r in &rows {
        let per = r.per_ns();
        let tput = r.scale as f64 / (r.us / 1e6);
        let thr = match r.threshold {
            Some(t) => format!("< {} ns", t),
            None => "-".into(),
        };
        let res = match r.threshold {
            Some(t) if per <= t => "PASS",
            Some(_) => "FAIL",
            None => "-",
        };
        println!(
            "| {} | {} | {:.3} ms | {:.2} {} | {:.0} /s | {} | {} |",
            r.tc,
            r.scale,
            r.us / 1000.0,
            per,
            r.unit,
            tput,
            thr,
            res
        );
    }
    println!();
    println!("note: per-event = total / events (amortized single-thread cost)");
}
