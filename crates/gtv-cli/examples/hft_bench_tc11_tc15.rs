//! TC11–TC15 micro-structure kernel benchmark (single-threaded, min-of-N).

use std::hint::black_box;
use std::time::Instant;

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
}

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

fn main() {
    let n = 1_000_000usize;
    let mut rng = SplitMix64::new(0xC0FFEE);
    let price: Vec<f64> = (0..n).map(|_| 150.0 + (rng.next_u64() % 1000) as f64 / 100.0).collect();
    let bid: Vec<f64> = price.iter().map(|p| p - 0.01).collect();
    let ask: Vec<f64> = price.iter().map(|p| p + 0.01).collect();
    let bid_sz: Vec<f64> = (0..n).map(|_| (1 + rng.next_u64() % 100) as f64).collect();
    let ask_sz: Vec<f64> = (0..n).map(|_| (1 + rng.next_u64() % 100) as f64).collect();
    let flags: Vec<&str> = (0..n)
        .map(|i| if i % 2 == 0 { "BUY" } else { "SELL" })
        .collect();

    println!("== TC11-TC15 single-thread, {} rows ==", n);

    let us = bench_us(5, || { black_box(gtv_array::micro::tick_rule(&price)); });
    println!("TC12 tick_rule       : {:8.2} ns/row", us * 1e3 / n as f64);

    let us = bench_us(5, || { black_box(gtv_array::micro::lee_ready(&price, &bid, &ask)); });
    println!("TC11 lee_ready       : {:8.2} ns/row", us * 1e3 / n as f64);

    let us = bench_us(5, || { black_box(gtv_array::micro::emo(&price, &bid, &ask)); });
    println!("TC13 emo             : {:8.2} ns/row", us * 1e3 / n as f64);

    let us = bench_us(5, || {
        black_box(gtv_array::micro::ofi_l1(&bid, &bid_sz, &ask, &ask_sz));
    });
    println!("TC14 ofi_l1          : {:8.2} ns/row", us * 1e3 / n as f64);

    let us = bench_us(5, || { black_box(gtv_array::micro::aggressor_flag(&flags)); });
    println!("TC15 aggressor_flag  : {:8.2} ns/row", us * 1e3 / n as f64);
}
