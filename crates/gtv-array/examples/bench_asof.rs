//! Micro-benchmark for `asof_join_f64` at the 1M-row scale.
//!
//! Run with: `cargo run --release -p gtv-array --example bench_asof`
//!
//! Measures the ascending-left fast path (monotonic two-pointer merge) against
//! the general order-independent path (binary search per left time).

use std::time::Instant;

use gtv_array::asof::asof_join_f64;

fn timeit(name: &str, f: impl FnOnce() -> Vec<Option<f64>>) {
    let t = Instant::now();
    let out = f();
    let dt = t.elapsed();
    let matched = out.iter().flatten().count();
    println!(
        "{name:34} {:>8.2} ms  (rows={}, matched={})",
        dt.as_secs_f64() * 1e3,
        out.len(),
        matched
    );
}

fn main() {
    const N: usize = 1_000_000;

    let left_sorted: Vec<i64> = (0..N as i64).map(|i| i * 2).collect();
    let right: Vec<i64> = (0..N as i64).map(|i| i * 2 + 1).collect();
    let right_vals: Vec<f64> = right.iter().map(|&r| r as f64).collect();

    println!("== N = {N} ==");
    timeit("sorted left (two-pointer merge)", || {
        asof_join_f64(&left_sorted, &right, &right_vals)
    });

    // Reversed input breaks ascending order -> exercises the binary-search path.
    let left_unsorted: Vec<i64> = left_sorted.iter().rev().copied().collect();
    timeit("unsorted left (binary search)", || {
        asof_join_f64(&left_unsorted, &right, &right_vals)
    });
}
