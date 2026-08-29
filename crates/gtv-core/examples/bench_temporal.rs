//! Benchmark for zone-map-pruned temporal edge filtering at 1M edges.
//!
//! Run with: `cargo run --release -p gtv-core --example bench_temporal`
//!
//! Generates 1M edges with strong temporal locality (ascending starts, fixed
//! duration) and times three "active at T" mask builders:
//!   1. scalar full scan (`temporal_mask_full`)       — O(n)
//!   2. SIMD full scan (Arrow comparison kernels)     — O(n), current prod path
//!   3. zone-map-pruned scan (`temporal_mask_pruned`) — O(chunks + active)

use std::time::Instant;

use arrow::array::{BooleanArray, Int64Array};
use arrow::compute::kernels::boolean::and;
use arrow::compute::kernels::cmp::{gt, lt_eq};
use gtv_core::temporal::{build_zone_maps, temporal_mask_full, temporal_mask_pruned};

fn bench<F: FnMut() -> BooleanArray>(name: &str, iters: usize, mut f: F) {
    let _ = f(); // warmup
    let t = Instant::now();
    for _ in 0..iters {
        std::hint::black_box(f());
    }
    let dt = t.elapsed();
    println!(
        "{name:30} {:>10.2} us/op",
        dt.as_secs_f64() * 1e6 / iters as f64
    );
}

fn main() {
    const N: usize = 1_000_000;
    const CHUNK: usize = 128;
    const DURATION: i64 = 100;

    // Ascending starts -> each 128-edge chunk spans a narrow time window, so a
    // single query overlaps only a handful of chunks.
    let valid_from: Vec<i64> = (0..N as i64).collect();
    let valid_to: Vec<i64> = valid_from.iter().map(|&f| f + DURATION).collect();
    let zones = build_zone_maps(&valid_from, &valid_to, CHUNK);

    let t = (N as i64) / 2;
    let skipped = zones.iter().filter(|z| z.excludes(t)).count();
    println!(
        "edges={N} chunks={} chunk_size={CHUNK} query T={t}\n\
         chunks skipped by zone map: {skipped}/{}",
        zones.len(),
        zones.len()
    );

    // Correctness: all three must agree.
    let full = temporal_mask_full(&valid_from, &valid_to, t);
    let pruned = temporal_mask_pruned(&valid_from, &valid_to, t, &zones);
    let from = Int64Array::from(valid_from.clone());
    let to = Int64Array::from(valid_to.clone());
    let t_scalar = Int64Array::new_scalar(t);
    let simd = and(&lt_eq(&from, &t_scalar).unwrap(), &gt(&to, &t_scalar).unwrap()).unwrap();
    assert_eq!(full, pruned, "pruned must equal full scan");
    assert_eq!(full, simd, "simd must equal full scan");
    let active = (0..N).filter(|&i| pruned.value(i)).count();
    println!("active edges at T: {active} (expect {DURATION})\n");

    bench("full scan (scalar, O(n))", 20, || {
        temporal_mask_full(&valid_from, &valid_to, t)
    });
    bench("full scan (SIMD, O(n))", 50, || {
        and(&lt_eq(&from, &t_scalar).unwrap(), &gt(&to, &t_scalar).unwrap()).unwrap()
    });
    bench("zone-map pruned (O(chunks+active))", 100_000, || {
        temporal_mask_pruned(&valid_from, &valid_to, t, &zones)
    });
}
