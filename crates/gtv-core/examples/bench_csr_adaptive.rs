//! Benchmark: adaptive TemporalCSR lookup (binary search on `valid_from` +
//! chunk zone maps on `valid_to`) vs the legacy linear per-source scan (B1-2).
//!
//! Run with: `cargo run --release -p gtv-core --example bench_csr_adaptive`

use std::time::Instant;

use arrow::array::{TimestampNanosecondArray, UInt16Array, UInt64Array};
use gtv_core::TemporalCSR;

fn bench<F: FnMut() -> usize>(name: &str, iters: usize, mut f: F) -> f64 {
    let _ = f(); // warmup
    let t = Instant::now();
    let mut acc = 0usize;
    for _ in 0..iters {
        acc = acc.wrapping_add(std::hint::black_box(f()));
    }
    let us = t.elapsed().as_secs_f64() * 1e6 / iters as f64;
    println!("{name:34} {us:10.3} us/op   (active={acc})", acc = acc / iters);
    us
}

fn main() {
    const DEGREE: usize = 1_000_000;
    const DURATION: i64 = 1_000;

    // One high-degree source: valid_from ascends by 1, each edge lasts DURATION.
    let src = vec![0u64; DEGREE];
    let dst: Vec<u64> = (1..=DEGREE as u64).collect();
    let valid_from: Vec<i64> = (0..DEGREE as i64).collect();
    let valid_to: Vec<i64> = valid_from.iter().map(|&f| f + DURATION).collect();
    let edge_type = vec![0u16; DEGREE];

    let csr = TemporalCSR::from_arrays(
        &UInt64Array::from(src),
        &UInt64Array::from(dst),
        &TimestampNanosecondArray::from(valid_from),
        &TimestampNanosecondArray::from(valid_to),
        &UInt16Array::from(edge_type),
        DEGREE + 1,
    )
    .unwrap();

    let query_t = DEGREE as i64 / 2;

    // Correctness: adaptive path must equal a brute-force linear filter.
    let adaptive: Vec<u64> = csr.neighbors(0, query_t).unwrap().map(|n| n.dst).collect();
    let (d, vf, vt, _) = csr.edge_slices(0).unwrap();
    let linear: Vec<u64> = (0..d.len())
        .filter(|&i| vf[i] <= query_t && query_t < vt[i])
        .map(|i| d[i])
        .collect();
    assert_eq!(adaptive, linear, "adaptive lookup must equal linear scan");

    let stats = csr.stats();
    let (_, strategy) = csr.neighbors_planned(0, query_t).unwrap();
    println!(
        "degree={DEGREE} edges={} chunks={} max_degree={} strategy={strategy:?} query T={query_t}",
        csr.edge_count(),
        stats.chunk_count,
        stats.max_degree,
    );

    let adaptive_us = bench("adaptive (binary + zone map)", 200, || {
        csr.neighbors(0, query_t).unwrap().count()
    });
    let linear_us = bench("legacy linear scan", 200, || {
        let (d, vf, vt, _) = csr.edge_slices(0).unwrap();
        (0..d.len())
            .filter(|&i| vf[i] <= query_t && query_t < vt[i])
            .count()
    });
    println!("\nlatency speedup: {:.1}x", linear_us / adaptive_us);
}
