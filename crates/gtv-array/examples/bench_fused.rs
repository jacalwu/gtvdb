//! Micro-benchmark: fused single-pass rolling-window kernel vs three separate
//! calls (`mavg` + `msum` + `deltas`).
//!
//! Run with: `cargo run --release -p gtv-array --example bench_fused`
//!
//! Each side is measured 3 times consecutively (same-size output buffers are
//! recycled by the allocator, giving a stable state) and the best of the last
//! two runs is reported. Large output buffers from the system allocator can be
//! either page-aligned (fresh mmap, engages the AVX2 non-temporal path) or
//! 16-byte-aligned (heap reuse, engages the SSE2 non-temporal path); the fused
//! kernel adapts automatically via a runtime alignment check.

use std::hint::black_box;
use std::time::Instant;

use gtv_array::window::{deltas, fused_ma_ms_delta, mavg, msum};

fn checksum(a: &[f64], b: &[f64], c: &[f64]) -> f64 {
    a.last().copied().unwrap_or(0.0) + b.last().copied().unwrap_or(0.0) + c.last().copied().unwrap_or(0.0)
}

fn main() {
    const N: usize = 4_000_000;
    let n = 64;

    let v_sep: Vec<f64> = black_box((0..N).map(|i| (i as f64 * 0.1).sin() * 100.0).collect());
    let v_fus: Vec<f64> = black_box((0..N).map(|i| (i as f64 * 0.17).cos() * 100.0).collect());

    let mut t_sep = Vec::new();
    let mut t_fus = Vec::new();

    for k in 0..3 {
        // separate: three independent scans + RFO output writes
        let t = Instant::now();
        let a = black_box(mavg(&v_sep, n));
        let b = black_box(msum(&v_sep, n));
        let c = black_box(deltas(&v_sep));
        let t_sep_ms = t.elapsed().as_secs_f64() * 1e3;
        black_box((&a, &b, &c));
        t_sep.push((k, t_sep_ms, checksum(&a, &b, &c)));

        // fused: one scan + non-temporal output writes
        let t = Instant::now();
        let (a, b, c) = black_box(fused_ma_ms_delta(&v_fus, n));
        let t_fus_ms = t.elapsed().as_secs_f64() * 1e3;
        black_box((&a, &b, &c));
        t_fus.push((k, t_fus_ms, checksum(&a, &b, &c)));
    }

    let best_sep = t_sep.iter().skip(1).map(|x| x.1).fold(f64::INFINITY, f64::min);
    let best_fus = t_fus.iter().skip(1).map(|x| x.1).fold(f64::INFINITY, f64::min);

    println!("== N = {N}, window = {n} ==");
    println!("per-run times (ms):");
    println!("  separate: {}", t_sep.iter().map(|x| format!("{:.1}", x.1)).collect::<Vec<_>>().join("  "));
    println!("  fused   : {}", t_fus.iter().map(|x| format!("{:.1}", x.1)).collect::<Vec<_>>().join("  "));
    println!(
        "best: separate {:.1} ms | fused {:.1} ms | speedup {:.2}x",
        best_sep, best_fus, best_sep / best_fus
    );
    // Sanity: identical checksums across runs of the same path.
    assert_eq!(t_sep[0].2, t_sep[1].2);
    assert_eq!(t_fus[0].2, t_fus[1].2);
    println!("checksums (sep / fus): {:.6} / {:.6}", t_sep[0].2, t_fus[0].2);
}
