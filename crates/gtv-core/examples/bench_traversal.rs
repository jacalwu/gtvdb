//! Benchmark: push vs pull (direction-optimizing) k-hop BFS on a dense random
//! graph (B1-3). `Auto` chooses the cheaper direction per hop.
//!
//! Run with: `cargo run --release -p gtv-core --example bench_traversal`

use std::time::Instant;

use arrow::array::{TimestampNanosecondArray, UInt16Array, UInt64Array};
use gtv_core::{DirectionMode, TemporalCSR, TraversalBudget};

struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }
}

fn main() {
    const N: usize = 50_000;
    const DEGREE: usize = 50;
    const EDGES: usize = N * DEGREE;

    let mut rng = Lcg(0xC0FFEE);
    let mut src = Vec::with_capacity(EDGES);
    let mut dst = Vec::with_capacity(EDGES);
    for s in 0..N as u64 {
        for _ in 0..DEGREE {
            src.push(s);
            dst.push(rng.next() % N as u64);
        }
    }
    let csr = TemporalCSR::from_arrays(
        &UInt64Array::from(src),
        &UInt64Array::from(dst),
        &TimestampNanosecondArray::from(vec![0i64; EDGES]),
        &TimestampNanosecondArray::from(vec![1000i64; EDGES]),
        &UInt16Array::from(vec![0u16; EDGES]),
        N,
    )
    .unwrap();
    let reverse = csr.transpose().unwrap();
    println!(
        "graph: nodes={N} edges={EDGES} avg_degree={:.1}",
        EDGES as f64 / N as f64
    );

    let seeds = UInt64Array::from(vec![0u64]);
    let budget = TraversalBudget::unlimited();
    let k = 4;

    let run = |mode: DirectionMode| {
        csr.khop_directed(
            &seeds,
            k,
            0,
            &budget,
            None,
            None,
            Some(&reverse),
            mode,
        )
        .unwrap()
    };

    // Correctness: all directions agree.
    let push = run(DirectionMode::Push);
    let pull = run(DirectionMode::Pull);
    let auto = run(DirectionMode::Auto);
    let fronts = |r: &gtv_core::KhopResult| -> Vec<Vec<u64>> {
        r.frontiers.iter().map(|a| a.values().to_vec()).collect()
    };
    assert_eq!(fronts(&push), fronts(&pull), "pull must match push");
    assert_eq!(fronts(&push), fronts(&auto), "auto must match push");
    let sizes: Vec<usize> = push.frontiers.iter().map(|f| f.len()).collect();
    println!("frontier sizes per hop: {sizes:?}");

    let bench = |name: &str, r: &gtv_core::KhopResult| {
        let iters = 20;
        let _ = run(if name.starts_with("push") {
            DirectionMode::Push
        } else if name.starts_with("pull") {
            DirectionMode::Pull
        } else {
            DirectionMode::Auto
        });
        let t = Instant::now();
        for _ in 0..iters {
            std::hint::black_box(run(if name.starts_with("push") {
                DirectionMode::Push
            } else if name.starts_with("pull") {
                DirectionMode::Pull
            } else {
                DirectionMode::Auto
            }));
        }
        let us = t.elapsed().as_secs_f64() * 1e6 / iters as f64;
        println!(
            "{name:12} {us:9.1} us/query   edges_scanned={}",
            r.stats.edges_scanned
        );
    };
    bench("push", &push);
    bench("pull", &pull);
    bench("auto", &auto);
}
