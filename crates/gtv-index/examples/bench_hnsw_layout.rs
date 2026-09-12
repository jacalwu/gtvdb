//! Benchmark: HNSW contiguous-layout memory + search latency (B1-4).
//!
//! Reports the actual `.gtvidx`-style contiguous footprint against an analytical
//! estimate of the previous `Vec<Node { vector: Vec<f32>, layers: Vec<Vec<usize>> }>`
//! layout, plus HNSW vs exact Flat latency.
//!
//! Run with: `cargo run --release -p gtv-index --example bench_hnsw_layout`

use std::time::Instant;

use gtv_core::VectorIndex;
use gtv_index::{FlatIndex, HnswIndex};

struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }
    fn next_f32(&mut self) -> f32 {
        (self.next() >> 40) as f32 / (1u64 << 24) as f32
    }
}

fn main() {
    const N: usize = 50_000;
    const DIM: usize = 128;
    const K: usize = 10;
    const QUERIES: usize = 100;

    let mut rng = Lcg(42);
    let ids: Vec<u64> = (0..N as u64).collect();
    let vectors: Vec<Vec<f32>> = (0..N)
        .map(|_| (0..DIM).map(|_| rng.next_f32()).collect())
        .collect();

    let t = Instant::now();
    let index = HnswIndex::build(ids.clone(), vectors.clone(), 16, 100, 100).unwrap();
    println!(
        "hnsw build: {:.2}s  (n={N}, dim={DIM}, m=16, ef_c=100)",
        t.elapsed().as_secs_f64()
    );

    let actual = index.memory_bytes();
    let legacy = index.estimated_legacy_bytes();
    println!(
        "neighbour slots={} live={}  dim={}",
        index.neighbour_slots(),
        index.neighbour_count(),
        index.dim(),
    );
    println!(
        "contiguous layout : {:>8.2} MB  ({:.1} bytes/vector)",
        actual as f64 / 1e6,
        actual as f64 / N as f64
    );
    println!(
        "legacy layout est : {:>8.2} MB  ({:.1} bytes/vector)",
        legacy as f64 / 1e6,
        legacy as f64 / N as f64
    );
    println!(
        "memory improvement: {:.2}x",
        legacy as f64 / actual as f64
    );

    let queries: Vec<Vec<f32>> = (0..QUERIES)
        .map(|_| (0..DIM).map(|_| rng.next_f32()).collect())
        .collect();

    let hnsw_us = {
        let mut acc = 0usize;
        let t = Instant::now();
        for q in &queries {
            acc += index.search(q, K, None).unwrap().len();
        }
        let us = t.elapsed().as_secs_f64() * 1e6 / QUERIES as f64;
        std::hint::black_box(acc);
        us
    };

    let flat = FlatIndex::new(ids, vectors).unwrap();
    let flat_us = {
        let mut acc = 0usize;
        let t = Instant::now();
        for q in &queries {
            acc += flat.search(q, K, None).unwrap().len();
        }
        let us = t.elapsed().as_secs_f64() * 1e6 / QUERIES as f64;
        std::hint::black_box(acc);
        us
    };

    // Recall@K against the exact Flat oracle.
    let sample = 30.min(queries.len());
    let mut hit = 0usize;
    let mut total = 0usize;
    for q in queries.iter().take(sample) {
        let exact: Vec<u64> = flat.search(q, K, None).unwrap().iter().map(|h| h.id).collect();
        let approx: Vec<u64> = index.search(q, K, None).unwrap().iter().map(|h| h.id).collect();
        hit += exact.iter().filter(|id| approx.contains(id)).count();
        total += exact.len();
    }

    println!("hnsw search: {hnsw_us:>8.1} us/query  (k={K})");
    println!("flat search: {flat_us:>8.1} us/query (exact oracle)");
    println!("hnsw/flat latency ratio: {:.3}x", hnsw_us / flat_us);
    println!("recall@{K} (ef_search): {:.3}", hit as f64 / total as f64);

    // Recall sweep over ef_search to separate graph quality from the search
    // budget (high-dimensional uniform data is intrinsically hard).
    for ef in [10usize, 50, 100, 200, 400] {
        let mut h = 0usize;
        let mut t = 0usize;
        for q in queries.iter().take(sample) {
            let exact: Vec<u64> = flat.search(q, K, None).unwrap().iter().map(|x| x.id).collect();
            let approx: Vec<u64> = index
                .search_with_ef(q, K, ef, None)
                .unwrap()
                .iter()
                .map(|x| x.id)
                .collect();
            h += exact.iter().filter(|id| approx.contains(id)).count();
            t += exact.len();
        }
        println!("  ef={ef:>3}  recall@{K} = {:.3}", h as f64 / t as f64);
    }
}
