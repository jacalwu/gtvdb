//! Benchmark: incremental (Fenwick + segment tree) vs full recomputation for
//! Large Exposure under historical corrections.
//!
//! Run: `cargo run --release -p gtv-largeexposure --example bench_le_incremental`
//!
//! Models `D` days, `N` exposure events for `G` counterparties, then `M`
//! historical corrections. The naive baseline rebuilds a per-entity daily
//! exposure array from scratch after each correction; the incremental ledger
//! applies a suffix range-add on the affected aggregate keys.

use std::collections::BTreeMap;
use std::time::Instant;

use gtv_largeexposure::{
    AggregateKind, Entity, EntityKind, EventKind, ExposureEvent, ExposureMeasure, LeConfig, Ledger,
    Measure, TimeAxis,
};

const D: usize = 3650; // ~10 years of days
const N: usize = 100_000;
const G: usize = 5_000;
const M: usize = 2_000;

fn event(id: &str, entity: &str, amount: f64, from: i64, to: i64) -> ExposureEvent {
    ExposureEvent::new(
        id,
        entity,
        ExposureMeasure::zero().on_balance(amount),
        from,
        to,
    )
}

fn main() {
    let mut x: u64 = 0x9E3779B97F4A7C15;
    let mut rng = move || {
        x = x
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        x >> 33
    };

    let entities: Vec<Entity> = (0..G)
        .map(|i| Entity::new(format!("E{i}"), EntityKind::Corporate))
        .collect();
    let evs: Vec<ExposureEvent> = (0..N)
        .map(|i| {
            let from = (rng() % (D as u64)) as i64;
            let to = (from + 1 + (rng() % 500) as i64).min(D as i64);
            let amount = (rng() % 100_000) as f64 + 1.0;
            event(
                &format!("e{i}"),
                &format!("E{}", rng() as usize % G),
                amount,
                from,
                to,
            )
        })
        .collect();
    let corrections: Vec<(String, f64)> = (0..M)
        .map(|_| {
            (
                format!("e{}", rng() as usize % N),
                (rng() % 100_000) as f64 + 1.0,
            )
        })
        .collect();

    // --- incremental ledger -------------------------------------------------
    let t = Instant::now();
    let mut ledger = Ledger::new(LeConfig::default(), TimeAxis::new(0, D));
    ledger.set_entities(entities.clone());
    ledger.bulk_load(evs.clone()).unwrap();
    let build = t.elapsed();

    let t = Instant::now();
    for (i, (target, amount)) in corrections.iter().enumerate() {
        let old = match ledger.event(target) {
            Some(e) => e.clone(),
            None => continue,
        };
        let mut c = event(
            &format!("c{i}"),
            &old.entity_id,
            *amount,
            old.business_from,
            old.business_to,
        );
        c.kind = EventKind::Correction;
        c.ref_event_id = Some(target.clone());
        ledger.correct(c).unwrap();
    }
    let incremental = t.elapsed();

    let t = Instant::now();
    let mut max_a = 0.0f64;
    let entity_ids: Vec<String> = ledger.entities().keys().cloned().collect();
    for e in &entity_ids {
        max_a = max_a.max(ledger.period_max(
            AggregateKind::Entity,
            e,
            Measure::BeforeCrm,
            0,
            D as i64,
        ));
    }
    let query = t.elapsed();

    // --- naive baseline -----------------------------------------------------
    // After each correction, rebuild a daily per-entity array from scratch.
    let t = Instant::now();
    let mut live: Vec<ExposureEvent> = evs.clone();
    let index: BTreeMap<String, usize> = live
        .iter()
        .enumerate()
        .map(|(i, e)| (e.event_id.clone(), i))
        .collect();
    for (i, (target, amount)) in corrections.iter().enumerate() {
        let Some(&pos) = index.get(target) else {
            continue;
        };
        let old = live[pos].clone();
        live[pos] = event(
            &format!("nc{i}"),
            &old.entity_id,
            *amount,
            old.business_from,
            old.business_to,
        );
        // full recompute of a daily array for the changed entity
        let mut daily = vec![0.0f64; D];
        for e in live.iter().filter(|e| e.entity_id == old.entity_id) {
            let f = e.business_from.max(0) as usize;
            let t2 = (e.business_to.min(D as i64)) as usize;
            for d in daily.iter_mut().take(t2).skip(f) {
                *d += e.before_crm();
            }
        }
        let _ = daily.iter().cloned().fold(0.0f64, f64::max);
    }
    let naive = t.elapsed();

    println!("LE incremental benchmark (D={D} days, N={N} events, G={G}, M={M})");
    println!("  ledger build                : {:>10.3?}", build);
    println!("  incremental corrections     : {:>10.3?}", incremental);
    println!("  period-max query (all ents) : {:>10.3?}", query);
    println!("  naive full recompute        : {:>10.3?}", naive);
    let speedup = naive.as_secs_f64() / incremental.as_secs_f64();
    println!("  speedup (naive / incremental): {speedup:.1}x");
}
