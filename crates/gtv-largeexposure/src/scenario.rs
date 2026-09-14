//! IRRBB × Large Exposure: stress scenario revaluation (LE-5).
//!
//! Reuses the `gtv-scenario::irrbb` shock scenarios and discount curve to
//! revalue rate-sensitive exposures (bonds / fixed-rate loans) under an
//! interest-rate shock, then re-runs the large-exposure / concentration
//! measures on the stressed exposures.

use std::collections::{BTreeMap, BTreeSet};

use gtv_scenario::irrbb::{post_shock_rate, ShockScenario};
use gtv_scenario::{DiscountCurve, ShockParams};

use crate::concentration::{ConcentrationDimension, ConcentrationRecord};
use crate::entity::Entity;
use crate::ledger::Ledger;

/// An IRRBB shock scenario to apply.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScenarioSpec {
    pub scenario: ShockScenario,
    pub params: ShockParams,
    /// Floor on the post-shock rate (regulatory default −2%).
    pub floor: f64,
}

impl ScenarioSpec {
    pub fn new(scenario: ShockScenario, params: ShockParams, floor: f64) -> Self {
        Self {
            scenario,
            params,
            floor,
        }
    }

    /// The shock to the risk-free rate at `tenor_years`, after the floor.
    pub fn delta_rate(&self, base_curve: &DiscountCurve, tenor_years: f64) -> f64 {
        let r0 = base_curve.zero_rate_years(tenor_years);
        post_shock_rate(r0, self.scenario, self.params, tenor_years, self.floor) - r0
    }
}

/// Duration-based rate sensitivity of one exposure event.
#[derive(Debug, Clone, PartialEq)]
pub struct RatePosition {
    pub event_id: String,
    /// Modified duration (years).
    pub modified_duration: f64,
    /// Tenor at which the shock is read (years).
    pub tenor_years: f64,
}

impl RatePosition {
    pub fn new(event_id: impl Into<String>, modified_duration: f64, tenor_years: f64) -> Self {
        Self {
            event_id: event_id.into(),
            modified_duration,
            tenor_years,
        }
    }
}

/// Stressed exposure result for one counterparty / LC group.
#[derive(Debug, Clone, PartialEq)]
pub struct StressResult {
    pub key: String,
    pub base_exposure: f64,
    pub stressed_exposure: f64,
    /// `stressed − base` (negative = market-value loss).
    pub mv_change: f64,
    pub ratio: f64,
    pub breached: bool,
}

fn revalue_factor(
    spec: &ScenarioSpec,
    base_curve: &DiscountCurve,
    position: Option<&RatePosition>,
) -> f64 {
    match position {
        Some(p) if p.modified_duration != 0.0 => {
            let dr = spec.delta_rate(base_curve, p.tenor_years);
            1.0 - p.modified_duration * dr
        }
        _ => 1.0,
    }
}

/// Stress one LC group (or standalone counterparty) as at `as_of`.
pub fn stress_group(
    ledger: &Ledger,
    group_rep: &str,
    as_of: i64,
    tier1: f64,
    spec: &ScenarioSpec,
    base_curve: &DiscountCurve,
    positions: &BTreeMap<String, RatePosition>,
) -> StressResult {
    let members: BTreeSet<&str> = ledger
        .groups()
        .members_of(group_rep)
        .iter()
        .map(String::as_str)
        .collect();
    let mut base = 0.0;
    let mut stressed = 0.0;
    for e in ledger.events().values() {
        if !members.contains(e.entity_id.as_str()) || !e.active_at(as_of) {
            continue;
        }
        let v = e.before_crm();
        if v == 0.0 {
            continue;
        }
        base += v;
        stressed += v * revalue_factor(spec, base_curve, positions.get(&e.event_id));
    }
    let ratio = if tier1 > 0.0 {
        stressed / tier1
    } else {
        f64::INFINITY
    };
    StressResult {
        key: group_rep.to_string(),
        base_exposure: base,
        stressed_exposure: stressed,
        mv_change: stressed - base,
        ratio,
        breached: ratio >= ledger.config().limit_ratio,
    }
}

/// Stress every LC group / standalone counterparty, ranked by stressed
/// exposure descending (ties by key ascending).
#[allow(clippy::too_many_arguments)]
pub fn stress_scan(
    ledger: &Ledger,
    as_of: i64,
    tier1: f64,
    spec: &ScenarioSpec,
    base_curve: &DiscountCurve,
    positions: &BTreeMap<String, RatePosition>,
) -> Vec<StressResult> {
    let mut out: Vec<StressResult> = ledger
        .groups()
        .groups()
        .map(|(rep, _)| {
            stress_group(ledger, rep, as_of, tier1, spec, base_curve, positions)
        })
        .filter(|r| r.base_exposure > 0.0)
        .collect();
    out.sort_by(|a, b| {
        b.stressed_exposure
            .partial_cmp(&a.stressed_exposure)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.key.cmp(&b.key))
    });
    out
}

/// Concentration by `dim` using stressed exposures (market value under the
/// scenario), so stress-driven worsening is visible.
#[allow(clippy::too_many_arguments)]
pub fn stress_concentration(
    ledger: &Ledger,
    entities: &BTreeMap<String, Entity>,
    connected: &BTreeSet<String>,
    as_of: i64,
    dim: ConcentrationDimension,
    tier1: f64,
    spec: &ScenarioSpec,
    base_curve: &DiscountCurve,
    positions: &BTreeMap<String, RatePosition>,
) -> Vec<ConcentrationRecord> {
    let mut totals: BTreeMap<String, f64> = BTreeMap::new();
    let mut book = 0.0;
    for e in ledger.events().values() {
        if !e.active_at(as_of) {
            continue;
        }
        let v = e.before_crm();
        if v == 0.0 {
            continue;
        }
        let stressed = v * revalue_factor(spec, base_curve, positions.get(&e.event_id));
        let key = match dim {
            ConcentrationDimension::EconomicSector => entities
                .get(&e.entity_id)
                .map(|x| x.sector().as_str().to_string())
                .unwrap_or_else(|| "others".to_string()),
            ConcentrationDimension::Country => entities
                .get(&e.entity_id)
                .and_then(|x| x.country_code.clone())
                .unwrap_or_else(|| "__unknown__".to_string()),
            ConcentrationDimension::Rating => entities
                .get(&e.entity_id)
                .and_then(|x| x.rating_grade.clone())
                .unwrap_or_else(|| "__unrated__".to_string()),
            ConcentrationDimension::ConnectedParty => {
                if connected.contains(&e.entity_id) {
                    e.entity_id.clone()
                } else {
                    "__not_connected__".to_string()
                }
            }
        };
        *totals.entry(key).or_insert(0.0) += stressed;
        book += stressed;
    }
    let rule = crate::limit::LimitRule::new(
        "stress_concentration",
        crate::limit::LimitMetric::Sector,
        ledger.config().limit_ratio,
    )
    .with_report_threshold(ledger.config().report_threshold);
    let mut out: Vec<ConcentrationRecord> = totals
        .into_iter()
        .map(|(key, exposure)| {
            let o = rule.evaluate(exposure, tier1);
            ConcentrationRecord {
                key,
                exposure,
                ratio_of_tier1: o.ratio,
                share_of_book: if book > 0.0 { exposure / book } else { 0.0 },
                status: o.status,
            }
        })
        .collect();
    out.sort_by(|a, b| {
        b.exposure
            .partial_cmp(&a.exposure)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.key.cmp(&b.key))
    });
    out
}

/// Total book exposure under the scenario (sum of stressed entity exposures).
pub fn stressed_book_total(
    ledger: &Ledger,
    as_of: i64,
    spec: &ScenarioSpec,
    base_curve: &DiscountCurve,
    positions: &BTreeMap<String, RatePosition>,
) -> f64 {
    ledger
        .entities()
        .keys()
        .map(|id| {
            let mut total = 0.0;
            for e in ledger.events().values() {
                if e.entity_id == *id && e.active_at(as_of) {
                    total += e.before_crm() * revalue_factor(spec, base_curve, positions.get(&e.event_id));
                }
            }
            total
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::LeConfig;
    use crate::entity::{Entity, EntityKind};
    use crate::exposure::{ExposureEvent, ExposureMeasure};
    use crate::temporal::TimeAxis;

    fn ev(id: &str, entity: &str, amount: f64) -> ExposureEvent {
        ExposureEvent::new(id, entity, ExposureMeasure::zero().on_balance(amount), 0, 400)
    }

    fn ledger() -> Ledger {
        let mut l = Ledger::new(LeConfig::default(), TimeAxis::new(0, 400));
        l.set_entities(vec![
            Entity::new("A", EntityKind::Corporate).with_economic_sector("banks"),
            Entity::new("B", EntityKind::Corporate).with_economic_sector("others"),
        ]);
        l
    }

    #[test]
    fn parallel_up_loses_on_rate_sensitive_positions() {
        let mut l = ledger();
        l.add(ev("e1", "A", 1000.0)).unwrap(); // duration 5
        l.add(ev("e2", "A", 500.0)).unwrap(); // insensitive
        let mut pos = BTreeMap::new();
        pos.insert("e1".to_string(), RatePosition::new("e1", 5.0, 5.0));
        let curve = DiscountCurve::flat(0.03);
        let spec = ScenarioSpec::new(
            ShockScenario::ParallelUp,
            ShockParams::new(200.0, 300.0, 150.0),
            -0.02,
        );
        let r = stress_group(&l, "A", 100, 1000.0, &spec, &curve, &pos);
        // Δr = +2%, duration 5 -> factor 0.9 on 1000 (900) + 500 = 1400
        assert!(r.base_exposure == 1500.0);
        assert!((r.stressed_exposure - 1400.0).abs() < 1e-6, "got {}", r.stressed_exposure);
        assert!((r.mv_change + 100.0).abs() < 1e-6);
        assert_eq!(r.ratio, 1.4);
        assert!(r.breached);
    }

    #[test]
    fn parallel_down_gains_and_scan_ranks() {
        let mut l = ledger();
        l.add(ev("e1", "A", 1000.0)).unwrap();
        l.add(ev("e2", "B", 800.0)).unwrap();
        let mut pos = BTreeMap::new();
        pos.insert("e1".to_string(), RatePosition::new("e1", 5.0, 5.0));
        pos.insert("e2".to_string(), RatePosition::new("e2", 2.0, 5.0));
        let curve = DiscountCurve::flat(0.03);
        let spec = ScenarioSpec::new(
            ShockScenario::ParallelDown,
            ShockParams::new(200.0, 300.0, 150.0),
            -0.02,
        );
        let scan = stress_scan(&l, 100, 1000.0, &spec, &curve, &pos);
        assert_eq!(scan.len(), 2);
        // rates fall 2%: A gains 10% (1100), B gains 4% (832)
        assert!(scan[0].mv_change > 0.0 && scan[0].key == "A");
        assert!((scan[0].stressed_exposure - 1100.0).abs() < 1e-6);
        assert!((scan[1].stressed_exposure - 832.0).abs() < 1e-6);
    }

    #[test]
    fn stress_concentration_and_book_total() {
        let mut l = ledger();
        l.add(ev("e1", "A", 1000.0)).unwrap();
        l.add(ev("e2", "B", 500.0)).unwrap();
        let mut pos = BTreeMap::new();
        pos.insert("e1".to_string(), RatePosition::new("e1", 5.0, 5.0));
        let curve = DiscountCurve::flat(0.03);
        let spec = ScenarioSpec::new(
            ShockScenario::ParallelUp,
            ShockParams::new(200.0, 300.0, 150.0),
            -0.02,
        );
        let recs = stress_concentration(
            &l,
            l.entities(),
            &BTreeSet::new(),
            100,
            ConcentrationDimension::EconomicSector,
            1000.0,
            &spec,
            &curve,
            &pos,
        );
        // banks = 900, others = 500
        assert_eq!(recs[0].key, "banks");
        assert!((recs[0].exposure - 900.0).abs() < 1e-6);
        let total = stressed_book_total(&l, 100, &spec, &curve, &pos);
        assert!((total - 1400.0).abs() < 1e-6);
    }
}
