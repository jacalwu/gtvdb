//! Concentration metrics by dimension (sector / country / rating / connected).

use std::collections::{BTreeMap, BTreeSet};

use crate::entity::Entity;
use crate::exposure::{ExposureEvent, Measure};
use crate::limit::{LimitRule, LimitStatus};

/// Concentration dimension.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConcentrationDimension {
    EconomicSector,
    Country,
    Rating,
    ConnectedParty,
}

impl ConcentrationDimension {
    pub fn as_str(self) -> &'static str {
        match self {
            ConcentrationDimension::EconomicSector => "sector",
            ConcentrationDimension::Country => "country",
            ConcentrationDimension::Rating => "rating",
            ConcentrationDimension::ConnectedParty => "connected",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "sector" | "economic_sector" => Some(ConcentrationDimension::EconomicSector),
            "country" => Some(ConcentrationDimension::Country),
            "rating" => Some(ConcentrationDimension::Rating),
            "connected" | "connected_party" => Some(ConcentrationDimension::ConnectedParty),
            _ => None,
        }
    }
}

/// One concentration bucket.
#[derive(Debug, Clone, PartialEq)]
pub struct ConcentrationRecord {
    pub key: String,
    pub exposure: f64,
    /// Exposure / Tier 1 (regulatory ratio).
    pub ratio_of_tier1: f64,
    /// Exposure / total book (internal risk-appetite share).
    pub share_of_book: f64,
    pub status: LimitStatus,
}

fn dimension_key(
    dim: ConcentrationDimension,
    entity_id: &str,
    entity: Option<&Entity>,
    connected: &BTreeSet<String>,
) -> String {
    match dim {
        ConcentrationDimension::EconomicSector => entity
            .map(|e| e.sector().as_str().to_string())
            .unwrap_or_else(|| "others".to_string()),
        ConcentrationDimension::Country => entity
            .and_then(|e| e.country_code.clone())
            .unwrap_or_else(|| "__unknown__".to_string()),
        ConcentrationDimension::Rating => entity
            .and_then(|e| e.rating_grade.clone())
            .unwrap_or_else(|| "__unrated__".to_string()),
        ConcentrationDimension::ConnectedParty => {
            if connected.contains(entity_id) {
                entity_id.to_string()
            } else {
                "__not_connected__".to_string()
            }
        }
    }
}

/// Aggregate active exposures by `dim` at `as_of` and evaluate each bucket
/// against `rule`.
#[allow(clippy::too_many_arguments)]
pub fn concentration(
    events: &[ExposureEvent],
    entities: &BTreeMap<String, Entity>,
    connected: &BTreeSet<String>,
    as_of: i64,
    dim: ConcentrationDimension,
    measure: Measure,
    tier1: f64,
    rule: &LimitRule,
) -> Vec<ConcentrationRecord> {
    let mut totals: BTreeMap<String, f64> = BTreeMap::new();
    let mut book = 0.0;
    for e in events.iter().filter(|e| e.active_at(as_of)) {
        let v = e.value(measure);
        if v == 0.0 {
            continue;
        }
        let key = dimension_key(dim, &e.entity_id, entities.get(&e.entity_id), connected);
        *totals.entry(key).or_insert(0.0) += v;
        book += v;
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::EntityKind;
    use crate::exposure::ExposureMeasure;
    use crate::limit::LimitMetric;

    fn ev(id: &str, entity: &str, amount: f64) -> ExposureEvent {
        ExposureEvent::new(
            id,
            entity,
            ExposureMeasure::zero().on_balance(amount),
            0,
            100,
        )
    }

    #[test]
    fn sector_concentration_and_shares() {
        let entities: BTreeMap<String, Entity> = [
            (
                "B1".to_string(),
                Entity::new("B1", EntityKind::Bank).with_economic_sector("banks"),
            ),
            (
                "C1".to_string(),
                Entity::new("C1", EntityKind::Corporate).with_economic_sector("others"),
            ),
        ]
        .into_iter()
        .collect();
        let events = vec![ev("e1", "B1", 60.0), ev("e2", "C1", 40.0)];
        let rule = LimitRule::new("sec", LimitMetric::Sector, 0.75).with_report_threshold(0.50);
        let recs = concentration(
            &events,
            &entities,
            &BTreeSet::new(),
            0,
            ConcentrationDimension::EconomicSector,
            Measure::BeforeCrm,
            100.0,
            &rule,
        );
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0].key, "banks");
        assert_eq!(recs[0].exposure, 60.0);
        assert_eq!(recs[0].share_of_book, 0.6);
        assert_eq!(recs[0].ratio_of_tier1, 0.6);
        assert_eq!(recs[1].key, "others");
    }

    #[test]
    fn connected_dimension_buckets_non_connected() {
        let entities: BTreeMap<String, Entity> = [(
            "C1".to_string(),
            Entity::new("C1", EntityKind::Corporate),
        )]
        .into_iter()
        .collect();
        let events = vec![ev("e1", "C1", 10.0)];
        let mut connected = BTreeSet::new();
        connected.insert("C1".to_string());
        let rule = LimitRule::new("cp", LimitMetric::ConnectedParty, 0.25);
        let recs = concentration(
            &events,
            &entities,
            &connected,
            0,
            ConcentrationDimension::ConnectedParty,
            Measure::BeforeCrm,
            100.0,
            &rule,
        );
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].key, "C1");
    }
}
