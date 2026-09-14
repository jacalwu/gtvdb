//! MA(BS)28 report projection (Parts I–V).
//!
//! Ranking is by the **maximum exposure during the reporting period**
//! (Parts I/II/III/V) or the reporting-date snapshot (Part IV), matching the
//! Completion Instructions. Component columns are read at the date of the
//! period maximum.

use crate::exposure::{ExposureEvent, ExposureMeasure, Measure};
use crate::ledger::{AggregateKind, Ledger};

/// MA(BS)28 report part.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MaBs28Part {
    /// Exposures to any connected party ≥5% of Tier 1.
    I,
    /// Twenty largest (and all ≥10%) before CRM.
    II,
    /// Twenty largest (and all ≥10%) after CRM.
    III,
    /// Exempted exposures before CRM ≥10% at the reporting date.
    IV,
    /// Intragroup exposures ≥5% / twenty largest.
    V,
}

impl MaBs28Part {
    pub fn as_str(self) -> &'static str {
        match self {
            MaBs28Part::I => "I",
            MaBs28Part::II => "II",
            MaBs28Part::III => "III",
            MaBs28Part::IV => "IV",
            MaBs28Part::V => "V",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_uppercase().as_str() {
            "I" | "1" => Some(MaBs28Part::I),
            "II" | "2" => Some(MaBs28Part::II),
            "III" | "3" => Some(MaBs28Part::III),
            "IV" | "4" => Some(MaBs28Part::IV),
            "V" | "5" => Some(MaBs28Part::V),
            _ => None,
        }
    }
}

/// One MA(BS)28 report row.
#[derive(Debug, Clone, PartialEq)]
pub struct MaBs28Row {
    pub rank: usize,
    pub counterparty_id: Option<String>,
    pub lc_group_id: Option<String>,
    pub maximum_exposure: f64,
    pub on_balance: f64,
    pub trading_book: f64,
    pub off_balance: f64,
    pub default_risk: f64,
    pub indirect: f64,
    pub additional_risk: f64,
    pub total: f64,
    pub deductions: f64,
    pub economic_sector: String,
    pub relationship_code: Option<String>,
    pub percent_of_tier1: f64,
    pub exemption_provision: Option<String>,
}

/// Per-event component contribution under a measure (CRM applied
/// proportionally for `AfterCrm` so the components sum to the after-CRM total).
fn event_components(event: &ExposureEvent, measure: Measure) -> ExposureMeasure {
    match measure {
        Measure::BeforeCrm => {
            if event.net_short {
                ExposureMeasure::zero()
            } else {
                event.measure
            }
        }
        Measure::Exempted => {
            if event.exempt && !event.net_short {
                event.measure
            } else {
                ExposureMeasure::zero()
            }
        }
        Measure::AfterCrm => {
            let before = event.before_crm();
            if before <= 0.0 {
                ExposureMeasure::zero()
            } else {
                event.measure.scale(event.after_crm() / before)
            }
        }
    }
}

/// Build one MA(BS)28 part.
///
/// `period_start..period_end` is the half-open reporting period used for the
/// period-maximum ranking; for Part IV, `period_end` is also the "as at"
/// reporting date and must lie inside the exposure intervals (`business_to`
/// is exclusive).
pub fn ma_bs28_report(
    ledger: &mut Ledger,
    part: MaBs28Part,
    period_start: i64,
    period_end: i64,
    tier1: f64,
) -> Vec<MaBs28Row> {
    let cfg = ledger.config().clone();
    let (kind, measure, threshold, top_n, group_level) = match part {
        MaBs28Part::I => (
            AggregateKind::ConnectedParty,
            Measure::BeforeCrm,
            cfg.connected_report_threshold,
            None,
            false,
        ),
        MaBs28Part::II => (
            AggregateKind::LcGroup,
            Measure::BeforeCrm,
            cfg.report_threshold,
            Some(cfg.default_top_n),
            true,
        ),
        MaBs28Part::III => (
            AggregateKind::LcGroup,
            Measure::AfterCrm,
            cfg.report_threshold,
            Some(cfg.default_top_n),
            true,
        ),
        MaBs28Part::IV => (
            AggregateKind::LcGroup,
            Measure::Exempted,
            cfg.report_threshold,
            None,
            true,
        ),
        MaBs28Part::V => (
            AggregateKind::GroupAffiliate,
            Measure::BeforeCrm,
            cfg.connected_report_threshold,
            Some(cfg.default_top_n),
            false,
        ),
    };

    // --- universe (owned; avoids holding a borrow while querying) -----------
    struct Candidate {
        key_id: String,
        members: Vec<String>,
        sector: String,
        relationship_code: Option<String>,
        value: f64,
        date: i64,
        group_level: bool,
    }
    let mut universe: Vec<(String, Vec<String>, String, Option<String>)> = Vec::new();
    match part {
        MaBs28Part::I => {
            for id in ledger.connected() {
                let (sector, rel) = match ledger.entities().get(id) {
                    Some(e) => (e.sector().as_str().to_string(), e.connected_paragraph.clone()),
                    None => ("others".to_string(), None),
                };
                universe.push((id.clone(), vec![id.clone()], sector, rel));
            }
        }
        MaBs28Part::V => {
            for id in ledger.affiliates() {
                let sector = ledger
                    .entities()
                    .get(id)
                    .map(|e| e.sector().as_str().to_string())
                    .unwrap_or_else(|| "others".to_string());
                universe.push((id.clone(), vec![id.clone()], sector, None));
            }
        }
        _ => {
            for (rep, members) in ledger.groups().groups() {
                let sector = ledger
                    .entities()
                    .get(rep)
                    .map(|e| e.sector().as_str().to_string())
                    .unwrap_or_else(|| "others".to_string());
                universe.push((rep.clone(), members.clone(), sector, None));
            }
        }
    }

    let mut candidates: Vec<Candidate> = Vec::new();
    for (key_id, members, sector, relationship_code) in universe {
        let (value, date) = match part {
            MaBs28Part::IV => (
                ledger.exposure_at(kind, &key_id, measure, period_end),
                period_end,
            ),
            _ => match ledger.period_argmax(kind, &key_id, measure, period_start, period_end) {
                Some((v, d)) => (v, d),
                None => (0.0, period_end),
            },
        };
        if value > 0.0 {
            candidates.push(Candidate {
                key_id,
                members,
                sector,
                relationship_code,
                value,
                date,
                group_level,
            });
        }
    }

    // --- rank + threshold/top-n selection -----------------------------------
    candidates.sort_by(|a, b| {
        b.value
            .partial_cmp(&a.value)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.key_id.cmp(&b.key_id))
    });
    let cutoff = threshold * tier1;
    let selected: Vec<Candidate> = candidates
        .into_iter()
        .enumerate()
        .filter(|(i, c)| c.value >= cutoff || top_n.is_some_and(|n| *i < n))
        .map(|(_, c)| c)
        .collect();

    // --- rows (components at the max date) ----------------------------------
    let mut rows = Vec::with_capacity(selected.len());
    for (i, c) in selected.into_iter().enumerate() {
        let member_set: std::collections::BTreeSet<&str> =
            c.members.iter().map(String::as_str).collect();
        let mut comp = ExposureMeasure::zero();
        let mut deductions = 0.0;
        let mut exemption_provision: Option<String> = None;
        for e in ledger.events().values() {
            if !member_set.contains(e.entity_id.as_str()) || !e.active_at(c.date) {
                continue;
            }
            if part == MaBs28Part::IV && !e.exempt {
                continue;
            }
            comp.add(&event_components(e, measure));
            deductions += e.deduction;
            if exemption_provision.is_none() {
                exemption_provision = e.exemption_provision.clone();
            }
        }
        let percent = if tier1 > 0.0 {
            c.value / tier1
        } else {
            f64::INFINITY
        };
        rows.push(MaBs28Row {
            rank: i + 1,
            counterparty_id: (!c.group_level).then(|| c.key_id.clone()),
            lc_group_id: c.group_level.then(|| c.key_id.clone()),
            maximum_exposure: c.value,
            on_balance: comp.on_balance,
            trading_book: comp.trading_book,
            off_balance: comp.off_balance,
            default_risk: comp.default_risk,
            indirect: comp.indirect,
            additional_risk: comp.additional_risk,
            total: comp.total(),
            deductions,
            economic_sector: c.sector,
            relationship_code: c.relationship_code,
            percent_of_tier1: percent,
            exemption_provision,
        });
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::LeConfig;
    use crate::entity::{Entity, EntityKind};
    use crate::exposure::ExposureMeasure;
    use crate::ledger::Ledger;
    use crate::temporal::TimeAxis;

    fn ev(id: &str, entity: &str, amount: f64) -> ExposureEvent {
        ExposureEvent::new(
            id,
            entity,
            ExposureMeasure::zero().on_balance(amount),
            0,
            400,
        )
    }

    fn ledger() -> Ledger {
        let mut l = Ledger::new(LeConfig::default(), TimeAxis::new(0, 400));
        l.set_entities(vec![
            Entity::new("A", EntityKind::Bank).with_economic_sector("banks"),
            Entity::new("B", EntityKind::Corporate).with_economic_sector("others"),
        ]);
        l
    }

    #[test]
    fn part_ii_ranks_by_period_max_with_threshold_and_top_n() {
        let mut l = ledger();
        l.add(ev("e1", "A", 300.0)).unwrap();
        l.add(ev("e2", "B", 100.0)).unwrap();
        let rows = ma_bs28_report(&mut l, MaBs28Part::II, 0, 400, 1000.0);
        // 10% of 1000 = 100: A (300) and B (100) both reportable
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].rank, 1);
        assert_eq!(rows[0].lc_group_id.as_deref(), Some("A"));
        assert_eq!(rows[0].maximum_exposure, 300.0);
        assert_eq!(rows[0].on_balance, 300.0);
        assert_eq!(rows[0].total, 300.0);
        assert_eq!(rows[0].percent_of_tier1, 0.30);
        assert_eq!(rows[0].economic_sector, "banks");
        assert_eq!(rows[0].counterparty_id, None);

        // huge Tier 1 -> threshold not met, but the top-20 still lists both
        let rows = ma_bs28_report(&mut l, MaBs28Part::II, 0, 400, 1e9);
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn part_iii_uses_after_crm_and_scales_components() {
        let mut l = ledger();
        l.add(ev("e1", "A", 300.0)).unwrap();
        l.add(ev("e2", "A", 200.0).with_crm_reduction(50.0)).unwrap();
        // A before = 500, after = 450
        let before = ma_bs28_report(&mut l, MaBs28Part::II, 0, 400, 1000.0);
        assert_eq!(before[0].maximum_exposure, 500.0);
        assert_eq!(before[0].total, 500.0);
        let after = ma_bs28_report(&mut l, MaBs28Part::III, 0, 400, 1000.0);
        assert_eq!(after[0].maximum_exposure, 450.0);
        assert_eq!(after[0].total, 450.0);
        // components scaled proportionally (250/300 + 150/200 split)
        assert!((after[0].on_balance - 450.0).abs() < 1e-9);
    }

    #[test]
    fn part_i_connected_parties_and_relationship_code() {
        let mut l = Ledger::new(LeConfig::default(), TimeAxis::new(0, 400));
        l.set_entities(vec![
            Entity::new("A", EntityKind::Corporate)
                .connected()
                .with_connected_paragraph("rule_85(1)(a)"),
            Entity::new("B", EntityKind::Corporate),
        ]);
        l.add(ev("e1", "A", 80.0)).unwrap();
        l.add(ev("e2", "B", 500.0)).unwrap();
        // 5% of 1000 = 50 -> only A (connected) is in Part I
        let rows = ma_bs28_report(&mut l, MaBs28Part::I, 0, 400, 1000.0);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].counterparty_id.as_deref(), Some("A"));
        assert_eq!(rows[0].relationship_code.as_deref(), Some("rule_85(1)(a)"));
        assert_eq!(rows[0].percent_of_tier1, 0.08);
    }

    #[test]
    fn part_iv_uses_reporting_date_exempted_exposure() {
        let mut l = ledger();
        l.add(ev("e1", "A", 150.0).exempt("rule_48(1)(a)")).unwrap();
        l.add(ev("e2", "B", 200.0)).unwrap(); // not exempt
        let rows = ma_bs28_report(&mut l, MaBs28Part::IV, 0, 399, 1000.0);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].lc_group_id.as_deref(), Some("A"));
        assert_eq!(rows[0].maximum_exposure, 150.0);
        assert_eq!(rows[0].exemption_provision.as_deref(), Some("rule_48(1)(a)"));
    }

    #[test]
    fn ranks_are_deterministic_on_ties() {
        let mut l = ledger();
        l.add(ev("e1", "A", 100.0)).unwrap();
        l.add(ev("e2", "B", 100.0)).unwrap();
        let rows = ma_bs28_report(&mut l, MaBs28Part::II, 0, 400, 100.0);
        assert_eq!(rows[0].lc_group_id.as_deref(), Some("A")); // id ascending
        assert_eq!(rows[1].lc_group_id.as_deref(), Some("B"));
    }
}
