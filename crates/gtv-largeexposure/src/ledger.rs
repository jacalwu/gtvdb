//! Incremental exposure ledger: append-only events + per-aggregate-key,
//! coordinate-compressed Fenwick / segment trees.
//!
//! Each aggregate key stores only the business-date boundaries of the events
//! that touch it, so memory is `O(events)` rather than `O(keys × days)`.
//! Adding an event applies `+value` over its interval; correcting one removes
//! the old contribution and applies the new one — each a range-add,
//! `O(log k)` per key (`k` = boundaries of that key).

use std::collections::{BTreeMap, BTreeSet};

use thiserror::Error;

use crate::config::LeConfig;
use crate::entity::Entity;
use crate::exposure::{ExposureEvent, Measure};
use crate::relationship::{connected_party_closure, GroupMap, Relationship, RelationshipKind};
use crate::temporal::{Fenwick, SegTree, TimeAxis};

/// Aggregate key kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum AggregateKind {
    Entity,
    LcGroup,
    ConnectedParty,
    GroupAffiliate,
    Sector,
    Country,
    Rating,
}

impl AggregateKind {
    pub fn as_str(self) -> &'static str {
        match self {
            AggregateKind::Entity => "entity",
            AggregateKind::LcGroup => "lc_group",
            AggregateKind::ConnectedParty => "connected_party",
            AggregateKind::GroupAffiliate => "group_affiliate",
            AggregateKind::Sector => "sector",
            AggregateKind::Country => "country",
            AggregateKind::Rating => "rating",
        }
    }
}

/// A fully-qualified aggregate key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AggKey {
    pub kind: AggregateKind,
    pub id: String,
    pub measure: Measure,
}

/// Ledger errors.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LedgerError {
    #[error("event `{0}` already exists")]
    Duplicate(String),
    #[error("referenced event `{0}` not found")]
    UnknownRef(String),
    #[error("correction event has no `ref_event_id`")]
    MissingRef,
    #[error("event business date is outside the ledger time axis")]
    OutOfAxis,
}

/// Compressed per-key index: boundaries + Fenwick + segment tree + the
/// contributions that define the boundaries.
struct KeyIndex {
    boundaries: Vec<i64>,
    fenwick: Fenwick,
    seg: SegTree,
    contrib: BTreeMap<String, (i64, i64, f64)>,
}

impl KeyIndex {
    fn empty() -> Self {
        Self {
            boundaries: Vec::new(),
            fenwick: Fenwick::new(1),
            seg: SegTree::new(1),
            contrib: BTreeMap::new(),
        }
    }

    fn boundary_set(contrib: &BTreeMap<String, (i64, i64, f64)>) -> Vec<i64> {
        let mut b: Vec<i64> = Vec::with_capacity(contrib.len() * 2);
        for (from, to, _) in contrib.values() {
            b.push(*from);
            b.push(*to);
        }
        b.sort_unstable();
        b.dedup();
        b
    }

    fn rebuild(&mut self) {
        self.boundaries = Self::boundary_set(&self.contrib);
        let n = self.boundaries.len().max(1);
        self.fenwick = Fenwick::new(n);
        self.seg = SegTree::new(n);
        let items: Vec<(i64, i64, f64)> = self.contrib.values().cloned().collect();
        for (from, to, v) in items {
            self.range_add(from, to, v);
        }
    }

    fn idx_ge(&self, date: i64) -> usize {
        self.boundaries.partition_point(|&b| b < date)
    }

    /// Index of the compressed interval containing `date`.
    fn interval_of(&self, date: i64) -> Option<usize> {
        if self.boundaries.is_empty() {
            return None;
        }
        let i = self.boundaries.partition_point(|&b| b <= date);
        if i == 0 || date >= *self.boundaries.last().unwrap() {
            return None;
        }
        Some(i - 1)
    }

    fn range_add(&mut self, from: i64, to: i64, v: f64) {
        let i = self.idx_ge(from);
        let j = self.idx_ge(to);
        if j > i {
            self.fenwick.range_add(i, j, v);
            self.seg.range_add(i, j, v);
        }
    }

    fn point_query(&self, date: i64) -> f64 {
        match self.interval_of(date) {
            Some(i) => self.fenwick.point_query(i),
            None => 0.0,
        }
    }

    fn period_max(&mut self, a: i64, b: i64) -> f64 {
        if self.boundaries.is_empty() {
            return 0.0;
        }
        let ia = self.boundaries.partition_point(|&x| x <= a).saturating_sub(1);
        let ib = self.boundaries.partition_point(|&x| x < b);
        if ib <= ia {
            return 0.0;
        }
        self.seg.range_max(ia, ib)
    }

    /// Maximum over `[a, b)` and the date (leftmost boundary) achieving it.
    fn period_argmax(&mut self, a: i64, b: i64) -> Option<(f64, i64)> {
        if self.boundaries.is_empty() {
            return None;
        }
        let ia = self.boundaries.partition_point(|&x| x <= a).saturating_sub(1);
        let ib = self.boundaries.partition_point(|&x| x < b);
        if ib <= ia {
            return None;
        }
        let (v, idx) = self.seg.range_argmax(ia, ib);
        if idx == usize::MAX {
            None
        } else {
            Some((v, self.boundaries[idx]))
        }
    }

    /// Insert/replace a contribution; rebuild only when a boundary is new.
    fn upsert(&mut self, event_id: &str, from: i64, to: i64, value: f64) {
        let new_boundary = self.boundaries.binary_search(&from).is_err()
            || self.boundaries.binary_search(&to).is_err();
        self.contrib.insert(event_id.to_string(), (from, to, value));
        if new_boundary {
            self.rebuild();
        } else {
            self.range_add(from, to, value);
        }
    }

    /// Remove a contribution. Boundaries are *not* shrunk (a stale boundary
    /// only adds a zero-valued interval), so this is `O(log k)`; a periodic
    /// `rebuild` can compact them.
    fn remove(&mut self, event_id: &str) {
        if let Some((from, to, value)) = self.contrib.remove(event_id) {
            self.range_add(from, to, -value);
        }
    }
}

/// An event-sourced exposure ledger with incremental (suffix) recomputation.
pub struct Ledger {
    cfg: LeConfig,
    axis: TimeAxis,
    entities: BTreeMap<String, Entity>,
    groups: GroupMap,
    connected: BTreeSet<String>,
    affiliates: BTreeSet<String>,
    events: BTreeMap<String, ExposureEvent>,
    structures: BTreeMap<AggKey, KeyIndex>,
}

impl Ledger {
    pub fn new(cfg: LeConfig, axis: TimeAxis) -> Self {
        Self {
            cfg,
            axis,
            entities: BTreeMap::new(),
            groups: GroupMap::default(),
            connected: BTreeSet::new(),
            affiliates: BTreeSet::new(),
            events: BTreeMap::new(),
            structures: BTreeMap::new(),
        }
    }

    pub fn config(&self) -> &LeConfig {
        &self.cfg
    }

    pub fn axis(&self) -> TimeAxis {
        self.axis
    }

    pub fn entities(&self) -> &BTreeMap<String, Entity> {
        &self.entities
    }

    pub fn connected(&self) -> &BTreeSet<String> {
        &self.connected
    }

    pub fn affiliates(&self) -> &BTreeSet<String> {
        &self.affiliates
    }

    pub fn events(&self) -> &BTreeMap<String, ExposureEvent> {
        &self.events
    }

    pub fn event(&self, event_id: &str) -> Option<&ExposureEvent> {
        self.events.get(event_id)
    }

    pub fn set_entities(&mut self, entities: Vec<Entity>) {
        self.entities = entities
            .into_iter()
            .map(|e| (e.entity_id.clone(), e))
            .collect();
        // identity grouping + connected flags until relationships are supplied
        self.groups = GroupMap::build(&self.entities, &[], 0, &self.cfg);
        self.connected = connected_party_closure(&self.entities, &[], 0);
        self.affiliates.clear();
    }

    pub fn set_relationships(&mut self, rels: &[Relationship], as_of: i64) {
        self.groups = GroupMap::build(&self.entities, rels, as_of, &self.cfg);
        self.connected = connected_party_closure(&self.entities, rels, as_of);
        self.affiliates = rels
            .iter()
            .filter(|r| r.kind == RelationshipKind::GroupAffiliate && r.active_at(as_of))
            .map(|r| r.child_id.clone())
            .collect();
    }

    pub fn groups(&self) -> &GroupMap {
        &self.groups
    }

    fn key_values(&self, event: &ExposureEvent) -> Vec<(AggKey, f64)> {
        let mut base: Vec<(AggregateKind, String)> = Vec::new();
        base.push((AggregateKind::Entity, event.entity_id.clone()));
        base.push((AggregateKind::LcGroup, self.groups.group_of(&event.entity_id)));
        if self.connected.contains(&event.entity_id) {
            base.push((AggregateKind::ConnectedParty, event.entity_id.clone()));
        }
        if self.affiliates.contains(&event.entity_id) {
            base.push((AggregateKind::GroupAffiliate, event.entity_id.clone()));
        }
        if let Some(e) = self.entities.get(&event.entity_id) {
            base.push((AggregateKind::Sector, e.sector().as_str().to_string()));
            if let Some(c) = &e.country_code {
                base.push((AggregateKind::Country, c.clone()));
            }
            if let Some(r) = &e.rating_grade {
                base.push((AggregateKind::Rating, r.clone()));
            }
        }
        let mut measures = vec![Measure::BeforeCrm, Measure::AfterCrm];
        if event.exempt {
            measures.push(Measure::Exempted);
        }
        let mut out: Vec<(AggKey, f64)> = Vec::new();
        for (kind, id) in base {
            for m in &measures {
                let value = event.value(*m);
                if value != 0.0 {
                    out.push((
                        AggKey {
                            kind,
                            id: id.clone(),
                            measure: *m,
                        },
                        value,
                    ));
                }
            }
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out.dedup_by(|a, b| a.0 == b.0);
        out
    }

    fn insert_event(&mut self, event: &ExposureEvent) {
        let (from, to) = (event.business_from, event.business_to);
        for (key, value) in self.key_values(event) {
            self.structures
                .entry(key)
                .or_insert_with(KeyIndex::empty)
                .upsert(&event.event_id, from, to, value);
        }
    }

    fn remove_event(&mut self, event: &ExposureEvent) {
        for (key, _) in self.key_values(event) {
            if let Some(ki) = self.structures.get_mut(&key) {
                ki.remove(&event.event_id);
            }
        }
    }

    /// Append a new event.
    pub fn add(&mut self, event: ExposureEvent) -> Result<(), LedgerError> {
        if self.events.contains_key(&event.event_id) {
            return Err(LedgerError::Duplicate(event.event_id));
        }
        if self.axis.index(event.business_from).is_none() {
            return Err(LedgerError::OutOfAxis);
        }
        self.insert_event(&event);
        self.events.insert(event.event_id.clone(), event);
        Ok(())
    }

    /// Correct an existing event (the new event carries `ref_event_id`).
    pub fn correct(&mut self, event: ExposureEvent) -> Result<(), LedgerError> {
        let ref_id = event.ref_event_id.clone().ok_or(LedgerError::MissingRef)?;
        let old = self
            .events
            .get(&ref_id)
            .cloned()
            .ok_or_else(|| LedgerError::UnknownRef(ref_id.clone()))?;
        self.remove_event(&old);
        self.insert_event(&event);
        self.events.remove(&ref_id);
        self.events.insert(event.event_id.clone(), event);
        Ok(())
    }

    /// Bulk-load events, building each key's index once (`O(N log N)`).
    pub fn bulk_load(&mut self, events: Vec<ExposureEvent>) -> Result<(), LedgerError> {
        let mut pending: BTreeMap<AggKey, BTreeMap<String, (i64, i64, f64)>> = BTreeMap::new();
        for event in events {
            if self.events.contains_key(&event.event_id) {
                return Err(LedgerError::Duplicate(event.event_id));
            }
            if self.axis.index(event.business_from).is_none() {
                return Err(LedgerError::OutOfAxis);
            }
            for (key, value) in self.key_values(&event) {
                pending.entry(key).or_default().insert(
                    event.event_id.clone(),
                    (event.business_from, event.business_to, value),
                );
            }
            self.events.insert(event.event_id.clone(), event);
        }
        for (key, contrib) in pending {
            let mut ki = KeyIndex::empty();
            ki.contrib = contrib;
            ki.rebuild();
            self.structures.insert(key, ki);
        }
        Ok(())
    }

    /// Exposure of one aggregate key at `date`.
    pub fn exposure_at(&self, kind: AggregateKind, id: &str, measure: Measure, date: i64) -> f64 {
        self.structures
            .get(&AggKey {
                kind,
                id: id.to_string(),
                measure,
            })
            .map(|ki| ki.point_query(date))
            .unwrap_or(0.0)
    }

    /// Maximum exposure of one aggregate key over `[a, b)`.
    pub fn period_max(
        &mut self,
        kind: AggregateKind,
        id: &str,
        measure: Measure,
        a: i64,
        b: i64,
    ) -> f64 {
        self.structures
            .get_mut(&AggKey {
                kind,
                id: id.to_string(),
                measure,
            })
            .map(|ki| ki.period_max(a, b))
            .unwrap_or(0.0)
    }

    /// Period maximum and the date achieving it.
    pub fn period_argmax(
        &mut self,
        kind: AggregateKind,
        id: &str,
        measure: Measure,
        a: i64,
        b: i64,
    ) -> Option<(f64, i64)> {
        self.structures
            .get_mut(&AggKey {
                kind,
                id: id.to_string(),
                measure,
            })
            .and_then(|ki| ki.period_argmax(a, b))
    }

    /// Total book exposure (sum over all entity keys) at `date`.
    pub fn book_total_at(&self, measure: Measure, date: i64) -> f64 {
        self.structures
            .iter()
            .filter(|(k, _)| k.kind == AggregateKind::Entity && k.measure == measure)
            .map(|(_, ki)| ki.point_query(date))
            .sum()
    }

    pub fn entity_exposure(&self, entity_id: &str, measure: Measure, date: i64) -> f64 {
        self.exposure_at(AggregateKind::Entity, entity_id, measure, date)
    }

    pub fn lc_group_exposure(&self, rep: &str, measure: Measure, date: i64) -> f64 {
        self.exposure_at(AggregateKind::LcGroup, rep, measure, date)
    }

    /// Number of aggregate keys currently indexed (diagnostics).
    pub fn key_count(&self) -> usize {
        self.structures.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::EntityKind;
    use crate::exposure::{EventKind, ExposureMeasure};
    use crate::relationship::RelationshipKind;
    use gtv_refdata::EffectiveRange;

    fn ledger() -> Ledger {
        let mut l = Ledger::new(LeConfig::default(), TimeAxis::new(0, 400));
        l.set_entities(vec![
            Entity::new("A", EntityKind::Corporate),
            Entity::new("B", EntityKind::Corporate),
            Entity::new("C", EntityKind::Bank),
        ]);
        l
    }

    fn ev(id: &str, entity: &str, amount: f64, from: i64, to: i64) -> ExposureEvent {
        ExposureEvent::new(
            id,
            entity,
            ExposureMeasure::zero().on_balance(amount),
            from,
            to,
        )
    }

    fn naive(ledger: &Ledger, entity: &str, date: i64) -> f64 {
        ledger
            .events()
            .values()
            .filter(|e| e.entity_id == entity && e.active_at(date))
            .map(ExposureEvent::before_crm)
            .sum()
    }

    #[test]
    fn add_and_query_matches_naive() {
        let mut l = ledger();
        l.add(ev("e1", "A", 100.0, 10, 200)).unwrap();
        l.add(ev("e2", "A", 50.0, 100, 300)).unwrap();
        l.add(ev("e3", "B", 70.0, 0, 400)).unwrap();
        for d in [0, 10, 50, 100, 150, 200, 250, 350] {
            assert_eq!(
                l.entity_exposure("A", Measure::BeforeCrm, d),
                naive(&l, "A", d),
                "date {d}"
            );
        }
        assert_eq!(l.entity_exposure("A", Measure::BeforeCrm, 150), 150.0);
        assert_eq!(l.entity_exposure("A", Measure::BeforeCrm, 250), 50.0);
    }

    #[test]
    fn period_max_matches_expectation() {
        let mut l = ledger();
        l.add(ev("e1", "A", 100.0, 10, 100)).unwrap();
        l.add(ev("e2", "A", 250.0, 100, 150)).unwrap();
        l.add(ev("e3", "A", 30.0, 150, 400)).unwrap();
        assert_eq!(l.period_max(AggregateKind::Entity, "A", Measure::BeforeCrm, 0, 400), 250.0);
        assert_eq!(l.period_max(AggregateKind::Entity, "A", Measure::BeforeCrm, 0, 100), 100.0);
        assert_eq!(l.period_max(AggregateKind::Entity, "A", Measure::BeforeCrm, 150, 400), 30.0);
    }

    #[test]
    fn correction_only_changes_the_suffix() {
        let mut l = ledger();
        l.add(ev("e1", "A", 100.0, 100, 200)).unwrap();
        assert_eq!(l.entity_exposure("A", Measure::BeforeCrm, 50), 0.0);
        let mut c = ev("e1c", "A", 140.0, 150, 200);
        c.kind = EventKind::Correction;
        c.ref_event_id = Some("e1".into());
        l.correct(c).unwrap();
        assert_eq!(l.entity_exposure("A", Measure::BeforeCrm, 120), 0.0);
        assert_eq!(l.entity_exposure("A", Measure::BeforeCrm, 160), 140.0);
        assert_eq!(l.period_max(AggregateKind::Entity, "A", Measure::BeforeCrm, 0, 400), 140.0);
    }

    #[test]
    fn group_and_connected_keys_aggregate_members() {
        let mut l = ledger();
        let rels = vec![Relationship::new(
            "A",
            "B",
            RelationshipKind::Control,
            0.6,
            EffectiveRange::from_now_on(0),
        )];
        l.set_relationships(&rels, 0);
        l.add(ev("e1", "A", 100.0, 0, 400)).unwrap();
        l.add(ev("e2", "B", 40.0, 0, 400)).unwrap();
        assert_eq!(l.groups().group_of("B"), "A");
        assert_eq!(l.lc_group_exposure("A", Measure::BeforeCrm, 10), 140.0);
        assert_eq!(l.book_total_at(Measure::BeforeCrm, 10), 140.0);
    }

    #[test]
    fn bulk_load_then_correct_matches_naive() {
        let mut l = ledger();
        let evs = vec![
            ev("e1", "A", 100.0, 0, 100),
            ev("e2", "A", 200.0, 100, 200),
            ev("e3", "B", 50.0, 50, 300),
        ];
        l.bulk_load(evs).unwrap();
        assert_eq!(l.entity_exposure("A", Measure::BeforeCrm, 150), 200.0);
        let mut c = ev("e2c", "A", 260.0, 100, 200);
        c.kind = EventKind::Correction;
        c.ref_event_id = Some("e2".into());
        l.correct(c).unwrap();
        assert_eq!(l.entity_exposure("A", Measure::BeforeCrm, 150), 260.0);
        assert_eq!(l.period_max(AggregateKind::Entity, "A", Measure::BeforeCrm, 0, 400), 260.0);
    }

    #[test]
    fn randomized_corrections_match_naive() {
        let mut l = ledger();
        let mut x: u64 = 987654321;
        let mut rng = || {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            x >> 33
        };
        let entities = ["A", "B", "C"];
        let mut ids: Vec<String> = Vec::new();
        for i in 0..300u64 {
            let from = (rng() % 300) as i64;
            let to = from + 1 + (rng() % 100) as i64;
            let amount = ((rng() % 1000) as f64) + 1.0;
            let entity = entities[(rng() % 3) as usize];
            let id = format!("e{i}");
            l.add(ev(&id, entity, amount, from, to)).unwrap();
            ids.push(id);
        }
        let mut live = ids.clone();
        for k in 0..100u64 {
            let pos = (rng() as usize) % live.len();
            let target = live[pos].clone();
            let old = l.event(&target).unwrap().clone();
            let mut c = ev(&format!("c{k}"), &old.entity_id, ((rng() % 1000) as f64) + 1.0, old.business_from, old.business_to);
            c.kind = EventKind::Correction;
            c.ref_event_id = Some(target.clone());
            l.correct(c).unwrap();
            live[pos] = format!("c{k}");
        }
        for entity in entities {
            for d in (0..400).step_by(17) {
                assert_eq!(
                    l.entity_exposure(entity, Measure::BeforeCrm, d),
                    naive(&l, entity, d),
                    "entity {entity} date {d}"
                );
            }
        }
    }
}
