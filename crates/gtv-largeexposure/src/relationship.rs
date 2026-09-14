//! Entity relationships: control / economic dependence / group affiliation /
//! connected persons, with effective dating.

use std::collections::{BTreeMap, BTreeSet};

use gtv_refdata::EffectiveRange;

use crate::config::LeConfig;
use crate::entity::Entity;

/// Kind of relationship edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RelationshipKind {
    /// Control (BELR rule 41 / BO control definition).
    Control,
    /// Economic interdependence (supervisory judgement).
    EconomicDependence,
    /// Group affiliate (intragroup; MA(BS)28 Part V).
    GroupAffiliate,
    /// Natural person / director / shareholder connected to the AI (Part I).
    ConnectedPerson,
}

impl RelationshipKind {
    pub fn as_str(self) -> &'static str {
        match self {
            RelationshipKind::Control => "control",
            RelationshipKind::EconomicDependence => "economic_dependence",
            RelationshipKind::GroupAffiliate => "group_affiliate",
            RelationshipKind::ConnectedPerson => "connected_person",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().replace([' ', '-'], "_").as_str() {
            "control" => Some(RelationshipKind::Control),
            "economic_dependence" | "economic" => Some(RelationshipKind::EconomicDependence),
            "group_affiliate" | "affiliate" | "intragroup" => Some(RelationshipKind::GroupAffiliate),
            "connected_person" | "connected" => Some(RelationshipKind::ConnectedPerson),
            _ => None,
        }
    }
}

/// One effective-dated relationship edge.
#[derive(Debug, Clone, PartialEq)]
pub struct Relationship {
    pub parent_id: String,
    pub child_id: String,
    pub kind: RelationshipKind,
    /// Ownership / voting-rights percentage (for `Control`).
    pub ownership_pct: f64,
    pub effective: EffectiveRange,
}

impl Relationship {
    pub fn new(
        parent_id: impl Into<String>,
        child_id: impl Into<String>,
        kind: RelationshipKind,
        ownership_pct: f64,
        effective: EffectiveRange,
    ) -> Self {
        Self {
            parent_id: parent_id.into(),
            child_id: child_id.into(),
            kind,
            ownership_pct,
            effective,
        }
    }

    #[inline]
    pub fn active_at(&self, as_of: i64) -> bool {
        self.effective.contains(as_of)
    }
}

fn find(parent: &mut BTreeMap<String, String>, x: &str) -> String {
    let mut root = x.to_string();
    while parent.get(&root).map(String::as_str) != Some(root.as_str()) {
        root = parent.get(&root).cloned().unwrap_or_else(|| root.clone());
    }
    // path compression
    let mut cur = x.to_string();
    while cur != root {
        let next = parent.get(&cur).cloned().unwrap_or_else(|| cur.clone());
        parent.insert(cur, root.clone());
        cur = next;
    }
    root
}

fn union(parent: &mut BTreeMap<String, String>, a: &str, b: &str) {
    let (ra, rb) = (find(parent, a), find(parent, b));
    if ra == rb {
        return;
    }
    // deterministic representative: lexicographically smallest id
    let (lo, hi) = if ra < rb { (ra, rb) } else { (rb, ra) };
    parent.insert(hi, lo);
}

/// Connected-component map of entities under the clustering rules.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct GroupMap {
    group_of: BTreeMap<String, String>,
    members: BTreeMap<String, Vec<String>>,
}

impl GroupMap {
    /// Build the LC-group map at `as_of`: control edges above
    /// `cfg.control_threshold`, plus economic-dependence edges when enabled.
    pub fn build(
        entities: &BTreeMap<String, Entity>,
        rels: &[Relationship],
        as_of: i64,
        cfg: &LeConfig,
    ) -> Self {
        let mut parent: BTreeMap<String, String> = entities
            .keys()
            .map(|id| (id.clone(), id.clone()))
            .collect();
        for r in rels.iter().filter(|r| r.active_at(as_of)) {
            parent.entry(r.parent_id.clone()).or_insert_with(|| r.parent_id.clone());
            parent.entry(r.child_id.clone()).or_insert_with(|| r.child_id.clone());
            let cluster = match r.kind {
                RelationshipKind::Control => r.ownership_pct >= cfg.control_threshold,
                RelationshipKind::EconomicDependence => cfg.include_economic_dependence,
                _ => false,
            };
            if cluster {
                union(&mut parent, &r.parent_id, &r.child_id);
            }
        }
        let ids: Vec<String> = parent.keys().cloned().collect();
        let mut group_of = BTreeMap::new();
        let mut members: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for id in ids {
            let rep = find(&mut parent, &id);
            group_of.insert(id.clone(), rep.clone());
            members.entry(rep).or_default().push(id);
        }
        for v in members.values_mut() {
            v.sort();
        }
        Self { group_of, members }
    }

    pub fn is_empty(&self) -> bool {
        self.group_of.is_empty()
    }

    /// The LC-group representative of `id` (itself when ungrouped/unknown).
    pub fn group_of(&self, id: &str) -> String {
        self.group_of
            .get(id)
            .cloned()
            .unwrap_or_else(|| id.to_string())
    }

    /// Members of the group keyed by representative (empty slice if unknown).
    pub fn members_of(&self, rep: &str) -> &[String] {
        self.members.get(rep).map(Vec::as_slice).unwrap_or(&[])
    }

    pub fn groups(&self) -> impl Iterator<Item = (&String, &Vec<String>)> {
        self.members.iter()
    }

    pub fn group_count(&self) -> usize {
        self.members.len()
    }
}

/// The set of connected parties at `as_of`: entities explicitly flagged
/// connected, plus children of active `ConnectedPerson` edges.
pub fn connected_party_closure(
    entities: &BTreeMap<String, Entity>,
    rels: &[Relationship],
    as_of: i64,
) -> BTreeSet<String> {
    let mut out: BTreeSet<String> = entities
        .values()
        .filter(|e| e.is_connected)
        .map(|e| e.entity_id.clone())
        .collect();
    for r in rels
        .iter()
        .filter(|r| r.kind == RelationshipKind::ConnectedPerson && r.active_at(as_of))
    {
        out.insert(r.child_id.clone());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::EntityKind;

    fn open(from: i64) -> EffectiveRange {
        EffectiveRange::from_now_on(from)
    }

    fn entities(ids: &[&str]) -> BTreeMap<String, Entity> {
        ids.iter()
            .map(|id| (id.to_string(), Entity::new(*id, EntityKind::Corporate)))
            .collect()
    }

    #[test]
    fn control_edges_cluster_and_use_min_representative() {
        let e = entities(&["A", "B", "C", "D"]);
        let rels = vec![
            Relationship::new("A", "B", RelationshipKind::Control, 0.6, open(0)),
            Relationship::new("B", "C", RelationshipKind::Control, 0.8, open(0)),
            // below threshold -> no cluster
            Relationship::new("A", "D", RelationshipKind::Control, 0.4, open(0)),
        ];
        let cfg = LeConfig::default();
        let g = GroupMap::build(&e, &rels, 100, &cfg);
        assert_eq!(g.group_of("A"), "A");
        assert_eq!(g.group_of("B"), "A");
        assert_eq!(g.group_of("C"), "A"); // transitive
        assert_eq!(g.group_of("D"), "D"); // isolated
        assert_eq!(g.members_of("A"), &["A".to_string(), "B".into(), "C".into()]);
        assert_eq!(g.group_count(), 2);
    }

    #[test]
    fn effective_dating_and_economic_dependence_toggle() {
        let e = entities(&["A", "B"]);
        let rels = vec![
            Relationship::new("A", "B", RelationshipKind::Control, 0.6, EffectiveRange::new(0, 50).unwrap()),
            Relationship::new("A", "B", RelationshipKind::Control, 0.6, EffectiveRange::new(50, i64::MAX).unwrap()),
        ];
        let mut cfg = LeConfig::default();
        assert_eq!(GroupMap::build(&e, &rels, 100, &cfg).group_of("B"), "A");

        // economic dependence only clusters when enabled
        let econ = vec![Relationship::new(
            "A",
            "B",
            RelationshipKind::EconomicDependence,
            0.0,
            open(0),
        )];
        cfg.include_economic_dependence = true;
        assert_eq!(GroupMap::build(&e, &econ, 0, &cfg).group_of("B"), "A");
        cfg.include_economic_dependence = false;
        assert_eq!(GroupMap::build(&e, &econ, 0, &cfg).group_of("B"), "B");
    }

    #[test]
    fn connected_party_closure_works() {
        let mut e = entities(&["A", "B", "C"]);
        e.get_mut("A").unwrap().is_connected = true;
        let rels = vec![Relationship::new(
            "P1",
            "C",
            RelationshipKind::ConnectedPerson,
            0.0,
            open(0),
        )];
        let set = connected_party_closure(&e, &rels, 0);
        assert!(set.contains("A") && set.contains("C") && !set.contains("B"));
    }
}
