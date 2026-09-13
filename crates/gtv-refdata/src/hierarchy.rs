//! Effective-dated hierarchies (legal entity / organisation / product).

use std::collections::{BTreeMap, BTreeSet};

use gtv_core::{BitemporalRange, OPEN_ENDED};

use crate::error::RefDataError;

/// Which master hierarchy an edge belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum HierarchyKind {
    LegalEntity,
    Organisation,
    Product,
}

impl HierarchyKind {
    pub const ALL: [HierarchyKind; 3] = [
        HierarchyKind::LegalEntity,
        HierarchyKind::Organisation,
        HierarchyKind::Product,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            HierarchyKind::LegalEntity => "legal_entity",
            HierarchyKind::Organisation => "organisation",
            HierarchyKind::Product => "product",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().replace('-', "_").as_str() {
            "legal_entity" | "legalentity" | "entity" => Some(HierarchyKind::LegalEntity),
            "organisation" | "organization" | "org" => Some(HierarchyKind::Organisation),
            "product" => Some(HierarchyKind::Product),
            _ => None,
        }
    }
}

/// A half-open effective interval `[from, to)` on the **business** time axis.
///
/// This is the effective-dating projection of [`BitemporalRange`]: it reuses
/// the same half-open semantics and the [`OPEN_ENDED`] sentinel for "still
/// current".
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EffectiveRange {
    pub from: i64,
    pub to: i64,
}

impl EffectiveRange {
    /// Build a validated interval. `from < to` is required.
    pub fn new(from: i64, to: i64) -> Result<Self, RefDataError> {
        if from >= to {
            return Err(RefDataError::InvalidInterval { from, to });
        }
        Ok(Self { from, to })
    }

    /// An interval that starts at `from` and never ends.
    pub fn from_now_on(from: i64) -> Self {
        Self {
            from,
            to: OPEN_ENDED,
        }
    }

    /// Project the business-time axis of a bitemporal range.
    pub fn from_bitemporal(range: &BitemporalRange) -> Result<Self, RefDataError> {
        Self::new(range.business_from, range.business_to)
    }

    #[inline]
    pub fn contains(&self, t: i64) -> bool {
        self.from <= t && t < self.to
    }

    #[inline]
    pub fn overlaps(&self, other: &Self) -> bool {
        self.from < other.to && other.from < self.to
    }
}

/// One `parent -> child` hierarchy edge, effective over a half-open interval.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HierarchyEdge {
    pub parent: String,
    pub child: String,
    pub kind: HierarchyKind,
    pub effective: EffectiveRange,
}

impl HierarchyEdge {
    pub fn new(
        kind: HierarchyKind,
        parent: impl Into<String>,
        child: impl Into<String>,
        effective: EffectiveRange,
    ) -> Self {
        Self {
            parent: parent.into(),
            child: child.into(),
            kind,
            effective,
        }
    }

    #[inline]
    pub fn active_at(&self, t: i64) -> bool {
        self.effective.contains(t)
    }
}

/// An effective-dated hierarchy forest.
///
/// Edges are `parent -> child`. Cycles that are simultaneously active at any
/// instant inside the candidate edge's interval are rejected; a reorganisation
/// where `A` is parent of `B` in one period and `B` is parent of `A` in a
/// later, non-overlapping period is allowed (the two edges never coexist).
#[derive(Debug, Default, Clone)]
pub struct Hierarchy {
    edges: Vec<HierarchyEdge>,
}

impl Hierarchy {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn edges(&self) -> &[HierarchyEdge] {
        &self.edges
    }

    pub fn len(&self) -> usize {
        self.edges.len()
    }

    pub fn is_empty(&self) -> bool {
        self.edges.is_empty()
    }

    /// Add an edge, rejecting empty ids, self references, time-overlapping
    /// duplicates and any cycle that would be simultaneously active.
    pub fn add_edge(&mut self, edge: HierarchyEdge) -> Result<(), RefDataError> {
        if edge.parent.trim().is_empty() || edge.child.trim().is_empty() {
            return Err(RefDataError::EmptyId);
        }
        if edge.parent == edge.child {
            return Err(RefDataError::SelfReference { id: edge.parent });
        }
        let duplicate = self.edges.iter().any(|e| {
            e.kind == edge.kind
                && e.parent == edge.parent
                && e.child == edge.child
                && e.effective.overlaps(&edge.effective)
        });
        if duplicate {
            return Err(RefDataError::DuplicateEdge {
                kind: edge.kind.as_str().to_string(),
                parent: edge.parent,
                child: edge.child,
            });
        }
        if self.would_create_cycle(&edge) {
            return Err(RefDataError::CyclicHierarchy {
                kind: edge.kind.as_str().to_string(),
                parent: edge.parent,
                child: edge.child,
            });
        }
        self.edges.push(edge);
        Ok(())
    }

    /// True if adding `candidate` makes `candidate.child` reach
    /// `candidate.parent` at some instant inside the candidate interval.
    ///
    /// Edge activity only changes at interval endpoints, so it is enough to
    /// test each sub-interval start.
    fn would_create_cycle(&self, candidate: &HierarchyEdge) -> bool {
        let mut instants = BTreeSet::new();
        instants.insert(candidate.effective.from);
        for e in self
            .edges
            .iter()
            .filter(|e| e.kind == candidate.kind && e.effective.overlaps(&candidate.effective))
        {
            let start = e.effective.from.max(candidate.effective.from);
            if start < candidate.effective.to {
                instants.insert(start);
            }
        }
        instants
            .into_iter()
            .any(|t| self.reaches_at(candidate.kind, &candidate.child, &candidate.parent, t))
    }

    /// BFS from `from` to `to` following active `parent -> child` edges.
    fn reaches_at(&self, kind: HierarchyKind, from: &str, to: &str, t: i64) -> bool {
        let mut visited = BTreeSet::new();
        let mut stack = vec![from.to_string()];
        visited.insert(from.to_string());
        while let Some(node) = stack.pop() {
            if node == to {
                return true;
            }
            for e in self
                .edges
                .iter()
                .filter(|e| e.kind == kind && e.parent == node && e.active_at(t))
            {
                if visited.insert(e.child.clone()) {
                    stack.push(e.child.clone());
                }
            }
        }
        false
    }

    /// Direct parents of `id` active at `as_of`, sorted and unique.
    pub fn parents(&self, id: &str, kind: HierarchyKind, as_of: i64) -> Vec<String> {
        let mut out: Vec<String> = self
            .edges
            .iter()
            .filter(|e| e.kind == kind && e.child == id && e.active_at(as_of))
            .map(|e| e.parent.clone())
            .collect();
        out.sort();
        out.dedup();
        out
    }

    /// Direct children of `id` active at `as_of`, sorted and unique.
    pub fn children(&self, id: &str, kind: HierarchyKind, as_of: i64) -> Vec<String> {
        let mut out: Vec<String> = self
            .edges
            .iter()
            .filter(|e| e.kind == kind && e.parent == id && e.active_at(as_of))
            .map(|e| e.child.clone())
            .collect();
        out.sort();
        out.dedup();
        out
    }

    /// All transitive ancestors of `id` (excluding `id`), sorted.
    pub fn ancestors(&self, id: &str, kind: HierarchyKind, as_of: i64) -> Vec<String> {
        self.transitive(id, kind, as_of, Direction::Up)
    }

    /// All transitive descendants of `id` (excluding `id`), sorted.
    pub fn descendants(&self, id: &str, kind: HierarchyKind, as_of: i64) -> Vec<String> {
        self.transitive(id, kind, as_of, Direction::Down)
    }

    fn transitive(
        &self,
        id: &str,
        kind: HierarchyKind,
        as_of: i64,
        direction: Direction,
    ) -> Vec<String> {
        let mut visited: BTreeSet<String> = BTreeSet::new();
        let mut stack = vec![id.to_string()];
        while let Some(node) = stack.pop() {
            let next = match direction {
                Direction::Up => self.parents(&node, kind, as_of),
                Direction::Down => self.children(&node, kind, as_of),
            };
            for n in next {
                if n != id && visited.insert(n.clone()) {
                    stack.push(n);
                }
            }
        }
        visited.into_iter().collect()
    }

    /// All node ids appearing in active edges at `as_of`, sorted.
    pub fn nodes(&self, kind: HierarchyKind, as_of: i64) -> Vec<String> {
        let mut out: BTreeSet<String> = BTreeSet::new();
        for e in self.edges.iter().filter(|e| e.kind == kind && e.active_at(as_of)) {
            out.insert(e.parent.clone());
            out.insert(e.child.clone());
        }
        out.into_iter().collect()
    }

    /// Nodes with no active parent.
    pub fn roots(&self, kind: HierarchyKind, as_of: i64) -> Vec<String> {
        self.nodes(kind, as_of)
            .into_iter()
            .filter(|n| self.parents(n, kind, as_of).is_empty())
            .collect()
    }

    /// Nodes with no active child.
    pub fn leaves(&self, kind: HierarchyKind, as_of: i64) -> Vec<String> {
        self.nodes(kind, as_of)
            .into_iter()
            .filter(|n| self.children(n, kind, as_of).is_empty())
            .collect()
    }

    /// Roll `(node, value)` weights up to every ancestor (including the node
    /// itself). Deterministic: keyed by node id in a [`BTreeMap`].
    pub fn rollup(
        &self,
        weights: &[(String, f64)],
        kind: HierarchyKind,
        as_of: i64,
    ) -> BTreeMap<String, f64> {
        let mut totals: BTreeMap<String, f64> = BTreeMap::new();
        for (node, value) in weights {
            *totals.entry(node.clone()).or_insert(0.0) += value;
            for ancestor in self.ancestors(node, kind, as_of) {
                *totals.entry(ancestor).or_insert(0.0) += value;
            }
        }
        totals
    }
}

#[derive(Clone, Copy)]
enum Direction {
    Up,
    Down,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn edge(parent: &str, child: &str, from: i64, to: i64) -> HierarchyEdge {
        HierarchyEdge::new(
            HierarchyKind::LegalEntity,
            parent,
            child,
            EffectiveRange::new(from, to).unwrap(),
        )
    }

    fn tree() -> Hierarchy {
        // root -> {a, b}; a -> {a1, a2}
        let mut h = Hierarchy::new();
        h.add_edge(edge("root", "a", 0, OPEN_ENDED)).unwrap();
        h.add_edge(edge("root", "b", 0, OPEN_ENDED)).unwrap();
        h.add_edge(edge("a", "a1", 0, OPEN_ENDED)).unwrap();
        h.add_edge(edge("a", "a2", 0, OPEN_ENDED)).unwrap();
        h
    }

    /// Independent brute-force oracle for transitive closure.
    fn oracle_ancestors(edges: &[HierarchyEdge], id: &str, as_of: i64) -> Vec<String> {
        let mut seen = BTreeSet::new();
        let mut stack = vec![id.to_string()];
        while let Some(n) = stack.pop() {
            for e in edges.iter().filter(|e| e.active_at(as_of)) {
                if e.child == n && seen.insert(e.parent.clone()) {
                    stack.push(e.parent.clone());
                }
            }
        }
        seen.into_iter().collect()
    }

    #[test]
    fn ancestor_and_descendant_queries_match_oracle() {
        let h = tree();
        assert_eq!(
            h.ancestors("a1", HierarchyKind::LegalEntity, 0),
            oracle_ancestors(h.edges(), "a1", 0)
        );
        assert_eq!(h.ancestors("a1", HierarchyKind::LegalEntity, 0), vec!["a", "root"]);
        assert_eq!(
            h.descendants("root", HierarchyKind::LegalEntity, 0),
            vec!["a", "a1", "a2", "b"]
        );
        assert_eq!(h.parents("a1", HierarchyKind::LegalEntity, 0), vec!["a"]);
        assert_eq!(
            h.children("root", HierarchyKind::LegalEntity, 0),
            vec!["a", "b"]
        );
        assert_eq!(h.roots(HierarchyKind::LegalEntity, 0), vec!["root"]);
        assert_eq!(
            h.leaves(HierarchyKind::LegalEntity, 0),
            vec!["a1", "a2", "b"]
        );
    }

    #[test]
    fn effective_dating_picks_the_right_parent() {
        let mut h = Hierarchy::new();
        // a1 sits under "old" for [0,100), then under "new" for [100,inf)
        h.add_edge(edge("old", "a1", 0, 100)).unwrap();
        h.add_edge(edge("new", "a1", 100, OPEN_ENDED)).unwrap();
        assert_eq!(h.parents("a1", HierarchyKind::LegalEntity, 50), vec!["old"]);
        assert_eq!(h.parents("a1", HierarchyKind::LegalEntity, 100), vec!["new"]);
        assert!(h.parents("a1", HierarchyKind::LegalEntity, 0).contains(&"old".to_string()));
        assert!(h.ancestors("a1", HierarchyKind::LegalEntity, 50) == vec!["old"]);
    }

    #[test]
    fn cycles_are_rejected_including_temporal_ones() {
        let mut h = Hierarchy::new();
        h.add_edge(edge("a", "b", 0, 10)).unwrap();
        // overlapping reverse edge -> cycle
        assert!(matches!(
            h.add_edge(edge("b", "a", 5, 15)),
            Err(RefDataError::CyclicHierarchy { .. })
        ));
        // non-overlapping reverse edge is a legitimate reorganisation
        h.add_edge(edge("b", "a", 10, 20)).unwrap();
        assert_eq!(h.len(), 2);
    }

    #[test]
    fn self_reference_and_duplicates_are_rejected() {
        let mut h = Hierarchy::new();
        assert!(matches!(
            h.add_edge(edge("a", "a", 0, 10)),
            Err(RefDataError::SelfReference { .. })
        ));
        h.add_edge(edge("a", "b", 0, 10)).unwrap();
        assert!(matches!(
            h.add_edge(edge("a", "b", 5, 15)),
            Err(RefDataError::DuplicateEdge { .. })
        ));
        assert!(matches!(
            h.add_edge(HierarchyEdge::new(
                HierarchyKind::LegalEntity,
                "",
                "b",
                EffectiveRange::from_now_on(0)
            )),
            Err(RefDataError::EmptyId)
        ));
    }

    #[test]
    fn rollup_aggregates_to_every_ancestor() {
        let h = tree();
        let weights = vec![
            ("a1".to_string(), 10.0),
            ("a2".to_string(), 5.0),
            ("b".to_string(), 2.0),
        ];
        let totals = h.rollup(&weights, HierarchyKind::LegalEntity, 0);
        assert_eq!(totals["a1"], 10.0);
        assert_eq!(totals["a2"], 5.0);
        assert_eq!(totals["a"], 15.0);
        assert_eq!(totals["b"], 2.0);
        assert_eq!(totals["root"], 17.0);
    }

    #[test]
    fn rollup_is_deterministic() {
        let h = tree();
        let weights = vec![("a1".to_string(), 10.0), ("a2".to_string(), 5.0)];
        assert_eq!(
            h.rollup(&weights, HierarchyKind::LegalEntity, 0),
            h.rollup(&weights, HierarchyKind::LegalEntity, 0)
        );
    }

    #[test]
    fn kinds_are_isolated() {
        let mut h = Hierarchy::new();
        h.add_edge(HierarchyEdge::new(
            HierarchyKind::LegalEntity,
            "p",
            "c",
            EffectiveRange::from_now_on(0),
        ))
        .unwrap();
        h.add_edge(HierarchyEdge::new(
            HierarchyKind::Product,
            "p",
            "c",
            EffectiveRange::from_now_on(0),
        ))
        .unwrap();
        assert_eq!(h.parents("c", HierarchyKind::Product, 0), vec!["p"]);
        assert!(h.ancestors("c", HierarchyKind::Organisation, 0).is_empty());
    }

    #[test]
    fn effective_range_projects_bitemporal_business_axis() {
        let bt = BitemporalRange::new(10, 20, 5, OPEN_ENDED);
        let eff = EffectiveRange::from_bitemporal(&bt).unwrap();
        assert_eq!(eff, EffectiveRange::new(10, 20).unwrap());
        assert!(matches!(
            EffectiveRange::new(20, 10),
            Err(RefDataError::InvalidInterval { .. })
        ));
    }
}
