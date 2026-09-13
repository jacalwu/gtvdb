//! AML case explainability (prod_p4 D3 / roadmap P2.3).
//!
//! Graph / vector alerting lives in the engine; this module owns the
//! *explanation* layer around it:
//!
//! * [`Transaction`] aggregation by amount / currency / jurisdiction / channel.
//! * [`explain_subgraph`] — the alert explanation subgraph (which nodes and
//!   flows actually triggered the alert), directed or undirected.
//! * [`BeneficialOwnership`] — beneficial-ownership closure over effective
//!   ownership percentages, with cycle-safe ultimate-owner resolution.
//! * [`hybrid_score`] — graph risk + embedding similarity fusion.
//! * [`CaseSnapshot`] — deterministic, replayable case artifact.
//! * [`FeedbackLedger`] — investigator true/false-positive feedback and a
//!   false-positive-rate-driven threshold suggestion.

use std::collections::{BTreeMap, BTreeSet};

/// Whether a motif/flow is traversed with or without edge direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Directed,
    Undirected,
}

/// One transaction edge in the AML graph.
#[derive(Debug, Clone, PartialEq)]
pub struct Transaction {
    pub from: String,
    pub to: String,
    pub amount: f64,
    pub currency: String,
    pub jurisdiction: String,
    pub channel: String,
    pub timestamp: i64,
}

impl Transaction {
    pub fn new(
        from: impl Into<String>,
        to: impl Into<String>,
        amount: f64,
        currency: impl Into<String>,
    ) -> Self {
        Self {
            from: from.into(),
            to: to.into(),
            amount,
            currency: currency.into(),
            jurisdiction: String::new(),
            channel: String::new(),
            timestamp: 0,
        }
    }

    pub fn with_jurisdiction(mut self, v: impl Into<String>) -> Self {
        self.jurisdiction = v.into();
        self
    }

    pub fn with_channel(mut self, v: impl Into<String>) -> Self {
        self.channel = v.into();
        self
    }

    pub fn with_timestamp(mut self, v: i64) -> Self {
        self.timestamp = v;
        self
    }
}

/// Aggregated alert statistics.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AlertAggregates {
    pub count: usize,
    pub total_amount: f64,
    pub by_currency: BTreeMap<String, f64>,
    pub by_jurisdiction: BTreeMap<String, f64>,
    pub by_channel: BTreeMap<String, f64>,
}

impl AlertAggregates {
    /// Aggregate a transaction set by amount, currency, jurisdiction and channel.
    pub fn from_transactions(transactions: &[Transaction]) -> Self {
        let mut out = Self {
            count: transactions.len(),
            ..Default::default()
        };
        for t in transactions {
            out.total_amount += t.amount;
            *out.by_currency.entry(t.currency.clone()).or_insert(0.0) += t.amount;
            *out.by_jurisdiction
                .entry(t.jurisdiction.clone())
                .or_insert(0.0) += t.amount;
            *out.by_channel.entry(t.channel.clone()).or_insert(0.0) += t.amount;
        }
        out
    }
}

/// One edge kept in an alert explanation subgraph.
#[derive(Debug, Clone, PartialEq)]
pub struct ExplainedEdge {
    pub src: String,
    pub dst: String,
    pub amount: f64,
    pub currency: String,
    pub jurisdiction: String,
    pub channel: String,
    pub timestamp: i64,
    /// Traversal direction relative to the original transaction.
    pub direction: Direction,
}

/// The subgraph that explains why `seed` raised an alert.
#[derive(Debug, Clone, PartialEq)]
pub struct ExplanationSubgraph {
    pub seed: String,
    /// Sorted unique node ids (including the seed).
    pub nodes: Vec<String>,
    /// Deterministic edge list.
    pub edges: Vec<ExplainedEdge>,
}

impl ExplanationSubgraph {
    pub fn aggregates(&self) -> AlertAggregates {
        let mut by_currency = BTreeMap::new();
        let mut by_jurisdiction = BTreeMap::new();
        let mut by_channel = BTreeMap::new();
        let mut total = 0.0;
        for e in &self.edges {
            total += e.amount;
            *by_currency.entry(e.currency.clone()).or_insert(0.0) += e.amount;
            *by_jurisdiction.entry(e.jurisdiction.clone()).or_insert(0.0) += e.amount;
            *by_channel.entry(e.channel.clone()).or_insert(0.0) += e.amount;
        }
        AlertAggregates {
            count: self.edges.len(),
            total_amount: total,
            by_currency,
            by_jurisdiction,
            by_channel,
        }
    }
}

/// BFS the explanation subgraph out to `max_hops` from `seed`.
///
/// `Directed` follows `from -> to`; `Undirected` follows both directions. Edge
/// ordering is deterministic, so the same inputs always yield the same
/// subgraph.
pub fn explain_subgraph(
    transactions: &[Transaction],
    seed: &str,
    max_hops: u32,
    direction: Direction,
) -> ExplanationSubgraph {
    let mut nodes: BTreeSet<String> = BTreeSet::new();
    nodes.insert(seed.to_string());
    let mut collected: BTreeSet<(usize, bool)> = BTreeSet::new(); // (tx index, reversed)
    let mut frontier = vec![seed.to_string()];

    for _ in 0..max_hops {
        let mut next = Vec::new();
        for node in &frontier {
            for (i, t) in transactions.iter().enumerate() {
                let forward = &t.from == node;
                let backward = &t.to == node;
                if forward && collected.insert((i, false)) {
                    nodes.insert(t.to.clone());
                    next.push(t.to.clone());
                }
                if direction == Direction::Undirected
                    && backward
                    && &t.from != node
                    && collected.insert((i, true))
                {
                    nodes.insert(t.from.clone());
                    next.push(t.from.clone());
                }
            }
        }
        frontier = next;
        if frontier.is_empty() {
            break;
        }
    }

    let edges = collected
        .into_iter()
        .map(|(i, _reversed)| {
            let t = &transactions[i];
            // Preserve the original transaction direction; `direction` records
            // how the edge entered the explanation.
            ExplainedEdge {
                src: t.from.clone(),
                dst: t.to.clone(),
                amount: t.amount,
                currency: t.currency.clone(),
                jurisdiction: t.jurisdiction.clone(),
                channel: t.channel.clone(),
                timestamp: t.timestamp,
                direction,
            }
        })
        .collect();

    ExplanationSubgraph {
        seed: seed.to_string(),
        nodes: nodes.into_iter().collect(),
        edges,
    }
}

/// One ownership edge: `owner` holds `pct` of `owned`.
#[derive(Debug, Clone, PartialEq)]
pub struct OwnershipEdge {
    pub owner: String,
    pub owned: String,
    pub pct: f64,
}

/// Beneficial-ownership closure over effective ownership percentages.
#[derive(Debug, Default, Clone)]
pub struct BeneficialOwnership {
    edges: Vec<OwnershipEdge>,
}

impl BeneficialOwnership {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_edge(
        &mut self,
        owner: impl Into<String>,
        owned: impl Into<String>,
        pct: f64,
    ) -> Result<(), &'static str> {
        if !(0.0..=1.0).contains(&pct) || !pct.is_finite() {
            return Err("ownership percentage must be in [0,1]");
        }
        self.edges.push(OwnershipEdge {
            owner: owner.into(),
            owned: owned.into(),
            pct,
        });
        Ok(())
    }

    pub fn edges(&self) -> &[OwnershipEdge] {
        &self.edges
    }

    /// Effective ownership of every (direct or indirect) owner of `entity`.
    ///
    /// The percentage of each path is the product of its edge percentages and
    /// paths are summed. Cycle-safe: a node is not revisited on a path.
    pub fn effective_ownership(&self, entity: &str) -> BTreeMap<String, f64> {
        let mut visited = BTreeSet::new();
        visited.insert(entity.to_string());
        self.walk(entity, &visited)
    }

    fn walk(&self, entity: &str, visited: &BTreeSet<String>) -> BTreeMap<String, f64> {
        let mut out: BTreeMap<String, f64> = BTreeMap::new();
        for e in self.edges.iter().filter(|e| e.owned == entity) {
            if visited.contains(&e.owner) {
                continue;
            }
            *out.entry(e.owner.clone()).or_insert(0.0) += e.pct;
            let mut next_visited = visited.clone();
            next_visited.insert(e.owner.clone());
            for (owner, pct) in self.walk(&e.owner, &next_visited) {
                *out.entry(owner).or_insert(0.0) += e.pct * pct;
            }
        }
        out
    }

    /// Ultimate owners (owners with no owner of their own) above `threshold`.
    pub fn beneficial_owners(&self, entity: &str, threshold: f64) -> BTreeMap<String, f64> {
        self.effective_ownership(entity)
            .into_iter()
            .filter(|(owner, pct)| {
                *pct > threshold
                    && self.edges.iter().all(|e| &e.owned != owner)
            })
            .collect()
    }

    /// True when the ownership graph has a cycle reachable from `entity`.
    pub fn has_cycle(&self, entity: &str) -> bool {
        fn dfs(
            edges: &[OwnershipEdge],
            node: &str,
            stack: &mut BTreeSet<String>,
            done: &mut BTreeSet<String>,
        ) -> bool {
            if stack.contains(node) {
                return true;
            }
            if done.contains(node) {
                return false;
            }
            stack.insert(node.to_string());
            for e in edges.iter().filter(|e| e.owned == node) {
                if dfs(edges, &e.owner, stack, done) {
                    return true;
                }
            }
            stack.remove(node);
            done.insert(node.to_string());
            false
        }
        let mut stack = BTreeSet::new();
        let mut done = BTreeSet::new();
        dfs(&self.edges, entity, &mut stack, &mut done)
    }
}

/// Cosine similarity between two equal-length vectors (`0.0` on mismatch or a
/// zero-norm input).
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f64 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let mut dot = 0.0;
    let mut na = 0.0;
    let mut nb = 0.0;
    for (x, y) in a.iter().zip(b.iter()) {
        dot += *x as f64 * *y as f64;
        na += (*x as f64).powi(2);
        nb += (*y as f64).powi(2);
    }
    if na == 0.0 || nb == 0.0 {
        return 0.0;
    }
    dot / (na.sqrt() * nb.sqrt())
}

/// Weights for the graph / vector hybrid score.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HybridWeights {
    pub graph: f64,
    pub vector: f64,
}

impl Default for HybridWeights {
    fn default() -> Self {
        Self {
            graph: 0.5,
            vector: 0.5,
        }
    }
}

/// Fuse a normalised graph risk score with an embedding similarity into
/// `[0, 1]`.
pub fn hybrid_score(graph_score: f64, vector_similarity: f64, weights: HybridWeights) -> f64 {
    let total = weights.graph + weights.vector;
    if total <= 0.0 {
        return 0.0;
    }
    let fused = (graph_score.clamp(0.0, 1.0) * weights.graph
        + vector_similarity.clamp(0.0, 1.0) * weights.vector)
        / total;
    fused.clamp(0.0, 1.0)
}

/// Investigator verdict on a case.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaseVerdict {
    TruePositive,
    FalsePositive,
    Inconclusive,
}

impl CaseVerdict {
    pub fn as_str(self) -> &'static str {
        match self {
            CaseVerdict::TruePositive => "true_positive",
            CaseVerdict::FalsePositive => "false_positive",
            CaseVerdict::Inconclusive => "inconclusive",
        }
    }
}

/// One investigator feedback record.
#[derive(Debug, Clone, PartialEq)]
pub struct FeedbackEntry {
    pub case_id: String,
    pub pattern: String,
    pub verdict: CaseVerdict,
    pub reason: String,
    pub investigator: String,
    pub at: i64,
}

/// Feedback ledger + false-positive-rate-driven threshold suggestion.
#[derive(Debug, Default, Clone)]
pub struct FeedbackLedger {
    entries: Vec<FeedbackEntry>,
}

impl FeedbackLedger {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&mut self, entry: FeedbackEntry) {
        self.entries.push(entry);
    }

    pub fn entries(&self) -> &[FeedbackEntry] {
        &self.entries
    }

    /// False-positive rate for `pattern` over conclusive verdicts, or `None`
    /// when there are none.
    pub fn false_positive_rate(&self, pattern: &str) -> Option<f64> {
        let conclusive: Vec<&FeedbackEntry> = self
            .entries
            .iter()
            .filter(|e| e.pattern == pattern && e.verdict != CaseVerdict::Inconclusive)
            .collect();
        if conclusive.is_empty() {
            return None;
        }
        let fp = conclusive
            .iter()
            .filter(|e| e.verdict == CaseVerdict::FalsePositive)
            .count();
        Some(fp as f64 / conclusive.len() as f64)
    }

    /// Suggest a threshold: raise `current` by `step` until the pattern's
    /// false-positive rate is at or below `target` (capped at 1.0).
    pub fn suggest_threshold(
        &self,
        pattern: &str,
        current: f64,
        target: f64,
        step: f64,
    ) -> f64 {
        let Some(fpr) = self.false_positive_rate(pattern) else {
            return current;
        };
        if step <= 0.0 || fpr <= target {
            return current;
        }
        let mut threshold = current;
        let mut rate = fpr;
        while rate > target && threshold < 1.0 {
            threshold = (threshold + step).min(1.0);
            // A coarse model: each step removes a proportional share of the
            // false positives; documented as a suggestion, not a decision.
            rate = (rate - step).max(0.0);
        }
        threshold
    }
}

/// A deterministic, replayable case artifact.
#[derive(Debug, Clone, PartialEq)]
pub struct CaseSnapshot {
    pub case_id: String,
    pub created_at: i64,
    pub execution_id: String,
    pub pattern: String,
    pub seed: String,
    pub explanation: ExplanationSubgraph,
    pub aggregates: AlertAggregates,
    pub hybrid_score: f64,
    pub feedback: Option<CaseVerdict>,
}

impl CaseSnapshot {
    /// Build a snapshot from the alert inputs.
    #[allow(clippy::too_many_arguments)]
    pub fn build(
        case_id: impl Into<String>,
        created_at: i64,
        execution_id: impl Into<String>,
        pattern: impl Into<String>,
        transactions: &[Transaction],
        seed: &str,
        max_hops: u32,
        direction: Direction,
        graph_score: f64,
        query_embedding: &[f32],
        pattern_embedding: &[f32],
        weights: HybridWeights,
        feedback: Option<CaseVerdict>,
    ) -> Self {
        let explanation = explain_subgraph(transactions, seed, max_hops, direction);
        let aggregates = explanation.aggregates();
        let similarity = cosine_similarity(query_embedding, pattern_embedding);
        Self {
            case_id: case_id.into(),
            created_at,
            execution_id: execution_id.into(),
            pattern: pattern.into(),
            seed: seed.to_string(),
            explanation,
            aggregates,
            hybrid_score: hybrid_score(graph_score, similarity, weights),
            feedback,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tx(from: &str, to: &str, amount: f64, currency: &str, jurisdiction: &str, channel: &str) -> Transaction {
        Transaction::new(from, to, amount, currency)
            .with_jurisdiction(jurisdiction)
            .with_channel(channel)
    }

    fn chain() -> Vec<Transaction> {
        vec![
            tx("A", "B", 100.0, "USD", "US", "wire"),
            tx("B", "C", 60.0, "USD", "HK", "wire"),
            tx("C", "D", 30.0, "EUR", "HK", "cash"),
            tx("X", "A", 10.0, "USD", "US", "ach"),
        ]
    }

    #[test]
    fn aggregates_group_by_currency_jurisdiction_channel() {
        let agg = AlertAggregates::from_transactions(&chain());
        assert_eq!(agg.count, 4);
        assert_eq!(agg.total_amount, 200.0);
        assert_eq!(agg.by_currency["USD"], 170.0);
        assert_eq!(agg.by_currency["EUR"], 30.0);
        assert_eq!(agg.by_jurisdiction["HK"], 90.0);
        assert_eq!(agg.by_channel["wire"], 160.0);
    }

    #[test]
    fn directed_explanation_follows_flow_and_respects_hops() {
        let sub = explain_subgraph(&chain(), "A", 2, Direction::Directed);
        // A->B->C, and X->A is backward so excluded when directed
        assert_eq!(sub.nodes, vec!["A", "B", "C"]);
        assert!(sub.edges.iter().any(|e| e.src == "A" && e.dst == "B"));
        assert!(sub.edges.iter().any(|e| e.src == "B" && e.dst == "C"));
        assert!(!sub.edges.iter().any(|e| e.dst == "A"));

        let one_hop = explain_subgraph(&chain(), "A", 1, Direction::Directed);
        assert_eq!(one_hop.nodes, vec!["A", "B"]);
    }

    #[test]
    fn undirected_explanation_includes_incoming_flow() {
        let sub = explain_subgraph(&chain(), "A", 1, Direction::Undirected);
        assert_eq!(sub.nodes, vec!["A", "B", "X"]);
        assert!(sub.edges.iter().any(|e| e.src == "X" && e.dst == "A"));
    }

    #[test]
    fn explanation_is_deterministic() {
        let a = explain_subgraph(&chain(), "A", 3, Direction::Undirected);
        let b = explain_subgraph(&chain(), "A", 3, Direction::Undirected);
        assert_eq!(a, b);
        assert_eq!(a.aggregates(), b.aggregates());
    }

    #[test]
    fn beneficial_ownership_closure_sums_paths_and_thresholds_ultimate() {
        // B holds 50% of A, A holds 100% of E; C holds 100% of B.
        let mut own = BeneficialOwnership::new();
        own.add_edge("A", "E", 1.0).unwrap();
        own.add_edge("B", "A", 0.5).unwrap();
        own.add_edge("C", "B", 1.0).unwrap();

        let effective = own.effective_ownership("E");
        assert!((effective["A"] - 1.0).abs() < 1e-12);
        assert!((effective["B"] - 0.5).abs() < 1e-12);
        assert!((effective["C"] - 0.5).abs() < 1e-12);

        // A and B are themselves owned, so only C is an ultimate owner.
        let ultimate = own.beneficial_owners("E", 0.4);
        assert_eq!(ultimate.len(), 1);
        assert!((ultimate["C"] - 0.5).abs() < 1e-12);
        assert!(own.beneficial_owners("E", 0.6).is_empty());
    }

    #[test]
    fn ownership_handles_multiple_paths_and_cycles() {
        let mut own = BeneficialOwnership::new();
        // Two paths to E: direct 0.2 and via A 0.8*0.5 = 0.4 -> 0.6
        own.add_edge("X", "E", 0.2).unwrap();
        own.add_edge("A", "E", 0.5).unwrap();
        own.add_edge("X", "A", 0.8).unwrap();
        let eff = own.effective_ownership("E");
        assert!((eff["X"] - 0.6).abs() < 1e-12);

        // cycle A -> B -> A does not loop forever
        let mut cyc = BeneficialOwnership::new();
        cyc.add_edge("A", "B", 0.5).unwrap();
        cyc.add_edge("B", "A", 0.5).unwrap();
        cyc.add_edge("A", "E", 1.0).unwrap();
        assert!(cyc.has_cycle("E"));
        assert!(cyc.effective_ownership("E").contains_key("A"));
    }

    #[test]
    fn hybrid_score_fuses_graph_and_vector() {
        let sim = cosine_similarity(&[1.0, 0.0], &[0.0, 1.0]);
        assert!(sim.abs() < 1e-12);
        let same = cosine_similarity(&[1.0, 1.0], &[1.0, 1.0]);
        assert!((same - 1.0).abs() < 1e-12);
        assert_eq!(cosine_similarity(&[1.0], &[1.0, 2.0]), 0.0);

        let s = hybrid_score(1.0, 0.0, HybridWeights { graph: 0.75, vector: 0.25 });
        assert!((s - 0.75).abs() < 1e-12);
        let s = hybrid_score(2.0, 2.0, HybridWeights::default());
        assert!((s - 1.0).abs() < 1e-12);
    }

    #[test]
    fn feedback_ledger_computes_fpr_and_suggests_threshold() {
        let mut ledger = FeedbackLedger::new();
        for (i, verdict) in [
            CaseVerdict::FalsePositive,
            CaseVerdict::FalsePositive,
            CaseVerdict::TruePositive,
            CaseVerdict::TruePositive,
            CaseVerdict::Inconclusive,
        ]
        .into_iter()
        .enumerate()
        {
            ledger.record(FeedbackEntry {
                case_id: format!("case{i}"),
                pattern: "ring".into(),
                verdict,
                reason: String::new(),
                investigator: "inv".into(),
                at: i as i64,
            });
        }
        assert_eq!(ledger.false_positive_rate("ring"), Some(0.5));
        assert_eq!(ledger.false_positive_rate("path"), None);
        let suggested = ledger.suggest_threshold("ring", 0.6, 0.3, 0.1);
        assert!(suggested >= 0.6);
        // no data -> unchanged
        assert_eq!(ledger.suggest_threshold("path", 0.6, 0.3, 0.1), 0.6);
    }

    #[test]
    fn case_snapshot_is_replayable_and_explains_itself() {
        let build = || {
            CaseSnapshot::build(
                "case-1",
                100,
                "exec-1",
                "ring",
                &chain(),
                "A",
                2,
                Direction::Directed,
                0.9,
                &[1.0, 0.0, 0.0],
                &[1.0, 0.0, 0.0],
                HybridWeights::default(),
                Some(CaseVerdict::TruePositive),
            )
        };
        let a = build();
        let b = build();
        assert_eq!(a, b);
        assert_eq!(a.aggregates.count, 2);
        assert!((a.hybrid_score - 0.95).abs() < 1e-12);
        assert_eq!(a.feedback, Some(CaseVerdict::TruePositive));
    }
}
