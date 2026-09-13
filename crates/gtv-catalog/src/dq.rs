//! Data-quality rules, publish-gate decisions and override audit (B2-5).
//!
//! A [`DqRule`] is a declarative check over a dataset. The engine evaluates the
//! rules (`gtv_engine::dq`) and produces a [`GateDecision`]; a failing decision
//! blocks publication. Every decision and every override is appended to an
//! append-only ledger in the catalog (JSONL) so it can be audited alongside the
//! B2-3 lineage record it is attached to.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::id::SnapshotId;
use crate::lineage::ExecutionId;

/// A single data-quality check.
///
/// Serialized with an internal `rule` tag, e.g.
/// `{"rule":"completeness","column":"price","min_ratio":0.99}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "rule", rename_all = "snake_case")]
pub enum DqRule {
    /// Non-null ratio of `column` must be `>= min_ratio`.
    Completeness { column: String, min_ratio: f64 },
    /// The tuple of `columns` must be unique (no duplicate rows).
    Uniqueness { columns: Vec<String> },
    /// `max(event_time_col)` must be no older than `max_lag_ns` from now.
    Freshness { event_time_col: String, max_lag_ns: i64 },
    /// Every non-null value of `column` must lie within `[min, max]`.
    Range {
        column: String,
        #[serde(default)]
        min: Option<f64>,
        #[serde(default)]
        max: Option<f64>,
    },
    /// Every non-null `child_col` value must exist in
    /// `parent` (a table) `.parent_col`.
    Referential {
        child_col: String,
        parent: String,
        parent_col: String,
    },
    /// Source/target row counts and amount sums must agree within `tolerance`
    /// (a relative fraction; `0.0` = exact). Counts must always match exactly.
    Reconciliation {
        name: String,
        source_rows: i64,
        target_rows: i64,
        #[serde(default)]
        tolerance: f64,
        #[serde(default)]
        source_sum: Option<f64>,
        #[serde(default)]
        target_sum: Option<f64>,
    },
}

impl DqRule {
    /// The stable rule kind (`completeness`, `uniqueness`, …).
    pub fn kind(&self) -> &'static str {
        match self {
            DqRule::Completeness { .. } => "completeness",
            DqRule::Uniqueness { .. } => "uniqueness",
            DqRule::Freshness { .. } => "freshness",
            DqRule::Range { .. } => "range",
            DqRule::Referential { .. } => "referential",
            DqRule::Reconciliation { .. } => "reconciliation",
        }
    }

    /// The column(s) the rule inspects, when applicable.
    pub fn columns(&self) -> Vec<String> {
        match self {
            DqRule::Completeness { column, .. } => vec![column.clone()],
            DqRule::Uniqueness { columns } => columns.clone(),
            DqRule::Freshness { event_time_col, .. } => vec![event_time_col.clone()],
            DqRule::Range { column, .. } => vec![column.clone()],
            DqRule::Referential { child_col, .. } => vec![child_col.clone()],
            DqRule::Reconciliation { .. } => Vec::new(),
        }
    }
}

/// One concrete rule violation: what was checked, what was seen and the limit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DqFailure {
    /// Rule kind, e.g. `completeness`.
    pub rule: String,
    /// Column (or joined columns) the failure is about, when applicable.
    #[serde(default)]
    pub column: Option<String>,
    /// The observed value.
    pub observed: f64,
    /// The threshold it was compared against.
    pub threshold: f64,
    /// Human-readable explanation.
    pub message: String,
}

/// The outcome of evaluating every rule: `pass` is true iff `failures` is empty.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GateDecision {
    pub pass: bool,
    pub failures: Vec<DqFailure>,
}

impl GateDecision {
    /// Build a decision from the list of failures.
    pub fn from_failures(failures: Vec<DqFailure>) -> Self {
        Self {
            pass: failures.is_empty(),
            failures,
        }
    }
}

/// A persisted publish-gate decision, linked to the execution that produced
/// the data and (when known) the snapshot it was read from.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GateDecisionRecord {
    #[serde(default)]
    pub execution_id: Option<ExecutionId>,
    /// Table (or output) the gate was applied to.
    pub target: String,
    #[serde(default)]
    pub snapshot_id: Option<SnapshotId>,
    pub pass: bool,
    #[serde(default)]
    pub failures: Vec<DqFailure>,
    /// True when publication proceeded through an override.
    #[serde(default)]
    pub overridden: bool,
    #[serde(default)]
    pub override_reason: Option<String>,
    #[serde(default)]
    pub override_approver: Option<String>,
    pub decided_at: i64,
}

/// A recorded override: who approved skipping a failing rule, why and when.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OverrideRecord {
    /// Table (or output) the override applies to.
    pub target: String,
    /// The failing rule being waived.
    pub rule: String,
    pub reason: String,
    pub approver: String,
    pub approved_at: i64,
    #[serde(default)]
    pub execution_id: Option<ExecutionId>,
}

impl OverrideRecord {
    /// Create an override stamped with the current time.
    pub fn new(
        target: &str,
        rule: &str,
        reason: &str,
        approver: &str,
        execution_id: Option<ExecutionId>,
    ) -> Self {
        Self {
            target: target.to_string(),
            rule: rule.to_string(),
            reason: reason.to_string(),
            approver: approver.to_string(),
            approved_at: crate::schema::now_ns(),
            execution_id,
        }
    }
}

/// Parse a JSON array of rules (`[{"rule":"completeness",…}, …]`).
pub fn parse_rules(json: &str) -> crate::Result<Vec<DqRule>> {
    Ok(serde_json::from_str(json)?)
}

/// The set of rule kinds overridden for `target` (all overrides, newest wins).
pub fn overridden_rules(overrides: &[OverrideRecord], target: &str) -> HashSet<String> {
    overrides
        .iter()
        .filter(|o| o.target == target)
        .map(|o| o.rule.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_tagged_rules() {
        let rules = parse_rules(
            r#"[{"rule":"completeness","column":"p","min_ratio":0.9},
                {"rule":"range","column":"q","min":0.0,"max":1.0},
                {"rule":"reconciliation","name":"r","source_rows":10,"target_rows":10}]"#,
        )
        .unwrap();
        assert_eq!(rules.len(), 3);
        assert_eq!(rules[0].kind(), "completeness");
        assert_eq!(rules[1].columns(), vec!["q".to_string()]);
        assert_eq!(rules[2].kind(), "reconciliation");
    }

    #[test]
    fn decision_from_failures() {
        let pass = GateDecision::from_failures(vec![]);
        assert!(pass.pass);
        let fail = GateDecision::from_failures(vec![DqFailure {
            rule: "freshness".into(),
            column: Some("ts".into()),
            observed: 10.0,
            threshold: 5.0,
            message: "stale".into(),
        }]);
        assert!(!fail.pass);
    }

    #[test]
    fn override_lookup_is_target_scoped() {
        let recs = vec![
            OverrideRecord::new("a", "freshness", "backfill", "alice", None),
            OverrideRecord::new("b", "freshness", "backfill", "bob", None),
        ];
        let got = overridden_rules(&recs, "a");
        assert!(got.contains("freshness"));
        assert_eq!(got.len(), 1);
    }
}
