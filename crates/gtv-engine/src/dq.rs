//! Data-quality rule evaluation and the publish gate (B2-5).
//!
//! This is the *enforcement* half of B2-5: it turns the declarative
//! [`DqRule`]s from `gtv-catalog` into a concrete [`GateDecision`] over the
//! batches of a session table, and exposes it to SQL as
//! `dq_gate('table', '<rules_json>')`.
//!
//! The existing [`crate::monitor`] functions stay diagnostic (report/health);
//! here a failing rule produces a [`DqFailure`] with the rule, column, observed
//! value and threshold, and the caller (`GtvContext::publish_guarded` / the CLI
//! `publish` command) refuses to publish unless it passes or is overridden.

use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use arrow::array::{
    as_primitive_array, Array, ArrayRef, BooleanArray, Float64Array, StringArray,
};
use arrow::datatypes::{DataType, Field, Float32Type, Float64Type, Int32Type, Int64Type, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result};

use gtv_catalog::{DqFailure, DqRule, GateDecision};

use crate::expr_util::expr_to_string;
use crate::hft_exec::HftRegistry;

/// The result of evaluating one rule (pass or fail, plus the evidence).
#[derive(Debug, Clone, PartialEq)]
pub struct RuleOutcome {
    pub rule: String,
    /// The column(s) / target the rule inspected.
    pub target: String,
    pub passed: bool,
    pub observed: f64,
    pub threshold: f64,
    pub message: String,
}

impl RuleOutcome {
    fn fail(rule: &DqRule, target: String, observed: f64, threshold: f64, message: String) -> Self {
        Self {
            rule: rule.kind().to_string(),
            target,
            passed: false,
            observed,
            threshold,
            message,
        }
    }

    fn ok(rule: &DqRule, target: String, observed: f64, threshold: f64, message: String) -> Self {
        Self {
            rule: rule.kind().to_string(),
            target,
            passed: true,
            observed,
            threshold,
            message,
        }
    }

    /// Convert a failing outcome into the catalog's [`DqFailure`].
    pub fn failure(&self) -> Option<DqFailure> {
        (!self.passed).then(|| DqFailure {
            rule: self.rule.clone(),
            column: (!self.target.is_empty()).then(|| self.target.clone()),
            observed: self.observed,
            threshold: self.threshold,
            message: self.message.clone(),
        })
    }
}

type ParentLookup<'a> = dyn Fn(&str, &str) -> Result<HashSet<String>> + 'a;

fn total_rows(batches: &[RecordBatch]) -> u64 {
    batches.iter().map(|b| b.num_rows() as u64).sum()
}

fn column<'a>(batches: &'a [RecordBatch], name: &str) -> Option<&'a ArrayRef> {
    batches.iter().find_map(|b| b.column_by_name(name))
}

/// Non-null numeric values of `arr`, or `None` when the type is not numeric.
fn non_null_f64(arr: &dyn Array) -> Option<Vec<f64>> {
    let out: Vec<f64> = match arr.data_type() {
        DataType::Float64 => as_primitive_array::<Float64Type>(arr).iter().flatten().collect(),
        DataType::Float32 => as_primitive_array::<Float32Type>(arr)
            .iter()
            .flatten()
            .map(f64::from)
            .collect(),
        DataType::Int64 => as_primitive_array::<Int64Type>(arr)
            .iter()
            .flatten()
            .map(|v| v as f64)
            .collect(),
        DataType::Int32 => as_primitive_array::<Int32Type>(arr)
            .iter()
            .flatten()
            .map(|v| v as f64)
            .collect(),
        _ => return None,
    };
    Some(out)
}

/// Evaluate one rule against `batches`.
pub fn evaluate_rule(
    rule: &DqRule,
    batches: &[RecordBatch],
    lookup: &ParentLookup<'_>,
) -> RuleOutcome {
    match rule {
        DqRule::Completeness { column: col, min_ratio } => {
            if column(batches, col).is_none() {
                return RuleOutcome::fail(
                    rule,
                    col.clone(),
                    0.0,
                    *min_ratio,
                    format!("column `{col}` not found"),
                );
            };
            let rows = total_rows(batches);
            let nulls: u64 = batches
                .iter()
                .filter_map(|b| b.column_by_name(col))
                .map(|a| a.null_count() as u64)
                .sum();
            let ratio = if rows == 0 {
                1.0
            } else {
                (rows - nulls) as f64 / rows as f64
            };
            let msg = format!(
                "non-null ratio {ratio:.4} ({} null / {} rows)",
                nulls, rows
            );
            if ratio + f64::EPSILON >= *min_ratio {
                RuleOutcome::ok(rule, col.clone(), ratio, *min_ratio, msg)
            } else {
                RuleOutcome::fail(rule, col.clone(), ratio, *min_ratio, msg)
            }
        }
        DqRule::Uniqueness { columns } => {
            if let Some(missing) = columns.iter().find(|c| column(batches, c).is_none()) {
                return RuleOutcome::fail(
                    rule,
                    columns.join(","),
                    0.0,
                    0.0,
                    format!("column `{missing}` not found"),
                );
            }
            let mut seen: HashSet<String> = HashSet::new();
            let mut rows = 0u64;
            let mut dups = 0u64;
            for b in batches {
                for i in 0..b.num_rows() {
                    rows += 1;
                    let key: Vec<String> = columns
                        .iter()
                        .map(|c| {
                            array_value_to_string(b.column_by_name(c).unwrap().as_ref(), i)
                                .unwrap_or_default()
                        })
                        .collect();
                    if !seen.insert(key.join("\u{1f}")) {
                        dups += 1;
                    }
                }
            }
            let target = columns.join(",");
            let msg = format!("{dups} duplicate of {rows} row(s)");
            if dups == 0 {
                RuleOutcome::ok(rule, target, dups as f64, 0.0, msg)
            } else {
                RuleOutcome::fail(rule, target, dups as f64, 0.0, msg)
            }
        }
        DqRule::Freshness { event_time_col, max_lag_ns } => {
            let Some(arr) = column(batches, event_time_col) else {
                return RuleOutcome::fail(
                    rule,
                    event_time_col.clone(),
                    f64::INFINITY,
                    *max_lag_ns as f64,
                    format!("column `{event_time_col}` not found"),
                );
            };
            if !matches!(arr.data_type(), DataType::Int64) {
                return RuleOutcome::fail(
                    rule,
                    event_time_col.clone(),
                    f64::INFINITY,
                    *max_lag_ns as f64,
                    format!("column `{event_time_col}` is not Int64 nanoseconds"),
                );
            }
            let max_ts = batches
                .iter()
                .filter_map(|b| b.column_by_name(event_time_col))
                .flat_map(|a| as_primitive_array::<Int64Type>(a.as_ref()).iter().flatten())
                .max();
            let Some(max_ts) = max_ts else {
                return RuleOutcome::fail(
                    rule,
                    event_time_col.clone(),
                    f64::INFINITY,
                    *max_lag_ns as f64,
                    "no event timestamps".to_string(),
                );
            };
            let lag = (gtv_catalog::schema::now_ns() - max_ts).max(0);
            let msg = format!("lag {} ns (limit {})", lag, max_lag_ns);
            if lag <= *max_lag_ns {
                RuleOutcome::ok(rule, event_time_col.clone(), lag as f64, *max_lag_ns as f64, msg)
            } else {
                RuleOutcome::fail(rule, event_time_col.clone(), lag as f64, *max_lag_ns as f64, msg)
            }
        }
        DqRule::Range { column: col, min, max } => {
            if column(batches, col).is_none() {
                return RuleOutcome::fail(
                    rule,
                    col.clone(),
                    0.0,
                    0.0,
                    format!("column `{col}` not found"),
                );
            };
            let mut values = Vec::new();
            for b in batches {
                let Some(a) = b.column_by_name(col) else { continue };
                match non_null_f64(a.as_ref()) {
                    Some(v) => values.extend(v),
                    None => {
                        return RuleOutcome::fail(
                            rule,
                            col.clone(),
                            0.0,
                            0.0,
                            format!("column `{col}` is not numeric"),
                        )
                    }
                }
            }
            let below = min.map_or(0, |lo| values.iter().filter(|v| **v < lo).count());
            let above = max.map_or(0, |hi| values.iter().filter(|v| **v > hi).count());
            let violations = (below + above) as f64;
            let (obs_min, obs_max) = if values.is_empty() {
                (f64::NAN, f64::NAN)
            } else {
                (
                    values.iter().cloned().fold(f64::INFINITY, f64::min),
                    values.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
                )
            };
            let msg = format!(
                "{violations} outside [{}, {}] (observed [{obs_min}, {obs_max}])",
                min.map(|v| v.to_string()).unwrap_or_else(|| "-inf".into()),
                max.map(|v| v.to_string()).unwrap_or_else(|| "+inf".into()),
            );
            if violations == 0.0 {
                RuleOutcome::ok(rule, col.clone(), 0.0, 0.0, msg)
            } else {
                RuleOutcome::fail(rule, col.clone(), violations, 0.0, msg)
            }
        }
        DqRule::Referential { child_col, parent, parent_col } => {
            if column(batches, child_col).is_none() {
                return RuleOutcome::fail(
                    rule,
                    child_col.clone(),
                    0.0,
                    0.0,
                    format!("column `{child_col}` not found"),
                );
            }
            let parents = match lookup(parent, parent_col) {
                Ok(p) => p,
                Err(e) => {
                    return RuleOutcome::fail(
                        rule,
                        child_col.clone(),
                        0.0,
                        0.0,
                        format!("parent `{parent}.{parent_col}` unavailable: {e}"),
                    )
                }
            };
            let mut orphans = 0u64;
            let mut checked = 0u64;
            for b in batches {
                let a = b.column_by_name(child_col).unwrap();
                for i in 0..a.len() {
                    if a.is_null(i) {
                        continue;
                    }
                    checked += 1;
                    let v = array_value_to_string(a.as_ref(), i).unwrap_or_default();
                    if !parents.contains(&v) {
                        orphans += 1;
                    }
                }
            }
            let msg = format!(
                "{orphans} orphan of {checked} non-null `{child_col}` vs `{parent}.{parent_col}`"
            );
            if orphans == 0 {
                RuleOutcome::ok(rule, child_col.clone(), 0.0, 0.0, msg)
            } else {
                RuleOutcome::fail(rule, child_col.clone(), orphans as f64, 0.0, msg)
            }
        }
        DqRule::Reconciliation { name, source_rows, target_rows, tolerance, source_sum, target_sum } => {
            let row_diff = (*source_rows - *target_rows).unsigned_abs();
            let (observed, threshold, mut msg) = if row_diff > 0 {
                (
                    row_diff as f64,
                    0.0,
                    format!("row count {source_rows} vs {target_rows} (diff {row_diff})"),
                )
            } else if let (Some(s), Some(t)) = (source_sum, target_sum) {
                let rel = (s - t).abs() / s.abs().max(1e-9);
                (
                    rel,
                    *tolerance,
                    format!("sum {s} vs {t} (rel diff {rel:.6}, tol {tolerance})"),
                )
            } else {
                (0.0, *tolerance, format!("row count {source_rows} == {target_rows}"))
            };
            if row_diff == 0 && (source_sum.is_none() || target_sum.is_none()) {
                // no sum supplied: counts alone are the check
                msg.push_str(" (sums not supplied)");
            }
            if observed <= threshold + f64::EPSILON {
                RuleOutcome::ok(rule, name.clone(), observed, threshold, msg)
            } else {
                RuleOutcome::fail(rule, name.clone(), observed, threshold, msg)
            }
        }
    }
}

/// Evaluate every rule, preserving order.
pub fn evaluate_rules(
    batches: &[RecordBatch],
    rules: &[DqRule],
    lookup: &ParentLookup<'_>,
) -> Vec<RuleOutcome> {
    rules.iter().map(|r| evaluate_rule(r, batches, lookup)).collect()
}

/// Collapse outcomes into a [`GateDecision`] (fails when any rule fails).
pub fn decision_from_outcomes(outcomes: &[RuleOutcome]) -> GateDecision {
    GateDecision::from_failures(outcomes.iter().filter_map(RuleOutcome::failure).collect())
}

/// The failures that still block publication once `overrides` for `target`
/// have been applied. An override waives every failure whose rule kind matches.
pub fn blocking_failures(
    decision: &GateDecision,
    overrides: &[gtv_catalog::OverrideRecord],
    target: &str,
) -> Vec<DqFailure> {
    let waived = gtv_catalog::overridden_rules(overrides, target);
    decision
        .failures
        .iter()
        .filter(|f| !waived.contains(&f.rule))
        .cloned()
        .collect()
}

/// Evaluate rules and return both the decision and the per-rule evidence.
pub fn evaluate(
    batches: &[RecordBatch],
    rules: &[DqRule],
    lookup: &ParentLookup<'_>,
) -> (GateDecision, Vec<RuleOutcome>) {
    let outcomes = evaluate_rules(batches, rules, lookup);
    let decision = decision_from_outcomes(&outcomes);
    (decision, outcomes)
}

// ---------------------------------------------------------------------------
// SQL surface: dq_gate('table', '<rules_json>')
// ---------------------------------------------------------------------------

fn fetch_table(registry: &Arc<RwLock<HftRegistry>>, name: &str) -> Result<Vec<RecordBatch>> {
    let reg = registry
        .read()
        .map_err(|_| DataFusionError::Execution("hft registry poisoned".into()))?;
    reg.tables
        .get(name)
        .map(|b| b.as_ref().clone())
        .ok_or_else(|| DataFusionError::Execution(format!("unknown table `{name}`")))
}

pub(crate) fn parent_lookup(
    registry: &Arc<RwLock<HftRegistry>>,
    parent: &str,
    col: &str,
) -> Result<HashSet<String>> {
    let batches = fetch_table(registry, parent)?;
    let mut set = HashSet::new();
    for b in &batches {
        let arr = b.column_by_name(col).ok_or_else(|| {
            DataFusionError::Execution(format!("parent `{parent}` has no column `{col}`"))
        })?;
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                set.insert(array_value_to_string(arr.as_ref(), i).unwrap_or_default());
            }
        }
    }
    Ok(set)
}

/// `dq_gate(table, rules_json)` — one row per rule with its evidence.
#[derive(Debug)]
pub struct DqGateTableFunction {
    registry: Arc<RwLock<HftRegistry>>,
}

impl DqGateTableFunction {
    pub fn new(registry: Arc<RwLock<HftRegistry>>) -> Self {
        Self { registry }
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("rule", DataType::Utf8, false),
            Field::new("target", DataType::Utf8, false),
            Field::new("passed", DataType::Boolean, false),
            Field::new("observed", DataType::Float64, false),
            Field::new("threshold", DataType::Float64, false),
            Field::new("message", DataType::Utf8, false),
        ]))
    }
}

impl TableFunctionImpl for DqGateTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let exprs = args.exprs();
        let name = expr_to_string(exprs.first().ok_or_else(|| {
            DataFusionError::Execution("dq_gate(table, rules_json): missing table".into())
        })?)?;
        let rules_json = expr_to_string(exprs.get(1).ok_or_else(|| {
            DataFusionError::Execution("dq_gate(table, rules_json): missing rules".into())
        })?)?;
        let rules = gtv_catalog::parse_rules(&rules_json)
            .map_err(|e| DataFusionError::Execution(format!("dq_gate: bad rules: {e}")))?;
        let batches = fetch_table(&self.registry, &name)?;
        let lookup = |parent: &str, col: &str| parent_lookup(&self.registry, parent, col);
        let outcomes = evaluate_rules(&batches, &rules, &lookup);

        let schema = Self::schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(
                    outcomes.iter().map(|o| o.rule.clone()).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(StringArray::from(
                    outcomes.iter().map(|o| o.target.clone()).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(BooleanArray::from(
                    outcomes.iter().map(|o| o.passed).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Float64Array::from(
                    outcomes.iter().map(|o| o.observed).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Float64Array::from(
                    outcomes.iter().map(|o| o.threshold).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(StringArray::from(
                    outcomes.iter().map(|o| o.message.clone()).collect::<Vec<_>>(),
                )) as ArrayRef,
            ],
        )?;
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use std::sync::Arc;

    fn batch() -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("sym", DataType::Utf8, false),
            Field::new("price", DataType::Float64, true),
            Field::new("ts", DataType::Int64, false),
        ]));
        let b = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2, 3, 4])) as ArrayRef,
                Arc::new(StringArray::from(vec!["A", "A", "B", "C"])) as ArrayRef,
                Arc::new(Float64Array::from(vec![
                    Some(1.0),
                    None,
                    Some(3.0),
                    Some(4.0),
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![10i64, 11, 12, 13])) as ArrayRef,
            ],
        )
        .unwrap();
        vec![b]
    }

    fn no_parent(_: &str, _: &str) -> Result<HashSet<String>> {
        Ok(HashSet::new())
    }

    #[test]
    fn completeness_reports_ratio() {
        let rules = vec![DqRule::Completeness {
            column: "price".into(),
            min_ratio: 0.99,
        }];
        let outcome = evaluate_rule(&rules[0], &batch(), &no_parent);
        assert!(!outcome.passed);
        assert!((outcome.observed - 0.75).abs() < 1e-9);
        assert_eq!(outcome.threshold, 0.99);
    }

    #[test]
    fn uniqueness_counts_duplicates() {
        let rules = vec![DqRule::Uniqueness {
            columns: vec!["sym".into()],
        }];
        let outcome = evaluate_rule(&rules[0], &batch(), &no_parent);
        assert!(!outcome.passed);
        assert_eq!(outcome.observed, 1.0); // A appears twice
    }

    #[test]
    fn range_counts_violations() {
        let rules = vec![DqRule::Range {
            column: "price".into(),
            min: Some(2.0),
            max: Some(3.5),
        }];
        let outcome = evaluate_rule(&rules[0], &batch(), &no_parent);
        assert!(!outcome.passed);
        assert_eq!(outcome.observed, 2.0); // 1.0 below, 4.0 above
    }

    #[test]
    fn freshness_detects_stale() {
        let rules = vec![DqRule::Freshness {
            event_time_col: "ts".into(),
            max_lag_ns: 1,
        }];
        let outcome = evaluate_rule(&rules[0], &batch(), &no_parent);
        assert!(!outcome.passed, "ts is ancient vs now");
    }

    #[test]
    fn reconciliation_flags_row_and_sum_mismatch() {
        let count_rule = DqRule::Reconciliation {
            name: "loan".into(),
            source_rows: 10,
            target_rows: 8,
            tolerance: 0.01,
            source_sum: None,
            target_sum: None,
        };
        let o = evaluate_rule(&count_rule, &batch(), &no_parent);
        assert!(!o.passed);
        assert_eq!(o.observed, 2.0);

        let sum_rule = DqRule::Reconciliation {
            name: "loan".into(),
            source_rows: 10,
            target_rows: 10,
            tolerance: 0.01,
            source_sum: Some(100.0),
            target_sum: Some(100.5),
        };
        let o = evaluate_rule(&sum_rule, &batch(), &no_parent);
        assert!(o.passed, "0.5% relative diff is within 1% tolerance");

        let bad_sum = DqRule::Reconciliation {
            name: "loan".into(),
            source_rows: 10,
            target_rows: 10,
            tolerance: 0.01,
            source_sum: Some(100.0),
            target_sum: Some(105.0),
        };
        let o = evaluate_rule(&bad_sum, &batch(), &no_parent);
        assert!(!o.passed, "5% relative diff exceeds 1% tolerance");
        assert!((o.observed - 0.05).abs() < 1e-9);
    }

    #[test]
    fn referential_detects_orphans() {
        let rules = vec![DqRule::Referential {
            child_col: "sym".into(),
            parent: "p".into(),
            parent_col: "sym".into(),
        }];
        let lookup = |_: &str, _: &str| {
            Ok(HashSet::from(["A".to_string(), "B".to_string()]))
        };
        let outcome = evaluate_rule(&rules[0], &batch(), &lookup);
        assert!(!outcome.passed);
        assert_eq!(outcome.observed, 1.0); // C missing
    }

    #[test]
    fn decision_aggregates_failures() {
        let rules = vec![
            DqRule::Uniqueness {
                columns: vec!["sym".into()],
            },
            DqRule::Range {
                column: "price".into(),
                min: Some(0.0),
                max: Some(10.0),
            },
        ];
        let (decision, outcomes) = evaluate(&batch(), &rules, &no_parent);
        assert!(!decision.pass);
        assert_eq!(decision.failures.len(), 1);
        assert_eq!(outcomes.len(), 2);
        assert_eq!(decision.failures[0].rule, "uniqueness");
    }

    #[test]
    fn override_waives_matching_rule() {
        let rules = vec![DqRule::Uniqueness {
            columns: vec!["sym".into()],
        }];
        let (decision, _) = evaluate(&batch(), &rules, &no_parent);
        assert!(!decision.pass);

        // No override: still blocking.
        assert_eq!(blocking_failures(&decision, &[], "t").len(), 1);

        // Override for a different target does not help.
        let other = vec![gtv_catalog::OverrideRecord::new(
            "other",
            "uniqueness",
            "ok",
            "alice",
            None,
        )];
        assert_eq!(blocking_failures(&decision, &other, "t").len(), 1);

        // Correct override clears the block.
        let ok = vec![gtv_catalog::OverrideRecord::new(
            "t",
            "uniqueness",
            "accepted by risk",
            "alice",
            None,
        )];
        assert!(blocking_failures(&decision, &ok, "t").is_empty());
    }
}
