//! B2-5: DQ gate evaluation over session tables and the `dq_gate` SQL surface.

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use gtv_catalog::{DqRule, OverrideRecord};
use gtv_engine::GtvContext;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("sym", DataType::Utf8, false),
        Field::new("price", DataType::Float64, true),
        Field::new("ts", DataType::Int64, false),
    ]))
}

fn batches() -> Vec<RecordBatch> {
    let b = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2, 3, 4])) as ArrayRef,
            Arc::new(StringArray::from(vec!["A", "A", "B", "C"])) as ArrayRef,
            Arc::new(Float64Array::from(vec![Some(1.0), None, Some(3.0), Some(4.0)])) as ArrayRef,
            Arc::new(Int64Array::from(vec![10i64, 11, 12, 13])) as ArrayRef,
        ],
    )
    .unwrap();
    vec![b]
}

#[test]
fn evaluate_dq_over_session_table() {
    let ctx = GtvContext::new();
    ctx.register_batches("risk", schema(), batches()).unwrap();

    let rules = vec![
        DqRule::Completeness {
            column: "price".into(),
            min_ratio: 0.99,
        },
        DqRule::Uniqueness {
            columns: vec!["sym".into()],
        },
    ];
    let (decision, outcomes) = ctx.evaluate_dq("risk", &rules).unwrap();
    assert_eq!(outcomes.len(), 2);
    assert!(!decision.pass);
    assert_eq!(decision.failures.len(), 2);
    let completeness = outcomes.iter().find(|o| o.rule == "completeness").unwrap();
    assert!((completeness.observed - 0.75).abs() < 1e-9);
    let uniqueness = outcomes.iter().find(|o| o.rule == "uniqueness").unwrap();
    assert_eq!(uniqueness.observed, 1.0);
}

#[test]
fn referential_uses_parent_session_table() {
    let ctx = GtvContext::new();
    ctx.register_batches("risk", schema(), batches()).unwrap();
    // Parent dimension only knows A and B; C is an orphan.
    let parent_schema = Arc::new(Schema::new(vec![Field::new("sym", DataType::Utf8, false)]));
    let parent = RecordBatch::try_new(
        parent_schema.clone(),
        vec![Arc::new(StringArray::from(vec!["A", "B"])) as ArrayRef],
    )
    .unwrap();
    ctx.register_batches("syms", parent_schema, vec![parent])
        .unwrap();

    let rules = vec![DqRule::Referential {
        child_col: "sym".into(),
        parent: "syms".into(),
        parent_col: "sym".into(),
    }];
    let (decision, outcomes) = ctx.evaluate_dq("risk", &rules).unwrap();
    assert!(!decision.pass);
    assert_eq!(outcomes[0].observed, 1.0, "C is the only orphan");
}

#[test]
fn reconciliation_passes_when_counts_and_sums_agree() {
    let ctx = GtvContext::new();
    ctx.register_batches("risk", schema(), batches()).unwrap();
    let rules = vec![DqRule::Reconciliation {
        name: "risk->report".into(),
        source_rows: 4,
        target_rows: 4,
        tolerance: 0.0,
        source_sum: Some(100.0),
        target_sum: Some(100.0),
    }];
    let (decision, outcomes) = ctx.evaluate_dq("risk", &rules).unwrap();
    assert!(decision.pass, "{:?}", decision.failures);
    assert!(outcomes[0].passed);
}

#[tokio::test]
async fn sql_dq_gate_reports_each_rule() {
    let ctx = GtvContext::new();
    ctx.register_batches("risk", schema(), batches()).unwrap();

    let rules = r#"[{"rule":"completeness","column":"price","min_ratio":0.99},
                    {"rule":"freshness","event_time_col":"ts","max_lag_ns":1}]"#;
    let out = ctx
        .sql(&format!("SELECT rule, passed FROM dq_gate('risk', '{rules}') ORDER BY rule"))
        .await
        .unwrap();
    assert_eq!(out.len(), 1);
    let b = &out[0];
    assert_eq!(b.num_rows(), 2);
    let passed = b
        .column_by_name("passed")
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    // Both should fail: price has a null, ts is ancient relative to max_lag_ns=1.
    assert!(!passed.value(0));
    assert!(!passed.value(1));
}

#[test]
fn override_waives_reported_failure_kind() {
    let ctx = GtvContext::new();
    ctx.register_batches("risk", schema(), batches()).unwrap();
    let rules = vec![DqRule::Uniqueness {
        columns: vec!["sym".into()],
    }];
    let (decision, _) = ctx.evaluate_dq("risk", &rules).unwrap();
    assert!(!decision.pass);

    let overrides = vec![OverrideRecord::new(
        "risk",
        "uniqueness",
        "known duplicate during migration",
        "risk-owner",
        None,
    )];
    assert!(gtv_engine::dq::blocking_failures(&decision, &overrides, "risk").is_empty());
    assert_eq!(
        gtv_engine::dq::blocking_failures(&decision, &overrides, "other").len(),
        1
    );
}
