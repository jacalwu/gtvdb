//! B2-5: append-only gate-decision and override ledger round-trips.

use std::fs;

use gtv_catalog::{
    overridden_rules, FsCatalog, GateDecisionRecord, OverrideRecord,
};

fn temp_dir(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("gtv_dq_{}_{}", tag, std::process::id()))
}

#[test]
fn gate_and_override_ledger_round_trip() {
    let dir = temp_dir("ledger");
    let _ = fs::remove_dir_all(&dir);

    {
        let cat = FsCatalog::open(&dir).unwrap();
        let rec = GateDecisionRecord {
            execution_id: None,
            target: "risk_out".into(),
            snapshot_id: None,
            pass: false,
            failures: vec![gtv_catalog::DqFailure {
                rule: "freshness".into(),
                column: Some("ts".into()),
                observed: 10.0,
                threshold: 5.0,
                message: "stale".into(),
            }],
            overridden: false,
            override_reason: None,
            override_approver: None,
            decided_at: 123,
        };
        cat.append_gate_decision(&rec).unwrap();
        cat.append_override(&OverrideRecord::new(
            "risk_out",
            "freshness",
            "backfill pending",
            "alice",
            None,
        ))
        .unwrap();
    }

    // Re-open and confirm both ledgers survived and are auditable.
    let cat = FsCatalog::open(&dir).unwrap();
    let decisions = cat.gate_decisions().unwrap();
    assert_eq!(decisions.len(), 1);
    assert!(!decisions[0].pass);
    assert_eq!(decisions[0].failures[0].rule, "freshness");
    assert_eq!(decisions[0].failures[0].column.as_deref(), Some("ts"));
    assert_eq!(decisions[0].failures[0].observed, 10.0);

    let overrides = cat.overrides().unwrap();
    assert_eq!(overrides.len(), 1);
    assert_eq!(overrides[0].approver, "alice");
    assert_eq!(overrides[0].reason, "backfill pending");
    assert!(overrides[0].approved_at > 0);
    assert!(overridden_rules(&overrides, "risk_out").contains("freshness"));
    assert!(overridden_rules(&overrides, "other").is_empty());

    let _ = fs::remove_dir_all(&dir);
}
