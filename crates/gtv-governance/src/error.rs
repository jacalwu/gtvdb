//! Governance errors.

use thiserror::Error;

/// Errors from rule registration and governed allocation.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum GovernanceError {
    #[error("rule set `{id}` version {version} already registered")]
    DuplicateRuleSet { id: String, version: u32 },
    #[error("rule set `{id}` version {version} not found")]
    RuleSetNotFound { id: String, version: u32 },
    #[error("no rule set registered for `{id}`")]
    NoRuleSet { id: String },
    #[error("no rule set for `{id}` is effective at {as_of}")]
    NoEffectiveRuleSet { id: String, as_of: i64 },
    #[error("invalid rule set `{id}` v{version}: {reason}")]
    InvalidRule {
        id: String,
        version: u32,
        reason: String,
    },
    #[error("duplicate exposure id {0}")]
    DuplicateExposure(u64),
    #[error("collateral pledge ({col_id} -> {loan_id}) references an unknown node")]
    DanglingCollateralPledge { col_id: u64, loan_id: u64 },
    #[error("guarantee pledge ({guarantor_id} -> {loan_id}) references an unknown node")]
    DanglingGuaranteePledge { guarantor_id: u64, loan_id: u64 },
    #[error("crm allocation failed: {0}")]
    Crm(String),
}
