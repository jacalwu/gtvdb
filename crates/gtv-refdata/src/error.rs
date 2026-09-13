//! Errors shared by the D6 reference-data layer.

use thiserror::Error;

/// Errors from hierarchies, master data and reference data.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RefDataError {
    #[error("empty identifier")]
    EmptyId,
    #[error("invalid effective interval [{from}, {to})")]
    InvalidInterval { from: i64, to: i64 },
    #[error("`{id}` cannot be its own parent/child")]
    SelfReference { id: String },
    #[error("duplicate {kind} edge `{parent}` -> `{child}` with overlapping validity")]
    DuplicateEdge {
        kind: String,
        parent: String,
        child: String,
    },
    #[error("adding {kind} edge `{parent}` -> `{child}` would create a cycle")]
    CyclicHierarchy {
        kind: String,
        parent: String,
        child: String,
    },
    #[error("duplicate {kind} key `{key}` with overlapping validity")]
    DuplicateKey { kind: String, key: String },
    #[error(transparent)]
    DanglingReference(Box<DanglingReference>),
    #[error("unknown reference domain `{0}`")]
    UnknownDomain(String),
}

/// Detail for [`RefDataError::DanglingReference`], boxed to keep the error enum
/// small enough to return by value.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error(
    "{kind} `{id}` field `{field}` references missing {target} `{target_id}` at {as_of}"
)]
pub struct DanglingReference {
    pub kind: String,
    pub id: String,
    pub field: String,
    pub target: String,
    pub target_id: String,
    pub as_of: i64,
}
