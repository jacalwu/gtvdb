//! gtv-refdata: hierarchy, master data and reference data with effective
//! dating (prod_p4 D6 / roadmap P2.6).
//!
//! Enterprise-batch crate with a one-way dependency boundary: the analysis
//! engine may consume it through UDF / table-function registration, but must
//! never depend on it (see `doc/prod_p4_design.md` §1–§3).
//!
//! * [`Hierarchy`] — legal-entity / organisation / product hierarchies with
//!   effective-dated edges, cycle rejection, ancestor / descendant queries and
//!   deterministic roll-up.
//! * [`MasterData`] — account / customer / instrument / counterparty master
//!   records with time-unique keys and referential checks.
//! * [`ReferenceData`] — effective-dated curve / calendar / currency /
//!   jurisdiction reference values.

pub mod error;
pub mod hierarchy;
pub mod reference;

pub use error::{DanglingReference, RefDataError};
pub use hierarchy::{EffectiveRange, Hierarchy, HierarchyEdge, HierarchyKind};
pub use reference::{
    MasterData, MasterKind, MasterRecord, ReferenceConstraint, ReferenceData, ReferenceEntry,
    ReferenceViolation,
};
