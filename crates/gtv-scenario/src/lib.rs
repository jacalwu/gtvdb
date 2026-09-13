//! gtv-scenario: versioned Risk / ALM / FTP scenario catalog (prod_p4 D1/D4/D5).
//!
//! This is an **enterprise-batch** crate. It is a pure domain layer with a
//! one-way dependency boundary: the analysis engine (`gtv-engine`,
//! `gtv-index`, `gtv-pattern`, …) may attach to it through UDF /
//! table-function registration, but must never depend on it
//! (see `doc/prod_p4_design.md` §1–§3).
//!
//! The first increment (D1) provides the scenario catalog, versioning,
//! inheritance / override resolution with provenance, and a deterministic
//! diff for reconciliation.

pub mod catalog;
pub mod scenario;

pub use catalog::ScenarioCatalog;
pub use scenario::{
    Change, Dimension, Provenance, ResolvedScenario, ResolvedShock, Scenario, ScenarioDiff,
    ScenarioError, ScenarioKind, ScenarioStatus, Shock,
};
