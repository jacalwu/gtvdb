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

pub mod alm;
pub mod catalog;
pub mod ftp;
pub mod irrbb;
pub mod scenario;

pub use alm::{
    AlmCell, AlmCube, AlmError, AlmFilter, CashflowType, DepositDecay, DiscountCurve, FxTable,
    LiquidityStress, PrepaymentModel,
};
pub use catalog::ScenarioCatalog;
pub use ftp::{
    reconcile, BasisSpread, BehaviouralAdjustment, FtpBreakdown, FtpCurve, FtpCurveCatalog,
    FtpEngine, FtpError, FtpPolicy, FtpPolicyCatalog, FtpReconciliation, FtpRequest, FtpStep,
    LiquidityPremium, OptionalityCharge,
};
pub use irrbb::{
    aggregate_eve, cpr, cube_bands, curve_zero, nearest_band, nmd_bands, post_shock_rate,
    prepayment_bands, prepayment_multiplier, shock_delta_bps, slot_cells, split_nmd,
    standard_time_bands, standardised_eve_from_cube, standardised_eve_scenario, standardised_irrbb,
    tdrr, tdrr_bands, tdrr_multiplier, EveScenarioResult, IrrbbError, IrrbbResult, NmdCategory,
    NmdPortfolio, NmdSplit, Regulator, ShockParams, ShockScenario, ShockTable, ShockTableVersion,
    SlotDate, TimeBand, DEFAULT_RATE_FLOOR,
};
pub use scenario::{
    Change, Dimension, Provenance, ResolvedScenario, ResolvedShock, Scenario, ScenarioDiff,
    ScenarioError, ScenarioKind, ScenarioStatus, Shock,
};
