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
    aggregate_eve, black_caplet, black_swaption, cpr, cpr_with, cube_bands, curve_zero,
    forward_rate, nearest_band, nmd_bands, nmd_bands_with, norm_cdf, option_risk_measure,
    option_risk_measure_with, post_shock_rate, post_shock_rate_with, prepayment_bands,
    prepayment_bands_with, prepayment_multiplier, shift_curve, shift_curve_with, shock_delta_bps,
    shock_delta_bps_with, slot_cells, split_nmd, split_nmd_with, standard_time_bands,
    standardised_eve_from_cube, standardised_eve_scenario, standardised_eve_scenario_with,
    standardised_irrbb, standardised_irrbb_with, tdrr, tdrr_bands, tdrr_bands_with, tdrr_multiplier,
    tdrr_with, Caplet, EveScenarioResult, IrrbbConfig, IrrbbError, IrrbbResult, NmdCaps,
    NmdCategory, NmdPortfolio, NmdSplit, OptionKind, OptionPortfolio, Regulator, ShockFormula,
    ShockParams, ShockScenario, ShockTable, ShockTableVersion, SlotDate, Swaption, SwaptionKind,
    TimeBand, DEFAULT_RATE_FLOOR,
};
pub use scenario::{
    Change, Dimension, Provenance, ResolvedScenario, ResolvedShock, Scenario, ScenarioDiff,
    ScenarioError, ScenarioKind, ScenarioStatus, Shock,
};
