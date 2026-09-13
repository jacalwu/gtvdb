//! gtv-governance: versioned CRM governance and governed allocation
//! (prod_p4 D2 / roadmap P2.2).
//!
//! The numerical allocation stays in the audited `gtv-array` CRM kernel;
//! this crate owns the *governance* around it:
//!
//! * [`RuleSet`] / [`RuleRegistry`] — externalised, versioned and
//!   effective-dated collateral eligibility / haircut / FX & maturity mismatch
//!   / guarantee eligibility / wrong-way / concentration rules.
//! * [`GovernedInputs`] — exposures, collateral, guarantees and pledges, turned
//!   into kernel inputs after eligibility, haircut and concentration are
//!   applied.
//! * [`GovernedResult`] — the kernel result plus every exclusion and
//!   concentration breach, so the whole run is auditable.
//! * `greedy_lp_diff` — loan-by-loan greedy vs LP reconciliation (requires the
//!   default `crm-lp` feature).

pub mod crm;
pub mod error;
pub mod rules;

pub use crm::{
    AllocationMethod, CollateralPledge, Exposure, GovernedCollateral, GovernedGuarantor,
    GovernedInputs, GovernedResult, GuaranteePledge, GreedyLpDiff, ConcentrationBreach, Exclusion,
};
pub use error::GovernanceError;
pub use rules::{CollateralRule, ConcentrationLimit, GuaranteeRule, RuleRegistry, RuleSet, WrongWayRisk};

pub use gtv_refdata::EffectiveRange;
