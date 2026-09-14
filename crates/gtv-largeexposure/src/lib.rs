//! gtv-largeexposure: Large Exposure / Concentration engine (MA(BS)28).
//!
//! Enterprise-batch crate (prod_p5). Design: `le.md`,
//! `doc/large_exposure_review.md`, `doc/large_exposure_incremental.md`,
//! `doc/large_exposure_reports.md`.
//!
//! Layers:
//! * [`config`] — every parameter (defaults = regulatory) is configurable.
//! * [`entity`] / [`relationship`] — graph + effective dating.
//! * [`exposure`] — the six MA(BS)28 exposure components, before/after CRM.
//! * [`limit`] / [`concentration`] — limit engine and concentration metrics.
//! * [`temporal`] / [`ledger`] / [`ranking`] — incremental recomputation
//!   (Fenwick + segment tree) and top-N ranking.

pub mod concentration;
pub mod config;
pub mod entity;
pub mod exposure;
pub mod ledger;
pub mod limit;
pub mod ma_bs28;
pub mod ranking;
pub mod relationship;
pub mod scenario;
pub mod temporal;

pub use concentration::{concentration, ConcentrationDimension, ConcentrationRecord};
pub use config::{DerivativeMeasure, LeConfig, Tier1Source};
pub use entity::{EconomicSector, Entity, EntityKind, Scope};
pub use exposure::{EventKind, ExposureEvent, ExposureMeasure, Measure};
pub use ledger::{AggKey, AggregateKind, Ledger, LedgerError};
pub use limit::{LimitMetric, LimitOutcome, LimitRule, LimitSet, LimitStatus};
pub use ma_bs28::{ma_bs28_report, MaBs28Part, MaBs28Row};
pub use ranking::{rank_top_n, Ranked};
pub use relationship::{GroupMap, Relationship, RelationshipKind};
pub use scenario::{
    stress_concentration, stress_group, stress_scan, stressed_book_total, RatePosition,
    ScenarioSpec, StressResult,
};
pub use temporal::{Fenwick, SegTree, TimeAxis};
