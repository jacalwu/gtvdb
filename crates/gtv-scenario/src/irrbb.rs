//! HKMA / BCBS standardised IRRBB framework (reference implementation).
//!
//! Implements the local standardised framework of HKMA SPM **IR-1** §5 (which
//! mirrors BCBS *Interest rate risk in the banking book* (d368, SRP31/SRP98)
//! as recalibrated by BCBS d578):
//!
//! * [`ShockScenario`] — the six prescribed EVE scenarios (§5.34);
//! * [`shock_delta_bps`] / [`post_shock_rate`] — the scenario parameterisations
//!   with the decay `exp(-t/4)` and the `-2%` floor;
//! * [`ShockTable`] — the specified shock sizes, both the **current** (BCBS
//!   d368) and the **recalibrated** (BCBS d578, HKMA effective 1 Jan 2026)
//!   tables;
//! * [`standard_time_bands`] — the 19 prescribed time buckets with their
//!   midpoints (d368 Table 1);
//! * [`standardised_eve_scenario`] / [`aggregate_eve`] — the standardised EVE
//!   measure (§5.1);
//! * [`split_nmd`] — core / non-core NMD split with the supervisory caps
//!   (§5.3);
//! * [`cpr`] / [`tdrr`] — scenario-dependent prepayment and term-deposit
//!   early-redemption rates (§5.2).
//!
//! The generic ALM primitives live in [`crate::alm`]; this module is the
//! authoritative, unit-tested encoding of the supervisory formulae.

use std::collections::BTreeMap;

use thiserror::Error;

use crate::alm::{AlmCell, AlmCube, AlmFilter, CashflowType, DiscountCurve};

/// Errors from the standardised IRRBB computations.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IrrbbError {
    #[error("cash-flow vector length {got} does not match {expected} time bands")]
    ShapeMismatch { expected: usize, got: usize },
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

/// The six prescribed interest rate shock scenarios (IR-1 §5.34.1 / BCBS
/// SRP31.90).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ShockScenario {
    ParallelUp,
    ParallelDown,
    Steepener,
    Flattener,
    ShortUp,
    ShortDown,
}

impl ShockScenario {
    pub const ALL: [ShockScenario; 6] = [
        ShockScenario::ParallelUp,
        ShockScenario::ParallelDown,
        ShockScenario::Steepener,
        ShockScenario::Flattener,
        ShockScenario::ShortUp,
        ShockScenario::ShortDown,
    ];

    /// Scenario number `i ∈ {1..6}` used by IR-1.
    pub fn number(self) -> u8 {
        match self {
            ShockScenario::ParallelUp => 1,
            ShockScenario::ParallelDown => 2,
            ShockScenario::Steepener => 3,
            ShockScenario::Flattener => 4,
            ShockScenario::ShortUp => 5,
            ShockScenario::ShortDown => 6,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            ShockScenario::ParallelUp => "parallel_up",
            ShockScenario::ParallelDown => "parallel_down",
            ShockScenario::Steepener => "steepener",
            ShockScenario::Flattener => "flattener",
            ShockScenario::ShortUp => "short_up",
            ShockScenario::ShortDown => "short_down",
        }
    }
}

/// Specified shock sizes for one currency, in basis points (IR-1 §5.34).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ShockParams {
    pub parallel_bps: f64,
    pub short_bps: f64,
    pub long_bps: f64,
}

impl ShockParams {
    pub fn new(parallel_bps: f64, short_bps: f64, long_bps: f64) -> Self {
        Self {
            parallel_bps,
            short_bps,
            long_bps,
        }
    }
}

/// Which calibration of the specified shock sizes to apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShockTableVersion {
    /// BCBS d368 (the calibration currently in IR-1's second table).
    Current2018,
    /// BCBS d578 recalibration, to be implemented by 1 January 2026
    /// (IR-1's first table / HKMA circular of 22 July 2024).
    Recalibrated2026,
}

/// Currency-specific specified shock sizes.
#[derive(Debug, Clone)]
pub struct ShockTable {
    pub version: ShockTableVersion,
    params: BTreeMap<String, ShockParams>,
}

impl ShockTable {
    fn build(version: ShockTableVersion, rows: &[(&str, f64, f64, f64)]) -> Self {
        Self {
            version,
            params: rows
                .iter()
                .map(|(c, p, s, l)| (c.to_string(), ShockParams::new(*p, *s, *l)))
                .collect(),
        }
    }

    /// The pre-recalibration table (BCBS d368), as listed in IR-1.
    pub fn current() -> Self {
        Self::build(
            ShockTableVersion::Current2018,
            &[
                ("ARS", 400.0, 500.0, 300.0),
                ("AUD", 300.0, 450.0, 200.0),
                ("BRL", 400.0, 500.0, 300.0),
                ("CAD", 200.0, 300.0, 150.0),
                ("CHF", 100.0, 150.0, 100.0),
                ("CNY", 250.0, 300.0, 150.0),
                ("CNH", 250.0, 300.0, 150.0),
                ("EUR", 200.0, 250.0, 100.0),
                ("GBP", 250.0, 300.0, 150.0),
                ("HKD", 200.0, 250.0, 100.0),
                ("IDR", 400.0, 500.0, 300.0),
                ("INR", 400.0, 500.0, 300.0),
                ("JPY", 100.0, 100.0, 100.0),
                ("KRW", 300.0, 400.0, 200.0),
                ("MXN", 400.0, 500.0, 300.0),
                ("RUB", 400.0, 500.0, 300.0),
                ("SAR", 200.0, 300.0, 150.0),
                ("SEK", 200.0, 300.0, 150.0),
                ("SGD", 150.0, 200.0, 100.0),
                ("TRY", 400.0, 500.0, 300.0),
                ("USD", 200.0, 300.0, 150.0),
                ("ZAR", 400.0, 500.0, 300.0),
            ],
        )
    }

    /// The BCBS d578 recalibrated table (HKMA effective 1 January 2026).
    pub fn recalibrated_2026() -> Self {
        Self::build(
            ShockTableVersion::Recalibrated2026,
            &[
                ("ARS", 400.0, 500.0, 300.0),
                ("AUD", 350.0, 425.0, 300.0),
                ("BRL", 400.0, 500.0, 300.0),
                ("CAD", 200.0, 275.0, 175.0),
                ("CHF", 175.0, 250.0, 200.0),
                ("CNH", 225.0, 300.0, 150.0),
                ("CNY", 225.0, 300.0, 150.0),
                ("EUR", 225.0, 350.0, 200.0),
                ("GBP", 275.0, 425.0, 250.0),
                ("HKD", 225.0, 375.0, 200.0),
                ("IDR", 400.0, 500.0, 300.0),
                ("INR", 325.0, 475.0, 225.0),
                ("JPY", 100.0, 100.0, 100.0),
                ("KRW", 225.0, 350.0, 225.0),
                ("MXN", 400.0, 500.0, 200.0),
                ("RUB", 400.0, 500.0, 300.0),
                ("SAR", 275.0, 375.0, 250.0),
                ("SEK", 275.0, 425.0, 200.0),
                ("SGD", 175.0, 250.0, 225.0),
                ("TRY", 400.0, 500.0, 300.0),
                ("USD", 200.0, 300.0, 225.0),
                ("ZAR", 325.0, 500.0, 300.0),
            ],
        )
    }

    /// Shock sizes for `currency`; MOP follows HKD and unknown currencies fall
    /// back to 400 / 500 / 300 bps (IR-1 §5.34.3–5.34.4).
    pub fn params(&self, currency: &str) -> ShockParams {
        let c = if currency.eq_ignore_ascii_case("MOP") {
            "HKD"
        } else {
            currency
        };
        self.params
            .get(&c.to_ascii_uppercase())
            .copied()
            .unwrap_or(ShockParams::new(400.0, 500.0, 300.0))
    }
}

// ---------------------------------------------------------------------------
// Scenario parameterisations
// ---------------------------------------------------------------------------

/// The instantaneous shock to the risk-free rate (in **basis points**) for
/// scenario `i` at tenor `t_years` (IR-1 §5.34.1):
///
/// ```text
/// parallel up   : +R_parallel
/// parallel down : -R_parallel
/// steepener     : -0.65*R_short*e^(-t/4) + 0.9*R_long*(1 - e^(-t/4))
/// flattener     :  0.8*R_short*e^(-t/4) - 0.6*R_long*(1 - e^(-t/4))
/// short up      : +R_short*e^(-t/4)
/// short down    : -R_short*e^(-t/4)
/// ```
pub fn shock_delta_bps(scenario: ShockScenario, params: ShockParams, t_years: f64) -> f64 {
    let decay = (-t_years / 4.0).exp();
    match scenario {
        ShockScenario::ParallelUp => params.parallel_bps,
        ShockScenario::ParallelDown => -params.parallel_bps,
        ShockScenario::Steepener => {
            -0.65 * params.short_bps * decay + 0.9 * params.long_bps * (1.0 - decay)
        }
        ShockScenario::Flattener => {
            0.8 * params.short_bps * decay - 0.6 * params.long_bps * (1.0 - decay)
        }
        ShockScenario::ShortUp => params.short_bps * decay,
        ShockScenario::ShortDown => -params.short_bps * decay,
    }
}

/// The supervisory floor on post-shock rates: `-2%` (IR-1 §5.34.2). A floor
/// chosen by national discretion must not exceed zero.
pub const DEFAULT_RATE_FLOOR: f64 = -0.02;

/// Post-shock risk-free rate: `max(r0 + delta, floor)` (IR-1 §5.34.2).
pub fn post_shock_rate(
    r0: f64,
    scenario: ShockScenario,
    params: ShockParams,
    t_years: f64,
    floor: f64,
) -> f64 {
    let delta = shock_delta_bps(scenario, params, t_years) / 10_000.0;
    (r0 + delta).max(floor)
}

// ---------------------------------------------------------------------------
// Time bands
// ---------------------------------------------------------------------------

/// One prescribed time bucket (IR-1 §5.1.1 / d368 Table 1).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TimeBand {
    pub label: &'static str,
    pub start_years: f64,
    pub end_years: f64,
    /// Midpoint in years used for discounting (`t_k`).
    pub midpoint_years: f64,
}

impl TimeBand {
    #[inline]
    pub fn midpoint_days(&self) -> f64 {
        self.midpoint_years * 365.0
    }
}

/// The 19 standardised time buckets and their midpoints (d368 Table 1).
pub fn standard_time_bands() -> Vec<TimeBand> {
    let b = |label, start_years, end_years, midpoint_years| TimeBand {
        label,
        start_years,
        end_years,
        midpoint_years,
    };
    vec![
        b("O/N", 0.0, 1.0 / 365.0, 0.0028),
        b("1M", 1.0 / 365.0, 1.0 / 12.0, 0.0417),
        b("3M", 1.0 / 12.0, 0.25, 0.1667),
        b("6M", 0.25, 0.5, 0.375),
        b("9M", 0.5, 0.75, 0.625),
        b("1Y", 0.75, 1.0, 0.875),
        b("1.5Y", 1.0, 1.5, 1.25),
        b("2Y", 1.5, 2.0, 1.75),
        b("3Y", 2.0, 3.0, 2.5),
        b("4Y", 3.0, 4.0, 3.5),
        b("5Y", 4.0, 5.0, 4.5),
        b("6Y", 5.0, 6.0, 5.5),
        b("7Y", 6.0, 7.0, 6.5),
        b("8Y", 7.0, 8.0, 7.5),
        b("9Y", 8.0, 9.0, 8.5),
        b("10Y", 9.0, 10.0, 9.5),
        b("15Y", 10.0, 15.0, 12.5),
        b("20Y", 15.0, 20.0, 17.5),
        b(">20Y", 20.0, f64::INFINITY, 25.0),
    ]
}

// ---------------------------------------------------------------------------
// Standardised EVE
// ---------------------------------------------------------------------------

/// One scenario's standardised EVE result for a currency.
#[derive(Debug, Clone, PartialEq)]
pub struct EveScenarioResult {
    pub scenario: ShockScenario,
    /// `ΔE_i,c(k)` per time band.
    pub per_band: Vec<f64>,
    /// Automatic interest-rate option risk `KAO_i,c`.
    pub option_risk: f64,
    /// `max(0, Σ_k ΔE(k) + KAO)` (IR-1 §5.1.1).
    pub delta_eve: f64,
}

/// Standardised EVE for one scenario and one currency (IR-1 §5.1.1):
///
/// `ΔE_i,c(k) = CF_0(k)·exp(-r_0(k)·t_k) - CF_i(k)·exp(-r_i(k)·t_k)`,
/// with `t_k` the band midpoint and `r_i = max(r_0 + Δr_i, floor)`.
///
/// `cf0` and `cf_shocked` are the net notional repricing cash flows (principal
/// **and** coupon) slotted into the bands at their earliest repricing dates;
/// `base_zero` returns the current risk-free zero rate at a midpoint `t_k`.
#[allow(clippy::too_many_arguments)]
pub fn standardised_eve_scenario(
    bands: &[TimeBand],
    cf0: &[f64],
    cf_shocked: &[f64],
    base_zero: impl Fn(f64) -> f64,
    scenario: ShockScenario,
    params: ShockParams,
    option_risk: f64,
    floor: f64,
) -> Result<EveScenarioResult, IrrbbError> {
    if cf0.len() != bands.len() || cf_shocked.len() != bands.len() {
        return Err(IrrbbError::ShapeMismatch {
            expected: bands.len(),
            got: cf0.len().max(cf_shocked.len()),
        });
    }
    let mut per_band = Vec::with_capacity(bands.len());
    let mut total = 0.0;
    for (k, band) in bands.iter().enumerate() {
        let t = band.midpoint_years;
        let r0 = base_zero(t);
        let ri = post_shock_rate(r0, scenario, params, t, floor);
        let d0 = (-r0 * t).exp();
        let di = (-ri * t).exp();
        let de = cf0[k] * d0 - cf_shocked[k] * di;
        per_band.push(de);
        total += de;
    }
    Ok(EveScenarioResult {
        scenario,
        per_band,
        option_risk,
        delta_eve: (total + option_risk).max(0.0),
    })
}

/// The aggregate standardised EVE risk measure across the six scenarios:
/// `max_i (Σ_c ΔE_i,c)` (IR-1 §5.1.1).
pub fn aggregate_eve(per_scenario_currency_totals: &[f64]) -> f64 {
    per_scenario_currency_totals
        .iter()
        .copied()
        .fold(0.0_f64, f64::max)
}

// ---------------------------------------------------------------------------
// Non-maturity deposits (NMDs)
// ---------------------------------------------------------------------------

/// NMD segmentation (IR-1 §5.3.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NmdCategory {
    RetailTransactional,
    RetailNonTransactional,
    NonRetail,
}

impl NmdCategory {
    /// `(cap on core proportion, cap on average maturity of core in years)`.
    pub fn caps(self) -> (f64, f64) {
        match self {
            NmdCategory::RetailTransactional => (0.90, 5.0),
            NmdCategory::RetailNonTransactional => (0.70, 4.5),
            NmdCategory::NonRetail => (0.50, 4.0),
        }
    }
}

/// Core / non-core split after applying the supervisory caps.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NmdSplit {
    pub total: f64,
    /// Core proportion actually applied (≤ the category cap).
    pub core_ratio: f64,
    pub core: f64,
    pub non_core: f64,
    /// Cap on the average behavioural maturity of the core portion (years).
    pub max_core_maturity_years: f64,
}

/// Apply the IR-1 §5.3.1 caps to an observed core proportion. Non-core
/// deposits are treated as overnight; core deposits are slotted by their
/// average behavioural maturity (≤ the category cap).
pub fn split_nmd(total: f64, observed_core_ratio: f64, category: NmdCategory) -> NmdSplit {
    let (cap_ratio, cap_maturity) = category.caps();
    let core_ratio = observed_core_ratio.clamp(0.0, cap_ratio);
    let core = total * core_ratio;
    NmdSplit {
        total,
        core_ratio,
        core,
        non_core: total - core,
        max_core_maturity_years: cap_maturity,
    }
}

// ---------------------------------------------------------------------------
// Behavioural option scenario multipliers
// ---------------------------------------------------------------------------

/// Conditional-prepayment-rate multiplier `γ_i` (IR-1 §5.2.1): 0.8 for
/// parallel up / steepener / short up, 1.2 for parallel down / flattener /
/// short down.
pub fn prepayment_multiplier(scenario: ShockScenario) -> f64 {
    match scenario {
        ShockScenario::ParallelUp | ShockScenario::Steepener | ShockScenario::ShortUp => 0.8,
        ShockScenario::ParallelDown | ShockScenario::Flattener | ShockScenario::ShortDown => 1.2,
    }
}

/// Scenario CPR: `min(1, γ_i · CPR_0)` (IR-1 §5.2.1).
pub fn cpr(scenario: ShockScenario, baseline_cpr: f64) -> f64 {
    (prepayment_multiplier(scenario) * baseline_cpr).min(1.0)
}

/// Term-deposit redemption-ratio multiplier `u_i` (IR-1 §5.2.2): 1.2 for
/// parallel up / flattener / short up, 0.8 for parallel down / steepener /
/// short down.
pub fn tdrr_multiplier(scenario: ShockScenario) -> f64 {
    match scenario {
        ShockScenario::ParallelUp | ShockScenario::Flattener | ShockScenario::ShortUp => 1.2,
        ShockScenario::ParallelDown | ShockScenario::Steepener | ShockScenario::ShortDown => 0.8,
    }
}

/// Scenario TDRR: `min(1, u_i · TDRR_0)` (IR-1 §5.2.2).
pub fn tdrr(scenario: ShockScenario, baseline_tdrr: f64) -> f64 {
    (tdrr_multiplier(scenario) * baseline_tdrr).min(1.0)
}

/// Convenience: the current risk-free zero rate from a [`DiscountCurve`].
pub fn curve_zero(curve: &DiscountCurve) -> impl Fn(f64) -> f64 + '_ {
    move |t_years: f64| curve.zero_rate((t_years * 365.0).round() as i64)
}

// ---------------------------------------------------------------------------
// Regulator selection
// ---------------------------------------------------------------------------

/// The supervisory regime whose standardised parameters are applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Regulator {
    /// Hong Kong Monetary Authority — SPM IR-1.
    Hkma,
    /// Monetary Authority of Singapore — Notice 653.
    Mas,
}

impl Regulator {
    pub fn as_str(self) -> &'static str {
        match self {
            Regulator::Hkma => "HKMA",
            Regulator::Mas => "MAS",
        }
    }

    /// The specified-shock table for a reporting year.
    ///
    /// Both HKMA and MAS follow the BCBS d578 recalibration timetable
    /// (implementation by 1 January 2026); before that the BCBS d368 table
    /// applies. MAS Notice 653 adopts the BCBS standardised approach, so the
    /// same BCBS tables apply (SGD included) pending any local overlay.
    pub fn shock_table(self, reporting_year: i32) -> ShockTable {
        if reporting_year >= 2026 {
            ShockTable::recalibrated_2026()
        } else {
            ShockTable::current()
        }
    }
}

// ---------------------------------------------------------------------------
// Slotting notional repricing cash flows into the 19 time bands
// ---------------------------------------------------------------------------

/// Which date of an [`AlmCell`] is used to choose the time band.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotDate {
    /// Earliest interest-rate repricing date (notional principal).
    RepricingDate,
    /// Cash-flow / payment date (coupons, fees).
    CashflowDate,
}

/// Index of the band whose midpoint is closest to `date_years` (ties resolve
/// to the earlier band).
pub fn nearest_band(bands: &[TimeBand], date_years: f64) -> usize {
    let mut best = 0usize;
    let mut best_d = f64::INFINITY;
    for (i, b) in bands.iter().enumerate() {
        let d = (b.midpoint_years - date_years).abs();
        if d < best_d - 1e-12 {
            best_d = d;
            best = i;
        }
    }
    best
}

fn slot_into(out: &mut [f64], bands: &[TimeBand], days: i64, amount: f64) {
    let idx = nearest_band(bands, days as f64 / 365.0);
    out[idx] += amount;
}

/// Slot cells into the bands using an explicit date selector.
pub fn slot_cells(
    cells: &[AlmCell],
    bands: &[TimeBand],
    filter: &AlmFilter,
    selector: SlotDate,
) -> Vec<f64> {
    let mut out = vec![0.0; bands.len()];
    for c in cells.iter().filter(|c| filter.matches(c)) {
        let days = match selector {
            SlotDate::RepricingDate => c.repricing_date,
            SlotDate::CashflowDate => c.time_bucket,
        };
        slot_into(&mut out, bands, days, c.amount);
    }
    out
}

/// Net notional repricing cash flows `CF_0(k)` for the standardised framework:
/// * `Principal` is slotted by its earliest repricing date;
/// * `Interest` / `Fee` / `Other` and plain `Deposit` cash flows are slotted by
///   their payment date;
/// * `Prepayment` / `Optionality` cells are excluded (they are scenario- or
///   option-dependent and are handled by [`prepayment_bands`] / the option
///   risk measure `KAO`).
///
/// NMDs should normally be supplied separately via [`NmdPortfolio`] /
/// [`nmd_bands`] rather than as plain `Deposit` cells.
pub fn cube_bands(cube: &AlmCube, bands: &[TimeBand], filter: &AlmFilter) -> Vec<f64> {
    let mut out = vec![0.0; bands.len()];
    for c in cube.cells() {
        if !filter.matches(c) {
            continue;
        }
        let days = match c.cashflow_type {
            CashflowType::Principal => c.repricing_date,
            CashflowType::Interest | CashflowType::Fee | CashflowType::Other => c.time_bucket,
            CashflowType::Deposit => c.time_bucket,
            CashflowType::Prepayment | CashflowType::Optionality => continue,
        };
        slot_into(&mut out, bands, days, c.amount);
    }
    out
}

// ---------------------------------------------------------------------------
// High-level standardised EVE on an AlmCube
// ---------------------------------------------------------------------------

/// Standardised EVE for one scenario, slotting `cube` into the bands.
#[allow(clippy::too_many_arguments)]
pub fn standardised_eve_from_cube(
    cube: &AlmCube,
    bands: &[TimeBand],
    filter: &AlmFilter,
    base_zero: impl Fn(f64) -> f64,
    scenario: ShockScenario,
    params: ShockParams,
    option_risk: f64,
    floor: f64,
) -> Result<EveScenarioResult, IrrbbError> {
    let cf = cube_bands(cube, bands, filter);
    standardised_eve_scenario(bands, &cf, &cf, base_zero, scenario, params, option_risk, floor)
}

/// The full six-scenario standardised EVE measure for one currency.
#[derive(Debug, Clone, PartialEq)]
pub struct IrrbbResult {
    pub per_scenario: Vec<EveScenarioResult>,
    /// `max_i ΔE_i` across the six scenarios.
    pub aggregate: f64,
    pub cf0: Vec<f64>,
    pub bands: Vec<TimeBand>,
}

/// Compute all six scenarios and the aggregate for one currency. TODO: the
/// shocked cash flows currently equal the base cash flows; scenario-dependent
/// products (CPR/NMD) must be folded into `cf` by the caller via
/// [`prepayment_bands`] / [`nmd_bands`].
#[allow(clippy::too_many_arguments)]
pub fn standardised_irrbb(
    cube: &AlmCube,
    bands: &[TimeBand],
    filter: &AlmFilter,
    base_zero: impl Fn(f64) -> f64,
    table: &ShockTable,
    currency: &str,
    option_risk: f64,
    floor: f64,
) -> Result<IrrbbResult, IrrbbError> {
    let cf = cube_bands(cube, bands, filter);
    let params = table.params(currency);
    let mut per_scenario = Vec::with_capacity(6);
    for scenario in ShockScenario::ALL {
        per_scenario.push(standardised_eve_scenario(
            bands,
            &cf,
            &cf,
            &base_zero,
            scenario,
            params,
            option_risk,
            floor,
        )?);
    }
    let totals: Vec<f64> = per_scenario.iter().map(|r| r.delta_eve).collect();
    Ok(IrrbbResult {
        aggregate: aggregate_eve(&totals),
        per_scenario,
        cf0: cf,
        bands: bands.to_vec(),
    })
}

// ---------------------------------------------------------------------------
// NMD slotting
// ---------------------------------------------------------------------------

/// One NMD portfolio to be slotted under the behavioural-maturity approach.
#[derive(Debug, Clone, PartialEq)]
pub struct NmdPortfolio {
    /// Signed balance (positive = inflow; negative = liability).
    pub total: f64,
    pub observed_core_ratio: f64,
    pub category: NmdCategory,
    /// AI-estimated average behavioural maturity of the core portion (years),
    /// capped at the category cap.
    pub average_core_maturity_years: f64,
    pub currency: String,
}

/// Slot an NMD portfolio: non-core is placed in the overnight band and core in
/// the band whose midpoint is closest to the (capped) average behavioural
/// maturity (IR-1 §5.3.1).
pub fn nmd_bands(portfolio: &NmdPortfolio, bands: &[TimeBand]) -> Vec<f64> {
    let split = split_nmd(
        portfolio.total,
        portfolio.observed_core_ratio,
        portfolio.category,
    );
    let mut out = vec![0.0; bands.len()];
    if !out.is_empty() {
        // Non-core deposits are overnight.
        out[0] += split.non_core;
    }
    let maturity = portfolio
        .average_core_maturity_years
        .clamp(0.0, split.max_core_maturity_years);
    let idx = nearest_band(bands, maturity);
    out[idx] += split.core;
    out
}

// ---------------------------------------------------------------------------
// Scenario-dependent behavioural cash flows
// ---------------------------------------------------------------------------

/// Scenario prepayment schedule for retail fixed-rate loans (IR-1 §5.2.1):
///
/// ```text
/// CF_i(k) = CF_S(k) + CPR_i · NO(k-1)
/// ```
///
/// `scheduled_principal[k]` is the scheduled principal repayment in band `k`;
/// `NO(k-1)` is the notional still outstanding after band `k-1`.
pub fn prepayment_bands(
    scheduled_principal: &[f64],
    baseline_cpr: f64,
    scenario: ShockScenario,
) -> Vec<f64> {
    let rate = cpr(scenario, baseline_cpr);
    let mut out = vec![0.0; scheduled_principal.len()];
    let mut outstanding: f64 = scheduled_principal.iter().sum();
    for k in 0..scheduled_principal.len() {
        let scheduled = scheduled_principal[k];
        let prepay = rate * outstanding;
        out[k] = scheduled + prepay;
        outstanding = (outstanding - scheduled - prepay).max(0.0);
    }
    out
}

/// Scenario cash flows for retail term deposits subject to early redemption
/// risk (IR-1 §5.2.2): `TD_0·TDRR_i` is withdrawn early and slotted into the
/// overnight band, the remainder stays at its contractual band.
pub fn tdrr_bands(
    term_deposits: &[f64],
    baseline_tdrr: f64,
    scenario: ShockScenario,
) -> Vec<f64> {
    let rate = tdrr(scenario, baseline_tdrr);
    let mut out = vec![0.0; term_deposits.len()];
    for (k, td) in term_deposits.iter().enumerate() {
        if k == 0 {
            out[0] += td;
        } else {
            out[0] += td * rate;
            out[k] += td * (1.0 - rate);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alm::{AlmCell, AlmCube, AlmFilter, CashflowType};

    fn approx(a: f64, b: f64, eps: f64) -> bool {
        (a - b).abs() <= eps
    }

    #[test]
    fn bcbs_worked_examples_short_steepener_flattener() {
        // BCBS d578 SRP31.92 worked examples at t_k = 3.5 years, R = 100 bp.
        let p = ShockParams::new(100.0, 100.0, 100.0);
        assert!(approx(
            shock_delta_bps(ShockScenario::ShortUp, p, 3.5),
            41.7,
            0.05
        ));
        assert!(approx(
            shock_delta_bps(ShockScenario::Steepener, p, 3.5),
            25.4,
            0.05
        ));
        assert!(approx(
            shock_delta_bps(ShockScenario::Flattener, p, 3.5),
            -1.6,
            0.05
        ));
        // Short down is the mirror of short up.
        assert!(approx(
            shock_delta_bps(ShockScenario::ShortDown, p, 3.5),
            -41.7,
            0.05
        ));
    }

    #[test]
    fn parallel_and_floor_behaviour() {
        let p = ShockParams::new(300.0, 500.0, 200.0);
        assert_eq!(shock_delta_bps(ShockScenario::ParallelUp, p, 1.0), 300.0);
        assert_eq!(
            shock_delta_bps(ShockScenario::ParallelDown, p, 1.0),
            -300.0
        );
        // r0 = 0, floor -2% -> parallel down cannot go below -2%.
        let ri = post_shock_rate(0.0, ShockScenario::ParallelDown, p, 1.0, DEFAULT_RATE_FLOOR);
        assert!(approx(ri, -0.02, 1e-12));
        // National-discretion floors (which must not exceed zero) are respected.
        let ri = post_shock_rate(0.0, ShockScenario::ParallelDown, p, 1.0, -0.01);
        assert!(approx(ri, -0.01, 1e-12)); // -0.03 floored at -0.01
        let ri = post_shock_rate(0.0, ShockScenario::ParallelDown, p, 1.0, 0.0);
        assert!(approx(ri, 0.0, 1e-12));
    }

    #[test]
    fn hkma_shock_tables() {
        let cur = ShockTable::current();
        assert_eq!(cur.version, ShockTableVersion::Current2018);
        let hkd = cur.params("HKD");
        assert_eq!((hkd.parallel_bps, hkd.short_bps, hkd.long_bps), (200.0, 250.0, 100.0));
        // MOP follows HKD
        assert_eq!(cur.params("MOP"), hkd);
        // Unknown currency default 400/500/300
        let xxx = cur.params("XXX");
        assert_eq!((xxx.parallel_bps, xxx.short_bps, xxx.long_bps), (400.0, 500.0, 300.0));

        let rec = ShockTable::recalibrated_2026();
        assert_eq!(rec.version, ShockTableVersion::Recalibrated2026);
        let hkd = rec.params("HKD");
        assert_eq!((hkd.parallel_bps, hkd.short_bps, hkd.long_bps), (225.0, 375.0, 200.0));
        let usd = rec.params("USD");
        assert_eq!((usd.parallel_bps, usd.short_bps, usd.long_bps), (200.0, 300.0, 225.0));
        // All recalibrated shocks are multiples of 25 bps (BCBS d578).
        for c in ["ARS", "AUD", "HKD", "USD", "JPY", "SGD", "EUR", "GBP"] {
            let s = rec.params(c);
            for v in [s.parallel_bps, s.short_bps, s.long_bps] {
                assert_eq!(v % 25.0, 0.0, "{c} shock {v} not a multiple of 25bp");
            }
        }
    }

    #[test]
    fn standard_time_bands_match_the_19_buckets() {
        let bands = standard_time_bands();
        assert_eq!(bands.len(), 19);
        assert_eq!(bands[0].midpoint_years, 0.0028);
        assert_eq!(bands[9].label, "4Y");
        assert_eq!(bands[9].midpoint_years, 3.5); // the BCBS worked-example bucket
        assert_eq!(bands[18].label, ">20Y");
        assert_eq!(bands[18].midpoint_years, 25.0);
        // midpoints are strictly increasing
        assert!(bands.windows(2).all(|w| w[0].midpoint_years < w[1].midpoint_years));
    }

    #[test]
    fn standardised_eve_parallel_up_is_a_loss_when_assets_exceed_liabilities() {
        let bands = standard_time_bands();
        // 100 of net notional repricing at 1Y; flat 5% curve.
        let mut cf0 = vec![0.0; bands.len()];
        cf0[5] = 100.0; // 1Y bucket, midpoint 0.875
        let cf_shocked = cf0.clone();
        let base = |_t: f64| 0.05;
        let p = ShockParams::new(200.0, 300.0, 150.0);
        let r = standardised_eve_scenario(
            &bands,
            &cf0,
            &cf_shocked,
            base,
            ShockScenario::ParallelUp,
            p,
            0.0,
            DEFAULT_RATE_FLOOR,
        )
        .unwrap();
        // EVE falls when rates rise -> positive loss.
        assert!(r.delta_eve > 0.0);
        // Per-band loss = 100*(e^-0.05*0.875 - e^-0.07*0.875)
        let expected = 100.0 * ((-0.05 * 0.875_f64).exp() - (-0.07 * 0.875_f64).exp());
        assert!(approx(r.per_band[5], expected, 1e-9));
        // A parallel-down shock produces a gain -> floored at zero.
        let down = standardised_eve_scenario(
            &bands,
            &cf0,
            &cf_shocked,
            base,
            ShockScenario::ParallelDown,
            p,
            0.0,
            DEFAULT_RATE_FLOOR,
        )
        .unwrap();
        assert_eq!(down.delta_eve, 0.0);
    }

    #[test]
    fn standardised_eve_includes_option_risk_and_aggregates_by_max() {
        let bands = standard_time_bands();
        let mut cf0 = vec![0.0; bands.len()];
        cf0[9] = 1000.0; // 4Y bucket
        let base = |_t: f64| 0.03;
        let p = ShockParams::new(200.0, 300.0, 150.0);
        let with_opt = standardised_eve_scenario(
            &bands,
            &cf0,
            &cf0,
            base,
            ShockScenario::ParallelUp,
            p,
            5.0, // KAO
            DEFAULT_RATE_FLOOR,
        )
        .unwrap();
        let no_opt = standardised_eve_scenario(
            &bands,
            &cf0,
            &cf0,
            base,
            ShockScenario::ParallelUp,
            p,
            0.0,
            DEFAULT_RATE_FLOOR,
        )
        .unwrap();
        assert!(approx(with_opt.delta_eve - no_opt.delta_eve, 5.0, 1e-9));
        // Aggregate is the max across scenarios.
        assert_eq!(
            aggregate_eve(&[1.0, 7.5, 3.0, 0.0, 2.0, 1.0]),
            7.5
        );
    }

    #[test]
    fn nmd_caps_are_enforced() {
        let s = split_nmd(1000.0, 0.95, NmdCategory::RetailTransactional);
        assert!(approx(s.core_ratio, 0.90, 1e-12));
        assert!(approx(s.core, 900.0, 1e-9));
        assert!(approx(s.non_core, 100.0, 1e-9));
        assert_eq!(s.max_core_maturity_years, 5.0);

        let s = split_nmd(1000.0, 0.60, NmdCategory::NonRetail);
        assert!(approx(s.core_ratio, 0.50, 1e-12));
        assert_eq!(s.max_core_maturity_years, 4.0);

        let s = split_nmd(1000.0, 0.40, NmdCategory::RetailNonTransactional);
        assert!(approx(s.core_ratio, 0.40, 1e-12));
        assert_eq!(s.max_core_maturity_years, 4.5);
    }

    #[test]
    fn behavioural_option_scenario_multipliers() {
        // Prepayment γ: up/steepener/short-up 0.8, down/flattener/short-down 1.2
        assert_eq!(prepayment_multiplier(ShockScenario::ParallelUp), 0.8);
        assert_eq!(prepayment_multiplier(ShockScenario::Steepener), 0.8);
        assert_eq!(prepayment_multiplier(ShockScenario::ParallelDown), 1.2);
        assert_eq!(prepayment_multiplier(ShockScenario::Flattener), 1.2);
        assert!(approx(cpr(ShockScenario::ParallelDown, 0.1), 0.12, 1e-12));
        assert!(approx(cpr(ShockScenario::ParallelUp, 0.1), 0.08, 1e-12));
        assert_eq!(cpr(ShockScenario::ParallelDown, 0.95), 1.0); // min(1, ...)

        // TDRR u: parallel up 1.2, parallel down 0.8, steepener 0.8, flattener 1.2
        assert_eq!(tdrr_multiplier(ShockScenario::ParallelUp), 1.2);
        assert_eq!(tdrr_multiplier(ShockScenario::Steepener), 0.8);
        assert_eq!(tdrr_multiplier(ShockScenario::Flattener), 1.2);
        assert!(approx(tdrr(ShockScenario::ParallelUp, 0.1), 0.12, 1e-12));
        assert!(approx(tdrr(ShockScenario::Steepener, 0.1), 0.08, 1e-12));
    }

    #[test]
    fn curve_zero_helper_reads_the_curve() {
        let curve = DiscountCurve::from_zero_rates(vec![(0, 0.01), (365, 0.03)]).unwrap();
        let f = curve_zero(&curve);
        assert!(approx(f(0.0), 0.01, 1e-12));
        assert!(approx(f(1.0), 0.03, 1e-12));
    }

    #[test]
    fn nearest_band_selects_the_closest_midpoint() {
        let bands = standard_time_bands();
        assert_eq!(nearest_band(&bands, 0.0), 0);
        assert_eq!(nearest_band(&bands, 0.9), 5); // 1Y midpoint 0.875
        assert_eq!(nearest_band(&bands, 3.5), 9); // 4Y band (BCBS example)
        assert_eq!(nearest_band(&bands, 30.0), 18);
    }

    #[test]
    fn cube_bands_slot_principal_by_repricing_and_coupon_by_cashflow() {
        let bands = standard_time_bands();
        let mut cube = AlmCube::new();
        // fixed-rate loan: principal reprices (matures) at 365d, coupon paid at 365d
        cube.push(
            AlmCell::new("s", "LE", "HKD", "Loan", 365, CashflowType::Principal, 100.0)
                .with_repricing(365),
        );
        cube.push(
            AlmCell::new("s", "LE", "HKD", "Loan", 365, CashflowType::Interest, 5.0)
                .with_repricing(365),
        );
        // floating: notional reprices at 90d even though the cash flow lands at 365d
        cube.push(
            AlmCell::new("s", "LE", "HKD", "Floater", 365, CashflowType::Principal, 50.0)
                .with_repricing(90),
        );
        let cf = cube_bands(&cube, &bands, &AlmFilter::default());
        assert!(approx(cf[5], 105.0, 1e-9)); // 1Y band: principal + coupon
        assert!(approx(cf[2], 50.0, 1e-9)); // 3M band: floating notional
    }

    #[test]
    fn regulator_selects_the_table_by_reporting_year() {
        assert_eq!(Regulator::Hkma.as_str(), "HKMA");
        assert_eq!(Regulator::Mas.as_str(), "MAS");
        assert_eq!(Regulator::Hkma.shock_table(2025).params("HKD").parallel_bps, 200.0);
        assert_eq!(Regulator::Hkma.shock_table(2026).params("HKD").parallel_bps, 225.0);
        // MAS adopts the BCBS standard: SGD current 150, recalibrated 175.
        assert_eq!(Regulator::Mas.shock_table(2025).params("SGD").parallel_bps, 150.0);
        assert_eq!(Regulator::Mas.shock_table(2026).params("SGD").parallel_bps, 175.0);
    }

    #[test]
    fn standardised_irrbb_runs_six_scenarios_for_hkma_and_mas() {
        let bands = standard_time_bands();
        let mut cube = AlmCube::new();
        cube.push(
            AlmCell::new("s", "LE", "HKD", "Loan", 365, CashflowType::Principal, 1000.0)
                .with_repricing(365),
        );
        let base = |_t: f64| 0.03;
        let hk = Regulator::Hkma.shock_table(2025);
        let r_hk = standardised_irrbb(
            &cube,
            &bands,
            &AlmFilter::default(),
            base,
            &hk,
            "HKD",
            0.0,
            DEFAULT_RATE_FLOOR,
        )
        .unwrap();
        assert_eq!(r_hk.per_scenario.len(), 6);
        assert!(r_hk.aggregate > 0.0);
        assert!(approx(r_hk.cf0[5], 1000.0, 1e-9));

        // Same cube, SGD + MAS table: 150bp parallel vs HKD 200bp -> smaller loss.
        let sg = Regulator::Mas.shock_table(2025);
        let r_sg = standardised_irrbb(
            &cube,
            &bands,
            &AlmFilter::default(),
            base,
            &sg,
            "SGD",
            0.0,
            DEFAULT_RATE_FLOOR,
        )
        .unwrap();
        assert!(
            r_hk.per_scenario[0].delta_eve > r_sg.per_scenario[0].delta_eve,
            "HKD 200bp parallel loss should exceed SGD 150bp"
        );
    }

    #[test]
    fn nmd_bands_place_noncore_overnight_and_core_at_capped_maturity() {
        let bands = standard_time_bands();
        let p = NmdPortfolio {
            total: 1000.0,
            observed_core_ratio: 0.95, // capped to 90% for transactional
            category: NmdCategory::RetailTransactional,
            average_core_maturity_years: 10.0, // capped to 5y
            currency: "HKD".into(),
        };
        let b = nmd_bands(&p, &bands);
        assert!(approx(b[0], 100.0, 1e-9)); // non-core overnight
        assert!(approx(b[10], 900.0, 1e-9)); // core at the 5Y band
        assert!(approx(b.iter().sum::<f64>(), 1000.0, 1e-9));
    }

    #[test]
    fn prepayment_bands_front_load_and_follow_the_scenario_multiplier() {
        let scheduled = vec![0.0, 0.0, 0.0, 100.0, 0.0];
        assert_eq!(
            prepayment_bands(&scheduled, 0.0, ShockScenario::ParallelUp),
            scheduled
        );
        let up = prepayment_bands(&scheduled, 0.1, ShockScenario::ParallelUp); // γ=0.8
        let down = prepayment_bands(&scheduled, 0.1, ShockScenario::ParallelDown); // γ=1.2
        assert!(up[0] > 0.0 && up[1] > 0.0 && up[2] > 0.0, "prepayments move earlier");
        assert!(up.iter().sum::<f64>() >= 100.0);
        assert!(down.iter().sum::<f64>() > up.iter().sum::<f64>());
    }

    #[test]
    fn tdrr_bands_conserve_total_and_move_early_redemptions_overnight() {
        let td = vec![0.0, 100.0, 0.0];
        let up = tdrr_bands(&td, 0.1, ShockScenario::ParallelUp); // u=1.2 -> 12%
        assert!(approx(up.iter().sum::<f64>(), 100.0, 1e-9));
        assert!(approx(up[0], 12.0, 1e-9));
        assert!(approx(up[1], 88.0, 1e-9));
        let down = tdrr_bands(&td, 0.1, ShockScenario::ParallelDown); // u=0.8 -> 8%
        assert!(approx(down[0], 8.0, 1e-9));
    }
}
