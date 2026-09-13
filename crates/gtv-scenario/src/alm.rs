//! ALM scenario cube (prod_p4 D4 / roadmap P2.4).
//!
//! A standard, versioned cash-flow cube plus the ALM analytics that banks
//! need: cash-flow ladder, NII / EVE, repricing gap, liquidity stress, deposit
//! decay, prepayment, optionality and multi-currency aggregation.
//!
//! The cube is a deterministic, in-memory table of [`AlmCell`] rows. Every
//! calculation filters rows with [`AlmFilter`] and returns sorted results, so
//! the same cube + scenario + behavioural-assumption version always yields the
//! same numbers.

use std::collections::BTreeMap;
use std::f64::consts::E;

use thiserror::Error;

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

/// Cash-flow classification (`cashflow_type`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum CashflowType {
    Interest,
    Principal,
    Fee,
    Prepayment,
    Deposit,
    Optionality,
    Other,
}

impl CashflowType {
    pub const ALL: [CashflowType; 7] = [
        CashflowType::Interest,
        CashflowType::Principal,
        CashflowType::Fee,
        CashflowType::Prepayment,
        CashflowType::Deposit,
        CashflowType::Optionality,
        CashflowType::Other,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            CashflowType::Interest => "interest",
            CashflowType::Principal => "principal",
            CashflowType::Fee => "fee",
            CashflowType::Prepayment => "prepayment",
            CashflowType::Deposit => "deposit",
            CashflowType::Optionality => "optionality",
            CashflowType::Other => "other",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "interest" => Some(CashflowType::Interest),
            "principal" => Some(CashflowType::Principal),
            "fee" => Some(CashflowType::Fee),
            "prepayment" => Some(CashflowType::Prepayment),
            "deposit" => Some(CashflowType::Deposit),
            "optionality" => Some(CashflowType::Optionality),
            "other" => Some(CashflowType::Other),
            _ => None,
        }
    }
}

/// One cube row: a projected cash flow at a time bucket for one dimension
/// combination.
#[derive(Debug, Clone, PartialEq)]
pub struct AlmCell {
    pub as_of_date: i64,
    pub scenario_id: String,
    pub legal_entity: String,
    pub currency: String,
    pub product: String,
    /// Bucket start in days from `as_of_date`.
    pub time_bucket: i64,
    pub cashflow_type: CashflowType,
    /// Signed amount in `currency` (positive = inflow, negative = outflow).
    pub amount: f64,
    /// Optional explicit discount factor overriding the curve.
    pub discount_factor: Option<f64>,
    /// Repricing date in days from `as_of_date`.
    pub repricing_date: i64,
    pub behavioural_assumption_version: String,
}

impl AlmCell {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        scenario_id: impl Into<String>,
        legal_entity: impl Into<String>,
        currency: impl Into<String>,
        product: impl Into<String>,
        time_bucket: i64,
        cashflow_type: CashflowType,
        amount: f64,
    ) -> Self {
        Self {
            as_of_date: 0,
            scenario_id: scenario_id.into(),
            legal_entity: legal_entity.into(),
            currency: currency.into(),
            product: product.into(),
            time_bucket,
            cashflow_type,
            amount,
            discount_factor: None,
            repricing_date: time_bucket,
            behavioural_assumption_version: String::new(),
        }
    }

    pub fn with_as_of(mut self, as_of_date: i64) -> Self {
        self.as_of_date = as_of_date;
        self
    }

    pub fn with_repricing(mut self, repricing_date: i64) -> Self {
        self.repricing_date = repricing_date;
        self
    }

    pub fn with_discount_factor(mut self, df: f64) -> Self {
        self.discount_factor = Some(df);
        self
    }

    pub fn with_assumption_version(mut self, version: impl Into<String>) -> Self {
        self.behavioural_assumption_version = version.into();
        self
    }

    pub fn is_inflow(&self) -> bool {
        self.amount >= 0.0
    }
}

/// Dimension / scenario filter applied before every calculation.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AlmFilter {
    pub scenario_id: Option<String>,
    pub legal_entity: Option<String>,
    pub currency: Option<String>,
    pub product: Option<String>,
    pub as_of_date: Option<i64>,
    pub behavioural_assumption_version: Option<String>,
}

impl AlmFilter {
    pub fn scenario(mut self, v: impl Into<String>) -> Self {
        self.scenario_id = Some(v.into());
        self
    }

    pub fn legal_entity(mut self, v: impl Into<String>) -> Self {
        self.legal_entity = Some(v.into());
        self
    }

    pub fn currency(mut self, v: impl Into<String>) -> Self {
        self.currency = Some(v.into());
        self
    }

    pub fn product(mut self, v: impl Into<String>) -> Self {
        self.product = Some(v.into());
        self
    }

    pub fn as_of(mut self, v: i64) -> Self {
        self.as_of_date = Some(v);
        self
    }

    pub fn assumption_version(mut self, v: impl Into<String>) -> Self {
        self.behavioural_assumption_version = Some(v.into());
        self
    }

    pub(crate) fn matches(&self, c: &AlmCell) -> bool {
        self.scenario_id.as_ref().is_none_or(|v| &c.scenario_id == v)
            && self.legal_entity.as_ref().is_none_or(|v| &c.legal_entity == v)
            && self.currency.as_ref().is_none_or(|v| &c.currency == v)
            && self.product.as_ref().is_none_or(|v| &c.product == v)
            && self.as_of_date.is_none_or(|v| c.as_of_date == v)
            && self
                .behavioural_assumption_version
                .as_ref()
                .is_none_or(|v| &c.behavioural_assumption_version == v)
    }
}

/// An in-memory ALM cube.
#[derive(Debug, Default, Clone)]
pub struct AlmCube {
    cells: Vec<AlmCell>,
}

impl AlmCube {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_cells(cells: Vec<AlmCell>) -> Self {
        Self { cells }
    }

    pub fn push(&mut self, cell: AlmCell) {
        self.cells.push(cell);
    }

    pub fn cells(&self) -> &[AlmCell] {
        &self.cells
    }

    pub fn len(&self) -> usize {
        self.cells.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    fn selected<'a>(&'a self, filter: &'a AlmFilter) -> impl Iterator<Item = &'a AlmCell> {
        self.cells.iter().filter(move |c| filter.matches(c))
    }

    /// Net cash flow per time bucket, ascending.
    pub fn cashflow_ladder(&self, filter: &AlmFilter) -> BTreeMap<i64, f64> {
        let mut out = BTreeMap::new();
        for c in self.selected(filter) {
            *out.entry(c.time_bucket).or_insert(0.0) += c.amount;
        }
        out
    }

    /// Net interest income over `horizon_days` (sum of interest cash flows).
    pub fn nii(&self, filter: &AlmFilter, horizon_days: i64) -> f64 {
        self.selected(filter)
            .filter(|c| c.cashflow_type == CashflowType::Interest && c.time_bucket <= horizon_days)
            .map(|c| c.amount)
            .sum()
    }

    /// Economic value of equity: PV of every cash flow.
    pub fn eve(&self, filter: &AlmFilter, curve: &DiscountCurve) -> f64 {
        self.selected(filter)
            .map(|c| {
                let df = c.discount_factor.unwrap_or_else(|| curve.df(c.time_bucket));
                c.amount * df
            })
            .sum()
    }

    /// Repricing gap per bucket defined by `edges` (ascending). A cell falls
    /// into `[edges[i], edges[i+1])`; the final edge is open-ended.
    /// Only `Principal` notional cells participate.
    pub fn repricing_gap(&self, filter: &AlmFilter, edges: &[i64]) -> Vec<(i64, f64)> {
        let mut buckets: Vec<(i64, f64)> = edges.iter().map(|&e| (e, 0.0)).collect();
        if buckets.is_empty() {
            return buckets;
        }
        for c in self.selected(filter) {
            if c.cashflow_type != CashflowType::Principal {
                continue;
            }
            let idx = match edges.iter().rposition(|&e| e <= c.repricing_date) {
                Some(i) => i,
                None => continue, // reprices before the first edge
            };
            buckets[idx].1 += c.amount;
        }
        buckets
    }

    /// Optionality charge: sum of `Optionality` cash flows.
    pub fn optionality_charge(&self, filter: &AlmFilter) -> f64 {
        self.selected(filter)
            .filter(|c| c.cashflow_type == CashflowType::Optionality)
            .map(|c| c.amount)
            .sum()
    }

    /// Cumulative stressed liquidity gap per bucket.
    ///
    /// * deposits run off by `stress.deposit_runoff`: the cash flow moves
    ///   further in the outflow direction (a positive deposit inflow shrinks,
    ///   a negative repayment outflow grows),
    /// * other outflows are multiplied by `1 + stress.wholesale_outflow`,
    /// * other inflows are haircut by `stress.inflow_haircut`.
    pub fn liquidity_stress(
        &self,
        filter: &AlmFilter,
        stress: &LiquidityStress,
    ) -> Vec<(i64, f64)> {
        let mut per_bucket: BTreeMap<i64, f64> = BTreeMap::new();
        for c in self.selected(filter) {
            let stressed = if c.cashflow_type == CashflowType::Deposit {
                c.amount - c.amount.abs() * stress.deposit_runoff
            } else if !c.is_inflow() {
                c.amount * (1.0 + stress.wholesale_outflow)
            } else {
                c.amount * (1.0 - stress.inflow_haircut)
            };
            *per_bucket.entry(c.time_bucket).or_insert(0.0) += stressed;
        }
        let mut running = 0.0;
        per_bucket
            .into_iter()
            .map(|(bucket, v)| {
                running += v;
                (bucket, running)
            })
            .collect()
    }

    /// Replace each `Deposit` balance with a decay schedule.
    ///
    /// `decay.survival[i]` is the fraction of the balance still present at
    /// period `i`; the runoff in period `i` is
    /// `balance * (survival[i-1] - survival[i])` (with `survival[-1] = 1`) and
    /// is emitted as a `Deposit` cash flow `period_days * (i+1)` days out.
    pub fn deposit_decay(&self, filter: &AlmFilter, decay: &DepositDecay) -> AlmCube {
        let mut out = Vec::new();
        for c in self.selected(filter) {
            if c.cashflow_type != CashflowType::Deposit {
                out.push(c.clone());
                continue;
            }
            let mut previous = 1.0;
            for (i, &survival) in decay.survival.iter().enumerate() {
                let runoff = (previous - survival).max(0.0) * c.amount;
                previous = survival;
                if runoff != 0.0 {
                    let mut cell = c.clone();
                    cell.time_bucket = c.time_bucket + decay.period_days * (i as i64 + 1);
                    cell.amount = runoff;
                    out.push(cell);
                }
            }
        }
        AlmCube::from_cells(out)
    }

    /// Apply a single-period prepayment rate to `Principal` inflows: `smm` of
    /// each principal cash flow is pulled forward by `shift_days`.
    pub fn prepayment(&self, filter: &AlmFilter, model: &PrepaymentModel) -> AlmCube {
        let mut out = Vec::new();
        for c in self.selected(filter) {
            if c.cashflow_type != CashflowType::Principal || c.amount < 0.0 {
                out.push(c.clone());
                continue;
            }
            let prepaid = c.amount * model.smm;
            if prepaid > 0.0 {
                let mut kept = c.clone();
                kept.amount = c.amount - prepaid;
                out.push(kept);

                let mut moved = c.clone();
                moved.cashflow_type = CashflowType::Prepayment;
                moved.amount = prepaid;
                moved.time_bucket = (c.time_bucket - model.shift_days).max(0);
                moved.repricing_date = moved.time_bucket;
                out.push(moved);
            } else {
                out.push(c.clone());
            }
        }
        AlmCube::from_cells(out)
    }

    /// Convert every cell to `base` and sum the signed amounts.
    pub fn aggregate_currency(&self, filter: &AlmFilter, fx: &FxTable, base: &str) -> f64 {
        self.selected(filter)
            .map(|c| c.amount * fx.rate(&c.currency, base))
            .sum()
    }

    /// Convert every cell to `base` and return per-currency subtotals.
    pub fn currency_breakdown(
        &self,
        filter: &AlmFilter,
        fx: &FxTable,
        base: &str,
    ) -> BTreeMap<String, f64> {
        let mut out = BTreeMap::new();
        for c in self.selected(filter) {
            *out.entry(c.currency.clone()).or_insert(0.0) +=
                c.amount * fx.rate(&c.currency, base);
        }
        out
    }
}

// ---------------------------------------------------------------------------
// Curve / stress / assumption helpers
// ---------------------------------------------------------------------------

/// Continuously-compounded zero curve, piecewise-linear in `(days, rate)` with
/// flat extrapolation.
#[derive(Debug, Clone, PartialEq)]
pub struct DiscountCurve {
    points: Vec<(i64, f64)>,
}

impl DiscountCurve {
    pub fn from_zero_rates(mut points: Vec<(i64, f64)>) -> Result<Self, AlmError> {
        if points.is_empty() {
            return Err(AlmError::EmptyCurve);
        }
        if points.iter().any(|(d, r)| *d < 0 || !r.is_finite()) {
            return Err(AlmError::InvalidCurve);
        }
        points.sort_by_key(|(d, _)| *d);
        for w in points.windows(2) {
            if w[0].0 == w[1].0 {
                return Err(AlmError::DuplicateTenor(w[0].0));
            }
        }
        Ok(Self { points })
    }

    /// Flat curve at `rate` for all tenors.
    pub fn flat(rate: f64) -> Self {
        Self {
            points: vec![(0, rate)],
        }
    }

    /// Zero rate at `days` (flat extrapolation at both ends).
    pub fn zero_rate(&self, days: i64) -> f64 {
        let days = days.max(0);
        let first = self.points[0];
        if days <= first.0 {
            return first.1;
        }
        let last = *self.points.last().unwrap();
        if days >= last.0 {
            return last.1;
        }
        for w in self.points.windows(2) {
            let (d0, r0) = w[0];
            let (d1, r1) = w[1];
            if days >= d0 && days <= d1 {
                let t = (days - d0) as f64 / (d1 - d0) as f64;
                return r0 + (r1 - r0) * t;
            }
        }
        last.1
    }

    /// Discount factor at `days` using ACT/365 continuous compounding.
    pub fn df(&self, days: i64) -> f64 {
        let days = days.max(0);
        E.powf(-self.zero_rate(days) * days as f64 / 365.0)
    }
}

/// Liquidity stress parameters.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LiquidityStress {
    /// Fraction of deposit inflow that runs off (`0.0..=1.0`).
    pub deposit_runoff: f64,
    /// Extra fraction applied to outflows (e.g. `0.25` = +25% outflow).
    pub wholesale_outflow: f64,
    /// Haircut applied to non-deposit inflows (`0.0..=1.0`).
    pub inflow_haircut: f64,
}

impl Default for LiquidityStress {
    fn default() -> Self {
        Self {
            deposit_runoff: 0.10,
            wholesale_outflow: 0.0,
            inflow_haircut: 0.0,
        }
    }
}

/// Deposit-decay assumption.
#[derive(Debug, Clone, PartialEq)]
pub struct DepositDecay {
    /// Survival fraction at the end of each period; must be non-increasing and
    /// start at `<= 1.0`, typically ending at `0.0`.
    pub survival: Vec<f64>,
    pub period_days: i64,
}

impl DepositDecay {
    pub fn new(survival: Vec<f64>, period_days: i64) -> Result<Self, AlmError> {
        if survival.is_empty() || period_days <= 0 {
            return Err(AlmError::InvalidDecay);
        }
        if survival.iter().any(|s| !s.is_finite() || *s < 0.0 || *s > 1.0) {
            return Err(AlmError::InvalidDecay);
        }
        if survival.windows(2).any(|w| w[1] > w[0]) {
            return Err(AlmError::InvalidDecay);
        }
        Ok(Self {
            survival,
            period_days,
        })
    }

    /// Geometric survival profile: `(1 - monthly_runoff)^periods`, monthly.
    pub fn from_monthly_runoff(monthly_runoff: f64, periods: usize) -> Self {
        let survival = (0..periods)
            .map(|i| (1.0 - monthly_runoff).powi(i as i32 + 1))
            .collect();
        Self {
            survival,
            period_days: 30,
        }
    }
}

/// Constant single-period prepayment model.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PrepaymentModel {
    /// Single-period mortality rate (`0.0..=1.0`).
    pub smm: f64,
    /// How many days earlier the prepaid amount lands.
    pub shift_days: i64,
}

impl PrepaymentModel {
    pub fn new(smm: f64, shift_days: i64) -> Result<Self, AlmError> {
        if !(0.0..=1.0).contains(&smm) || shift_days < 0 {
            return Err(AlmError::InvalidPrepayment);
        }
        Ok(Self { smm, shift_days })
    }
}

/// FX rate table expressed as base units per one unit of the quoted currency.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FxTable {
    rates: BTreeMap<(String, String), f64>,
}

impl FxTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record `1 from = rate to`.
    pub fn set(&mut self, from: impl Into<String>, to: impl Into<String>, rate: f64) -> Result<(), AlmError> {
        let from = from.into();
        let to = to.into();
        if !rate.is_finite() || rate <= 0.0 {
            return Err(AlmError::InvalidFxRate(from, to));
        }
        self.rates.insert((from, to), rate);
        Ok(())
    }

    /// Conversion factor from `from` to `to` (identity when equal).
    pub fn rate(&self, from: &str, to: &str) -> f64 {
        if from == to {
            return 1.0;
        }
        if let Some(r) = self.rates.get(&(from.to_string(), to.to_string())) {
            return *r;
        }
        if let Some(r) = self.rates.get(&(to.to_string(), from.to_string())) {
            return 1.0 / *r;
        }
        1.0
    }
}

/// Errors from curve / assumption validation.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AlmError {
    #[error("discount curve must have at least one point")]
    EmptyCurve,
    #[error("discount curve contains a negative tenor or non-finite rate")]
    InvalidCurve,
    #[error("discount curve has duplicate tenor {0}")]
    DuplicateTenor(i64),
    #[error("deposit-decay survival profile must be non-empty, in [0,1] and non-increasing")]
    InvalidDecay,
    #[error("prepayment smm must be in [0,1] and shift_days >= 0")]
    InvalidPrepayment,
    #[error("invalid FX rate {0} -> {1}")]
    InvalidFxRate(String, String),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cube() -> AlmCube {
        let mut c = AlmCube::new();
        // Assets: 1000 principal at 90d, 5% interest annually at 365d
        c.push(
            AlmCell::new("base", "LE1", "USD", "Loan", 90, CashflowType::Principal, 1000.0)
                .with_repricing(90),
        );
        c.push(
            AlmCell::new("base", "LE1", "USD", "Loan", 365, CashflowType::Interest, 50.0)
                .with_repricing(365),
        );
        // Liability: 800 deposit at 30d
        c.push(
            AlmCell::new("base", "LE1", "USD", "Deposit", 30, CashflowType::Deposit, -800.0)
                .with_repricing(30),
        );
        c
    }

    #[test]
    fn cashflow_ladder_is_net_per_bucket() {
        let ladder = cube().cashflow_ladder(&AlmFilter::default());
        assert_eq!(ladder[&30], -800.0);
        assert_eq!(ladder[&90], 1000.0);
        assert_eq!(ladder[&365], 50.0);
    }

    #[test]
    fn nii_respects_the_horizon() {
        let c = cube();
        assert_eq!(c.nii(&AlmFilter::default(), 100), 0.0);
        assert_eq!(c.nii(&AlmFilter::default(), 365), 50.0);
    }

    #[test]
    fn eve_uses_the_curve_and_explicit_overrides() {
        let curve = DiscountCurve::flat(0.05);
        let c = cube();
        let expected: f64 = c
            .cells()
            .iter()
            .map(|cell| cell.amount * curve.df(cell.time_bucket))
            .sum();
        assert!((c.eve(&AlmFilter::default(), &curve) - expected).abs() < 1e-12);

        let mut with_override = AlmCube::new();
        with_override.push(
            AlmCell::new("base", "LE1", "USD", "X", 100, CashflowType::Other, 100.0)
                .with_discount_factor(0.9),
        );
        assert!((with_override.eve(&AlmFilter::default(), &curve) - 90.0).abs() < 1e-12);
    }

    #[test]
    fn repricing_gap_buckets_principal_by_repricing_date() {
        let c = cube();
        let gap = c.repricing_gap(&AlmFilter::default(), &[0, 60, 180, 400]);
        assert_eq!(gap, vec![(0, 0.0), (60, 1000.0), (180, 0.0), (400, 0.0)]);
    }

    #[test]
    fn liquidity_stress_applies_haircuts_and_is_cumulative() {
        let c = cube();
        let stress = LiquidityStress {
            deposit_runoff: 0.25,
            wholesale_outflow: 0.0,
            inflow_haircut: 0.10,
        };
        let out = c.liquidity_stress(&AlmFilter::default(), &stress);
        // deposit repayment -800 grows to -1000 (runoff moves it outward)
        assert!((out[0].1 - (-1000.0)).abs() < 1e-9);
        // principal inflow haircut: 1000 * 0.9 = 900 -> cumulative -100
        assert!((out[1].1 - (-100.0)).abs() < 1e-9);
        // interest 50 * 0.9 = 45 -> cumulative -55
        assert!((out[2].1 - (-55.0)).abs() < 1e-9);
    }

    #[test]
    fn deposit_decay_conserves_the_balance() {
        let c = cube();
        let decay = DepositDecay::new(vec![0.7, 0.4, 0.0], 30).unwrap();
        let decayed = c.deposit_decay(&AlmFilter::default(), &decay);
        let deposits: f64 = decayed
            .cells()
            .iter()
            .filter(|c| c.cashflow_type == CashflowType::Deposit)
            .map(|c| c.amount)
            .sum();
        // -800 * (1 - 0) = -800 spread across three periods
        assert!((deposits - (-800.0)).abs() < 1e-9);
        let buckets: Vec<i64> = decayed
            .cells()
            .iter()
            .filter(|c| c.cashflow_type == CashflowType::Deposit)
            .map(|c| c.time_bucket)
            .collect();
        assert_eq!(buckets, vec![60, 90, 120]);
    }

    #[test]
    fn prepayment_pulls_a_fraction_forward() {
        let c = cube();
        let model = PrepaymentModel::new(0.20, 30).unwrap();
        let adjusted = c.prepayment(&AlmFilter::default(), &model);
        let kept: f64 = adjusted
            .cells()
            .iter()
            .filter(|c| c.cashflow_type == CashflowType::Principal)
            .map(|c| c.amount)
            .sum();
        let prepaid: f64 = adjusted
            .cells()
            .iter()
            .filter(|c| c.cashflow_type == CashflowType::Prepayment)
            .map(|c| c.amount)
            .sum();
        assert!((kept - 800.0).abs() < 1e-9);
        assert!((prepaid - 200.0).abs() < 1e-9);
        let prepaid_bucket = adjusted
            .cells()
            .iter()
            .find(|c| c.cashflow_type == CashflowType::Prepayment)
            .unwrap()
            .time_bucket;
        assert_eq!(prepaid_bucket, 60);
    }

    #[test]
    fn multi_currency_aggregation_uses_inverse_rates() {
        let mut c = AlmCube::new();
        c.push(AlmCell::new(
            "base",
            "LE1",
            "USD",
            "Loan",
            90,
            CashflowType::Principal,
            100.0,
        ));
        c.push(AlmCell::new(
            "base",
            "LE1",
            "HKD",
            "Loan",
            90,
            CashflowType::Principal,
            780.0,
        ));
        let mut fx = FxTable::new();
        fx.set("USD", "HKD", 7.8).unwrap();
        // 100 USD * 7.8 + 780 HKD = 1560 HKD
        assert!((c.aggregate_currency(&AlmFilter::default(), &fx, "HKD") - 1560.0).abs() < 1e-9);
        // inverse: 780 HKD / 7.8 + 100 USD = 200 USD
        assert!((c.aggregate_currency(&AlmFilter::default(), &fx, "USD") - 200.0).abs() < 1e-9);
        let breakdown = c.currency_breakdown(&AlmFilter::default(), &fx, "HKD");
        assert_eq!(breakdown["USD"], 780.0);
        assert_eq!(breakdown["HKD"], 780.0);
    }

    #[test]
    fn filters_isolate_dimensions() {
        let mut c = cube();
        c.push(AlmCell::new(
            "stress",
            "LE2",
            "EUR",
            "Loan",
            90,
            CashflowType::Principal,
            500.0,
        ));
        let base_usd = c.cashflow_ladder(&AlmFilter::default().scenario("base").currency("USD"));
        assert_eq!(base_usd.len(), 3);
        let stress = c.cashflow_ladder(&AlmFilter::default().scenario("stress"));
        assert_eq!(stress[&90], 500.0);
    }

    #[test]
    fn calculations_are_deterministic() {
        let c = cube();
        let curve = DiscountCurve::flat(0.03);
        assert_eq!(c.cashflow_ladder(&AlmFilter::default()), c.cashflow_ladder(&AlmFilter::default()));
        assert_eq!(c.eve(&AlmFilter::default(), &curve), c.eve(&AlmFilter::default(), &curve));
    }

    #[test]
    fn curve_interpolates_and_validates() {
        let curve = DiscountCurve::from_zero_rates(vec![(0, 0.01), (365, 0.05)]).unwrap();
        assert!((curve.zero_rate(0) - 0.01).abs() < 1e-12);
        assert!((curve.zero_rate(365) - 0.05).abs() < 1e-12);
        let mid = curve.zero_rate(183);
        assert!(mid > 0.01 && mid < 0.05);
        assert!((curve.zero_rate(1000) - 0.05).abs() < 1e-12);
        assert!(matches!(
            DiscountCurve::from_zero_rates(vec![]),
            Err(AlmError::EmptyCurve)
        ));
        assert!(matches!(
            DiscountCurve::from_zero_rates(vec![(0, 0.01), (0, 0.02)]),
            Err(AlmError::DuplicateTenor(0))
        ));
    }
}
