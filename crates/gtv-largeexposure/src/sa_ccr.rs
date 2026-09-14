//! SA-CCR — Standardised Approach for Counterparty Credit Risk (LE-6).
//!
//! Computes the default-risk exposure of derivative netting sets:
//!
//! ```text
//! EAD = α · (RC + PFE)
//! RC  = max(V − C, TH + MTA − NICA, 0)          (margined)
//!     = max(V − C, 0)                            (unmargined)
//! PFE = multiplier · AddOn_aggregate
//! AddOn_aggregate = Σ_{asset class, hedging set} SF · |effective notional|
//! effective notional = Σ direction · delta · adjusted notional · MF
//! ```
//!
//! Simplifications (documented): hedging sets are netted additively within
//! `(asset class, hedging_set)` — the full CRE52 maturity-bucket / correlation
//! aggregation for interest rate and credit is not implemented; supervisory
//! factors are per asset class (rating-specific credit factors to follow).

use std::collections::BTreeMap;

use crate::exposure::{ExposureEvent, ExposureMeasure};

/// Derivative asset class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum AssetClass {
    InterestRate,
    Fx,
    Credit,
    Equity,
    Commodity,
}

impl AssetClass {
    pub fn as_str(self) -> &'static str {
        match self {
            AssetClass::InterestRate => "interest_rate",
            AssetClass::Fx => "fx",
            AssetClass::Credit => "credit",
            AssetClass::Equity => "equity",
            AssetClass::Commodity => "commodity",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().replace([' ', '-'], "_").as_str() {
            "interest_rate" | "ir" | "rates" => Some(AssetClass::InterestRate),
            "fx" | "foreign_exchange" => Some(AssetClass::Fx),
            "credit" => Some(AssetClass::Credit),
            "equity" => Some(AssetClass::Equity),
            "commodity" => Some(AssetClass::Commodity),
            _ => None,
        }
    }
}

/// One derivative instrument in a netting set.
#[derive(Debug, Clone, PartialEq)]
pub struct DerivativeInstrument {
    pub instrument_id: String,
    pub asset_class: AssetClass,
    /// Hedging set key (currency for IR/FX, entity for credit/equity, type for commodity).
    pub hedging_set: String,
    pub notional: f64,
    /// Start of the transaction, in years from the measurement date.
    pub start_years: f64,
    /// End (maturity), in years from the measurement date.
    pub end_years: f64,
    /// +1 long, −1 short.
    pub direction: i8,
    /// Option delta (1.0 for linear instruments).
    pub delta: f64,
}

impl DerivativeInstrument {
    pub fn linear(
        instrument_id: impl Into<String>,
        asset_class: AssetClass,
        hedging_set: impl Into<String>,
        notional: f64,
        start_years: f64,
        end_years: f64,
        direction: i8,
    ) -> Self {
        Self {
            instrument_id: instrument_id.into(),
            asset_class,
            hedging_set: hedging_set.into(),
            notional,
            start_years,
            end_years,
            direction,
            delta: 1.0,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn option(
        instrument_id: impl Into<String>,
        asset_class: AssetClass,
        hedging_set: impl Into<String>,
        notional: f64,
        start_years: f64,
        end_years: f64,
        direction: i8,
        delta: f64,
    ) -> Self {
        Self {
            delta,
            ..Self::linear(
                instrument_id,
                asset_class,
                hedging_set,
                notional,
                start_years,
                end_years,
                direction,
            )
        }
    }

    /// Supervisory duration (IR / credit): `(e^{-0.05 S} − e^{-0.05 E}) / 0.05`.
    pub fn supervisory_duration(&self) -> f64 {
        let s = self.start_years.max(0.0);
        let e = self.end_years.max(0.0);
        if e <= s {
            return 0.0;
        }
        ((-0.05 * s).exp() - (-0.05 * e).exp()) / 0.05
    }

    /// Adjusted notional: `notional × SD` for IR/credit, `notional` otherwise.
    pub fn adjusted_notional(&self) -> f64 {
        match self.asset_class {
            AssetClass::InterestRate | AssetClass::Credit => {
                self.notional * self.supervisory_duration()
            }
            _ => self.notional,
        }
    }

    /// Unmargined maturity factor: `sqrt(min(M, 1))`.
    pub fn unmargined_maturity_factor(&self) -> f64 {
        self.end_years.clamp(0.0, 1.0).sqrt()
    }
}

/// A derivative netting set with collateral terms.
#[derive(Debug, Clone, PartialEq)]
pub struct NettingSet {
    pub netting_set_id: String,
    pub instruments: Vec<DerivativeInstrument>,
    /// Current market value of the netting set (V), positive = in the bank's favour.
    pub market_value: f64,
    /// Collateral currently held (C).
    pub collateral: f64,
    /// Threshold (TH).
    pub threshold: f64,
    /// Minimum transfer amount (MTA).
    pub mta: f64,
    /// Net independent collateral amount (NICA).
    pub nica: f64,
    pub margined: bool,
    /// Margin period of risk in business days (≥ 10 when margined).
    pub mpor_days: u32,
}

impl NettingSet {
    pub fn new(netting_set_id: impl Into<String>) -> Self {
        Self {
            netting_set_id: netting_set_id.into(),
            instruments: Vec::new(),
            market_value: 0.0,
            collateral: 0.0,
            threshold: 0.0,
            mta: 0.0,
            nica: 0.0,
            margined: false,
            mpor_days: 10,
        }
    }

    pub fn with_instrument(mut self, i: DerivativeInstrument) -> Self {
        self.instruments.push(i);
        self
    }

    pub fn with_market_value(mut self, v: f64) -> Self {
        self.market_value = v;
        self
    }

    pub fn with_collateral(mut self, c: f64) -> Self {
        self.collateral = c;
        self
    }

    pub fn margined(mut self, threshold: f64, mta: f64, nica: f64, mpor_days: u32) -> Self {
        self.margined = true;
        self.threshold = threshold;
        self.mta = mta;
        self.nica = nica;
        self.mpor_days = mpor_days;
        self
    }

    /// Maturity factor: margined `1.5·sqrt(MPOR/1y)` (MPOR ≥ 10 business days),
    /// otherwise the instrument's unmargined factor.
    pub fn maturity_factor(&self, i: &DerivativeInstrument) -> f64 {
        if self.margined {
            let mpor = self.mpor_days.max(10) as f64;
            1.5 * (mpor / 250.0).sqrt()
        } else {
            i.unmargined_maturity_factor()
        }
    }
}

/// SA-CCR parameters (configurable; defaults = CRE52).
#[derive(Debug, Clone, PartialEq)]
pub struct SaCcrConfig {
    pub alpha: f64,
    pub multiplier_floor: f64,
    pub supervisory_factors: BTreeMap<AssetClass, f64>,
}

impl Default for SaCcrConfig {
    fn default() -> Self {
        Self {
            alpha: 1.4,
            multiplier_floor: 0.05,
            supervisory_factors: [
                (AssetClass::InterestRate, 0.005),
                (AssetClass::Fx, 0.04),
                (AssetClass::Credit, 0.0054),
                (AssetClass::Equity, 0.32),
                (AssetClass::Commodity, 0.18),
            ]
            .into_iter()
            .collect(),
        }
    }
}

impl SaCcrConfig {
    pub fn supervisory_factor(&self, class: AssetClass) -> f64 {
        self.supervisory_factors.get(&class).copied().unwrap_or(0.0)
    }
}

/// SA-CCR exposure result.
#[derive(Debug, Clone, PartialEq)]
pub struct SaCcrResult {
    pub replacement_cost: f64,
    pub add_on: f64,
    pub multiplier: f64,
    pub pfe: f64,
    pub ead: f64,
}

/// Compute the SA-CCR default-risk exposure of one netting set.
pub fn sa_ccr(netting_set: &NettingSet, cfg: &SaCcrConfig) -> SaCcrResult {
    // effective notional per (asset class, hedging set)
    let mut effective: BTreeMap<(AssetClass, String), f64> = BTreeMap::new();
    for i in &netting_set.instruments {
        let contribution = (i.direction as f64)
            * i.delta
            * i.adjusted_notional()
            * netting_set.maturity_factor(i);
        *effective
            .entry((i.asset_class, i.hedging_set.clone()))
            .or_insert(0.0) += contribution;
    }
    let add_on: f64 = effective
        .iter()
        .map(|((class, _), v)| cfg.supervisory_factor(*class) * v.abs())
        .sum();

    // replacement cost
    let v_minus_c = netting_set.market_value - netting_set.collateral;
    let replacement_cost = if netting_set.margined {
        v_minus_c.max(netting_set.threshold + netting_set.mta - netting_set.nica).max(0.0)
    } else {
        v_minus_c.max(0.0)
    };

    // multiplier
    let floor = cfg.multiplier_floor;
    let multiplier = if add_on <= 0.0 {
        1.0
    } else {
        let exponent = v_minus_c / (2.0 * (1.0 - floor) * add_on);
        (floor + (1.0 - floor) * exponent.exp()).min(1.0)
    };
    let pfe = multiplier * add_on;
    SaCcrResult {
        replacement_cost,
        add_on,
        multiplier,
        pfe,
        ead: cfg.alpha * (replacement_cost + pfe),
    }
}

/// Build an exposure event whose `default_risk` component is the SA-CCR EAD.
pub fn to_exposure_event(
    netting_set: &NettingSet,
    cfg: &SaCcrConfig,
    event_id: impl Into<String>,
    entity_id: impl Into<String>,
    currency: impl Into<String>,
    business_from: i64,
    business_to: i64,
) -> ExposureEvent {
    let ead = sa_ccr(netting_set, cfg).ead;
    ExposureEvent::new(
        event_id,
        entity_id,
        ExposureMeasure::zero().default_risk(ead),
        business_from,
        business_to,
    )
    .with_currency(currency)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ir_swap(notional: f64, direction: i8) -> DerivativeInstrument {
        DerivativeInstrument::linear(
            "s",
            AssetClass::InterestRate,
            "USD",
            notional,
            0.0,
            10.0,
            direction,
        )
    }

    #[test]
    fn unmargined_ir_swap_matches_hand_calc() {
        let ns = NettingSet::new("NS").with_instrument(ir_swap(100.0, 1));
        let r = sa_ccr(&ns, &SaCcrConfig::default());
        // SD = (1 - e^-0.5)/0.05 = 7.869; d = 786.9; MF = 1; AddOn = 0.005*d = 3.9347
        assert!((r.add_on - 3.9347).abs() < 1e-3, "add_on {}", r.add_on);
        assert_eq!(r.replacement_cost, 0.0);
        assert_eq!(r.multiplier, 1.0);
        assert!((r.ead - 1.4 * r.add_on).abs() < 1e-9);
    }

    #[test]
    fn offsetting_positions_net_within_a_hedging_set() {
        let ns = NettingSet::new("NS")
            .with_instrument(ir_swap(100.0, 1))
            .with_instrument(ir_swap(60.0, -1));
        let r = sa_ccr(&ns, &SaCcrConfig::default());
        // net notional 40 -> AddOn = 0.005 * 40 * SD
        let expected = 0.005 * 40.0 * ir_swap(1.0, 1).supervisory_duration();
        assert!((r.add_on - expected).abs() < 1e-6);
    }

    #[test]
    fn margined_replacement_cost_and_maturity_factor() {
        let ns = NettingSet::new("NS")
            .with_instrument(ir_swap(100.0, 1))
            .with_market_value(50.0)
            .with_collateral(0.0)
            .margined(10.0, 5.0, 0.0, 10);
        let r = sa_ccr(&ns, &SaCcrConfig::default());
        // RC = max(50 - 0, 10 + 5 - 0, 0) = 50
        assert_eq!(r.replacement_cost, 50.0);
        // margined MF = 1.5*sqrt(10/250) = 0.3 -> d*MF = 0.3 * SD * 100
        let expected_addon =
            0.005 * 100.0 * ir_swap(1.0, 1).supervisory_duration() * 0.3;
        assert!((r.add_on - expected_addon).abs() < 1e-6);
        // multiplier saturates at 1 when V − C is large versus the add-on
        assert!(r.multiplier <= 1.0 && r.multiplier >= 0.05);
        // ...and drops below 1 when the netting set is out-of-the-money
        let ns2 = NettingSet::new("NS")
            .with_instrument(ir_swap(100.0, 1))
            .with_market_value(-100.0)
            .margined(0.0, 0.0, 0.0, 10);
        let r2 = sa_ccr(&ns2, &SaCcrConfig::default());
        assert!(r2.multiplier < 1.0);
    }

    #[test]
    fn fx_and_option_delta() {
        let ns = NettingSet::new("NS").with_instrument(DerivativeInstrument::option(
            "o",
            AssetClass::Fx,
            "EURUSD",
            1_000_000.0,
            0.0,
            1.0,
            1,
            0.5,
        ));
        let r = sa_ccr(&ns, &SaCcrConfig::default());
        // d = 1e6 (FX), MF = 1, delta = 0.5 -> effective = 5e5; SF 4% -> AddOn 20000
        assert!((r.add_on - 20_000.0).abs() < 1e-6);
    }

    #[test]
    fn to_exposure_event_sets_default_risk() {
        let ns = NettingSet::new("NS").with_instrument(ir_swap(100.0, 1));
        let e = to_exposure_event(&ns, &SaCcrConfig::default(), "ev", "CP1", "USD", 0, 100);
        assert!(e.measure.default_risk > 0.0);
        assert_eq!(e.measure.on_balance, 0.0);
        assert_eq!(e.currency, "USD");
    }
}
