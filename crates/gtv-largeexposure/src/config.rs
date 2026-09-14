//! Configurable Large Exposure parameters.
//!
//! `Default` reproduces the regulatory (HKMA MA(BS)28 / BELR / BCBS LEX)
//! values; every field can be overridden from configuration tables
//! (`le_config`, `le_limit_set`, `le_ccf`, `le_fx`).

/// Where the Tier 1 denominator comes from (IR / MA(BS)28 §7(n)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier1Source {
    /// End of the previous quarter (local AIs).
    PrevQuarterEnd,
    /// Latest available (AIs incorporated outside Hong Kong).
    Latest,
}

impl Tier1Source {
    pub fn as_str(self) -> &'static str {
        match self {
            Tier1Source::PrevQuarterEnd => "prev_quarter_end",
            Tier1Source::Latest => "latest",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().replace([' ', '-'], "_").as_str() {
            "prev_quarter_end" | "previous_quarter_end" | "prev" => Some(Tier1Source::PrevQuarterEnd),
            "latest" => Some(Tier1Source::Latest),
            _ => None,
        }
    }
}

/// How derivative / SFT default-risk exposure is obtained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DerivativeMeasure {
    /// Caller supplies `default_risk` per event (first version).
    Provided,
    /// Compute via SA-CCR (deferred; LE-6).
    SaCcr,
}

impl DerivativeMeasure {
    pub fn as_str(self) -> &'static str {
        match self {
            DerivativeMeasure::Provided => "provided",
            DerivativeMeasure::SaCcr => "sa_ccr",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().replace([' ', '-'], "_").as_str() {
            "provided" | "caller" => Some(DerivativeMeasure::Provided),
            "sa_ccr" | "saccr" => Some(DerivativeMeasure::SaCcr),
            _ => None,
        }
    }
}

/// All configurable Large Exposure parameters.
#[derive(Debug, Clone, PartialEq)]
pub struct LeConfig {
    /// Control threshold for LC-group clustering (BELR rule 41 / BO control).
    pub control_threshold: f64,
    /// Include economic-dependence edges when clustering LC groups.
    pub include_economic_dependence: bool,
    /// Reporting currency (MA(BS)28: HKD).
    pub report_currency: String,
    /// Tier 1 denominator source.
    pub tier1_source: Tier1Source,
    /// Default `top_n` when a limit rule does not specify one.
    pub default_top_n: usize,
    /// Default warning ratio.
    pub warn_ratio: f64,
    /// General large-exposure limit (25% of Tier 1).
    pub limit_ratio: f64,
    /// G-SIB-to-G-SIB limit (15% of Tier 1).
    pub g_sib_limit: f64,
    /// Reporting threshold for "large exposure" (10%).
    pub report_threshold: f64,
    /// Reporting threshold for connected-party exposures (5%).
    pub connected_report_threshold: f64,
    /// Derivative / SFT default-risk source.
    pub derivative_measure: DerivativeMeasure,
}

impl Default for LeConfig {
    fn default() -> Self {
        Self {
            control_threshold: 0.50,
            include_economic_dependence: true,
            report_currency: "HKD".to_string(),
            tier1_source: Tier1Source::PrevQuarterEnd,
            default_top_n: 20,
            warn_ratio: 0.20,
            limit_ratio: 0.25,
            g_sib_limit: 0.15,
            report_threshold: 0.10,
            connected_report_threshold: 0.05,
            derivative_measure: DerivativeMeasure::Provided,
        }
    }
}

impl LeConfig {
    /// The applicable limit ratio for a pair of counterparties: the stricter of
    /// the general 25% and the G-SIB 15% when both sides are G-SIB.
    pub fn applicable_limit(&self, ai_is_g_sib: bool, cp_is_g_sib: bool) -> f64 {
        if ai_is_g_sib && cp_is_g_sib {
            self.g_sib_limit.min(self.limit_ratio)
        } else {
            self.limit_ratio
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_regulation() {
        let c = LeConfig::default();
        assert_eq!(c.control_threshold, 0.50);
        assert_eq!(c.report_currency, "HKD");
        assert_eq!(c.default_top_n, 20);
        assert_eq!(c.limit_ratio, 0.25);
        assert_eq!(c.g_sib_limit, 0.15);
        assert_eq!(c.report_threshold, 0.10);
        assert_eq!(c.connected_report_threshold, 0.05);
        assert_eq!(c.tier1_source, Tier1Source::PrevQuarterEnd);
        assert_eq!(c.derivative_measure, DerivativeMeasure::Provided);
    }

    #[test]
    fn g_sib_overlay_and_parsing() {
        let c = LeConfig::default();
        assert_eq!(c.applicable_limit(false, false), 0.25);
        assert_eq!(c.applicable_limit(true, true), 0.15);
        assert_eq!(c.applicable_limit(true, false), 0.25);
        assert_eq!(Tier1Source::parse("prev quarter end"), Some(Tier1Source::PrevQuarterEnd));
        assert_eq!(DerivativeMeasure::parse("SA-CCR"), Some(DerivativeMeasure::SaCcr));
    }
}
