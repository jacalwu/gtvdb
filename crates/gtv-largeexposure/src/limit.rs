//! Configurable limit engine (reporting thresholds vs regulatory limits).

use crate::config::LeConfig;

/// Severity of a limit evaluation (`Ok < Warn < Reportable < Breach`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum LimitStatus {
    Ok,
    Warn,
    Reportable,
    Breach,
}

impl LimitStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            LimitStatus::Ok => "ok",
            LimitStatus::Warn => "warn",
            LimitStatus::Reportable => "reportable",
            LimitStatus::Breach => "breach",
        }
    }
}

/// What a limit applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum LimitMetric {
    SingleCounterparty,
    LcGroup,
    ConnectedParty,
    Sector,
    Country,
    Rating,
    Intragroup,
}

impl LimitMetric {
    pub fn as_str(self) -> &'static str {
        match self {
            LimitMetric::SingleCounterparty => "single_counterparty",
            LimitMetric::LcGroup => "lc_group",
            LimitMetric::ConnectedParty => "connected_party",
            LimitMetric::Sector => "sector",
            LimitMetric::Country => "country",
            LimitMetric::Rating => "rating",
            LimitMetric::Intragroup => "intragroup",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().replace([' ', '-'], "_").as_str() {
            "single_counterparty" | "counterparty" => Some(LimitMetric::SingleCounterparty),
            "lc_group" | "group" => Some(LimitMetric::LcGroup),
            "connected_party" | "connected" => Some(LimitMetric::ConnectedParty),
            "sector" => Some(LimitMetric::Sector),
            "country" => Some(LimitMetric::Country),
            "rating" => Some(LimitMetric::Rating),
            "intragroup" => Some(LimitMetric::Intragroup),
            _ => None,
        }
    }
}

/// One configurable limit rule.
#[derive(Debug, Clone, PartialEq)]
pub struct LimitRule {
    pub limit_id: String,
    pub metric: LimitMetric,
    /// Optional key (`None` = the metric default `'*'`).
    pub key: Option<String>,
    /// Reporting threshold (e.g. 0.10 for a large exposure, 0.05 for connected).
    pub report_threshold: f64,
    /// Internal warning level.
    pub warn_ratio: f64,
    /// Regulatory / internal hard limit.
    pub limit_ratio: f64,
    /// Optional top-N override.
    pub top_n: Option<usize>,
    /// Denominator basis (`tier1`).
    pub applied_to: String,
}

impl LimitRule {
    pub fn new(limit_id: impl Into<String>, metric: LimitMetric, limit_ratio: f64) -> Self {
        Self {
            limit_id: limit_id.into(),
            metric,
            key: None,
            report_threshold: 0.10,
            warn_ratio: 0.20,
            limit_ratio,
            top_n: None,
            applied_to: "tier1".to_string(),
        }
    }

    pub fn with_report_threshold(mut self, v: f64) -> Self {
        self.report_threshold = v;
        self
    }

    pub fn with_warn_ratio(mut self, v: f64) -> Self {
        self.warn_ratio = v;
        self
    }

    pub fn with_key(mut self, v: impl Into<String>) -> Self {
        self.key = Some(v.into());
        self
    }

    pub fn with_top_n(mut self, n: usize) -> Self {
        self.top_n = Some(n);
        self
    }

    /// Evaluate an exposure against the rule.
    pub fn evaluate(&self, exposure: f64, tier1: f64) -> LimitOutcome {
        let ratio = if tier1 > 0.0 {
            exposure / tier1
        } else if exposure > 0.0 {
            f64::INFINITY
        } else {
            0.0
        };
        let status = if ratio >= self.limit_ratio {
            LimitStatus::Breach
        } else if ratio >= self.report_threshold {
            LimitStatus::Reportable
        } else if ratio >= self.warn_ratio {
            LimitStatus::Warn
        } else {
            LimitStatus::Ok
        };
        LimitOutcome {
            status,
            ratio,
            headroom: self.limit_ratio * tier1 - exposure,
        }
    }
}

/// The result of evaluating one limit.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LimitOutcome {
    pub status: LimitStatus,
    pub ratio: f64,
    pub headroom: f64,
}

/// A set of limit rules.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct LimitSet {
    rules: Vec<LimitRule>,
}

impl LimitSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// The shipped regulatory default set (configurable; `le_limit_set` rows).
    pub fn regulatory_defaults(cfg: &LeConfig) -> Self {
        let mut s = Self::new();
        s.add(
            LimitRule::new("le_lc_group", LimitMetric::LcGroup, cfg.limit_ratio)
                .with_report_threshold(cfg.report_threshold)
                .with_warn_ratio(cfg.warn_ratio)
                .with_top_n(cfg.default_top_n),
        );
        s.add(
            LimitRule::new(
                "le_single_counterparty",
                LimitMetric::SingleCounterparty,
                cfg.limit_ratio,
            )
            .with_report_threshold(cfg.report_threshold)
            .with_warn_ratio(cfg.warn_ratio)
            .with_top_n(cfg.default_top_n),
        );
        s.add(
            LimitRule::new("g_sib_to_g_sib", LimitMetric::LcGroup, cfg.g_sib_limit)
                .with_report_threshold(cfg.report_threshold)
                .with_warn_ratio(cfg.warn_ratio)
                .with_key("g_sib"),
        );
        s.add(
            LimitRule::new(
                "connected_party",
                LimitMetric::ConnectedParty,
                cfg.limit_ratio,
            )
            .with_report_threshold(cfg.connected_report_threshold)
            .with_warn_ratio(cfg.warn_ratio),
        );
        s.add(
            LimitRule::new("intragroup", LimitMetric::Intragroup, cfg.limit_ratio)
                .with_report_threshold(cfg.connected_report_threshold)
                .with_warn_ratio(cfg.warn_ratio),
        );
        s
    }

    pub fn add(&mut self, rule: LimitRule) {
        self.rules.push(rule);
    }

    pub fn rules(&self) -> &[LimitRule] {
        &self.rules
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    pub fn for_metric(&self, metric: LimitMetric) -> impl Iterator<Item = &LimitRule> {
        self.rules.iter().filter(move |r| r.metric == metric)
    }

    /// Evaluate `exposure` against every rule (returns rule id + outcome).
    pub fn evaluate_all(&self, exposure: f64, tier1: f64) -> Vec<(&str, LimitOutcome)> {
        self.rules
            .iter()
            .map(|r| (r.limit_id.as_str(), r.evaluate(exposure, tier1)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_thresholds_and_headroom() {
        let rule = LimitRule::new("r", LimitMetric::LcGroup, 0.25)
            .with_report_threshold(0.10)
            .with_warn_ratio(0.20);
        let o = rule.evaluate(5.0, 100.0); // 5%
        assert_eq!(o.status, LimitStatus::Ok);
        assert_eq!(o.headroom, 20.0);
        assert_eq!(rule.evaluate(15.0, 100.0).status, LimitStatus::Reportable);
        assert_eq!(rule.evaluate(22.0, 100.0).status, LimitStatus::Reportable);
        assert_eq!(rule.evaluate(25.0, 100.0).status, LimitStatus::Breach);
        assert_eq!(rule.evaluate(30.0, 100.0).status, LimitStatus::Breach);
        assert!(rule.evaluate(30.0, 100.0).headroom < 0.0);
    }

    #[test]
    fn warn_below_report_threshold_is_used_when_configured() {
        // warn 0.30 > report 0.10: a 0.32 exposure satisfies both, but
        // Reportable outranks Warn.
        let rule = LimitRule::new("r", LimitMetric::Sector, 0.50)
            .with_report_threshold(0.10)
            .with_warn_ratio(0.30);
        assert_eq!(rule.evaluate(32.0, 100.0).status, LimitStatus::Reportable);
        // below report but above warn -> Warn
        let rule2 = LimitRule::new("r", LimitMetric::Sector, 0.50)
            .with_report_threshold(0.40)
            .with_warn_ratio(0.30);
        assert_eq!(rule2.evaluate(35.0, 100.0).status, LimitStatus::Warn);
    }

    #[test]
    fn regulatory_defaults() {
        let cfg = LeConfig::default();
        let s = LimitSet::regulatory_defaults(&cfg);
        assert_eq!(s.rules().len(), 5);
        assert_eq!(s.for_metric(LimitMetric::LcGroup).count(), 2); // le + g-sib
        let o = s
            .evaluate_all(30.0, 100.0)
            .into_iter()
            .find(|(id, _)| *id == "le_lc_group")
            .unwrap()
            .1;
        assert_eq!(o.status, LimitStatus::Breach);
        assert_eq!(o.ratio, 0.30);
    }
}
