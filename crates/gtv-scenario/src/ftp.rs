//! FTP (funds-transfer-pricing) curve engine (prod_p4 D5 / roadmap P2.5).
//!
//! Builds a fully itemised FTP rate from versioned inputs:
//!
//! ```text
//! ftp_rate = base_curve_rate(tenor)
//!          + liquidity_premium(product, tenor)
//!          + basis_spread(currency, tenor)
//!          + optionality_charge(product)
//!          + behavioural_adjustment(product)
//! ```
//!
//! Curve and policy versions are pinned per request, product attributes are
//! resolved through the D6 product hierarchy, and every term is returned as a
//! labelled [`FtpStep`] so a price is explainable end to end.

use std::collections::BTreeMap;

use gtv_refdata::{Hierarchy, HierarchyKind};
use thiserror::Error;

use crate::alm::DiscountCurve;

/// Piecewise-linear interpolation with flat extrapolation.
fn interpolate(points: &[(i64, f64)], x: i64) -> f64 {
    if points.is_empty() {
        return 0.0;
    }
    let first = points[0];
    if x <= first.0 {
        return first.1;
    }
    let last = *points.last().unwrap();
    if x >= last.0 {
        return last.1;
    }
    for w in points.windows(2) {
        let (x0, y0) = w[0];
        let (x1, y1) = w[1];
        if x >= x0 && x <= x1 {
            let t = (x - x0) as f64 / (x1 - x0) as f64;
            return y0 + (y1 - y0) * t;
        }
    }
    last.1
}

/// One versioned base curve (zero rates by tenor in days).
#[derive(Debug, Clone, PartialEq)]
pub struct FtpCurve {
    pub id: String,
    pub version: u32,
    pub currency: String,
    pub effective_from: i64,
    pub effective_to: i64,
    pub curve: DiscountCurve,
}

impl FtpCurve {
    pub fn new(
        id: impl Into<String>,
        version: u32,
        currency: impl Into<String>,
        effective_from: i64,
        effective_to: i64,
        tenors: Vec<(i64, f64)>,
    ) -> Result<Self, FtpError> {
        if effective_from >= effective_to {
            return Err(FtpError::InvalidInterval);
        }
        let curve = DiscountCurve::from_zero_rates(tenors).map_err(|e| FtpError::Curve(e.to_string()))?;
        Ok(Self {
            id: id.into(),
            version,
            currency: currency.into(),
            effective_from,
            effective_to,
            curve,
        })
    }

    #[inline]
    pub fn zero_rate(&self, days: i64) -> f64 {
        self.curve.zero_rate(days)
    }

    #[inline]
    pub fn active_at(&self, as_of: i64) -> bool {
        self.effective_from <= as_of && as_of < self.effective_to
    }
}

/// Append-only curve catalog.
#[derive(Debug, Default, Clone)]
pub struct FtpCurveCatalog {
    curves: BTreeMap<(String, u32), FtpCurve>,
}

impl FtpCurveCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, curve: FtpCurve) -> Result<(), FtpError> {
        let key = (curve.id.clone(), curve.version);
        if self.curves.contains_key(&key) {
            return Err(FtpError::DuplicateCurve {
                id: curve.id,
                version: curve.version,
            });
        }
        self.curves.insert(key, curve);
        Ok(())
    }

    pub fn get(&self, id: &str, version: u32) -> Option<&FtpCurve> {
        self.curves.get(&(id.to_string(), version))
    }

    pub fn versions(&self, id: &str) -> Vec<u32> {
        self.curves
            .keys()
            .filter(|(cid, _)| cid == id)
            .map(|(_, v)| *v)
            .collect()
    }

    pub fn latest_version(&self, id: &str) -> Option<u32> {
        self.versions(id).into_iter().max()
    }

    pub fn resolve_latest(&self, id: &str) -> Result<&FtpCurve, FtpError> {
        let v = self
            .latest_version(id)
            .ok_or_else(|| FtpError::CurveNotFound {
                id: id.to_string(),
                version: None,
            })?;
        Ok(self.curves.get(&(id.to_string(), v)).unwrap())
    }

    pub fn resolve_as_of(&self, id: &str, as_of: i64) -> Result<&FtpCurve, FtpError> {
        self.curves
            .values()
            .filter(|c| c.id == id && c.active_at(as_of))
            .max_by_key(|c| c.version)
            .ok_or_else(|| FtpError::CurveNotFound {
                id: id.to_string(),
                version: None,
            })
    }
}

/// Liquidity premium tenor point for one product (`*` = default).
#[derive(Debug, Clone, PartialEq)]
pub struct LiquidityPremium {
    pub product: String,
    pub tenor_days: i64,
    pub spread: f64,
}

/// Currency basis spread tenor point.
#[derive(Debug, Clone, PartialEq)]
pub struct BasisSpread {
    pub currency: String,
    pub tenor_days: i64,
    pub spread: f64,
}

/// Optionality charge for one product (`*` = default).
#[derive(Debug, Clone, PartialEq)]
pub struct OptionalityCharge {
    pub product: String,
    pub charge: f64,
}

/// Behavioural adjustment for one product (`*` = default).
#[derive(Debug, Clone, PartialEq)]
pub struct BehaviouralAdjustment {
    pub product: String,
    pub adjustment: f64,
}

/// A versioned FTP policy: spreads and charges.
#[derive(Debug, Clone, PartialEq)]
pub struct FtpPolicy {
    pub id: String,
    pub version: u32,
    pub effective_from: i64,
    pub effective_to: i64,
    pub liquidity: Vec<LiquidityPremium>,
    pub basis: Vec<BasisSpread>,
    pub optionality: Vec<OptionalityCharge>,
    pub behavioural: Vec<BehaviouralAdjustment>,
}

impl FtpPolicy {
    pub fn new(
        id: impl Into<String>,
        version: u32,
        effective_from: i64,
        effective_to: i64,
    ) -> Self {
        Self {
            id: id.into(),
            version,
            effective_from,
            effective_to,
            liquidity: Vec::new(),
            basis: Vec::new(),
            optionality: Vec::new(),
            behavioural: Vec::new(),
        }
    }

    pub fn with_liquidity(mut self, v: Vec<LiquidityPremium>) -> Self {
        self.liquidity = v;
        self
    }

    pub fn with_basis(mut self, v: Vec<BasisSpread>) -> Self {
        self.basis = v;
        self
    }

    pub fn with_optionality(mut self, v: Vec<OptionalityCharge>) -> Self {
        self.optionality = v;
        self
    }

    pub fn with_behavioural(mut self, v: Vec<BehaviouralAdjustment>) -> Self {
        self.behavioural = v;
        self
    }

    pub fn validate(&self) -> Result<(), FtpError> {
        if self.id.trim().is_empty() || self.version == 0 {
            return Err(FtpError::InvalidPolicy("empty id or zero version".into()));
        }
        if self.effective_from >= self.effective_to {
            return Err(FtpError::InvalidPolicy("empty effective range".into()));
        }
        let mut seen = std::collections::BTreeSet::new();
        for p in &self.liquidity {
            if !seen.insert((p.product.clone(), p.tenor_days)) {
                return Err(FtpError::InvalidPolicy(format!(
                    "duplicate liquidity point {} @ {}d",
                    p.product, p.tenor_days
                )));
            }
        }
        let mut seen = std::collections::BTreeSet::new();
        for b in &self.basis {
            if !seen.insert((b.currency.clone(), b.tenor_days)) {
                return Err(FtpError::InvalidPolicy(format!(
                    "duplicate basis point {} @ {}d",
                    b.currency, b.tenor_days
                )));
            }
        }
        Ok(())
    }

    #[inline]
    pub fn active_at(&self, as_of: i64) -> bool {
        self.effective_from <= as_of && as_of < self.effective_to
    }

    fn liquidity_points(&self, product: &str) -> Option<Vec<(i64, f64)>> {
        let mut pts: Vec<(i64, f64)> = self
            .liquidity
            .iter()
            .filter(|p| p.product == product)
            .map(|p| (p.tenor_days, p.spread))
            .collect();
        if pts.is_empty() {
            None
        } else {
            pts.sort_by_key(|(d, _)| *d);
            Some(pts)
        }
    }

    fn basis_points(&self, currency: &str) -> Option<Vec<(i64, f64)>> {
        let mut pts: Vec<(i64, f64)> = self
            .basis
            .iter()
            .filter(|b| b.currency == currency)
            .map(|b| (b.tenor_days, b.spread))
            .collect();
        if pts.is_empty() {
            None
        } else {
            pts.sort_by_key(|(d, _)| *d);
            Some(pts)
        }
    }
}

/// Append-only policy catalog.
#[derive(Debug, Default, Clone)]
pub struct FtpPolicyCatalog {
    policies: BTreeMap<(String, u32), FtpPolicy>,
}

impl FtpPolicyCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, policy: FtpPolicy) -> Result<(), FtpError> {
        policy.validate()?;
        let key = (policy.id.clone(), policy.version);
        if self.policies.contains_key(&key) {
            return Err(FtpError::DuplicatePolicy {
                id: policy.id,
                version: policy.version,
            });
        }
        self.policies.insert(key, policy);
        Ok(())
    }

    pub fn get(&self, id: &str, version: u32) -> Option<&FtpPolicy> {
        self.policies.get(&(id.to_string(), version))
    }

    pub fn latest_version(&self, id: &str) -> Option<u32> {
        self.policies
            .keys()
            .filter(|(pid, _)| pid == id)
            .map(|(_, v)| *v)
            .max()
    }

    pub fn resolve_latest(&self, id: &str) -> Result<&FtpPolicy, FtpError> {
        let v = self
            .latest_version(id)
            .ok_or_else(|| FtpError::PolicyNotFound {
                id: id.to_string(),
                version: None,
            })?;
        Ok(self.policies.get(&(id.to_string(), v)).unwrap())
    }
}

/// A pricing request. Dates are days since epoch.
#[derive(Debug, Clone, PartialEq)]
pub struct FtpRequest {
    pub curve_id: String,
    pub curve_version: u32,
    pub policy_id: String,
    pub policy_version: u32,
    pub product: String,
    pub currency: String,
    pub booking_date: i64,
    pub value_date: i64,
    pub maturity_date: i64,
}

impl FtpRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        curve_id: impl Into<String>,
        curve_version: u32,
        policy_id: impl Into<String>,
        policy_version: u32,
        product: impl Into<String>,
        currency: impl Into<String>,
        booking_date: i64,
        value_date: i64,
        maturity_date: i64,
    ) -> Self {
        Self {
            curve_id: curve_id.into(),
            curve_version,
            policy_id: policy_id.into(),
            policy_version,
            product: product.into(),
            currency: currency.into(),
            booking_date,
            value_date,
            maturity_date,
        }
    }

    pub fn tenor_days(&self) -> i64 {
        self.maturity_date - self.value_date
    }
}

/// One labelled term in the FTP build-up.
#[derive(Debug, Clone, PartialEq)]
pub struct FtpStep {
    pub component: String,
    pub value: f64,
    /// Where the number came from (curve / policy version / inherited product).
    pub source: String,
}

/// Fully itemised FTP price.
#[derive(Debug, Clone, PartialEq)]
pub struct FtpBreakdown {
    pub curve_id: String,
    pub curve_version: u32,
    pub policy_id: String,
    pub policy_version: u32,
    pub product: String,
    pub currency: String,
    /// Product chain resolved through the D6 hierarchy (self first).
    pub product_chain: Vec<String>,
    pub tenor_days: i64,
    pub base_rate: f64,
    pub liquidity_premium: f64,
    pub basis_spread: f64,
    pub optionality_charge: f64,
    pub behavioural_adjustment: f64,
    pub total_rate: f64,
    pub steps: Vec<FtpStep>,
}

/// Predicted-vs-actual cost reconciliation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FtpReconciliation {
    pub expected_rate: f64,
    pub actual_rate: f64,
    pub variance: f64,
    pub variance_bps: f64,
}

/// Errors from the FTP engine.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum FtpError {
    #[error("invalid effective interval")]
    InvalidInterval,
    #[error("invalid policy: {0}")]
    InvalidPolicy(String),
    #[error("curve `{id}`{version} not found", version = .version.map(|v| format!(" v{v}")).unwrap_or_default())]
    CurveNotFound { id: String, version: Option<u32> },
    #[error("policy `{id}`{version} not found", version = .version.map(|v| format!(" v{v}")).unwrap_or_default())]
    PolicyNotFound { id: String, version: Option<u32> },
    #[error("duplicate curve `{id}` v{version}")]
    DuplicateCurve { id: String, version: u32 },
    #[error("duplicate policy `{id}` v{version}")]
    DuplicatePolicy { id: String, version: u32 },
    #[error("invalid dates: booking {booking} / value {value} / maturity {maturity}")]
    InvalidDates {
        booking: i64,
        value: i64,
        maturity: i64,
    },
    #[error("curve construction failed: {0}")]
    Curve(String),
}

/// The FTP engine over a curve catalog, a policy catalog and the product
/// hierarchy.
pub struct FtpEngine<'a> {
    pub curves: &'a FtpCurveCatalog,
    pub policies: &'a FtpPolicyCatalog,
    pub products: &'a Hierarchy,
}

impl<'a> FtpEngine<'a> {
    pub fn new(
        curves: &'a FtpCurveCatalog,
        policies: &'a FtpPolicyCatalog,
        products: &'a Hierarchy,
    ) -> Self {
        Self {
            curves,
            policies,
            products,
        }
    }

    fn product_chain(&self, product: &str, as_of: i64) -> Vec<String> {
        let mut chain = vec![product.to_string()];
        chain.extend(self.products.ancestors(product, HierarchyKind::Product, as_of));
        chain
    }

    /// Price one request, returning the full build-up.
    pub fn price(&self, req: &FtpRequest) -> Result<FtpBreakdown, FtpError> {
        if !(req.booking_date <= req.value_date && req.value_date <= req.maturity_date) {
            return Err(FtpError::InvalidDates {
                booking: req.booking_date,
                value: req.value_date,
                maturity: req.maturity_date,
            });
        }
        let curve = self
            .curves
            .get(&req.curve_id, req.curve_version)
            .ok_or_else(|| FtpError::CurveNotFound {
                id: req.curve_id.clone(),
                version: Some(req.curve_version),
            })?;
        let policy = self
            .policies
            .get(&req.policy_id, req.policy_version)
            .ok_or_else(|| FtpError::PolicyNotFound {
                id: req.policy_id.clone(),
                version: Some(req.policy_version),
            })?;

        let tenor = req.tenor_days();
        let chain = self.product_chain(&req.product, req.booking_date);

        let base_rate = curve.zero_rate(tenor);
        let mut steps = vec![FtpStep {
            component: "base_curve".into(),
            value: base_rate,
            source: format!("{} v{} @ {}d", curve.id, curve.version, tenor),
        }];

        // Liquidity premium: first product in the chain with points, else `*`.
        let (liquidity, liquidity_source) = self.lookup_points(
            &policy.liquidity_points("*"),
            &chain,
            |p| policy.liquidity_points(p),
            tenor,
            "liquidity_premium",
        );
        steps.push(FtpStep {
            component: "liquidity_premium".into(),
            value: liquidity,
            source: liquidity_source,
        });

        // Currency basis spread.
        let basis = policy
            .basis_points(&req.currency)
            .map(|pts| interpolate(&pts, tenor))
            .unwrap_or(0.0);
        let basis_source = if policy.basis_points(&req.currency).is_some() {
            format!("{} basis {} @ {}d", policy.id, req.currency, tenor)
        } else {
            format!("no basis rule for {} (0)", req.currency)
        };
        steps.push(FtpStep {
            component: "basis_spread".into(),
            value: basis,
            source: basis_source,
        });

        let (optionality, opt_source) = self.lookup_scalar(
            "optionality",
            &chain,
            |p| {
                policy
                    .optionality
                    .iter()
                    .find(|o| o.product == p)
                    .map(|o| o.charge)
            },
        );
        steps.push(FtpStep {
            component: "optionality_charge".into(),
            value: optionality,
            source: opt_source,
        });

        let (behavioural, beh_source) = self.lookup_scalar(
            "behavioural",
            &chain,
            |p| {
                policy
                    .behavioural
                    .iter()
                    .find(|b| b.product == p)
                    .map(|b| b.adjustment)
            },
        );
        steps.push(FtpStep {
            component: "behavioural_adjustment".into(),
            value: behavioural,
            source: beh_source,
        });

        let total_rate = base_rate + liquidity + basis + optionality + behavioural;
        Ok(FtpBreakdown {
            curve_id: curve.id.clone(),
            curve_version: curve.version,
            policy_id: policy.id.clone(),
            policy_version: policy.version,
            product: req.product.clone(),
            currency: req.currency.clone(),
            product_chain: chain,
            tenor_days: tenor,
            base_rate,
            liquidity_premium: liquidity,
            basis_spread: basis,
            optionality_charge: optionality,
            behavioural_adjustment: behavioural,
            total_rate,
            steps,
        })
    }

    fn lookup_points<F>(
        &self,
        default: &Option<Vec<(i64, f64)>>,
        chain: &[String],
        mut by_product: F,
        tenor: i64,
        component: &str,
    ) -> (f64, String)
    where
        F: FnMut(&str) -> Option<Vec<(i64, f64)>>,
    {
        for (i, product) in chain.iter().enumerate() {
            if let Some(pts) = by_product(product) {
                let label = if i == 0 {
                    format!("{component}: product {product} @ {tenor}d")
                } else {
                    format!("{component}: inherited from {product} @ {tenor}d")
                };
                return (interpolate(&pts, tenor), label);
            }
        }
        if let Some(pts) = default {
            return (
                interpolate(pts, tenor),
                format!("{component}: default `*` @ {tenor}d"),
            );
        }
        (0.0, format!("{component}: no rule (0)"))
    }

    fn lookup_scalar<F>(
        &self,
        component: &str,
        chain: &[String],
        mut lookup: F,
    ) -> (f64, String)
    where
        F: FnMut(&str) -> Option<f64>,
    {
        for (i, product) in chain.iter().enumerate() {
            if let Some(v) = lookup(product) {
                let label = if i == 0 {
                    format!("{component}: product {product}")
                } else {
                    format!("{component}: inherited from {product}")
                };
                return (v, label);
            }
        }
        if let Some(v) = lookup("*") {
            return (v, format!("{component}: default `*`"));
        }
        (0.0, format!("{component}: no rule (0)"))
    }
}

/// Reconcile a predicted FTP price against the realised rate.
pub fn reconcile(predicted: &FtpBreakdown, actual_rate: f64) -> FtpReconciliation {
    let variance = actual_rate - predicted.total_rate;
    FtpReconciliation {
        expected_rate: predicted.total_rate,
        actual_rate,
        variance,
        variance_bps: variance * 10_000.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gtv_refdata::{EffectiveRange, HierarchyEdge};

    fn hierarchy() -> Hierarchy {
        let mut h = Hierarchy::new();
        // Loan -> SecuredLoan: SecuredLoan is the parent of Loan.
        h.add_edge(HierarchyEdge::new(
            HierarchyKind::Product,
            "SecuredLoan",
            "Loan",
            EffectiveRange::from_now_on(0),
        ))
        .unwrap();
        h
    }

    fn curve_catalog() -> FtpCurveCatalog {
        let mut c = FtpCurveCatalog::new();
        c.register(
            FtpCurve::new("USD-OIS", 1, "USD", 0, i64::MAX, vec![(0, 0.01), (365, 0.03)])
                .unwrap(),
        )
        .unwrap();
        c
    }

    fn policy_catalog() -> FtpPolicyCatalog {
        let mut p = FtpPolicyCatalog::new();
        p.register(
            FtpPolicy::new("P1", 1, 0, i64::MAX)
                .with_liquidity(vec![
                    LiquidityPremium {
                        product: "SecuredLoan".into(),
                        tenor_days: 0,
                        spread: 0.005,
                    },
                    LiquidityPremium {
                        product: "SecuredLoan".into(),
                        tenor_days: 365,
                        spread: 0.015,
                    },
                    LiquidityPremium {
                        product: "*".into(),
                        tenor_days: 0,
                        spread: 0.001,
                    },
                ])
                .with_basis(vec![
                    BasisSpread {
                        currency: "USD".into(),
                        tenor_days: 0,
                        spread: 0.0,
                    },
                    BasisSpread {
                        currency: "USD".into(),
                        tenor_days: 365,
                        spread: 0.002,
                    },
                ])
                .with_optionality(vec![OptionalityCharge {
                    product: "SecuredLoan".into(),
                    charge: 0.004,
                }])
                .with_behavioural(vec![BehaviouralAdjustment {
                    product: "SecuredLoan".into(),
                    adjustment: -0.001,
                }]),
        )
        .unwrap();
        p
    }

    fn request() -> FtpRequest {
        FtpRequest::new("USD-OIS", 1, "P1", 1, "Loan", "USD", 0, 0, 365)
    }

    #[test]
    fn price_itemises_every_component_and_inherits_from_parent() {
        let curves = curve_catalog();
        let policies = policy_catalog();
        let products = hierarchy();
        let engine = FtpEngine::new(&curves, &policies, &products);
        let b = engine.price(&request()).unwrap();

        assert_eq!(b.product_chain, vec!["Loan", "SecuredLoan"]);
        assert!((b.base_rate - 0.03).abs() < 1e-12);
        // inherited from SecuredLoan at 365d
        assert!((b.liquidity_premium - 0.015).abs() < 1e-12);
        assert!((b.basis_spread - 0.002).abs() < 1e-12);
        assert!((b.optionality_charge - 0.004).abs() < 1e-12);
        assert!((b.behavioural_adjustment - (-0.001)).abs() < 1e-12);
        assert!((b.total_rate - 0.05).abs() < 1e-12);
        assert_eq!(b.steps.len(), 5);
        let sum: f64 = b.steps.iter().map(|s| s.value).sum();
        assert!((sum - b.total_rate).abs() < 1e-12);
        assert!(b.steps[1].source.contains("inherited from SecuredLoan"));
    }

    #[test]
    fn default_star_applies_when_no_product_rule() {
        let curves = curve_catalog();
        let policies = policy_catalog();
        let products = Hierarchy::new();
        let engine = FtpEngine::new(&curves, &policies, &products);
        let b = engine
            .price(&FtpRequest::new("USD-OIS", 1, "P1", 1, "Other", "USD", 0, 0, 90))
            .unwrap();
        // no product points -> default * at 0d = 0.001
        assert!((b.liquidity_premium - 0.001).abs() < 1e-12);
        assert!(b.steps[1].source.contains("default `*`"));
    }

    #[test]
    fn interpolation_is_between_tenors() {
        let curves = curve_catalog();
        let policies = policy_catalog();
        let products = hierarchy();
        let engine = FtpEngine::new(&curves, &policies, &products);
        let b = engine
            .price(&FtpRequest::new("USD-OIS", 1, "P1", 1, "Loan", "USD", 0, 0, 183))
            .unwrap();
        // liquidity 0.005 -> 0.015 at mid: ~0.010
        assert!(b.liquidity_premium > 0.005 && b.liquidity_premium < 0.015);
        // base 0.01 -> 0.03 at mid
        assert!(b.base_rate > 0.01 && b.base_rate < 0.03);
    }

    #[test]
    fn reconciliation_reports_variance_and_bps() {
        let curves = curve_catalog();
        let policies = policy_catalog();
        let products = hierarchy();
        let engine = FtpEngine::new(&curves, &policies, &products);
        let b = engine.price(&request()).unwrap();
        let r = reconcile(&b, 0.052);
        assert!((r.variance - 0.002).abs() < 1e-12);
        assert!((r.variance_bps - 20.0).abs() < 1e-9);
    }

    #[test]
    fn invalid_dates_and_missing_versions_are_rejected() {
        let curves = curve_catalog();
        let policies = policy_catalog();
        let products = hierarchy();
        let engine = FtpEngine::new(&curves, &policies, &products);
        assert!(matches!(
            engine.price(&FtpRequest::new("USD-OIS", 1, "P1", 1, "Loan", "USD", 10, 0, 365)),
            Err(FtpError::InvalidDates { .. })
        ));
        assert!(matches!(
            engine.price(&FtpRequest::new("USD-OIS", 9, "P1", 1, "Loan", "USD", 0, 0, 365)),
            Err(FtpError::CurveNotFound { .. })
        ));
        assert!(matches!(
            engine.price(&FtpRequest::new("USD-OIS", 1, "P1", 9, "Loan", "USD", 0, 0, 365)),
            Err(FtpError::PolicyNotFound { .. })
        ));
    }

    #[test]
    fn catalog_versioning_and_effective_dating() {
        let mut curves = FtpCurveCatalog::new();
        curves
            .register(FtpCurve::new("C", 1, "USD", 0, 100, vec![(0, 0.01)]).unwrap())
            .unwrap();
        curves
            .register(FtpCurve::new("C", 2, "USD", 100, i64::MAX, vec![(0, 0.02)]).unwrap())
            .unwrap();
        assert_eq!(curves.versions("C"), vec![1, 2]);
        assert_eq!(curves.resolve_as_of("C", 50).unwrap().version, 1);
        assert_eq!(curves.resolve_as_of("C", 150).unwrap().version, 2);
        assert_eq!(curves.resolve_latest("C").unwrap().version, 2);
        assert!(matches!(
            curves.register(FtpCurve::new("C", 1, "USD", 0, 100, vec![(0, 0.01)]).unwrap()),
            Err(FtpError::DuplicateCurve { .. })
        ));
    }

    #[test]
    fn pricing_is_deterministic() {
        let curves = curve_catalog();
        let policies = policy_catalog();
        let products = hierarchy();
        let engine = FtpEngine::new(&curves, &policies, &products);
        assert_eq!(engine.price(&request()).unwrap(), engine.price(&request()).unwrap());
    }
}
