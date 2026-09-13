//! Versioned, effective-dated CRM rule sets.

use std::collections::BTreeMap;

use gtv_refdata::EffectiveRange;

use crate::error::GovernanceError;

/// Eligibility, priority and haircut rules for one collateral type.
#[derive(Debug, Clone, PartialEq)]
pub struct CollateralRule {
    pub collateral_type: String,
    pub eligible: bool,
    /// Higher = consumed first.
    pub priority: f64,
    /// Supervisory haircut (fraction of value).
    pub haircut: f64,
    /// Extra haircut when the collateral currency differs from the loan's.
    pub fx_haircut: f64,
    /// Extra haircut when the collateral matures before the loan.
    pub maturity_haircut: f64,
    /// Allowed currencies (empty = any).
    pub eligible_currencies: Vec<String>,
}

impl CollateralRule {
    pub fn new(collateral_type: impl Into<String>, priority: f64, haircut: f64) -> Self {
        Self {
            collateral_type: collateral_type.into(),
            eligible: true,
            priority,
            haircut,
            fx_haircut: 0.0,
            maturity_haircut: 0.0,
            eligible_currencies: Vec::new(),
        }
    }

    pub fn ineligible(mut self) -> Self {
        self.eligible = false;
        self
    }

    pub fn with_fx_haircut(mut self, h: f64) -> Self {
        self.fx_haircut = h;
        self
    }

    pub fn with_maturity_haircut(mut self, h: f64) -> Self {
        self.maturity_haircut = h;
        self
    }

    pub fn with_currencies(mut self, currencies: Vec<String>) -> Self {
        self.eligible_currencies = currencies;
        self
    }
}

/// Eligibility and priority rules for one guarantor type.
#[derive(Debug, Clone, PartialEq)]
pub struct GuaranteeRule {
    pub guarantor_type: String,
    pub eligible: bool,
    pub priority: f64,
    /// Allowed jurisdictions (empty = any).
    pub eligible_jurisdictions: Vec<String>,
}

impl GuaranteeRule {
    pub fn new(guarantor_type: impl Into<String>, priority: f64) -> Self {
        Self {
            guarantor_type: guarantor_type.into(),
            eligible: true,
            priority,
            eligible_jurisdictions: Vec::new(),
        }
    }

    pub fn ineligible(mut self) -> Self {
        self.eligible = false;
        self
    }

    pub fn with_jurisdictions(mut self, jurisdictions: Vec<String>) -> Self {
        self.eligible_jurisdictions = jurisdictions;
        self
    }
}

/// A forbidden counterparty / collateral-type pairing (wrong-way risk).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct WrongWayRisk {
    pub counterparty: String,
    pub collateral_type: String,
}

/// Cap on the total capacity allocated to one collateral type.
#[derive(Debug, Clone, PartialEq)]
pub struct ConcentrationLimit {
    pub collateral_type: String,
    pub limit: f64,
}

/// One immutable, effective-dated rule set version.
#[derive(Debug, Clone, PartialEq)]
pub struct RuleSet {
    pub id: String,
    pub version: u32,
    pub effective: EffectiveRange,
    pub collateral: Vec<CollateralRule>,
    pub guarantees: Vec<GuaranteeRule>,
    pub wrong_way: Vec<WrongWayRisk>,
    pub concentration: Vec<ConcentrationLimit>,
}

impl RuleSet {
    pub fn new(id: impl Into<String>, version: u32, effective: EffectiveRange) -> Self {
        Self {
            id: id.into(),
            version,
            effective,
            collateral: Vec::new(),
            guarantees: Vec::new(),
            wrong_way: Vec::new(),
            concentration: Vec::new(),
        }
    }

    pub fn with_collateral(mut self, rules: Vec<CollateralRule>) -> Self {
        self.collateral = rules;
        self
    }

    pub fn with_guarantees(mut self, rules: Vec<GuaranteeRule>) -> Self {
        self.guarantees = rules;
        self
    }

    pub fn with_wrong_way(mut self, rules: Vec<WrongWayRisk>) -> Self {
        self.wrong_way = rules;
        self
    }

    pub fn with_concentration(mut self, limits: Vec<ConcentrationLimit>) -> Self {
        self.concentration = limits;
        self
    }

    pub fn validate(&self, as_of: i64) -> Result<(), GovernanceError> {
        let invalid = |reason: &str| GovernanceError::InvalidRule {
            id: self.id.clone(),
            version: self.version,
            reason: reason.to_string(),
        };
        if self.id.trim().is_empty() {
            return Err(invalid("empty rule-set id"));
        }
        if self.version == 0 {
            return Err(invalid("version must be >= 1"));
        }
        if !self.effective.contains(as_of) {
            return Err(invalid("as_of is outside the rule set's effective range"));
        }
        for r in &self.collateral {
            if !(0.0..1.0).contains(&r.haircut)
                || !(0.0..1.0).contains(&r.fx_haircut)
                || !(0.0..1.0).contains(&r.maturity_haircut)
            {
                return Err(invalid("haircuts must be in [0,1)"));
            }
            if r.haircut + r.fx_haircut + r.maturity_haircut >= 1.0 {
                return Err(invalid("combined haircuts must be < 1"));
            }
        }
        for l in &self.concentration {
            if l.limit < 0.0 || !l.limit.is_finite() {
                return Err(invalid("concentration limits must be finite and >= 0"));
            }
        }
        Ok(())
    }

    pub fn collateral_rule(&self, collateral_type: &str) -> Option<&CollateralRule> {
        self.collateral
            .iter()
            .find(|r| r.collateral_type == collateral_type)
    }

    pub fn guarantee_rule(&self, guarantor_type: &str) -> Option<&GuaranteeRule> {
        self.guarantees
            .iter()
            .find(|r| r.guarantor_type == guarantor_type)
    }

    pub fn is_wrong_way(&self, counterparty: &str, collateral_type: &str) -> bool {
        self.wrong_way.iter().any(|w| {
            w.counterparty == counterparty && w.collateral_type == collateral_type
        })
    }

    pub fn concentration_limit(&self, collateral_type: &str) -> Option<f64> {
        self.concentration
            .iter()
            .find(|l| l.collateral_type == collateral_type)
            .map(|l| l.limit)
    }
}

/// Append-only registry of rule-set versions.
#[derive(Debug, Default, Clone)]
pub struct RuleRegistry {
    sets: BTreeMap<(String, u32), RuleSet>,
}

impl RuleRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, rules: RuleSet, as_of: i64) -> Result<(), GovernanceError> {
        rules.validate(as_of)?;
        let key = (rules.id.clone(), rules.version);
        if self.sets.contains_key(&key) {
            return Err(GovernanceError::DuplicateRuleSet {
                id: rules.id,
                version: rules.version,
            });
        }
        self.sets.insert(key, rules);
        Ok(())
    }

    pub fn get(&self, id: &str, version: u32) -> Option<&RuleSet> {
        self.sets.get(&(id.to_string(), version))
    }

    pub fn versions(&self, id: &str) -> Vec<u32> {
        self.sets
            .keys()
            .filter(|(sid, _)| sid == id)
            .map(|(_, v)| *v)
            .collect()
    }

    pub fn latest_version(&self, id: &str) -> Option<u32> {
        self.versions(id).into_iter().max()
    }

    pub fn resolve_latest(&self, id: &str) -> Result<&RuleSet, GovernanceError> {
        let version = self
            .latest_version(id)
            .ok_or_else(|| GovernanceError::NoRuleSet { id: id.to_string() })?;
        Ok(self.sets.get(&(id.to_string(), version)).unwrap())
    }

    /// Highest version whose effective range contains `as_of`.
    pub fn resolve_as_of(&self, id: &str, as_of: i64) -> Result<&RuleSet, GovernanceError> {
        self.sets
            .values()
            .filter(|r| r.id == id && r.effective.contains(as_of))
            .max_by_key(|r| r.version)
            .ok_or_else(|| GovernanceError::NoEffectiveRuleSet {
                id: id.to_string(),
                as_of,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rules(id: &str, version: u32, from: i64, to: i64) -> RuleSet {
        RuleSet::new(id, version, EffectiveRange::new(from, to).unwrap())
            .with_collateral(vec![CollateralRule::new("cash", 10.0, 0.0)])
    }

    #[test]
    fn registry_is_versioned_and_effective_dated() {
        let mut reg = RuleRegistry::new();
        reg.register(rules("crm", 1, 0, 100), 0).unwrap();
        reg.register(rules("crm", 2, 100, i64::MAX), 100).unwrap();
        assert_eq!(reg.versions("crm"), vec![1, 2]);
        assert_eq!(reg.resolve_as_of("crm", 50).unwrap().version, 1);
        assert_eq!(reg.resolve_as_of("crm", 100).unwrap().version, 2);
        assert_eq!(reg.resolve_latest("crm").unwrap().version, 2);
        assert!(matches!(
            reg.resolve_as_of("crm", -1),
            Err(GovernanceError::NoEffectiveRuleSet { .. })
        ));
    }

    #[test]
    fn duplicates_and_invalid_haircuts_are_rejected() {
        let mut reg = RuleRegistry::new();
        reg.register(rules("crm", 1, 0, i64::MAX), 0).unwrap();
        assert!(matches!(
            reg.register(rules("crm", 1, 0, i64::MAX), 0),
            Err(GovernanceError::DuplicateRuleSet { .. })
        ));
        let bad = RuleSet::new("x", 1, EffectiveRange::from_now_on(0))
            .with_collateral(vec![CollateralRule::new("cash", 1.0, 1.5)]);
        assert!(matches!(
            reg.register(bad, 0),
            Err(GovernanceError::InvalidRule { .. })
        ));
    }

    #[test]
    fn wrong_way_and_concentration_lookups() {
        let rs = RuleSet::new("crm", 1, EffectiveRange::from_now_on(0))
            .with_wrong_way(vec![WrongWayRisk {
                counterparty: "CP1".into(),
                collateral_type: "equity".into(),
            }])
            .with_concentration(vec![ConcentrationLimit {
                collateral_type: "cash".into(),
                limit: 500.0,
            }]);
        assert!(rs.is_wrong_way("CP1", "equity"));
        assert!(!rs.is_wrong_way("CP1", "cash"));
        assert_eq!(rs.concentration_limit("cash"), Some(500.0));
        assert_eq!(rs.concentration_limit("equity"), None);
    }
}
