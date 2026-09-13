//! Governed CRM allocation: transform raw inputs through a [`RuleSet`], run the
//! audited `gtv-array` kernel, and record every exclusion / breach.

use std::collections::{BTreeMap, BTreeSet};

use gtv_array::crm::{
    adjusted_collateral_value, crm_alloc_greedy, AllocationMode, Collateral, CollateralEdge,
    CrmResult, GuaranteeEdge, Guarantor, Loan,
};
#[cfg(feature = "crm-lp")]
use gtv_array::crm_lp::crm_alloc_lp;

use crate::error::GovernanceError;
use crate::rules::RuleSet;

/// A loan / exposure needing mitigation.
#[derive(Debug, Clone, PartialEq)]
pub struct Exposure {
    pub loan_id: u64,
    pub counterparty: String,
    pub exposure: f64,
    /// Higher = covered first by a shared source.
    pub priority: f64,
    pub currency: String,
    pub maturity_days: i64,
    pub netting_set: Option<String>,
}

impl Exposure {
    pub fn new(
        loan_id: u64,
        counterparty: impl Into<String>,
        exposure: f64,
        currency: impl Into<String>,
    ) -> Self {
        Self {
            loan_id,
            counterparty: counterparty.into(),
            exposure,
            priority: 0.0,
            currency: currency.into(),
            maturity_days: 0,
            netting_set: None,
        }
    }

    pub fn with_priority(mut self, priority: f64) -> Self {
        self.priority = priority;
        self
    }

    pub fn with_maturity(mut self, maturity_days: i64) -> Self {
        self.maturity_days = maturity_days;
        self
    }

    pub fn with_netting_set(mut self, netting_set: impl Into<String>) -> Self {
        self.netting_set = Some(netting_set.into());
        self
    }
}

/// A collateral source with raw (pre-haircut) value.
#[derive(Debug, Clone, PartialEq)]
pub struct GovernedCollateral {
    pub col_id: u64,
    pub collateral_type: String,
    pub value: f64,
    pub currency: String,
    pub maturity_days: i64,
}

impl GovernedCollateral {
    pub fn new(
        col_id: u64,
        collateral_type: impl Into<String>,
        value: f64,
        currency: impl Into<String>,
    ) -> Self {
        Self {
            col_id,
            collateral_type: collateral_type.into(),
            value,
            currency: currency.into(),
            maturity_days: 0,
        }
    }

    pub fn with_maturity(mut self, maturity_days: i64) -> Self {
        self.maturity_days = maturity_days;
        self
    }
}

/// A guarantor source.
#[derive(Debug, Clone, PartialEq)]
pub struct GovernedGuarantor {
    pub guarantor_id: u64,
    pub guarantor_type: String,
    pub capacity: f64,
    pub jurisdiction: String,
}

impl GovernedGuarantor {
    pub fn new(
        guarantor_id: u64,
        guarantor_type: impl Into<String>,
        capacity: f64,
        jurisdiction: impl Into<String>,
    ) -> Self {
        Self {
            guarantor_id,
            guarantor_type: guarantor_type.into(),
            capacity,
            jurisdiction: jurisdiction.into(),
        }
    }
}

/// Pledge edge collateral → loan with a contractual ratio.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CollateralPledge {
    pub col_id: u64,
    pub loan_id: u64,
    pub ratio: f64,
}

/// Pledge edge guarantor → loan with a contractual amount.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GuaranteePledge {
    pub guarantor_id: u64,
    pub loan_id: u64,
    pub amount: f64,
}

/// Which kernel allocator to run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllocationMethod {
    Greedy,
    Lp,
}

/// Why a source or edge was removed before allocation.
#[derive(Debug, Clone, PartialEq)]
pub enum Exclusion {
    CollateralTypeUnknown {
        col_id: u64,
        collateral_type: String,
    },
    CollateralIneligible {
        col_id: u64,
        collateral_type: String,
    },
    CollateralCurrency {
        col_id: u64,
        currency: String,
    },
    GuarantorTypeUnknown {
        guarantor_id: u64,
        guarantor_type: String,
    },
    GuarantorIneligible {
        guarantor_id: u64,
        guarantor_type: String,
    },
    GuarantorJurisdiction {
        guarantor_id: u64,
        jurisdiction: String,
    },
    WrongWay {
        col_id: u64,
        loan_id: u64,
        counterparty: String,
        collateral_type: String,
    },
}

/// One concentration breach (the requested capacity exceeded what the limit
/// still allowed).
#[derive(Debug, Clone, PartialEq)]
pub struct ConcentrationBreach {
    pub collateral_type: String,
    pub requested: f64,
    pub allowed: f64,
}

/// The complete governed result.
#[derive(Debug, Clone)]
pub struct GovernedResult {
    pub rule_set_id: String,
    pub rule_version: u32,
    pub as_of: i64,
    pub method: AllocationMethod,
    pub result: CrmResult,
    pub exclusions: Vec<Exclusion>,
    pub concentration_breaches: Vec<ConcentrationBreach>,
}

/// Normalised kernel inputs + governance annotations.
struct KernelInputs {
    loans: Vec<Loan>,
    collaterals: Vec<Collateral>,
    guarantors: Vec<Guarantor>,
    coll_edges: Vec<CollateralEdge>,
    guar_edges: Vec<GuaranteeEdge>,
    exclusions: Vec<Exclusion>,
    breaches: Vec<ConcentrationBreach>,
}

/// Raw governed inputs.
#[derive(Debug, Clone, Default)]
pub struct GovernedInputs {
    pub exposures: Vec<Exposure>,
    pub collaterals: Vec<GovernedCollateral>,
    pub guarantors: Vec<GovernedGuarantor>,
    pub collateral_pledges: Vec<CollateralPledge>,
    pub guarantee_pledges: Vec<GuaranteePledge>,
}

impl GovernedInputs {
    pub fn new() -> Self {
        Self::default()
    }

    /// Sum of exposure per netting set (`__unrated__` when unset).
    pub fn netting_summary(&self) -> BTreeMap<String, f64> {
        let mut out = BTreeMap::new();
        for e in &self.exposures {
            let key = e
                .netting_set
                .clone()
                .unwrap_or_else(|| "__unrated__".to_string());
            *out.entry(key).or_insert(0.0) += e.exposure;
        }
        out
    }

    /// Governed allocation with the chosen kernel.
    pub fn allocate(
        &self,
        method: AllocationMethod,
        rules: &RuleSet,
        as_of: i64,
    ) -> Result<GovernedResult, GovernanceError> {
        rules.validate(as_of)?;
        let kernel = self.build(rules)?;
        let result = run_kernel(method, &kernel)?;
        Ok(GovernedResult {
            rule_set_id: rules.id.clone(),
            rule_version: rules.version,
            as_of,
            method,
            result,
            exclusions: kernel.exclusions,
            concentration_breaches: kernel.breaches,
        })
    }

    pub fn allocate_greedy(
        &self,
        rules: &RuleSet,
        as_of: i64,
    ) -> Result<GovernedResult, GovernanceError> {
        self.allocate(AllocationMethod::Greedy, rules, as_of)
    }

    pub fn allocate_lp(
        &self,
        rules: &RuleSet,
        as_of: i64,
    ) -> Result<GovernedResult, GovernanceError> {
        self.allocate(AllocationMethod::Lp, rules, as_of)
    }

    fn build(&self, rules: &RuleSet) -> Result<KernelInputs, GovernanceError> {
        // --- exposures ---------------------------------------------------
        let mut exposure_ids = BTreeSet::new();
        let mut loans = Vec::with_capacity(self.exposures.len());
        let mut exposure_by_id: BTreeMap<u64, &Exposure> = BTreeMap::new();
        for e in &self.exposures {
            if !exposure_ids.insert(e.loan_id) {
                return Err(GovernanceError::DuplicateExposure(e.loan_id));
            }
            exposure_by_id.insert(e.loan_id, e);
            loans.push(Loan {
                id: e.loan_id,
                exposure: e.exposure,
                priority: e.priority,
            });
        }

        let collateral_by_id: BTreeMap<u64, &GovernedCollateral> =
            self.collaterals.iter().map(|c| (c.col_id, c)).collect();
        let guarantor_by_id: BTreeMap<u64, &GovernedGuarantor> =
            self.guarantors.iter().map(|g| (g.guarantor_id, g)).collect();

        // --- validate pledges -------------------------------------------
        for p in &self.collateral_pledges {
            if !collateral_by_id.contains_key(&p.col_id) || !exposure_by_id.contains_key(&p.loan_id)
            {
                return Err(GovernanceError::DanglingCollateralPledge {
                    col_id: p.col_id,
                    loan_id: p.loan_id,
                });
            }
        }
        for p in &self.guarantee_pledges {
            if !guarantor_by_id.contains_key(&p.guarantor_id)
                || !exposure_by_id.contains_key(&p.loan_id)
            {
                return Err(GovernanceError::DanglingGuaranteePledge {
                    guarantor_id: p.guarantor_id,
                    loan_id: p.loan_id,
                });
            }
        }

        let mut exclusions = Vec::new();

        // --- collateral eligibility + haircuts ---------------------------
        struct Candidate {
            col_id: u64,
            collateral_type: String,
            priority: f64,
            capacity: f64,
        }
        let mut candidates: Vec<Candidate> = Vec::new();
        for c in &self.collaterals {
            let Some(rule) = rules.collateral_rule(&c.collateral_type) else {
                exclusions.push(Exclusion::CollateralTypeUnknown {
                    col_id: c.col_id,
                    collateral_type: c.collateral_type.clone(),
                });
                continue;
            };
            if !rule.eligible {
                exclusions.push(Exclusion::CollateralIneligible {
                    col_id: c.col_id,
                    collateral_type: c.collateral_type.clone(),
                });
                continue;
            }
            if !rule.eligible_currencies.is_empty()
                && !rule.eligible_currencies.contains(&c.currency)
            {
                exclusions.push(Exclusion::CollateralCurrency {
                    col_id: c.col_id,
                    currency: c.currency.clone(),
                });
                continue;
            }
            let pledged: Vec<&Exposure> = self
                .collateral_pledges
                .iter()
                .filter(|p| p.col_id == c.col_id)
                .filter_map(|p| exposure_by_id.get(&p.loan_id).copied())
                .collect();
            let fx_mismatch = pledged.iter().any(|e| e.currency != c.currency);
            let maturity_mismatch = pledged.iter().any(|e| e.maturity_days > c.maturity_days);
            let adjusted = adjusted_collateral_value(
                c.value,
                rule.haircut,
                if fx_mismatch { rule.fx_haircut } else { 0.0 },
                if maturity_mismatch {
                    rule.maturity_haircut
                } else {
                    0.0
                },
            );
            candidates.push(Candidate {
                col_id: c.col_id,
                collateral_type: c.collateral_type.clone(),
                priority: rule.priority,
                capacity: adjusted,
            });
        }

        // --- concentration limits ---------------------------------------
        // Deterministic order: priority desc, then id asc.
        candidates.sort_by(|a, b| {
            b.priority
                .partial_cmp(&a.priority)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.col_id.cmp(&b.col_id))
        });
        let mut used: BTreeMap<String, f64> = BTreeMap::new();
        let mut breaches = Vec::new();
        for cand in candidates.iter_mut() {
            let Some(limit) = rules.concentration_limit(&cand.collateral_type) else {
                continue;
            };
            let spent = used.entry(cand.collateral_type.clone()).or_insert(0.0);
            let allowed = (limit - *spent).max(0.0).min(cand.capacity);
            if allowed + 1e-9 < cand.capacity {
                breaches.push(ConcentrationBreach {
                    collateral_type: cand.collateral_type.clone(),
                    requested: cand.capacity,
                    allowed,
                });
            }
            *spent += allowed;
            cand.capacity = allowed;
        }

        let collaterals: Vec<Collateral> = candidates
            .iter()
            .filter(|c| c.capacity > 0.0)
            .map(|c| Collateral {
                id: c.col_id,
                capacity: c.capacity,
                priority: c.priority,
            })
            .collect();
        let eligible_collateral: BTreeSet<u64> = collaterals.iter().map(|c| c.id).collect();
        let collateral_type: BTreeMap<u64, String> = candidates
            .iter()
            .map(|c| (c.col_id, c.collateral_type.clone()))
            .collect();

        // --- collateral edges (wrong-way filtered) -----------------------
        let mut coll_edges = Vec::new();
        for p in &self.collateral_pledges {
            if !eligible_collateral.contains(&p.col_id) {
                continue; // source already excluded
            }
            let Some(loan) = exposure_by_id.get(&p.loan_id) else {
                continue;
            };
            let ctype = collateral_type
                .get(&p.col_id)
                .cloned()
                .unwrap_or_default();
            if rules.is_wrong_way(&loan.counterparty, &ctype) {
                exclusions.push(Exclusion::WrongWay {
                    col_id: p.col_id,
                    loan_id: p.loan_id,
                    counterparty: loan.counterparty.clone(),
                    collateral_type: ctype,
                });
                continue;
            }
            coll_edges.push(CollateralEdge {
                col_id: p.col_id,
                loan_id: p.loan_id,
                ratio: p.ratio,
                mode: AllocationMode::Optimizable,
            });
        }

        // --- guarantee eligibility --------------------------------------
        let mut guarantors = Vec::new();
        let mut eligible_guarantor = BTreeSet::new();
        for g in &self.guarantors {
            let Some(rule) = rules.guarantee_rule(&g.guarantor_type) else {
                exclusions.push(Exclusion::GuarantorTypeUnknown {
                    guarantor_id: g.guarantor_id,
                    guarantor_type: g.guarantor_type.clone(),
                });
                continue;
            };
            if !rule.eligible {
                exclusions.push(Exclusion::GuarantorIneligible {
                    guarantor_id: g.guarantor_id,
                    guarantor_type: g.guarantor_type.clone(),
                });
                continue;
            }
            if !rule.eligible_jurisdictions.is_empty()
                && !rule.eligible_jurisdictions.contains(&g.jurisdiction)
            {
                exclusions.push(Exclusion::GuarantorJurisdiction {
                    guarantor_id: g.guarantor_id,
                    jurisdiction: g.jurisdiction.clone(),
                });
                continue;
            }
            eligible_guarantor.insert(g.guarantor_id);
            guarantors.push(Guarantor {
                id: g.guarantor_id,
                capacity: g.capacity,
                priority: rule.priority,
            });
        }

        let mut guar_edges = Vec::new();
        for p in &self.guarantee_pledges {
            if !eligible_guarantor.contains(&p.guarantor_id) {
                continue;
            }
            guar_edges.push(GuaranteeEdge {
                guarantor_id: p.guarantor_id,
                loan_id: p.loan_id,
                amount: p.amount,
                mode: AllocationMode::Optimizable,
            });
        }

        Ok(KernelInputs {
            loans,
            collaterals,
            guarantors,
            coll_edges,
            guar_edges,
            exclusions,
            breaches,
        })
    }
}

fn run_kernel(
    method: AllocationMethod,
    kernel: &KernelInputs,
) -> Result<CrmResult, GovernanceError> {
    let call_greedy = || {
        crm_alloc_greedy(
            &kernel.loans,
            &kernel.collaterals,
            &kernel.guarantors,
            &kernel.coll_edges,
            &kernel.guar_edges,
        )
    };
    match method {
        AllocationMethod::Greedy => call_greedy().map_err(|e| GovernanceError::Crm(e.to_string())),
        AllocationMethod::Lp => {
            #[cfg(feature = "crm-lp")]
            {
                crm_alloc_lp(
                    &kernel.loans,
                    &kernel.collaterals,
                    &kernel.guarantors,
                    &kernel.coll_edges,
                    &kernel.guar_edges,
                )
                .map_err(|e| GovernanceError::Crm(e.to_string()))
            }
            #[cfg(not(feature = "crm-lp"))]
            {
                let _ = call_greedy;
                Err(GovernanceError::Crm(
                    "crm-lp feature is disabled; cannot run the exact LP allocator".into(),
                ))
            }
        }
    }
}

/// Loan-by-loan greedy vs LP reconciliation.
#[derive(Debug, Clone, PartialEq)]
pub struct GreedyLpDiff {
    pub loan_id: u64,
    pub greedy_net: f64,
    pub lp_net: f64,
    /// `lp_net - greedy_net` (negative = the LP reduced residual exposure).
    pub delta: f64,
}

/// Compare the net exposure of a greedy and an LP run over the same inputs.
pub fn greedy_lp_diff(greedy: &GovernedResult, lp: &GovernedResult) -> Vec<GreedyLpDiff> {
    let lp_by_loan: BTreeMap<u64, f64> = lp
        .result
        .loan_id
        .iter()
        .copied()
        .zip(lp.result.net_exposure.iter().copied())
        .collect();
    greedy
        .result
        .loan_id
        .iter()
        .copied()
        .zip(greedy.result.net_exposure.iter().copied())
        .map(|(loan_id, greedy_net)| {
            let lp_net = lp_by_loan.get(&loan_id).copied().unwrap_or(greedy_net);
            GreedyLpDiff {
                loan_id,
                greedy_net,
                lp_net,
                delta: lp_net - greedy_net,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rules::{CollateralRule, ConcentrationLimit, RuleSet, WrongWayRisk};

    fn rules(haircut: f64) -> RuleSet {
        RuleSet::new("crm", 1, gtv_refdata::EffectiveRange::from_now_on(0))
            .with_collateral(vec![CollateralRule::new("cash", 10.0, haircut)])
    }

    #[test]
    fn eligible_collateral_is_haircut_and_allocated() {
        let inputs = GovernedInputs {
            exposures: vec![Exposure::new(1, "CP1", 100.0, "USD")],
            collaterals: vec![GovernedCollateral::new(10, "cash", 100.0, "USD")],
            collateral_pledges: vec![CollateralPledge {
                col_id: 10,
                loan_id: 1,
                ratio: 1.0,
            }],
            ..Default::default()
        };
        let out = inputs.allocate_greedy(&rules(0.2), 0).unwrap();
        assert_eq!(out.result.collateral_cover, vec![80.0]);
        assert_eq!(out.result.net_exposure, vec![20.0]);
        assert!(out.exclusions.is_empty());
        assert!(out.result.allocations.iter().all(|a| a.amount <= 80.0));
    }

    #[test]
    fn ineligible_and_unknown_types_are_excluded() {
        let inputs = GovernedInputs {
            exposures: vec![Exposure::new(1, "CP1", 100.0, "USD")],
            collaterals: vec![
                GovernedCollateral::new(10, "equity", 100.0, "USD"),
                GovernedCollateral::new(20, "cash", 100.0, "USD"),
            ],
            collateral_pledges: vec![
                CollateralPledge {
                    col_id: 10,
                    loan_id: 1,
                    ratio: 1.0,
                },
                CollateralPledge {
                    col_id: 20,
                    loan_id: 1,
                    ratio: 1.0,
                },
            ],
            ..Default::default()
        };
        let out = inputs.allocate_greedy(&rules(0.0), 0).unwrap();
        assert_eq!(out.exclusions.len(), 1);
        assert!(matches!(
            out.exclusions[0],
            Exclusion::CollateralTypeUnknown { col_id: 10, .. }
        ));
        assert_eq!(out.result.collateral_cover, vec![100.0]);
    }

    #[test]
    fn wrong_way_edge_is_removed() {
        let rs = rules(0.0).with_wrong_way(vec![WrongWayRisk {
            counterparty: "CP1".into(),
            collateral_type: "cash".into(),
        }]);
        let inputs = GovernedInputs {
            exposures: vec![Exposure::new(1, "CP1", 100.0, "USD")],
            collaterals: vec![GovernedCollateral::new(10, "cash", 100.0, "USD")],
            collateral_pledges: vec![CollateralPledge {
                col_id: 10,
                loan_id: 1,
                ratio: 1.0,
            }],
            ..Default::default()
        };
        let out = inputs.allocate_greedy(&rs, 0).unwrap();
        assert_eq!(out.result.collateral_cover, vec![0.0]);
        assert_eq!(out.result.net_exposure, vec![100.0]);
        assert!(matches!(out.exclusions[0], Exclusion::WrongWay { .. }));
    }

    #[test]
    fn concentration_limit_caps_type_capacity() {
        let rs = rules(0.0).with_concentration(vec![ConcentrationLimit {
            collateral_type: "cash".into(),
            limit: 60.0,
        }]);
        let inputs = GovernedInputs {
            exposures: vec![Exposure::new(1, "CP1", 100.0, "USD")],
            collaterals: vec![GovernedCollateral::new(10, "cash", 100.0, "USD")],
            collateral_pledges: vec![CollateralPledge {
                col_id: 10,
                loan_id: 1,
                ratio: 1.0,
            }],
            ..Default::default()
        };
        let out = inputs.allocate_greedy(&rs, 0).unwrap();
        assert_eq!(out.result.collateral_cover, vec![60.0]);
        assert_eq!(out.concentration_breaches.len(), 1);
        assert_eq!(out.concentration_breaches[0].allowed, 60.0);
    }

    #[test]
    fn fx_and_maturity_mismatch_apply_extra_haircuts() {
        let rs = RuleSet::new("crm", 1, gtv_refdata::EffectiveRange::from_now_on(0))
            .with_collateral(vec![CollateralRule::new("bond", 10.0, 0.0)
                .with_fx_haircut(0.05)
                .with_maturity_haircut(0.10)]);
        let inputs = GovernedInputs {
            exposures: vec![Exposure::new(1, "CP1", 100.0, "USD").with_maturity(365)],
            collaterals: vec![GovernedCollateral::new(10, "bond", 100.0, "EUR").with_maturity(90)],
            collateral_pledges: vec![CollateralPledge {
                col_id: 10,
                loan_id: 1,
                ratio: 1.0,
            }],
            ..Default::default()
        };
        let out = inputs.allocate_greedy(&rs, 0).unwrap();
        // 100 * (1 - 0 - 0.05 - 0.10) = 85
        assert_eq!(out.result.collateral_cover, vec![85.0]);
    }

    #[test]
    fn guarantee_eligibility_and_jurisdiction() {
        use crate::rules::GuaranteeRule;
        let rs = rules(0.0).with_guarantees(vec![GuaranteeRule::new("bank", 5.0)
            .with_jurisdictions(vec!["HK".into()])]);
        let inputs = GovernedInputs {
            exposures: vec![
                Exposure::new(1, "CP1", 100.0, "USD"),
                Exposure::new(2, "CP2", 100.0, "USD"),
            ],
            guarantors: vec![
                GovernedGuarantor::new(20, "bank", 100.0, "HK"),
                GovernedGuarantor::new(30, "bank", 100.0, "US"),
            ],
            guarantee_pledges: vec![
                GuaranteePledge {
                    guarantor_id: 20,
                    loan_id: 1,
                    amount: 100.0,
                },
                GuaranteePledge {
                    guarantor_id: 30,
                    loan_id: 2,
                    amount: 100.0,
                },
            ],
            ..Default::default()
        };
        let out = inputs.allocate_greedy(&rs, 0).unwrap();
        assert_eq!(out.result.guarantee_cover, vec![100.0, 0.0]);
        assert!(out
            .exclusions
            .iter()
            .any(|e| matches!(e, Exclusion::GuarantorJurisdiction { guarantor_id: 30, .. })));
    }

    #[test]
    fn netting_summary_groups_exposures() {
        let inputs = GovernedInputs {
            exposures: vec![
                Exposure::new(1, "CP1", 10.0, "USD").with_netting_set("NS1"),
                Exposure::new(2, "CP1", 5.0, "USD").with_netting_set("NS1"),
                Exposure::new(3, "CP2", 7.0, "USD"),
            ],
            ..Default::default()
        };
        let summary = inputs.netting_summary();
        assert_eq!(summary["NS1"], 15.0);
        assert_eq!(summary["__unrated__"], 7.0);
    }

    #[test]
    #[cfg(feature = "crm-lp")]
    fn greedy_vs_lp_diff_reports_per_edge_cap_effect() {
        // One collateral (100), two loans each ratio 0.4. Greedy ignores the
        // ratio cap; the LP honours it (40/40), changing net exposure.
        let rs = rules(0.0);
        let inputs = GovernedInputs {
            exposures: vec![
                Exposure::new(1, "CP1", 80.0, "USD").with_priority(2.0),
                Exposure::new(2, "CP1", 80.0, "USD").with_priority(1.0),
            ],
            collaterals: vec![GovernedCollateral::new(10, "cash", 100.0, "USD")],
            collateral_pledges: vec![
                CollateralPledge {
                    col_id: 10,
                    loan_id: 1,
                    ratio: 0.4,
                },
                CollateralPledge {
                    col_id: 10,
                    loan_id: 2,
                    ratio: 0.4,
                },
            ],
            ..Default::default()
        };
        let greedy = inputs.allocate_greedy(&rs, 0).unwrap();
        let lp = inputs.allocate_lp(&rs, 0).unwrap();
        assert_eq!(greedy.result.collateral_cover, vec![80.0, 20.0]);
        assert_eq!(lp.result.collateral_cover, vec![40.0, 40.0]);

        let diff = greedy_lp_diff(&greedy, &lp);
        assert_eq!(diff.len(), 2);
        assert_eq!(diff[0].loan_id, 1);
        assert_eq!(diff[0].greedy_net, 0.0);
        assert_eq!(diff[0].lp_net, 40.0);
        assert_eq!(diff[0].delta, 40.0);
        assert_eq!(diff[1].delta, -20.0);
    }

    #[test]
    fn duplicate_and_dangling_inputs_are_rejected() {
        let dup = GovernedInputs {
            exposures: vec![
                Exposure::new(1, "CP1", 1.0, "USD"),
                Exposure::new(1, "CP1", 1.0, "USD"),
            ],
            ..Default::default()
        };
        assert!(matches!(
            dup.allocate_greedy(&rules(0.0), 0),
            Err(GovernanceError::DuplicateExposure(1))
        ));

        let dangling = GovernedInputs {
            exposures: vec![Exposure::new(1, "CP1", 1.0, "USD")],
            collateral_pledges: vec![CollateralPledge {
                col_id: 99,
                loan_id: 1,
                ratio: 1.0,
            }],
            ..Default::default()
        };
        assert!(matches!(
            dangling.allocate_greedy(&rules(0.0), 0),
            Err(GovernanceError::DanglingCollateralPledge { col_id: 99, .. })
        ));
    }

    #[test]
    fn allocation_is_deterministic_with_full_audit_trail() {
        let inputs = GovernedInputs {
            exposures: vec![
                Exposure::new(1, "CP1", 100.0, "USD").with_priority(1.0),
                Exposure::new(2, "CP2", 100.0, "USD").with_priority(2.0),
            ],
            collaterals: vec![GovernedCollateral::new(10, "cash", 150.0, "USD")],
            collateral_pledges: vec![
                CollateralPledge {
                    col_id: 10,
                    loan_id: 1,
                    ratio: 1.0,
                },
                CollateralPledge {
                    col_id: 10,
                    loan_id: 2,
                    ratio: 1.0,
                },
            ],
            ..Default::default()
        };
        let a = inputs.allocate_greedy(&rules(0.0), 0).unwrap();
        let b = inputs.allocate_greedy(&rules(0.0), 0).unwrap();
        assert_eq!(a.result.net_exposure, b.result.net_exposure);
        assert_eq!(a.result.allocations.len(), b.result.allocations.len());
        assert_eq!(a.rule_version, 1);
        // audit: covers never exceed supply or demand
        assert!(a.result.collateral_cover.iter().sum::<f64>() <= 150.0 + 1e-9);
        assert!(a
            .result
            .allocations
            .iter()
            .all(|x| x.loan_remaining >= -1e-9 && x.source_remaining >= -1e-9));
    }
}
