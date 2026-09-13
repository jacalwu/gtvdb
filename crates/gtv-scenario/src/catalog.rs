//! Versioned scenario catalog with inheritance / override resolution.

use std::collections::{BTreeMap, HashSet};

use crate::scenario::{
    Dimension, Provenance, ResolvedScenario, ResolvedShock, Scenario, ScenarioError,
};

/// An in-memory, append-only catalog of immutable scenario versions.
///
/// Corrections are new versions, never in-place edits (bitemporal semantics).
/// Shock keys are `(factor, dimension)`; a child overrides a parent only for
/// the exact same key.
#[derive(Debug, Default, Clone)]
pub struct ScenarioCatalog {
    entries: BTreeMap<(String, u32), Scenario>,
}

impl ScenarioCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register one immutable version. Duplicate `(id, version)` is rejected.
    pub fn register(&mut self, scenario: Scenario) -> Result<(), ScenarioError> {
        scenario.validate()?;
        let key = (scenario.id.clone(), scenario.version);
        if self.entries.contains_key(&key) {
            return Err(ScenarioError::Duplicate {
                id: scenario.id,
                version: scenario.version,
            });
        }
        self.entries.insert(key, scenario);
        Ok(())
    }

    pub fn get(&self, id: &str, version: u32) -> Option<&Scenario> {
        self.entries.get(&(id.to_string(), version))
    }

    pub fn contains(&self, id: &str, version: u32) -> bool {
        self.entries.contains_key(&(id.to_string(), version))
    }

    /// All registered versions of `id`, ascending.
    pub fn versions(&self, id: &str) -> Vec<u32> {
        self.entries
            .keys()
            .filter(|(sid, _)| sid == id)
            .map(|(_, v)| *v)
            .collect()
    }

    pub fn latest_version(&self, id: &str) -> Option<u32> {
        self.versions(id).into_iter().max()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Resolve `(id, version)` along its inheritance chain.
    ///
    /// Walks the parent chain, rejects cycles / missing parents, then applies
    /// shocks root → leaf so a child overrides its ancestors on the exact same
    /// `(factor, dimension)` key. The output ordering and provenance are
    /// deterministic.
    pub fn resolve(&self, id: &str, version: u32) -> Result<ResolvedScenario, ScenarioError> {
        // Walk target → root.
        let mut chain_rev: Vec<&Scenario> = Vec::new();
        let mut seen: HashSet<(String, u32)> = HashSet::new();
        let mut cursor = self
            .get(id, version)
            .ok_or_else(|| ScenarioError::NotFound {
                id: id.to_string(),
                version,
            })?;
        loop {
            let key = (cursor.id.clone(), cursor.version);
            if !seen.insert(key) {
                return Err(ScenarioError::CyclicInheritance {
                    id: cursor.id.clone(),
                    version: cursor.version,
                });
            }
            chain_rev.push(cursor);
            match &cursor.parent {
                None => break,
                Some((pid, pv)) => {
                    cursor = self.get(pid, *pv).ok_or_else(|| {
                        ScenarioError::ParentNotFound {
                            id: cursor.id.clone(),
                            version: cursor.version,
                            parent: pid.clone(),
                            parent_version: *pv,
                        }
                    })?;
                }
            }
        }

        // Root → target.
        chain_rev.reverse();
        let leaf = *chain_rev.last().expect("chain is non-empty");

        let mut merged: BTreeMap<(String, Dimension), (f64, Provenance)> = BTreeMap::new();
        // `source_cutoff == 0` / empty `model_version` mean "unset"; the child
        // inherits the nearest ancestor that sets them.
        let mut source_cutoff: Option<i64> = None;
        let mut model_version: Option<String> = None;
        for scenario in &chain_rev {
            if scenario.source_cutoff != 0 {
                source_cutoff = Some(scenario.source_cutoff);
            }
            if !scenario.model_version.is_empty() {
                model_version = Some(scenario.model_version.clone());
            }
            let provenance = Provenance {
                scenario_id: scenario.id.clone(),
                version: scenario.version,
            };
            for shock in &scenario.shocks {
                merged.insert(
                    (shock.factor.clone(), shock.dimension.clone()),
                    (shock.value, provenance.clone()),
                );
            }
        }

        let shocks = merged
            .into_iter()
            .map(|((factor, dimension), (value, provenance))| ResolvedShock {
                factor,
                dimension,
                value,
                provenance,
            })
            .collect();
        let chain = chain_rev
            .iter()
            .map(|s| (s.id.clone(), s.version))
            .collect::<Vec<_>>();

        Ok(ResolvedScenario {
            id: leaf.id.clone(),
            version: leaf.version,
            kind: leaf.kind,
            chain,
            source_cutoff: source_cutoff.unwrap_or(0),
            model_version: model_version.unwrap_or_default(),
            shocks,
        })
    }

    /// Resolve the highest registered version of `id`.
    pub fn resolve_latest(&self, id: &str) -> Result<ResolvedScenario, ScenarioError> {
        let version = self
            .latest_version(id)
            .ok_or_else(|| ScenarioError::NoVersions { id: id.to_string() })?;
        self.resolve(id, version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scenario::{Change, ScenarioKind, ScenarioStatus, Shock};

    fn base() -> Scenario {
        Scenario::new("base", 1, ScenarioKind::Baseline)
            .with_shocks(vec![Shock::new("IR.USD.5Y", 0.02), Shock::new("FX.USDCNY", 7.0)])
            .with_source_cutoff(1_000)
            .with_model_version("mdl-1")
            .with_status(ScenarioStatus::Approved)
    }

    #[test]
    fn resolve_inherits_then_overrides() {
        let mut cat = ScenarioCatalog::new();
        cat.register(base()).unwrap();
        cat.register(
            Scenario::new("stress", 1, ScenarioKind::Stress)
                .with_parent("base", 1)
                .with_shocks(vec![Shock::new("IR.USD.5Y", 0.05)]),
        )
        .unwrap();

        let r = cat.resolve("stress", 1).unwrap();
        assert_eq!(r.chain, vec![("base".into(), 1), ("stress".into(), 1)]);
        assert_eq!(r.kind, ScenarioKind::Stress);
        assert_eq!(r.value("IR.USD.5Y", &Dimension::all()), Some(0.05));
        assert_eq!(r.value("FX.USDCNY", &Dimension::all()), Some(7.0));

        let ir = r
            .shocks
            .iter()
            .find(|s| s.factor == "IR.USD.5Y")
            .unwrap();
        assert_eq!(ir.provenance.scenario_id, "stress");
        let fx = r.shocks.iter().find(|s| s.factor == "FX.USDCNY").unwrap();
        assert_eq!(fx.provenance.scenario_id, "base");
        // metadata is inherited from the nearest ancestor that sets it
        assert_eq!(r.source_cutoff, 1_000);
        assert_eq!(r.model_version, "mdl-1");
    }

    #[test]
    fn resolution_is_deterministic() {
        let mut cat = ScenarioCatalog::new();
        cat.register(base()).unwrap();
        cat.register(
            Scenario::new("stress", 1, ScenarioKind::Stress)
                .with_parent("base", 1)
                .with_shocks(vec![Shock::new("IR.USD.5Y", 0.05)]),
        )
        .unwrap();
        assert_eq!(cat.resolve("stress", 1).unwrap(), cat.resolve("stress", 1).unwrap());
    }

    #[test]
    fn dimension_is_part_of_the_override_key() {
        let mut cat = ScenarioCatalog::new();
        cat.register(
            Scenario::new("base", 1, ScenarioKind::Baseline).with_shocks(vec![
                Shock::new("IR", 0.02),
                Shock::new("IR", 0.03).with_dimension(Dimension::all().currency("USD")),
            ]),
        )
        .unwrap();
        cat.register(
            Scenario::new("stress", 1, ScenarioKind::Stress)
                .with_parent("base", 1)
                .with_shocks(vec![Shock::new("IR", 0.05)]),
        )
        .unwrap();

        let r = cat.resolve("stress", 1).unwrap();
        assert_eq!(r.value("IR", &Dimension::all()), Some(0.05));
        assert_eq!(
            r.value("IR", &Dimension::all().currency("USD")),
            Some(0.03)
        );
    }

    #[test]
    fn cyclic_inheritance_is_rejected() {
        let mut cat = ScenarioCatalog::new();
        cat.register(
            Scenario::new("a", 1, ScenarioKind::Stress).with_parent("b", 1),
        )
        .unwrap();
        cat.register(
            Scenario::new("b", 1, ScenarioKind::Stress).with_parent("a", 1),
        )
        .unwrap();
        assert_eq!(
            cat.resolve("a", 1),
            Err(ScenarioError::CyclicInheritance {
                id: "a".into(),
                version: 1
            })
        );
    }

    #[test]
    fn missing_parent_is_rejected() {
        let mut cat = ScenarioCatalog::new();
        cat.register(
            Scenario::new("orphan", 1, ScenarioKind::Adverse).with_parent("ghost", 9),
        )
        .unwrap();
        assert_eq!(
            cat.resolve("orphan", 1),
            Err(ScenarioError::ParentNotFound {
                id: "orphan".into(),
                version: 1,
                parent: "ghost".into(),
                parent_version: 9,
            })
        );
    }

    #[test]
    fn duplicate_registration_is_rejected() {
        let mut cat = ScenarioCatalog::new();
        cat.register(base()).unwrap();
        assert_eq!(
            cat.register(base()),
            Err(ScenarioError::Duplicate {
                id: "base".into(),
                version: 1
            })
        );
    }

    #[test]
    fn invalid_shocks_are_rejected() {
        let mut cat = ScenarioCatalog::new();
        let err = cat
            .register(Scenario::new("bad", 1, ScenarioKind::Stress).with_shocks(vec![
                Shock::new("", 1.0),
            ]))
            .unwrap_err();
        assert!(matches!(err, ScenarioError::Invalid { .. }));

        let err = cat
            .register(Scenario::new("nan", 1, ScenarioKind::Stress).with_shocks(vec![
                Shock::new("IR", f64::NAN),
            ]))
            .unwrap_err();
        assert!(matches!(err, ScenarioError::Invalid { .. }));
    }

    #[test]
    fn latest_version_and_resolve_latest() {
        let mut cat = ScenarioCatalog::new();
        cat.register(base()).unwrap();
        cat.register(
            Scenario::new("base", 2, ScenarioKind::Baseline)
                .with_shocks(vec![Shock::new("IR.USD.5Y", 0.01)]),
        )
        .unwrap();
        assert_eq!(cat.versions("base"), vec![1, 2]);
        assert_eq!(cat.latest_version("base"), Some(2));
        let r = cat.resolve_latest("base").unwrap();
        assert_eq!(r.version, 2);
        assert_eq!(r.value("IR.USD.5Y", &Dimension::all()), Some(0.01));
        assert_eq!(
            cat.resolve_latest("ghost"),
            Err(ScenarioError::NoVersions { id: "ghost".into() })
        );
    }

    #[test]
    fn diff_reports_added_removed_and_changed() {
        let mut cat = ScenarioCatalog::new();
        cat.register(base()).unwrap();
        cat.register(
            Scenario::new("stress", 1, ScenarioKind::Stress)
                .with_parent("base", 1)
                .with_shocks(vec![Shock::new("IR.USD.5Y", 0.05), Shock::new("EQ", -0.3)]),
        )
        .unwrap();

        let a = cat.resolve("base", 1).unwrap();
        let b = cat.resolve("stress", 1).unwrap();
        let diff = a.diff(&b);
        assert_eq!(diff.len(), 2);
        // sorted by (factor, dimension): EQ then IR.USD.5Y
        assert_eq!(diff[0].factor, "EQ");
        assert_eq!(diff[0].change, Change::Added(-0.3));
        assert_eq!(diff[1].factor, "IR.USD.5Y");
        assert_eq!(
            diff[1].change,
            Change::Changed {
                from: 0.02,
                to: 0.05
            }
        );
        // symmetrical: b vs a shows Removed
        let back = b.diff(&a);
        assert_eq!(back[0].change, Change::Removed(-0.3));
    }
}
