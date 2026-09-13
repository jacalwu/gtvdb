//! Scenario types, inherited resolution results and reconciliation diffs.

use std::collections::BTreeMap;

use thiserror::Error;

/// The four scenario families required by the roadmap (P2.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ScenarioKind {
    Baseline,
    Stress,
    Adverse,
    ReverseStress,
}

impl ScenarioKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ScenarioKind::Baseline => "baseline",
            ScenarioKind::Stress => "stress",
            ScenarioKind::Adverse => "adverse",
            ScenarioKind::ReverseStress => "reverse_stress",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().replace('-', "_").as_str() {
            "baseline" | "base" => Some(ScenarioKind::Baseline),
            "stress" => Some(ScenarioKind::Stress),
            "adverse" => Some(ScenarioKind::Adverse),
            "reverse_stress" | "reversestress" | "reverse" => {
                Some(ScenarioKind::ReverseStress)
            }
            _ => None,
        }
    }
}

/// Lifecycle status of one immutable scenario version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ScenarioStatus {
    Draft,
    Approved,
    Retired,
}

impl ScenarioStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            ScenarioStatus::Draft => "draft",
            ScenarioStatus::Approved => "approved",
            ScenarioStatus::Retired => "retired",
        }
    }
}

/// The four orthogonal risk dimensions (P2.1). `None` means "all".
///
/// Field order is significant: it defines the deterministic ordering used by
/// [`ResolvedScenario`] and [`ResolvedScenario::diff`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Dimension {
    pub legal_entity: Option<String>,
    pub portfolio: Option<String>,
    pub product: Option<String>,
    pub currency: Option<String>,
}

impl Dimension {
    /// A dimension that applies to everything (all fields `None`).
    pub fn all() -> Self {
        Self::default()
    }

    pub fn legal_entity(mut self, v: impl Into<String>) -> Self {
        self.legal_entity = Some(v.into());
        self
    }

    pub fn portfolio(mut self, v: impl Into<String>) -> Self {
        self.portfolio = Some(v.into());
        self
    }

    pub fn product(mut self, v: impl Into<String>) -> Self {
        self.product = Some(v.into());
        self
    }

    pub fn currency(mut self, v: impl Into<String>) -> Self {
        self.currency = Some(v.into());
        self
    }

    pub fn is_all(&self) -> bool {
        self.legal_entity.is_none()
            && self.portfolio.is_none()
            && self.product.is_none()
            && self.currency.is_none()
    }
}

/// One factor shock, scoped by a [`Dimension`].
#[derive(Debug, Clone, PartialEq)]
pub struct Shock {
    pub factor: String,
    pub dimension: Dimension,
    pub value: f64,
}

impl Shock {
    pub fn new(factor: impl Into<String>, value: f64) -> Self {
        Self {
            factor: factor.into(),
            dimension: Dimension::all(),
            value,
        }
    }

    pub fn with_dimension(mut self, dimension: Dimension) -> Self {
        self.dimension = dimension;
        self
    }
}

/// One immutable, versioned scenario. A child only declares its deltas and
/// points at a `parent`; resolution merges the whole chain.
#[derive(Debug, Clone, PartialEq)]
pub struct Scenario {
    pub id: String,
    pub version: u32,
    pub kind: ScenarioKind,
    pub parent: Option<(String, u32)>,
    pub dimensions: Dimension,
    pub shocks: Vec<Shock>,
    /// Business/source cutoff this scenario was calibrated against.
    /// `0` means unset; resolution inherits the nearest ancestor that sets it.
    pub source_cutoff: i64,
    /// Model version this scenario belongs to. Empty means unset; resolution
    /// inherits the nearest ancestor that sets it.
    pub model_version: String,
    pub status: ScenarioStatus,
}

impl Scenario {
    pub fn new(id: impl Into<String>, version: u32, kind: ScenarioKind) -> Self {
        Self {
            id: id.into(),
            version,
            kind,
            parent: None,
            dimensions: Dimension::all(),
            shocks: Vec::new(),
            source_cutoff: 0,
            model_version: String::new(),
            status: ScenarioStatus::Draft,
        }
    }

    pub fn with_parent(mut self, parent: impl Into<String>, version: u32) -> Self {
        self.parent = Some((parent.into(), version));
        self
    }

    pub fn with_shocks(mut self, shocks: Vec<Shock>) -> Self {
        self.shocks = shocks;
        self
    }

    pub fn with_source_cutoff(mut self, cutoff: i64) -> Self {
        self.source_cutoff = cutoff;
        self
    }

    pub fn with_model_version(mut self, model: impl Into<String>) -> Self {
        self.model_version = model.into();
        self
    }

    pub fn with_status(mut self, status: ScenarioStatus) -> Self {
        self.status = status;
        self
    }

    /// Validate the scenario in isolation (parent existence is checked at
    /// resolution time because registrations may arrive out of order).
    pub fn validate(&self) -> Result<(), ScenarioError> {
        let invalid = |reason: &str| ScenarioError::Invalid {
            id: self.id.clone(),
            version: self.version,
            reason: reason.to_string(),
        };
        if self.id.trim().is_empty() {
            return Err(invalid("empty scenario id"));
        }
        if self.version == 0 {
            return Err(invalid("version must be >= 1"));
        }
        if let Some((pid, pv)) = &self.parent {
            if pid == &self.id && *pv == self.version {
                return Err(invalid("scenario cannot inherit from itself"));
            }
        }
        for shock in &self.shocks {
            if shock.factor.trim().is_empty() {
                return Err(invalid("shock factor must not be empty"));
            }
            if !shock.value.is_finite() {
                return Err(invalid(&format!(
                    "shock `{}` value must be finite",
                    shock.factor
                )));
            }
        }
        Ok(())
    }
}

/// Where a resolved shock value came from.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Provenance {
    pub scenario_id: String,
    pub version: u32,
}

/// One shock after inheritance / override resolution.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedShock {
    pub factor: String,
    pub dimension: Dimension,
    pub value: f64,
    pub provenance: Provenance,
}

/// The fully-resolved scenario, with a deterministic shock ordering and the
/// inheritance chain that produced it.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedScenario {
    pub id: String,
    pub version: u32,
    pub kind: ScenarioKind,
    /// Root → target chain of `(scenario_id, version)`.
    pub chain: Vec<(String, u32)>,
    pub source_cutoff: i64,
    pub model_version: String,
    pub shocks: Vec<ResolvedShock>,
}

impl ResolvedScenario {
    /// Look up a factor at an exact dimension.
    pub fn value(&self, factor: &str, dimension: &Dimension) -> Option<f64> {
        self.shocks
            .iter()
            .find(|s| s.factor == factor && &s.dimension == dimension)
            .map(|s| s.value)
    }

    fn shock_map(&self) -> BTreeMap<(String, Dimension), f64> {
        self.shocks
            .iter()
            .map(|s| ((s.factor.clone(), s.dimension.clone()), s.value))
            .collect()
    }

    /// Deterministic per-shock reconciliation between two resolved scenarios.
    pub fn diff(&self, other: &Self) -> Vec<ScenarioDiff> {
        let a = self.shock_map();
        let b = other.shock_map();
        let mut keys: Vec<&(String, Dimension)> = a.keys().chain(b.keys()).collect();
        keys.sort();
        keys.dedup();

        keys.into_iter()
            .filter_map(|key| {
                let change = match (a.get(key), b.get(key)) {
                    (None, Some(to)) => Some(Change::Added(*to)),
                    (Some(from), None) => Some(Change::Removed(*from)),
                    (Some(from), Some(to)) if from != to => Some(Change::Changed {
                        from: *from,
                        to: *to,
                    }),
                    _ => None,
                }?;
                Some(ScenarioDiff {
                    factor: key.0.clone(),
                    dimension: key.1.clone(),
                    change,
                })
            })
            .collect()
    }
}

/// A single difference between two resolved scenarios.
#[derive(Debug, Clone, PartialEq)]
pub enum Change {
    Added(f64),
    Removed(f64),
    Changed { from: f64, to: f64 },
}

/// One entry in a [`ResolvedScenario::diff`] report.
#[derive(Debug, Clone, PartialEq)]
pub struct ScenarioDiff {
    pub factor: String,
    pub dimension: Dimension,
    pub change: Change,
}

/// Errors from catalog registration and resolution.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ScenarioError {
    #[error("scenario `{id}` version {version} not found")]
    NotFound { id: String, version: u32 },
    #[error("scenario `{id}` version {version} already registered")]
    Duplicate { id: String, version: u32 },
    #[error("scenario `{id}` v{version} parent `{parent}` v{parent_version} not found")]
    ParentNotFound {
        id: String,
        version: u32,
        parent: String,
        parent_version: u32,
    },
    #[error("cyclic scenario inheritance at `{id}` version {version}")]
    CyclicInheritance { id: String, version: u32 },
    #[error("no versions registered for scenario `{id}`")]
    NoVersions { id: String },
    #[error("invalid scenario `{id}` v{version}: {reason}")]
    Invalid {
        id: String,
        version: u32,
        reason: String,
    },
}
