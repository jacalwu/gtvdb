//! Shared in-memory enterprise registry.

use std::sync::{Arc, RwLock};

use gtv_refdata::{Hierarchy, MasterData, ReferenceData};
use gtv_scenario::ScenarioCatalog;

/// Scenarios, hierarchies, reference data and master data used by the
/// enterprise SQL surface.
#[derive(Debug, Default)]
pub struct EnterpriseRegistry {
    pub scenarios: ScenarioCatalog,
    pub hierarchy: Hierarchy,
    pub reference: ReferenceData,
    pub master: MasterData,
}

impl EnterpriseRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a shared handle ready to be cloned into the table functions.
    pub fn handle() -> Registry {
        Arc::new(RwLock::new(Self::new()))
    }
}

/// Shared handle to the [`EnterpriseRegistry`].
pub type Registry = Arc<RwLock<EnterpriseRegistry>>;
