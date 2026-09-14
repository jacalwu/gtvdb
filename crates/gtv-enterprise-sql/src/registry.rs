//! Shared in-memory enterprise registry.

use std::sync::{Arc, RwLock};

use gtv_governance::RuleRegistry;
use gtv_refdata::{Hierarchy, MasterData, ReferenceData};
use gtv_scenario::{
    AlmCube, DiscountCurve, FtpCurveCatalog, FtpPolicyCatalog, IrrbbConfig, ScenarioCatalog,
    ShockTable,
};

/// Scenarios, hierarchies, reference data, master data and the config-driven
/// ALM / IRRBB / FTP / CRM state used by the enterprise SQL surface.
#[derive(Debug)]
pub struct EnterpriseRegistry {
    pub scenarios: ScenarioCatalog,
    pub hierarchy: Hierarchy,
    pub reference: ReferenceData,
    pub master: MasterData,
    // --- ALM / IRRBB ---
    pub alm_cube: AlmCube,
    pub irrbb_config: IrrbbConfig,
    /// Optional override for the specified-shock table; when `None`,
    /// `irrbb_eve` builds the table from the regulator + reporting year.
    pub irrbb_shock_override: Option<ShockTable>,
    pub irrbb_base_curve: Option<DiscountCurve>,
    // --- FTP ---
    pub ftp_curves: FtpCurveCatalog,
    pub ftp_policies: FtpPolicyCatalog,
    // --- CRM governance ---
    pub crm_rules: RuleRegistry,
}

impl Default for EnterpriseRegistry {
    fn default() -> Self {
        Self {
            scenarios: ScenarioCatalog::new(),
            hierarchy: Hierarchy::new(),
            reference: ReferenceData::new(),
            master: MasterData::new(),
            alm_cube: AlmCube::new(),
            irrbb_config: IrrbbConfig::default(),
            irrbb_shock_override: None,
            irrbb_base_curve: None,
            ftp_curves: FtpCurveCatalog::new(),
            ftp_policies: FtpPolicyCatalog::new(),
            crm_rules: RuleRegistry::new(),
        }
    }
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
