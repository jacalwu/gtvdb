//! gtv-enterprise-sql: DataFusion UDF / table-function adapters for the
//! enterprise domain crates (prod_p4 D1 / D6).
//!
//! SQL surface:
//!
//! | function | kind | arguments |
//! |---|---|---|
//! | `resolve_scenario(name [, version])` | table | resolved shocks with provenance |
//! | `hierarchy_ancestors(kind, node, as_of)` | table | transitive ancestors |
//! | `hierarchy_descendants(kind, node, as_of)` | table | transitive descendants |
//! | `master_get(kind, id, as_of)` | table | one row per attribute |
//! | `refdata_get(domain, key, as_of)` | scalar | effective-dated value |
//!
//! The registry is populated from session tables by [`load`], then registered
//! on a DataFusion [`SessionContext`] with [`register`]. The analysis engine is
//! never modified: the composition root (CLI / server) owns the registry and
//! calls these two functions.

pub mod load;
pub mod registry;
pub mod udf;

pub use registry::{EnterpriseRegistry, Registry};
pub use udf::{
    CrmAllocV2TableFunction, CrmExplainV2TableFunction, FtpPriceTableFunction, HierarchyDirection,
    HierarchyTableFunction, IrrbbEveTableFunction, MasterGetTableFunction, RefdataGetUdf,
    ResolveScenarioTableFunction,
};

// Re-export the domain identifiers callers need, so composition roots only
// depend on this one crate.
pub use gtv_refdata::{HierarchyKind, MasterKind};
pub use gtv_scenario::ScenarioCatalog;

use std::sync::Arc;

use datafusion::error::Result;
use datafusion::prelude::SessionContext;

/// Register every enterprise table function / scalar UDF on `session`.
pub fn register(session: &SessionContext, registry: Registry) -> Result<()> {
    session.register_udtf(
        "resolve_scenario",
        Arc::new(ResolveScenarioTableFunction::new(registry.clone())),
    );
    session.register_udtf(
        "hierarchy_ancestors",
        Arc::new(HierarchyTableFunction::new(
            registry.clone(),
            HierarchyDirection::Ancestors,
        )),
    );
    session.register_udtf(
        "hierarchy_descendants",
        Arc::new(HierarchyTableFunction::new(
            registry.clone(),
            HierarchyDirection::Descendants,
        )),
    );
    session.register_udtf(
        "master_get",
        Arc::new(MasterGetTableFunction::new(registry.clone())),
    );
    session.register_udtf(
        "irrbb_eve",
        Arc::new(IrrbbEveTableFunction::new(registry.clone())),
    );
    session.register_udtf(
        "ftp_price",
        Arc::new(FtpPriceTableFunction::new(registry.clone())),
    );
    session.register_udtf(
        "crm_alloc_v2",
        Arc::new(CrmAllocV2TableFunction::new(registry.clone())),
    );
    session.register_udtf(
        "crm_explain_v2",
        Arc::new(CrmExplainV2TableFunction::new(registry.clone())),
    );
    session.register_udf(datafusion::logical_expr::ScalarUDF::from(
        RefdataGetUdf::new(registry),
    ));
    Ok(())
}
