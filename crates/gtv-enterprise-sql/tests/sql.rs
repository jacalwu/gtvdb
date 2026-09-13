//! End-to-end SQL tests for the enterprise surface: load session tables into
//! the registry, register the UDFs on a DataFusion context and query them.

use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int64Array, StringArray, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use gtv_enterprise_sql::{load, EnterpriseRegistry, HierarchyKind, MasterKind};

fn batch(schema: Schema, columns: Vec<Arc<dyn Array>>) -> RecordBatch {
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

fn scenario_batch() -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("scenario_id", DataType::Utf8, false),
        Field::new("version", DataType::UInt32, false),
        Field::new("kind", DataType::Utf8, false),
        Field::new("factor", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
        Field::new("parent_id", DataType::Utf8, true),
        Field::new("parent_version", DataType::Int64, true),
        Field::new("source_cutoff", DataType::Int64, false),
        Field::new("model_version", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("dim_currency", DataType::Utf8, true),
    ]);
    batch(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["base", "base", "stress"])) as Arc<dyn Array>,
            Arc::new(UInt32Array::from(vec![1u32, 1, 1])),
            Arc::new(StringArray::from(vec!["baseline", "baseline", "stress"])),
            Arc::new(StringArray::from(vec!["IR", "FX", "IR"])),
            Arc::new(Float64Array::from(vec![0.02, 7.0, 0.05])),
            Arc::new(StringArray::from(vec![None, None, Some("base")])),
            Arc::new(Int64Array::from(vec![None, None, Some(1i64)])),
            Arc::new(Int64Array::from(vec![1000i64, 1000, 0])),
            Arc::new(StringArray::from(vec!["mdl-1", "mdl-1", ""])),
            Arc::new(StringArray::from(vec!["approved", "approved", "draft"])),
            Arc::new(StringArray::from(vec![None::<&str>, None, None])),
        ],
    )
}

fn hierarchy_batch() -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("parent", DataType::Utf8, false),
        Field::new("child", DataType::Utf8, false),
        Field::new("valid_from", DataType::Int64, false),
        Field::new("valid_to", DataType::Int64, true),
    ]);
    batch(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["root", "a"])) as Arc<dyn Array>,
            Arc::new(StringArray::from(vec!["a", "a1"])),
            Arc::new(Int64Array::from(vec![0i64, 0])),
            Arc::new(Int64Array::from(vec![None, None])),
        ],
    )
}

fn reference_batch() -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("domain", DataType::Utf8, false),
        Field::new("key", DataType::Utf8, false),
        Field::new("valid_from", DataType::Int64, false),
        Field::new("valid_to", DataType::Int64, true),
        Field::new("value", DataType::Utf8, false),
    ]);
    batch(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["curve"])) as Arc<dyn Array>,
            Arc::new(StringArray::from(vec!["USD.5Y"])),
            Arc::new(Int64Array::from(vec![0i64])),
            Arc::new(Int64Array::from(vec![None])),
            Arc::new(StringArray::from(vec!["0.02"])),
        ],
    )
}

fn master_batch() -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("valid_from", DataType::Int64, false),
        Field::new("valid_to", DataType::Int64, true),
        Field::new("customer", DataType::Utf8, true),
    ]);
    batch(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["A1"])) as Arc<dyn Array>,
            Arc::new(Int64Array::from(vec![0i64])),
            Arc::new(Int64Array::from(vec![None])),
            Arc::new(StringArray::from(vec![Some("C1")])),
        ],
    )
}

async fn context() -> SessionContext {
    let registry = EnterpriseRegistry::handle();
    load::load_scenarios(&registry, &[scenario_batch()]).unwrap();
    load::load_hierarchy(&registry, HierarchyKind::LegalEntity, &[hierarchy_batch()]).unwrap();
    load::load_reference(&registry, &[reference_batch()]).unwrap();
    load::load_master(&registry, MasterKind::Account, &[master_batch()]).unwrap();
    let ctx = SessionContext::new();
    gtv_enterprise_sql::register(&ctx, registry).unwrap();
    ctx
}

fn string_col(batches: &[RecordBatch], index: usize) -> Vec<String> {
    let mut out = Vec::new();
    for b in batches {
        let a = b.column(index).as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..a.len() {
            out.push(a.value(i).to_string());
        }
    }
    out
}

fn f64_col(batches: &[RecordBatch], index: usize) -> Vec<f64> {
    let mut out = Vec::new();
    for b in batches {
        let a = b.column(index).as_any().downcast_ref::<Float64Array>().unwrap();
        for i in 0..a.len() {
            out.push(a.value(i));
        }
    }
    out
}

#[tokio::test]
async fn resolve_scenario_returns_shocks_with_provenance() {
    let ctx = context().await;
    let batches = ctx
        .sql(
            "SELECT factor, value, source_scenario, source_version, chain \
             FROM resolve_scenario('stress', 1) ORDER BY factor",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(string_col(&batches, 0), vec!["FX", "IR"]);
    assert_eq!(f64_col(&batches, 1), vec![7.0, 0.05]);
    assert_eq!(string_col(&batches, 2), vec!["base", "stress"]);
    assert_eq!(string_col(&batches, 4), vec!["base:1>stress:1", "base:1>stress:1"]);
}

#[tokio::test]
async fn resolve_scenario_latest_and_unknown() {
    let ctx = context().await;
    let batches = ctx
        .sql("SELECT count(*) AS n FROM resolve_scenario('stress')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
    );

    let err = async {
        let df = ctx.sql("SELECT * FROM resolve_scenario('ghost', 1)").await?;
        df.collect().await
    }
    .await
    .unwrap_err();
    assert!(err.to_string().contains("not found"));
}

#[tokio::test]
async fn hierarchy_ancestors_and_descendants() {
    let ctx = context().await;
    let up = ctx
        .sql(
            "SELECT related FROM hierarchy_ancestors('legal_entity', 'a1', 0) ORDER BY related",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(string_col(&up, 0), vec!["a", "root"]);

    let down = ctx
        .sql("SELECT related FROM hierarchy_descendants('legal_entity', 'root', 0) ORDER BY related")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(string_col(&down, 0), vec!["a", "a1"]);
}

#[tokio::test]
async fn refdata_get_is_effective_dated() {
    let ctx = context().await;
    let batches = ctx
        .sql("SELECT refdata_get('curve', 'USD.5Y', 50) AS v")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(string_col(&batches, 0), vec!["0.02"]);

    let missing = ctx
        .sql("SELECT refdata_get('curve', 'USD.5Y', -1) AS v")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert!(missing[0].column(0).is_null(0));
}

#[tokio::test]
async fn master_get_explodes_attributes() {
    let ctx = context().await;
    let batches = ctx
        .sql("SELECT attr_key, attr_value FROM master_get('account', 'A1', 0) ORDER BY attr_key")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(string_col(&batches, 0), vec!["customer"]);
    assert_eq!(string_col(&batches, 1), vec!["C1"]);

    let none = ctx
        .sql("SELECT * FROM master_get('account', 'GHOST', 0)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(none.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
}

// ---------------------------------------------------------------------------
// IRRBB config loaders (prod_p4 audit P0)
// ---------------------------------------------------------------------------

fn utf8(values: Vec<&str>) -> Arc<dyn Array> {
    Arc::new(StringArray::from(values))
}

fn f64s(values: Vec<f64>) -> Arc<dyn Array> {
    Arc::new(Float64Array::from(values))
}

#[test]
fn irrbb_config_loaders_override_defaults() {
    use gtv_scenario::{NmdCategory, ShockScenario, ShockTableVersion, IrrbbConfig};

    let mut config = IrrbbConfig::default();

    // scalar overrides
    let scalars = batch(
        Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]),
        vec![
            utf8(vec!["floor", "vol_bump", "decay_divisor"]),
            f64s(vec![-0.01, 1.5, 3.0]),
        ],
    );
    let n = gtv_enterprise_sql::load::load_irrbb_scalars(&mut config, &[scalars]).unwrap();
    assert_eq!(n, 3);
    assert_eq!(config.floor, -0.01);
    assert_eq!(config.vol_bump, 1.5);
    assert_eq!(config.shock_formula.decay_divisor, 3.0);

    // NMD caps (partial table keeps the other categories' defaults)
    let caps = batch(
        Schema::new(vec![
            Field::new("category", DataType::Utf8, false),
            Field::new("core_ratio_cap", DataType::Float64, false),
            Field::new("maturity_cap_years", DataType::Float64, false),
        ]),
        vec![
            utf8(vec!["non_retail"]),
            f64s(vec![0.8]),
            f64s(vec![2.0]),
        ],
    );
    gtv_enterprise_sql::load::load_irrbb_nmd_caps(&mut config, &[caps]).unwrap();
    assert_eq!(config.nmd_caps(NmdCategory::NonRetail).core_ratio_cap, 0.8);
    assert_eq!(
        config
            .nmd_caps(NmdCategory::RetailTransactional)
            .core_ratio_cap,
        0.90
    );

    // scenario multipliers
    let mults = batch(
        Schema::new(vec![
            Field::new("scenario", DataType::Utf8, false),
            Field::new("cpr_gamma", DataType::Float64, false),
            Field::new("tdrr_u", DataType::Float64, false),
        ]),
        vec![
            utf8(vec!["parallel up"]),
            f64s(vec![0.5]),
            f64s(vec![1.5]),
        ],
    );
    gtv_enterprise_sql::load::load_irrbb_scenario_multipliers(&mut config, &[mults]).unwrap();
    assert_eq!(config.cpr_multiplier(ShockScenario::ParallelUp), 0.5);
    assert_eq!(config.tdrr_multiplier(ShockScenario::ParallelUp), 1.5);

    // time bands (loaded in arbitrary order -> sorted by midpoint)
    let bands = batch(
        Schema::new(vec![
            Field::new("label", DataType::Utf8, false),
            Field::new("start_years", DataType::Float64, false),
            Field::new("end_years", DataType::Float64, false),
            Field::new("midpoint_years", DataType::Float64, false),
        ]),
        vec![
            utf8(vec!["1Y", "O/N"]),
            f64s(vec![0.75, 0.0]),
            f64s(vec![1.0, 0.0028]),
            f64s(vec![0.875, 0.0028]),
        ],
    );
    let n = gtv_enterprise_sql::load::load_irrbb_time_bands(&mut config, &[bands]).unwrap();
    assert_eq!(n, 2);
    assert_eq!(config.time_bands[0].label, "O/N");
    assert_eq!(config.time_bands[1].label, "1Y");

    // shock table
    let shocks = batch(
        Schema::new(vec![
            Field::new("currency", DataType::Utf8, false),
            Field::new("parallel_bps", DataType::Float64, false),
            Field::new("short_bps", DataType::Float64, false),
            Field::new("long_bps", DataType::Float64, false),
        ]),
        vec![
            utf8(vec!["HKD"]),
            f64s(vec![225.0]),
            f64s(vec![375.0]),
            f64s(vec![200.0]),
        ],
    );
    let table =
        gtv_enterprise_sql::load::load_shock_table(ShockTableVersion::Recalibrated2026, &[shocks])
            .unwrap();
    assert_eq!(table.params("HKD").parallel_bps, 225.0);
    assert_eq!(table.params("MOP").short_bps, 375.0);
}

fn i64s(values: Vec<i64>) -> Arc<dyn Array> {
    Arc::new(Int64Array::from(values))
}

#[test]
fn ftp_loaders_build_catalog_and_price() {
    use gtv_refdata::Hierarchy;
    use gtv_scenario::{FtpCurveCatalog, FtpEngine, FtpPolicyCatalog, FtpRequest};

    let curve_rows = batch(
        Schema::new(vec![
            Field::new("curve_id", DataType::Utf8, false),
            Field::new("version", DataType::Int64, false),
            Field::new("currency", DataType::Utf8, false),
            Field::new("effective_from", DataType::Int64, false),
            Field::new("effective_to", DataType::Int64, false),
            Field::new("tenor_days", DataType::Int64, false),
            Field::new("zero_rate", DataType::Float64, false),
        ]),
        vec![
            utf8(vec!["USD-OIS", "USD-OIS"]),
            i64s(vec![1, 1]),
            utf8(vec!["USD", "USD"]),
            i64s(vec![0, 0]),
            i64s(vec![i64::MAX, i64::MAX]),
            i64s(vec![0, 365]),
            f64s(vec![0.01, 0.03]),
        ],
    );
    let mut curves = FtpCurveCatalog::new();
    assert_eq!(
        gtv_enterprise_sql::load::load_ftp_curves(&mut curves, &[curve_rows]).unwrap(),
        1
    );
    assert_eq!(curves.get("USD-OIS", 1).unwrap().zero_rate(365), 0.03);

    let headers = batch(
        Schema::new(vec![
            Field::new("policy_id", DataType::Utf8, false),
            Field::new("version", DataType::Int64, false),
            Field::new("effective_from", DataType::Int64, false),
            Field::new("effective_to", DataType::Int64, false),
        ]),
        vec![utf8(vec!["P1"]), i64s(vec![1]), i64s(vec![0]), i64s(vec![i64::MAX])],
    );
    let liquidity = batch(
        Schema::new(vec![
            Field::new("policy_id", DataType::Utf8, false),
            Field::new("version", DataType::Int64, false),
            Field::new("product", DataType::Utf8, false),
            Field::new("tenor_days", DataType::Int64, false),
            Field::new("spread", DataType::Float64, false),
        ]),
        vec![
            utf8(vec!["P1"]),
            i64s(vec![1]),
            utf8(vec!["Loan"]),
            i64s(vec![365]),
            f64s(vec![0.015]),
        ],
    );
    let basis = batch(
        Schema::new(vec![
            Field::new("policy_id", DataType::Utf8, false),
            Field::new("version", DataType::Int64, false),
            Field::new("currency", DataType::Utf8, false),
            Field::new("tenor_days", DataType::Int64, false),
            Field::new("spread", DataType::Float64, false),
        ]),
        vec![
            utf8(vec!["P1"]),
            i64s(vec![1]),
            utf8(vec!["USD"]),
            i64s(vec![365]),
            f64s(vec![0.002]),
        ],
    );
    let optionality = batch(
        Schema::new(vec![
            Field::new("policy_id", DataType::Utf8, false),
            Field::new("version", DataType::Int64, false),
            Field::new("product", DataType::Utf8, false),
            Field::new("charge", DataType::Float64, false),
        ]),
        vec![utf8(vec!["P1"]), i64s(vec![1]), utf8(vec!["Loan"]), f64s(vec![0.004])],
    );
    let behavioural = batch(
        Schema::new(vec![
            Field::new("policy_id", DataType::Utf8, false),
            Field::new("version", DataType::Int64, false),
            Field::new("product", DataType::Utf8, false),
            Field::new("adjustment", DataType::Float64, false),
        ]),
        vec![
            utf8(vec!["P1"]),
            i64s(vec![1]),
            utf8(vec!["Loan"]),
            f64s(vec![-0.001]),
        ],
    );
    let mut policies = FtpPolicyCatalog::new();
    gtv_enterprise_sql::load::load_ftp_policies(
        &mut policies,
        &[headers],
        &[liquidity],
        &[basis],
        &[optionality],
        &[behavioural],
    )
    .unwrap();

    let hierarchy = Hierarchy::new();
    let engine = FtpEngine::new(&curves, &policies, &hierarchy);
    let b = engine
        .price(&FtpRequest::new("USD-OIS", 1, "P1", 1, "Loan", "USD", 0, 0, 365))
        .unwrap();
    assert!((b.liquidity_premium - 0.015).abs() < 1e-12);
    assert!((b.basis_spread - 0.002).abs() < 1e-12);
    assert!((b.total_rate - 0.05).abs() < 1e-9);
}

#[test]
fn crm_ruleset_loader_builds_registry() {
    use gtv_governance::RuleRegistry;

    let header = batch(
        Schema::new(vec![
            Field::new("ruleset_id", DataType::Utf8, false),
            Field::new("version", DataType::Int64, false),
            Field::new("effective_from", DataType::Int64, false),
            Field::new("effective_to", DataType::Int64, false),
        ]),
        vec![utf8(vec!["crm"]), i64s(vec![1]), i64s(vec![0]), i64s(vec![i64::MAX])],
    );
    let collateral = batch(
        Schema::new(vec![
            Field::new("ruleset_id", DataType::Utf8, false),
            Field::new("version", DataType::Int64, false),
            Field::new("collateral_type", DataType::Utf8, false),
            Field::new("priority", DataType::Float64, false),
            Field::new("eligible", DataType::Utf8, true),
            Field::new("haircut", DataType::Float64, true),
            Field::new("currencies", DataType::Utf8, true),
        ]),
        vec![
            utf8(vec!["crm", "crm"]),
            i64s(vec![1, 1]),
            utf8(vec!["cash", "equity"]),
            f64s(vec![10.0, 5.0]),
            Arc::new(StringArray::from(vec![Some("true"), Some("false")])) as Arc<dyn Array>,
            Arc::new(Float64Array::from(vec![Some(0.05), Some(0.2)])) as Arc<dyn Array>,
            Arc::new(StringArray::from(vec![Some("USD,HKD"), None])) as Arc<dyn Array>,
        ],
    );
    let guarantees = batch(
        Schema::new(vec![
            Field::new("ruleset_id", DataType::Utf8, false),
            Field::new("version", DataType::Int64, false),
            Field::new("guarantor_type", DataType::Utf8, false),
            Field::new("priority", DataType::Float64, false),
            Field::new("jurisdictions", DataType::Utf8, true),
        ]),
        vec![
            utf8(vec!["crm"]),
            i64s(vec![1]),
            utf8(vec!["bank"]),
            f64s(vec![7.0]),
            Arc::new(StringArray::from(vec![Some("HK")])) as Arc<dyn Array>,
        ],
    );
    let wrong_way = batch(
        Schema::new(vec![
            Field::new("ruleset_id", DataType::Utf8, false),
            Field::new("version", DataType::Int64, false),
            Field::new("counterparty", DataType::Utf8, false),
            Field::new("collateral_type", DataType::Utf8, false),
        ]),
        vec![utf8(vec!["crm"]), i64s(vec![1]), utf8(vec!["CP1"]), utf8(vec!["equity"])],
    );
    let concentration = batch(
        Schema::new(vec![
            Field::new("ruleset_id", DataType::Utf8, false),
            Field::new("version", DataType::Int64, false),
            Field::new("collateral_type", DataType::Utf8, false),
            Field::new("limit", DataType::Float64, false),
        ]),
        vec![utf8(vec!["crm"]), i64s(vec![1]), utf8(vec!["cash"]), f64s(vec![500.0])],
    );

    let mut registry = RuleRegistry::new();
    let n = gtv_enterprise_sql::load::load_crm_rulesets(
        &mut registry,
        &[header],
        &[collateral],
        &[guarantees],
        &[wrong_way],
        &[concentration],
        0,
    )
    .unwrap();
    assert_eq!(n, 1);
    let rs = registry.resolve_as_of("crm", 0).unwrap();
    assert_eq!(rs.collateral_rule("cash").unwrap().haircut, 0.05);
    assert_eq!(
        rs.collateral_rule("cash").unwrap().eligible_currencies,
        vec!["USD".to_string(), "HKD".to_string()]
    );
    assert!(!rs.collateral_rule("equity").unwrap().eligible);
    assert_eq!(rs.guarantee_rule("bank").unwrap().priority, 7.0);
    assert!(rs.is_wrong_way("CP1", "equity"));
    assert_eq!(rs.concentration_limit("cash"), Some(500.0));
}
