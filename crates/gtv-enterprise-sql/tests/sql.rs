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
