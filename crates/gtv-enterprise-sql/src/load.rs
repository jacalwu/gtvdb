//! Load enterprise registry content from Arrow session tables.
//!
//! Every loader is column-oriented and deterministic; missing optional columns
//! fall back to documented defaults (e.g. an absent `valid_to` means
//! "still current").

use std::collections::BTreeMap;

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray, UInt32Array};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use gtv_refdata::{EffectiveRange, HierarchyEdge, HierarchyKind, MasterKind, MasterRecord};
use gtv_scenario::{Dimension, Scenario, ScenarioKind, ScenarioStatus, Shock};

use crate::registry::Registry;

fn err(msg: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Execution(msg.to_string())
}

fn column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|_| err(format!("missing column `{name}`")))?;
    Ok(batch.column(idx))
}

fn has_column(batch: &RecordBatch, name: &str) -> bool {
    batch.schema().index_of(name).is_ok()
}

fn strings_from(arr: &ArrayRef) -> Result<Vec<Option<String>>> {
    let casted = cast(arr, &DataType::Utf8).map_err(err)?;
    let s = casted
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| err("expected a string column"))?;
    Ok((0..s.len())
        .map(|i| (!s.is_null(i)).then(|| s.value(i).to_string()))
        .collect())
}

fn i64_from(arr: &ArrayRef) -> Result<Vec<Option<i64>>> {
    let casted = cast(arr, &DataType::Int64).map_err(err)?;
    let v = casted
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| err("expected an integer column"))?;
    Ok((0..v.len())
        .map(|i| (!v.is_null(i)).then(|| v.value(i)))
        .collect())
}

fn f64_from(arr: &ArrayRef) -> Result<Vec<f64>> {
    let casted = cast(arr, &DataType::Float64).map_err(err)?;
    let v = casted
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| err("expected a float column"))?;
    Ok((0..v.len()).map(|i| v.value(i)).collect())
}

fn u32_from(arr: &ArrayRef) -> Result<Vec<u32>> {
    let casted = cast(arr, &DataType::UInt32).map_err(err)?;
    let v = casted
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| err("expected an unsigned integer column"))?;
    Ok((0..v.len()).map(|i| v.value(i)).collect())
}

fn required_strings(batch: &RecordBatch, name: &str) -> Result<Vec<String>> {
    strings_from(column(batch, name)?)?
        .into_iter()
        .map(|v| v.ok_or_else(|| err(format!("column `{name}` must not be null"))))
        .collect()
}

fn required_i64(batch: &RecordBatch, name: &str) -> Result<Vec<i64>> {
    i64_from(column(batch, name)?)?
        .into_iter()
        .map(|v| v.ok_or_else(|| err(format!("column `{name}` must not be null"))))
        .collect()
}

fn optional_strings(batch: &RecordBatch, name: &str) -> Result<Vec<Option<String>>> {
    if has_column(batch, name) {
        strings_from(column(batch, name)?)
    } else {
        Ok(vec![None; batch.num_rows()])
    }
}

fn optional_i64(batch: &RecordBatch, name: &str, default: i64) -> Result<Vec<i64>> {
    if has_column(batch, name) {
        Ok(i64_from(column(batch, name)?)?
            .into_iter()
            .map(|v| v.unwrap_or(default))
            .collect())
    } else {
        Ok(vec![default; batch.num_rows()])
    }
}

fn effective(from: i64, to: i64) -> Result<EffectiveRange> {
    EffectiveRange::new(from, to).map_err(err)
}

/// Load scenario versions from a flat table.
///
/// Required columns: `scenario_id`, `version`, `kind`, `factor`, `value`.
/// Optional: `parent_id`, `parent_version`, `source_cutoff`, `model_version`,
/// `status`, `dim_legal_entity`, `dim_portfolio`, `dim_product`, `dim_currency`.
/// Rows sharing `(scenario_id, version)` become shocks of one scenario.
pub fn load_scenarios(registry: &Registry, batches: &[RecordBatch]) -> Result<usize> {
    let mut pending: BTreeMap<(String, u32), Scenario> = BTreeMap::new();
    for batch in batches {
        let ids = required_strings(batch, "scenario_id")?;
        let versions = u32_from(column(batch, "version")?)?;
        let kinds = required_strings(batch, "kind")?;
        let factors = required_strings(batch, "factor")?;
        let values = f64_from(column(batch, "value")?)?;
        let parent_ids = optional_strings(batch, "parent_id")?;
        let parent_versions = if has_column(batch, "parent_version") {
            i64_from(column(batch, "parent_version")?)?
        } else {
            vec![None; batch.num_rows()]
        };
        let cutoffs = optional_i64(batch, "source_cutoff", 0)?;
        let models = optional_strings(batch, "model_version")?;
        let statuses = optional_strings(batch, "status")?;
        let legal_entity = optional_strings(batch, "dim_legal_entity")?;
        let portfolio = optional_strings(batch, "dim_portfolio")?;
        let product = optional_strings(batch, "dim_product")?;
        let currency = optional_strings(batch, "dim_currency")?;

        for i in 0..batch.num_rows() {
            let kind = ScenarioKind::parse(&kinds[i])
                .ok_or_else(|| err(format!("unknown scenario kind `{}`", kinds[i])))?;
            let key = (ids[i].clone(), versions[i]);
            let entry = pending.entry(key).or_insert_with(|| {
                let mut s = Scenario::new(ids[i].clone(), versions[i], kind);
                s.parent = match (&parent_ids[i], parent_versions[i]) {
                    (Some(p), Some(v)) if v > 0 => Some((p.clone(), v as u32)),
                    _ => None,
                };
                s.source_cutoff = cutoffs[i];
                s.model_version = models[i].clone().unwrap_or_default();
                s.status = statuses[i]
                    .as_deref()
                    .and_then(ScenarioStatus::parse)
                    .unwrap_or(ScenarioStatus::Draft);
                s
            });
            let dimension = Dimension {
                legal_entity: legal_entity[i].clone(),
                portfolio: portfolio[i].clone(),
                product: product[i].clone(),
                currency: currency[i].clone(),
            };
            entry
                .shocks
                .push(Shock::new(factors[i].clone(), values[i]).with_dimension(dimension));
        }
    }

    let mut reg = registry
        .write()
        .map_err(|_| err("enterprise registry poisoned"))?;
    let count = pending.len();
    for (_, scenario) in pending {
        reg.scenarios.register(scenario).map_err(err)?;
    }
    Ok(count)
}

/// Load hierarchy edges. Columns: `parent`, `child`, `valid_from`, optional
/// `valid_to` (default: open-ended).
pub fn load_hierarchy(
    registry: &Registry,
    kind: HierarchyKind,
    batches: &[RecordBatch],
) -> Result<usize> {
    let mut added = 0;
    let mut reg = registry
        .write()
        .map_err(|_| err("enterprise registry poisoned"))?;
    for batch in batches {
        let parents = required_strings(batch, "parent")?;
        let children = required_strings(batch, "child")?;
        let froms = required_i64(batch, "valid_from")?;
        let tos = optional_i64(batch, "valid_to", i64::MAX)?;
        for i in 0..batch.num_rows() {
            let edge = HierarchyEdge::new(
                kind,
                parents[i].clone(),
                children[i].clone(),
                effective(froms[i], tos[i])?,
            );
            reg.hierarchy.add_edge(edge).map_err(err)?;
            added += 1;
        }
    }
    Ok(added)
}

/// Load effective-dated reference values. Columns: `domain`, `key`,
/// `valid_from`, optional `valid_to`, `value`.
pub fn load_reference(registry: &Registry, batches: &[RecordBatch]) -> Result<usize> {
    let mut added = 0;
    let mut reg = registry
        .write()
        .map_err(|_| err("enterprise registry poisoned"))?;
    for batch in batches {
        let domains = required_strings(batch, "domain")?;
        let keys = required_strings(batch, "key")?;
        let froms = required_i64(batch, "valid_from")?;
        let tos = optional_i64(batch, "valid_to", i64::MAX)?;
        let values = required_strings(batch, "value")?;
        for i in 0..batch.num_rows() {
            reg.reference
                .set(
                    domains[i].clone(),
                    keys[i].clone(),
                    effective(froms[i], tos[i])?,
                    values[i].clone(),
                )
                .map_err(err)?;
            added += 1;
        }
    }
    Ok(added)
}

/// Load master records for one kind. Columns: `id`, `valid_from`, optional
/// `valid_to`; every other column becomes a string attribute.
pub fn load_master(
    registry: &Registry,
    kind: MasterKind,
    batches: &[RecordBatch],
) -> Result<usize> {
    let mut added = 0;
    let mut reg = registry
        .write()
        .map_err(|_| err("enterprise registry poisoned"))?;
    for batch in batches {
        let ids = required_strings(batch, "id")?;
        let froms = required_i64(batch, "valid_from")?;
        let tos = optional_i64(batch, "valid_to", i64::MAX)?;
        let attr_names: Vec<String> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .filter(|n| n != "id" && n != "valid_from" && n != "valid_to")
            .collect();
        let attr_cols: Vec<Vec<Option<String>>> = attr_names
            .iter()
            .map(|n| strings_from(column(batch, n)?))
            .collect::<Result<_>>()?;

        for i in 0..batch.num_rows() {
            let mut record =
                MasterRecord::new(kind, ids[i].clone(), effective(froms[i], tos[i])?);
            for (name, col) in attr_names.iter().zip(&attr_cols) {
                if let Some(value) = &col[i] {
                    record.attributes.insert(name.clone(), value.clone());
                }
            }
            reg.master.put(record).map_err(err)?;
            added += 1;
        }
    }
    Ok(added)
}
