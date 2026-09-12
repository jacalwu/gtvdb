//! CRM (credit-risk-mitigation) allocation — DataFusion table functions.
//!
//! `crm_alloc` / `crm_audit` run the CRM allocator of [`gtv_array::crm`]
//! (`crm_alloc_greedy` — phases 1+2) — or the optional Phase-3 LP optimizer
//! ([`gtv_array::crm_lp`], feature `crm-lp`) — over the five tables of
//! `crm-allocation.md`:
//!
//! ```text
//! crm_alloc('loan_exposure','collateral','guarantee',
//!           'collateral_edges','guarantee_edges', scenario, T
//!           [, exposure_col [, method]])
//! ```
//!
//! * tables are the session tables loaded via `LOAD CSV … INTO` / `loadcsv`;
//! * `scenario` filters `scenario_id` rows ('' or '*' = any scenario) — edges /
//!   CRM sources are scenario-invariant in the reference schema and pass
//!   through untouched;
//! * `T` is the as-of instant in ns; only rows with `valid_from <= T <
//!   valid_to` participate (`T < 0` = no temporal filter);
//! * collateral capacity is haircut-adjusted
//!   (`C × (1 − Hc − Hfx − Hmm)`, floored at 0) before allocation;
//! * edges whose endpoint was **filtered out** of the as-of / scenario slice
//!   are inert and dropped (a loan or CRM source that is not part of the
//!   snapshot cannot be allocated against); edges that reference an id that
//!   never existed in the source tables are a data error and abort the run;
//! * default priorities: loans by `pd` (riskier loans covered first),
//!   collaterals by `type` (CASH > BOND > EQUITY), guarantors by `rating`
//!   (AAA > AA > A …).  Adding a `priority` column to any source/loan table
//!   overrides the default for that table;
//! * optional `exposure_col` = loan exposure to mitigate (default `ead`,
//!   e.g. `'pv'`);
//! * optional `method`:
//!   - `'greedy'` (default; phase 2) — collateral by type quality, loans by
//!     `pd` (riskier first);
//!   - `'haircut_efficiency'` (phase-2 variant) — collateral consumed by
//!     haircut-adjusted effective value (largest first), loans by risk weight
//!     (`rw` column, else standardised rating map, else `pd`);
//!   - `'lp'` (phase 3 — exact LP over the optimizable pool that also honours
//!     the per-edge `ratio` / `amount` caps).  `'lp'` needs the `crm-lp`
//!     feature: rebuild with `--features gtv-engine/crm-lp`.
//!
//! `crm_alloc` returns one row per loan:
//! `loan_id, exposure, collateral_cover, guarantee_cover, net_exposure`.
//!
//! `crm_audit` returns the full, replayable allocation trail (same optional
//! `method`): `seq, stage (specified | greedy | lp), source_kind, source_id,
//! loan_id, amount, source_remaining, loan_remaining`.

use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use arrow::array::{
    ArrayRef, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    LargeStringArray, RecordBatch, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::Expr;
use gtv_array::crm::{
    adjusted_collateral_value, AllocationMode, Collateral, CollateralEdge, CrmAllocation,
    CrmResult, GuaranteeEdge, Guarantor, Loan,
};

use crate::expr_util::{expr_to_i64, expr_to_string};
use crate::hft_exec::HftRegistry;

/// Snapshot of the five CRM tables (all rows already scenario/as-of filtered).
#[derive(Debug, Default)]
pub(crate) struct CrmSnapshot {
    pub loans: Vec<Loan>,
    pub collaterals: Vec<Collateral>,
    pub guarantors: Vec<Guarantor>,
    pub coll_edges: Vec<CollateralEdge>,
    pub guar_edges: Vec<GuaranteeEdge>,
}

// ---------------------------------------------------------------------------
// Column materialisation helpers
// ---------------------------------------------------------------------------

fn rows_len(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

fn is_numeric(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
    )
}

/// Concatenate a numeric column across batches as `i64`.
fn col_i64(batches: &[RecordBatch], name: &str) -> DfResult<Option<Vec<i64>>> {
    let Some(probe) = batches.iter().find_map(|b| b.column_by_name(name)) else {
        return Ok(None);
    };
    if !is_numeric(probe.data_type()) {
        return Err(DataFusionError::Execution(format!(
            "crm: column `{name}` has unsupported type {} (expected a numeric type)",
            probe.data_type()
        )));
    }
    let mut out = Vec::with_capacity(rows_len(batches));
    for b in batches {
        let arr = b.column_by_name(name).unwrap();
        let n = arr.len();
        macro_rules! push {
            ($t:ty) => {
                if let Some(a) = arr.as_any().downcast_ref::<$t>() {
                    for i in 0..n {
                        out.push(a.value(i) as i64);
                    }
                    continue;
                }
            };
        }
        push!(Int8Array);
        push!(Int16Array);
        push!(Int32Array);
        push!(Int64Array);
        push!(UInt8Array);
        push!(UInt16Array);
        push!(UInt32Array);
        push!(UInt64Array);
        push!(Float32Array);
        push!(Float64Array);
    }
    Ok(Some(out))
}

/// Concatenate a numeric column across batches as `f64`.
fn col_f64(batches: &[RecordBatch], name: &str) -> DfResult<Option<Vec<f64>>> {
    let Some(probe) = batches.iter().find_map(|b| b.column_by_name(name)) else {
        return Ok(None);
    };
    if !is_numeric(probe.data_type()) {
        return Err(DataFusionError::Execution(format!(
            "crm: column `{name}` has unsupported type {} (expected a numeric type)",
            probe.data_type()
        )));
    }
    let mut out = Vec::with_capacity(rows_len(batches));
    for b in batches {
        let arr = b.column_by_name(name).unwrap();
        let n = arr.len();
        macro_rules! push {
            ($t:ty) => {
                if let Some(a) = arr.as_any().downcast_ref::<$t>() {
                    for i in 0..n {
                        out.push(a.value(i) as f64);
                    }
                    continue;
                }
            };
        }
        push!(Int8Array);
        push!(Int16Array);
        push!(Int32Array);
        push!(Int64Array);
        push!(UInt8Array);
        push!(UInt16Array);
        push!(UInt32Array);
        push!(UInt64Array);
        push!(Float32Array);
        push!(Float64Array);
    }
    Ok(Some(out))
}

/// Concatenate a string column across batches.
fn col_str(batches: &[RecordBatch], name: &str) -> DfResult<Option<Vec<String>>> {
    let Some(probe) = batches.iter().find_map(|b| b.column_by_name(name)) else {
        return Ok(None);
    };
    let mut out = Vec::with_capacity(rows_len(batches));
    for b in batches {
        let arr = b.column_by_name(name).unwrap();
        let n = arr.len();
        if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
            for i in 0..n {
                out.push(a.value(i).to_string());
            }
        } else if let Some(a) = arr.as_any().downcast_ref::<LargeStringArray>() {
            for i in 0..n {
                out.push(a.value(i).to_string());
            }
        } else {
            return Err(DataFusionError::Execution(format!(
                "crm: column `{name}` has unsupported type {} (expected Utf8)",
                probe.data_type()
            )));
        }
    }
    Ok(Some(out))
}

/// Row-level filter: `scenario_id = scenario` (when the column exists and a
/// scenario was requested) and `valid_from <= T < valid_to` (when the temporal
/// columns exist and `T >= 0`).
fn row_mask(batches: &[RecordBatch], scenario: &str, asof: i64) -> DfResult<Vec<bool>> {
    let scenario_filter = scenario != "" && scenario != "*";
    let sc = if scenario_filter {
        col_str(batches, "scenario_id")?
    } else {
        None
    };
    let temporal = asof >= 0
        && batches
            .iter()
            .any(|b| b.column_by_name("valid_from").is_some());
    let (vf, vt) = if temporal {
        (
            col_i64(batches, "valid_from")?.unwrap(),
            col_i64(batches, "valid_to")?.unwrap(),
        )
    } else {
        (Vec::new(), Vec::new())
    };
    let n = rows_len(batches);
    let mut mask = Vec::with_capacity(n);
    for i in 0..n {
        let ok_scenario = match &sc {
            None => true,
            Some(v) => v[i].eq_ignore_ascii_case(scenario),
        };
        let ok_time = if temporal {
            vf[i] <= asof && asof < vt[i]
        } else {
            true
        };
        mask.push(ok_scenario && ok_time);
    }
    Ok(mask)
}

fn keep<T: Clone>(v: &[T], mask: &[bool]) -> Vec<T> {
    v.iter()
        .zip(mask.iter())
        .filter(|(_, &m)| m)
        .map(|(x, _)| x.clone())
        .collect()
}

fn require_col<T>(opt: Option<Vec<T>>, what: &str) -> DfResult<Vec<T>> {
    opt.ok_or_else(|| DataFusionError::Execution(format!("crm: missing {what}")))
}

fn u64_ids(ids: &[i64], what: &str) -> DfResult<Vec<u64>> {
    if ids.iter().any(|&v| v < 0) {
        return Err(DataFusionError::Execution(format!(
            "crm: {what} contains a negative id"
        )));
    }
    Ok(ids.iter().map(|&v| v as u64).collect())
}

// ---------------------------------------------------------------------------
// Default priority mappings (overridable with a `priority` column)
// ---------------------------------------------------------------------------

/// Standardised (Basel corporate) risk weight by rating — ordering key of the
/// `haircut_efficiency` method (larger = covered first). `rw` on the loan table
/// overrides this map.
fn loan_rating_rw(rating: &str) -> f64 {
    match rating.to_ascii_uppercase().as_str() {
        "AAA" | "AA" => 0.2,
        "A" => 0.5,
        "BBB" | "BB" => 1.0,
        "B" => 1.5,
        _ => 1.0,
    }
}

/// Larger = loan covered first (higher default risk).
fn loan_rating_risk_priority(rating: &str) -> f64 {
    match rating.to_ascii_uppercase().as_str() {
        "BB" => 3.0,
        "BBB" => 2.0,
        "A" => 1.0,
        "AA" => 0.5,
        "AAA" => 0.0,
        _ => 0.0,
    }
}

/// Larger = consumed first (higher-quality collateral allocated first).
fn collateral_type_priority(ty: &str) -> f64 {
    match ty.to_ascii_uppercase().as_str() {
        "CASH" => 3.0,
        "BOND" => 2.0,
        "EQUITY" => 1.0,
        _ => 0.0,
    }
}

/// Larger = consumed first (stronger guarantors allocated first).
fn guarantor_rating_priority(rating: &str) -> f64 {
    match rating.to_ascii_uppercase().as_str() {
        "AAA" => 3.0,
        "AA" => 2.0,
        "A" => 1.0,
        "BBB" => 0.5,
        "BB" => 0.0,
        _ => 0.0,
    }
}

// ---------------------------------------------------------------------------
// Snapshot builder: table batches -> allocator inputs
// ---------------------------------------------------------------------------

pub(crate) fn snapshot_from_batches(
    loans_b: &[RecordBatch],
    collateral_b: &[RecordBatch],
    guarantee_b: &[RecordBatch],
    coll_edges_b: &[RecordBatch],
    guar_edges_b: &[RecordBatch],
    scenario: &str,
    asof: i64,
    exposure_col: &str,
    method: &str,
) -> DfResult<CrmSnapshot> {
    // `haircut_efficiency` (Phase 2 variant): collateral sources are ordered by
    // haircut-adjusted effective value (largest first) and loans by risk weight
    // (largest first) instead of the type / pd defaults of the plain greedy.
    let efficiency = method == "haircut_efficiency";
    // --- loans ---------------------------------------------------------------
    let mask = row_mask(loans_b, scenario, asof)?;
    let raw_ids = require_col(col_i64(loans_b, "loan_id")?, "loan_exposure.loan_id")?;
    let ids = u64_ids(&raw_ids, "loan_exposure.loan_id")?;
    let loan_all: HashSet<u64> = ids.iter().copied().collect();
    let exposure = require_col(
        col_f64(loans_b, exposure_col)?,
        &format!("loan_exposure.{exposure_col}"),
    )?;
    // priority: explicit `priority` column wins everywhere. Otherwise
    // `greedy`/`lp` use `pd` > rating-risk map; `haircut_efficiency` uses
    // `rw` > rating risk-weight map > `pd` fallback.
    let explicit_prio = col_f64(loans_b, "priority")?;
    let pd = col_f64(loans_b, "pd")?;
    let rw = col_f64(loans_b, "rw")?;
    let rating = col_str(loans_b, "rating")?;
    let ids = keep(&ids, &mask);
    let exposure = keep(&exposure, &mask);
    let explicit_prio = explicit_prio.map(|v| keep(&v, &mask));
    let pd = pd.map(|v| keep(&v, &mask));
    let rw = rw.map(|v| keep(&v, &mask));
    let rating = rating.map(|v| keep(&v, &mask));
    let loan_kept: HashSet<u64> = ids.iter().copied().collect();
    let loan_risk_priority = |i: usize| -> f64 {
        if let Some(v) = explicit_prio.as_ref() {
            return v[i];
        }
        if efficiency {
            if let Some(v) = rw.as_ref() {
                return v[i];
            }
            if let Some(v) = rating.as_ref() {
                return loan_rating_rw(&v[i]);
            }
        }
        if let Some(v) = pd.as_ref() {
            return v[i];
        }
        if let Some(v) = rating.as_ref() {
            return loan_rating_risk_priority(&v[i]);
        }
        0.0
    };
    let loans = (0..ids.len())
        .map(|i| Loan {
            id: ids[i],
            exposure: exposure[i],
            priority: loan_risk_priority(i),
        })
        .collect();

    // --- collaterals ----------------------------------------------------------
    let mask = row_mask(collateral_b, scenario, asof)?;
    let raw_ids = require_col(col_i64(collateral_b, "col_id")?, "collateral.col_id")?;
    let ids = u64_ids(&raw_ids, "collateral.col_id")?;
    let col_all: HashSet<u64> = ids.iter().copied().collect();
    let value = require_col(col_f64(collateral_b, "value")?, "collateral.value")?;
    let haircut = col_f64(collateral_b, "haircut")?;
    let fx_haircut = col_f64(collateral_b, "fx_haircut")?;
    let maturity = col_f64(collateral_b, "maturity_mm")?;
    let explicit_prio = col_f64(collateral_b, "priority")?;
    let ty = col_str(collateral_b, "type")?;
    let ids = keep(&ids, &mask);
    let value = keep(&value, &mask);
    let haircut = haircut.map(|v| keep(&v, &mask));
    let fx_haircut = fx_haircut.map(|v| keep(&v, &mask));
    let maturity = maturity.map(|v| keep(&v, &mask));
    let explicit_prio = explicit_prio.map(|v| keep(&v, &mask));
    let ty = ty.map(|v| keep(&v, &mask));
    let col_kept: HashSet<u64> = ids.iter().copied().collect();
    // collateral ordering key: explicit `priority` column, else for
    // `haircut_efficiency` the haircut-adjusted effective value (largest
    // first), else the type-quality map (CASH > BOND > EQUITY).
    let coll_eff = |i: usize| -> f64 {
        adjusted_collateral_value(
            value[i],
            haircut.as_ref().map(|v| v[i]).unwrap_or(0.0),
            fx_haircut.as_ref().map(|v| v[i]).unwrap_or(0.0),
            maturity.as_ref().map(|v| v[i]).unwrap_or(0.0),
        )
    };
    let collateral_priority = |i: usize, eff: f64| -> f64 {
        if let Some(v) = explicit_prio.as_ref() {
            return v[i];
        }
        if efficiency {
            eff
        } else {
            ty.as_ref()
                .map(|v| collateral_type_priority(&v[i]))
                .unwrap_or(0.0)
        }
    };
    let collaterals = (0..ids.len())
        .map(|i| {
            let eff = coll_eff(i);
            Collateral {
                id: ids[i],
                capacity: eff,
                priority: collateral_priority(i, eff),
            }
        })
        .collect();

    // --- guarantors -----------------------------------------------------------
    let mask = row_mask(guarantee_b, scenario, asof)?;
    let raw_ids = require_col(
        col_i64(guarantee_b, "guarantor_id")?,
        "guarantee.guarantor_id",
    )?;
    let ids = u64_ids(&raw_ids, "guarantee.guarantor_id")?;
    let guar_all: HashSet<u64> = ids.iter().copied().collect();
    let amount = require_col(col_f64(guarantee_b, "amount")?, "guarantee.amount")?;
    let explicit_prio = col_f64(guarantee_b, "priority")?;
    let rating = col_str(guarantee_b, "rating")?;
    let ids = keep(&ids, &mask);
    let amount = keep(&amount, &mask);
    let explicit_prio = explicit_prio.map(|v| keep(&v, &mask));
    let rating = rating.map(|v| keep(&v, &mask));
    let guar_kept: HashSet<u64> = ids.iter().copied().collect();
    let guarantors = (0..ids.len())
        .map(|i| Guarantor {
            id: ids[i],
            capacity: amount[i],
            priority: explicit_prio
                .as_ref()
                .map(|v| v[i])
                .or_else(|| rating.as_ref().map(|v| guarantor_rating_priority(&v[i])))
                .unwrap_or(0.0),
        })
        .collect();

    // --- edges (cross-table slice consistency: edges whose endpoint was
    // filtered out of the as-of / scenario slice are inert and dropped;
    // edges referencing an id that never existed are a data error) -----------
    let coll_edges = load_coll_edges(
        coll_edges_b,
        scenario,
        asof,
        &loan_all,
        &loan_kept,
        &col_all,
        &col_kept,
    )?;
    let guar_edges = load_guar_edges(
        guar_edges_b,
        scenario,
        asof,
        &loan_all,
        &loan_kept,
        &guar_all,
        &guar_kept,
    )?;

    Ok(CrmSnapshot {
        loans,
        collaterals,
        guarantors,
        coll_edges,
        guar_edges,
    })
}

fn parse_mode(raw: &str) -> DfResult<AllocationMode> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "specified" => Ok(AllocationMode::Specified),
        "optimizable" => Ok(AllocationMode::Optimizable),
        other => Err(DataFusionError::Execution(format!(
            "crm: allocation_mode `{other}` must be 'specified' or 'optimizable'"
        ))),
    }
}

fn load_coll_edges(
    batches: &[RecordBatch],
    scenario: &str,
    asof: i64,
    loan_all: &HashSet<u64>,
    loan_kept: &HashSet<u64>,
    col_all: &HashSet<u64>,
    col_kept: &HashSet<u64>,
) -> DfResult<Vec<CollateralEdge>> {
    let mask = row_mask(batches, scenario, asof)?;
    let raw_cols = require_col(col_i64(batches, "col_id")?, "collateral_edges.col_id")?;
    let col_full = u64_ids(&raw_cols, "collateral_edges.col_id")?;
    let raw_loans = require_col(col_i64(batches, "loan_id")?, "collateral_edges.loan_id")?;
    let loan_full = u64_ids(&raw_loans, "collateral_edges.loan_id")?;
    let ratio_full = col_f64(batches, "ratio")?.unwrap_or_else(|| vec![1.0; col_full.len()]);
    let mode_full = require_col(
        col_str(batches, "allocation_mode")?,
        "collateral_edges.allocation_mode",
    )?;
    let mut out = Vec::new();
    for i in 0..col_full.len() {
        if !mask[i] {
            continue;
        }
        let (c, l) = (col_full[i], loan_full[i]);
        // endpoint truly unknown in the source tables -> data error
        if !col_all.contains(&c) || !loan_all.contains(&l) {
            return Err(DataFusionError::Execution(format!(
                "crm: collateral edge ({c} -> {l}) references an id absent from the collateral / loan_exposure tables"
            )));
        }
        // endpoint valid but excluded by this as-of / scenario slice -> inert
        if !col_kept.contains(&c) || !loan_kept.contains(&l) {
            continue;
        }
        out.push(CollateralEdge {
            col_id: c,
            loan_id: l,
            ratio: ratio_full[i],
            mode: parse_mode(&mode_full[i])?,
        });
    }
    Ok(out)
}

fn load_guar_edges(
    batches: &[RecordBatch],
    scenario: &str,
    asof: i64,
    loan_all: &HashSet<u64>,
    loan_kept: &HashSet<u64>,
    guar_all: &HashSet<u64>,
    guar_kept: &HashSet<u64>,
) -> DfResult<Vec<GuaranteeEdge>> {
    let mask = row_mask(batches, scenario, asof)?;
    let raw_gs = require_col(
        col_i64(batches, "guarantor_id")?,
        "guarantee_edges.guarantor_id",
    )?;
    let g_full = u64_ids(&raw_gs, "guarantee_edges.guarantor_id")?;
    let raw_loans = require_col(col_i64(batches, "loan_id")?, "guarantee_edges.loan_id")?;
    let loan_full = u64_ids(&raw_loans, "guarantee_edges.loan_id")?;
    let amount_full = col_f64(batches, "amount")?.unwrap_or_else(|| vec![0.0; g_full.len()]);
    let mode_full = require_col(
        col_str(batches, "allocation_mode")?,
        "guarantee_edges.allocation_mode",
    )?;
    let mut out = Vec::new();
    for i in 0..g_full.len() {
        if !mask[i] {
            continue;
        }
        let (g, l) = (g_full[i], loan_full[i]);
        // endpoint truly unknown in the source tables -> data error
        if !guar_all.contains(&g) || !loan_all.contains(&l) {
            return Err(DataFusionError::Execution(format!(
                "crm: guarantee edge ({g} -> {l}) references an id absent from the guarantee / loan_exposure tables"
            )));
        }
        // endpoint valid but excluded by this as-of / scenario slice -> inert
        if !guar_kept.contains(&g) || !loan_kept.contains(&l) {
            continue;
        }
        out.push(GuaranteeEdge {
            guarantor_id: g,
            loan_id: l,
            amount: amount_full[i],
            mode: parse_mode(&mode_full[i])?,
        });
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Output builders
// ---------------------------------------------------------------------------

fn alloc_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("loan_id", DataType::UInt64, false),
        Field::new("exposure", DataType::Float64, false),
        Field::new("collateral_cover", DataType::Float64, false),
        Field::new("guarantee_cover", DataType::Float64, false),
        Field::new("net_exposure", DataType::Float64, false),
    ]))
}

fn audit_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("seq", DataType::Int64, false),
        Field::new("stage", DataType::Utf8, false),
        Field::new("source_kind", DataType::Utf8, false),
        Field::new("source_id", DataType::UInt64, false),
        Field::new("loan_id", DataType::UInt64, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("source_remaining", DataType::Float64, false),
        Field::new("loan_remaining", DataType::Float64, false),
    ]))
}

fn alloc_batch(res: &CrmResult) -> DfResult<RecordBatch> {
    let ids: Vec<u64> = res.loan_id.clone();
    let batch = RecordBatch::try_new(
        alloc_schema(),
        vec![
            Arc::new(arrow::array::UInt64Array::from(ids)) as ArrayRef,
            Arc::new(Float64Array::from(res.exposure.clone())) as ArrayRef,
            Arc::new(Float64Array::from(res.collateral_cover.clone())) as ArrayRef,
            Arc::new(Float64Array::from(res.guarantee_cover.clone())) as ArrayRef,
            Arc::new(Float64Array::from(res.net_exposure.clone())) as ArrayRef,
        ],
    )?;
    Ok(batch)
}

fn audit_batch(allocs: &[CrmAllocation]) -> DfResult<RecordBatch> {
    let mut seq = Vec::with_capacity(allocs.len());
    let mut stage = Vec::with_capacity(allocs.len());
    let mut kind = Vec::with_capacity(allocs.len());
    let mut source_id = Vec::with_capacity(allocs.len());
    let mut loan_id = Vec::with_capacity(allocs.len());
    let mut amount = Vec::with_capacity(allocs.len());
    let mut source_remaining = Vec::with_capacity(allocs.len());
    let mut loan_remaining = Vec::with_capacity(allocs.len());
    for (i, a) in allocs.iter().enumerate() {
        seq.push(i as i64 + 1);
        stage.push(
            match a.stage {
                gtv_array::crm::CrmStage::Specified => "specified",
                gtv_array::crm::CrmStage::Greedy => "greedy",
                gtv_array::crm::CrmStage::Lp => "lp",
            }
            .to_string(),
        );
        kind.push(
            match a.source_kind {
                gtv_array::crm::CrmSourceKind::Collateral => "collateral",
                gtv_array::crm::CrmSourceKind::Guarantee => "guarantee",
            }
            .to_string(),
        );
        source_id.push(a.source_id);
        loan_id.push(a.loan_id);
        amount.push(a.amount);
        source_remaining.push(a.source_remaining);
        loan_remaining.push(a.loan_remaining);
    }
    let batch = RecordBatch::try_new(
        audit_schema(),
        vec![
            Arc::new(Int64Array::from(seq)) as ArrayRef,
            Arc::new(StringArray::from(stage)) as ArrayRef,
            Arc::new(StringArray::from(kind)) as ArrayRef,
            Arc::new(UInt64Array::from(source_id)) as ArrayRef,
            Arc::new(UInt64Array::from(loan_id)) as ArrayRef,
            Arc::new(Float64Array::from(amount)) as ArrayRef,
            Arc::new(Float64Array::from(source_remaining)) as ArrayRef,
            Arc::new(Float64Array::from(loan_remaining)) as ArrayRef,
        ],
    )?;
    Ok(batch)
}

// ---------------------------------------------------------------------------
// Argument parsing + registry access
// ---------------------------------------------------------------------------

struct CrmArgs {
    loan_tbl: String,
    coll_tbl: String,
    guar_tbl: String,
    coll_edges_tbl: String,
    guar_edges_tbl: String,
    scenario: String,
    asof: i64,
    exposure_col: String,
    /// Allocation method: `greedy` (default) or `lp` (needs `crm-lp` feature).
    method: String,
}

fn parse_args(exprs: &[Expr]) -> DfResult<CrmArgs> {
    let need = |i: usize, what: &str| -> DfResult<String> {
        exprs.get(i).map(|e| expr_to_string(e)).unwrap_or_else(|| {
            Err(DataFusionError::Execution(format!(
                "crm: missing {what} argument"
            )))
        })
    };
    Ok(CrmArgs {
        loan_tbl: need(0, "loan table name")?,
        coll_tbl: need(1, "collateral table name")?,
        guar_tbl: need(2, "guarantee table name")?,
        coll_edges_tbl: need(3, "collateral_edges table name")?,
        guar_edges_tbl: need(4, "guarantee_edges table name")?,
        scenario: need(5, "scenario")?,
        asof: exprs.get(6).map(|e| expr_to_i64(e)).unwrap_or_else(|| {
            Err(DataFusionError::Execution(format!(
                "crm: missing as-of instant T argument (ns; use -1 for no filter)"
            )))
        })?,
        exposure_col: exprs
            .get(7)
            .map(|e| expr_to_string(e))
            .transpose()?
            .unwrap_or_else(|| "ead".to_string()),
        method: exprs
            .get(8)
            .map(|e| expr_to_string(e))
            .transpose()?
            .unwrap_or_else(|| "greedy".to_string())
            .to_ascii_lowercase(),
    })
}

fn snapshot_and_run(reg: &HftRegistry, args: &CrmArgs) -> DfResult<CrmResult> {
    let table = |name: &str| -> DfResult<&Vec<RecordBatch>> {
        reg.tables.get(name).map(|b| b.as_ref()).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "crm: table `{name}` is not registered (use `loadcsv {name} <file>` first)"
            ))
        })
    };
    let loans = table(&args.loan_tbl)?;
    let collateral = table(&args.coll_tbl)?;
    let guarantee = table(&args.guar_tbl)?;
    let coll_edges = table(&args.coll_edges_tbl)?;
    let guar_edges = table(&args.guar_edges_tbl)?;
    let snap = snapshot_from_batches(
        loans,
        collateral,
        guarantee,
        coll_edges,
        guar_edges,
        &args.scenario,
        args.asof,
        &args.exposure_col,
        &args.method,
    )?;
    match args.method.as_str() {
        // `greedy`: collateral by type quality, loans by pd/rating-risk.
        // `haircut_efficiency`: collateral by haircut-adjusted effective value,
        // loans by risk weight (the ordering keys are chosen in the snapshot).
        "greedy" | "haircut_efficiency" => gtv_array::crm::crm_alloc_greedy(
            &snap.loans,
            &snap.collaterals,
            &snap.guarantors,
            &snap.coll_edges,
            &snap.guar_edges,
        )
        .map_err(|e| DataFusionError::Execution(format!("crm_alloc: {e}"))),
        "lp" => {
            #[cfg(feature = "crm-lp")]
            {
                gtv_array::crm_lp::crm_alloc_lp(
                    &snap.loans,
                    &snap.collaterals,
                    &snap.guarantors,
                    &snap.coll_edges,
                    &snap.guar_edges,
                )
                .map_err(|e| DataFusionError::Execution(format!("crm_alloc_lp: {e}")))
            }
            #[cfg(not(feature = "crm-lp"))]
            {
                Err(DataFusionError::Execution(
                    "crm_alloc: method 'lp' needs the `crm-lp` feature — \
                     rebuild with `--features gtv-engine/crm-lp` (Phase-3 LP solver)"
                        .to_string(),
                ))
            }
        }
        other => Err(DataFusionError::Execution(format!(
            "crm_alloc: unknown method `{other}` \
             (expected 'greedy', 'haircut_efficiency' or 'lp')"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Table functions
// ---------------------------------------------------------------------------

/// `crm_alloc(loan_tbl, coll_tbl, guar_tbl, coll_edges_tbl, guar_edges_tbl,
/// scenario, T [, exposure_col [, method]])` — per-loan CRM allocation summary
/// (`method` = 'greedy' | 'haircut_efficiency' | 'lp'; 'lp' needs the `crm-lp`
/// feature).
#[derive(Debug)]
pub struct CrmAllocTableFunction {
    registry: Arc<RwLock<HftRegistry>>,
}

impl CrmAllocTableFunction {
    pub fn new(registry: Arc<RwLock<HftRegistry>>) -> Self {
        Self { registry }
    }
}

impl TableFunctionImpl for CrmAllocTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> DfResult<Arc<dyn TableProvider>> {
        let crm_args = parse_args(args.exprs())?;
        let reg = self
            .registry
            .read()
            .map_err(|_| DataFusionError::Execution("hft registry poisoned".into()))?;
        let res = snapshot_and_run(&reg, &crm_args)?;
        let batch = alloc_batch(&res)?;
        Ok(Arc::new(MemTable::try_new(
            alloc_schema(),
            vec![vec![batch]],
        )?))
    }
}

/// `crm_audit(loan_tbl, coll_tbl, guar_tbl, coll_edges_tbl, guar_edges_tbl,
/// scenario, T [, exposure_col [, method]])` — full allocation audit trail
/// (same inputs, same deterministic allocator).
#[derive(Debug)]
pub struct CrmAuditTableFunction {
    registry: Arc<RwLock<HftRegistry>>,
}

impl CrmAuditTableFunction {
    pub fn new(registry: Arc<RwLock<HftRegistry>>) -> Self {
        Self { registry }
    }
}

impl TableFunctionImpl for CrmAuditTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> DfResult<Arc<dyn TableProvider>> {
        let crm_args = parse_args(args.exprs())?;
        let reg = self
            .registry
            .read()
            .map_err(|_| DataFusionError::Execution("hft registry poisoned".into()))?;
        let res = snapshot_and_run(&reg, &crm_args)?;
        let batch = audit_batch(&res.allocations)?;
        Ok(Arc::new(MemTable::try_new(
            audit_schema(),
            vec![vec![batch]],
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::Schema;

    /// Doc-schema helper tables. `scenario` rows on loan_exposure only.
    fn schema(fields: Vec<(&str, DataType)>) -> SchemaRef {
        Arc::new(Schema::new(
            fields
                .into_iter()
                .map(|(n, t)| Field::new(n, t, false))
                .collect::<Vec<Field>>(),
        ))
    }

    fn batches(schema: SchemaRef, cols: Vec<ArrayRef>) -> Vec<RecordBatch> {
        vec![RecordBatch::try_new(schema, cols).unwrap()]
    }

    fn i64(v: Vec<i64>) -> ArrayRef {
        Arc::new(Int64Array::from(v))
    }
    fn f64(v: Vec<f64>) -> ArrayRef {
        Arc::new(Float64Array::from(v))
    }
    fn str(v: Vec<&str>) -> ArrayRef {
        Arc::new(StringArray::from(v))
    }

    fn loan_tbl(scenario: &str, asof: i64) -> Vec<RecordBatch> {
        let rows = 3;
        batches(
            schema(vec![
                ("loan_id", DataType::Int64),
                ("ead", DataType::Float64),
                ("pd", DataType::Float64),
                ("rating", DataType::Utf8),
                ("scenario_id", DataType::Utf8),
                ("valid_from", DataType::Int64),
                ("valid_to", DataType::Int64),
            ]),
            vec![
                i64(vec![1, 2, 3]),
                f64(vec![100.0, 100.0, 50.0]),
                f64(vec![0.02, 0.01, 0.04]),
                str(vec!["A", "BBB", "BB"]),
                str(vec![scenario; rows]),
                i64(vec![asof - 1000; rows]),
                i64(vec![asof + 1000; rows]),
            ],
        )
    }

    fn coll_tbl() -> Vec<RecordBatch> {
        batches(
            schema(vec![
                ("col_id", DataType::Int64),
                ("value", DataType::Float64),
                ("haircut", DataType::Float64),
                ("fx_haircut", DataType::Float64),
                ("maturity_mm", DataType::Float64),
                ("type", DataType::Utf8),
            ]),
            vec![
                i64(vec![10, 20]),
                f64(vec![100.0, 100.0]),
                f64(vec![0.1, 0.0]),
                f64(vec![0.0, 0.0]),
                f64(vec![0.0, 0.0]),
                str(vec!["CASH", "EQUITY"]),
            ],
        )
    }

    fn guar_tbl() -> Vec<RecordBatch> {
        batches(
            schema(vec![
                ("guarantor_id", DataType::Int64),
                ("amount", DataType::Float64),
                ("rating", DataType::Utf8),
            ]),
            vec![i64(vec![100]), f64(vec![120.0]), str(vec!["AAA"])],
        )
    }

    fn coll_edges_tbl() -> Vec<RecordBatch> {
        batches(
            schema(vec![
                ("col_id", DataType::Int64),
                ("loan_id", DataType::Int64),
                ("ratio", DataType::Float64),
                ("allocation_mode", DataType::Utf8),
            ]),
            vec![
                i64(vec![10, 20]),
                i64(vec![1, 2]),
                f64(vec![1.0, 1.0]),
                str(vec!["specified", "optimizable"]),
            ],
        )
    }

    fn guar_edges_tbl() -> Vec<RecordBatch> {
        batches(
            schema(vec![
                ("guarantor_id", DataType::Int64),
                ("loan_id", DataType::Int64),
                ("amount", DataType::Float64),
                ("allocation_mode", DataType::Utf8),
            ]),
            vec![
                i64(vec![100, 100]),
                i64(vec![2, 3]),
                f64(vec![50.0, 40.0]),
                str(vec!["optimizable", "optimizable"]),
            ],
        )
    }

    fn run(scenario: &str, asof: i64) -> CrmResult {
        let snap = snapshot_from_batches(
            &loan_tbl(scenario, asof),
            &coll_tbl(),
            &guar_tbl(),
            &coll_edges_tbl(),
            &guar_edges_tbl(),
            scenario,
            asof,
            "ead",
            "greedy",
        )
        .unwrap();
        gtv_array::crm::crm_alloc_greedy(
            &snap.loans,
            &snap.collaterals,
            &snap.guarantors,
            &snap.coll_edges,
            &snap.guar_edges,
        )
        .unwrap()
    }

    fn sums(v: &[f64]) -> f64 {
        v.iter().sum()
    }

    #[test]
    fn end_to_end_alloc_over_doc_schema_tables() {
        // scenario BASE, no temporal filter.
        let asof = 1_700_000_000_000_000_000i64;
        let r = run("BASE", asof);
        assert_eq!(r.loan_id, vec![1, 2, 3]);

        // L1: specified collateral 10 value 100 with haircut 0.1 -> cap 90.
        assert!((r.collateral_cover[0] - 90.0).abs() < 1e-9);
        assert_eq!(r.exposure, vec![100.0, 100.0, 50.0]);

        // L2: optimizable collateral 20 (cap 100) + optimizable guarantor 100.
        // Collateral phases run first: C20 covers all 100 of L2's remaining
        // (L2 pd 0.01 < L3 pd 0.04, so L2 is *less* risky; but L2 is the only
        // optimizable edge of C20, and loans are walked risk-desc -> L3 before
        // L2 only if both are reachable from the same source. C20 only links L2
        // here, so L2 still gets its full 100 from C20.)
        assert!((r.collateral_cover[1] - 100.0).abs() < 1e-9);
        // L3: guarantor covers min(G, E - CRMc) = min(120, 50) = 50.
        assert!((r.guarantee_cover[2] - 50.0).abs() < 1e-9);
        // L2 leftover 0 -> no guarantee is allocated to L2 even though an
        // optimizable edge exists.
        assert!((r.guarantee_cover[1]).abs() < 1e-9);

        for i in 0..3 {
            assert!(r.net_exposure[i] > -1e-9, "net {i} >= 0");
        }
        assert!((r.net_exposure[0] - 10.0).abs() < 1e-9); // 100 - 90
        assert!((r.net_exposure[1]).abs() < 1e-9);
        assert!((r.net_exposure[2]).abs() < 1e-9);
    }

    #[test]
    fn audit_trail_is_complete_and_bounded() {
        let r = run("BASE", 1_700_000_000_000_000_000i64);
        assert!(!r.allocations.is_empty());
        // every non-zero amount recorded exactly once
        assert!(
            (sums(&r.collateral_cover) + sums(&r.guarantee_cover))
                - r.allocations.iter().map(|a| a.amount).sum::<f64>()
                < 1e-9
        );
        for a in &r.allocations {
            assert!(a.amount > 0.0);
            assert!(a.source_remaining >= -1e-9);
            assert!(a.loan_remaining >= -1e-9);
        }
        // stage string mapping used by the SQL function
        let batch = audit_batch(&r.allocations).unwrap();
        assert_eq!(batch.num_rows(), r.allocations.len());
        let stage = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(stage.value(0), "specified");
    }

    #[test]
    fn scenario_and_asof_filter_rows() {
        let asof = 1_700_000_000_000_000_000i64;
        // loan 3 is the only BB row and has pd 0.04 > others; with a second
        // loan_exposure copy we prove scenario/asof filtering happens before
        // allocation (a duplicated loan id would otherwise error).
        let mut extra = loan_tbl("BASE", asof);
        let schema = extra[0].schema();
        // Add a row for loan 1 under scenario STRESS with a far-future window:
        // it must be excluded when scenario=BASE and/or asof is earlier.
        let stress = RecordBatch::try_new(
            schema,
            vec![
                i64(vec![1]),
                f64(vec![999.0]),
                f64(vec![0.9]),
                str(vec!["BB"]),
                str(vec!["STRESS"]),
                i64(vec![asof + 100_000_000]),
                i64(vec![asof + 200_000_000]),
            ],
        )
        .unwrap();
        let combined = vec![extra.remove(0), stress];
        let snap = snapshot_from_batches(
            &combined,
            &coll_tbl(),
            &guar_tbl(),
            &coll_edges_tbl(),
            &guar_edges_tbl(),
            "BASE",
            asof,
            "ead",
            "greedy",
        )
        .unwrap();
        // STRESS row excluded by scenario + window; only the BASE row of loan 1
        // survives -> exactly three loans.
        assert_eq!(
            snap.loans.iter().map(|l| l.id).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(snap.loans[0].exposure, 100.0, "stress row excluded");
    }

    #[test]
    fn empty_slice_is_inert_not_an_error() {
        // No loan rows under STRESS (loan table is BASE-only): edges that point
        // at loans outside the slice are dropped, allocation is empty, and the
        // run must not fail with a dangling-edge error.
        let asof = 1_700_000_000_000_000_000i64;
        let snap = snapshot_from_batches(
            &loan_tbl("BASE", asof),
            &coll_tbl(),
            &guar_tbl(),
            &coll_edges_tbl(),
            &guar_edges_tbl(),
            "STRESS",
            asof,
            "ead",
            "greedy",
        )
        .unwrap();
        assert!(snap.loans.is_empty());
        assert!(snap.coll_edges.is_empty());
        assert!(snap.guar_edges.is_empty());
        let r = gtv_array::crm::crm_alloc_greedy(
            &snap.loans,
            &snap.collaterals,
            &snap.guarantors,
            &snap.coll_edges,
            &snap.guar_edges,
        )
        .unwrap();
        assert!(r.loan_id.is_empty());
        assert!(r.allocations.is_empty());
    }

    #[test]
    fn edge_to_never_existing_id_is_a_data_error() {
        let asof = 1_700_000_000_000_000_000i64;
        let ce = batches(
            schema(vec![
                ("col_id", DataType::Int64),
                ("loan_id", DataType::Int64),
                ("ratio", DataType::Float64),
                ("allocation_mode", DataType::Utf8),
            ]),
            vec![
                i64(vec![10]),
                i64(vec![77]), // never present in loan_exposure
                f64(vec![1.0]),
                str(vec!["optimizable"]),
            ],
        );
        let err = snapshot_from_batches(
            &loan_tbl("BASE", asof),
            &coll_tbl(),
            &guar_tbl(),
            &ce,
            &guar_edges_tbl(),
            "BASE",
            asof,
            "ead",
            "greedy",
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("collateral edge (10 -> 77) references an id absent"),
            "got: {err}"
        );
    }

    /// Phase 3: the LP allocator shares the same snapshot/Phase-1 front end and
    /// honours the per-edge guarantee `amount` caps (the greedy engine doesn't).
    #[cfg(feature = "crm-lp")]
    #[test]
    fn lp_method_honours_edge_caps_on_the_engine_snapshot() {
        let asof = 1_700_000_000_000_000_000i64;
        let snap = snapshot_from_batches(
            &loan_tbl("BASE", asof),
            &coll_tbl(),
            &guar_tbl(),
            &coll_edges_tbl(),
            &guar_edges_tbl(),
            "BASE",
            asof,
            "ead",
            "greedy",
        )
        .unwrap();
        let greedy = gtv_array::crm::crm_alloc_greedy(
            &snap.loans,
            &snap.collaterals,
            &snap.guarantors,
            &snap.coll_edges,
            &snap.guar_edges,
        )
        .unwrap();
        let lp = gtv_array::crm_lp::crm_alloc_lp(
            &snap.loans,
            &snap.collaterals,
            &snap.guarantors,
            &snap.coll_edges,
            &snap.guar_edges,
        )
        .unwrap();

        // L3 (loan 3): residual 50 after collateral (no collateral edge for it),
        // guarantee line capped at amount 40 on the edge -> LP = 40, greedy = 50.
        assert!((greedy.guarantee_cover[2] - 50.0).abs() < 1e-9);
        assert!((lp.guarantee_cover[2] - 40.0).abs() < 1e-6);
        assert!((lp.net_exposure[2] - 10.0).abs() < 1e-6);

        // audit labels the LP optimizable records distinctly
        assert!(lp
            .allocations
            .iter()
            .any(|a| a.stage == gtv_array::crm::CrmStage::Lp));
        // identical Phase-1 (specified) records in both engines
        let spec = |r: &gtv_array::crm::CrmResult| {
            r.allocations
                .iter()
                .filter(|a| a.stage == gtv_array::crm::CrmStage::Specified)
                .count()
        };
        assert_eq!(spec(&greedy), spec(&lp));
    }

    /// `haircut_efficiency` (Phase-2 variant): empty guarantee pools + bespoke
    /// loan / collateral tables.
    fn empty_batches(fields: Vec<(&str, DataType)>) -> Vec<RecordBatch> {
        let sch = schema(fields.clone());
        let cols: Vec<ArrayRef> = fields
            .iter()
            .map(|(_, t)| match t {
                DataType::Int64 => i64(Vec::new()),
                DataType::Float64 => f64(Vec::new()),
                _ => str(Vec::<&str>::new()),
            })
            .collect();
        vec![RecordBatch::try_new(sch, cols).unwrap()]
    }

    fn efficiency_snapshot(
        loans: &[RecordBatch],
        collateral: &[RecordBatch],
        coll_edges: &[RecordBatch],
        method: &str,
    ) -> gtv_array::crm::CrmResult {
        // an empty guarantee + guarantee_edges pair (no guarantors in these tests)
        let guar = empty_batches(vec![
            ("guarantor_id", DataType::Int64),
            ("amount", DataType::Float64),
            ("rating", DataType::Utf8),
        ]);
        let guar_edges = empty_batches(vec![
            ("guarantor_id", DataType::Int64),
            ("loan_id", DataType::Int64),
            ("amount", DataType::Float64),
            ("allocation_mode", DataType::Utf8),
        ]);
        let snap = snapshot_from_batches(
            loans,
            collateral,
            &guar,
            coll_edges,
            &guar_edges,
            "*",
            -1,
            "ead",
            method,
        )
        .unwrap();
        gtv_array::crm::crm_alloc_greedy(
            &snap.loans,
            &snap.collaterals,
            &snap.guarantors,
            &snap.coll_edges,
            &snap.guar_edges,
        )
        .unwrap()
    }

    fn loan_table(rows: Vec<(i64, f64, f64, &str)>) -> Vec<RecordBatch> {
        batches(
            schema(vec![
                ("loan_id", DataType::Int64),
                ("ead", DataType::Float64),
                ("pd", DataType::Float64),
                ("rating", DataType::Utf8),
            ]),
            vec![
                i64(rows.iter().map(|r| r.0).collect()),
                f64(rows.iter().map(|r| r.1).collect()),
                f64(rows.iter().map(|r| r.2).collect()),
                str(rows.iter().map(|r| r.3).collect::<Vec<_>>()),
            ],
        )
    }

    #[test]
    fn haircut_efficiency_orders_loans_by_risk_weight_not_pd() {
        // L1 rating BB (RW 1.0) but low pd; L2 rating A (RW 0.5) but high pd.
        // greedy covers by pd -> L2 first; haircut_efficiency by RW -> L1 first.
        let loans = loan_table(vec![(1, 100.0, 0.01, "BB"), (2, 100.0, 0.05, "A")]);
        // one collateral, haircut 0.2 -> eff value / capacity 80, both edges opt
        let collateral = batches(
            schema(vec![
                ("col_id", DataType::Int64),
                ("value", DataType::Float64),
                ("haircut", DataType::Float64),
            ]),
            vec![i64(vec![10]), f64(vec![100.0]), f64(vec![0.2])],
        );
        let edges = batches(
            schema(vec![
                ("col_id", DataType::Int64),
                ("loan_id", DataType::Int64),
                ("ratio", DataType::Float64),
                ("allocation_mode", DataType::Utf8),
            ]),
            vec![
                i64(vec![10, 10]),
                i64(vec![1, 2]),
                f64(vec![1.0, 1.0]),
                str(vec!["optimizable", "optimizable"]),
            ],
        );

        let greedy = efficiency_snapshot(&loans, &collateral, &edges, "greedy");
        assert_eq!(greedy.collateral_cover, vec![0.0, 80.0]); // pd 0.05 -> L2 first

        let eff = efficiency_snapshot(&loans, &collateral, &edges, "haircut_efficiency");
        assert_eq!(eff.collateral_cover, vec![80.0, 0.0]); // RW BB(1.0) > A(0.5)
        assert!((eff.net_exposure[0] - 20.0).abs() < 1e-9);
        assert_eq!(eff.net_exposure[1], 100.0);
    }

    #[test]
    fn haircut_efficiency_consumes_largest_effective_value_collateral_first() {
        // Both collaterals can only reach loan 1 (exposure 200):
        // small CASH (eff 60, greedy type-priority 3) and big EQUITY (eff 100,
        // greedy type-priority 1). Same total cover; only the source order
        // differs between the methods.
        let loans = loan_table(vec![(1, 200.0, 0.02, "BB")]);
        let collateral = batches(
            schema(vec![
                ("col_id", DataType::Int64),
                ("value", DataType::Float64),
                ("type", DataType::Utf8),
            ]),
            vec![
                i64(vec![1, 2]), // 1 = small CASH, 2 = big EQUITY
                f64(vec![60.0, 100.0]),
                str(vec!["CASH", "EQUITY"]),
            ],
        );
        let edges = batches(
            schema(vec![
                ("col_id", DataType::Int64),
                ("loan_id", DataType::Int64),
                ("ratio", DataType::Float64),
                ("allocation_mode", DataType::Utf8),
            ]),
            vec![
                i64(vec![1, 2]),
                i64(vec![1, 1]),
                f64(vec![1.0, 1.0]),
                str(vec!["optimizable", "optimizable"]),
            ],
        );

        let greedy = efficiency_snapshot(&loans, &collateral, &edges, "greedy");
        let eff = efficiency_snapshot(&loans, &collateral, &edges, "haircut_efficiency");
        let (g_total, e_total) = (greedy.collateral_cover[0], eff.collateral_cover[0]);
        assert!((g_total - 160.0).abs() < 1e-9 && (e_total - 160.0).abs() < 1e-9);

        let greedy_first = greedy
            .allocations
            .iter()
            .find(|a| a.stage == gtv_array::crm::CrmStage::Greedy)
            .unwrap()
            .source_id;
        let eff_first = eff
            .allocations
            .iter()
            .find(|a| a.stage == gtv_array::crm::CrmStage::Greedy)
            .unwrap()
            .source_id;
        assert_eq!(
            greedy_first, 1,
            "greedy consumes CASH (type priority) first"
        );
        assert_eq!(
            eff_first, 2,
            "efficiency consumes the larger effective value first"
        );
    }
}
