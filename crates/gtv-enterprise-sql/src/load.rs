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
use gtv_governance::{
    CollateralPledge, Exposure, GovernedCollateral, GovernedGuarantor, GovernedInputs,
    GuaranteePledge,
};
use gtv_largeexposure::{
    Entity, EntityKind, EventKind, ExposureEvent, ExposureMeasure, LeConfig, LimitMetric, LimitRule,
    LimitSet, Relationship, RelationshipKind, Scope,
};
use gtv_refdata::{EffectiveRange, HierarchyEdge, HierarchyKind, MasterKind, MasterRecord};
use gtv_scenario::{
    AlmCell, AlmConfig, BasisSpread, BehaviouralAdjustment, CashflowType, DayCount, Dimension,
    FtpCurve, FtpCurveCatalog, FtpPolicy, FtpPolicyCatalog, IrrbbConfig, LiquidityPremium, NmdCaps,
    NmdCategory, OptionalityCharge, Scenario, ScenarioKind, ScenarioStatus, Shock, ShockParams,
    ShockScenario, ShockTable, ShockTableVersion, TimeBand,
};

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

// ---------------------------------------------------------------------------
// IRRBB configuration loaders (prod_p4 audit P0)
// ---------------------------------------------------------------------------

/// `irrbb_scalars(key, value)` — override scalar IRRBB parameters. Recognised
/// keys: `floor`, `vol_bump`, `steepener_short`, `steepener_long`,
/// `flattener_short`, `flattener_long`, `decay_divisor`.
pub fn load_irrbb_scalars(config: &mut IrrbbConfig, batches: &[RecordBatch]) -> Result<usize> {
    let mut n = 0;
    for batch in batches {
        let keys = required_strings(batch, "key")?;
        let values = f64_from(column(batch, "value")?)?;
        for i in 0..keys.len() {
            match keys[i].as_str() {
                "floor" => config.floor = values[i],
                "vol_bump" => config.vol_bump = values[i],
                "steepener_short" => config.shock_formula.steepener_short = values[i],
                "steepener_long" => config.shock_formula.steepener_long = values[i],
                "flattener_short" => config.shock_formula.flattener_short = values[i],
                "flattener_long" => config.shock_formula.flattener_long = values[i],
                "decay_divisor" => config.shock_formula.decay_divisor = values[i],
                other => {
                    return Err(err(format!("load_irrbb_scalars: unknown key `{other}`")))
                }
            }
            n += 1;
        }
    }
    Ok(n)
}

/// `irrbb_nmd_caps(category, core_ratio_cap, maturity_cap_years)`.
pub fn load_irrbb_nmd_caps(config: &mut IrrbbConfig, batches: &[RecordBatch]) -> Result<usize> {
    let mut n = 0;
    for batch in batches {
        let categories = required_strings(batch, "category")?;
        let ratios = f64_from(column(batch, "core_ratio_cap")?)?;
        let maturities = f64_from(column(batch, "maturity_cap_years")?)?;
        for i in 0..categories.len() {
            let category = NmdCategory::parse(&categories[i]).ok_or_else(|| {
                err(format!(
                    "load_irrbb_nmd_caps: unknown NMD category `{}`",
                    categories[i]
                ))
            })?;
            config.nmd_caps.insert(
                category,
                NmdCaps {
                    core_ratio_cap: ratios[i],
                    maturity_cap_years: maturities[i],
                },
            );
            n += 1;
        }
    }
    Ok(n)
}

/// `irrbb_scenario_multipliers(scenario, cpr_gamma, tdrr_u)`.
pub fn load_irrbb_scenario_multipliers(
    config: &mut IrrbbConfig,
    batches: &[RecordBatch],
) -> Result<usize> {
    let mut n = 0;
    for batch in batches {
        let scenarios = required_strings(batch, "scenario")?;
        let cpr = f64_from(column(batch, "cpr_gamma")?)?;
        let tdrr = f64_from(column(batch, "tdrr_u")?)?;
        for i in 0..scenarios.len() {
            let scenario = ShockScenario::parse(&scenarios[i]).ok_or_else(|| {
                err(format!(
                    "load_irrbb_scenario_multipliers: unknown scenario `{}`",
                    scenarios[i]
                ))
            })?;
            config.cpr_multipliers.insert(scenario, cpr[i]);
            config.tdrr_multipliers.insert(scenario, tdrr[i]);
            n += 1;
        }
    }
    Ok(n)
}

/// `irrbb_time_bands(label, start_years, end_years, midpoint_years)`.
pub fn load_irrbb_time_bands(config: &mut IrrbbConfig, batches: &[RecordBatch]) -> Result<usize> {
    let mut bands = Vec::new();
    for batch in batches {
        let labels = required_strings(batch, "label")?;
        let starts = f64_from(column(batch, "start_years")?)?;
        let ends = f64_from(column(batch, "end_years")?)?;
        let midpoints = f64_from(column(batch, "midpoint_years")?)?;
        for i in 0..labels.len() {
            bands.push(TimeBand {
                label: labels[i].clone(),
                start_years: starts[i],
                end_years: ends[i],
                midpoint_years: midpoints[i],
            });
        }
    }
    if bands.is_empty() {
        return Err(err("load_irrbb_time_bands: no rows"));
    }
    bands.sort_by(|a, b| {
        a.midpoint_years
            .partial_cmp(&b.midpoint_years)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let n = bands.len();
    config.time_bands = bands;
    Ok(n)
}

/// `irrbb_shock_params(currency, parallel_bps, short_bps, long_bps)` — build a
/// [`ShockTable`] of the given version.
pub fn load_shock_table(
    version: ShockTableVersion,
    batches: &[RecordBatch],
) -> Result<ShockTable> {
    let mut params: BTreeMap<String, ShockParams> = BTreeMap::new();
    for batch in batches {
        let currencies = required_strings(batch, "currency")?;
        let parallel = f64_from(column(batch, "parallel_bps")?)?;
        let short = f64_from(column(batch, "short_bps")?)?;
        let long = f64_from(column(batch, "long_bps")?)?;
        for i in 0..currencies.len() {
            params.insert(
                currencies[i].to_ascii_uppercase(),
                ShockParams::new(parallel[i], short[i], long[i]),
            );
        }
    }
    Ok(ShockTable::from_params(version, params))
}

// ---------------------------------------------------------------------------
// FTP loaders
// ---------------------------------------------------------------------------

/// `ftp_curve_points(curve_id, version, currency, effective_from,
/// effective_to, tenor_days, zero_rate)`.
///
/// Rows sharing `(curve_id, version, currency, effective_from, effective_to)`
/// form one curve.
pub fn load_ftp_curves(
    catalog: &mut FtpCurveCatalog,
    batches: &[RecordBatch],
) -> Result<usize> {
    type Key = (String, u32, String, i64, i64);
    let mut groups: BTreeMap<Key, Vec<(i64, f64)>> = BTreeMap::new();
    for batch in batches {
        let ids = required_strings(batch, "curve_id")?;
        let versions = required_i64(batch, "version")?;
        let currencies = required_strings(batch, "currency")?;
        let froms = required_i64(batch, "effective_from")?;
        let tos = required_i64(batch, "effective_to")?;
        let tenors = required_i64(batch, "tenor_days")?;
        let rates = f64_from(column(batch, "zero_rate")?)?;
        for i in 0..ids.len() {
            groups
                .entry((
                    ids[i].clone(),
                    versions[i] as u32,
                    currencies[i].clone(),
                    froms[i],
                    tos[i],
                ))
                .or_default()
                .push((tenors[i], rates[i]));
        }
    }
    let n = groups.len();
    for ((id, version, currency, from, to), tenors) in groups {
        let curve = FtpCurve::new(id, version, currency, from, to, tenors).map_err(err)?;
        catalog.register(curve).map_err(err)?;
    }
    Ok(n)
}

/// Load FTP policies from a header table plus the four component tables.
///
/// * headers: `policy_id, version, effective_from, effective_to`
/// * liquidity: `policy_id, version, product, tenor_days, spread`
/// * basis: `policy_id, version, currency, tenor_days, spread`
/// * optionality: `policy_id, version, product, charge`
/// * behavioural: `policy_id, version, product, adjustment`
#[allow(clippy::too_many_arguments)]
pub fn load_ftp_policies(
    catalog: &mut FtpPolicyCatalog,
    headers: &[RecordBatch],
    liquidity: &[RecordBatch],
    basis: &[RecordBatch],
    optionality: &[RecordBatch],
    behavioural: &[RecordBatch],
) -> Result<usize> {
    type Key = (String, u32);
    let mut heads: BTreeMap<Key, (i64, i64)> = BTreeMap::new();
    for batch in headers {
        let ids = required_strings(batch, "policy_id")?;
        let versions = required_i64(batch, "version")?;
        let froms = required_i64(batch, "effective_from")?;
        let tos = required_i64(batch, "effective_to")?;
        for i in 0..ids.len() {
            heads.insert((ids[i].clone(), versions[i] as u32), (froms[i], tos[i]));
        }
    }

    let mut liq: BTreeMap<Key, Vec<LiquidityPremium>> = BTreeMap::new();
    for batch in liquidity {
        let ids = required_strings(batch, "policy_id")?;
        let versions = required_i64(batch, "version")?;
        let products = required_strings(batch, "product")?;
        let tenors = required_i64(batch, "tenor_days")?;
        let spreads = f64_from(column(batch, "spread")?)?;
        for i in 0..ids.len() {
            liq.entry((ids[i].clone(), versions[i] as u32))
                .or_default()
                .push(LiquidityPremium {
                    product: products[i].clone(),
                    tenor_days: tenors[i],
                    spread: spreads[i],
                });
        }
    }

    let mut bas: BTreeMap<Key, Vec<BasisSpread>> = BTreeMap::new();
    for batch in basis {
        let ids = required_strings(batch, "policy_id")?;
        let versions = required_i64(batch, "version")?;
        let currencies = required_strings(batch, "currency")?;
        let tenors = required_i64(batch, "tenor_days")?;
        let spreads = f64_from(column(batch, "spread")?)?;
        for i in 0..ids.len() {
            bas.entry((ids[i].clone(), versions[i] as u32))
                .or_default()
                .push(BasisSpread {
                    currency: currencies[i].clone(),
                    tenor_days: tenors[i],
                    spread: spreads[i],
                });
        }
    }

    let mut opt: BTreeMap<Key, Vec<OptionalityCharge>> = BTreeMap::new();
    for batch in optionality {
        let ids = required_strings(batch, "policy_id")?;
        let versions = required_i64(batch, "version")?;
        let products = required_strings(batch, "product")?;
        let charges = f64_from(column(batch, "charge")?)?;
        for i in 0..ids.len() {
            opt.entry((ids[i].clone(), versions[i] as u32))
                .or_default()
                .push(OptionalityCharge {
                    product: products[i].clone(),
                    charge: charges[i],
                });
        }
    }

    let mut beh: BTreeMap<Key, Vec<BehaviouralAdjustment>> = BTreeMap::new();
    for batch in behavioural {
        let ids = required_strings(batch, "policy_id")?;
        let versions = required_i64(batch, "version")?;
        let products = required_strings(batch, "product")?;
        let adjustments = f64_from(column(batch, "adjustment")?)?;
        for i in 0..ids.len() {
            beh.entry((ids[i].clone(), versions[i] as u32))
                .or_default()
                .push(BehaviouralAdjustment {
                    product: products[i].clone(),
                    adjustment: adjustments[i],
                });
        }
    }

    let n = heads.len();
    for ((id, version), (from, to)) in heads {
        let key = (id.clone(), version);
        let mut policy = FtpPolicy::new(id, version, from, to);
        policy.liquidity = liq.remove(&key).unwrap_or_default();
        policy.basis = bas.remove(&key).unwrap_or_default();
        policy.optionality = opt.remove(&key).unwrap_or_default();
        policy.behavioural = beh.remove(&key).unwrap_or_default();
        catalog.register(policy).map_err(err)?;
    }
    for name in ["liquidity", "basis", "optionality", "behavioural"] {
        let leftover = match name {
            "liquidity" => liq.len(),
            "basis" => bas.len(),
            "optionality" => opt.len(),
            _ => beh.len(),
        };
        if leftover > 0 {
            return Err(err(format!(
                "load_ftp_policies: {name} rows reference an unknown policy header"
            )));
        }
    }
    Ok(n)
}

// ---------------------------------------------------------------------------
// CRM governance (RuleSet) loaders
// ---------------------------------------------------------------------------

fn split_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

/// Load versioned CRM rule sets from five tables:
///
/// * headers: `ruleset_id, version, effective_from, effective_to`
/// * collateral: `ruleset_id, version, collateral_type, priority` +
///   optional `eligible, haircut, fx_haircut, maturity_haircut, currencies`
/// * guarantees: `ruleset_id, version, guarantor_type, priority` +
///   optional `eligible, jurisdictions`
/// * wrong-way: `ruleset_id, version, counterparty, collateral_type`
/// * concentration: `ruleset_id, version, collateral_type, limit`
#[allow(clippy::too_many_arguments)]
pub fn load_crm_rulesets(
    registry: &mut gtv_governance::RuleRegistry,
    headers: &[RecordBatch],
    collateral: &[RecordBatch],
    guarantees: &[RecordBatch],
    wrong_way: &[RecordBatch],
    concentration: &[RecordBatch],
) -> Result<usize> {
    use gtv_governance::{CollateralRule, ConcentrationLimit, GuaranteeRule, RuleSet, WrongWayRisk};
    type Key = (String, u32);
    let mut heads: BTreeMap<Key, (i64, i64)> = BTreeMap::new();
    for batch in headers {
        let ids = required_strings(batch, "ruleset_id")?;
        let versions = required_i64(batch, "version")?;
        let froms = required_i64(batch, "effective_from")?;
        let tos = required_i64(batch, "effective_to")?;
        for i in 0..ids.len() {
            heads.insert((ids[i].clone(), versions[i] as u32), (froms[i], tos[i]));
        }
    }

    let eligible_of = |v: &[Option<String>], i: usize| -> bool {
        match &v[i] {
            None => true,
            Some(s) => matches!(
                s.trim().to_ascii_lowercase().as_str(),
                "true" | "1" | "yes" | "y"
            ),
        }
    };

    let mut coll: BTreeMap<Key, Vec<CollateralRule>> = BTreeMap::new();
    for batch in collateral {
        let ids = required_strings(batch, "ruleset_id")?;
        let versions = required_i64(batch, "version")?;
        let types = required_strings(batch, "collateral_type")?;
        let priorities = f64_from(column(batch, "priority")?)?;
        let eligible = optional_strings(batch, "eligible")?;
        let haircut = optional_f64(batch, "haircut")?;
        let fx = optional_f64(batch, "fx_haircut")?;
        let maturity = optional_f64(batch, "maturity_haircut")?;
        let currencies = optional_strings(batch, "currencies")?;
        for i in 0..ids.len() {
            coll.entry((ids[i].clone(), versions[i] as u32))
                .or_default()
                .push(CollateralRule {
                    collateral_type: types[i].clone(),
                    eligible: eligible_of(&eligible, i),
                    priority: priorities[i],
                    haircut: haircut[i],
                    fx_haircut: fx[i],
                    maturity_haircut: maturity[i],
                    eligible_currencies: currencies[i]
                        .as_deref()
                        .map(split_list)
                        .unwrap_or_default(),
                });
        }
    }

    let mut guar: BTreeMap<Key, Vec<GuaranteeRule>> = BTreeMap::new();
    for batch in guarantees {
        let ids = required_strings(batch, "ruleset_id")?;
        let versions = required_i64(batch, "version")?;
        let types = required_strings(batch, "guarantor_type")?;
        let priorities = f64_from(column(batch, "priority")?)?;
        let eligible = optional_strings(batch, "eligible")?;
        let jurisdictions = optional_strings(batch, "jurisdictions")?;
        for i in 0..ids.len() {
            guar.entry((ids[i].clone(), versions[i] as u32))
                .or_default()
                .push(GuaranteeRule {
                    guarantor_type: types[i].clone(),
                    eligible: eligible_of(&eligible, i),
                    priority: priorities[i],
                    eligible_jurisdictions: jurisdictions[i]
                        .as_deref()
                        .map(split_list)
                        .unwrap_or_default(),
                });
        }
    }

    let mut ww: BTreeMap<Key, Vec<WrongWayRisk>> = BTreeMap::new();
    for batch in wrong_way {
        let ids = required_strings(batch, "ruleset_id")?;
        let versions = required_i64(batch, "version")?;
        let counterparties = required_strings(batch, "counterparty")?;
        let types = required_strings(batch, "collateral_type")?;
        for i in 0..ids.len() {
            ww.entry((ids[i].clone(), versions[i] as u32))
                .or_default()
                .push(WrongWayRisk {
                    counterparty: counterparties[i].clone(),
                    collateral_type: types[i].clone(),
                });
        }
    }

    let mut conc: BTreeMap<Key, Vec<ConcentrationLimit>> = BTreeMap::new();
    for batch in concentration {
        let ids = required_strings(batch, "ruleset_id")?;
        let versions = required_i64(batch, "version")?;
        let types = required_strings(batch, "collateral_type")?;
        let limits = f64_from(column(batch, "limit")?)?;
        for i in 0..ids.len() {
            conc.entry((ids[i].clone(), versions[i] as u32))
                .or_default()
                .push(ConcentrationLimit {
                    collateral_type: types[i].clone(),
                    limit: limits[i],
                });
        }
    }

    let n = heads.len();
    for ((id, version), (from, to)) in heads {
        let key = (id.clone(), version);
        let mut rule =
            RuleSet::new(id, version, EffectiveRange::new(from, to).map_err(err)?);
        rule.collateral = coll.remove(&key).unwrap_or_default();
        rule.guarantees = guar.remove(&key).unwrap_or_default();
        rule.wrong_way = ww.remove(&key).unwrap_or_default();
        rule.concentration = conc.remove(&key).unwrap_or_default();
        // Validate against the rule set's own effective start (always inside
        // its range), so rule sets effective in the future can still be loaded.
        registry.register(rule, from).map_err(err)?;
    }
    Ok(n)
}

/// Load [`GovernedInputs`] (exposures, collateral, guarantors and pledges):
///
/// * exposures: `loan_id, counterparty, exposure, currency` + optional
///   `priority, maturity_days, netting_set`
/// * collateral: `col_id, collateral_type, value, currency` + optional
///   `maturity_days`
/// * guarantors: `guarantor_id, guarantor_type, capacity, jurisdiction`
/// * collateral pledges: `col_id, loan_id` + optional `ratio` (default 1.0)
/// * guarantee pledges: `guarantor_id, loan_id` + optional `amount` (default 0)
pub fn load_governed_inputs(
    exposures: &[RecordBatch],
    collateral: &[RecordBatch],
    guarantors: &[RecordBatch],
    collateral_pledges: &[RecordBatch],
    guarantee_pledges: &[RecordBatch],
) -> Result<GovernedInputs> {
    let mut out = GovernedInputs::new();
    for batch in exposures {
        let ids = required_i64(batch, "loan_id")?;
        let counterparties = required_strings(batch, "counterparty")?;
        let exposure = f64_from(column(batch, "exposure")?)?;
        let currencies = required_strings(batch, "currency")?;
        let priorities = optional_f64(batch, "priority")?;
        let maturities = optional_i64(batch, "maturity_days", 0)?;
        let netting = optional_strings(batch, "netting_set")?;
        for i in 0..ids.len() {
            let mut e = Exposure::new(
                ids[i] as u64,
                counterparties[i].clone(),
                exposure[i],
                currencies[i].clone(),
            )
            .with_priority(priorities[i])
            .with_maturity(maturities[i]);
            if let Some(ns) = &netting[i] {
                e = e.with_netting_set(ns.clone());
            }
            out.exposures.push(e);
        }
    }
    for batch in collateral {
        let ids = required_i64(batch, "col_id")?;
        let types = required_strings(batch, "collateral_type")?;
        let value = f64_from(column(batch, "value")?)?;
        let currencies = required_strings(batch, "currency")?;
        let maturities = optional_i64(batch, "maturity_days", 0)?;
        for i in 0..ids.len() {
            out.collaterals.push(
                GovernedCollateral::new(
                    ids[i] as u64,
                    types[i].clone(),
                    value[i],
                    currencies[i].clone(),
                )
                .with_maturity(maturities[i]),
            );
        }
    }
    for batch in guarantors {
        let ids = required_i64(batch, "guarantor_id")?;
        let types = required_strings(batch, "guarantor_type")?;
        let capacity = f64_from(column(batch, "capacity")?)?;
        let jurisdictions = required_strings(batch, "jurisdiction")?;
        for i in 0..ids.len() {
            out.guarantors.push(GovernedGuarantor::new(
                ids[i] as u64,
                types[i].clone(),
                capacity[i],
                jurisdictions[i].clone(),
            ));
        }
    }
    for batch in collateral_pledges {
        let cols = required_i64(batch, "col_id")?;
        let loans = required_i64(batch, "loan_id")?;
        let ratios = if has_column(batch, "ratio") {
            f64_from(column(batch, "ratio")?)?
        } else {
            vec![1.0; batch.num_rows()]
        };
        for i in 0..cols.len() {
            out.collateral_pledges.push(CollateralPledge {
                col_id: cols[i] as u64,
                loan_id: loans[i] as u64,
                ratio: ratios[i],
            });
        }
    }
    for batch in guarantee_pledges {
        let guars = required_i64(batch, "guarantor_id")?;
        let loans = required_i64(batch, "loan_id")?;
        let amounts = optional_f64(batch, "amount")?;
        for i in 0..guars.len() {
            out.guarantee_pledges.push(GuaranteePledge {
                guarantor_id: guars[i] as u64,
                loan_id: loans[i] as u64,
                amount: amounts[i],
            });
        }
    }
    Ok(out)
}

/// Optional `Float64` column as `Vec<f64>` defaulting to `0.0`.
fn optional_f64(batch: &RecordBatch, name: &str) -> Result<Vec<f64>> {
    if has_column(batch, name) {
        f64_from(column(batch, name)?)
    } else {
        Ok(vec![0.0; batch.num_rows()])
    }
}

// ---------------------------------------------------------------------------
// ALM loaders
// ---------------------------------------------------------------------------

/// `alm_params(key, value)` — override ALM conventions and default assumptions.
/// Recognised keys: `day_count` (`act/365` | `act/360`), `deposit_runoff`,
/// `wholesale_outflow`, `inflow_haircut`, `deposit_decay_period_days`.
pub fn load_alm_config(config: &mut AlmConfig, batches: &[RecordBatch]) -> Result<usize> {
    let mut n = 0;
    for batch in batches {
        let keys = required_strings(batch, "key")?;
        let values = required_strings(batch, "value")?;
        for i in 0..keys.len() {
            let raw = values[i].trim();
            match keys[i].as_str() {
                "day_count" => {
                    config.day_count = DayCount::parse(raw).ok_or_else(|| {
                        err(format!("load_alm_config: unknown day_count `{raw}`"))
                    })?;
                }
                "deposit_runoff" => config.liquidity_stress.deposit_runoff = parse_f64(raw)?,
                "wholesale_outflow" => {
                    config.liquidity_stress.wholesale_outflow = parse_f64(raw)?
                }
                "inflow_haircut" => config.liquidity_stress.inflow_haircut = parse_f64(raw)?,
                "deposit_decay_period_days" => {
                    config.deposit_decay_period_days = parse_i64(raw)?
                }
                other => return Err(err(format!("load_alm_config: unknown key `{other}`"))),
            }
            n += 1;
        }
    }
    Ok(n)
}

/// Load `AlmCell` rows. Required columns: `scenario_id, legal_entity, currency,
/// product, time_bucket, cashflow_type, amount`. Optional: `as_of_date`,
/// `discount_factor`, `repricing_date` (default `time_bucket`),
/// `assumption_version`.
pub fn load_alm_cells(batches: &[RecordBatch]) -> Result<Vec<AlmCell>> {
    let mut out = Vec::new();
    for batch in batches {
        let scenarios = required_strings(batch, "scenario_id")?;
        let legal = required_strings(batch, "legal_entity")?;
        let currencies = required_strings(batch, "currency")?;
        let products = required_strings(batch, "product")?;
        let buckets = required_i64(batch, "time_bucket")?;
        let types = required_strings(batch, "cashflow_type")?;
        let amounts = f64_from(column(batch, "amount")?)?;
        let as_of = optional_i64(batch, "as_of_date", 0)?;
        let repricing = if has_column(batch, "repricing_date") {
            i64_from(column(batch, "repricing_date")?)?
        } else {
            vec![None; batch.num_rows()]
        };
        let dfs = if has_column(batch, "discount_factor") {
            Some(f64_from(column(batch, "discount_factor")?)?)
        } else {
            None
        };
        let versions = optional_strings(batch, "assumption_version")?;
        for i in 0..batch.num_rows() {
            let cashflow_type = CashflowType::parse(&types[i]).ok_or_else(|| {
                err(format!(
                    "load_alm_cells: unknown cashflow_type `{}`",
                    types[i]
                ))
            })?;
            let mut cell = AlmCell::new(
                scenarios[i].clone(),
                legal[i].clone(),
                currencies[i].clone(),
                products[i].clone(),
                buckets[i],
                cashflow_type,
                amounts[i],
            );
            cell.as_of_date = as_of[i];
            cell.repricing_date = repricing[i].unwrap_or(buckets[i]);
            if let Some(dfs) = &dfs {
                if dfs[i].is_finite() {
                    cell.discount_factor = Some(dfs[i]);
                }
            }
            cell.behavioural_assumption_version =
                versions[i].clone().unwrap_or_default();
            out.push(cell);
        }
    }
    Ok(out)
}

/// `irrbb_curve_points(tenor_days, zero_rate [, day_count])` — a base
/// risk-free zero curve for the standardised EVE.
pub fn load_discount_curve(batches: &[RecordBatch]) -> Result<gtv_scenario::DiscountCurve> {
    let mut points = Vec::new();
    let mut day_count = DayCount::Act365;
    for batch in batches {
        let tenors = required_i64(batch, "tenor_days")?;
        let rates = f64_from(column(batch, "zero_rate")?)?;
        if has_column(batch, "day_count") {
            let dcs = required_strings(batch, "day_count")?;
            if let Some(first) = dcs.first() {
                day_count = DayCount::parse(first)
                    .ok_or_else(|| err(format!("unknown day_count `{first}`")))?;
            }
        }
        for i in 0..tenors.len() {
            points.push((tenors[i], rates[i]));
        }
    }
    gtv_scenario::DiscountCurve::from_zero_rates_dc(points, day_count).map_err(err)
}

fn parse_f64(raw: &str) -> Result<f64> {
    raw.parse::<f64>()
        .map_err(|_| err(format!("expected a number, got `{raw}`")))
}

fn parse_i64(raw: &str) -> Result<i64> {
    raw.parse::<i64>()
        .map_err(|_| err(format!("expected an integer, got `{raw}`")))
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

// ---------------------------------------------------------------------------
// Large Exposure (MA(BS)28) loaders
// ---------------------------------------------------------------------------

fn truthy(v: &Option<String>) -> bool {
    v.as_deref().is_some_and(|s| {
        matches!(
            s.trim().to_ascii_lowercase().as_str(),
            "true" | "1" | "yes" | "y" | "t"
        )
    })
}

/// `le_entity(entity_id, entity_type?, economic_sector?, country_code?,
/// rating_grade?, is_connected?, connected_paragraph?, scope?, is_g_sib?)`.
pub fn load_le_entities(batches: &[RecordBatch]) -> Result<Vec<Entity>> {
    let mut out = Vec::new();
    for batch in batches {
        let ids = required_strings(batch, "entity_id")?;
        let kinds = optional_strings(batch, "entity_type")?;
        let sectors = optional_strings(batch, "economic_sector")?;
        let countries = optional_strings(batch, "country_code")?;
        let ratings = optional_strings(batch, "rating_grade")?;
        let connected = optional_strings(batch, "is_connected")?;
        let paragraphs = optional_strings(batch, "connected_paragraph")?;
        let scopes = optional_strings(batch, "scope")?;
        let gsib = optional_strings(batch, "is_g_sib")?;
        for i in 0..ids.len() {
            let kind = kinds[i]
                .as_deref()
                .and_then(EntityKind::parse)
                .unwrap_or(EntityKind::Corporate);
            let mut e = Entity::new(ids[i].clone(), kind);
            e.economic_sector = sectors[i].clone();
            e.country_code = countries[i].clone();
            e.rating_grade = ratings[i].clone();
            e.is_connected = truthy(&connected[i]);
            e.connected_paragraph = paragraphs[i].clone();
            e.scope = scopes[i]
                .as_deref()
                .and_then(Scope::parse)
                .unwrap_or(Scope::Both);
            e.is_g_sib = truthy(&gsib[i]);
            out.push(e);
        }
    }
    Ok(out)
}

/// `le_relationship(parent_id, child_id, relation, ownership_pct?,
/// valid_from, valid_to?)`.
pub fn load_le_relationships(batches: &[RecordBatch]) -> Result<Vec<Relationship>> {
    let mut out = Vec::new();
    for batch in batches {
        let parents = required_strings(batch, "parent_id")?;
        let children = required_strings(batch, "child_id")?;
        let relations = required_strings(batch, "relation")?;
        let pcts = optional_f64(batch, "ownership_pct")?;
        let froms = required_i64(batch, "valid_from")?;
        let tos = optional_i64(batch, "valid_to", i64::MAX)?;
        for i in 0..parents.len() {
            let kind = RelationshipKind::parse(&relations[i]).ok_or_else(|| {
                err(format!("le_relationship: unknown relation `{}`", relations[i]))
            })?;
            out.push(Relationship::new(
                parents[i].clone(),
                children[i].clone(),
                kind,
                pcts[i],
                EffectiveRange::new(froms[i], tos[i]).map_err(err)?,
            ));
        }
    }
    Ok(out)
}

/// `le_exposure(event_id, entity_id, business_from, business_to?,
/// on_balance?, trading_book?, off_balance?, default_risk?, additional_risk?,
/// indirect?, crm_reduction?, currency?, net_short?, exempt?,
/// exemption_provision?, deduction?, kind?, ref_event_id?, system_from?)`.
pub fn load_le_exposures(batches: &[RecordBatch]) -> Result<Vec<ExposureEvent>> {
    let mut out = Vec::new();
    for batch in batches {
        let ids = required_strings(batch, "event_id")?;
        let entities = required_strings(batch, "entity_id")?;
        let froms = required_i64(batch, "business_from")?;
        let tos = optional_i64(batch, "business_to", i64::MAX)?;
        let on_balance = optional_f64(batch, "on_balance")?;
        let trading = optional_f64(batch, "trading_book")?;
        let off_balance = optional_f64(batch, "off_balance")?;
        let default_risk = optional_f64(batch, "default_risk")?;
        let additional = optional_f64(batch, "additional_risk")?;
        let indirect = optional_f64(batch, "indirect")?;
        let crm = optional_f64(batch, "crm_reduction")?;
        let currencies = optional_strings(batch, "currency")?;
        let net_short = optional_strings(batch, "net_short")?;
        let exempt = optional_strings(batch, "exempt")?;
        let provisions = optional_strings(batch, "exemption_provision")?;
        let deductions = optional_f64(batch, "deduction")?;
        let kinds = optional_strings(batch, "kind")?;
        let refs = optional_strings(batch, "ref_event_id")?;
        let systems = optional_i64(batch, "system_from", 0)?;
        for i in 0..ids.len() {
            let measure = ExposureMeasure::zero()
                .on_balance(on_balance[i])
                .trading_book(trading[i])
                .off_balance(off_balance[i])
                .default_risk(default_risk[i])
                .additional_risk(additional[i])
                .indirect(indirect[i]);
            let mut e = ExposureEvent::new(ids[i].clone(), entities[i].clone(), measure, froms[i], tos[i]);
            e.crm_reduction = crm[i];
            e.currency = currencies[i].clone().unwrap_or_else(|| "HKD".to_string());
            e.net_short = truthy(&net_short[i]);
            e.exempt = truthy(&exempt[i]);
            e.exemption_provision = provisions[i].clone();
            e.deduction = deductions[i];
            e.kind = kinds[i]
                .as_deref()
                .and_then(EventKind::parse)
                .unwrap_or(EventKind::Orig);
            e.ref_event_id = refs[i].clone();
            e.system_from = systems[i];
            out.push(e);
        }
    }
    Ok(out)
}

/// `le_config(key, value)` — override scalar Large Exposure parameters.
pub fn load_le_config(config: &mut LeConfig, batches: &[RecordBatch]) -> Result<usize> {
    let mut n = 0;
    for batch in batches {
        let keys = required_strings(batch, "key")?;
        let values = required_strings(batch, "value")?;
        for i in 0..keys.len() {
            let raw = values[i].trim();
            match keys[i].as_str() {
                "control_threshold" => config.control_threshold = parse_f64(raw)?,
                "include_economic_dependence" => {
                    config.include_economic_dependence = matches!(
                        raw.to_ascii_lowercase().as_str(),
                        "true" | "1" | "yes" | "y"
                    )
                }
                "report_currency" => config.report_currency = raw.to_string(),
                "tier1_source" => {
                    config.tier1_source = gtv_largeexposure::Tier1Source::parse(raw)
                        .ok_or_else(|| err(format!("unknown tier1_source `{raw}`")))?
                }
                "default_top_n" => config.default_top_n = parse_i64(raw)? as usize,
                "warn_ratio" => config.warn_ratio = parse_f64(raw)?,
                "limit_ratio" => config.limit_ratio = parse_f64(raw)?,
                "g_sib_limit" => config.g_sib_limit = parse_f64(raw)?,
                "report_threshold" => config.report_threshold = parse_f64(raw)?,
                "connected_report_threshold" => {
                    config.connected_report_threshold = parse_f64(raw)?
                }
                "derivative_measure" => {
                    config.derivative_measure = gtv_largeexposure::DerivativeMeasure::parse(raw)
                        .ok_or_else(|| err(format!("unknown derivative_measure `{raw}`")))?
                }
                other => return Err(err(format!("load_le_config: unknown key `{other}`"))),
            }
            n += 1;
        }
    }
    Ok(n)
}

/// `le_limit(limit_id, metric, limit_ratio, key?, report_threshold?,
/// warn_ratio?, top_n?, applied_to?)`.
pub fn load_le_limits(limits: &mut LimitSet, batches: &[RecordBatch]) -> Result<usize> {
    let mut n = 0;
    for batch in batches {
        let ids = required_strings(batch, "limit_id")?;
        let metrics = required_strings(batch, "metric")?;
        let ratios = f64_from(column(batch, "limit_ratio")?)?;
        let keys = optional_strings(batch, "key")?;
        let reports = optional_f64(batch, "report_threshold")?;
        let warns = optional_f64(batch, "warn_ratio")?;
        let tops = optional_i64(batch, "top_n", 0)?;
        let applied = optional_strings(batch, "applied_to")?;
        for i in 0..ids.len() {
            let metric = LimitMetric::parse(&metrics[i]).ok_or_else(|| {
                err(format!("le_limit: unknown metric `{}`", metrics[i]))
            })?;
            let mut rule = LimitRule::new(ids[i].clone(), metric, ratios[i]);
            if let Some(k) = &keys[i] {
                rule = rule.with_key(k.clone());
            }
            if reports[i] > 0.0 {
                rule = rule.with_report_threshold(reports[i]);
            }
            if warns[i] > 0.0 {
                rule = rule.with_warn_ratio(warns[i]);
            }
            if tops[i] > 0 {
                rule = rule.with_top_n(tops[i] as usize);
            }
            if let Some(a) = &applied[i] {
                rule.applied_to = a.clone();
            }
            limits.add(rule);
            n += 1;
        }
    }
    Ok(n)
}

/// `le_capital(tier1 [, as_of])` — the Tier 1 denominator (latest row wins).
pub fn load_le_tier1(batches: &[RecordBatch]) -> Result<f64> {
    let mut best: Option<(i64, f64)> = None;
    for batch in batches {
        let tier1 = f64_from(column(batch, "tier1")?)?;
        let as_of = optional_i64(batch, "as_of", 0)?;
        for i in 0..tier1.len() {
            let candidate = (as_of[i], tier1[i]);
            if best.is_none_or(|(d, _)| as_of[i] >= d) {
                best = Some(candidate);
            }
        }
    }
    best.map(|(_, v)| v)
        .ok_or_else(|| err("le_capital: no rows"))
}
