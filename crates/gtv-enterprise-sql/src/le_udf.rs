//! Large Exposure SQL table functions (MA(BS)28 / LE-4).

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Expr;
use datafusion::scalar::ScalarValue;

use gtv_largeexposure::{
    concentration, AggregateKind, ConcentrationDimension, LimitRule, LimitStatus, MaBs28Part,
    Measure,
};

use crate::registry::Registry;

fn poisoned(what: &str) -> DataFusionError {
    DataFusionError::Execution(format!("{what}: enterprise registry poisoned"))
}

fn lit_str(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)), _) => {
            Ok(v.clone())
        }
        other => Err(DataFusionError::Execution(format!(
            "expected a string literal, got {other:?}"
        ))),
    }
}

fn lit_i64(expr: &Expr) -> Result<i64> {
    match expr {
        Expr::Literal(sv, _) => match sv.clone().cast_to(&DataType::Int64)? {
            ScalarValue::Int64(Some(v)) => Ok(v),
            other => Err(DataFusionError::Execution(format!(
                "expected an integer literal, got {other:?}"
            ))),
        },
        other => Err(DataFusionError::Execution(format!(
            "expected a literal, got {other:?}"
        ))),
    }
}

fn lit_f64(expr: &Expr) -> Result<f64> {
    match expr {
        Expr::Literal(sv, _) => match sv.clone().cast_to(&DataType::Float64)? {
            ScalarValue::Float64(Some(v)) => Ok(v),
            other => Err(DataFusionError::Execution(format!(
                "expected a numeric literal, got {other:?}"
            ))),
        },
        other => Err(DataFusionError::Execution(format!(
            "expected a literal, got {other:?}"
        ))),
    }
}

fn strings(values: &[String]) -> ArrayRef {
    Arc::new(StringArray::from(
        values.iter().map(String::as_str).collect::<Vec<_>>(),
    ))
}

fn opt_strings(values: &[Option<String>]) -> ArrayRef {
    Arc::new(StringArray::from_iter(
        values.iter().map(|v| v.as_deref()),
    ))
}

fn mem_table(schema: SchemaRef, batch: RecordBatch) -> Result<Arc<dyn TableProvider>> {
    Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
}

fn parse_kind(s: &str, what: &str) -> Result<AggregateKind> {
    match s.trim().to_ascii_lowercase().replace([' ', '-'], "_").as_str() {
        "entity" => Ok(AggregateKind::Entity),
        "lc_group" | "group" => Ok(AggregateKind::LcGroup),
        "connected_party" | "connected" => Ok(AggregateKind::ConnectedParty),
        "group_affiliate" | "affiliate" | "intragroup" => Ok(AggregateKind::GroupAffiliate),
        other => Err(DataFusionError::Execution(format!(
            "{what}: unknown kind `{other}`"
        ))),
    }
}

/// `le_ma_bs28(part, period_start, period_end)`.
#[derive(Debug)]
pub struct LeMaBs28TableFunction {
    registry: Registry,
}

impl LeMaBs28TableFunction {
    pub fn new(registry: Registry) -> Self {
        Self { registry }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("rank", DataType::UInt64, false),
            Field::new("counterparty_id", DataType::Utf8, true),
            Field::new("lc_group_id", DataType::Utf8, true),
            Field::new("maximum_exposure", DataType::Float64, false),
            Field::new("on_balance", DataType::Float64, false),
            Field::new("trading_book", DataType::Float64, false),
            Field::new("off_balance", DataType::Float64, false),
            Field::new("default_risk", DataType::Float64, false),
            Field::new("indirect", DataType::Float64, false),
            Field::new("additional_risk", DataType::Float64, false),
            Field::new("total", DataType::Float64, false),
            Field::new("deductions", DataType::Float64, false),
            Field::new("economic_sector", DataType::Utf8, false),
            Field::new("relationship_code", DataType::Utf8, true),
            Field::new("percent_of_tier1", DataType::Float64, false),
            Field::new("exemption_provision", DataType::Utf8, true),
        ]))
    }
}

impl TableFunctionImpl for LeMaBs28TableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let usage = "le_ma_bs28(part, period_start, period_end)";
        let exprs = args.exprs();
        let part = MaBs28Part::parse(&lit_str(exprs.first().ok_or_else(|| {
            DataFusionError::Execution(format!("{usage}: missing `part`"))
        })?)?)
        .ok_or_else(|| DataFusionError::Execution(format!("{usage}: unknown part")))?;
        let start = lit_i64(exprs.get(1).ok_or_else(|| {
            DataFusionError::Execution(format!("{usage}: missing `period_start`"))
        })?)?;
        let end = lit_i64(exprs.get(2).ok_or_else(|| {
            DataFusionError::Execution(format!("{usage}: missing `period_end`"))
        })?)?;

        let mut reg = self.registry.write().map_err(|_| poisoned("le_ma_bs28"))?;
        let tier1 = reg.le_tier1;
        let rows = gtv_largeexposure::ma_bs28_report(&mut reg.le_ledger, part, start, end, tier1);
        drop(reg);

        let schema = Self::schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(
                    rows.iter().map(|r| r.rank as u64).collect::<Vec<_>>(),
                )) as ArrayRef,
                opt_strings(
                    &rows
                        .iter()
                        .map(|r| r.counterparty_id.clone())
                        .collect::<Vec<_>>(),
                ),
                opt_strings(&rows.iter().map(|r| r.lc_group_id.clone()).collect::<Vec<_>>()),
                Arc::new(Float64Array::from(
                    rows.iter().map(|r| r.maximum_exposure).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Float64Array::from(
                    rows.iter().map(|r| r.on_balance).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Float64Array::from(
                    rows.iter().map(|r| r.trading_book).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Float64Array::from(
                    rows.iter().map(|r| r.off_balance).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Float64Array::from(
                    rows.iter().map(|r| r.default_risk).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Float64Array::from(
                    rows.iter().map(|r| r.indirect).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Float64Array::from(
                    rows.iter().map(|r| r.additional_risk).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Float64Array::from(
                    rows.iter().map(|r| r.total).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Float64Array::from(
                    rows.iter().map(|r| r.deductions).collect::<Vec<_>>(),
                )) as ArrayRef,
                strings(
                    &rows
                        .iter()
                        .map(|r| r.economic_sector.clone())
                        .collect::<Vec<_>>(),
                ),
                opt_strings(
                    &rows
                        .iter()
                        .map(|r| r.relationship_code.clone())
                        .collect::<Vec<_>>(),
                ),
                Arc::new(Float64Array::from(
                    rows.iter().map(|r| r.percent_of_tier1).collect::<Vec<_>>(),
                )) as ArrayRef,
                opt_strings(
                    &rows
                        .iter()
                        .map(|r| r.exemption_provision.clone())
                        .collect::<Vec<_>>(),
                ),
            ],
        )?;
        mem_table(schema, batch)
    }
}

/// `le_ratio(kind, id, as_of [, measure])`.
#[derive(Debug)]
pub struct LeRatioTableFunction {
    registry: Registry,
}

impl LeRatioTableFunction {
    pub fn new(registry: Registry) -> Self {
        Self { registry }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("kind", DataType::Utf8, false),
            Field::new("id", DataType::Utf8, false),
            Field::new("measure", DataType::Utf8, false),
            Field::new("exposure", DataType::Float64, false),
            Field::new("tier1", DataType::Float64, false),
            Field::new("ratio", DataType::Float64, false),
            Field::new("status", DataType::Utf8, false),
            Field::new("headroom", DataType::Float64, false),
        ]))
    }
}

impl TableFunctionImpl for LeRatioTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let usage = "le_ratio(kind, id, as_of [, measure])";
        let exprs = args.exprs();
        let kind_raw = lit_str(exprs.first().ok_or_else(|| {
            DataFusionError::Execution(format!("{usage}: missing `kind`"))
        })?)?;
        let kind = parse_kind(&kind_raw, usage)?;
        let id = lit_str(exprs.get(1).ok_or_else(|| {
            DataFusionError::Execution(format!("{usage}: missing `id`"))
        })?)?;
        let as_of = lit_i64(exprs.get(2).ok_or_else(|| {
            DataFusionError::Execution(format!("{usage}: missing `as_of`"))
        })?)?;
        let measure = match exprs.get(3) {
            Some(e) => Measure::parse(&lit_str(e)?).ok_or_else(|| {
                DataFusionError::Execution(format!("{usage}: unknown measure"))
            })?,
            None => Measure::BeforeCrm,
        };

        let reg = self.registry.read().map_err(|_| poisoned("le_ratio"))?;
        let exposure = reg.le_ledger.exposure_at(kind, &id, measure, as_of);
        let tier1 = reg.le_tier1;
        let cfg = reg.le_ledger.config().clone();
        drop(reg);
        let rule = LimitRule::new("le_ratio", gtv_largeexposure::LimitMetric::LcGroup, cfg.limit_ratio)
            .with_report_threshold(cfg.report_threshold)
            .with_warn_ratio(cfg.warn_ratio);
        let o = rule.evaluate(exposure, tier1);

        let schema = Self::schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                strings(&[kind.as_str().to_string()]),
                strings(&[id]),
                strings(&[measure.as_str().to_string()]),
                Arc::new(Float64Array::from(vec![exposure])) as ArrayRef,
                Arc::new(Float64Array::from(vec![tier1])) as ArrayRef,
                Arc::new(Float64Array::from(vec![o.ratio])) as ArrayRef,
                strings(&[o.status.as_str().to_string()]),
                Arc::new(Float64Array::from(vec![o.headroom])) as ArrayRef,
            ],
        )?;
        mem_table(schema, batch)
    }
}

/// `le_breach_scan(period_start, period_end [, top_n])`.
#[derive(Debug)]
pub struct LeBreachScanTableFunction {
    registry: Registry,
}

impl LeBreachScanTableFunction {
    pub fn new(registry: Registry) -> Self {
        Self { registry }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("rank", DataType::UInt64, false),
            Field::new("lc_group_id", DataType::Utf8, false),
            Field::new("maximum_exposure", DataType::Float64, false),
            Field::new("ratio", DataType::Float64, false),
            Field::new("status", DataType::Utf8, false),
        ]))
    }
}

impl TableFunctionImpl for LeBreachScanTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let usage = "le_breach_scan(period_start, period_end [, top_n])";
        let exprs = args.exprs();
        let start = lit_i64(exprs.first().ok_or_else(|| {
            DataFusionError::Execution(format!("{usage}: missing `period_start`"))
        })?)?;
        let end = lit_i64(exprs.get(1).ok_or_else(|| {
            DataFusionError::Execution(format!("{usage}: missing `period_end`"))
        })?)?;
        let top_n = match exprs.get(2) {
            Some(e) => lit_i64(e)?.max(0) as usize,
            None => 0,
        };

        let mut reg = self.registry.write().map_err(|_| poisoned("le_breach_scan"))?;
        let tier1 = reg.le_tier1;
        let cfg = reg.le_ledger.config().clone();
        let n = if top_n > 0 { top_n } else { cfg.default_top_n };
        let reps: Vec<String> = reg.le_ledger.groups().groups().map(|(r, _)| r.clone()).collect();
        let mut items: Vec<(String, f64)> = reps
            .iter()
            .map(|rep| {
                let v = reg
                    .le_ledger
                    .period_max(AggregateKind::LcGroup, rep, Measure::BeforeCrm, start, end);
                (rep.clone(), v)
            })
            .filter(|(_, v)| *v > 0.0)
            .collect();
        drop(reg);
        items.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        let cutoff = cfg.report_threshold * tier1;
        let selected: Vec<(String, f64)> = items
            .into_iter()
            .enumerate()
            .filter(|(i, (_, v))| *v >= cutoff || *i < n)
            .map(|(_, kv)| kv)
            .collect();

        let rule = LimitRule::new("le", gtv_largeexposure::LimitMetric::LcGroup, cfg.limit_ratio)
            .with_report_threshold(cfg.report_threshold)
            .with_warn_ratio(cfg.warn_ratio);
        let ranks: Vec<u64> = (1..=selected.len() as u64).collect();
        let keys: Vec<String> = selected.iter().map(|(k, _)| k.clone()).collect();
        let values: Vec<f64> = selected.iter().map(|(_, v)| *v).collect();
        let ratios: Vec<f64> = values.iter().map(|v| rule.evaluate(*v, tier1).ratio).collect();
        let statuses: Vec<String> = values
            .iter()
            .map(|v| rule.evaluate(*v, tier1).status.as_str().to_string())
            .collect();

        let schema = Self::schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(ranks)) as ArrayRef,
                strings(&keys),
                Arc::new(Float64Array::from(values)) as ArrayRef,
                Arc::new(Float64Array::from(ratios)) as ArrayRef,
                strings(&statuses),
            ],
        )?;
        mem_table(schema, batch)
    }
}

/// `le_concentration(dimension, as_of [, measure])`.
#[derive(Debug)]
pub struct LeConcentrationTableFunction {
    registry: Registry,
}

impl LeConcentrationTableFunction {
    pub fn new(registry: Registry) -> Self {
        Self { registry }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("exposure", DataType::Float64, false),
            Field::new("ratio_of_tier1", DataType::Float64, false),
            Field::new("share_of_book", DataType::Float64, false),
            Field::new("status", DataType::Utf8, false),
        ]))
    }
}

impl TableFunctionImpl for LeConcentrationTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let usage = "le_concentration(dimension, as_of [, measure])";
        let exprs = args.exprs();
        let dim_raw = lit_str(exprs.first().ok_or_else(|| {
            DataFusionError::Execution(format!("{usage}: missing `dimension`"))
        })?)?;
        let dim = ConcentrationDimension::parse(&dim_raw).ok_or_else(|| {
            DataFusionError::Execution(format!("{usage}: unknown dimension `{dim_raw}`"))
        })?;
        let as_of = lit_i64(exprs.get(1).ok_or_else(|| {
            DataFusionError::Execution(format!("{usage}: missing `as_of`"))
        })?)?;
        let measure = match exprs.get(2) {
            Some(e) => Measure::parse(&lit_str(e)?).ok_or_else(|| {
                DataFusionError::Execution(format!("{usage}: unknown measure"))
            })?,
            None => Measure::BeforeCrm,
        };

        let reg = self.registry.read().map_err(|_| poisoned("le_concentration"))?;
        let tier1 = reg.le_tier1;
        let cfg = reg.le_ledger.config().clone();
        let rule = LimitRule::new("conc", gtv_largeexposure::LimitMetric::Sector, cfg.limit_ratio)
            .with_report_threshold(cfg.report_threshold)
            .with_warn_ratio(cfg.warn_ratio);
        let events: Vec<gtv_largeexposure::ExposureEvent> =
            reg.le_ledger.events().values().cloned().collect();
        let recs = concentration(
            &events,
            reg.le_ledger.entities(),
            reg.le_ledger.connected(),
            as_of,
            dim,
            measure,
            tier1,
            &rule,
        );
        drop(reg);

        let schema = Self::schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                strings(&recs.iter().map(|r| r.key.clone()).collect::<Vec<_>>()),
                Arc::new(Float64Array::from(
                    recs.iter().map(|r| r.exposure).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Float64Array::from(
                    recs.iter().map(|r| r.ratio_of_tier1).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Float64Array::from(
                    recs.iter().map(|r| r.share_of_book).collect::<Vec<_>>(),
                )) as ArrayRef,
                strings(
                    &recs
                        .iter()
                        .map(|r| r.status.as_str().to_string())
                        .collect::<Vec<_>>(),
                ),
            ],
        )?;
        mem_table(schema, batch)
    }
}

/// `le_pre_trade_check(entity_id, amount, as_of)`.
#[derive(Debug)]
pub struct LePreTradeTableFunction {
    registry: Registry,
}

impl LePreTradeTableFunction {
    pub fn new(registry: Registry) -> Self {
        Self { registry }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("entity_id", DataType::Utf8, false),
            Field::new("lc_group_id", DataType::Utf8, false),
            Field::new("entity_exposure", DataType::Float64, false),
            Field::new("entity_projected", DataType::Float64, false),
            Field::new("group_exposure", DataType::Float64, false),
            Field::new("group_projected", DataType::Float64, false),
            Field::new("group_ratio", DataType::Float64, false),
            Field::new("status", DataType::Utf8, false),
            Field::new("headroom", DataType::Float64, false),
            Field::new("breached", DataType::Boolean, false),
        ]))
    }
}

impl TableFunctionImpl for LePreTradeTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let usage = "le_pre_trade_check(entity_id, amount, as_of)";
        let exprs = args.exprs();
        let entity_id = lit_str(exprs.first().ok_or_else(|| {
            DataFusionError::Execution(format!("{usage}: missing `entity_id`"))
        })?)?;
        let amount = lit_f64(exprs.get(1).ok_or_else(|| {
            DataFusionError::Execution(format!("{usage}: missing `amount`"))
        })?)?;
        let as_of = lit_i64(exprs.get(2).ok_or_else(|| {
            DataFusionError::Execution(format!("{usage}: missing `as_of`"))
        })?)?;

        let reg = self.registry.read().map_err(|_| poisoned("le_pre_trade_check"))?;
        let tier1 = reg.le_tier1;
        let cfg = reg.le_ledger.config().clone();
        let rep = reg.le_ledger.groups().group_of(&entity_id);
        let entity_exposure = reg
            .le_ledger
            .exposure_at(AggregateKind::Entity, &entity_id, Measure::BeforeCrm, as_of);
        let group_exposure = reg
            .le_ledger
            .exposure_at(AggregateKind::LcGroup, &rep, Measure::BeforeCrm, as_of);
        drop(reg);

        let group_projected = group_exposure + amount;
        let rule = LimitRule::new("le", gtv_largeexposure::LimitMetric::LcGroup, cfg.limit_ratio)
            .with_report_threshold(cfg.report_threshold)
            .with_warn_ratio(cfg.warn_ratio);
        let o = rule.evaluate(group_projected, tier1);

        let schema = Self::schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                strings(&[entity_id]),
                strings(&[rep]),
                Arc::new(Float64Array::from(vec![entity_exposure])) as ArrayRef,
                Arc::new(Float64Array::from(vec![entity_exposure + amount])) as ArrayRef,
                Arc::new(Float64Array::from(vec![group_exposure])) as ArrayRef,
                Arc::new(Float64Array::from(vec![group_projected])) as ArrayRef,
                Arc::new(Float64Array::from(vec![o.ratio])) as ArrayRef,
                strings(&[o.status.as_str().to_string()]),
                Arc::new(Float64Array::from(vec![o.headroom])) as ArrayRef,
                Arc::new(arrow::array::BooleanArray::from(vec![matches!(
                    o.status,
                    LimitStatus::Breach
                )])) as ArrayRef,
            ],
        )?;
        mem_table(schema, batch)
    }
}
