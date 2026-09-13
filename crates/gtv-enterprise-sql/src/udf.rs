//! DataFusion table functions and scalar UDF for the enterprise registry.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion::scalar::ScalarValue;
use gtv_refdata::{HierarchyKind, MasterKind};

use crate::registry::Registry;

// ---------------------------------------------------------------------------
// Literal / array helpers
// ---------------------------------------------------------------------------

fn literal_string(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Literal(sv, _) => match sv {
            ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => Ok(v.clone()),
            other => Err(DataFusionError::Execution(format!(
                "expected a string literal, got {other:?}"
            ))),
        },
        _ => Err(DataFusionError::Execution(
            "arguments must be literals".into(),
        )),
    }
}

fn literal_i64(expr: &Expr) -> Result<i64> {
    match expr {
        Expr::Literal(sv, _) => match sv.clone().cast_to(&DataType::Int64)? {
            ScalarValue::Int64(Some(v)) => Ok(v),
            other => Err(DataFusionError::Execution(format!(
                "expected an integer literal, got {other:?}"
            ))),
        },
        _ => Err(DataFusionError::Execution(
            "arguments must be literals".into(),
        )),
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

fn poisoned(what: &str) -> DataFusionError {
    DataFusionError::Execution(format!("{what}: enterprise registry poisoned"))
}

// ---------------------------------------------------------------------------
// resolve_scenario(name [, version])
// ---------------------------------------------------------------------------

/// `resolve_scenario('stress' [, 1])` — one row per resolved shock, with the
/// inheritance chain and the version that contributed each value.
#[derive(Debug)]
pub struct ResolveScenarioTableFunction {
    registry: Registry,
}

impl ResolveScenarioTableFunction {
    pub fn new(registry: Registry) -> Self {
        Self { registry }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("scenario_id", DataType::Utf8, false),
            Field::new("version", DataType::UInt32, false),
            Field::new("kind", DataType::Utf8, false),
            Field::new("chain", DataType::Utf8, false),
            Field::new("source_cutoff", DataType::Int64, false),
            Field::new("model_version", DataType::Utf8, false),
            Field::new("factor", DataType::Utf8, false),
            Field::new("legal_entity", DataType::Utf8, true),
            Field::new("portfolio", DataType::Utf8, true),
            Field::new("product", DataType::Utf8, true),
            Field::new("currency", DataType::Utf8, true),
            Field::new("value", DataType::Float64, false),
            Field::new("source_scenario", DataType::Utf8, false),
            Field::new("source_version", DataType::UInt32, false),
        ]))
    }
}

impl TableFunctionImpl for ResolveScenarioTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let exprs = args.exprs();
        let usage = "resolve_scenario(scenario [, version])";
        let name = literal_string(
            exprs
                .first()
                .ok_or_else(|| DataFusionError::Execution(format!("{usage}: missing `scenario`")))?,
        )?;
        let requested = exprs.get(1).map(literal_i64).transpose()?;

        let resolved = {
            let reg = self
                .registry
                .read()
                .map_err(|_| poisoned("resolve_scenario"))?;
            match requested {
                Some(v) if v > 0 => reg.scenarios.resolve(&name, v as u32),
                Some(_) => Err(gtv_scenario::ScenarioError::Invalid {
                    id: name.clone(),
                    version: 0,
                    reason: "version must be >= 1".into(),
                }),
                None => reg.scenarios.resolve_latest(&name),
            }
            .map_err(|e| DataFusionError::Execution(format!("{usage}: {e}")))?
        };

        let chain = resolved
            .chain
            .iter()
            .map(|(id, v)| format!("{id}:{v}"))
            .collect::<Vec<_>>()
            .join(">");
        let n = resolved.shocks.len();
        let mut factors = Vec::with_capacity(n);
        let mut legal_entity = Vec::with_capacity(n);
        let mut portfolio = Vec::with_capacity(n);
        let mut product = Vec::with_capacity(n);
        let mut currency = Vec::with_capacity(n);
        let mut values = Vec::with_capacity(n);
        let mut source_scenario = Vec::with_capacity(n);
        let mut source_version = Vec::with_capacity(n);
        for s in &resolved.shocks {
            factors.push(s.factor.clone());
            legal_entity.push(s.dimension.legal_entity.clone());
            portfolio.push(s.dimension.portfolio.clone());
            product.push(s.dimension.product.clone());
            currency.push(s.dimension.currency.clone());
            values.push(s.value);
            source_scenario.push(s.provenance.scenario_id.clone());
            source_version.push(s.provenance.version);
        }

        let schema = Self::schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                strings(&vec![resolved.id.clone(); n]),
                Arc::new(UInt32Array::from(vec![resolved.version; n])) as ArrayRef,
                strings(&vec![resolved.kind.as_str().to_string(); n]),
                strings(&vec![chain; n]),
                Arc::new(Int64Array::from(vec![resolved.source_cutoff; n])) as ArrayRef,
                strings(&vec![resolved.model_version.clone(); n]),
                strings(&factors),
                opt_strings(&legal_entity),
                opt_strings(&portfolio),
                opt_strings(&product),
                opt_strings(&currency),
                Arc::new(Float64Array::from(values)) as ArrayRef,
                strings(&source_scenario),
                Arc::new(UInt32Array::from(source_version)) as ArrayRef,
            ],
        )?;
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}

// ---------------------------------------------------------------------------
// hierarchy_ancestors / hierarchy_descendants
// ---------------------------------------------------------------------------

/// Which direction [`HierarchyTableFunction`] walks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HierarchyDirection {
    Ancestors,
    Descendants,
}

/// `hierarchy_ancestors(kind, node, as_of)` /
/// `hierarchy_descendants(kind, node, as_of)`.
#[derive(Debug)]
pub struct HierarchyTableFunction {
    registry: Registry,
    direction: HierarchyDirection,
}

impl HierarchyTableFunction {
    pub fn new(registry: Registry, direction: HierarchyDirection) -> Self {
        Self {
            registry,
            direction,
        }
    }

    fn name(&self) -> &'static str {
        match self.direction {
            HierarchyDirection::Ancestors => "hierarchy_ancestors",
            HierarchyDirection::Descendants => "hierarchy_descendants",
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("kind", DataType::Utf8, false),
            Field::new("node", DataType::Utf8, false),
            Field::new("related", DataType::Utf8, false),
        ]))
    }
}

impl TableFunctionImpl for HierarchyTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let name = self.name();
        let exprs = args.exprs();
        let usage = format!("{name}(kind, node, as_of)");
        let kind_raw = literal_string(exprs.first().ok_or_else(|| {
            DataFusionError::Execution(format!("{usage}: missing `kind`"))
        })?)?;
        let node = literal_string(exprs.get(1).ok_or_else(|| {
            DataFusionError::Execution(format!("{usage}: missing `node`"))
        })?)?;
        let as_of = literal_i64(exprs.get(2).ok_or_else(|| {
            DataFusionError::Execution(format!("{usage}: missing `as_of`"))
        })?)?;
        let kind = HierarchyKind::parse(&kind_raw).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "{usage}: unknown hierarchy kind `{kind_raw}` \
                 (legal_entity | organisation | product)"
            ))
        })?;

        let related = {
            let reg = self.registry.read().map_err(|_| poisoned(name))?;
            match self.direction {
                HierarchyDirection::Ancestors => reg.hierarchy.ancestors(&node, kind, as_of),
                HierarchyDirection::Descendants => reg.hierarchy.descendants(&node, kind, as_of),
            }
        };

        let n = related.len();
        let schema = Self::schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                strings(&vec![kind.as_str().to_string(); n]),
                strings(&vec![node; n]),
                strings(&related),
            ],
        )?;
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}

// ---------------------------------------------------------------------------
// master_get(kind, id, as_of)
// ---------------------------------------------------------------------------

/// `master_get(kind, id, as_of)` — one row per attribute of the active version.
#[derive(Debug)]
pub struct MasterGetTableFunction {
    registry: Registry,
}

impl MasterGetTableFunction {
    pub fn new(registry: Registry) -> Self {
        Self { registry }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("kind", DataType::Utf8, false),
            Field::new("id", DataType::Utf8, false),
            Field::new("valid_from", DataType::Int64, false),
            Field::new("valid_to", DataType::Int64, false),
            Field::new("attr_key", DataType::Utf8, false),
            Field::new("attr_value", DataType::Utf8, false),
        ]))
    }
}

impl TableFunctionImpl for MasterGetTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let usage = "master_get(kind, id, as_of)";
        let exprs = args.exprs();
        let kind_raw = literal_string(exprs.first().ok_or_else(|| {
            DataFusionError::Execution(format!("{usage}: missing `kind`"))
        })?)?;
        let id = literal_string(exprs.get(1).ok_or_else(|| {
            DataFusionError::Execution(format!("{usage}: missing `id`"))
        })?)?;
        let as_of = literal_i64(exprs.get(2).ok_or_else(|| {
            DataFusionError::Execution(format!("{usage}: missing `as_of`"))
        })?)?;
        let kind = MasterKind::parse(&kind_raw).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "{usage}: unknown master kind `{kind_raw}` \
                 (account | customer | instrument | counterparty)"
            ))
        })?;

        let schema = Self::schema();
        let record = {
            let reg = self.registry.read().map_err(|_| poisoned("master_get"))?;
            reg.master.get(kind, &id, as_of).cloned()
        };
        let Some(record) = record else {
            let empty = RecordBatch::new_empty(schema.clone());
            return Ok(Arc::new(MemTable::try_new(schema, vec![vec![empty]])?));
        };

        let keys: Vec<String> = record.attributes.keys().cloned().collect();
        let values: Vec<String> = record.attributes.values().cloned().collect();
        let n = keys.len();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                strings(&vec![kind.as_str().to_string(); n]),
                strings(&vec![record.id.clone(); n]),
                Arc::new(Int64Array::from(vec![record.effective.from; n])) as ArrayRef,
                Arc::new(Int64Array::from(vec![record.effective.to; n])) as ArrayRef,
                strings(&keys),
                strings(&values),
            ],
        )?;
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}

// ---------------------------------------------------------------------------
// refdata_get(domain, key, as_of) scalar UDF
// ---------------------------------------------------------------------------

/// `refdata_get(domain, key, as_of)` — effective-dated value or NULL.
#[derive(Debug)]
pub struct RefdataGetUdf {
    registry: Registry,
    signature: Signature,
}

// `ScalarUDFImpl` requires `Hash + Eq`; the shared registry is deliberately not
// part of the function identity (name + signature are enough).
impl PartialEq for RefdataGetUdf {
    fn eq(&self, other: &Self) -> bool {
        self.signature == other.signature
    }
}

impl Eq for RefdataGetUdf {}

impl std::hash::Hash for RefdataGetUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.signature.hash(state);
    }
}

impl RefdataGetUdf {
    pub fn new(registry: Registry) -> Self {
        Self {
            registry,
            signature: Signature::one_of(
                vec![TypeSignature::Any(3)],
                Volatility::Immutable,
            ),
        }
    }
}

fn string_values(arr: &ArrayRef) -> Result<Vec<Option<String>>> {
    let casted = arrow::compute::cast(arr, &DataType::Utf8)
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    let s = casted
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Execution("refdata_get: expected string arguments".into()))?;
    Ok((0..s.len())
        .map(|i| (!s.is_null(i)).then(|| s.value(i).to_string()))
        .collect())
}

fn i64_values(arr: &ArrayRef) -> Result<Vec<Option<i64>>> {
    let casted = arrow::compute::cast(arr, &DataType::Int64)
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    let v = casted
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DataFusionError::Execution("refdata_get: `as_of` must be an integer".into()))?;
    Ok((0..v.len())
        .map(|i| (!v.is_null(i)).then(|| v.value(i)))
        .collect())
}

fn pick<T>(values: &[T], i: usize) -> Option<&T> {
    if values.len() == 1 {
        values.first()
    } else {
        values.get(i)
    }
}

impl ScalarUDFImpl for RefdataGetUdf {
    fn name(&self) -> &str {
        "refdata_get"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        if arrays.len() != 3 {
            return Err(DataFusionError::Execution(
                "refdata_get(domain, key, as_of): expected 3 arguments".into(),
            ));
        }
        let domains = string_values(&arrays[0])?;
        let keys = string_values(&arrays[1])?;
        let times = i64_values(&arrays[2])?;
        let n = domains.len().max(keys.len()).max(times.len());
        if n == 0 {
            return Ok(ColumnarValue::Array(opt_strings(&[])));
        }
        let reg = self
            .registry
            .read()
            .map_err(|_| poisoned("refdata_get"))?;
        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            let domain = pick(&domains, i).and_then(|o| o.as_deref());
            let key = pick(&keys, i).and_then(|o| o.as_deref());
            let as_of = pick(&times, i).copied().flatten();
            out.push(match (domain, key, as_of) {
                (Some(d), Some(k), Some(t)) => {
                    reg.reference.get(d, k, t).map(str::to_string)
                }
                _ => None,
            });
        }
        Ok(ColumnarValue::Array(opt_strings(&out)))
    }
}
