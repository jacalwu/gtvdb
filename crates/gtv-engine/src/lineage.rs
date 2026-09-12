//! Execution lineage: UDF determinism, replay, and the SQL surface (B2-3).
//!
//! [`GtvContext::execute_with_lineage`](crate::GtvContext::execute_with_lineage)
//! records the source snapshots, model/index versions and a blake3 checksum of
//! the output. This module adds the other half of the contract:
//!
//! * [`extract_udfs`] walks a DataFusion logical plan and reports every
//!   function it references, flagging the nondeterministic ones (`random()`,
//!   `now()`, …) via function volatility.
//! * [`GtvContext::replay`] re-registers the *pinned* catalog snapshots from a
//!   record and re-runs its SQL, refusing to replay records that used a
//!   nondeterministic function unless explicitly forced, and verifying that the
//!   output checksum is byte-identical.
//! * [`GtvContext::register_lineage`] exposes recorded executions as the
//!   `gtv_lineage` table so lineage itself is queryable from SQL.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::{Expr, LogicalPlan, Volatility, WindowFunctionDefinition};
use gtv_catalog::{ExecutionId, ExecutionRecord, FileFormat, FsCatalog, TableRef, UdfRef};

use crate::context::GtvContext;

/// Engine version stamped onto every UDF reference.
pub const ENGINE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// blake3 (hex) of the Arrow IPC encoding of `batches`. Public so tests and
/// callers can verify a replayed result independently of [`GtvContext::replay`].
pub fn output_checksum(batches: &[RecordBatch]) -> String {
    crate::context::batches_checksum(batches)
}

/// Name of the SQL table that exposes recorded executions.
pub const LINEAGE_TABLE: &str = "gtv_lineage";

/// A function is replay-safe only when it is [`Volatility::Immutable`].
/// `Stable` (e.g. `now()`) and `Volatile` (e.g. `random()`) may change between
/// executions, so replay must refuse them by default.
fn is_nondeterministic(volatility: Volatility) -> bool {
    !matches!(volatility, Volatility::Immutable)
}

fn udf_from_expr(expr: &Expr) -> Option<UdfRef> {
    match expr {
        Expr::ScalarFunction(f) => {
            let name = f.func.name();
            Some(UdfRef::new(
                name,
                ENGINE_VERSION,
                is_nondeterministic(f.func.signature().volatility),
            ))
        }
        Expr::AggregateFunction(f) => {
            let name = f.func.name();
            Some(UdfRef::new(
                name,
                ENGINE_VERSION,
                is_nondeterministic(f.func.signature().volatility),
            ))
        }
        Expr::WindowFunction(f) => {
            let (name, volatility) = match &f.fun {
                WindowFunctionDefinition::AggregateUDF(u) => {
                    (u.name(), u.signature().volatility)
                }
                WindowFunctionDefinition::WindowUDF(u) => {
                    (u.name(), u.signature().volatility)
                }
            };
            Some(UdfRef::new(
                name,
                ENGINE_VERSION,
                is_nondeterministic(volatility),
            ))
        }
        _ => None,
    }
}

/// Every scalar / aggregate / window function referenced by `plan`, deduplicated
/// by name and sorted. Nondeterministic functions carry `nondeterministic =
/// true`.
pub fn extract_udfs(plan: &LogicalPlan) -> Vec<UdfRef> {
    let mut found: BTreeMap<String, UdfRef> = BTreeMap::new();
    let _ = plan.apply(&mut |node: &LogicalPlan| {
        for expr in node.expressions() {
            let _ = expr.apply(&mut |e: &Expr| {
                if let Some(r) = udf_from_expr(e) {
                    // Prefer a nondeterministic hit if the same name appears
                    // with both classifications.
                    found
                        .entry(r.name.clone())
                        .and_modify(|existing| {
                            existing.nondeterministic |= r.nondeterministic;
                        })
                        .or_insert(r);
                }
                Ok(TreeNodeRecursion::Continue)
            });
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found.into_values().collect()
}

/// Why a replay could not reproduce the recorded result.
#[derive(Debug)]
pub enum ReplayError {
    /// No lineage record with that id.
    NotFound(ExecutionId),
    /// The recorded query used a nondeterministic function.
    Nondeterministic(Vec<String>),
    /// The pinned snapshots could not be loaded.
    NoSourceData(String),
    /// Re-running the query produced a different output checksum.
    ChecksumMismatch { expected: String, actual: String },
    /// A DataFusion or catalog error during replay.
    Backend(String),
}

impl std::fmt::Display for ReplayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplayError::NotFound(id) => write!(f, "no lineage record for execution {id}"),
            ReplayError::Nondeterministic(names) => write!(
                f,
                "replay refused: nondeterministic function(s) used: {} \
                 (pass allow_nondeterministic to force)",
                names.join(", ")
            ),
            ReplayError::NoSourceData(msg) => write!(f, "pinned source unavailable: {msg}"),
            ReplayError::ChecksumMismatch { expected, actual } => write!(
                f,
                "replay checksum mismatch: expected {expected}, got {actual}"
            ),
            ReplayError::Backend(msg) => write!(f, "replay backend error: {msg}"),
        }
    }
}

impl std::error::Error for ReplayError {}

fn backend(e: impl std::fmt::Display) -> ReplayError {
    ReplayError::Backend(e.to_string())
}

/// Load every data file of a pinned snapshot (managed Parquet or external
/// CSV/Parquet reference) as record batches.
fn load_snapshot(catalog: &FsCatalog, source: &TableRef) -> Result<Vec<RecordBatch>, ReplayError> {
    let files = catalog
        .files(source.table_id, source.snapshot_id)
        .map_err(backend)?;
    let mut batches = Vec::new();
    for file in files {
        let read = match file.format {
            FileFormat::Parquet => gtv_storage::read_batches(&file.path),
            FileFormat::Csv => gtv_storage::read_csv(&file.path),
        };
        let bs = read.map_err(|e| ReplayError::NoSourceData(format!("{}: {e}", file.path)))?;
        batches.extend(bs);
    }
    Ok(batches)
}

impl GtvContext {
    /// Replay the execution `id` from `catalog`.
    ///
    /// Re-registers the pinned snapshots recorded in the lineage entry, re-runs
    /// the original SQL and verifies the output checksum is byte-identical.
    /// Nondeterministic UDFs are refused unless `allow_nondeterministic` is set
    /// (in which case the checksum comparison usually still fails, which is the
    /// point).
    pub async fn replay(
        &self,
        catalog: &FsCatalog,
        id: ExecutionId,
        allow_nondeterministic: bool,
    ) -> Result<Vec<RecordBatch>, ReplayError> {
        let record = catalog
            .lineage(id)
            .map_err(backend)?
            .ok_or(ReplayError::NotFound(id))?;

        if !allow_nondeterministic {
            let bad: Vec<String> = record
                .udf_versions
                .iter()
                .filter(|u| u.nondeterministic)
                .map(|u| u.name.clone())
                .collect();
            if !bad.is_empty() {
                return Err(ReplayError::Nondeterministic(bad));
            }
        }

        // Pin every source table to the snapshot it was read from, even if the
        // table has since advanced to a newer snapshot.
        for source in &record.source_tables {
            let batches = load_snapshot(catalog, source)?;
            if let Some(first) = batches.first() {
                self.register_batches(&source.table_name, first.schema(), batches)
                    .map_err(backend)?;
                self.set_table_source(&source.table_name, source.clone());
            }
        }

        let batches = self.sql(&record.query_text).await.map_err(backend)?;
        let actual = output_checksum(&batches);
        if actual != record.output_checksum {
            return Err(ReplayError::ChecksumMismatch {
                expected: record.output_checksum,
                actual,
            });
        }
        Ok(batches)
    }

    /// Register `records` as the queryable [`LINEAGE_TABLE`] (`gtv_lineage`).
    pub fn register_lineage(&self, records: &[ExecutionRecord]) -> DfResult<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("execution_id", DataType::Utf8, false),
            Field::new("query_text", DataType::Utf8, false),
            Field::new("query_hash", DataType::Utf8, false),
            Field::new("engine_version", DataType::Utf8, false),
            Field::new("output_checksum", DataType::Utf8, false),
            Field::new("output_rows", DataType::UInt64, false),
            Field::new("source_tables", DataType::Utf8, false),
            Field::new("model_versions", DataType::Utf8, false),
            Field::new("index_snapshots", DataType::Utf8, false),
            Field::new("udf_versions", DataType::Utf8, false),
            Field::new("nondeterministic", DataType::Utf8, false),
            Field::new("scenario_version", DataType::Utf8, true),
            Field::new("business_cutoff", DataType::Int64, true),
            Field::new("started_at", DataType::Int64, false),
            Field::new("finished_at", DataType::Int64, false),
        ]));

        let json = |v: serde_json::Value| serde_json::to_string(&v).unwrap_or_default();
        let execution_id: Vec<String> = records.iter().map(|r| r.execution_id.to_string()).collect();
        let query_text: Vec<&str> = records.iter().map(|r| r.query_text.as_str()).collect();
        let query_hash: Vec<&str> = records.iter().map(|r| r.query_hash.as_str()).collect();
        let engine: Vec<&str> = records.iter().map(|r| r.engine_version.as_str()).collect();
        let checksum: Vec<&str> = records.iter().map(|r| r.output_checksum.as_str()).collect();
        let rows: Vec<u64> = records.iter().map(|r| r.output_rows).collect();
        let source_tables: Vec<String> = records
            .iter()
            .map(|r| json(serde_json::to_value(&r.source_tables).unwrap_or_default()))
            .collect();
        let models: Vec<String> = records
            .iter()
            .map(|r| json(serde_json::to_value(&r.model_versions).unwrap_or_default()))
            .collect();
        let indexes: Vec<String> = records
            .iter()
            .map(|r| json(serde_json::to_value(&r.index_snapshots).unwrap_or_default()))
            .collect();
        let udfs: Vec<String> = records
            .iter()
            .map(|r| json(serde_json::to_value(&r.udf_versions).unwrap_or_default()))
            .collect();
        let nondet: Vec<String> = records
            .iter()
            .map(|r| {
                r.udf_versions
                    .iter()
                    .filter(|u| u.nondeterministic)
                    .map(|u| u.name.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .collect();
        let scenario: Vec<Option<&str>> = records
            .iter()
            .map(|r| r.scenario_version.as_deref())
            .collect();
        let cutoff: Vec<Option<i64>> = records.iter().map(|r| r.business_cutoff).collect();
        let started: Vec<i64> = records.iter().map(|r| r.started_at).collect();
        let finished: Vec<i64> = records.iter().map(|r| r.finished_at).collect();

        let cols: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from_iter_values(execution_id.iter().map(|s| s.as_str()))),
            Arc::new(StringArray::from(query_text)),
            Arc::new(StringArray::from(query_hash)),
            Arc::new(StringArray::from(engine)),
            Arc::new(StringArray::from(checksum)),
            Arc::new(UInt64Array::from(rows)),
            Arc::new(StringArray::from(source_tables)),
            Arc::new(StringArray::from(models)),
            Arc::new(StringArray::from(indexes)),
            Arc::new(StringArray::from(udfs)),
            Arc::new(StringArray::from(nondet)),
            Arc::new(StringArray::from(scenario)),
            Arc::new(Int64Array::from(cutoff)),
            Arc::new(Int64Array::from(started)),
            Arc::new(Int64Array::from(finished)),
        ];

        let batch = RecordBatch::try_new(schema.clone(), cols).map_err(|e| {
            DataFusionError::Execution(format!("build lineage batch: {e}"))
        })?;
        self.register_batches(LINEAGE_TABLE, schema, vec![batch])
    }
}
