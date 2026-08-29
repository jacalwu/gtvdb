//! HFT table functions (M1): `point_in_time` and `wash_trade`.
//!
//! Thin wrappers over `gtv_core::temporal::point_in_time_range` (O(log N),
//! zero-copy slicing) and `gtv_pattern::find` (temporal ring detection).

use std::sync::{Arc, RwLock};

use arrow::array::{as_primitive_array, ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Float64Type, Int64Type, Schema, SchemaRef};
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result};
use gtv_core::temporal::point_in_time_range;
use gtv_core::TemporalCSR;
use gtv_pattern::{find, Pattern};

use crate::expr_util::{expr_to_i64, expr_to_string};
use crate::hft_exec::HftRegistry;

/// Downcast a column (by name) across a registered table's batches to `f64`.
fn extract_f64_col(batches: &[RecordBatch], col: &str) -> Option<Vec<f64>> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b.column_by_name(col)?;
        out.extend_from_slice(as_primitive_array::<Float64Type>(arr.as_ref()).values());
    }
    Some(out)
}

/// Downcast a column (by name) to `f64`, accepting any numeric type.
fn extract_num_col(batches: &[RecordBatch], col: &str) -> Option<Vec<f64>> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b.column_by_name(col)?;
        match arr.data_type() {
            DataType::Float64 => out.extend_from_slice(as_primitive_array::<Float64Type>(arr.as_ref()).values()),
            DataType::Int64 => out.extend(as_primitive_array::<Int64Type>(arr.as_ref()).values().iter().map(|&v| v as f64)),
            DataType::UInt64 => out.extend(as_primitive_array::<arrow::datatypes::UInt64Type>(arr.as_ref()).values().iter().map(|&v| v as f64)),
            DataType::Int32 => out.extend(as_primitive_array::<arrow::datatypes::Int32Type>(arr.as_ref()).values().iter().map(|&v| v as f64)),
            _ => return None,
        }
    }
    Some(out)
}

/// `point_in_time(T)` over temporally-sorted rows: returns the active
/// `[valid_from <= T < valid_to)` index range via two `partition_point` binary
/// searches — O(log N), no mask materialization, zero writes.
#[derive(Debug)]
pub struct PointInTimeTableFunction {
    valid_from: Vec<i64>,
    valid_to: Vec<i64>,
}

impl PointInTimeTableFunction {
    pub fn new(valid_from: Vec<i64>, valid_to: Vec<i64>) -> Self {
        debug_assert_eq!(valid_from.len(), valid_to.len());
        Self {
            valid_from,
            valid_to,
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("idx", DataType::Int64, false),
            Field::new("valid_from", DataType::Int64, false),
            Field::new("valid_to", DataType::Int64, false),
        ]))
    }
}

impl TableFunctionImpl for PointInTimeTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let t = expr_to_i64(
            args.exprs()
                .first()
                .ok_or_else(|| DataFusionError::Execution("point_in_time(T): missing T".into()))?,
        )?;

        let range = point_in_time_range(&self.valid_from, &self.valid_to, t);
        let idx: Vec<i64> = (range.start..range.end).map(|i| i as i64).collect();
        let vf = self.valid_from[range.clone()].to_vec();
        let vt = self.valid_to[range].to_vec();

        let batch = RecordBatch::try_new(
            Self::schema(),
            vec![
                Arc::new(Int64Array::from(idx)) as ArrayRef,
                Arc::new(Int64Array::from(vf)) as ArrayRef,
                Arc::new(Int64Array::from(vt)) as ArrayRef,
            ],
        )?;
        Ok(Arc::new(MemTable::try_new(Self::schema(), vec![vec![batch]])?))
    }
}

/// `wash_trade(T)` over a temporal transfer graph: detects every `A->B->C->A`
/// ring active at `T` with strictly increasing event times, returning
/// `(a, b, c)` node triples.
#[derive(Debug)]
pub struct WashTradeTableFunction {
    csr: TemporalCSR,
}

impl WashTradeTableFunction {
    pub fn new(csr: &TemporalCSR) -> Self {
        Self { csr: csr.clone() }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt64, false),
            Field::new("b", DataType::UInt64, false),
            Field::new("c", DataType::UInt64, false),
        ]))
    }
}

impl TableFunctionImpl for WashTradeTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let t = expr_to_i64(
            args.exprs()
                .first()
                .ok_or_else(|| DataFusionError::Execution("wash_trade(T): missing T".into()))?,
        )?;

        let matches = find(&self.csr, &Pattern::ring(3), t, 10_000)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        let mut a = Vec::new();
        let mut b = Vec::new();
        let mut c = Vec::new();
        for m in &matches {
            a.push(m.nodes[0]);
            b.push(m.nodes[1]);
            c.push(m.nodes[2]);
        }

        let batch = RecordBatch::try_new(
            Self::schema(),
            vec![
                Arc::new(UInt64Array::from(a)) as ArrayRef,
                Arc::new(UInt64Array::from(b)) as ArrayRef,
                Arc::new(UInt64Array::from(c)) as ArrayRef,
            ],
        )?;
        Ok(Arc::new(MemTable::try_new(Self::schema(), vec![vec![batch]])?))
    }
}

// ---------------------------------------------------------------------------
// TC7 — tick-to-trade (compiled signal + order generation over a table)
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct TickToTradeTableFunction {
    registry: Arc<RwLock<HftRegistry>>,
}

impl TickToTradeTableFunction {
    pub fn new(registry: Arc<RwLock<HftRegistry>>) -> Self {
        Self { registry }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("side", DataType::Int32, false),
            Field::new("price", DataType::Float64, false),
            Field::new("qty", DataType::UInt64, false),
        ]))
    }
}

impl TableFunctionImpl for TickToTradeTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let exprs = args.exprs();
        let name = expr_to_string(
            exprs.first().ok_or_else(|| {
                DataFusionError::Execution("tick_to_trade(name, window): missing name".into())
            })?,
        )?;
        let window = expr_to_i64(
            exprs.get(1).ok_or_else(|| {
                DataFusionError::Execution("tick_to_trade(name, window): missing window".into())
            })?,
        )?
        .max(1) as usize;

        let reg = self.registry.read().map_err(|_| {
            DataFusionError::Execution("hft registry poisoned".into())
        })?;
        let batches = reg.tables.get(&name).ok_or_else(|| {
            DataFusionError::Execution(format!("unknown table `{name}`"))
        })?;
        let price = extract_f64_col(batches, "price").ok_or_else(|| {
            DataFusionError::Execution(format!("table `{name}` has no Float64 `price` column"))
        })?;

        let (side, price, qty) = gtv_array::hft_ops::tick_to_trade(&price, window);
        let batch = RecordBatch::try_new(
            Self::schema(),
            vec![
                Arc::new(Int32Array::from(side.iter().map(|&s| s as i32).collect::<Vec<_>>())) as ArrayRef,
                Arc::new(Float64Array::from(price)) as ArrayRef,
                Arc::new(UInt64Array::from(qty)) as ArrayRef,
            ],
        )?;
        Ok(Arc::new(MemTable::try_new(Self::schema(), vec![vec![batch]])?))
    }
}

// ---------------------------------------------------------------------------
// TC10 — local matching engine over an order table
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct MatchOrdersTableFunction {
    registry: Arc<RwLock<HftRegistry>>,
}

impl MatchOrdersTableFunction {
    pub fn new(registry: Arc<RwLock<HftRegistry>>) -> Self {
        Self { registry }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("side", DataType::Int32, false),
            Field::new("price", DataType::Float64, false),
            Field::new("qty", DataType::UInt64, false),
        ]))
    }
}

impl TableFunctionImpl for MatchOrdersTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let name = expr_to_string(
            args.exprs()
                .first()
                .ok_or_else(|| DataFusionError::Execution("match_orders(name): missing name".into()))?,
        )?;
        let reg = self.registry.read().map_err(|_| {
            DataFusionError::Execution("hft registry poisoned".into())
        })?;
        let batches = reg.tables.get(&name).ok_or_else(|| {
            DataFusionError::Execution(format!("unknown table `{name}`"))
        })?;
        let side = extract_num_col(batches, "side").ok_or_else(|| {
            DataFusionError::Execution(format!("table `{name}` has no `side` column"))
        })?;
        let is_mkt = extract_num_col(batches, "is_mkt").ok_or_else(|| {
            DataFusionError::Execution(format!("table `{name}` has no `is_mkt` column"))
        })?;
        let price = extract_f64_col(batches, "price").ok_or_else(|| {
            DataFusionError::Execution(format!("table `{name}` has no Float64 `price` column"))
        })?;
        let qty = extract_f64_col(batches, "qty").ok_or_else(|| {
            DataFusionError::Execution(format!("table `{name}` has no Float64 `qty` column"))
        })?;

        let side_u8: Vec<u8> = side.iter().map(|&s| s as u8).collect();
        let is_mkt_u8: Vec<u8> = is_mkt.iter().map(|&s| s as u8).collect();
        let qty_u64: Vec<u64> = qty.iter().map(|&q| q as u64).collect();
        let (fs, fp, fq) = gtv_array::hft_ops::match_orders(&side_u8, &is_mkt_u8, &price, &qty_u64);

        let batch = RecordBatch::try_new(
            Self::schema(),
            vec![
                Arc::new(Int32Array::from(fs.iter().map(|&s| s as i32).collect::<Vec<_>>())) as ArrayRef,
                Arc::new(Float64Array::from(fp)) as ArrayRef,
                Arc::new(UInt64Array::from(fq)) as ArrayRef,
            ],
        )?;
        Ok(Arc::new(MemTable::try_new(Self::schema(), vec![vec![batch]])?))
    }
}

// ---------------------------------------------------------------------------
// TC9 — streaming covariance matrix over a returns table (ret_0..ret_{m-1})
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct CovarianceTableFunction {
    registry: Arc<RwLock<HftRegistry>>,
}

impl CovarianceTableFunction {
    pub fn new(registry: Arc<RwLock<HftRegistry>>) -> Self {
        Self { registry }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int32, false),
            Field::new("j", DataType::Int32, false),
            Field::new("cov", DataType::Float64, false),
        ]))
    }
}

impl TableFunctionImpl for CovarianceTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let exprs = args.exprs();
        let name = expr_to_string(
            exprs.first().ok_or_else(|| {
                DataFusionError::Execution("covariance_matrix(name, m): missing name".into())
            })?,
        )?;
        let m = expr_to_i64(
            exprs.get(1).ok_or_else(|| {
                DataFusionError::Execution("covariance_matrix(name, m): missing m".into())
            })?,
        )?
        .max(1) as usize;

        let reg = self.registry.read().map_err(|_| {
            DataFusionError::Execution("hft registry poisoned".into())
        })?;
        let batches = reg.tables.get(&name).ok_or_else(|| {
            DataFusionError::Execution(format!("unknown table `{name}`"))
        })?;

        // Gather m return columns into a row-major [n][m] flat matrix.
        let mut cols = Vec::with_capacity(m);
        for c in 0..m {
            let col = format!("ret_{c}");
            cols.push(extract_f64_col(batches, &col).ok_or_else(|| {
                DataFusionError::Execution(format!("table `{name}` has no Float64 `{col}` column"))
            })?);
        }
        let n = cols.first().map(|c| c.len()).unwrap_or(0);
        let mut flat = Vec::with_capacity(n * m);
        for r in 0..n {
            for c in 0..m {
                flat.push(cols[c][r]);
            }
        }
        let cov = gtv_array::hft_ops::covariance_matrix(&flat, m);

        let mut i = Vec::with_capacity(m * m);
        let mut j = Vec::with_capacity(m * m);
        let mut vals = Vec::with_capacity(m * m);
        for a in 0..m {
            for b in 0..m {
                i.push(a as i32);
                j.push(b as i32);
                vals.push(cov[a * m + b]);
            }
        }
        let batch = RecordBatch::try_new(
            Self::schema(),
            vec![
                Arc::new(Int32Array::from(i)) as ArrayRef,
                Arc::new(Int32Array::from(j)) as ArrayRef,
                Arc::new(Float64Array::from(vals)) as ArrayRef,
            ],
        )?;
        Ok(Arc::new(MemTable::try_new(Self::schema(), vec![vec![batch]])?))
    }
}
