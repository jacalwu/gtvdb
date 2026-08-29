//! M2 — precompiled KernelPlan for HFT mode.
//!
//! A `KernelPlan` is a parsed HFT query bound directly to the compiled kernels
//! (`gtv_core`/`gtv_array`/`gtv_pattern`) and to the registered resources. It is
//! compiled **once** (the SQL is parsed, operator names bound, constant args
//! baked in) and then executed repeatedly with **zero DataFusion planning and
//! zero allocation** — the hot path is just function-pointer dispatch onto raw
//! slices.
//!
//! Supported subset (hft mode):
//!   * `pit(T)` / `point_in_time(T)`           — O(log N) point-in-time slice
//!   * `wash(T)` / `wash_trade(T)`             — ring(3) wash-trade detection
//!   * `aj(t0, t1, …)` / `asof_join(t0, …)`    — multi-column as-of join
//!   * `ofi(b, a, bs, as, w) OVER (ORDER BY t) FROM <table>` — fused OFI + msum
//!   * bare table name / `SELECT * FROM <table>`

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{as_primitive_array, ArrayRef, Float64Array, Int64Array, RecordBatch, UInt64Array};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Float64Type, Int64Type, Schema, SchemaRef};
use datafusion::error::{DataFusionError, Result};
use gtv_core::temporal::point_in_time_range;
use gtv_core::TemporalCSR;
use gtv_pattern::{find, Pattern};

// ---------------------------------------------------------------------------
// Registered resources (shared with the DataFusion UDTFs)
// ---------------------------------------------------------------------------

/// Named resources the KernelPlan binds to.
#[derive(Default)]
pub struct HftRegistry {
    pub pit: HashMap<String, Arc<PitResource>>,
    pub asof: HashMap<String, Arc<AsofResource>>,
    pub wash: HashMap<String, Arc<TemporalCSR>>,
    /// Registered tables, for column-extraction operators (OFI).
    pub tables: HashMap<String, Arc<Vec<RecordBatch>>>,
}

pub struct PitResource {
    pub valid_from: Arc<Vec<i64>>,
    pub valid_to: Arc<Vec<i64>>,
}

pub struct AsofResource {
    pub times: Arc<Vec<i64>>,
    pub price: Arc<Vec<f64>>,
    pub spread: Arc<Vec<f64>>,
    pub tolerance: i64,
}

// ---------------------------------------------------------------------------
// KernelPlan
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub enum KernelPlan {
    Table(Arc<Vec<RecordBatch>>),
    Pit {
        res: Arc<PitResource>,
        t: i64,
    },
    Wash {
        csr: Arc<TemporalCSR>,
        t: i64,
    },
    Asof {
        res: Arc<AsofResource>,
        left: Arc<Vec<i64>>,
    },
    Ofi {
        t: Arc<Vec<i64>>,
        bid: Arc<Vec<f64>>,
        ask: Arc<Vec<f64>>,
        bid_sz: Arc<Vec<f64>>,
        ask_sz: Arc<Vec<f64>>,
        window: usize,
    },
}

impl std::fmt::Debug for KernelPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

impl KernelPlan {
    pub fn name(&self) -> &'static str {
        match self {
            KernelPlan::Table(_) => "KernelPlan::Table",
            KernelPlan::Pit { .. } => "KernelPlan::Pit",
            KernelPlan::Wash { .. } => "KernelPlan::Wash",
            KernelPlan::Asof { .. } => "KernelPlan::Asof",
            KernelPlan::Ofi { .. } => "KernelPlan::Ofi",
        }
    }

    pub fn execute(&self) -> Result<RecordBatch> {
        match self {
            KernelPlan::Table(batches) => {
                let first = batches
                    .first()
                    .ok_or_else(|| DataFusionError::Execution("empty table".into()))?;
                if batches.len() == 1 {
                    Ok(first.clone())
                } else {
                    concat_batches(&first.schema(), batches.as_ref())
                        .map_err(|e| DataFusionError::Execution(e.to_string()))
                }
            }
            KernelPlan::Pit { res, t } => {
                let range = point_in_time_range(&res.valid_from, &res.valid_to, *t);
                let idx: Vec<i64> = (range.start..range.end).map(|i| i as i64).collect();
                let vf = res.valid_from[range.clone()].to_vec();
                let vt = res.valid_to[range].to_vec();
                Ok(RecordBatch::try_new(
                    Self::pit_schema(),
                    vec![
                        Arc::new(Int64Array::from(idx)) as ArrayRef,
                        Arc::new(Int64Array::from(vf)) as ArrayRef,
                        Arc::new(Int64Array::from(vt)) as ArrayRef,
                    ],
                )?)
            }
            KernelPlan::Wash { csr, t } => {
                let matches = find(csr, &Pattern::ring(3), *t, 10_000)
                    .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                let mut a = Vec::new();
                let mut b = Vec::new();
                let mut c = Vec::new();
                for m in &matches {
                    a.push(m.nodes[0]);
                    b.push(m.nodes[1]);
                    c.push(m.nodes[2]);
                }
                Ok(RecordBatch::try_new(
                    Self::wash_schema(),
                    vec![
                        Arc::new(UInt64Array::from(a)) as ArrayRef,
                        Arc::new(UInt64Array::from(b)) as ArrayRef,
                        Arc::new(UInt64Array::from(c)) as ArrayRef,
                    ],
                )?)
            }
            KernelPlan::Asof { res, left } => {
                let n = left.len();
                let mut t = Vec::with_capacity(n);
                let mut price = Vec::with_capacity(n);
                let mut spread = Vec::with_capacity(n);
                for &lt in left.iter() {
                    t.push(lt);
                    let j = res.times.partition_point(|&rt| rt <= lt);
                    if j == 0 || lt - res.times[j - 1] > res.tolerance {
                        price.push(None);
                        spread.push(None);
                    } else {
                        price.push(Some(res.price[j - 1]));
                        spread.push(Some(res.spread[j - 1]));
                    }
                }
                Ok(RecordBatch::try_new(
                    Self::asof_schema(),
                    vec![
                        Arc::new(Int64Array::from(t)) as ArrayRef,
                        Arc::new(Float64Array::from(price)) as ArrayRef,
                        Arc::new(Float64Array::from(spread)) as ArrayRef,
                    ],
                )?)
            }
            KernelPlan::Ofi {
                t,
                bid,
                ask,
                bid_sz,
                ask_sz,
                window,
            } => {
                let ofi = gtv_array::window::ofi_rolling(bid, ask, bid_sz, ask_sz, *window);
                Ok(RecordBatch::try_new(
                    Self::ofi_schema(),
                    vec![
                        Arc::new(Int64Array::from(t.as_ref().clone())) as ArrayRef,
                        Arc::new(Float64Array::from(ofi)) as ArrayRef,
                    ],
                )?)
            }
        }
    }

    fn pit_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("idx", DataType::Int64, false),
            Field::new("valid_from", DataType::Int64, false),
            Field::new("valid_to", DataType::Int64, false),
        ]))
    }

    fn wash_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt64, false),
            Field::new("b", DataType::UInt64, false),
            Field::new("c", DataType::UInt64, false),
        ]))
    }

    fn asof_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("t", DataType::Int64, false),
            Field::new("price", DataType::Float64, true),
            Field::new("spread", DataType::Float64, true),
        ]))
    }

    fn ofi_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("t", DataType::Int64, false),
            Field::new("ofi", DataType::Float64, false),
        ]))
    }
}

// ---------------------------------------------------------------------------
// Compiler: parse the HFT subset into a KernelPlan
// ---------------------------------------------------------------------------

/// Compile a HFT-subset SQL string into a [`KernelPlan`], or `None` when the
/// statement is outside the fast-path subset (caller falls back to DataFusion).
pub fn compile_hft(sql: &str, reg: &HftRegistry) -> Option<KernelPlan> {
    let s = sql.trim().trim_end_matches(';').trim();
    if s.is_empty() {
        return None;
    }
    let lower = s.to_lowercase();

    // OFI window: `… ofi(a,b,c,d,w) OVER (…) FROM <table>`
    if let Some(plan) = compile_ofi(s, &lower, reg) {
        return Some(plan);
    }

    // Strip an optional `SELECT * FROM` prefix.
    let src = if let Some(_) = lower.strip_prefix("select * from ") {
        &s[14..]
    } else if let Some(_) = lower.strip_prefix("select * from") {
        &s[13..]
    } else {
        s
    };
    let src = src.trim();
    let src_lower = src.to_lowercase();

    // Bare table name.
    if !src.contains('(') {
        if let Some(batches) = reg.tables.get(src) {
            return Some(KernelPlan::Table(batches.clone()));
        }
        return None;
    }

    // Function call: `name(args)`.
    let open = src.find('(')?;
    let name = src_lower[..open].trim();
    let close = src.rfind(')')?;
    let args = &src[open + 1..close];

    match name {
        "pit" | "point_in_time" => {
            let t: i64 = args.trim().parse().ok()?;
            let res = reg.pit.get("pit")?.clone();
            Some(KernelPlan::Pit { res, t })
        }
        "wash" | "wash_trade" => {
            let t: i64 = args.trim().parse().ok()?;
            let csr = reg.wash.get("wash")?.clone();
            Some(KernelPlan::Wash { csr, t })
        }
        "aj" | "asof_join" => {
            let left: Vec<i64> = args
                .split(',')
                .map(|x| x.trim().parse::<i64>().ok())
                .collect::<Option<_>>()?;
            let res = reg.asof.get("aj")?.clone();
            Some(KernelPlan::Asof {
                res,
                left: Arc::new(left),
            })
        }
        _ => None,
    }
}

fn compile_ofi(s: &str, lower: &str, reg: &HftRegistry) -> Option<KernelPlan> {
    let fn_idx = lower.find("ofi(").or_else(|| lower.find("order_flow_imbalance("))?;
    let from_idx = lower.rfind("from ")?;
    let table = s[from_idx + 5..].trim().trim_end_matches(';').trim();
    let batches = reg.tables.get(table)?;

    let open = fn_idx + s[fn_idx..].find('(')?;
    let close = fn_idx + s[fn_idx..].find(')')?;
    let args: Vec<&str> = s[open + 1..close].split(',').map(str::trim).collect();
    if args.len() < 5 {
        return None;
    }
    let window: usize = args[4].parse().ok()?;
    let t = extract_i64(batches, "t")?;
    let bid = extract_f64(batches, args[0])?;
    let ask = extract_f64(batches, args[1])?;
    let bid_sz = extract_f64(batches, args[2])?;
    let ask_sz = extract_f64(batches, args[3])?;
    Some(KernelPlan::Ofi {
        t,
        bid,
        ask,
        bid_sz,
        ask_sz,
        window,
    })
}

fn extract_f64(batches: &[RecordBatch], col: &str) -> Option<Arc<Vec<f64>>> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b.column_by_name(col)?;
        let fa = as_primitive_array::<Float64Type>(arr.as_ref());
        out.extend_from_slice(fa.values());
    }
    Some(Arc::new(out))
}

fn extract_i64(batches: &[RecordBatch], col: &str) -> Option<Arc<Vec<i64>>> {
    let mut out = Vec::new();
    for b in batches {
        let arr = b.column_by_name(col)?;
        let ia = as_primitive_array::<Int64Type>(arr.as_ref());
        out.extend_from_slice(ia.values());
    }
    Some(Arc::new(out))
}
