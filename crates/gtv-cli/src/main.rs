//! `gtv` — interactive REPL for the Temporal-Columnar Graph-Vector engine.
//!
//! Phase 1 exposed the in-memory primitives through a small command language.
//! Phase 2 layers a SQL REPL (DataFusion) on top: any input that is not a
//! built-in command is executed as SQL.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{anyhow, Result};

mod lse;
use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampNanosecondArray, UInt64Array,
};
use arrow::compute::{cast, concat_batches, filter_record_batch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::util::pretty::print_batches;
use rustyline::DefaultEditor;

use gtv_array::{asof, window};
use gtv_core::{EdgeTable, NodeTable, TemporalGraph, VectorIndex};
use gtv_delta::{DeltaEdge, LsmStore};
use gtv_engine::hft_exec::KernelPlan;
use gtv_engine::GtvContext;
use gtv_index::HnswIndex;
use gtv_pattern::Pattern;
use gtv_storage::{parquet, HdbStore, SnapshotStore};
use gtv_udf::WasmUdf;

const DEFAULT_T: i64 = 0;

/// A sandboxed UDF: applies a 10% markup (`x * 1.1`) to a price.
const MARKUP_WAT: &str = r#"
(module
  (func (export "map") (param f64) (result f64)
    local.get 0
    f64.const 1.1
    f64.mul))
"#;

enum Action {
    Continue,
    Quit,
}

/// SQL execution mode selected via `ALTER SESSION SET sqlmode = hft|full`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqlMode {
    /// Thin wrapper: limited operator subset + abbreviated names, latency-first.
    Hft,
    /// Full DataFusion SQL: joins / group by / window / CTE / subqueries.
    Full,
}

impl Default for SqlMode {
    fn default() -> Self {
        SqlMode::Hft
    }
}

struct Demo {
    graph: TemporalGraph,
    times: Vec<i64>,
    prices: Vec<f64>,
    /// Per-node embeddings (one row per node, aligned with node id).
    embeddings: Vec<Vec<f32>>,
    /// Approximate K-NN index over the embeddings.
    hnsw: HnswIndex,
    /// Time-travel store seeded with point-in-time edge snapshots.
    store: SnapshotStore,
    /// LSM delta buffer over the demo graph.
    lsm: LsmStore,
    /// A transfer graph with distinct event times for pattern matching.
    transfers: TemporalGraph,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Select the rustls `ring` crypto provider (pure-Rust TLS for the LSE feed).
    let _ = rustls::crypto::ring::default_provider().install_default();

    let demo = build_demo()?;
    let ctx = GtvContext::new();
    register_tables(&ctx, &demo)?;

    let mut rl = DefaultEditor::new()?;
    let mut mode = SqlMode::default();
    let mut timing = false;
    let mut cache: HashMap<String, KernelPlan> = HashMap::new();
    println!("gtv — temporal graph/array shell. sqlmode=hft (default). Type `help` for commands.");
    loop {
        match rl.readline("gtv> ") {
            Ok(line) => {
                let line = line.trim().to_string();
                if line.is_empty() {
                    continue;
                }
                let _ = rl.add_history_entry(&line);
                let result = run(&demo, &ctx, &line, &mut mode, &mut timing, &mut cache).await;
                match result {
                    Ok(Action::Continue) => {}
                    Ok(Action::Quit) => break,
                    Err(e) => eprintln!("error: {e:#}"),
                }
            }
            Err(rustyline::error::ReadlineError::Interrupted) => {
                println!("^C (type `quit` to exit)");
            }
            Err(rustyline::error::ReadlineError::Eof) => break,
            Err(e) => {
                eprintln!("readline error: {e}");
                break;
            }
        }
    }
    Ok(())
}

fn build_demo() -> Result<Demo> {
    let node_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("value", DataType::Float64, false),
        ])),
        vec![
            Arc::new(UInt64Array::from(vec![0u64, 1, 2, 3, 4, 5])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0])) as ArrayRef,
        ],
    )?;
    let nodes = NodeTable::new(node_batch)?;

    let edges = EdgeTable::from_vecs(
        vec![0, 0, 1, 1, 2, 3],
        vec![1, 2, 3, 4, 5, 5],
        vec![1u16, 1, 2, 2, 1, 3],
        vec![0, 50, 0, 100, 0, 150],
        vec![100, 200, 100, 300, 300, 400],
    )?;

    let graph = TemporalGraph::new(nodes, edges.clone())?;

    // Deterministic 4-dim node embeddings (aligned with node id 0..5).
    let embeddings: Vec<Vec<f32>> = (0..6u64)
        .map(|i| {
            vec![
                (i & 1) as f32,
                ((i >> 1) & 1) as f32,
                ((i >> 2) & 1) as f32,
                0.0,
            ]
        })
        .collect();
    let ids: Vec<u64> = (0..6).collect();
    let hnsw = HnswIndex::build(ids, embeddings.clone(), 4, 16, 16)?;

    // Seed the time-travel store with point-in-time edge snapshots.
    let mut store = SnapshotStore::new();
    for t in [0i64, 100, 200] {
        store.insert("edges", t, vec![edges_active_at(&edges, t)])?;
    }

    // LSM delta buffer over the same demo graph.
    let lsm = LsmStore::new(graph.clone());

    // A transfer graph with distinct event times for pattern matching.
    let transfers = build_transfers()?;

    Ok(Demo {
        graph,
        times: vec![0, 10, 20, 30, 40, 50],
        prices: vec![100.0, 101.0, 99.0, 102.0, 103.0, 104.0],
        embeddings,
        hnsw,
        store,
        lsm,
        transfers,
    })
}

/// A small "money transfer" graph whose edges carry distinct event times,
/// supporting temporal ring / path / diamond pattern matches.
fn build_transfers() -> Result<TemporalGraph> {
    let node_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("value", DataType::Float64, false),
        ])),
        vec![
            Arc::new(UInt64Array::from(vec![0u64, 1, 2, 3])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])) as ArrayRef,
        ],
    )?;
    let nodes = NodeTable::new(node_batch)?;
    let edges = EdgeTable::from_vecs(
        vec![0, 1, 2, 3, 0, 1],
        vec![1, 2, 3, 0, 2, 3],
        vec![1u16, 1, 1, 1, 1, 1],
        vec![10, 20, 30, 40, 15, 25],
        vec![1000, 1000, 1000, 1000, 1000, 1000],
    )?;
    Ok(TemporalGraph::new(nodes, edges)?)
}

fn register_tables(ctx: &GtvContext, demo: &Demo) -> Result<()> {
    ctx.register_batches(
        "nodes",
        demo.graph.nodes().batch().schema(),
        vec![demo.graph.nodes().batch().clone()],
    )?;
    // Expose temporal columns as Int64 nanoseconds so SQL slicing is ergonomic
    // (kdb convention: raw timestamp counts), rather than the Arrow Timestamp type.
    let (edge_schema, edges_batch) = edges_int64_batch(demo.graph.edges())?;
    ctx.register_batches("edges", edge_schema, vec![edges_batch])?;

    let (price_schema, price_batch) = prices_batch(&demo.times, &demo.prices)?;
    ctx.register_batches("prices", price_schema, vec![price_batch])?;

    ctx.register_neighbors(demo.graph.csr());
    ctx.register_asof_join(demo.times.clone(), demo.prices.clone());
    register_hft_demo(ctx, demo)?;
    register_knn_collections(ctx)?;
    Ok(())
}

/// Register the HFT demo tables (ticks/orders/book) and the table functions
/// that back TC1 (as-of multi), TC3 (wash trade), TC5 (point-in-time).
fn register_hft_demo(ctx: &GtvContext, demo: &Demo) -> Result<()> {
    // TC2: order-flow ticks (t, bid, ask, bid_sz, ask_sz).
    let t: Vec<i64> = (0..6).map(|i| i * 10).collect();
    let bid = vec![100.0, 100.1, 100.3, 100.2, 100.4, 100.5];
    let ask = vec![100.2, 100.3, 100.5, 100.4, 100.6, 100.7];
    let bid_sz = vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0];
    let ask_sz = vec![5.0, 15.0, 25.0, 35.0, 45.0, 55.0];
    let ticks_schema = Arc::new(Schema::new(vec![
        Field::new("t", DataType::Int64, false),
        Field::new("bid", DataType::Float64, false),
        Field::new("ask", DataType::Float64, false),
        Field::new("bid_sz", DataType::Float64, false),
        Field::new("ask_sz", DataType::Float64, false),
    ]));
    ctx.register_batches(
        "ticks",
        ticks_schema.clone(),
        vec![RecordBatch::try_new(
            ticks_schema,
            vec![
                Arc::new(Int64Array::from(t)) as ArrayRef,
                Arc::new(Float64Array::from(bid)) as ArrayRef,
                Arc::new(Float64Array::from(ask)) as ArrayRef,
                Arc::new(Float64Array::from(bid_sz)) as ArrayRef,
                Arc::new(Float64Array::from(ask_sz)) as ArrayRef,
            ],
        )?],
    )?;

    // TC6: risk orders (id, price, qty, mid, smp).
    let orders_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("price", DataType::Float64, false),
        Field::new("qty", DataType::Float64, false),
        Field::new("mid", DataType::Float64, false),
        Field::new("smp", DataType::Float64, false),
    ]));
    let orders_batch = RecordBatch::try_new(
        orders_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![0i64, 1, 2, 3, 4, 5])) as ArrayRef,
            Arc::new(Float64Array::from(vec![150.0, 151.0, 149.5, 152.0, 148.0, 150.5])) as ArrayRef,
            Arc::new(Float64Array::from(vec![100.0, 20_000.0, 500.0, 5_000.0, 300.0, 1_000.0])) as ArrayRef,
            Arc::new(Float64Array::from(vec![150.0; 6])) as ArrayRef,
            Arc::new(Float64Array::from(vec![0.0, 0.0, 1.0, 0.0, 0.0, 1.0])) as ArrayRef,
        ],
    )?;
    ctx.register_batches("orders", orders_schema, vec![orders_batch])?;

    // TC8: L2 book (sym, level, bid_px, ask_px, bid_sz, ask_sz), 3 symbols x 10 levels.
    let bases = vec![150.0, 200.0, 100.0];
    let mut sym = Vec::new();
    let mut level = Vec::new();
    let mut bid_px = Vec::new();
    let mut ask_px = Vec::new();
    let mut bsz = Vec::new();
    let mut asz = Vec::new();
    for (s, &base) in bases.iter().enumerate() {
        for l in 0..10i64 {
            sym.push(s as i64);
            level.push(l);
            bid_px.push(base - (l as f64 + 1.0) * 0.01);
            ask_px.push(base + (l as f64 + 1.0) * 0.01);
            bsz.push((l as f64 + 1.0) * 10.0);
            asz.push((l as f64 + 1.0) * 8.0);
        }
    }
    let book_schema = Arc::new(Schema::new(vec![
        Field::new("sym", DataType::Int64, false),
        Field::new("level", DataType::Int64, false),
        Field::new("bid_px", DataType::Float64, false),
        Field::new("ask_px", DataType::Float64, false),
        Field::new("bid_sz", DataType::Float64, false),
        Field::new("ask_sz", DataType::Float64, false),
    ]));
    let book_batch = RecordBatch::try_new(
        book_schema.clone(),
        vec![
            Arc::new(Int64Array::from(sym)) as ArrayRef,
            Arc::new(Int64Array::from(level)) as ArrayRef,
            Arc::new(Float64Array::from(bid_px)) as ArrayRef,
            Arc::new(Float64Array::from(ask_px)) as ArrayRef,
            Arc::new(Float64Array::from(bsz)) as ArrayRef,
            Arc::new(Float64Array::from(asz)) as ArrayRef,
        ],
    )?;
    ctx.register_batches("book", book_schema, vec![book_batch])?;

    // TC1: multi-column as-of join (price + spread) with 500us tolerance.
    let spread: Vec<f64> = demo.prices.iter().map(|p| 0.02 + p * 0.0001).collect();
    ctx.register_asof_join_multi(demo.times.clone(), demo.prices.clone(), spread, 500_000);

    // TC5: point-in-time over a temporally-sorted series (vf ascending, vt = vf + 100).
    let vf: Vec<i64> = (0..1000).collect();
    let vt: Vec<i64> = vf.iter().map(|f| f + 100).collect();
    ctx.register_point_in_time(vf, vt);

    // TC3: wash-trade ring(3) over the transfer graph.
    ctx.register_wash_trade(demo.transfers.csr());

    // TC11–TC15: trade/quote micro-structure demo (aligned via as-of join).
    let time: Vec<i64> = vec![100, 200, 300, 400, 500, 600];
    let price = vec![180.50, 180.55, 180.55, 180.45, 180.45, 180.50];
    let bid = vec![180.40, 180.50, 180.50, 180.40, 180.40, 180.45];
    let ask = vec![180.60, 180.60, 180.55, 180.50, 180.50, 180.55];
    let bid_size = vec![100.0, 200.0, 150.0, 300.0, 200.0, 250.0];
    let ask_size = vec![200.0, 150.0, 100.0, 100.0, 150.0, 100.0];
    let native_flag = vec!["BUY", "BUY", "SELL", "SELL", "BUY", "BUY"];
    let t_schema = Arc::new(Schema::new(vec![
        Field::new("time", DataType::Int64, false),
        Field::new("price", DataType::Float64, false),
        Field::new("bid", DataType::Float64, false),
        Field::new("ask", DataType::Float64, false),
        Field::new("bid_size", DataType::Float64, false),
        Field::new("ask_size", DataType::Float64, false),
        Field::new("native_flag", DataType::Utf8, false),
    ]));
    let t_batch = RecordBatch::try_new(
        t_schema.clone(),
        vec![
            Arc::new(Int64Array::from(time)) as ArrayRef,
            Arc::new(Float64Array::from(price)) as ArrayRef,
            Arc::new(Float64Array::from(bid)) as ArrayRef,
            Arc::new(Float64Array::from(ask)) as ArrayRef,
            Arc::new(Float64Array::from(bid_size)) as ArrayRef,
            Arc::new(Float64Array::from(ask_size)) as ArrayRef,
            Arc::new(StringArray::from(native_flag)) as ArrayRef,
        ],
    )?;
    ctx.register_batches("t", t_schema, vec![t_batch])?;

    // TC9: aligned returns table (3 series), for covariance_matrix.
    let returns_schema = Arc::new(Schema::new(vec![
        Field::new("ret_0", DataType::Float64, false),
        Field::new("ret_1", DataType::Float64, false),
        Field::new("ret_2", DataType::Float64, false),
    ]));
    let returns_batch = RecordBatch::try_new(
        returns_schema.clone(),
        vec![
            Arc::new(Float64Array::from(vec![0.010, 0.005, -0.020, 0.015, -0.010])) as ArrayRef,
            Arc::new(Float64Array::from(vec![0.008, 0.004, -0.018, 0.012, -0.008])) as ArrayRef,
            Arc::new(Float64Array::from(vec![0.012, 0.006, -0.022, 0.018, -0.012])) as ArrayRef,
        ],
    )?;
    ctx.register_batches("returns", returns_schema, vec![returns_batch])?;

    // TC10: order stream (side, is_mkt, price, qty) for the matching engine.
    let orders_schema = Arc::new(Schema::new(vec![
        Field::new("side", DataType::Float64, false),
        Field::new("is_mkt", DataType::Float64, false),
        Field::new("price", DataType::Float64, false),
        Field::new("qty", DataType::Float64, false),
    ]));
    let orders_batch = RecordBatch::try_new(
        orders_schema.clone(),
        vec![
            Arc::new(Float64Array::from(vec![1.0, 0.0, 1.0, 0.0, 0.0, 1.0])) as ArrayRef,
            Arc::new(Float64Array::from(vec![0.0, 1.0, 0.0, 1.0, 0.0, 1.0])) as ArrayRef,
            Arc::new(Float64Array::from(vec![100.0, 100.0, 99.5, 100.0, 100.2, 99.0])) as ArrayRef,
            Arc::new(Float64Array::from(vec![10.0, 5.0, 8.0, 3.0, 6.0, 4.0])) as ArrayRef,
        ],
    )?;
    ctx.register_batches("orderstream", orders_schema, vec![orders_batch])?;

    // Phase 2 quant demos: options (Black-Scholes), MBO stream (L2 rebuild),
    // and a single-column returns table (historical VaR).
    let options_schema = Arc::new(Schema::new(vec![
        Field::new("option_type", DataType::Utf8, false),
        Field::new("s", DataType::Float64, false),
        Field::new("k", DataType::Float64, false),
        Field::new("t", DataType::Float64, false),
        Field::new("r", DataType::Float64, false),
        Field::new("sigma", DataType::Float64, false),
    ]));
    let options_batch = RecordBatch::try_new(
        options_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["call", "put", "call", "put"])) as ArrayRef,
            Arc::new(Float64Array::from(vec![100.0, 100.0, 120.0, 110.0])) as ArrayRef,
            Arc::new(Float64Array::from(vec![100.0, 100.0, 110.0, 110.0])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0, 1.0, 0.5, 0.5])) as ArrayRef,
            Arc::new(Float64Array::from(vec![0.05, 0.05, 0.03, 0.03])) as ArrayRef,
            Arc::new(Float64Array::from(vec![0.2, 0.2, 0.25, 0.25])) as ArrayRef,
        ],
    )?;
    ctx.register_batches("options", options_schema, vec![options_batch])?;

    let mbo_schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::UInt64, false),
        Field::new("side", DataType::Float64, false),
        Field::new("price", DataType::Float64, false),
        Field::new("qty", DataType::Float64, false),
        Field::new("action", DataType::Float64, false),
    ]));
    let mbo_batch = RecordBatch::try_new(
        mbo_schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![1u64, 2, 3, 1, 4, 3])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0, 1.0, 0.0, 1.0, 1.0, 0.0])) as ArrayRef,
            Arc::new(Float64Array::from(vec![100.0, 100.0, 99.0, 100.0, 100.5, 99.0])) as ArrayRef,
            Arc::new(Float64Array::from(vec![10.0, 5.0, 7.0, 0.0, 8.0, 2.0])) as ArrayRef,
            Arc::new(Float64Array::from(vec![0.0, 0.0, 0.0, 1.0, 0.0, 2.0])) as ArrayRef,
        ],
    )?;
    ctx.register_batches("mbo", mbo_schema, vec![mbo_batch])?;

    let rets_schema = Arc::new(Schema::new(vec![Field::new(
        "returns",
        DataType::Float64,
        false,
    )]));
    let rets_batch = RecordBatch::try_new(
        rets_schema.clone(),
        vec![Arc::new(Float64Array::from(vec![
            -0.02, -0.01, 0.0, 0.01, 0.02, -0.03, 0.005, 0.015,
        ])) as ArrayRef],
    )?;
    ctx.register_batches("rets", rets_schema, vec![rets_batch])?;

    Ok(())
}


/// Register the canonical vector collections (songs + tss_series) used by the
/// KDB.AI parity tests TC-07 / TC-11, mirroring `testcase/data/gen_data.py`.
fn register_knn_collections(ctx: &GtvContext) -> Result<()> {
    let song_ids: Vec<u64> = (0..10).collect();
    let song_vectors = vec![
        vec![0.0, 0.0],
        vec![0.5, 0.5],
        vec![5.0, 5.0],
        vec![5.2, 5.1],
        vec![1.0, 1.0],
        vec![1.1, 1.0],
        vec![9.0, 9.0],
        vec![9.1, 9.0],
        vec![0.2, 0.1],
        vec![4.9, 5.0],
    ];
    let genres = ["pop", "pop", "rock", "rock", "jazz", "jazz", "classical", "classical", "pop", "rock"];
    ctx.register_knn(
        "songs",
        song_ids,
        song_vectors,
        Some(genres.iter().map(|s| s.to_string()).collect()),
    )?;

    let tss_ids: Vec<u64> = (0..4).collect();
    let tss_vectors = vec![
        vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        vec![5.0, 4.0, 2.0, 2.0, 4.0, 5.0],
        vec![3.0, 3.0, 3.0, 3.0, 3.0, 3.0],
        vec![6.0, 5.0, 4.0, 3.0, 2.0, 1.0],
    ];
    ctx.register_knn("tss", tss_ids, tss_vectors, None)?;

    Ok(())
}

/// Edge table with temporal columns as `Int64` nanoseconds (ergonomic for SQL).
fn edges_int64_batch(edges: &EdgeTable) -> Result<(SchemaRef, RecordBatch)> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("src", DataType::UInt64, false),
        Field::new("dst", DataType::UInt64, false),
        Field::new("edge_type", DataType::UInt16, false),
        Field::new("valid_from", DataType::Int64, false),
        Field::new("valid_to", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(edges.src().clone()) as ArrayRef,
            Arc::new(edges.dst().clone()) as ArrayRef,
            Arc::new(edges.edge_type().clone()) as ArrayRef,
            cast(edges.valid_from(), &DataType::Int64)?,
            cast(edges.valid_to(), &DataType::Int64)?,
        ],
    )?;
    Ok((schema, batch))
}

/// Price series as a two-column `(t, price)` batch.
fn prices_batch(times: &[i64], prices: &[f64]) -> Result<(SchemaRef, RecordBatch)> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("t", DataType::Int64, false),
        Field::new("price", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(times.to_vec())) as ArrayRef,
            Arc::new(Float64Array::from(prices.to_vec())) as ArrayRef,
        ],
    )?;
    Ok((schema, batch))
}

/// The subset of edges active at time `t` (`valid_from <= t < valid_to`).
///
/// Built with Arrow's SIMD comparison kernels (`lt_eq` + `gt` fused via `and`)
/// rather than a per-element scalar loop, so mask generation stays vectorized
/// for million-edge tables.
fn edges_active_at(edges: &EdgeTable, t: i64) -> RecordBatch {
    let from = edges.valid_from();
    let to = edges.valid_to();
    let t_scalar = TimestampNanosecondArray::new_scalar(t);
    let mask = arrow::compute::kernels::cmp::lt_eq(from, &t_scalar)
        .and_then(|after| {
            let before = arrow::compute::kernels::cmp::gt(to, &t_scalar)?;
            arrow::compute::kernels::boolean::and(&after, &before)
        })
        .expect("temporal mask over timestamp columns");
    filter_record_batch(edges.batch(), &mask).expect("filter preserves schema")
}

/// Parse an optional `--mask a,b,c` flag into allowed node ids, if present.
fn parse_mask(tokens: &[&str]) -> Result<Option<Vec<u64>>> {
    let Some(pos) = tokens.iter().position(|&t| t == "--mask") else {
        return Ok(None);
    };
    let raw = tokens
        .get(pos + 1)
        .ok_or_else(|| anyhow!("usage: knn <node> [k] [--mask a,b,c]"))?;
    raw.split(',')
        .map(|s| {
            s.trim()
                .parse::<u64>()
                .map_err(|_| anyhow!("invalid mask id `{s}`"))
        })
        .collect::<Result<Vec<_>>>()
        .map(Some)
}

async fn run(
    demo: &Demo,
    ctx: &GtvContext,
    line: &str,
    mode: &mut SqlMode,
    timing: &mut bool,
    cache: &mut HashMap<String, KernelPlan>,
) -> Result<Action> {
    let tokens: Vec<&str> = line.split_whitespace().collect();
    let Some(cmd) = tokens.first().copied() else {
        return Ok(Action::Continue);
    };
    match cmd {
        "set" | "SET" => {
            // SET DURATION = ON | OFF
            let joined = tokens[1..].join(" ");
            let lower = joined.trim_end_matches(';').trim().to_lowercase();
            if lower == "duration = on" || lower == "duration=on" {
                *timing = true;
                println!("duration = on");
            } else if lower == "duration = off" || lower == "duration=off" {
                *timing = false;
                println!("duration = off");
            } else {
                return Err(anyhow!(
                    "usage: SET DURATION = ON | OFF (got `{joined}`)"
                ));
            }
        }
        "alter" | "ALTER" => {
            // ALTER SESSION SET sqlmode = hft | full
            let joined = tokens[1..].join(" ");
            let lower = joined.trim_end_matches(';').trim().to_lowercase();
            if lower == "session set sqlmode = hft" {
                *mode = SqlMode::Hft;
                println!("sqlmode = hft");
            } else if lower == "session set sqlmode = full" {
                *mode = SqlMode::Full;
                println!("sqlmode = full");
            } else {
                return Err(anyhow!(
                    "usage: ALTER SESSION SET sqlmode = hft | full (got `{joined}`)"
                ));
            }
        }
        "help" | "?" => print_help(),
        "tables" => show_tables(demo),
        "neighbors" => {
            let node = require_arg(&tokens, 1, "neighbors <node> [T]")?.parse::<u64>()?;
            let t = optional_arg(&tokens, 2).map_or(Ok(DEFAULT_T), |s| s.parse::<i64>())?;
            let batch = demo
                .graph
                .csr()
                .neighbors_record_batch(&UInt64Array::from(vec![node]), t)?;
            let _ = print_batches(&[batch]);
        }
        "khop" => {
            let node = require_arg(&tokens, 1, "khop <node> <k> [T]")?.parse::<u64>()?;
            let k = require_arg(&tokens, 2, "khop <node> <k> [T]")?.parse::<usize>()?;
            let t = optional_arg(&tokens, 3).map_or(Ok(DEFAULT_T), |s| s.parse::<i64>())?;
            let frontiers = demo.graph.khop(&UInt64Array::from(vec![node]), k, t)?;
            for (i, f) in frontiers.iter().enumerate() {
                println!("hop {} = {:?}", i + 1, f.values().as_ref());
            }
        }
        "mavg" => {
            let n = require_arg(&tokens, 1, "mavg <n>")?.parse::<usize>()?;
            println!("mavg[{n}] = {:?}", window::mavg(&demo.prices, n));
        }
        "msum" => {
            let n = require_arg(&tokens, 1, "msum <n>")?.parse::<usize>()?;
            println!("msum[{n}] = {:?}", window::msum(&demo.prices, n));
        }
        "deltas" => {
            println!("deltas = {:?}", window::deltas(&demo.prices));
        }
        "asof" => {
            let left = parse_left_times(&tokens[1..])?;
            let got = asof::asof_join_f64(&left, &demo.times, &demo.prices);
            print_asof(&left, &got);
        }
        "knn" => {
            let node = require_arg(&tokens, 1, "knn <node> [k] [--mask a,b,c]")?.parse::<u64>()?;
            let k = optional_arg(&tokens, 2)
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(3);
            let mask_ids = parse_mask(&tokens)?;
            let query = demo
                .embeddings
                .get(node as usize)
                .cloned()
                .ok_or_else(|| anyhow!("node {node} out of range"))?;
            let mask = mask_ids.as_ref().map(|allowed| {
                BooleanArray::from(
                    (0..demo.embeddings.len())
                        .map(|i| allowed.contains(&(i as u64)))
                        .collect::<Vec<bool>>(),
                )
            });
            let got = demo.hnsw.search_knn(&query, k, mask.as_ref())?;
            println!("knn(node {node}, k={k}) = {:?}", got.values().as_ref());
        }
        "save" => {
            let table = require_arg(&tokens, 1, "save <table> <path>")?;
            let path = require_arg(&tokens, 2, "save <table> <path>")?;
            let batches = ctx.sql(&format!("SELECT * FROM {table}")).await?;
            let Some(first) = batches.first() else {
                return Err(anyhow!("table `{table}` is empty"));
            };
            let all = concat_batches(&first.schema(), &batches)?;
            parquet::write_batch(path, &all)?;
            println!("wrote `{table}` ({} rows) -> {path}", all.num_rows());
        }
        "load" => {
            let table = require_arg(&tokens, 1, "load <table> <path>")?;
            let path = require_arg(&tokens, 2, "load <table> <path>")?;
            let batches = parquet::read_batches(path)?;
            let Some(first) = batches.first() else {
                return Err(anyhow!("`{path}` contains no batches"));
            };
            ctx.register_batches(table, first.schema(), batches)?;
            println!("loaded `{table}` from {path}");
        }
        "loadcsv" => {
            // Method 1/2: load CSV from disk and assign it to a session table.
            let table = require_arg(&tokens, 1, "loadcsv <table> <path>")?;
            let path = require_arg(&tokens, 2, "loadcsv <table> <path>")?;
            ctx.register_csv(path, table)?;
            println!("loaded `{table}` from {path}");
        }
        "hdb_save" => {
            // hdb_save <table> <date> [root] — persist a table to the HDB layout
            // (<root>/<date>/<table>/<symbol>.parquet, split by `symbol` if present).
            let table = require_arg(&tokens, 1, "hdb_save <table> <date> [root]")?;
            let date = require_arg(&tokens, 2, "hdb_save <table> <date> [root]")?;
            let root = optional_arg(&tokens, 3).unwrap_or("hdb");
            let batches = ctx.sql(&format!("SELECT * FROM {table}")).await?;
            let Some(first) = batches.first() else {
                return Err(anyhow!("table `{table}` is empty"));
            };
            let all = concat_batches(&first.schema(), &batches)?;
            let hdb = HdbStore::new(root);
            let n = hdb.write_table(date, table, &all)?;
            println!(
                "hdb_save: wrote {n} partition(s) to {}/{date}/{table}/",
                hdb.root().display()
            );
        }
        "hdb_load" => {
            // hdb_load <table> <date> <sym> [root] — read one HDB partition.
            let table = require_arg(&tokens, 1, "hdb_load <table> <date> <sym> [root]")?;
            let date = require_arg(&tokens, 2, "hdb_load <table> <date> <sym> [root]")?;
            let sym = require_arg(&tokens, 3, "hdb_load <table> <date> <sym> [root]")?;
            let root = optional_arg(&tokens, 4).unwrap_or("hdb");
            let hdb = HdbStore::new(root);
            let batches = hdb.read_partition(date, table, sym)?;
            let Some(first) = batches.first() else {
                return Err(anyhow!("no data in {date}/{table}/{sym}"));
            };
            ctx.register_batches(table, first.schema(), batches)?;
            println!("hdb_load: loaded `{table}` from {date}/{sym}");
        }
        "hdb_scan" => {
            // hdb_scan <table> <start> <end> [sym] [root] — prune + scan a date range.
            let table = require_arg(&tokens, 1, "hdb_scan <table> <start> <end> [sym] [root]")?;
            let start = require_arg(&tokens, 2, "hdb_scan <table> <start> <end> [sym] [root]")?;
            let end = require_arg(&tokens, 3, "hdb_scan <table> <start> <end> [sym] [root]")?;
            let sym = optional_arg(&tokens, 4);
            let root = optional_arg(&tokens, 5).unwrap_or("hdb");
            let syms: Vec<String> = sym.map(|s| vec![s.to_string()]).unwrap_or_default();
            let hdb = HdbStore::new(root);
            let batches = hdb.scan(table, start, end, &syms)?;
            let Some(first) = batches.first() else {
                return Err(anyhow!("no data in {start}..{end} for `{table}`"));
            };
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            ctx.register_batches(table, first.schema(), batches)?;
            println!("hdb_scan: loaded `{table}` {start}..{end} ({rows} rows)");
        }
        "hdb_flush" => {
            // hdb_flush <table> [root] [interval_secs] — background task that
            // flushes the memory table to HDB (symbol-enumerated) on an interval.
            let table = require_arg(&tokens, 1, "hdb_flush <table> [root] [interval_secs]")?.to_string();
            let root = optional_arg(&tokens, 2).unwrap_or("hdb").to_string();
            let interval = optional_arg(&tokens, 3)
                .map_or(Ok(86400u64), |s| s.parse::<u64>())?;
            let ctx2 = ctx.clone();
            let table2 = table.clone();
            let root2 = root.clone();
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(std::time::Duration::from_secs(interval));
                // first tick fires immediately; skip it and flush at the first interval.
                ticker.tick().await;
                loop {
                    ticker.tick().await;
                    let date = chrono::Local::now().format("%Y.%m.%d").to_string();
                    match ctx2.sql(&format!("SELECT * FROM {table2}")).await {
                        Ok(batches) => {
                            let Some(first) = batches.first() else { continue };
                            let all = match concat_batches(&first.schema(), &batches) {
                                Ok(b) => b,
                                Err(e) => {
                                    eprintln!("hdb_flush `{table2}`: {e}");
                                    continue;
                                }
                            };
                            let hdb = HdbStore::new(&root2);
                            match hdb.write_table(&date, &table2, &all) {
                                Ok(n) => eprintln!(
                                    "hdb_flush: wrote {n} partition(s) to {date}/{table2}/"
                                ),
                                Err(e) => eprintln!("hdb_flush `{table2}`: {e}"),
                            }
                        }
                        Err(e) => eprintln!("hdb_flush `{table2}`: {e}"),
                    }
                }
            });
            println!("hdb_flush: `{table}` -> {root}/<date>/<table>/ every {interval}s");
        }
        "fetch" => {
            // fetch <table> <symbol> [limit] — pull historical ticks from the
            // London Strategic Edge REST API into a session table.
            let table = require_arg(&tokens, 1, "fetch <table> <symbol> [limit]")?.to_string();
            let symbol = require_arg(&tokens, 2, "fetch <table> <symbol> [limit]")?;
            let limit = optional_arg(&tokens, 3)
                .map_or(Ok(100_000usize), |s| s.parse::<usize>())?;
            let key = gtv_engine::tickdata::api_key();
            let ticks = gtv_engine::tickdata::fetch_history(symbol, limit, &key)?;
            let batch = gtv_engine::tickdata::hist_to_batch(&ticks);
            ctx.register_batches(&table, gtv_engine::tickdata::hist_schema(), vec![batch])?;
            println!("fetched `{table}` <- {symbol} ({} rows)", ticks.len());
        }
        "live" => {
            // live <table> <symbol...> — stream LSE ticks into a session table.
            let table = require_arg(&tokens, 1, "live <table> <symbol...>")?.to_string();
            let symbols: Vec<String> = tokens[2..].iter().map(|s| s.to_string()).collect();
            if symbols.is_empty() {
                return Err(anyhow!("usage: live <table> <symbol...> (e.g. live q MCO TSLA)"));
            }
            let api_key = std::env::var("LSE_API_KEY").map_err(|_| {
                anyhow!("set LSE_API_KEY (London Strategic Edge live API key) before `live`")
            })?;
            let ticks: Arc<Mutex<Vec<lse::Tick>>> = Arc::new(Mutex::new(Vec::new()));
            ctx.register_batches(&table, lse::live_schema(), vec![lse::empty_batch()])?;
            let feed_ticks = ticks.clone();
            let symbols_feed = symbols.clone();
            tokio::spawn(async move {
                if let Err(e) = lse::run_feed(&api_key, &symbols_feed, feed_ticks).await {
                    eprintln!("lse feed: {e:#}");
                }
            });
            let ctx2 = ctx.clone();
            let flush_ticks = ticks.clone();
            let table2 = table.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
                loop {
                    interval.tick().await;
                    let mut guard = match flush_ticks.lock() {
                        Ok(g) => g,
                        Err(_) => return,
                    };
                    if guard.is_empty() {
                        continue;
                    }
                    let batch = lse::ticks_to_batch(&guard);
                    guard.clear();
                    let _ = ctx2.register_batches(&table2, lse::live_schema(), vec![batch]);
                }
            });
            println!(
                "live: streaming `{table}` <- {} ({})",
                symbols.join(","),
                lse::WSS_URL
            );
        }
        "bgload" => {
            // Method 3: a background thread re-imports the data file (CSV or
            // Parquet, auto-detected by extension) on an interval, refreshing
            // the registered table the session can keep reading.
            let table = require_arg(&tokens, 1, "bgload <table> <path> [interval_ms]")?.to_string();
            let path = require_arg(&tokens, 2, "bgload <table> <path> [interval_ms]")?.to_string();
            let interval_ms = optional_arg(&tokens, 3)
                .map_or(Ok(1000u64), |s| s.parse::<u64>())?;
            register_datafile(ctx, &path, &table)?;
            println!("bgload: `{table}` <- {path} every {interval_ms}ms");
            let ctx2 = ctx.clone();
            std::thread::spawn(move || loop {
                std::thread::sleep(std::time::Duration::from_millis(interval_ms));
                match read_datafile(&path) {
                    Ok(batches) => {
                        if let Some(first) = batches.first() {
                            ctx2.deregister_table(&table);
                            let _ = ctx2.register_batches(&table, first.schema(), batches);
                        }
                    }
                    Err(e) => eprintln!("bgload `{table}`: {e}"),
                }
            });
        }
        "tt" => {
            let table = require_arg(&tokens, 1, "tt <table> <T>")?;
            let t = require_arg(&tokens, 2, "tt <table> <T>")?.parse::<i64>()?;
            let batches = demo.store.as_of(table, t)?;
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            println!("{table} as-of T={t} ({rows} rows):");
            let _ = print_batches(&batches);
        }
        "pattern" => {
            let csr = demo.transfers.csr();
            let valid_at = optional_arg(&tokens, 1)
                .map_or(Ok(500), |s| s.parse::<i64>())?;
            for (name, pat) in [
                ("ring(4)", Pattern::ring(4)),
                ("path(3)", Pattern::temporal_path(3)),
                ("diamond", Pattern::diamond()),
            ] {
                let m = gtv_pattern::find(csr, &pat, valid_at, 10)?;
                println!("{name}: {} match(es)", m.len());
                for mm in &m {
                    println!("  nodes = {:?}", mm.nodes);
                }
            }
        }
        "delta" => {
            let before = demo.lsm.merged_edges()?.len();
            println!("delta: {before} edges (merged snapshot+delta)");
            demo.lsm.insert(DeltaEdge {
                src: 3,
                dst: 1,
                edge_type: 5,
                valid_from: 0,
                valid_to: 500,
            });
            println!(
                "insert 3->1 @[0,500): pending={}, merged={} edges",
                demo.lsm.pending(),
                demo.lsm.merged_edges()?.len()
            );
            demo.lsm.compact_now()?;
            println!(
                "compacted: pending={}, merged={} edges",
                demo.lsm.pending(),
                demo.lsm.merged_edges()?.len()
            );
        }
        "udf" => {
            let input: Vec<f64> = if tokens.len() > 1 {
                tokens[1..]
                    .iter()
                    .map(|s| s.parse::<f64>().map_err(|_| anyhow!("invalid number `{s}`")))
                    .collect::<Result<Vec<_>>>()?
            } else {
                demo.prices.clone()
            };
            let mut udf = WasmUdf::from_wat(MARKUP_WAT, "map")?;
            let out = udf.map(&input)?;
            println!("WASM UDF (x * 1.1):");
            for (x, y) in input.iter().zip(&out) {
                println!("  {x} -> {y}");
            }
        }
        "remote" => {
            let addr = require_arg(&tokens, 1, "remote <host:port> <sql>")?;
            let sql = tokens[2..].join(" ");
            if sql.is_empty() {
                return Err(anyhow!("usage: remote <host:port> <sql>"));
            }
            let batches = gtv_proto::query_remote(addr, &sql).await?;
            if !batches.is_empty() {
                let _ = print_batches(&batches);
            }
        }
        "quit" | "exit" => return Ok(Action::Quit),
        "sql" => {
            let q = line.get(3..).unwrap_or("").trim();
            run_sql(ctx, q, timing).await?;
        }
        _ => {
            // Any other input is executed as SQL.
            if *mode == SqlMode::Hft {
                // M2: precompiled KernelPlan fast path (compiled once, cached),
                // covering bare table names, pit/wash/aj table functions and OFI.
                if let Some(plan) = cache.get(line) {
                    let t0 = Instant::now();
                    let batch = plan.execute()?;
                    let us = t0.elapsed().as_secs_f64() * 1e6;
                    let _ = print_batches(std::slice::from_ref(&batch));
                    if *timing {
                        println!("duration: {us:.3} µs (kernel, cache hit)");
                    }
                    return Ok(Action::Continue);
                }
                if let Some(plan) = ctx.compile_hft(line) {
                    let t0 = Instant::now();
                    let batch = plan.execute()?;
                    let us = t0.elapsed().as_secs_f64() * 1e6;
                    let _ = print_batches(std::slice::from_ref(&batch));
                    if *timing {
                        println!("duration: {us:.3} µs (kernel, compiled)");
                    }
                    // Table plans re-read the registry on each compile (keeps
                    // bgload-refreshed tables fresh); operator plans are cached.
                    if !matches!(plan, KernelPlan::Table(_)) {
                        cache.insert(line.to_string(), plan);
                    }
                    return Ok(Action::Continue);
                }
                // DataFusion-only table functions (file loaders / index search /
                // stateful ops): wrap a bare `fn(args)` as `SELECT * FROM fn(args)`.
                let fn_name = cmd.split('(').next().unwrap_or(cmd);
                if cmd.contains('(') && DF_TABLE_FNS.contains(&fn_name) {
                    run_sql(ctx, &format!("SELECT * FROM {line}"), timing).await?;
                    return Ok(Action::Continue);
                }
                check_hft_subset(line)?;
            }
            run_sql(ctx, line, timing).await?;
        }
    }
    Ok(Action::Continue)
}

/// DataFusion table functions that have no KernelPlan fast path, callable as
/// bare `fn(args)` shorthands in HFT mode.
const DF_TABLE_FNS: &[&str] = &[
    "read_csv",
    "read_parquet",
    "knn",
    "vector_search",
    "neighbors",
    "tick_to_trade",
    "ttrade",
    "match_orders",
    "match",
    "covariance_matrix",
    "cov",
];

/// Read a data file, auto-detecting CSV vs Parquet by extension.
fn read_datafile(path: &str) -> gtv_storage::Result<Vec<RecordBatch>> {
    if path.ends_with(".parquet") || path.ends_with(".pq") {
        gtv_storage::read_batches(path)
    } else {
        gtv_storage::read_csv(path)
    }
}

/// Register a data file (CSV or Parquet) as a session table.
fn register_datafile(ctx: &GtvContext, path: &str, name: &str) -> Result<()> {
    if path.ends_with(".parquet") || path.ends_with(".pq") {
        ctx.register_parquet(path, name)?;
    } else {
        ctx.register_csv(path, name)?;
    }
    Ok(())
}

/// HFT mode accepts only the latency-first subset: SELECT over tables/operators
/// with simple WHERE/ORDER BY/LIMIT — no JOIN/GROUP BY/HAVING/CTE/subquery/UNION.
fn check_hft_subset(sql: &str) -> Result<()> {
    let upper = sql.to_uppercase();
    let forbidden = [
        "JOIN", "GROUP", "HAVING", "WITH", "UNION", "DISTINCT", "EXCEPT",
        "INTERSECT", "CROSS", "LATERAL",
    ];
    for f in forbidden {
        if upper.contains(f) {
            return Err(anyhow!(
                "hft mode does not support `{f}`; switch with: ALTER SESSION SET sqlmode = full"
            ));
        }
    }
    // Reject subqueries (`FROM (SELECT …)`, `IN (SELECT …)`), but keep the
    // function-call parentheses that operators use (e.g. `ofi(a,b,c)`).
    if upper.contains("(SELECT") || upper.contains("FROM (") {
        return Err(anyhow!(
            "hft mode does not support subqueries; switch with: ALTER SESSION SET sqlmode = full"
        ));
    }
    Ok(())
}

async fn run_sql(ctx: &GtvContext, query: &str, timing: &bool) -> Result<()> {
    if query.trim().is_empty() {
        eprintln!("usage: `sql <query>`, or type a query directly (e.g. `SELECT * FROM prices`)");
        return Ok(());
    }
    let t0 = Instant::now();
    let batches = ctx.sql(query).await?;
    let us = t0.elapsed().as_secs_f64() * 1e6;
    if *timing {
        println!("duration: {us:.3} µs (sql)");
    }
    if !batches.is_empty() {
        let _ = print_batches(&batches);
    }
    Ok(())
}

fn parse_left_times(args: &[&str]) -> Result<Vec<i64>> {
    if args.is_empty() {
        return Ok(vec![0, 5, 15, 25, 35, 45, 55, 60]);
    }
    args.iter()
        .map(|s| s.parse::<i64>().map_err(|_| anyhow!("invalid time `{s}`")))
        .collect()
}

fn print_asof(left: &[i64], got: &[Option<f64>]) {
    for (t, v) in left.iter().zip(got) {
        match v {
            Some(x) => println!("  t={t:<4} -> {x}"),
            None => println!("  t={t:<4} -> NULL"),
        }
    }
}

fn show_tables(demo: &Demo) {
    println!("== nodes ==");
    let _ = print_batches(std::slice::from_ref(demo.graph.nodes().batch()));
    println!("== edges ==");
    let _ = print_batches(std::slice::from_ref(demo.graph.edges().batch()));
    println!("== price series ==");
    for (t, p) in demo.times.iter().zip(&demo.prices) {
        println!("  t={t:<4} price={p}");
    }
}

fn print_help() {
    println!(
        "commands:\n\
         \x20 help | ?              this help\n\
         \x20 tables                show node/edge tables and price series\n\
         \x20 neighbors <node> [T]  temporal neighbors at time T (default 0)\n\
         \x20 khop <node> <k> [T]   k-hop traversal at time T\n\
         \x20 mavg <n> / msum <n>   rolling average/sum over the price series\n\
         \x20 deltas                successive differences\n\
         \x20 asof [t ...]          as-of join against the price series\n\
         \x20 knn <node> [k] [--mask a,b,c]  HNSW K-NN over node embeddings\n\
         \x20 save <table> <path>   write a table to a Parquet file\n\
         \x20 load <table> <path>   load a Parquet file as a table\n\
         \x20 loadcsv <table> <path>  load CSV from disk into a session table\n\
         \x20 load <table> <path>   load Parquet from disk into a session table\n\
         \x20 bgload <table> <path> [ms]  background re-import (CSV or Parquet)\n\
         \x20 live <table> <symbol...>  stream LSE live ticks (needs LSE_API_KEY)\n\
         \x20 fetch <table> <symbol> [limit]  pull LSE historical ticks (REST API)\n\
         \x20 hdb_save <table> <date> [root]  persist table to HDB partitions\n\
         \x20 hdb_load <table> <date> <sym> [root]  read one HDB partition\n\
         \x20 hdb_scan <table> <start> <end> [sym] [root]  scan HDB date range\n\
         \x20 hdb_flush <table> [root] [secs]  background HDB flush (sym-enumerated)\n\
         \x20 tt <table> <T>        time-travel: table snapshot as-of T\n\
         \x20 pattern [T]           temporal pattern matching (ring/path/diamond)\n\
         \x20 delta                 LSM delta buffer insert + compaction demo\n\
         \x20 udf [x ...]           WASM sandbox UDF (x * 1.1) over prices\n\
         \x20 remote <host:port> <sql>  execute SQL on a remote gtv-server\n\
         \x20 quit | exit\n\
         \n\
         session:\n\
         \x20 ALTER SESSION SET sqlmode = hft | full   (default: hft)\n\
         \x20 SET DURATION = ON | OFF                  time each action (us)\n\
         \n\
         SQL (hft mode: thin subset + abbreviated ops + kdb shorthand):\n\
         \x20 ticks / orders / book                   bare table name dumps rows\n\
         \x20 pit(500) / wash(500)                    bare table fn == SELECT * FROM it\n\
         \x20 SELECT t, ofi(bid,ask,bid_sz,ask_sz,100) OVER (ORDER BY t) FROM ticks;\n\
         \x20 SELECT * FROM pit(500);\n\
         \x20 SELECT * FROM wash(500);\n\
         \x20 SELECT count(*) FROM orders WHERE risk(price,qty,mid,smp);\n\
         \x20 SELECT sym, mp(bid_px,ask_px,bid_sz,ask_sz) FROM book WHERE level = 0;\n\
         \x20 tick_to_trade('mco', 100) / match_orders('orderstream') / covariance_matrix('returns', 3);\n\
         \n\
         SQL (full mode: complete DataFusion SQL, full names):\n\
         \x20 SELECT sym, order_book_imbalance(bid_sz,ask_sz) FROM book GROUP BY sym;\n\
         \x20 SELECT id FROM vector_search('songs', '0.1,0.1', 3);\n\
         \x20 SELECT * FROM asof_join(0, 5, 15, 25, 35, 45, 55, 60);\n\
         \x20 SELECT t, mavg(price, 3) OVER (ORDER BY t) FROM prices;\n\
         \x20 SELECT * FROM read_csv('path/to/ticks.csv');\n\
         \x20 SELECT * FROM read_parquet('path/to/ticks.parquet');\n\
         \x20 CREATE TABLE t AS SELECT * FROM read_csv('path/to/ticks.csv');"
    );
}

fn require_arg<'a>(tokens: &'a [&'a str], idx: usize, usage: &str) -> Result<&'a str> {
    tokens
        .get(idx)
        .copied()
        .ok_or_else(|| anyhow!("missing argument; usage: {usage}"))
}

fn optional_arg<'a>(tokens: &'a [&'a str], idx: usize) -> Option<&'a str> {
    tokens.get(idx).copied()
}
