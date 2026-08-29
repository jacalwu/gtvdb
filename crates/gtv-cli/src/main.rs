//! `gtv` — interactive REPL for the Temporal-Columnar Graph-Vector engine.
//!
//! Phase 1 exposed the in-memory primitives through a small command language.
//! Phase 2 layers a SQL REPL (DataFusion) on top: any input that is not a
//! built-in command is executed as SQL.

use std::sync::Arc;

use anyhow::{anyhow, Result};
use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, UInt64Array,
};
use arrow::compute::{cast, filter_record_batch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::util::pretty::print_batches;
use rustyline::DefaultEditor;

use gtv_array::{asof, window};
use gtv_core::{EdgeTable, NodeTable, TemporalGraph, VectorIndex};
use gtv_delta::{DeltaEdge, LsmStore};
use gtv_engine::GtvContext;
use gtv_index::HnswIndex;
use gtv_pattern::Pattern;
use gtv_storage::{parquet, SnapshotStore};
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
    let demo = build_demo()?;
    let ctx = GtvContext::new();
    register_tables(&ctx, &demo)?;

    let mut rl = DefaultEditor::new()?;
    println!("gtv — temporal graph/array shell (SQL enabled). Type `help` for commands.");
    loop {
        match rl.readline("gtv> ") {
            Ok(line) => {
                let line = line.trim().to_string();
                if line.is_empty() {
                    continue;
                }
                let _ = rl.add_history_entry(&line);
                match run(&demo, &ctx, &line).await {
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
fn edges_active_at(edges: &EdgeTable, t: i64) -> RecordBatch {
    let mask: BooleanArray = (0..edges.len())
        .map(|i| {
            let from = edges.valid_from().value(i);
            let to = edges.valid_to().value(i);
            from <= t && t < to
        })
        .collect();
    filter_record_batch(edges.batch(), &mask).expect("filter preserves schema")
}

/// Fetch a demo table by name (for `save`).
fn demo_batch(demo: &Demo, name: &str) -> Result<RecordBatch> {
    match name {
        "nodes" => Ok(demo.graph.nodes().batch().clone()),
        "edges" => Ok(edges_int64_batch(demo.graph.edges())?.1),
        "prices" => Ok(prices_batch(&demo.times, &demo.prices)?.1),
        _ => Err(anyhow!("unknown table `{name}` (try nodes|edges|prices)")),
    }
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

async fn run(demo: &Demo, ctx: &GtvContext, line: &str) -> Result<Action> {
    let tokens: Vec<&str> = line.split_whitespace().collect();
    let Some(cmd) = tokens.first().copied() else {
        return Ok(Action::Continue);
    };
    match cmd {
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
            let batch = demo_batch(demo, table)?;
            parquet::write_batch(path, &batch)?;
            println!("wrote `{table}` ({} rows) -> {path}", batch.num_rows());
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
        "quit" | "exit" => return Ok(Action::Quit),
        "sql" => {
            let q = line.get(3..).unwrap_or("").trim();
            run_sql(ctx, q).await?;
        }
        _ => {
            // Any other input is executed as SQL.
            run_sql(ctx, line).await?;
        }
    }
    Ok(Action::Continue)
}

async fn run_sql(ctx: &GtvContext, query: &str) -> Result<()> {
    if query.trim().is_empty() {
        eprintln!("usage: `sql <query>`, or type a query directly (e.g. `SELECT * FROM prices`)");
        return Ok(());
    }
    let batches = ctx.sql(query).await?;
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
         \x20 tt <table> <T>        time-travel: table snapshot as-of T\n\
         \x20 pattern [T]           temporal pattern matching (ring/path/diamond)\n\
         \x20 delta                 LSM delta buffer insert + compaction demo\n\
         \x20 udf [x ...]           WASM sandbox UDF (x * 1.1) over prices\n\
         \x20 quit | exit\n\
         \n\
         SQL: any other input is executed as SQL over the `nodes`, `edges` and\n\
         `prices` tables. Temporal columns are Int64 nanoseconds.\n\
         \x20 SELECT src, dst FROM edges WHERE valid_from <= 150 AND 150 < valid_to;\n\
         \x20 SELECT t, mavg(price, 3) OVER (ORDER BY t) FROM prices;\n\
         \x20 SELECT t, msum(price, 2) OVER (ORDER BY t), deltas(price) OVER (ORDER BY t) FROM prices;\n\
         \x20 SELECT * FROM neighbors(0, 100);\n\
         \x20 SELECT * FROM asof_join(0, 5, 15, 25, 35, 45, 55, 60);"
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
