//! `gtv` — interactive REPL for the Temporal-Columnar Graph-Vector engine.
//!
//! Phase 1 exposes the in-memory primitives through a small command language.
//! Phase 2 will layer a full SQL REPL on top of DataFusion.

use std::sync::Arc;

use anyhow::{anyhow, Result};
use arrow::array::{ArrayRef, Float64Array, RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::pretty::print_batches;
use rustyline::DefaultEditor;

use gtv_array::{asof, window};
use gtv_core::{EdgeTable, NodeTable, TemporalGraph};

const DEFAULT_T: i64 = 0;

enum Action {
    Continue,
    Quit,
}

struct Demo {
    graph: TemporalGraph,
    times: Vec<i64>,
    prices: Vec<f64>,
}

fn main() -> Result<()> {
    let demo = build_demo()?;
    let mut rl = DefaultEditor::new()?;
    println!("gtv — temporal graph/array shell (in-memory MVP). Type `help` for commands.");
    loop {
        match rl.readline("gtv> ") {
            Ok(line) => {
                let line = line.trim().to_string();
                if line.is_empty() {
                    continue;
                }
                let _ = rl.add_history_entry(&line);
                match run(&demo, &line) {
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

    let graph = TemporalGraph::new(nodes, edges)?;

    Ok(Demo {
        graph,
        times: vec![0, 10, 20, 30, 40, 50],
        prices: vec![100.0, 101.0, 99.0, 102.0, 103.0, 104.0],
    })
}

fn run(demo: &Demo, line: &str) -> Result<Action> {
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
        "quit" | "exit" => return Ok(Action::Quit),
        "" => {}
        other => eprintln!("unknown command `{other}`; type `help`"),
    }
    Ok(Action::Continue)
}

fn parse_left_times(args: &[&str]) -> Result<Vec<i64>> {
    if args.is_empty() {
        return Ok(vec![0, 5, 15, 25, 35, 45, 55, 60]);
    }
    args.iter().map(|s| s.parse::<i64>().map_err(|_| anyhow!("invalid time `{s}`"))).collect()
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
         \x20 mavg <n>              moving average over the price series\n\
         \x20 msum <n>              moving sum over the price series\n\
         \x20 deltas                successive differences\n\
         \x20 asof [t ...]          as-of join against the price series\n\
         \x20 quit | exit"
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
