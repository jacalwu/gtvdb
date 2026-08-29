# gtvdb (Graph-Temporal-Vector DataBase)

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Language: Rust](https://img.shields.io/badge/Language-Rust-orange.svg)](https://www.rust-lang.org/)

**gtvdb** is a high-performance, unified single-engine In-Memory database written in Rust. It eliminates the "Dual-Database Trap" by seamlessly integrating **G**raph topology traversal, **T**emporal array processing (kdb+-style), **V**ector semantic search, and **Columnar** OLAP analytics into a single **Zero-Copy** runtime powered by Apache Arrow and DataFusion.

---

## Key Features

- **Unified Single-Engine Architecture**: Unifies Graph, Temporal, Vector, and Columnar analytics inside a shared Apache Arrow memory representation.
- **Temporal-CSR Graph Engine**: Interval-based (`valid_from`, `valid_to`) Compressed Sparse Row topology supporting time-travel graph traversals and dynamic relationship evolution.
- **kdb+-Style Array Analytics**: High-frequency time-series primitives including vectorized `asof join` and rolling window aggregations (`mavg`, `msum`).
- **Filtered Vector Search**: Integrated `VectorIndex` abstraction (backed by HNSW) supporting K-NN search constrained by dynamic Arrow Bitmasks.
- **Extensible Query Execution**: Built on top of **Apache DataFusion**, supporting custom physical execution plans, WASM UDF sandboxing, and Parquet/Lance storage tiering.

---

## Architecture Overview

```text
crates/
├── gtv-core/    # Arrow RecordBatch primitives, Temporal-CSR graph, memory layouts
├── gtv-array/   # kdb+-style vectorized array ops (asof / mavg / msum / deltas)
├── gtv-engine/  # DataFusion integration: GtvContext, WindowUDFs, table functions
├── gtv-index/   # vector search: FlatIndex (exact) + HnswIndex (approx, bitmask pruning)
├── gtv-storage/ # Arrow ↔ Parquet + SnapshotStore (multi-version time-travel)
├── gtv-delta/   # LSM delta buffer + compaction
├── gtv-pattern/ # temporal graph pattern matching (path/diamond/ring)
├── gtv-udf/     # wasmtime sandbox UDF
└── gtv-cli/     # interactive SQL REPL (bin: gtv)
```

Planned (P5): distributed query dispatch via tonic gRPC + Arrow Flight.

---

## Quick Start (SQL REPL)

```sh
cargo run -p gtv-cli --bin gtv
```

Type SQL over the demo tables (`nodes`, `edges`, `prices`); temporal columns are
`Int64` nanoseconds and edge validity is the half-open interval
`[valid_from, valid_to)`.

```sql
-- temporal slice: edges active at T = 150
SELECT src, dst FROM edges WHERE valid_from <= 150 AND 150 < valid_to;

-- kdb+-style window functions
SELECT t, mavg(price, 3) OVER (ORDER BY t) FROM prices;

-- temporal graph traversal
SELECT * FROM neighbors(0, 100);

-- as-of join against the price series
SELECT * FROM asof_join(0, 5, 15, 25, 35, 45, 55, 60);
```

Beyond SQL there are also shell commands:

```text
knn <node> [k] [--mask a,b,c]  HNSW K-NN vector search (with optional bitmask filter)
save <table> <path>            write a table to Parquet
load <table> <path>            load a Parquet file as a new table
tt <table> <T>                 time-travel: read the snapshot at or before T
pattern [T]                    temporal graph pattern matching (ring/path/diamond)
delta                          LSM delta buffer insert + compaction demo
udf [x ...]                    wasmtime sandbox UDF (x * 1.1)
```
