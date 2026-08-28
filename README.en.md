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
├── gtv-core/   # Arrow RecordBatch primitives, Temporal-CSR graph, memory layouts
├── gtv-array/  # kdb+-style vectorized array ops (asof / mavg / msum / deltas)
├── gtv-engine/ # DataFusion integration: GtvContext, WindowUDFs, table functions
└── gtv-cli/    # interactive SQL REPL (bin: gtv)
```

Planned (P3–P5): HNSW vector index, Parquet/LSM storage tiering, and WASM UDF sandbox.

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
