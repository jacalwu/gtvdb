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
gtvdb/
├── gtvdb-core/       # Arrow RecordBatch primitives, Temporal-CSR, memory layouts
├── gtvdb-query/      # DataFusion execution extensions, custom logical/physical plans
├── gtvdb-index/      # Pluggable HNSW vector index & temporal index implementations
├── gtvdb-storage/    # LSM dynamic delta buffer, Parquet/Lance tiering, WAL recovery
└── gtvdb-plugin/     # Wasmtime runtime for sandboxed Zero-Copy UDF execution
