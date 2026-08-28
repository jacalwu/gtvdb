gtvdb (Graph-Temporal-Vector DataBase)
gtvdb is a high-performance, unified single-engine In-Memory database written in Rust. It eliminates the "Dual-Database Trap" by integrating Graph topology traversal, Temporal array processing (kdb+-style), Vector semantic search, and Columnar OLAP analytics into a single Zero-Copy runtime powered by Apache Arrow and DataFusion.

Key Features
Unified Single-Engine Architecture: Unifies Graph, Temporal, Vector, and Columnar analytics inside a shared Apache Arrow memory representation.

Temporal-CSR Graph Engine: Interval-based (valid_from, valid_to) Compressed Sparse Row topology supporting time-travel graph traversals.

kdb+-Style Array Analytics: High-frequency time-series primitives including vectorized asof join and rolling window aggregations (mavg, msum).

Filtered Vector Search: Integrated VectorIndex abstraction (backed by HNSW) supporting K-NN search constrained by dynamic Arrow Bitmasks.

Extensible Query Execution: Built on top of Apache DataFusion, supporting custom physical execution plans, WASM UDF sandboxing, and Parquet storage tiering.

Architecture Overview
Plaintext
gtvdb/
├── gtvdb-core/       # Arrow RecordBatch primitives, Temporal-CSR, memory layouts
├── gtvdb-query/      # DataFusion execution extensions, custom logical/physical plans
├── gtvdb-index/      # Pluggable HNSW vector index & temporal index implementations
├── gtvdb-storage/    # LSM dynamic delta buffer, Parquet tiering, WAL recovery
└── gtvdb-plugin/     # Wasmtime runtime for sandboxed Zero-Copy UDF execution
Quick Start Example (Rust)
Add gtvdb to your Cargo.toml:

Ini, TOML
[dependencies]
gtvdb-core = "0.1.0"
gtvdb-query = "0.1.0"
Querying dynamic temporal graph topologies with vector similarity:

Rust
use anyhow::Result;
use gtvdb_core::TemporalGraphIndex;
use gtvdb_index::VectorIndex;

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Vector Search to retrieve seed nodes
    let query_vector = vec![0.12, -0.45, 0.89, 0.33];
    let seed_nodes = vector_index.search_knn(&query_vector, 10, None)?;

    // 2. Perform Temporal Graph Traversal at specific timestamp T
    let valid_at_timestamp = 1756420000000; // Epoch nanoseconds
    let neighbors = graph_index.fetch_temporal_neighbors(&seed_nodes, valid_at_timestamp)?;

    // 3. Vectorized Array Aggregation via Apache Arrow
    println!("Retrieved {} temporal neighbors.", neighbors.num_rows());
    Ok(())
}
License
This project is licensed under the Apache License 2.0 - see the LICENSE file for details.