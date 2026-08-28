Architecture Specification: Temporal-Columnar Graph-Vector Engine (Rust)
Project Vision & Core Concept
本專案旨在開發一款兼具 kdb+ 高頻陣列計算、Graph 拓撲走訪、Vector 語意檢索 與 Columnar 極速 OLAP 掃描 的單引擎數據庫 (Single-Engine Architecture)。

底層架構： 以 Apache Arrow (arrow-rs) 作為 In-Memory 列式資料標準，搭配 Apache DataFusion 作為向量化查詢與執行引擎。

時序拓撲： 採用 Interval-Based (valid_from, valid_to) 的 Temporal-CSR (Compressed Sparse Row) 結構，將動態圖拓撲轉換為連續記憶體陣列。

擴充機制： 採用 Trait-Driven 模組化設計，提供 DataFusion 自訂 Physical Plan 註冊、外掛式 Vector Index 與 WASM 沙盒 UDF 運算。

Technical Stack & Dependencies
Core Engine: Rust (edition 2021+)

Memory & Columnar Format: arrow / arrow-rs

Query & Execution Engine: datafusion

Persistence & Cold Storage: parquet, object_store

Vector Search: hnsw-rs / usearch

Concurrency & Networking: tokio, tonic (gRPC)

Plugin Architecture: wasmtime (WASM UDF Runtime)

Data Layout & Key Abstractions
1. Temporal Edge Chunk Layout
Rust
pub struct TemporalEdgeChunk {
    pub src_nodes: UInt64Array,
    pub dst_nodes: UInt64Array,
    pub valid_from: TimestampNanosecondArray,
    pub valid_to: TimestampNanosecondArray,
    pub edge_type: UInt16Array,
}
2. Core Extension Traits
Rust
pub trait TemporalGraphIndex: Send + Sync {
    fn fetch_temporal_neighbors(
        &self, 
        src_nodes: &UInt64Array, 
        valid_at: i64
    ) -> Result<RecordBatch>;
}

pub trait VectorIndex: Send + Sync {
    fn search_knn(
        &self, 
        query: &[f32], 
        k: usize, 
        filter_mask: Option<&BooleanArray>
    ) -> Result<UInt64Array>;
}

pub trait CustomTemporalOperator: Send + Sync {
    fn name(&self) -> &str;
    fn eval_batch(&self, input: &RecordBatch) -> Result<RecordBatch>;
}
Phased Development Roadmap
Phase 1: In-Memory Core MVP (Temporal-CSR + kdb+ Arrays)
  │
  ├─► Phase 2: Engine Integration (DataFusion + Vector + Storage Tiering)
  │
  └─► Phase 3: Advanced Extensions (WASM UDFs + Pattern Matching + Dist. Engine)
Phase 1: In-Memory Core MVP (核心最小可行性產品)
目標：建立基於 Arrow 的記憶體資料結構，實現基本的時序陣列計算與 Temporal-CSR 圖走訪。

Memory & Data Structure

實現基於 Arrow RecordBatch 的 Node Table 與 Edge Table。

實現不可變 (Immutable) 的 In-Memory Temporal-CSR 結構，支援傳入 T 時間點進行切片。

kdb+ Style Array Primitive

實現 basic 向量化運算：asof join 算子 (基於時間戳對齊)。

實現滾動窗口 (Rolling Window) 聚合函數 (mavg, msum, deltas)。

Graph Traversal Operators

實作基本的 Vectorized k-hop 走訪算子，輸入為 UInt64Array 節點列表，輸出下一層鄰居。

Phase 2: Query Engine & Multi-Modal Integration (中層與查詢整合)
目標：整合 Apache DataFusion，引入向量檢索與持久化分層能力。

DataFusion Execution Engine Extension

將圖走訪與 asof join 註冊為 DataFusion 的 UserDefinedLogicalNode 與 ExecutionPlan。

實現 Cost-Based Optimizer (CBO) 規則：自動推導「時序過濾與圖走訪」的最佳執行優先級。

Vector Search Integration

整合 VectorIndex Trait，實現基於 HNSW 的內存向量索引。

支援帶有 Arrow Bitmask (Temporal Filter) 剪枝的向量 K-NN 檢索。

Persistence & Storage Tiering

實現 MemTable (Arrow) 到 Disk (Parquet) 的 Flush 機制。

實現基礎的 Time-Travel 查詢語意 (valid_from <= T < valid_to)。

Phase 3: Advanced Extensions & Enterprise Ecosystem (非核心/進階擴充)
目標：提供動態 UDF 沙盒、高級圖模式匹配與分佈式擴展能力。

Dynamic Sandbox & Plugin Architecture

整合 wasmtime 執行環境，允許使用者撰寫 Rust/C 編譯為 WASM 的自訂 Zero-Copy 算子。

Temporal Graph Pattern Matching (GQL/Cypher Lite)

實現 Temporal Pattern Matching 引擎，支援時序約束圖匹配（例如 T 
1
​
 <T 
2
​
 <T 
3
​
  的環狀轉帳鏈或菱形結構）。

LSM Dynamic Delta Buffer & Compaction

導入 Dynamic Delta Buffer (DashMap / BTreeMap) 以支援高頻動態拓撲異動。

實現背景 Thread Compaction，自動將 Delta 併入 Snapshot Temporal-CSR。

Distributed Node Communication

利用 tonic (gRPC) 實現跨節點的分佈式查詢分發與 Arrow Flight 資料傳輸。