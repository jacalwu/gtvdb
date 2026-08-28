gtvdb —— 資料庫界嘅超級武器「攞你命 3000」
「司令！經過我多年的研究，我終於研發出這個集十種功能於一身的超級資料庫——『攞你命 3000』！」

普通人做系統，要開一個 Neo4j 睇圖拓撲，開一個 kdb+ 睇時序，再開一個 Milvus 做向量，最後仲要掛個 ClickHouse 做 OLAP 分析……廢！極之廢！

gtvdb 表面上係一個單引擎 In-Memory 資料庫，實際上——佢係一個將 Graph、Temporal、Vector 同埋 Columnar 四大武器完全融為一體嘅「攞你命 3000」！

「攞你命 3000」四大核心武器組合：
G (Graph 拓撲走訪)：好似西瓜刀咁切開動態圖關係！Temporal-CSR 結構，支援時間旅行走訪，邊個時間點嘅關係都逃唔過你法眼！

T (Temporal 時序分析)：好似鐵鏈咁將時間線鎖得死死！kdb+ 風格 asof join 同滾動窗口算子，高頻數據秒級對齊！

V (Vector 語意檢索)：好似殺蟲水咁精準！HNSW 向量索引配合 Arrow Bitmask 條件過濾，一噴即中 K-NN 最相似目標！

Columnar (Apache Arrow + DataFusion)：火藥加毒藥！全 Zero-Copy 記憶體共享，SQL 查詢一出，所有數據喺記憶體瞬間溶化！

「每樣嘢單獨拎出嚟都已經獨當一面，但係集合埋喺同一個引擎裡面，問你死未？！」

架構全貌（武器零件圖）
Plaintext
gtvdb/
├── gtvdb-core/       # Arrow 記憶體佈局與 Temporal-CSR（主體）
├── gtvdb-query/      # DataFusion 查詢優化器（開關）
├── gtvdb-index/      # HNSW 向量與時序索引（瞄準器）
├── gtvdb-storage/    # LSM Dynamic Delta & Parquet 分層（彈藥庫）
└── gtvdb-plugin/     # WASM 零拷貝 UDF 沙盒（附加配件）
快速上手（攞起即用）
喺你嘅 Cargo.toml 加入零件：

Ini, TOML
[dependencies]
gtvdb-core = "0.1.0"
gtvdb-query = "0.1.0"
一行 Rust 程式碼召喚「攞你命 3000」：

Rust
use anyhow::Result;
use gtvdb_core::TemporalGraphIndex;
use gtvdb_index::VectorIndex;

#[tokio::main]
async fn main() -> Result<()> {
    // 1. 向量搜尋（標定目標）
    let query_vector = vec![0.12, -0.45, 0.89, 0.33];
    let seed_nodes = vector_index.search_knn(&query_vector, 10, None)?;

    // 2. 時序圖走訪（時間鎖定）
    let valid_at_timestamp = 1756420000000;
    let neighbors = graph_index.fetch_temporal_neighbors(&seed_nodes, valid_at_timestamp)?;

    // 3. 輸出結果（問你死未）
    println!("成功擷取 {} 個時序鄰居點！", neighbors.num_rows());
    Ok(())
}