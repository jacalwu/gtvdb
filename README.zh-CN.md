# gtvdb —— 資料庫界嘅超級武器「攞你命 3000」

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Language: Rust](https://img.shields.io/badge/Language-Rust-orange.svg)](https://www.rust-lang.org/)

> **「司令！經過我多年的研究，我終於研發出這個集四種超級功能於一身的單引擎資料庫——『攞你命 3000』！」**

普通人做系統，要開一個 Neo4j 睇圖拓撲，開一個 kdb+ 睇時序，再開一個 Milvus 做向量，最後仲要掛個 ClickHouse 做 OLAP 分析……**廢！極之廢！**

`gtvdb` 表面上係一個普通嘅單引擎 In-Memory 資料庫，**實際上——佢係一個將 Graph、Temporal、Vector 同埋 Columnar 四大武器完全融為一體嘅「攞你命 3000」！**

---

### 「攞你命 3000」四大核心武器組合：

1. **G (Graph 拓撲走訪)**：好似西瓜刀咁切開動態圖關係！Temporal-CSR 結構，支援時間旅行走訪，邊個時間點嘅關係都逃唔過你法眼！
2. **T (Temporal 時序分析)**：好似鐵鏈咁將時間線鎖得死死！kdb+ 風格 `asof join` 同滾動窗口算子，高頻數據秒級對齊！
3. **V (Vector 語意檢索)**：好似殺蟲水咁精準！HNSW 向量索引配合 Arrow Bitmask 條件過濾，一噴即中 K-NN 最相似目標！
4. **Columnar (Apache Arrow + DataFusion)**：火藥加毒藥！全 Zero-Copy 記憶體共享，SQL 查詢一出，所有數據喺記憶體瞬間溶化！

> **「每樣嘢單獨拎出嚟都已經獨當一面，但係集合埋喺同一個引擎裡面，問你死未？！」**

---

### 架構全貌（武器零件圖）

```text
crates/
├── gtv-core/    # Arrow 記憶體佈局與 Temporal-CSR（主體）
├── gtv-array/   # kdb+ 風格向量化陣列算子（asof / mavg / msum / deltas）
├── gtv-engine/  # DataFusion 整合：GtvContext、WindowUDF、table function
├── gtv-index/   # 向量檢索：FlatIndex（精確）+ HnswIndex（近似，bitmask 剪枝）
├── gtv-storage/ # Arrow ↔ Parquet + SnapshotStore（time-travel 多版本）
├── gtv-delta/   # LSM Delta Buffer + Compaction
├── gtv-pattern/ # 時序圖 Pattern Matching（path/diamond/ring）
├── gtv-udf/     # wasmtime 沙盒 UDF
└── gtv-cli/     # 互動式 SQL REPL（bin: gtv）
```

後續（P5）：tonic gRPC + Arrow Flight 分散式。

---

### SQL REPL（撳個掣即用）

```sh
cargo run -p gtv-cli --bin gtv
```

直接打 SQL 查 demo 表（`nodes`、`edges`、`prices`）；時間欄位係 `Int64` 奈秒，
邊嘅有效期係半開區間 `[valid_from, valid_to)`。

```sql
-- 時序切片：T = 150 仍生效嘅邊
SELECT src, dst FROM edges WHERE valid_from <= 150 AND 150 < valid_to;

-- kdb+ 風格窗口函數
SELECT t, mavg(price, 3) OVER (ORDER BY t) FROM prices;

-- 時序圖走訪
SELECT * FROM neighbors(0, 100);

-- as-of join 對齊價格序列
SELECT * FROM asof_join(0, 5, 15, 25, 35, 45, 55, 60);
```

除 SQL 之外亦有 shell 指令：

```text
knn <node> [k] [--mask a,b,c]  HNSW K-NN 向量檢索（可加 bitmask 過濾）
save <table> <path>            將表寫成 Parquet
load <table> <path>            載入 Parquet 成為新表
tt <table> <T>                 time-travel：讀取 T 時間點（或之前）嘅快照
pattern [T]                    時序圖 Pattern Matching（ring / path / diamond）
delta                          LSM Delta Buffer 插入 + Compaction 示範
udf [x ...]                    wasmtime 沙盒 UDF（x * 1.1）
```
