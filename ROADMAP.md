# gtvdb — Temporal-Columnar Graph-Vector Engine Roadmap

單引擎資料庫：kdb+ 高頻陣列計算 + 圖拓撲走訪 + 向量檢索 + 列式 OLAP。

- [x] **P1 — In-Memory 核心**：Temporal-CSR + kdb+ 陣列算子（asof/mavg/msum/deltas）+ k-hop + 最小 CLI
- [x] **P2 — DataFusion 查詢引擎**：SQL REPL（sqlplus 風格）、mavg/msum/deltas 註冊為 WindowUDF、`neighbors()` 圖走訪與 `asof_join()` table function（UDLN/CBO 列為後續優化）
- [x] **P3 — 向量檢索 + 持久化**：HNSW（bitmask 剪枝）、MemTable→Parquet flush、time-travel
- [ ] **P4 — 進階擴充**：wasmtime UDF、時序 Pattern Matching、LSM Delta + Compaction
- [ ] **P5 — 分散式**：tonic gRPC + Arrow Flight

## Workspace layout

```
crates/
├── gtv-core    # Arrow 資料結構、traits、Temporal-CSR、圖走訪
├── gtv-array   # kdb+ 風格向量化陣列算子（asof/mavg/msum/deltas）
├── gtv-engine  # DataFusion 整合：GtvContext、WindowUDF、neighbors table function
├── gtv-index   # 向量檢索：FlatIndex（精確）+ HnswIndex（近似，bitmask 剪枝）
├── gtv-storage # Arrow ↔ Parquet + SnapshotStore（time-travel 多版本）
└── gtv-cli     # 互動式 REPL（bin: gtv，SQL enabled）
```
