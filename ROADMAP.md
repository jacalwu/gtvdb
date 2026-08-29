# gtvdb — Temporal-Columnar Graph-Vector Engine Roadmap

單引擎資料庫：kdb+ 高頻陣列計算 + 圖拓撲走訪 + 向量檢索 + 列式 OLAP。

- [x] **P1 — In-Memory 核心**：Temporal-CSR + kdb+ 陣列算子（asof/mavg/msum/deltas）+ k-hop + 最小 CLI
- [x] **P2 — DataFusion 查詢引擎**：SQL REPL（sqlplus 風格）、mavg/msum/deltas 註冊為 WindowUDF、`neighbors()` 圖走訪與 `asof_join()` table function（UDLN/CBO 列為後續優化）
- [x] **P3 — 向量檢索 + 持久化**：HNSW（bitmask 剪枝）、MemTable→Parquet flush、time-travel
- [x] **P4 — 進階擴充**：wasmtime UDF、時序 Pattern Matching、LSM Delta + Compaction
- [x] **P5 — 分散式**：tonic gRPC 查詢派送 + Arrow IPC（Flight wire format）傳輸

## Workspace layout

```
crates/
├── gtv-core     # Arrow 資料結構、traits、Temporal-CSR、圖走訪
├── gtv-array    # kdb+ 風格向量化陣列算子（asof/mavg/msum/deltas）
├── gtv-engine   # DataFusion 整合：GtvContext、WindowUDF、neighbors table function
├── gtv-index    # 向量檢索：FlatIndex（精確）+ HnswIndex（近似，bitmask 剪枝）
├── gtv-storage  # Arrow ↔ Parquet + SnapshotStore（time-travel 多版本）
├── gtv-delta    # LSM Delta Buffer（memtable）+ Compaction（背景 thread）
├── gtv-pattern  # 時序圖 Pattern Matching（path/diamond/ring，事件時間排序）
├── gtv-udf      # wasmtime 沙盒 UDF（WAT→f64→f64 算子）
├── gtv-proto    # tonic gRPC 服務定義 + Arrow IPC 編解碼（client/server 共用）
├── gtv-server   # gRPC QueryService 端點（bin: gtv-server）
└── gtv-cli      # 互動式 REPL（bin: gtv，SQL enabled，含 remote 派送）
```
