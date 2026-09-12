# prod_p1 — 第一批：Kernel 快速改進（無 catalog 依賴）

> 上層路線圖：`BANKING_ANALYTICS_GAP_ROADMAP.md`
> 設計文件：`doc/prod_p1_design.md`
> **命名注意**：本文嘅 `p1` = 「第一批（Phase/Batch 1）」，**唔係** roadmap 嘅優先級 P1。
> 每項任務用 `B1-n` 編號，並註明對應 roadmap 編號。

---

## 0. 這批為何先做

呢批全部係**現有 kernel 內部改動**，唔依賴 catalog / manifest / streaming：

- 立即修補正確性同安全問題（metric 語意、AML 圖查詢會 OOM）；
- 消掉代碼裡已標記嘅 `TODO(P1)`；
- 為第二、三批（recall oracle、index lifecycle、filtered ANN）打底；
- 可同第二批（catalog）**並行開發**，互不阻塞。

**前置條件**：無。**預計總工期**：約 3–4 週（單人；HNSW 重構佔大部分）。

---

## 1. 任務總覽

| ID | 任務 | Roadmap | 主要檔案 | 工期 | 依賴 |
|---|---|---|---|---|---|
| B1-1 | Distance metric contract（L2 / Cosine / Dot） | P0.5 | `gtv-core`, `gtv-index`, `gtv-engine/knn.rs` | 2–3 日 | — |
| B1-2 | TemporalCSR 自適應時間索引 | P1.1 | `gtv-core/src/csr.rs` | 3–5 日 | — |
| B1-3 | AML / 圖走訪安全與資源預算 | P1.2 | `gtv-core/src/csr.rs`, `gtv-pattern` | 4–6 日 | B1-2（共用 chunk/degree） |
| B1-4 | HNSW 連續記憶體佈局重構 | P1.3 | `gtv-index/src/hnsw.rs` | 1.5–2.5 週 | B1-1（metric） |

> B1-2 同 B1-3 應順序做（B1-3 重用 B1-2 嘅 degree/chunk 結構）。B1-1 同 B1-4
> 可並行。B1-4 應最後做，因為佢會鎖定 index 序列化格式，供第二批生命週期使用。

---

## 2. B1-1 Distance metric contract

**問題**：`cosine`/`dot`/`DistanceMetric` 全 repo 零命中；三個索引各自硬編碼 `squared_l2`。
銀行 embedding 場景必需 Cosine；現時完全唔支援。

**交付物**

1. `gtv-core/src/metric.rs`（新）：
   - `pub enum Metric { L2, Cosine, Ip }`
   - `Metric::distance(a,b) -> f32`（**統一「越小越近」**；Ip 內部用負內積，見設計文件）。
   - `Metric::requires_normalization()`、`Metric::from_str()`（SQL 層解析）。
   - `DistanceMetric` 型別 alias（兼容 roadmap 叫法）。
2. `gtv-core/src/traits.rs`：
   - 新增 `pub struct VectorHit { pub id: u64, pub distance: f32 }`。
   - `VectorIndex` 加 `fn metric(&self) -> Metric` 同
     `fn search(&self, query, k, filter) -> Result<Vec<VectorHit>>`；
     `search_knn`（回 `UInt64Array`）保留為 default method，不破壞現有 caller。
3. `gtv-core/src/error.rs`：新增 `MetricMismatch { index: Metric, query: Metric }`、
   `DimensionMismatch { index: usize, query: usize }`。
4. `gtv-index/src/{flat,hnsw,ivf}.rs`：索引建構時鎖定 `metric` 及 `dim`；
   `search()` 依 metric 分派 kernel；Cosine 於 build/insert 做 normalization（記錄 policy）。
5. `gtv-engine/src/knn.rs`：`KnnCollection` 加 `metric` / `dim` / `normalized` metadata；
   查詢 metric ≠ 索引 metric → 明確拒絕。
6. `gtv-cli`：`knn` shell 命令加 `--metric l2|cosine|dot`。

**驗收條件**

- [ ] Flat / HNSW / IVF 三者對同一 corpus + metric，Top-K 與獨立參考實作一致。
- [ ] Cosine：(a) 已 normalize 輸入 vs (b) 未 normalize 輸入，結果一致（policy 生效）。
- [ ] 查詢 metric 與索引不符 → 回 `MetricMismatch`，唔係靜默用錯 metric。
- [ ] dimension 不符 → 回 `DimensionMismatch`。
- [ ] HNSW/Ivf 現有 L2 測試**零回歸**。
- [ ] 對 Ip 索引，文件明確記錄「非 metric、ANN recall 可能下降」及緩解方式。

**風險**

- **Ip 唔係 metric**（違反三角不等式）：HNSW/IVF 對 MIPS 嘅 recall 會退化，需在
  設計文件寫清楚緩解（normalize 成 cosine，或 norm-augmentation 化為 L2）。
- distance 語意方向：必須全系統統一「lower = closer」，否則排序會反。

---

## 3. B1-2 TemporalCSR 自適應時間索引

**問題**：`crates/gtv-core/src/csr.rs` 嘅 `neighbors()` 對每個 source 嘅 run
**線性全掃**，代碼自己寫住 `TODO(P1): the per-node run is scanned linearly`。
高 degree 節點（銀行圖常見）會退化成 O(degree)。

**交付物**

1. `csr.rs` 新增 degree 資訊：`degree(src) -> u32`、`degree_histogram() -> Vec<u64>`、
   `max_degree()`。
2. **chunk zone map**：每個 source run 再切 chunk（預設 64 edges），
   記錄每 chunk `max_valid_to`；掃描時 `max_valid_to <= T` 嘅 chunk 直接跳過。
3. **binary search**：run 已按 `(valid_from, valid_to, dst)` 排序 →
   對 `valid_from` 二分取 `valid_from <= T` 上界，再 SIMD 驗 `valid_to > T`。
4. **degree-based 策略分派**（`NeighborStrategy`）：
   - `degree < 16`：linear scan（現狀，最快）。
   - `16 ≤ degree < 4096`：binary search + chunk zone map。
   - `degree ≥ 4096`：per-source time bucket 索引（按 `valid_from` 分桶 + 每桶 zone map）。
   - 極高 degree（可配置門檻）：標記為 parallel-friendly，走 rayon 並行掃描。
5. `csr.rs` 統計：`TemporalCsrStats { degree_histogram, active_ratio, strategy_counts }`，
   經 `metrics` 命令輸出。
6. `neighbors()` API **簽名不變**（對外兼容）；新增 `neighbors_planned()` 回傳
   所用策略（供 telemetry 同測試）。

**驗收條件**

- [ ] 隨機 temporal edge 資料集上，`neighbors()` 結果與線性 oracle **逐位元一致**。
- [ ] 高 degree（≥ 100k edges/node）查詢延遲較 baseline 下降 ≥ 10×（單點 T 中介）。
- [ ] `all_active_at(t)` fast path 保留且仍生效。
- [ ] `khop` 正確性測試零回歸。
- [ ] 提供 degree histogram benchmark（`cargo bench` 或 example）。

**風險**

- run 內 `valid_to` 非單調，binary search 只可剪 `valid_from`；靠 chunk zone map
  剪已過期區段——兩者必須一齊用先有明顯收益。
- 新增 chunk metadata 會增加 build 記憶體（每 64 edges 一個 i64），需量度。

---

## 4. B1-3 AML / 圖走訪安全與資源預算

**問題**：`csr.rs::khop` 每個 hop `sort + dedup`，冇全域 visited、冇任何上限。
高 degree + k 大 = 記憶體爆炸 / 尾延遲不可預測；`gtv-pattern` DFS 亦無預算。

**交付物**

1. **全域 visited**：`VisitedSet`——generation-stamped `Vec<u32>`（4 bytes/node，
   `reset()` 只需 bump generation，O(1) 清空），免用 HashSet。
2. **資源預算**：`TraversalBudget { max_hops, max_edges, max_frontier, max_rows,
   max_memory_bytes, deadline: Option<Instant> }`；
   超出 → `GtvError::BudgetExceeded { stage, limit, observed }`（新增 error variant）。
3. **取消機制**：`cancel: Arc<AtomicBool>`，每 N 個 edge 檢查一次。
4. **direction-optimizing BFS**：dense frontier 走 pull（需 reverse CSR）。
   新增 `TemporalCSR::transpose()`（可選、lazy build）。
5. **high-degree node guard**：degree 超門檻嘅節點，若無 predicate 縮減則拒絕或告警。
6. **predicate pushdown**：edge filter closure（amount / currency / channel /
   jurisdiction / edge_type / time），喺展開 frontier 之前過濾。
7. **確定性排序**：輸出 frontier 一律按 node id 升序（或明確定義嘅順序）。
8. `csr.rs`：`khop_bounded(seeds, k, valid_at, budget, filter, cancel) -> KhopResult`；
   `KhopResult { frontiers, stats }`。
9. `gtv-pattern/src/lib.rs`：`find()` 接同一個 budget / visited / cancel。
10. SQL / CLI 暴露 `khop(src, k, valid_at [, max_hops, max_edges])` table function。

**驗收條件**

- [ ] 對「高 degree 稠密圖」查詢，記憶體有硬上限，超出回 `BudgetExceeded` 而非 OOM。
- [ ] 同一查詢 + 同一預算，多次執行結果完全一致（確定性）。
- [ ] timeout / cancel 可喺毫秒級中斷長查詢。
- [ ] predicate pushdown 生效時，掃描 edge 數／記憶體顯著下降（量度）。
- [ ] `khop` 預設行為（無預算）向後兼容，現有測試零回歸。
- [ ] `pattern` ring/path/diamond 結果零回歸。

**風險**

- reverse CSR 會增加記憶體；設計為 lazy + 可選。
- predicate pushdown 要避免破壞現有 `pattern` 嘅時間排序語意。

---

## 5. B1-4 HNSW 連續記憶體佈局重構

**問題**：`crates/gtv-index/src/hnsw.rs` 用 `Node { vector: Vec<f32>,
layers: Vec<Vec<usize>> }`，即每個 node 多次 heap allocation + pointer chasing，
cache locality 差——正是 roadmap P1.3 點名嘅反模式。且無批次／並行查詢、無
tombstone、無 recall telemetry。

**交付物**

1. **連續佈局**：
   - `vectors: Vec<f32>`（n×dim row-major）
   - `ids: Vec<u64>`
   - CSR-style layer neighbour 區塊：`node_offset[i]` + 每層固定 stride
     （level 0 用 `m0`，其餘用 `m`）；`neighbours: Vec<u32>`。
   - `levels: Vec<u8>`、`entry: u32`、`max_level: u8`。
2. **generation-stamped visited**：查詢用 scratch（`Vec<u32>` + gen counter），
   零 allocation、支援並行。
3. **min-heap candidates + bounded max-heap results**。
4. **批次 / 並行查詢**：`search_batch(&[&[f32]], k, filter)`（rayon，每 thread 自帶 scratch）。
5. **tombstone**：`deleted` bitmap + `tombstone_count()`；search 跳過；
   compaction/rebuild 於比例超門檻觸發。
6. **dynamic ef_search**：`search_with_ef(query, k, ef, filter)`。
7. **filter-aware routing 介面**：接受 `BooleanArray` bitmask；selective filter
   交給 B3-3 策略（本批只留 hook）。
8. **recall / latency telemetry**：`SearchReport { candidates_visited, filtered,
   exact_distance }`。
9. **版本化序列化**：`to_bytes()` / `from_bytes()`（versioned header + flat payload），
   供第二批 index lifecycle 直接用。
10. `HnswIndex::build(ids, Vec<Vec<f32>>, ...)`、`insert`、`VectorIndex` impl
    全部**保持兼容**。

**驗收條件**

- [ ] 對隨機資料，recall@10 對 FlatIndex oracle **不低於**重構前（現有測試必須過）。
- [ ] 記憶體 bytes/vector 較重構前明顯下降（目標 ≥ 2×，需 benchmark 前後對比）。
- [ ] 批次查詢結果與逐條查詢**完全一致**（確定性）。
- [ ] tombstone 後 search 不再回傳已刪 id；compaction 後索引仍正確。
- [ ] `to_bytes` → `from_bytes` round-trip 後 recall 與 latency 一致。
- [ ] `ef_search` 動態調整可量度 recall/latency 曲線。

**風險**

- HNSW 佈局改動面大，容易引入 subtle recall 回歸；必須用 FlatIndex oracle
  做 property test。
- 序列化格式一旦定下，第二批 lifecycle 依賴佢，需及早 freeze 版本號。

---

## 6. 本批完成定義（Definition of Done）

- [ ] B1-1 ~ B1-4 全部驗收條件通過。
- [ ] `cargo test --workspace` 全綠；`cargo clippy` 無新 warning。
- [ ] 新增 benchmark（`examples/` 或 `benches/`）：metric、CSR 策略、遍歷預算、HNSW 記憶體。
- [ ] 文件更新：`doc/` 設計文件、`user-menu-cn/en.md` 若有新 SQL/CLI 表面。
- [ ] 對外 API 向後兼容（舊測試零回歸）。

---

## 7. 明確唔喺本批做

- Catalog / manifest / schema evolution（→ 第二批 B2-1）
- 向量索引持久化 / shadow swap（→ 第二批 B2-2）
- Execution lineage / replay（→ 第二批 B2-3）
- Embedding table 完整 schema（→ 第二批 B2-4）
- Filtered ANN 策略分派、exact rerank、IVF k-means、CBO（→ 第三批）
- Streaming ingestion、bitemporal（→ 第三批）
