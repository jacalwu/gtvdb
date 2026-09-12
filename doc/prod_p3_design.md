# prod_p3 設計文件 — 大型能力（Streaming / Bitemporal / ANN 進階）

> 對應批次清單：`prod_p3.md`
> 上層路線圖：`BANKING_ANALYTICS_GAP_ROADMAP.md`
> 範圍：B3-1 Streaming、B3-2 Bitemporal、B3-3 Filtered ANN + rerank、
> B3-4 IVF k-means、B3-5 CBO、B3-6 Workload management。
> 前置：`prod_p2`（catalog / index lifecycle / lineage）已完成並穩定。

---

## 1. 總覽

第三批分兩條線，喺 B2-1 catalog 之上並行：

```
資料線：B3-1 Streaming ──> B3-2 Bitemporal ──┐
                                            ├──> B3-5 CBO ──> B3-6 Workload
ANN 線：B3-4 IVF k-means ──> B3-3 Filtered ANN ┘
```

- **資料線**改 catalog schema + 核心時間模型，風險高。
- **ANN 線**改 `gtv-index` / `gtv-engine`，相對獨立。
- B3-5 / B3-6 要兩條線都就緒。

---

## 2. B3-1 Streaming ingestion contract

### 2.1 新 crate 結構

```
crates/gtv-ingest/
├── Cargo.toml          # gtv-catalog, arrow, rdkafka(optional), tokio, serde
└── src/
    ├── lib.rs
    ├── envelope.rs     # Envelope
    ├── source.rs       # SourceAdapter trait
    ├── kafka.rs        # Kafka adapter (feature "kafka")
    ├── file.rs         # file replay adapter (always on, for tests/replay)
    ├── offsets.rs      # committed offset store (via gtv-catalog)
    ├── dedup.rs        # event_id dedup
    ├── watermark.rs    # watermark + late policy
    ├── dlq.rs          # dead-letter queue
    ├── batch.rs        # micro-batch executor
    └── error.rs
```

```crates/gtv-stream/
└── src/lib.rs          # micro-batch scheduler, backpressure, health
```

> Kafka/Pulsar 用 feature gate（`kafka` / `pulsar`），預設 build 唔會拖慢編譯
> （對應 workspace `Cargo.toml` 嘅 build-time tuning 原則）。

### 2.2 Envelope

```rust
pub struct Envelope {
    pub source: String,
    pub partition: i32,
    pub offset: i64,
    pub event_id: [u8; 16],      // uuid / hash，用作 dedup key
    pub event_time: i64,         // ns
    pub ingest_time: i64,        // ns
    pub schema_version: u32,
    pub payload: bytes::Bytes,   // 原始 bytes（Arrow IPC / JSON / Avro）
}
```

### 2.3 SourceAdapter

```rust
pub trait SourceAdapter: Send {
    fn name(&self) -> &str;
    /// Poll up to `max` events (non-blocking / short timeout).
    fn poll(&mut self, max: usize) -> Result<Vec<Envelope>>;
    /// Commit offsets after a successful atomic publish.
    fn commit(&mut self, offsets: &[PartitionOffset]) -> Result<()>;
    /// Low watermark per partition (for lag metrics).
    fn lag(&self) -> Vec<PartitionLag>;
}
```

實作：

- `KafkaAdapter`（`rdkafka`，手動 commit offset）
- `PulsarAdapter`（可選）
- `FileReplayAdapter`（讀 Parquet/JSONL，重演用 + 測試；**always on**）

### 2.4 Offset store

```rust
pub struct PartitionOffset { pub source: String, pub partition: i32, pub offset: i64 }
```

- 存於 `gtv-catalog`：`metadata/offsets/<source>.jsonl`（append-only）。
- **關鍵**：offset 必須同 micro-batch 資料**同一 atomic commit** 一齊寫，
  否則會重複或漏。做法：offset list 放入 `Snapshot.summary`（B2-1 已支援
  `summary: serde_json::Value`），並喺 commit 後先 `adapter.commit()`。

### 2.5 Micro-batch 原子發布

```
loop:
  envs = source.poll(max_batch)
  if envs.empty: sleep(backoff); continue
  envs = dedup.filter(envs)                      # 去重
  {on_time, late} = watermark.split(envs)        # 水位線
  batch = encode_arrow(envs.on_time)
  # 經 B2-1 atomic commit 寫入 delta/分區，offset 記入 snapshot.summary
  snap = catalog.commit(table, Append, [batch_file],
                        summary = { "source_offsets": offsets })
  source.commit(offsets)                         # 資料已持久化先 commit offset
  handle_late(late)                              # 重算 or DLQ
```

- 未完成 commit → 資料不可見、offset 未 commit → 重啟重播（at-least-once）。
- event_id dedup 令重播冪等（effectively-once）。

### 2.6 去重（dedup）

```rust
pub struct DedupStore {
    /// windowed per (source, partition)：最近 N 個 event_id。
    windows: HashMap<(String, i32), RingBloomOrRoaring>,
    /// 持久層：最近 committed window 嘅 event_id（供重啟）。
    persistent: Box<dyn DedupPersist>,
}
```

- 記憶體用 bloom（誤判率可配）或 roaring bitmap（event_id 做 hash）。
- 持久層：`metadata/dedup/<source>/<partition>.bin`，隨 commit 更新。
- 對「精確去重」要求高嘅場景，用 hash set（成本換正確）。

### 2.7 Watermark / late policy

```rust
pub struct WatermarkConfig {
    pub allowed_lateness_ns: i64,
    pub policy: LatePolicy,     // Recompute | Dlq | Drop
}
```

- `watermark = max_event_time_seen - allowed_lateness`。
- `event_time < watermark` → late event。
- `Recompute`：觸發受影響時間窗重算（依賴 catalog snapshot / delta）。
- `Dlq`：寫入 `deadletter/`。
- `Drop`：只計數（不建議銀行場景）。

### 2.8 DLQ

```
deadletter/<source>/<date>/<partition>.parquet
  schema: envelope 欄位 + error_code + error_message + rejected_at
```

- CLI/SQL：`dlq_list(source)` / `dlq_reprocess(source, date)`。
- reprocess 走同一 micro-batch 管線，成功後可標記 resolved。

### 2.9 Backpressure / health

```rust
pub struct StreamConfig {
    pub max_batch: usize,
    pub max_inflight_batches: usize,
    pub poll_interval_ms: u64,
    pub rate_limit_eps: Option<u64>,
}
```

- Bounded channel；inflight 滿 → 暫停 poll（backpressure）。
- Health：每 source `lag`、last_poll_ts、error count。
- Metrics（B3-6 / `gtv-observe`）：end-to-end lag、event-time lag、dropped、offset lag。

### 2.10 驗收對應

| 驗收 | 設計支撐 |
|---|---|
| 重啟由 committed offset 續 | §2.4 offset 同 commit 綁定 |
| 重播不重複 | §2.6 event_id dedup |
| 未完整 batch 不可見 | B2-1 atomic commit |
| 量度 lag | §2.3 `lag()` + §2.9 metrics |
| late event 可審計 | §2.7 / §2.8 |

### 2.11 測試

- FileReplayAdapter 重播同一批 100 次 → 結果完全一致（冪等）。
- 隨機 crash（kill -9）後重啟 → 無重複、無漏（用 counts + checksum 比對）。
- Watermark 邊界：剛好 late / 剛好 on-time。
- DLQ：壞 event 入 DLQ 且可 reprocess。
- Backpressure：來源高速時記憶體有上限。

---

## 3. B3-2 Bitemporal 時間模型

### 3.1 核心型別（`gtv-core`）

```rust
pub struct BitemporalRange {
    pub business_from: i64,
    pub business_to: i64,     // exclusive
    pub system_from: i64,
    pub system_to: i64,       // exclusive
}
```

### 3.2 表 schema 擴充

現有 edge table（`gtv-core/src/table.rs`）：

```
src, dst, edge_type, valid_from, valid_to
```

新增（bitemporal）：

```
business_valid_from, business_valid_to,   // 業務有效
system_valid_from,   system_valid_to,     // 系統知悉
event_time, ingest_time, business_date
```

- `valid_from/valid_to`（legacy）→ 映射為 `business_valid_from/to`（兼容 view）。
- Node table 同樣擴充。

### 3.3 索引策略（關鍵決定）

**唔可以同時喺 CSR 索引兩個時間軸**（結構會爆炸）。設計：

- **CSR 只索引 business time**：`TemporalCSR` 嘅 `valid_from/valid_to` 改為
  business 語意（B1-2 嘅自適應索引繼續有效）。
- **System time 由 catalog snapshot 版本處理**：
  - 每次系統寫入 = 新 `SnapshotId`（B2-1）。
  - 每個 snapshot 對應一份 immutable table / CSR version。
  - `AS OF SYSTEM TIME ts` = 揀「該 ts 之前最新嘅 snapshot」再讀。

```
AS OF SYSTEM TIME ts  -> catalog.snapshot_as_of(table, ts)  -> 讀該 version
AS OF BUSINESS TIME t -> 喺該 snapshot 嘅 CSR 上做 temporal query
兩者合用             -> 先揀 system snapshot，再喺其 CSR 上查 business time
```

### 3.4 SQL 語意

```sql
-- 例子（語法待 DataFusion parser 評估）
SELECT * FROM edges AS OF BUSINESS TIME 1700000000000000000;
SELECT * FROM edges AS OF SYSTEM TIME   '2024-01-15T10:00:00Z';
SELECT * FROM edges AS OF SYSTEM TIME '2024-01-15T10:00:00Z'
                    AS OF BUSINESS TIME 1700000000000000000;
```

或 table function fallback（較易實作）：

```sql
SELECT * FROM as_of('edges', business_ts := 1700..., system_ts := 1700...);
```

### 3.5 更正流程

- 更正 = append 新 system version（新 snapshot），**永不覆寫**。
- 舊 snapshot 永久保留（retention 由 P3 tiering 管）。
- Bitemporal overlap 檢查（B2-5 DQ 規則）：
  同一 entity 喺同一 business 區間唔可以有兩個互相矛盾嘅 system version。

### 3.6 遷移

- 舊單時間軸資料：`valid_from/to` → `business_valid_from/to`；
  `system_from = created_at`、`system_to = i64::MAX`。
- 提供兼容 view，舊查詢結果不變。

### 3.7 測試

- `AS OF SYSTEM TIME` 回「該時點所知」資料。
- 更正後舊 system snapshot 查詢結果不變。
- 兩軸獨立：business 切片 + system 切片組合正確。
- 舊資料遷移後結果不變。
- Overlap 檢測。

---

## 4. B3-3 Filter-aware ANN + exact rerank

### 4.1 策略

```rust
pub enum AnnStrategy {
    PreFilterExact,     // selective filter：先 bitmap，再 Flat/IVF 精確掃
    FilteredIvf,        // 只探含允許 id 嘅 cell
    OversampledHnsw,    // HNSW + 放大 ef，再 filter
    PostFilterRerank,   // 一般 ANN，再 metadata filter + rerank
    Exact,              // 監管 / 高風險強制精確
}

pub struct AnnPlan {
    pub strategy: AnnStrategy,
    pub oversample: usize,
    pub exact_rerank: bool,
    pub reason: String,
}
```

### 4.2 選擇率分派（planner）

```
selectivity = estimated_allowed / total          # 來自 catalog stats (B3-5)

sel < 0.01                 -> PreFilterExact
0.01 <= sel < 0.20         -> FilteredIvf 或 OversampledHnsw（揀成本低者）
sel >= 0.20                -> OversampledHnsw / PostFilterRerank
regulatory / force_exact   -> Exact
```

- `FilteredIvf`：只掃含允許 id 嘅 cell（cell 內仍精確 f32）。
- `OversampledHnsw`：`ef = clamp(k / sel * safety, k, ef_max)`。
- `PostFilterRerank`：ANN 取 `K * oversample`，過濾後可能不足 k → 再放大或 fallback exact。

### 4.3 Exact rerank

```rust
pub struct RerankResult {
    pub hits: Vec<RerankedHit>,
    pub approx_scores: Vec<f32>,   // ANN 分數
    pub exact_scores: Vec<f32>,    // 原始 f32 精確分數
}
pub struct RerankedHit { pub id: u64, pub approx: f32, pub exact: f32 }
```

- 對 ANN 候選用原始 `f32` embedding 精確計 `metric.distance`。
- 同時套 metadata / temporal / entity filter。
- `Exact` 模式 = FlatIndex（oracle）。

### 4.4 Telemetry / recall

```rust
pub struct AnnTelemetry {
    pub strategy: AnnStrategy,
    pub candidate_count: u64,
    pub filtered_count: u64,
    pub recall_estimate: Option<f64>,
    pub latency_breakdown: LatencyBreakdown,  // filter / ann / rerank
}
```

- Recall estimate：抽樣查詢對 FlatIndex exact 比對（`sample_rate` 可配）。
- 每個查詢可輸出 telemetry（經 `EXPLAIN` 或 metrics）。

### 4.5 整合

- `gtv-engine` knn UDTF：先算 selectivity（catalog stats），選 `AnnPlan`，再執行。
- `VectorIndex` trait（B1-1）已回 `VectorHit`，足夠 rerank。

### 4.6 測試

- 三種 selectivity 情境，自適應優於固定策略（latency / recall 量測）。
- `Exact` == FlatIndex。
- Telemetry 欄位齊全。
- Recall@K 回歸 baseline。
- filter + temporal 同時套用正確。

---

## 5. B3-4 IVF k-means coarse quantizer

### 5.1 現況

`ivf.rs:72-76`：均勻取樣 centroid，無 k-means、無空 cell 處理。

### 5.2 演算法

```rust
pub struct KMeansConfig {
    pub nlist: usize,
    pub max_iters: usize,       // 預設 25
    pub restarts: usize,        // 預設 3
    pub sample: Option<usize>,  // 訓練抽樣
    pub seed: u64,              // deterministic
}
pub fn kmeans_train(data: &[f32], n: usize, dim: usize, cfg: &KMeansConfig)
    -> (Vec<f32>, Vec<u32>);   // centroids, assignments
```

- **k-means++ init**（seeded RNG，沿用 `SplitMix64` 風格）。
- Lloyd 迭代至收斂或 `max_iters`。
- **multiple restarts**：取最低 inertia。
- **empty cell**：重指派最遠點（或最差 inertia 點）。
- **oversized cell split**：cell 人口 > μ + kσ → 切分。
- 訓練可用 sample（大 corpus）。

### 5.3 cell 統計 / retrain trigger

```rust
pub struct CellStats { pub counts: Vec<u32>, pub min: u32, pub max: u32, pub mean: f64, pub std: f64 }
pub enum RetrainTrigger { None, PopulationImbalance { ratio: f64 }, CorpusDrift { ... }, RecallBelow { target: f64 } }
```

### 5.4 nlist / nprobe 自動調優

- 對抽樣 query 掃 `(nlist, nprobe)` 組合，量 Recall@K vs latency，
  揀符合 target recall 嘅最低成本組合。
- 結果記入 index manifest `build_params`。

### 5.5 兼容

- 保留舊均勻取樣做 `IvfIndex::build_uniform`（對照 / fallback）。
- `IvfIndex` 序列化（B2-2）新增 centroid 訓練 metadata。

### 5.6 測試

- 不均衡 corpus：k-means recall > uniform recall。
- 同 seed + 資料 → 相同 centroids。
- 無空 cell；oversized 可切。
- 調優曲線單調。
- 現有 IVF 測試零回歸。

---

## 6. B3-5 多模態 cost-based optimizer

### 6.1 統計（放 `gtv-catalog`）

```rust
pub struct TableStats {
    pub row_count: u64,
    pub column: HashMap<String, ColumnStat>,   // distinct / null / min / max
    pub partitions: Vec<PartitionStat>,
}
pub struct GraphStats {
    pub degree_histogram: Vec<u64>,
    pub temporal_active_ratio: f64,
}
pub struct VectorStats {
    pub corpus_size: u64,
    pub cell_distribution: Vec<u32>,     // IVF
    pub recall_latency_curve: Vec<(usize, f64, f64)>, // (ef/nprobe, recall, latency)
}
pub struct SelectivityStats { /* filter 估算 */ }
```

- 統計由 B2-1 `column_stats` + B3-4 cell stats 匯總。
- 隨 catalog commit 更新（避免用過期 stats）。

### 6.2 成本模型決策

```
- 先 SQL filter 定先 ANN？
- Flat / IVF / HNSW？
- temporal filter 是否先產 bitmap？
- graph expansion 是否先裁剪 source nodes？
- 是否 exact rerank？
```

```rust
pub struct CostModel { /* weights */ }
pub struct QueryPlanChoice {
    pub filter_first: bool,
    pub index_type: IndexType,
    pub temporal_bitmap_first: bool,
    pub prune_graph_sources: bool,
    pub exact_rerank: bool,
    pub estimated_cost: f64,
}
```

### 6.3 DataFusion 整合

- 實作 `Statistics` provider（由 `TableStats` 提供）。
- 實作 custom `PhysicalOptimizerRule` / `ExtensionPlanner`，喺 plan 階段
  注入 `AnnPlan` / 執行策略。
- 可配置開關：`gtv.optimizer.multimodal = on|off`（off = 固定策略 fallback）。

### 6.4 EXPLAIN

```sql
EXPLAIN SELECT ... ;
-- 輸出：chosen strategy + estimated cost + rationale
```

### 6.5 測試

- `EXPLAIN` 顯示策略 + 成本。
- 3 個代表查詢（純向量 / filter+向量 / 圖+向量）選中合理策略。
- 對比固定策略整體改善。
- 統計更新後 plan 隨之改變。

---

## 7. B3-6 Workload management / isolation

### 7.1 Workload classes

```rust
pub enum WorkloadClass { Ingestion, InteractiveAml, RiskBatch, AlmBatch, FtpBatch, IndexBuild }
```

### 7.2 Resource group

```rust
pub struct ResourceGroup {
    pub class: WorkloadClass,
    pub cpu_quota: f64,          // 相對份額
    pub max_concurrency: usize,
    pub memory_limit_bytes: usize,
    pub io_limit_bps: Option<u64>,
    pub priority: u8,
}
```

### 7.3 Admission control

```rust
pub enum Admission { Admit, Queue { position: usize }, Reject { reason: String } }
```

- 超載：`InteractiveAml` 優先；batch 可排隊或拒絕。
- 拒絕附明確錯誤（配合 B2-3 execution record）。

### 7.4 Preemption

- 重用 B1-3 `CancelToken`。
- 低優先級查詢可被取消；取消記錄入 lineage。

### 7.5 Spill-to-disk

- 大 sort / join 超記憶體 → 落 `catalog_root/tmp/spill/`。
- spill bytes 上報 metrics。

### 7.6 Observability（`gtv-observe`）

最少提供：ingestion lag、watermark lag、offset lag、query p50/p95/p99、
rows/edges/vectors scanned、cache hit、partition pruning ratio、
ANN candidate count、Recall@K sample、graph frontier size、spill bytes、
index build duration、DQ pass rate、scenario batch completion。

- 可由 `gtv-engine/src/monitor.rs`（已有 `prometheus_text`）擴充；
  **唔一定開新 crate**（避免 crate  sprawl）。

### 7.7 測試

- 混合負載壓測：interactive P99 達 SLO。
- index build 唔影響 interactive（隔離量測）。
- 超載 admission 決策可觀測。
- preemption 即時取消。

---

## 8. 交付順序

| 里程碑 | 內容 | 出口 |
|---|---|---|
| M1 | B3-4 IVF k-means | recall > uniform；可重現 |
| M2 | B3-3 filtered ANN + rerank | 自適應優於固定；Exact == Flat |
| M3 | B3-1 streaming | 重播冪等；crash 後無重複無漏 |
| M4 | B3-2 bitemporal | 兩軸可查；更正不覆寫 |
| M5 | B3-5 CBO | EXPLAIN 選路合理 |
| M6 | B3-6 workload | 混合負載 SLO 達標 |

## 9. 風險登記

| 風險 | 影響 | 緩解 |
|---|---|---|
| Kafka/Pulsar 依賴拖慢編譯 | 中 | feature gate；預設唔開 |
| Exactly-once 語意誤解 | 高 | 文件明示 at-least-once + dedup；測試 |
| Bitemporal 改核心模型 | 高 | 完整回歸；兼容 view；分階段 |
| 兩時間軸索引爆炸 | 高 | CSR 只索引 business；system 交 snapshot |
| CBO overfit | 中 | 可關閉 + fallback 固定策略 |
| 單 process 隔離有限 | 中 | 先 best-effort；真隔離留 compute-storage 分離 |
| crate sprawl | 低 | 只開 `gtv-ingest`；`gtv-stream`/`gtv-observe` 可先做 module |

---

## 10. 展望：企業批（未排期）

以下喺呢三批之外，需另立批次：

- **銀行業務（Roadmap Milestone D）**：Risk scenario framework、CRM model
  governance、AML case explainability、ALM scenario cube、FTP curve engine、
  hierarchy / reference data。
- **企業營運（Roadmap Milestone E）**：compute-storage separation、distributed
  partition catalog、hot/warm/cold tiering、security（mTLS/RBAC/encryption）、
  multi-tenant isolation、observability、HA/DR。

呢啲項目應喺第三批穩定、SLO 達標之後，按業務優先級獨立排期。
