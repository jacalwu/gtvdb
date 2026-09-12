# gtvdb 銀行實時分析平台 Gap 與優先改進路線圖

## 1. 產品定位

gtvdb 定位為銀行實時及近實時多模態分析平台，主要服務：

- 實時數據分析
- 風險管理（Risk）
- 反洗錢（AML）
- 資產負債管理（ALM）
- 資金轉移定價（FTP）
- Embedding 儲存與相似度搜索

本系統不定位為在線交易處理或核心賬本系統。設計目標因此不是完整 OLTP serializable transaction，而是：

> 低延遲攝取、時序一致性、大規模列式分析、圖關係分析、可過濾向量搜索、可重演及可審計。

## 2. 設計原則

1. 業務事件、計算結果、模型、情景及索引均必須版本化。
2. 所有 Risk、AML、ALM、FTP 結果必須可以按指定資料截點重演。
3. Graph、Temporal、Vector 和 Columnar 查詢應共用統一的過濾、統計及執行框架。
4. Embedding、ANN 索引及圖索引是可重建衍生結構，來源資料及 metadata 才是權威依據。
5. 即使不是交易系統，攝取和分區發布仍須具備去重、冪等、原子可見及可恢復能力。
6. 性能目標必須同時包含吞吐量、尾延遲、查詢可預測性、召回率及資料新鮮度。

---

# P0：投產前必要能力

P0 是正式銀行分析環境的最低門檻。未完成前，系統只適合研究、驗證或受控試點。

## P0.1 Streaming Ingestion Contract

### Gap

現有 CSV、Parquet、CLI 及市場資料載入能力未形成正式流式攝取協議，欠缺來源 offset、watermark、late event、去重及 replay 控制。

### 改進要求

- 新增 `gtv-ingest`、`gtv-stream`、`gtv-catalog`。
- 支援 Kafka、Pulsar 或 CDC 類來源適配器。
- 定義統一事件 envelope：
  - source
  - partition
  - offset
  - event_id
  - event_time
  - ingest_time
  - schema_version
  - payload
- 保存 committed source offsets。
- 支援 micro-batch 原子發布。
- 支援 idempotent replay 和 duplicate suppression。
- 定義 watermark、late arrival 及 correction policy。
- 提供 dead-letter queue 和錯誤重處理流程。
- 加入 back-pressure、流量限制及來源健康監控。

### 驗收條件

- 任意重啟後可由最後 committed offset 繼續。
- 同一批資料重播不造成重複結果。
- 未完整發布的 batch 對讀者不可見。
- 可以量度 end-to-end lag、event-time lag 和 dropped events。

## P0.2 Catalog、Manifest 與 Schema Evolution

### Gap

分區檔案、schema、dictionary、來源 offset、模型和索引之間欠缺統一 metadata control plane。

### 改進要求

- 為每張表建立 immutable table UUID。
- 建立 versioned schema registry。
- 建立 partition spec version。
- 建立 manifest，記錄：
  - file ID
  - path
  - row count
  - min/max event time
  - column statistics
  - schema version
  - source offsets
  - commit ID
  - encryption key ID
  - lineage metadata
- 分區寫入採 temp file、fsync、校驗、atomic rename、manifest commit。
- 支援 add column、rename、widening cast 及相容性檢查。
- 避免 `date/table/symbol` 造成大量小檔案，改用 date/table/hash-bucket 或可配置 partition spec。
- 全域 symbol dictionary 改為單寫者、版本化 CAS，或 per-file Arrow dictionary。

### 驗收條件

- 讀者只會看到完整 committed snapshot。
- schema 升級不會令舊分區不可讀。
- 任意結果可追溯至明確檔案、schema 和來源 offset。

## P0.3 Bitemporal 時間模型

### Gap

單一 `valid_from/valid_to` 未能同時表示業務有效時間與系統知悉時間。

### 改進要求

核心表支援：

- `business_valid_from`
- `business_valid_to`
- `system_valid_from`
- `system_valid_to`
- `event_time`
- `ingest_time`
- `business_date`

SQL 層提供明確的 `AS OF BUSINESS TIME` 和 `AS OF SYSTEM TIME` 語意。

### 驗收條件

- 可重演「當日收市時系統所知道的資料」。
- 更正資料不覆蓋歷史版本。
- Risk、ALM 和 FTP 結果可按原始 cutoff 重算。

## P0.4 Embedding 標準資料模型

### Gap

已有向量索引，但未形成完整 embedding table、catalog 和版本治理。

### 改進要求

使用 Arrow `FixedSizeList<Float32>` 作主要向量欄位，標準 schema 至少包括：

- entity_id
- embedding
- model_id
- model_version
- tokenizer_version
- dimension
- distance_metric
- normalized
- created_at
- effective_from
- effective_to
- source_hash
- feature_version
- tenant_id
- classification

禁止不同 model、dimension、metric 或 normalization policy 的向量混入同一索引。

### 驗收條件

- schema 層可驗證 dimension。
- 每個檢索結果可追溯 embedding model、來源和生成版本。
- 支援 embedding 過期、替換及重建。

## P0.5 Distance Metric

### Gap

現有主要實作集中於 squared L2，未形成統一 metric contract。

### 改進要求

支援：

- L2
- Cosine
- Inner Product / Dot Product

建立 `DistanceMetric` enum，metric 固定在索引 metadata。Cosine 向量須記錄 normalization policy。

### 驗收條件

- 查詢 metric 與索引 metric 不一致時明確拒絕。
- FlatIndex 可作所有 ANN 索引的正確性及 recall oracle。

## P0.6 Vector Index Lifecycle

### Gap

索引缺少完整 snapshot、restore、rebuild 和版本關聯。

### 改進要求

每個索引 manifest 記錄：

- index ID
- index type
- corpus snapshot ID
- source file IDs
- embedding model/version
- dimension及metric
- build parameters
- build timestamp
- checksum
- engine compatibility version
- row count
- tombstone count

支援：

- snapshot/save/load
- offline rebuild
- shadow index build
- atomic index swap
- rollback
- corruption detection

### 驗收條件

- 重啟後無需由應用逐筆重新 insert。
- 新舊索引切換期間查詢不中斷。
- 索引可由權威 embedding table 完整重建。

## P0.7 可重演及可審計執行上下文

### Gap

Risk、AML、ALM、FTP 結果尚欠統一執行證據包。

### 改進要求

每次執行保存：

- query text及hash
- engine version
- source snapshot ID
- source offsets
- schema version
- model version
- embedding model version
- vector index snapshot
- scenario version
- business cutoff
- UDF version
- runtime parameters
- output checksum

### 驗收條件

- 可按 execution ID 重演結果。
- 可解釋結果使用了哪批資料、模型、情景、UDF 和索引。

## P0.8 資料質量與對賬閘門

### 改進要求

- completeness、uniqueness、freshness、range、referential integrity 規則。
- source-to-target count 和 amount reconciliation。
- bitemporal overlap 檢查。
- entity、account、instrument、curve 和 scenario key 完整性。
- 資料質量未達門檻時阻止正式風險結果發布。
- 所有 override 必須有原因、批准人及審計記錄。

---

# P1：實時性能與多模態查詢能力

## P1.1 TemporalCSR 自適應時間索引

### Gap

鄰接查詢仍可能線性掃描單一 source 的完整 adjacency run。

### 改進要求

- 低 degree：linear scan。
- 中 degree：binary search `valid_from` + SIMD `valid_to`。
- 高 degree：per-source zone map + time bucket。
- 極高 degree：獨立分區 + parallel scan。
- 將現有 zone-map 正式整合到 neighbor path。
- 收集 degree histogram、active ratio 及每種策略成本。

## P1.2 AML Traversal 安全及性能

### 改進要求

- 全域 visited bitmap。
- dense frontier 使用 bitset。
- sparse frontier 使用 Roaring Bitmap 或 hash structure。
- direction-optimising BFS。
- high-degree node guard。
- max hops、max edges、max rows、max memory 和 timeout。
- cancellation token。
- amount、currency、channel、jurisdiction、edge type 和時間 predicate pushdown。
- deterministic result ordering。

## P1.3 HNSW 工業級記憶體佈局

### Gap

每個 node 的 `Vec<f32>` 和 `Vec<Vec<usize>>` 會造成大量 allocation、pointer chasing 和較差 cache locality。

### 改進要求

重構為：

- contiguous row-major vector buffer
- contiguous node IDs
- CSR-style layer offsets
- contiguous neighbour IDs
- generation-stamped visited array
- min-heap candidates
- bounded max-heap results

並支援：

- batch query
- parallel query
- tombstone deletion
- compaction/rebuild
- dynamic `ef_search`
- recall/latency monitoring
- filter-aware routing

## P1.4 Filtered ANN 策略

### 改進要求

明確區分：

- pre-filter
- traversal filter
- post-filter
- exact rerank

按 filter selectivity 自適應選擇：

- selective filter：先 bitmap，再 Flat/IVF 掃候選。
- 中等選擇率：filtered IVF 或 oversampled HNSW。
- 低選擇率：一般 ANN，再 metadata filter 和 rerank。

每個查詢輸出 ANN strategy、candidate count、filtered count、recall estimate 和 latency breakdown。

## P1.5 IVF Coarse Quantizer

### Gap

均勻樣本 centroid 不足以穩定處理不均衡銀行 embedding corpus。

### 改進要求

- sampled k-means。
- k-means++ initialization。
- multiple restarts。
- empty cell handling。
- oversized cell split。
- cell population statistics。
- retraining trigger。
- nlist/nprobe 自動調優。
- 以 FlatIndex 量度 Recall@K。

## P1.6 精確 Rerank

- ANN 先取 `K * oversample_factor`。
- 對候選以原始 f32 embedding 精確計算。
- 支援 metadata、temporal 和 entity filter。
- 輸出近似分數及精確分數。
- 對監管或高風險用途允許強制 exact mode。

## P1.7 多模態 Cost-Based Optimizer

建立統計資料：

- row count
- distinct count
- null count
- partition min/max
- graph degree histogram
- temporal active ratio
- vector corpus size
- IVF cell distribution
- filter selectivity
- ANN recall/latency curve

Optimizer 決定：

- 先 SQL filter 或先 ANN。
- Flat、IVF 或 HNSW。
- temporal filter 是否先產 bitmap。
- graph expansion 是否先裁剪 source nodes。
- 是否 exact rerank。

## P1.8 Workload Management

- workload classes：ingestion、interactive AML、Risk batch、ALM batch、FTP batch、index build。
- CPU、memory、IO 和 concurrency quota。
- admission control。
- query priority 和 preemption policy。
- spill-to-disk。
- resource group isolation。
- 防止單一大圖查詢或 index build 影響實時查詢。

---

# P2：銀行業務能力完善

## P2.1 Risk Scenario Framework

- scenario catalog及version。
- baseline、stress、adverse 和 reverse stress。
- legal entity、portfolio、product、currency 維度。
- source cutoff 和 model version。
- scenario inheritance及override。
- deterministic batch rerun。
- 結果 reconciliation 和 explainability。

## P2.2 CRM 模型治理

- 抵押品資格及優先級規則外部化。
- haircut、FX mismatch、maturity mismatch 版本化。
- wrong-way risk。
- netting set。
- concentration limit。
- guarantee eligibility。
- greedy 與 LP 結果差異報告。
- 每次 allocation 的完整 audit trail。

## P2.3 AML Pattern 與 Case Explainability

- directed/undirected semantics。
- rolling window motifs。
- transaction sequence constraints。
- ring、path、diamond 以外的可組合作圖語言。
- amount、currency、jurisdiction、channel 聚合。
- beneficial ownership closure。
- entity-resolution embedding。
- graph + vector hybrid score。
- alert explanation subgraph。
- case snapshot 和 investigator feedback。
- false-positive feedback loop。

## P2.4 ALM Scenario Cube

建立標準資料模型：

- as_of_date
- scenario_id
- legal_entity
- currency
- product
- time_bucket
- cashflow_type
- amount
- discount_factor
- repricing_date
- behavioural_assumption_version

支援：

- cash-flow ladder
- NII/EVE
- repricing gap
- liquidity stress
- deposit decay
- prepayment
- optionality
- multi-currency aggregation

## P2.5 FTP Curve 與定價引擎

- curve catalog及version。
- tenor interpolation。
- liquidity premium。
- basis spread。
- optionality charge。
- behavioural adjustment。
- product hierarchy。
- booking date、value date、maturity date。
- 預測與實際成本對賬。
- 定價結果逐步 explainability。

## P2.6 Hierarchy 與 Reference Data

- legal entity hierarchy。
- organisation hierarchy。
- product hierarchy。
- account、customer、instrument、counterparty master。
- curve、calendar、currency 和 jurisdiction reference data。
- effective dating 和歷史版本。

---

# P3：規模化、可靠性與運維

## P3.1 Compute / Storage Separation

- object storage 或共享持久層。
- stateless query workers。
- metadata/catalog service。
- local SSD cache。
- cache eviction 和 warming。
- data locality-aware scheduling。

## P3.2 分散式 Partition Catalog

- partition ownership。
- shard map version。
- online rebalance。
- replica placement。
- stale catalog detection。
- atomic metadata update。

## P3.3 Hot / Warm / Cold Tiering

- Hot：近期事件、活躍 CSR、熱門 embeddings。
- Warm：Parquet + mmap、IVF-Flat。
- Cold：壓縮 Parquet、IVF-PQ 或按需 index。
- 自動 promotion/demotion。
- retention、archive 和 legal hold。

## P3.4 Security

- mTLS。
- workload identity。
- RBAC 和必要時 ABAC。
- table/column/row-level policy。
- privileged access management。
- encryption at rest。
- key rotation。
- field tokenisation。
- immutable audit log。
- secrets externalisation。
- signed artifacts、SBOM 和 dependency scanning。

## P3.5 Multi-Tenant Isolation

- tenant ID 強制注入。
- memory、CPU、IO、storage quota。
- vector index tenant isolation。
- noisy-neighbour control。
- per-tenant encryption context。
- per-tenant usage和成本統計。

## P3.6 Observability

至少提供：

- ingestion lag
- watermark lag
- source offset lag
- query p50/p95/p99
- rows/edges/vectors scanned
- cache hit ratio
- partition pruning ratio
- ANN candidate count
- Recall@K sample
- graph frontier size
- spill bytes
- index build duration
- data quality pass rate
- scenario batch completion

## P3.7 High Availability 與 Disaster Recovery

- query service 多副本。
- catalog 及 manifest 備份。
- index snapshot 多副本。
- 自動或受控 failover。
- regular restore test。
- degraded read mode。
- RTO/RPO 分業務工作負載定義。
- severe-but-plausible scenario 演練。

---

# 3. 建議新增或重構 Crates

```text
gtv-ingest       # streaming/CDC adapters, offsets, watermark, dedup
gtv-stream       # micro-batch and event-time execution
gtv-catalog      # schema, table, partition, snapshot and lineage metadata
gtv-embedding    # embedding schema, model catalog and generation metadata
gtv-index-store  # vector index manifest, snapshot, load, rebuild and swap
gtv-scenario     # Risk/ALM/FTP scenario catalog and execution context
gtv-governance   # audit, lineage, data quality and policy hooks
gtv-observe      # metrics, tracing and workload telemetry
```

現有 crates 的建議責任：

```text
gtv-core         # Arrow tables, temporal graph primitives, bitemporal types
gtv-array        # vectorised temporal, Risk, ALM and FTP kernels
gtv-engine       # DataFusion integration and multimodal optimizer
gtv-index        # Flat/IVF/HNSW kernels only
gtv-storage      # immutable data files, HDB, manifests and cache
gtv-pattern      # AML graph pattern runtime
gtv-delta        # mutable near-real-time delta layer and compaction
gtv-proto        # service protocol
gtv-server       # query/ingestion service endpoints
gtv-cli          # administration and developer interface
gtv-udf          # governed sandbox UDF runtime
```

---

# 4. 建議實施順序

## Milestone A：可重演分析基礎

- Streaming ingestion contract
- source offset、dedup、replay
- atomic partition publication
- catalog、manifest、schema evolution
- bitemporal model
- execution lineage
- data quality gates

**完成標準：** 任意 Risk/AML/ALM/FTP 執行都可以按 snapshot 和 execution ID 重演。

## Milestone B：Embedding Database 基礎

- Arrow embedding schema
- model/version catalog
- L2/Cosine/Dot
- index manifest
- snapshot/load/rebuild
- exact Flat baseline
- ANN recall test harness

**完成標準：** 索引可持久化、重建、版本切換，結果可追溯至 model 和 corpus snapshot。

## Milestone C：低延遲及 Hybrid Query

- TemporalCSR adaptive index
- AML traversal budgets
- HNSW contiguous layout
- IVF k-means quantizer
- filtered ANN planner
- exact rerank
- DataFusion multimodal cost model
- workload isolation

**完成標準：** 實時、AML、Risk batch 和索引建立可並行運作，並符合各自 SLO。

## Milestone D：銀行領域功能

- Risk scenario framework
- CRM governance
- AML explainability
- ALM scenario cube
- FTP curve engine
- hierarchy/reference data

**完成標準：** 計算規則、模型、情景和輸入均版本化，結果可對賬及解釋。

## Milestone E：企業級營運

- compute/storage separation
- distributed catalog
- tiering
- HA/DR
- security
- multi-tenant isolation
- observability
- capacity certification

**完成標準：** 通過性能、恢復、安全、資料質量及嚴重但合理故障情景測試。

---

# 5. 非目標

目前不應優先投入：

- 核心賬本雙重記賬引擎
- OLTP serializable transaction manager
- 跨 shard 在線金融交易 commit
- 客戶餘額主檔
- 低延遲支付 authorization write path

若未來產品定位改變，以上項目須另開獨立架構路線，不應混入分析引擎的主要執行路徑。

---

# 6. 最終優先級摘要

## P0

1. Streaming ingestion、offset、dedup、replay
2. Catalog、manifest、atomic publication、schema evolution
3. Bitemporal semantics
4. Embedding schema 和 model governance
5. L2、Cosine、Dot metric contract
6. Vector index lifecycle
7. Query lineage 和 deterministic rerun
8. Data quality 和 reconciliation gates

## P1

1. TemporalCSR adaptive index
2. AML visited bitmap、query budget 和 predicate pushdown
3. HNSW contiguous layout
4. Filter-aware ANN
5. IVF k-means coarse quantizer
6. Exact rerank
7. Multimodal optimizer
8. Workload isolation

## P2

1. Risk scenario framework
2. CRM governance
3. AML pattern 和 case explainability
4. ALM scenario cube
5. FTP curve/version engine
6. Hierarchy 和 reference data

## P3

1. Compute/storage separation
2. Distributed partition catalog
3. Hot/warm/cold tiering
4. Security
5. Multi-tenant isolation
6. Observability
7. HA/DR

---

# 7. 成功判斷

gtvdb 達到銀行生產分析平台標準，不應只以「功能可執行」判斷，而應同時證明：

- 資料可按來源 offset 重播且不重複。
- 任意結果可按指定 cutoff、snapshot、model 和 scenario 重演。
- 時間更正不會破壞歷史視圖。
- Embedding 和索引有完整版本及 lineage。
- ANN 有可量度的 recall、latency 和 filter 行為。
- 圖查詢有資源上限及可預測尾延遲。
- Risk、AML、ALM、FTP 結果可對賬、可解釋及可審計。
- 系統在 ingestion、interactive query、batch 和 index build 混合負載下仍符合 SLO。
