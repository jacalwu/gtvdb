# prod_p3 — 第三批：大型能力（Streaming / Bitemporal / ANN 進階）

> 上層路線圖：`BANKING_ANALYTICS_GAP_ROADMAP.md`
> 設計文件：`doc/prod_p3_design.md`
> **命名注意**：本文嘅 `p3` = 「第三批（Phase/Batch 3）」，**唔係** roadmap 嘅優先級 P3。
> 每項任務用 `B3-n` 編號，並註明對應 roadmap 編號。

---

## 0. 這批定位

第三批係**依賴第二批 catalog 嘅大型能力**：串流攝取、雙時間軸、以及 ANN 進階。
呢批風險最高、工期最長，必須喺 B2-1（catalog/manifest）穩定之後先開始。

**前置條件**：第二批 B2-1 ~ B2-4 完成（尤其 catalog + index lifecycle + lineage）。
**預計總工期**：約 3–5 個月（單人；B3-1 streaming 佔最大）。

---

## 1. 任務總覽

| ID | 任務 | Roadmap | 主要檔案 / 新 crate | 工期 | 依賴 |
|---|---|---|---|---|---|
| B3-1 | Streaming ingestion contract | P0.1 | 新 `gtv-ingest` / `gtv-stream` | 6–10 週 | B2-1 |
| B3-2 | Bitemporal 時間模型 | P0.3 | `gtv-core` + catalog schema evolution | 4–6 週 | B2-1 |
| B3-3 | Filter-aware ANN + exact rerank | P1.4, P1.6 | `gtv-index`, `gtv-engine` | 3–4 週 | B1-1, B1-4, B3-4 |
| B3-4 | IVF k-means coarse quantizer | P1.5 | `gtv-index/src/ivf.rs` | 2–3 週 | B1-1 |
| B3-5 | 多模態 cost-based optimizer | P1.7 | `gtv-engine`, `gtv-catalog` stats | 4–6 週 | B2-1, B3-3, B3-4 |
| B3-6 | Workload management / isolation | P1.8 | `gtv-engine`, `gtv-observe` | 3–4 週 | B1-3, B2-3, B3-5 |

> 建議次序：**B3-4 → B3-3 → B3-5 → B3-6**（ANN 線）；**B3-1 → B3-2**
> （資料線，可與 ANN 線並行，但兩者都食 B2-1）。
> B3-1 同 B3-2 都會改 catalog schema，需協調。

---

## 2. B3-1 Streaming ingestion contract

**問題**：現時只有 CSV / Parquet / CLI / 市場資料載入，冇 source offset、watermark、
late event、去重、replay、DLQ、backpressure。

**交付物**

1. **新 crate `gtv-ingest`**（來源適配器）同 **`gtv-stream`**（micro-batch 執行），
   或先合併為一個 `gtv-ingest`。
2. **統一 event envelope**：
   ```
   Envelope { source, partition, offset, event_id, event_time,
              ingest_time, schema_version, payload }
   ```
3. **來源適配器**：`trait SourceAdapter { poll(); commit(offsets); }` —
   Kafka（rdkafka）、Pulsar、CDC（Debezium JSON）；先做 Kafka + 一個
   檔案 replay 適配器（供測試 / 重演）。
4. **Offset store**：per (source, partition) committed offset，寫入 `gtv-catalog`。
5. **Micro-batch 原子發布**：batch → 經 B2-1 atomic commit 寫入 delta / 分區 →
   **同一 transaction 記錄 offset**（offset 記入 Snapshot summary）。
6. **去重**：event_id 為 key 嘅 dedup store（windowed bloom / roaring + 持久層）。
7. **Watermark / late event**：`watermark = max_event_time - allowed_lateness`；
   late event 走 correction policy（重算或 DLQ）。
8. **DLQ**：`deadletter/<source>/<date>.parquet` + `reprocess` 命令。
9. **Backpressure / rate limit / source health**：bounded channel + 監控。
10. **Metrics**：end-to-end lag、event-time lag、dropped events、offset lag。

**驗收條件**

- [ ] 任意重啟後由最後 committed offset 繼續，唔會重複或漏。
- [ ] 同一批資料重播不造成重複結果（冪等，靠 event_id dedup）。
- [ ] 未完整發布嘅 batch 對讀者不可見（依賴 B2-1 atomic commit）。
- [ ] 可量度 end-to-end lag、event-time lag、dropped events。
- [ ] late event 有明確處理（重算或 DLQ），並可審計。
- [ ] backpressure 生效時，來源唔會壓垮查詢路徑。

**風險**

- Exactly-once 語意：現實係 at-least-once + dedup，需明確向用戶講清楚。
- Watermark / late policy 一旦定錯，下游結果會錯；需業務確認 allowed lateness。
- Kafka/Pulsar 依賴會大幅增加編譯時間同 binary 大小（見 Cargo.toml 註解嘅
  build-time tuning 考量）。

---

## 3. B3-2 Bitemporal 時間模型

**問題**：核心表只有單一 `valid_from/valid_to`，無法同時表示「業務有效時間」
同「系統知悉時間」。更正資料會覆蓋歷史，違反可重演原則。

**交付物**

1. **核心型別**（`gtv-core`）：
   ```
   BitemporalRange { business_from, business_to, system_from, system_to }
   ```
2. **表 schema 擴充**：`business_valid_from/to`、`system_valid_from/to`、
   `event_time`、`ingest_time`、`business_date`。
3. **索引策略**（關鍵設計決定）：
   - CSR 只索引 **business time**（現有 `valid_from/valid_to` 改為 business 語意）。
   - **system time 交由 catalog snapshot 版本處理**：每個 system-time snapshot
     係一份獨立 immutable CSR / table version。
   - 避免同時索引兩個時間軸造成結構爆炸。
4. **SQL 語意**：`AS OF BUSINESS TIME <ts>` 同 `AS OF SYSTEM TIME <ts>`；
   或 table function `as_of(table, business_ts, system_ts)`。
5. **更正流程**：更正 = append 新 system version，永不覆寫；bitemporal overlap 檢查。
6. **遷移**：舊 `valid_from/valid_to` 映射為 business time，提供兼容 view。

**驗收條件**

- [ ] 可重演「當日收市時系統所知嘅資料」（system time 切片）。
- [ ] 更正資料唔覆蓋歷史版本；兩個時間軸可獨立查詢。
- [ ] Risk / ALM / FTP 結果可按原始 cutoff 重算。
- [ ] bitemporal overlap 檢查可偵測同一 entity 嘅矛盾版本。
- [ ] 舊資料（單時間軸）遷移後查詢結果不變。

**風險**

- 呢個係**核心資料模型改動**，影響 `gtv-core` 三個檔 + 所有 UDF；
  風險最高，必須有完整回歸測試。
- SQL 語意（`AS OF`）同 DataFusion parser 整合需要評估。

---

## 4. B3-3 Filter-aware ANN + exact rerank

**問題**：現時 filtered ANN 只有 `BooleanArray` pre-filter（`VectorIndex::search_knn`
嘅 bitmask），冇 selectivity 分派、oversample、rerank、telemetry。

**交付物**

1. **`AnnStrategy` enum**：
   `PreFilterExact | FilteredIvf | OversampledHnsw | PostFilterRerank | Exact`。
2. **選擇率自適應分派**：
   - selectivity < 1%：先建 bitmap，Flat / IVF 對允許 id 精確掃描。
   - 1–20%：filtered IVF（只探含允許 id 嘅 cell）或 oversampled HNSW。
   - > 20%：一般 HNSW，`ef = k / selectivity`，再 post-filter。
   - 監管 / 高風險用途：強制 `Exact`。
3. **Exact rerank**：ANN 先取 `K × oversample`，對候選用原始 `f32`
   精確計距離，並套 metadata / temporal / entity filter；
   同時輸出**近似分數**同**精確分數**。
4. **Telemetry**：strategy、candidate_count、filtered_count、recall estimate、
   latency breakdown。
5. **Recall harness**：抽樣查詢對 FlatIndex oracle 計 Recall@K。

**驗收條件**

- [ ] 三種 selectivity 情境下，自適應策略嘅 recall / latency 優於固定策略（量度）。
- [ ] `Exact` 模式結果與 FlatIndex 完全一致。
- [ ] 每個查詢可輸出所用 strategy + candidate / filtered count + latency。
- [ ] Recall@K 可持續量度（抽樣），並有 baseline 回歸測試。
- [ ] filter 與 temporal predicate 可同時套用。

**風險**

- recall estimate 需要準確嘅 selectivity 統計（依賴 B3-5 / catalog stats）。
- oversampling 倍數需調參，否則高選擇率時反而變慢。

---

## 5. B3-4 IVF k-means coarse quantizer

**問題**：`ivf.rs:72-76` 明寫用「evenly-spaced deterministic centroid sampling」，
對不均衡銀行 embedding corpus 唔穩定。

**交付物**

1. **sampled k-means**：k-means++ 初始化、Lloyd 迭代、multiple restarts（取最低 inertia）。
2. **deterministic seeded RNG**（沿用 HNSW `SplitMix64` 風格）保證可重現。
3. **empty cell handling**（重指派最遠點）+ **oversized cell split**。
4. **cell population statistics** + **retraining trigger**（漂移 / 不均衡）。
5. **nlist / nprobe 自動調優**：以 FlatIndex 量 Recall@K，揀出符合 target
   recall 嘅最低成本組合。
6. 保留舊均勻取樣做 fallback / 對照。

**驗收條件**

- [x] 對不均衡 corpus，k-means 版 Recall@K 明顯高於均勻取樣版（量度）。
- [x] 相同 seed + 資料 → 完全相同 centroids（可重現）。
- [x] 無空 cell；oversized cell 可自動切分。
- [x] `nlist`/`nprobe` 調優可輸出 recall/latency 曲線。
- [x] 現有 IVF 測試零回歸。

**風險**

- k-means 訓練時間／記憶體；需 sampled + 迭代上限。
- 與 B2-2 index snapshot 格式綁定，retrain 後要能重新發布。

---

## 6. B3-5 多模態 cost-based optimizer

**問題**：現時冇統計、冇成本模型；filter 先定 ANN 先、用邊個索引，全靠寫死。

**交付物**

1. **統計**（放 `gtv-catalog`）：row count、distinct、null count、
   partition min/max、graph degree histogram、temporal active ratio、
   vector corpus size、IVF cell distribution、filter selectivity、
   ANN recall/latency curve。
2. **成本模型 / planner**決定：
   - 先 SQL filter 定先 ANN；
   - 用 Flat / IVF / HNSW；
   - temporal filter 是否先產 bitmap；
   - graph expansion 是否先裁剪 source nodes；
   - 是否 exact rerank。
3. **DataFusion 整合**：custom statistics provider + `PhysicalOptimizerRule`。
4. **`EXPLAIN`** 輸出所選策略 + 估算成本。

**驗收條件**

- [ ] `EXPLAIN` 可顯示策略選擇同估算成本。
- [ ] 至少 3 個代表性查詢（純向量、filter+向量、圖+向量）選中合理策略。
- [ ] 對比固定策略，整體 latency / 資源有可量度改善。
- [ ] 統計可隨 catalog commit 更新（唔會用過期 stats）。

**風險**

- 成本模型容易 overfit 特定 workload；需可配置 + 可關閉（fallback 固定策略）。
- 與 DataFusion 版本升級耦合。

---

## 7. B3-6 Workload management / isolation

**問題**：冇 workload class、冇資源配額、冇 admission control；一個大圖查詢或
index build 可以拖死實時查詢。

**交付物**

1. **Workload classes**：ingestion、interactive AML、Risk batch、ALM batch、
   FTP batch、index build。
2. **Resource group**：CPU / memory / IO / concurrency quota。
3. **Admission control**：超載時排隊或拒絕，附明確錯誤。
4. **優先級 + preemption**：低優先級可被取消（重用 B1-3 cancel token）。
5. **Spill-to-disk**：大 sort / join 落磁碟。
6. **隔離**：防止單一大圖查詢 / index build 影響實時查詢。
7. **`gtv-observe`**：metrics / tracing / workload telemetry（可由
   `gtv-engine/src/monitor.rs` 擴充，唔一定開新 crate）。

**驗收條件**

- [ ] 混合負載（ingestion + interactive + batch + index build）下，interactive
      P99 符合 SLO。
- [ ] index build 唔會令 interactive 查詢超時（隔離量測）。
- [ ] 超載時有明確 admission 決策 + 可觀測。
- [ ] preemption 可即時取消低優先級查詢。

**風險**

- 資源隔離喺單 process in-memory 架構下有限；真正隔離可能要靠 B3-1 之後嘅
  compute-storage 分離（企業批）。
- 需先有 B1-3 嘅 budget/cancel 同 B2-3 嘅 execution context。

---

## 8. 本批完成定義（Definition of Done）

- [ ] B3-1 ~ B3-6 全部驗收條件通過。
- [ ] 端到端：Kafka 攝取 → catalog 原子發布 → bitemporal 查詢 →
      filtered ANN + rerank → CBO 選路 → workload 隔離 → lineage 重演。
- [ ] 混合負載 SLO 壓測報告。
- [ ] `cargo test --workspace` 全綠；recall / latency / lag 監控上線。
- [ ] 文件：`doc/prod_p3_design.md`、運維手冊、SLO 定義。

---

## 9. 後續（企業批，未排期）

Roadmap Milestone D / E 嘅內容**唔喺呢三批之內**，需另立批次：

- **銀行業務能力（Milestone D）**：Risk scenario framework、CRM model governance、
  AML pattern / case explainability、ALM scenario cube、FTP curve engine、
  hierarchy / reference data。
- **企業營運（Milestone E）**：compute-storage separation、distributed partition
  catalog、hot/warm/cold tiering、security（mTLS/RBAC/encryption）、
  multi-tenant isolation、observability、HA/DR。

呢啲項目應喺第三批穩定、SLO 達標之後，再按業務優先級獨立排期，唔應混入
analysis engine 嘅主要執行路徑。
