# prod_p2 — 第二批：可重演分析基礎（Catalog 為核心）

> 上層路線圖：`BANKING_ANALYTICS_GAP_ROADMAP.md`
> 設計文件：`doc/prod_p2_design.md`
> **命名注意**：本文嘅 `p2` = 「第二批（Phase/Batch 2）」，**唔係** roadmap 嘅優先級 P2。
> 每項任務用 `B2-n` 編號，並註明對應 roadmap 編號。

---

## 0. 這批為何關鍵

Roadmap 將 streaming（P0.1）排第一，但依賴圖顯示 **catalog/manifest 才是 keystone**：

```
B2-1 Catalog/Manifest ──┬──> 原子發布 + offset/replay（第三批 B3-1）
                        ├──> index manifest / shadow swap（B2-2）
                        ├──> execution lineage（B2-3，要 snapshot_id）
                        ├──> embedding governance（B2-4）
                        ├──> DQ gate（B2-5，要 snapshot）
                        └──> bitemporal（第三批 B3-2，要 schema evolution）
```

冇 catalog，第三批啲嘢全部做唔到「原子可見、可稽核、可重演」。

**前置條件**：B1-1（metric）同 B1-4（HNSW 序列化）最好已完成。
**預計總工期**：約 8–12 週（單人；B2-1 佔一半）。

---

## 1. 任務總覽

| ID | 任務 | Roadmap | 主要檔案 / 新 crate | 工期 | 依賴 |
|---|---|---|---|---|---|
| B2-1 | Catalog / Manifest / Schema evolution | P0.2 | 新 `gtv-catalog`，整合 `gtv-storage` | 4–6 週 | — |
| B2-2 | Vector index lifecycle | P0.6 | 新 `gtv-index-store`（用 B1-4 序列化） | 2–3 週 | B2-1, B1-4 |
| B2-3 | Execution context / lineage / replay | P0.7 | `gtv-engine/context.rs` + `gtv-catalog` | 1–2 週 | B2-1 |
| B2-4 | Embedding 標準 schema 與治理 | P0.4 | `gtv-catalog/embedding` 或 `gtv-embedding` | 2–3 週 | B2-1, B1-1 |
| B2-5 | 資料質量與對賬閘門（最小可用） | P0.8 | `gtv-engine/monitor.rs` + `gtv-catalog` | 2–3 週 | B2-1 |

> 建議次序：**B2-1 → B2-4 ∥ B2-2 → B2-3 → B2-5**。B2-1 係一切前提，唔應該並行開工。

---

## 2. B2-1 Catalog / Manifest / Schema evolution（keystone）

**問題**：現時只有 `crates/gtv-cli/src/catalog.rs` 一張 `catalog.tsv`
（name / kind / path / rows / created）。冇 table UUID、schema 版本、
partition spec、file-level manifest、atomic commit、schema evolution。
`gtv-storage/src/hdb.rs` 用 `date/table/symbol` 分區，會產生大量小檔案
（roadmap 明確警告）。

**交付物**

1. **新 crate `gtv-catalog`**，提供：
   - `TableId(Uuid)`：每表 immutable。
   - **Versioned schema registry**：`SchemaVersion(u32)` + schema 歷史；
     支援 add column / rename / widening cast + 相容性檢查。
   - **Partition spec version**：`PartitionSpec { version, columns: [identity |
     date_trunc | hash_bucket] }`；解決 date/table/symbol 小檔案問題。
   - **Manifest**：
     ```
     DataFile { file_id, path, format, row_count, column_stats,
                event_time_min/max, schema_version, partition,
                checksum(blake3), size_bytes, source_offsets, commit_id }
     Snapshot { snapshot_id, parent, table_id, schema_version, spec_version,
                files, created_at, op, summary }
     ```
   - **Atomic commit protocol**：
     `tmp write → fsync → checksum → atomic rename → manifest tmp → fsync +
     atomic rename → version-hint CAS`。
   - **Reader isolation**：讀者只經已提交 `Snapshot`，未完整提交嘅 batch 不可見。
   - Catalog API：`create_table / table / commit / snapshot / latest /
     evolve_schema / scan(table_id, snapshot_id, filter)`。
2. **整合 `gtv-storage`**：新增 atomic partition 寫入 + snapshot 讀取；
   `HdbStore` 改為經 catalog 提交（保留舊路徑做 legacy import）。
3. **遷移**：`gtv-cli/src/catalog.rs` 改為 delegate 去 `gtv-catalog`；
   `catalog.tsv` 提供一次性 import。
4. **統計**：每個 `DataFile` 記 `column_stats`（null_count / min / max /
   distinct_est），供第三批 CBO 用。

**驗收條件**

- [ ] 斷電／中斷提交後重啟，讀者**只見到完整 committed snapshot**。
- [ ] 同一批資料重播不產生重複 manifest entry（冪等）。
- [ ] schema 升級後，舊分區仍可讀；不相容升級被拒並回明確錯誤。
- [ ] 任意查詢結果可追溯至明確 file_id / schema_version / commit_id / source_offset。
- [ ] 分區數目唔再隨 symbol 數量爆炸（用 hash_bucket / 可配置 spec 驗證）。
- [ ] `catalog.tsv` legacy import 後所有表可正常 replay。

**風險**

- Atomic commit 嘅 fsync/rename 語意跨 OS（Linux/macOS）要小心；
  需 crash-injection 測試。
- Schema evolution 相容性規則一旦定錯，往後難改；設計文件要先 freeze 規則。

---

## 3. B2-2 Vector index lifecycle

**問題**：`crates/gtv-index` 冇 `serde`、冇 `gtv-storage` 依賴；HNSW grep
`save/load/snapshot/tombstone` 全空。索引只能即場 `build`，重啟要重新 insert。
`IvfIndex` 甚至未接上任何 query path。

**交付物**

1. **Index manifest**：
   ```
   IndexManifest { index_id, table_id, index_type(Flat|Ivf|Hnsw),
     corpus_snapshot_id, source_file_ids, model_id/version, embedding_model,
     dim, metric, build_params, build_ts, checksum, engine_version,
     row_count, tombstone_count }
   ```
2. **Snapshot 格式**（版本化 binary `<index>.gtvidx`）：
   `magic + version + manifest_json_len + manifest + payload`。
   - HNSW payload = B1-4 嘅 flat buffers。
   - IVF = centroids + list_offsets + ids + data。
   - Flat = ids + data。
3. **`gtv-index-store` crate**（依賴 `gtv-index` + `gtv-catalog`，避免 cycle）：
   `save / load / rebuild(corpus_snapshot) / checksum / verify`。
4. **Shadow build + atomic swap**：
   `index/<id>/v<n>/` → 寫 manifest → `CURRENT` 指針 CAS 切換；
   舊 reader 仍讀舊 snapshot，查詢不中斷。
5. **Rollback**：保留最近 k 個版本；`rollback(index_id, snapshot)`。
6. **Corruption detection**：load 時驗 checksum；失敗回明確錯誤。
7. **接線**：`gtv-engine` 嘅 `knn` / `vector_search` 由 `KnnCollection`（暴力）
   改為可選 `Flat/Ivf/Hnsw` snapshot；`IvfIndex` 正式接入。

**驗收條件**

- [ ] 重啟後無需逐筆 re-insert，直接 load snapshot 即可查詢。
- [ ] 索引可由權威 embedding table（B2-4）+ corpus snapshot **完整重建**。
- [ ] Shadow swap 期間查詢 P99 不中斷（併發壓測）。
- [ ] load 被篡改檔案 → checksum 失敗並拒絕。
- [ ] `IvfIndex` 至少有一條 SQL 查詢路徑，且回傳正確 Top-K。
- [ ] rollback 後查詢結果 = 舊版本結果（確定性）。

**風險**

- 索引格式版本相容：engine 升級後要能讀舊格式（或明確要求 rebuild）。
- 大索引 load 時間／記憶體：需量化，避免重啟停機過久。

---

## 4. B2-3 Execution context / lineage / replay

**問題**：全 repo 無 `execution_id` / `snapshot_id` / `output checksum`。
Risk/AML/ALM/FTP 結果無法重演或解釋。

**交付物**

1. `ExecutionId(Uuid)`、`EngineVersion`（`env!("CARGO_PKG_VERSION")`）。
2. **`ExecutionRecord`**：
   ```
   execution_id, query_text, query_hash(blake3), engine_version,
   source_snapshots[(table_id, snapshot_id, schema_version)],
   source_offsets, model_versions, embedding_model_versions,
   vector_index_snapshots[(index_id, snapshot_id)],
   scenario_version, business_cutoff,
   udf_versions[(name, version, hash)], runtime_params,
   output_checksum(blake3), output_rows, started_at, finished_at
   ```
3. **Hook**：`GtvContext::execute_with_lineage(sql, opts) -> (batches, ExecutionRecord)`；
   由 DataFusion logical plan 抽出所引用嘅表 / UDF，配合 catalog 解析 snapshot id。
4. **Lineage 儲存**：append-only（Parquet / JSONL）喺 catalog。
5. **Replay**：`replay(execution_id)` → 用 pinned snapshots 重新 register + 重跑 SQL。
6. **Determinism 防護**：`rand` / `now` 類 UDF 標記為 nondeterministic，
   重演時拒絕或警告；UDF registry 記 version + hash。

**驗收條件**

- [x] 按 `execution_id` 可完整重演結果，output checksum 一致。
- [x] 可回答「呢個結果用咗邊批資料、邊個 model、邊個 scenario、邊個 index」。
- [x] 涉及 nondeterministic UDF 時，replay 明確拒絕或標旗。
- [x] lineage 記錄本身可經 SQL 查詢。

**風險**

- 從 logical plan 抽表引用要對 DataFusion 版本升級保持兼容。
- 「重演」定義需先釘死：**byte-identical** vs **semantic-equivalent**
  （建議 P0 要 byte-identical，浮點 UDF 例外需標記）。

---

## 5. B2-4 Embedding 標準 schema 與治理

**問題**：`KnnCollection` 只係 `ids / Vec<Vec<f32>> / labels`。冇 `FixedSizeList`、
`model_id`、`source_hash`、`tenant_id`、`classification`，亦冇 dimension/metric 混用防護。

**交付物**

1. **Arrow 標準 schema**：
   ```
   entity_id, embedding: FixedSizeList<Float32>[dim],
   model_id, model_version, tokenizer_version, dimension,
   distance_metric, normalized, created_at,
   effective_from, effective_to, source_hash,
   feature_version, tenant_id, classification
   ```
2. **驗證**：同一索引只准單一 dimension / metric / model；混用 → 明確拒絕。
3. **治理**：embedding 過期、替換、重建流程；每個檢索結果可追溯
   model / 來源 / 生成版本。
4. **整合 `gtv-index-store`**：embedding table → build index（B2-2）。
5. （可選）`gtv-embedding` 獨立 crate，或先做 `gtv-catalog::embedding` module
   （避免 crate 過度拆分）。

**驗收條件**

- [ ] schema 層可驗 `dimension` 同 `FixedSizeList` 長度一致。
- [ ] 混入不同 model / dim / metric 嘅向量 → 建索引時被拒。
- [ ] 每個檢索結果可列出 model_id / version / source_hash / feature_version。
- [ ] 支援 embedding 過期（effective_to）後唔再被檢索命中。
- [ ] tenant_id 隔離：跨 tenant 查詢唔會互相命中（P0 最基本）。

**風險**

- tenant/classification 一旦入 schema，就要一併諗 P3 多租戶隔離嘅一致性。
- 向量欄位用 `FixedSizeList` 會影響 Parquet/Arrow 讀寫同 catalog stats 支援。

---

## 6. B2-5 資料質量與對賬閘門（最小可用）

**問題**：`gtv-engine/src/monitor.rs` 已有 `dq_report / dq_check / health_check /
strategy_stats`，但**只係診斷，唔會阻止結果發布**，亦冇對賬、override 審計。

**交付物**

1. **規則引擎**（最小集）：
   completeness（非空比例）、uniqueness（PK）、freshness（max event_time vs
   業務日曆）、range、referential integrity（edge → node）、
   bitemporal overlap（第三批 B3-2 後補）、
   reconciliation（source-to-target count 同 amount 總和）。
2. **`DQGate`**：`evaluate(snapshot_id, rules) -> GateDecision { pass, failures }`。
3. **發布閘門**：Risk/AML/ALM/FTP 正式結果發布前必須過 gate；fail 則 block 並附原因。
4. **Override 流程**：override 必須有原因 + 批准人 + 審計記錄（寫入 B2-1 catalog）。

**驗收條件**

- [ ] DQ 未達門檻時，正式結果**無法發布**（唔係只出 warning）。
- [ ] 每個 gate failure 有具體規則、欄位、實際值、門檻值。
- [ ] 每次 override 都有原因 / 批准人 / 時間 / 對象，可事後審計。
- [ ] 對賬報表：source vs target row count 同 amount sum 一致或列出差異。
- [ ] gate 決策連同 B2-3 execution_id 一齊記錄。

**風險**

- 規則門檻需要業務定義，技術層面應做成可配置而唔係硬編碼。
- referential integrity 對大圖可能慢，需 pushdown / 抽樣策略。

---

## 7. 本批完成定義（Definition of Done）

- [ ] B2-1 ~ B2-5 全部驗收條件通過。
- [ ] 一個端到端 demo：`載入 → catalog 提交 → 建 embedding index → 查詢 →
      lineage 記錄 → DQ gate → 按 execution_id 重演`。
- [ ] `cargo test --workspace` 全綠；crash-injection 測試。
- [ ] 文件：`doc/prod_p2_design.md`、操作手冊、catalog schema 文件。
- [ ] 由 legacy `catalog.tsv` 可遷移，舊 CLI 行為不變。

---

## 8. 明確唔喺本批做

- Streaming / Kafka / watermark（→ 第三批 B3-1）
- Bitemporal 雙時間軸（→ 第三批 B3-2）
- Filtered ANN 策略 / rerank / IVF k-means / CBO / workload（→ 第三批 B3-3 ~ B3-6）
- Compute-storage 分離、分散式 catalog、tiering、security、multi-tenant（→ 企業批，未排期）
