# prod_p2 設計文件 — 可重演分析基礎（Catalog 為核心）

> 對應批次清單：`prod_p2.md`
> 上層路線圖：`BANKING_ANALYTICS_GAP_ROADMAP.md`
> 範圍：B2-1 Catalog/Manifest、B2-2 Index lifecycle、B2-3 Execution lineage、
> B2-4 Embedding schema、B2-5 DQ gates。

---

## 1. 總覽

本批新增 **`gtv-catalog`** 為 metadata control plane，係後續所有「可重演、
可稽核、原子發布」能力嘅地基。

```
                       ┌──────────────────────────────┐
                       │         gtv-catalog           │
                       │  TableId / SchemaVersion      │
                       │  PartitionSpec / DataFile     │
                       │  Snapshot / atomic commit     │
                       │  lineage store / DQ rules     │
                       └───────┬───────────┬───────────┘
                               │           │
              ┌────────────────┘           └───────────────┐
              ▼                                             ▼
     gtv-storage (files)                        gtv-index-store (B2-2)
     atomic parquet/HDB                                  │
                                                         ▼
                                        gtv-engine (lineage hook B2-3,
                                        embedding table B2-4, DQ gate B2-5)
```

**設計原則**

1. Catalog 係唯一權威 metadata 來源；資料檔只係 immutable blobs。
2. 所有提交係 atomic：要麼完整可見，要麼完全不可見（無中間態）。
3. 讀者以 `SnapshotId` 為單位隔離；唔會讀到 half-written batch。
4. 所有 downstream（index、embedding、lineage、DQ）都引用 `SnapshotId`。

---

## 2. B2-1 Catalog / Manifest / Schema evolution

### 2.1 新 crate 結構

```
crates/gtv-catalog/
├── Cargo.toml           # arrow, gtv-storage, uuid, blake3, serde, serde_json, chrono
└── src/
    ├── lib.rs           # re-exports
    ├── id.rs            # TableId, SnapshotId, DataFileId, CommitId, IndexId
    ├── schema.rs        # SchemaVersion, evolution compat rules
    ├── partition.rs     # PartitionSpec, transforms
    ├── manifest.rs      # DataFile, Snapshot
    ├── store.rs         # Catalog trait + FsCatalog (filesystem impl)
    ├── commit.rs        # atomic commit protocol
    └── error.rs
```

> `gtv-catalog` 依賴 `gtv-storage`（檔案 IO）同 `arrow`（schema）。
> **唔依賴** `gtv-engine`，避免 cycle。

### 2.2 ID 型別（`id.rs`）

```rust
pub struct TableId(pub Uuid);      // immutable, created once
pub struct SnapshotId(pub Uuid);
pub struct DataFileId(pub Uuid);
pub struct CommitId(pub Uuid);
pub struct IndexId(pub Uuid);
```

- 用 `uuid::Uuid::new_v4()`；序列化做字串（36 chars）或 16 bytes binary。
- `TableId` 一旦建立永不變；表改名只改 `TableMeta.name` 映射，唔改 id。

### 2.3 Schema registry（`schema.rs`）

```rust
pub struct SchemaVersion(pub u32);

pub struct SchemaRecord {
    pub version: SchemaVersion,
    pub arrow_schema: SchemaRef,
    pub created_at: i64,
    pub comment: Option<String>,
}

pub enum SchemaChange {
    AddColumn { field: Field, default: Option<String> },
    RenameColumn { from: String, to: String },
    WidenType { column: String, to: DataType },
}

pub fn check_compatible(old: &SchemaRef, new: &SchemaRef) -> Result<Vec<SchemaChange>>;
pub fn evolve(table: &TableId, change: SchemaChange) -> Result<SchemaVersion>;
```

**相容性規則（freeze）**

| 變更 | 允許 | 條件 |
|---|---|---|
| add nullable column | ✅ | 舊檔案讀取時補 null |
| add non-null column | ❌ | 舊檔案冇值 |
| rename | ✅ | 只改 catalog 映射，檔案欄名不變（讀取時映射） |
| widen int32→int64、float32→float64、date→timestamp | ✅ | Arrow cast 安全 |
| narrow / 型別不相容 | ❌ | 回 `SchemaIncompatible` |
| drop column | ✅（軟刪） | 保留歷史 schema，舊 snapshot 仍可讀 |

- 舊分區永遠按其 `schema_version` 解讀；讀取時如 schema 已進化，套 column mapping。
- Schema history 以 JSON list 存放 `metadata/<table>/schemas.json`。

### 2.4 Partition spec（`partition.rs`）

```rust
pub enum Transform {
    Identity,                 // 直接用欄位值
    DateTrunc { unit: TimeUnit },  // day / hour
    HashBucket { buckets: u32 },   // murmur3(col) % buckets
}

pub struct PartitionColumn { pub source: String, pub transform: Transform, pub name: String }

pub struct PartitionSpec {
    pub version: u32,
    pub columns: Vec<PartitionColumn>,
}
```

- 解決現時 `date/table/symbol` 小檔案問題：改用
  `date/table/hash_bucket` 或完全可配置。
- Partition spec 版本化：改 spec = 新版本，舊檔案按舊 spec 解讀。
- 檔案路徑模板：`data/<table_id>/<spec_version>/<partition_values>/<file_id>.parquet`。

### 2.5 Manifest（`manifest.rs`）

```rust
pub struct ColumnStat {
    pub name: String,
    pub null_count: u64,
    pub min: Option<ScalarValue>,
    pub max: Option<ScalarValue>,
    pub distinct_est: Option<u64>,
}

pub struct SourceOffset {
    pub source: String,
    pub partition: i32,
    pub offset: i64,
}

pub struct DataFile {
    pub file_id: DataFileId,
    pub path: String,
    pub format: FileFormat,           // Parquet
    pub row_count: u64,
    pub size_bytes: u64,
    pub column_stats: Vec<ColumnStat>,
    pub event_time_min: i64,
    pub event_time_max: i64,
    pub schema_version: SchemaVersion,
    pub partition: Vec<PartitionValue>,
    pub checksum: [u8; 32],            // blake3
    pub source_offsets: Vec<SourceOffset>,
    pub commit_id: CommitId,
}

pub enum CommitOp { Append, Overwrite, Delete }

pub struct Snapshot {
    pub snapshot_id: SnapshotId,
    pub parent: Option<SnapshotId>,
    pub table_id: TableId,
    pub schema_version: SchemaVersion,
    pub spec_version: u32,
    pub files: Vec<DataFileId>,
    pub op: CommitOp,
    pub summary: serde_json::Value,
    pub created_at: i64,
}
```

- Snapshot 用 file id 引用 DataFile（DataFile 內容存 `metadata/<table>/files/<file_id>.json`
  或集中一個 `files.jsonl`，append-only）。
- 為避免 manifest 過大，可選：每 k 個 snapshot 做一次 manifest compaction。

### 2.6 檔案佈局

```
<catalog_root>/
  version-hint                       # 指向最新 metadata version（text）
  metadata/
    tables.json                      # name -> TableId 映射
    <table_id>/
      meta.json                      # TableMeta（name, spec_version, created）
      schemas.json                   # schema 歷史
      snapshots.jsonl                # append-only snapshot 記錄
      files.jsonl                    # append-only DataFile 記錄
      manifests/
        <snapshot_id>.json           # 完整 snapshot manifest
  data/
    <table_id>/<spec_version>/<partition...>/<file_id>.parquet
  tmp/
    <uuid>.parquet                   # 未提交檔案
  deadletter/                        # B3-1 用
```

### 2.7 Atomic commit protocol（`commit.rs`）

```
commit(table_id, op, new_files) -> SnapshotId:
  1. parent = latest(table_id)                        # 讀目前 SnapshotId
  2. for each new file:
       write data/<...>/<file_id>.parquet.tmp
       fsync(file)
       checksum = blake3(file)
       atomic_rename(tmp -> data/<...>/<file_id>.parquet)
       fsync(parent_dir)
  3. manifest = Snapshot { snapshot_id: new_uuid, parent, files: parent.files ∪ new, ... }
  4. write manifests/<snapshot_id>.json.tmp
     fsync(file); atomic_rename(-> manifests/<snapshot_id>.json); fsync(dir)
  5. append snapshot 記錄到 snapshots.jsonl（append + fsync）
  6. update version-hint 指向新 snapshot（atomic write + rename）
  return snapshot_id
```

- Step 4/5 之間若 crash：`snapshots.jsonl` 可能落後，但 `manifests/<id>.json`
  已存在。恢復時以 manifests 目錄為準補寫 jsonl（idempotent）。
- **讀者只透過 `latest()` 或明確 `snapshot(id)` 讀已提交 manifest**，
  唔會掃 `data/` 目錄，所以半寫檔案不可見。
- 所有 fsync 用 `std::fs::File::sync_all`；目錄 fsync 需 `File::open(dir)` +
  `sync_all`（Linux/macOS）。

### 2.8 Catalog trait（`store.rs`）

```rust
pub struct TableMeta {
    pub table_id: TableId,
    pub name: String,
    pub spec_version: u32,
    pub created_at: i64,
}

pub trait Catalog: Send + Sync {
    fn create_table(&self, name: &str, schema: SchemaRef, spec: PartitionSpec) -> Result<TableId>;
    fn table(&self, name: &str) -> Result<TableMeta>;
    fn table_by_id(&self, id: TableId) -> Result<TableMeta>;

    fn latest(&self, table: TableId) -> Result<SnapshotId>;
    fn snapshot(&self, table: TableId, id: SnapshotId) -> Result<Snapshot>;
    fn files(&self, table: TableId, snap: SnapshotId) -> Result<Vec<DataFile>>;
    fn scan(&self, table: TableId, snap: SnapshotId, filter: &ScanFilter) -> Result<Vec<DataFile>>;

    fn commit(&self, table: TableId, op: CommitOp, files: Vec<NewFile>) -> Result<SnapshotId>;
    fn evolve_schema(&self, table: TableId, change: SchemaChange) -> Result<SchemaVersion>;

    // lineage / dq (B2-3 / B2-5)
    fn append_lineage(&self, rec: &ExecutionRecord) -> Result<()>;
    fn lineage(&self, id: ExecutionId) -> Result<Option<ExecutionRecord>>;
}
```

`FsCatalog` 為預設實作；日後可換 object-store / 分散式（企業批）。

### 2.9 整合 `gtv-storage`

- `gtv-storage/src/parquet.rs` 保留現有 `write_batch/read_batches`。
- 新增 `gtv-storage/src/atomic.rs`：
  ```rust
  pub fn write_atomic(path: &Path, batch: &RecordBatch) -> Result<[u8;32]>;
  pub fn fsync_dir(dir: &Path) -> Result<()>;
  ```
- `HdbStore` 改為經 `gtv-catalog` 提交（保留舊 `write_partition` 做 legacy）。

### 2.10 遷移 `gtv-cli/src/catalog.rs`

- `Catalog`（CLI）改為 `FsCatalog` façade：
  - `record_csv/record_parquet/record_snapshot` → `create_table` + `commit`。
  - `replay` → 讀 `latest()` + `scan()`。
- 提供 `gtv-cli` 命令 `catalog import-legacy <home>`：讀舊 `catalog.tsv`，
  為每個 entry 建表 + 提交單一 DataFile。

### 2.11 測試

| 測試 | 方法 |
|---|---|
| 原子性（crash injection） | 在 commit 各 step 前後 kill process，重啟後讀者只見完整 snapshot |
| 冪等重播 | 同一 commit 重跑不產生重複 entry |
| Reader isolation | 併發寫入 + 讀取，讀者唔會見到半寫檔案 |
| Schema evolution | 加欄/改名/widen 後舊分區可讀；不相容被拒 |
| Partition pruning | `scan` filter 只回相關 DataFile（用 min/max stats） |
| Legacy import | `catalog.tsv` 全部表可 replay |
| 校驗 | 篡改 data file → checksum 失敗 |

---

## 3. B2-2 Vector index lifecycle

### 3.1 新 crate `gtv-index-store`

```
crates/gtv-index-store/
├── Cargo.toml        # gtv-index, gtv-catalog, gtv-storage, blake3, serde
└── src/
    ├── lib.rs
    ├── manifest.rs    # IndexManifest
    ├── format.rs      # .gtvidx versioned container
    ├── store.rs       # save/load/rebuild/swap/rollback
    └── error.rs
```

> 唔放入 `gtv-index`，因為 `gtv-index` 要對 `gtv-storage`/`gtv-catalog`
> 保持零依賴（kernel 純淨）；亦避免 cycle。

### 3.2 Index manifest

```rust
pub struct IndexManifest {
    pub index_id: IndexId,
    pub table_id: TableId,
    pub index_type: IndexType,          // Flat | Ivf | Hnsw
    pub corpus_snapshot_id: SnapshotId, // 權威來源
    pub source_file_ids: Vec<DataFileId>,
    pub model_id: String,
    pub model_version: String,
    pub embedding_model: String,
    pub dim: u32,
    pub metric: Metric,
    pub build_params: serde_json::Value,
    pub build_ts: i64,
    pub checksum: [u8; 32],
    pub engine_version: String,
    pub row_count: u64,
    pub tombstone_count: u64,
}
```

### 3.3 `.gtvidx` 容器格式

```
offset 0:  magic      "GTVIDX\0\0" (8 bytes)
offset 8:  version    u16           (目前 = 1)
offset 10: flags      u16
offset 12: manifest_len u32
offset 16: manifest_json (utf8)
offset ..: payload (index-type specific, little-endian)
末尾:      checksum   [u8;32]  (blake3 over bytes[0..len-32])
```

- HNSW payload：直接沿用 B1-4 `HnswIndex::to_bytes`。
- IVF payload：`nlist, nprobe, dim, metric, centroids(nlist*dim f32),
  list_offsets(nlist+1 u32), ids(n u64), data(n*dim f32)`。
- Flat payload：`dim, metric, ids(n u64), data(n*dim f32)`。

### 3.4 Store / versioning / swap

```
index/<index_id>/
  v1/  index.gtvidx  manifest.json
  v2/  index.gtvidx  manifest.json
  CURRENT            # text: "v2"（atomic write + rename）
```

```rust
pub struct IndexStore { root: PathBuf }
impl IndexStore {
    pub fn save(&self, index: &dyn PersistableIndex, manifest: IndexManifest) -> Result<SnapshotRef>;
    pub fn load(&self, index_id: IndexId, version: Option<u32>) -> Result<LoadedIndex>;
    pub fn rebuild(&self, table: TableId, snap: SnapshotId, params: BuildParams) -> Result<IndexManifest>;
    pub fn swap_current(&self, index_id: IndexId, version: u32) -> Result<()>;   // CAS
    pub fn rollback(&self, index_id: IndexId, version: u32) -> Result<()>;
    pub fn verify(&self, index_id: IndexId, version: u32) -> Result<()>;
}
```

- **Shadow build**：`rebuild` 寫去新 `v<n+1>/`，期間 `CURRENT` 仍指舊版本，
  查詢不中斷。
- **Atomic swap**：`CURRENT` 用 temp + fsync + rename 更新。
- **Rollback**：`CURRENT` 指回舊版本（舊版本保留）。
- **GC**：保留最近 k 個版本（可配），其餘刪除。

### 3.5 `PersistableIndex` trait

```rust
pub trait PersistableIndex: Send + Sync {
    fn index_type(&self) -> IndexType;
    fn to_bytes(&self) -> Vec<u8>;
    fn checksum(&self) -> [u8; 32];
    fn search(&self, query: &[f32], k: usize, filter: Option<&BooleanArray>) -> Result<Vec<VectorHit>>;
}
```

`FlatIndex` / `IvfIndex` / `HnswIndex` 都 impl（HNSW 用 B1-4 序列化）。

### 3.6 接線引擎

- `gtv-engine` 加：
  ```rust
  pub enum IndexHandle { Memory(KnnCollection), Persisted(LoadedIndex) }
  pub fn register_persisted_index(&self, name: &str, handle: LoadedIndex, metric: Metric, dim: usize);
  ```
- `knn` UDTF 統一走 `IndexHandle::search`；不再只靠暴力 `KnnCollection`。
- `IvfIndex` 正式有 SQL 路徑。

### 3.7 測試

| 測試 | 方法 |
|---|---|
| Round-trip | save → load，Top-K 一致 |
| Rebuild | 由權威 table + snapshot rebuild，結果 == 直接 build |
| Shadow swap | 併發查詢下切換，P99 不中斷 |
| Corruption | 篡改 bytes → checksum 失敗 |
| Rollback | 指回舊版本，結果 == 舊版本 |
| Ivf SQL 路徑 | 至少一條查詢會用 IVf 且 Top-K 正確 |

---

## 4. B2-3 Execution context / lineage / replay

### 4.1 資料結構

```rust
pub struct ExecutionId(pub Uuid);

pub struct TableRef { pub table_id: TableId, pub snapshot_id: SnapshotId, pub schema_version: SchemaVersion }
pub struct IndexRef { pub index_id: IndexId, pub snapshot: SnapshotRef, pub metric: Metric, pub dim: u32 }
pub struct ModelRef { pub model_id: String, pub version: String }
pub struct UdfRef { pub name: String, pub version: String, pub hash: [u8;32] }
pub struct ScenarioRef { pub scenario_id: String, pub version: String }

pub struct ExecutionRecord {
    pub execution_id: ExecutionId,
    pub query_text: String,
    pub query_hash: [u8; 32],
    pub engine_version: String,
    pub source_snapshots: Vec<TableRef>,
    pub source_offsets: Vec<SourceOffset>,
    pub model_versions: Vec<ModelRef>,
    pub embedding_model_versions: Vec<ModelRef>,
    pub vector_index_snapshots: Vec<IndexRef>,
    pub scenario_version: Option<ScenarioRef>,
    pub business_cutoff: Option<i64>,
    pub udf_versions: Vec<UdfRef>,
    pub runtime_params: serde_json::Value,
    pub output_checksum: [u8; 32],
    pub output_rows: u64,
    pub started_at: i64,
    pub finished_at: i64,
}
```

### 4.2 Lineage hook

```rust
pub struct LineageOptions {
    pub business_cutoff: Option<i64>,
    pub scenario: Option<ScenarioRef>,
    pub runtime_params: serde_json::Value,
    pub pin_snapshots: bool,     // true = 用已記錄 snapshot，唔用 latest
}

impl GtvContext {
    pub async fn execute_with_lineage(
        &self,
        sql: &str,
        opts: LineageOptions,
    ) -> Result<(Vec<RecordBatch>, ExecutionRecord)>;
}
```

流程：

1. `ctx.sql(sql)` 得 logical plan。
2. 由 plan 抽出所引用表名 / UDF 名（`plan.visit` / `display` 解析）。
3. 經 `gtv-catalog` 把表名 → `TableRef`（用 latest 或 pinned snapshot）。
4. 執行；對每個 output batch 累加 blake3。
5. 組 `ExecutionRecord`，`append_lineage` 寫入 catalog。
6. 回傳 batches + record。

### 4.3 Replay

```rust
pub async fn replay(ctx: &GtvContext, catalog: &dyn Catalog, id: ExecutionId)
    -> Result<Vec<RecordBatch>>;
```

- 讀 `ExecutionRecord`。
- 重新 register 所有 `TableRef`（用 pinned `snapshot_id`）同 `IndexRef`。
- 重跑 `query_text`；比對 output checksum。
- 若 checksum 不符 → `ReplayMismatch`，附差異資訊。

### 4.4 Determinism 防護

- UDF registry 記 `version` + `hash`。
- Nondeterministic UDF（rand / now / uuid）標記 `nondeterministic = true`。
- Replay 遇到→回 `ReplayNondeterministic { udf }`（除非 `allow_nondeterministic`）。
- 浮點：定義「byte-identical」為預設；若 UDF 引入非確定性浮點，需明確標記。

### 4.5 Lineage 儲存

- `metadata/lineage/<date>/<execution_id>.json`（append-only）。
- 或用 `gtv-catalog` 一個 append-only Parquet 表，方便 SQL 查詢。
- 索引：`execution_id` → 檔案；另有 `table_id/snapshot_id` 反向索引（可選）。

### 4.6 測試

| 測試 | 方法 |
|---|---|
| Reproducibility | 同 SQL 執行兩次，output checksum 一致 |
| Replay | 記錄後 replay，checksum 一致 |
| Pinned snapshot | 執行後再寫入新資料，replay 仍用舊 snapshot |
| Nondeterministic | rand UDF → replay 拒絕 |
| Table extraction | 多表 / 子查詢 / UDTF 都抽到正確 TableRef |

---

## 5. B2-4 Embedding 標準 schema 與治理

### 5.1 Arrow schema

```rust
pub fn embedding_schema(dim: i32) -> SchemaRef {
    Schema::new(vec![
        Field::new("entity_id", DataType::UInt64, false),
        Field::new("embedding",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)), dim), false),
        Field::new("model_id", DataType::Utf8, false),
        Field::new("model_version", DataType::Utf8, false),
        Field::new("tokenizer_version", DataType::Utf8, true),
        Field::new("dimension", DataType::UInt32, false),
        Field::new("distance_metric", DataType::Utf8, false),
        Field::new("normalized", DataType::Boolean, false),
        Field::new("created_at", DataType::Int64, false),
        Field::new("effective_from", DataType::Int64, false),
        Field::new("effective_to", DataType::Int64, true),
        Field::new("source_hash", DataType::Utf8, false),
        Field::new("feature_version", DataType::Utf8, false),
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("classification", DataType::Utf8, true),
    ])
}
```

### 5.2 驗證（`gtv-catalog/src/embedding.rs` 或 `gtv-embedding`）

```rust
pub fn validate_embedding_batch(schema: &SchemaRef, batch: &RecordBatch) -> Result<()>;
```

檢查：

- `FixedSizeList` 長度 == `dimension` 欄（每行）。
- 同一 index 只允許單一 `model_id` / `model_version` / `dimension` /
  `distance_metric` / `normalized`。
- `source_hash` 非空。
- `effective_from <= effective_to`（若有）。

### 5.3 治理 / 生命週期

- `effective_to` 到咗 → 檢索唔再命中（filter 用 business/system time，視 B3-2）。
- 替換：新 `model_version` 寫新列，舊列設 `effective_to`。
- 重建：由權威 embedding table + snapshot 重新 build index（B2-2 `rebuild`）。

### 5.4 與索引整合

```rust
pub struct EmbeddingIndexSpec {
    pub name: String,
    pub table: TableId,
    pub column: String,          // "embedding"
    pub model_id: String,
    pub model_version: String,
    pub metric: Metric,
    pub dim: u32,
    pub index_type: IndexType,
}
```

- 由 spec + snapshot 讀 embedding → build index → `IndexStore::save`。
- `KnnCollection` 未來由 `LoadedIndex` 取代（B2-2 已接線）。

### 5.5 測試

| 測試 | 方法 |
|---|---|
| Dimension 驗證 | FixedSizeList 長度不符 → 拒 |
| 混用防護 | 混 model / dim / metric → 拒 |
| 追溯 | 檢索結果可列出 model_id/version/source_hash/feature_version |
| 過期 | `effective_to` 過後唔再命中 |
| Tenant 隔離 | 跨 tenant 查詢唔互相命中 |

---

## 6. B2-5 資料質量與對賬閘門

### 6.1 規則模型

```rust
pub enum DqRule {
    Completeness { column: String, min_ratio: f64 },
    Uniqueness   { columns: Vec<String> },
    Freshness    { event_time_col: String, max_lag_ns: i64 },
    Range        { column: String, min: Option<f64>, max: Option<f64> },
    Referential  { child: TableId, child_col: String, parent: TableId, parent_col: String },
    BitemporalOverlap { key: Vec<String>, business_from: String, business_to: String },
    Reconciliation { source_rows: i64, target_rows: i64, tolerance: f64,
                     source_sum: Option<f64>, target_sum: Option<f64> },
}

pub struct DqFailure {
    pub rule: String, pub column: Option<String>,
    pub observed: f64, pub threshold: f64, pub message: String,
}
pub struct GateDecision { pub pass: bool, pub failures: Vec<DqFailure> }
```

### 6.2 Gate 流程

```rust
pub trait DqGate {
    fn evaluate(&self, table: TableId, snap: SnapshotId, rules: &[DqRule]) -> Result<GateDecision>;
}
```

- Risk/AML/ALM/FTP 結果發布函式呼叫 `evaluate`；`pass == false` → 拒絕發布。
- 用 catalog `column_stats` + 抽樣 / 全掃（視規則）。
- 決策連同 `ExecutionId` 記錄。

### 6.3 Override

```rust
pub struct OverrideRecord {
    pub rule: String, pub reason: String, pub approver: String,
    pub approved_at: i64, pub execution_id: ExecutionId, pub target: String,
}
```

- Override 寫入 catalog（append-only），可事後審計。
- 未經 override 唔可以繞過 gate。

### 6.4 整合 `monitor.rs`

- 現有 `dq_analyze` / `dq_report` / `health_check` 保留為「診斷」。
- 新增 `dq_gate` 路徑用上述規則 + gate decision。
- SQL：`dq_gate('table', rules_json)` 或經 config file。

### 6.5 測試

| 測試 | 方法 |
|---|---|
| Fail blocks publish | 觸犯規則時發布被拒 |
| Failure 詳情 | 回報規則 / 欄位 / 實際值 / 門檻 |
| Override 審計 | override 有原因 / 批准人 / 時間 |
| Reconciliation | source/target count 同 sum 一致或列差異 |
| Lineage 連結 | gate 決策連 execution_id |

---

## 7. 交付順序

| 里程碑 | 內容 | 出口 |
|---|---|---|
| M1 | B2-1 catalog + atomic commit + legacy import | crash-injection 通過；讀者隔離成立 |
| M2 | B2-4 embedding schema + B2-2 index lifecycle | 索引可持久化 / 重建 / swap |
| M3 | B2-3 lineage + replay | 端到端重演 checksum 一致 |
| M4 | B2-5 DQ gate | 未達標阻止發布；override 可審計 |

## 8. 風險登記

| 風險 | 影響 | 緩解 |
|---|---|---|
| Atomic commit 跨 OS 語意差異 | 高 | crash-injection 測試；封裝 fsync/rename |
| Schema compat 規則定錯 | 高 | 先 freeze 規則 + 大量測試 |
| Catalog 成為單點 | 中 | 先本地 `FsCatalog`；分散式留企業批 |
| Index 格式與 engine 版本耦合 | 中 | version header + 明確 rebuild 策略 |
| 「重演」定義模糊 | 中 | P0 定 byte-identical；浮點 UDF 標記 |
| DQ 規則對大表太慢 | 中 | 用 column_stats + 抽樣；pushdown |
| 新 crate 過多 | 低 | 只開 `gtv-catalog` + `gtv-index-store`；其餘用 module |
