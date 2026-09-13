# prod_p4 設計 — 企業批（Milestone D / E）

> 計劃文件：`prod_p4.md`
> 上層路線圖：`BANKING_ANALYTICS_GAP_ROADMAP.md`
> 本文描述企業批嘅**架構邊界、crate 佈局、接入契約**同首批（D1/D6）嘅資料模型。

---

## 1. 為何要一條硬邊界

`gtv-core` / `gtv-engine` / `gtv-index` / `gtv-pattern` 係 analysis engine 主執行路徑，
已由 p1–p3 三批固化（metric contract、adaptive CSR、bounded traversal、HNSW layout、
catalog、streaming、bitemporal、ANN、CBO、workload）。企業批係**領域 + 營運**能力，
同 kernel 演算法無關。若任由領域規則 / 安全 / 分散式邏輯長入 kernel：

- kernel 會被業務 version、audit、policy 污染，難以再獨立演進；
- 回歸測試面爆炸；
- 非目標項目（見 roadmap §5）會偷偷進入主路徑。

因此企業批採**單向依賴 + plug-in 接入**。

```
              ┌─────────────────────────────────────────────┐
              │            enterprise crates                 │
              │  gtv-scenario  gtv-refdata  gtv-governance   │
              │  gtv-observe   gtv-security  gtv-ops         │
              └───────────────┬─────────────────────────────┘
                              │ depends on (one-way, read-only)
                              ▼
   ┌──────────────────────────────────────────────────────────┐
   │  analysis engine: gtv-engine, gtv-index, gtv-pattern,     │
   │                  gtv-array, gtv-core, gtv-catalog, ...    │
   └──────────────────────────────────────────────────────────┘
        ▲
        │ 接入點：UDF / table function registration, trait impl,
        │         wrapper service — 全部唔改 kernel 內部
```

**驗證方式**：CI 加一條 dependency-direction check（`cargo tree` / script），
確認 engine crates 嘅 `Cargo.toml` 冇出現 `gtv-scenario` / `gtv-refdata` /
`gtv-governance` / `gtv-observe` / `gtv-security` / `gtv-ops`。

---

## 2. Crate 佈局與職責

| crate | 職責 | 依賴（單向） |
|---|---|---|
| `gtv-scenario` | scenario catalog / 版本 / 繼承 / cube / curve / deterministic rerun | `gtv-core`（型別）、`gtv-catalog`（版本登記，可選） |
| `gtv-refdata` | hierarchy / master data / reference data / effective dating | `gtv-core`（bitemporal 型別） |
| `gtv-governance` | CRM 規則治理、audit、AML case / explainability | `gtv-scenario`、`gtv-refdata`、`gtv-catalog` |
| `gtv-observe` | telemetry 聚合、SLO 報表 | `gtv-engine`（**唯讀** metrics 介面） |
| `gtv-security` | identity / RBAC / policy / encryption / audit | `gtv-server`（wrapper）、`gtv-catalog` |
| `gtv-ops` | storage backend / distributed catalog / tiering / HA | `gtv-catalog`、`gtv-storage` |

> `gtv-observe` 係唯一會「見到」`gtv-engine` 嘅企業 crate，而且只可以經既有
> `GtvContext::prometheus()` / `monitor` 唯讀介面，**唔可以**呼叫 kernel 內部。

---

## 3. 接入契約（唔改 kernel）

### 3.1 領域 UDF / table function

沿用現有模式：企業 crate 暴露純函式 / `TableFunctionImpl`，由 CLI / server 喺
`GtvContext` 上註冊。例如（D1 之後）：

```rust
// gtv-cli（或 gtv-server）嘅註冊點 — 只加註冊，唔改 engine
ctx.register_udf(gtv_scenario::resolve_scenario_udf(catalog.clone()));
ctx.register_udtf("scenario_shocks", gtv_scenario::ScenarioShocksTableFunction::new(catalog));
```

- UDF 回 Arrow 結果，經 DataFusion 當普通函式呼叫；
- **唔**加 `PhysicalOptimizerRule`、**唔**改 `KernelPlan`、**唔**改 ANN/CSR。

### 3.2 營運 wrapper

- `gtv-security` 以 middleware 包住 `gtv-server` 嘅 request path，唔改 handler 邏輯。
- `gtv-ops` 以 `StorageBackend` / `CatalogClient` trait 注入，唔改 `gtv-storage`
  嘅檔案格式。
- `gtv-observe` 只讀 engine 已輸出嘅 Prometheus 文字 / monitor 計數器。

---

## 4. D1 Risk Scenario 資料模型（首批落地）

### 4.1 核心型別

```rust
pub enum ScenarioKind { Baseline, Stress, Adverse, ReverseStress }

pub struct Dimension {
    pub legal_entity: Option<String>,
    pub portfolio: Option<String>,
    pub product: Option<String>,
    pub currency: Option<String>,
}

pub struct Shock {
    pub factor: String,          // e.g. "IR.USD.5Y", "FX.USDCNY", "PD.Mortgage"
    pub dimension: Dimension,    // 適用範圍（空 = 全部）
    pub value: f64,              // 絕對值或相對 shock（由 kind 演繹）
}

pub struct Scenario {
    pub id: String,
    pub version: u32,
    pub kind: ScenarioKind,
    pub parent: Option<(String, u32)>,   // 繼承來源
    pub dimensions: Dimension,           // scenario 自身適用維度
    pub shocks: Vec<Shock>,
    pub source_cutoff: i64,              // 資料 cutoff（event/business time）
    pub model_version: String,
    pub status: ScenarioStatus,          // Draft | Approved | Retired
}
```

### 4.2 解析（繼承 + override）

`ScenarioCatalog::resolve(id, version) -> ResolvedScenario`：

1. 由 target 沿 `parent` 鏈向上，收集所有 ancestor；
2. **循環鏈**（A→B→A）→ `ScenarioError::CyclicInheritance`；
3. 由 root 到 leaf 逐層 apply shock；key = `(factor, dimension)`；
   後者（child）覆蓋前者（parent）；
4. 輸出 `ResolvedScenario { shocks: Vec<(Shock, Provenance)>, chain: Vec<(id, version)> }`；
   `Provenance = (scenario_id, version)`，令每個值都可解釋；
5. shock 以 `(factor, dimension)` 排序，保證**逐位元確定性**。

### 4.3 Deterministic rerun / reconciliation

- `ResolvedScenario` 帶 `source_cutoff` + `model_version` + `chain`；
  同一 (id, version) 永遠同一結果。
- `diff(a, b)` 回傳兩 resolved scenario 嘅 per-shock 差異（新增 / 刪除 / 改值），
  用於 reconciliation 同 explainability。

### 4.4 持久化

首版用 `gtv-catalog` 嘅版本化 metadata（append-only snapshot）登記 scenario；
scenario 內容為不可變 version，更正 = append 新 version，沿用 bitemporal 語意。

---

## 5. D6 Hierarchy / Reference Data 資料模型

```rust
pub struct HierarchyEdge {
    pub parent: String,
    pub child: String,
    pub level: HierarchyKind,     // LegalEntity | Organisation | Product | ...
    pub valid_from: i64,
    pub valid_to: i64,            // OPEN_ENDED = i64::MAX
}
```

- 重用 `gtv_core::BitemporalRange` 做 effective dating；
- `ancestors(id, as_of)` / `descendants(id, as_of)` / `rollup(id, as_of)` 對 oracle 測試；
- 循環偵測於 insert 時拒絕；
- reference data（curve / calendar / currency / jurisdiction）以 effective-dated key-value 存。

---

## 6. E 系列（營運）設計要點

### E1 Compute / Storage Separation

- `StorageBackend` trait：`get / put / list / delete`（物件儲存或共享 FS）；
- query worker 變 stateless：所有狀態喺 catalog + object store；
- local SSD cache（LRU + warming），cache key 用 file_id + checksum。

### E2 Distributed Partition Catalog

- catalog 加 `shard_map_version`；partition ownership 由 coordinator 指派；
- metadata update 走兩階段（prepare → commit）令讀者只見到完整版本；
- stale client 帶舊 shard_map_version → 拒絕並要求 refresh。

### E3 Tiering

- 分層由 `storage_tier` metadata 驅動；promotion/demotion 係 catalog append（可審計）；
- Hot/Warm/Cold 對應唔同壓縮 / 索引格式；legal hold 阻止刪除。

### E4 / E5 Security & Multi-Tenant

- identity → principal → role → policy（table/column/row）；
- tenant_id 由 principal 強制注入，唔可以由 query 參數覆蓋；
- per-tenant encryption context + quota（重用 B3-6 workload 概念但喺 server 層）；
- audit log append-only + hash chain。

### E6 Observability

- 定義標準 metric registry（名稱 / 單位 / 型別），來源可為 engine Prometheus、
  catalog stats、stream metrics、workload status；
- 輸出文字 / JSON，唔硬綁後端。

### E7 HA / DR

- query service 多副本 + health probe；catalog / manifest 定時備份 + checksum；
- index snapshot 多副本；restore test 為可重複 CI job；
- degraded read mode = 唯讀、停 ingestion。

---

## 7. 測試策略

1. **每個新 crate 自帶 unit test**（純型別 / 解析 / 版本）。
2. **property test**：scenario resolution 確定性、hierarchy 對 oracle。
3. **boundary check**：CI script 驗證依賴方向。
4. **零回歸**：企業批唔改 engine crate，所以 `cargo test --workspace` 內
   engine / index / pattern 測試數字必須不變。
5. **integration**：由 CLI / server 註冊 UDF 後，用 SQL 驗證端到端（D1 之後）。
