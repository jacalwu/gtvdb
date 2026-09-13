# prod_p4 — 企業批：Milestone D（銀行領域）＋ Milestone E（企業營運）

> 上層路線圖：`BANKING_ANALYTICS_GAP_ROADMAP.md`（P2 / P3）
> 設計文件：`doc/prod_p4_design.md`
> **命名注意**：`prod_p1` ~ `prod_p3` 係 analysis engine 嘅三批；呢一批係**企業批**，
> 對應 roadmap 嘅 **Milestone D（P2：銀行業務能力）** 同 **Milestone E（P3：規模化/
> 可靠性/運維）**。佢**唔係** analysis engine 嘅延續，亦**唔會改** engine 主執行路徑。

---

## 0. 定位與硬邊界

前三批（p1–p3）已完成：metric contract、adaptive CSR / bounded traversal、HNSW 重構、
catalog / index lifecycle / lineage / embedding 治理 / DQ gate、streaming / bitemporal /
filtered ANN / IVF k-means / CBO / workload 隔離。企業批在此之上疊加**銀行領域能力**同
**企業營運能力**。

**硬邊界（唔可以違反）：**

1. **依賴單向**：企業批嘅 crate 可以依賴 `gtv-core` / `gtv-catalog` / `gtv-storage` /
   `gtv-array`，但 `gtv-core` / `gtv-engine` / `gtv-index` / `gtv-pattern` **唔可以**
   反過來依賴企業批 crate。analysis engine 主路徑零污染。
2. **接入方式**：
   - 領域功能以 **UDF / table function / catalog 服務** 形式提供，透過既有
     `GtvContext` 註冊，**唔改** planner / kernel / CSR / ANN 內部。
   - 營運功能以 **wrapper / trait impl / sidecar 服務** 形式提供（見 §10–§15），
     **唔侵入** kernel。
3. **每批可獨立交付**：新 crate 獨立可測；`cargo test --workspace` 必須保持全綠，
   舊測試零回歸。
4. **企業批延後項目唔可以偷偷拉入 engine**：見 §17 非目標。

---

## 1. 新增 Crates

| crate | 用途 | 對應 roadmap |
|---|---|---|
| `gtv-scenario` | Risk / ALM / FTP scenario catalog、版本、繼承 / override、deterministic rerun、reconciliation | P2.1 D1, P2.4 D4, P2.5 D5 |
| `gtv-refdata` | legal entity / organisation / product hierarchy、master & reference data、effective dating | P2.6 D6 |
| `gtv-governance` | CRM 規則治理、audit trail、AML case 解釋、model / scenario 版本登記 | P2.2 D2, P2.3 D3 |
| `gtv-observe` | metrics / tracing / SLO telemetry 聚合、報表 | P3.6 E6 |
| `gtv-security` | mTLS / workload identity、RBAC / ABAC、row/column policy、encryption、audit log | P3.4 E4, P3.5 E5 |
| `gtv-ops` | compute-storage separation、distributed partition catalog、tiering、HA/DR | P3.1 E1, P3.2 E2, P3.3 E3, P3.7 E7 |

> `gtv-scenario`、`gtv-governance`、`gtv-observe` 同 roadmap §3 建議一致；
> `gtv-refdata` / `gtv-security` / `gtv-ops` 係呢批再拆分，避免巨型 crate。

---

## 2. 任務總覽

| ID | 任務 | Roadmap | crate | 工期 | 依賴 |
|---|---|---|---|---|---|
| D1 | Risk Scenario Framework | P2.1 | `gtv-scenario` | 4–6 週 | p2 catalog, p2 lineage, p3 bitemporal |
| D2 | CRM 模型治理 | P2.2 | `gtv-governance` | 3–5 週 | D6, `gtv-array::crm` |
| D3 | AML Pattern 與 Case Explainability | P2.3 | `gtv-governance` + `gtv-pattern`（唯讀） | 4–6 週 | p3 filtered ANN, B2-3 lineage |
| D4 | ALM Scenario Cube | P2.4 | `gtv-scenario` | 4–6 週 | D1, bitemporal |
| D5 | FTP Curve 與定價引擎 | P2.5 | `gtv-scenario` | 4–6 週 | D1, D4, D6 |
| D6 | Hierarchy 與 Reference Data | P2.6 | `gtv-refdata` | 3–4 週 | bitemporal |
| E1 | Compute / Storage Separation | P3.1 | `gtv-ops` | 6–10 週 | p2 catalog, B3-1 |
| E2 | 分散式 Partition Catalog | P3.2 | `gtv-ops` | 6–8 週 | E1 |
| E3 | Hot / Warm / Cold Tiering | P3.3 | `gtv-ops` | 4–6 週 | E1, `gtv-storage` |
| E4 | Security | P3.4 | `gtv-security` | 6–8 週 | `gtv-server` |
| E5 | Multi-Tenant Isolation | P3.5 | `gtv-security` | 4–6 週 | E4, B2-4 tenant_id |
| E6 | Observability | P3.6 | `gtv-observe` | 3–4 週 | B3-6 workload, B3-1 stream |
| E7 | High Availability / DR | P3.7 | `gtv-ops` | 6–10 週 | E1, E2 |

**建議次序**：`D6 → D1 → D4 → D5`（領域資料線）；`D2 / D3` 可與 D4/D5 並行；
`E6`（唯讀聚合，低風險）可最早做；`E1 → E2 → E3 → E7`；`E4 → E5`。

---

## 3. D1 Risk Scenario Framework（P2.1）

**問題**：現時冇 scenario catalog、冇版本、冇 baseline / stress / adverse / reverse
stress，亦冇 source cutoff / model version / inheritance / override / deterministic rerun。

**交付物**（`gtv-scenario` + `gtv-enterprise-sql`）

1. `ScenarioCatalog`：版本化 catalog，支援 `Scenario { id, version, kind, parent,
   dimensions, shocks, source_cutoff, model_version, status }`。
2. `ScenarioKind`：`Baseline | Stress | Adverse | ReverseStress`。
3. **維度**：legal entity / portfolio / product / currency（可擴充）。
4. **繼承 + override**：child 只聲明 delta，resolution 沿 parent chain 合併，
   同 key（factor × dimension）以 child 為準；輸出每個 shock 嘅 **provenance**。
5. **Source cutoff + model version** 綁定，令 batch 可重演。
6. **Deterministic rerun**：同一 (scenario, cutoff, model) → 同一 resolved 結果；
   shock 排序確定。
7. **Reconciliation / explainability**：輸出 scenario diff（parent vs child）、
   每格來源、版本鏈。

> **已交付**：核心 `gtv-scenario`（9 unit tests）＋ SQL surface
> `resolve_scenario(name [, version])` 由 `gtv-enterprise-sql` 提供，
> CLI 用 `scenario_load <table>` 載入（user menu §20）。

**驗收條件**

- [x] 同一 scenario + cutoff + model，多次 resolve 逐位元一致。
- [x] child override 正確覆蓋父 shock；未 override 嘅繼承父值，provenance 指回父版本。
- [x] 四個維度可獨立 filter / aggregate（SQL 可直接篩 dimension 欄位）。
- [x] baseline / stress / adverse / reverse stress 四類均可建立並解析。
- [x] reversed / 循環 parent 鏈被拒絕，回明確錯誤。
- [x] 每次 resolution 記錄 source cutoff 同 model version，可經 SQL 查詢。

---

## 4. D2 CRM 模型治理（P2.2）

**問題**：`gtv-array::crm` 已有 greedy / LP allocation，但抵押品資格、haircut、
FX / maturity mismatch、wrong-way risk、netting、concentration、guarantee eligibility
全部寫死或缺失，且無版本化 / audit。

**交付物**（`gtv-governance`）

1. **規則外部化**：collateral eligibility、priority、haircut、FX mismatch、
   maturity mismatch、guarantee eligibility 以版本化規則集宣告（JSON）。
2. **Netting set / concentration limit**。
3. **Wrong-way risk** 標記與處理。
4. **版本化**：規則集有 version + effective dating；allocation 綁定規則版本。
5. **Greedy vs LP 差異報告**（沿用既有 `crm_alloc` / `crm_lp`）。
6. **完整 audit trail**：每次 allocation 記錄規則版本、輸入 snapshot、結果 checksum。
7. SQL：`crm_alloc_v2(...)` / `crm_explain(...)`（新增，唔改舊 `crm_alloc`）。

> **已交付**：`crates/gtv-governance` — 版本化 + effective-dated `RuleSet` /
> `RuleRegistry`（collateral eligibility、priority、haircut、FX / maturity
> mismatch、guarantee eligibility、wrong-way、concentration）；`GovernedInputs`
> 將 raw inputs 轉成 gtv-array kernel 輸入（附 `Exclusion` / `ConcentrationBreach`
> 記錄）；`GovernedResult` 帶完整 audit trail 同 rule provenance；
> `greedy_lp_diff` 出 greedy vs LP 逐 loan 差額（預設 `crm-lp` feature，真實
> 跑 `gtv_array::crm_lp`）。13 個 unit tests（含真實 greedy≠LP 案例）。
> SQL surface `crm_alloc_v2` / `crm_explain` 為後續接入（見 §16 DoD）。

**驗收條件**

- [x] 規則集版本化，allocation 可追至明確規則版本（`GovernedResult::rule_version`）。
- [x] haircut / mismatch / wrong-way / netting / concentration 各自有測試
      （netting 由 `netting_summary` 覆蓋）。
- [x] greedy 與 LP 差異報表可輸出（`greedy_lp_diff`）。
- [x] 每次 allocation audit trail 完整（kernel `allocations` + exclusions +
      breaches + rule provenance）；SQL surface 待接入。
- [x] 舊 `crm_alloc` / `crm_audit` 行為零回歸（gtv-array 未改，workspace 全綠）。

---

## 5. D3 AML Pattern 與 Case Explainability（P2.3）

**問題**：`gtv-pattern` 只有 ring / path / diamond，冇 directed/undirected 語意開關、
rolling-window motif、sequence constraint、可組合作圖語言、beneficial-ownership closure、
graph+vector hybrid score、alert explanation subgraph、case snapshot / feedback loop。

**交付物**

1. `gtv-pattern` **唯讀**擴充：motif 組合 DSL、rolling window、sequence constraint、
   directed/undirected 語意（唔改現有 ring/path/diamond 結果）。
2. `gtv-governance`：beneficial-ownership closure、graph + vector hybrid score、
   alert explanation subgraph、case snapshot、investigator feedback、FP feedback loop。
3. 聚合：amount / currency / jurisdiction / channel。
4. Case 可重演：綁定 lineage execution_id + snapshot。

> **已交付**（`crates/gtv-governance/src/aml.rs`）：`Transaction` 按
> amount / currency / jurisdiction / channel 聚合；`explain_subgraph`（directed /
> undirected、hop 上限、確定性）；`BeneficialOwnership`（多路徑乘積求和、
> cycle-safe、ultimate-owner threshold）；`cosine_similarity` + `hybrid_score`
> （graph/vector 加權融合）；`CaseSnapshot`（execution_id + 確定性重演）；
> `FeedbackLedger`（TP/FP/inconclusive、FPR、threshold 建議）。9 個 unit tests。
> **待做**：`gtv-pattern` 嘅 rolling-window motif DSL / sequence constraint
> （屬唯讀擴充，未接）。

**驗收條件**

- [x] 現有 `pattern` ring / path / diamond 零回歸（gtv-pattern 未改，workspace 全綠）。
- [ ] rolling-window motif + sequence constraint 正確性對 oracle（待做）。
- [x] alert explanation subgraph 可輸出且只含相關節點 / 邊。
- [x] case snapshot 可完整重演（同輸入 → 逐位元一致）。
- [x] hybrid score 可量度（graph risk + embedding cosine，加權融合並 clamp）。

---

## 6. D4 ALM Scenario Cube（P2.4）

**交付物**（`gtv-scenario`）

- 標準 cube schema：`as_of_date, scenario_id, legal_entity, currency, product,
  time_bucket, cashflow_type, amount, discount_factor, repricing_date,
  behavioural_assumption_version`。
- 計算：cash-flow ladder、NII / EVE、repricing gap、liquidity stress、deposit decay、
  prepayment、optionality、multi-currency aggregation。

> **已交付**：`crates/gtv-scenario/src/alm.rs` — `AlmCell` / `AlmCube` / `AlmFilter`、
> `CashflowType`、`DiscountCurve`（分段線性 zero curve + ACT/365 df）、
> `LiquidityStress`、`DepositDecay`、`PrepaymentModel`、`FxTable`；計算方法
> `cashflow_ladder` / `nii` / `eve` / `repricing_gap` / `liquidity_stress` /
> `deposit_decay` / `prepayment` / `optionality_charge` / `aggregate_currency` /
> `currency_breakdown`。11 個 unit tests（D4 令 gtv-scenario 增至 20 個）。

**驗收條件**

- [x] cube schema + 版本化 behavioural assumption
      （`behavioural_assumption_version` 欄位 + `AlmFilter::assumption_version`）。
- [x] NII / EVE / repricing gap 對獨立 oracle 一致（測試手算對比）。
- [x] liquidity stress / deposit decay / prepayment 各自可量度。
- [x] multi-currency 聚合正確（FX rate table；FX 版本化由 `gtv-refdata`
      effective-dated reference data 承載）。
- [x] 同一 scenario 重算確定性。

---

## 7. D5 FTP Curve 與定價引擎（P2.5）

**交付物**（`gtv-scenario`）

- curve catalog + version；tenor interpolation；liquidity premium；basis spread；
  optionality charge；behavioural adjustment；product hierarchy；booking/value/maturity
  date；預測 vs 實際成本對賬；逐步 explainability。

> **已交付**：`crates/gtv-scenario/src/ftp.rs` — `FtpCurve` / `FtpCurveCatalog`
> （版本 + effective dating、分段線性 tenor interpolation）、`FtpPolicy` /
> `FtpPolicyCatalog`（liquidity premium / basis spread / optionality /
> behavioural，含 `*` default）、`FtpEngine::price` 經 D6 product hierarchy 繼承並
> 回傳逐項 `FtpStep`（可解釋）、`reconcile`（predicted vs actual，bps）。
> 7 個 unit tests（gtv-scenario 20 → 27）。

**驗收條件**

- [x] curve 版本化 + interpolation 對獨立實作一致（分段線性 + flat extrapolation）。
- [x] 各 charge / adjustment 可分解（`FtpBreakdown` + `steps`，每步有 source）。
- [x] 預測 vs 實際成本對賬報表（`reconcile` variance / bps）。
- [x] product hierarchy 繼承定價正確（child 缺則用 parent，再 fallback `*`）。
- [x] 重算確定性。

---

## 8. D6 Hierarchy 與 Reference Data（P2.6）

**交付物**（`gtv-refdata`）

1. legal entity / organisation / product hierarchy（effective-dated）。
2. account / customer / instrument / counterparty master。
3. curve / calendar / currency / jurisdiction reference data。
4. 歷史版本查詢（`as_of`）。

> **已交付（首增量）**：`crates/gtv-refdata` — `EffectiveRange`（`gtv_core::BitemporalRange`
> business axis 投影）、`Hierarchy`（effective-dated edges、同時段循環拒絕、
> parents/children/ancestors/descendants/roots/leaves、deterministic `rollup`）、
> `MasterData`（`(kind, id)` 時間唯一鍵、referential constraints + 檢查）、
> `ReferenceData`（`(domain, key)` effective-dated 值）。14 個 unit tests。

**驗收條件**

- [x] hierarchy 查詢（祖先 / 後代 / roll-up）對 oracle 一致。
- [x] effective dating 正確：`as_of(t)` 回當時版本。
- [x] master / reference data 有唯一鍵與 referential 檢查。
- [x] 循環 hierarchy 被拒絕（含時段重疊循環；非重疊重組允許）。
- [x] 重算確定性 + 零回歸。

---

## 9. E1 Compute / Storage Separation（P3.1）

**交付物**（`gtv-ops`）

- object storage / 共享持久層抽象（`StorageBackend` trait）。
- stateless query workers；metadata / catalog service；local SSD cache；
  cache eviction / warming；data-locality-aware scheduling。

**驗收條件**

- [ ] worker 可隨時重啟，狀態全在 catalog / object store。
- [ ] cache hit / miss、warming 行為可量度。
- [ ] locality-aware scheduling 較 naive 有可量度改善。

---

## 10. E2 分散式 Partition Catalog（P3.2）

- partition ownership、shard map version、online rebalance、replica placement、
  stale catalog detection、atomic metadata update。

**驗收條件**

- [ ] rebalance 期間查詢不中斷。
- [ ] stale catalog 被偵測並拒絕。
- [ ] metadata update 對讀者原子可見。

---

## 11. E3 Hot / Warm / Cold Tiering（P3.3）

- Hot：近期事件 / 活躍 CSR / 熱門 embeddings。
- Warm：Parquet + mmap / IVF-Flat。
- Cold：壓縮 Parquet / IVF-PQ 或按需 index。
- 自動 promotion / demotion；retention / archive / legal hold。

**驗收條件**

- [ ] 分層邊界可配置並可量度成本 / 延遲。
- [ ] promotion / demotion 自動且可審計。
- [ ] legal hold 阻止 archive 刪除。

---

## 12. E4 Security（P3.4）

- mTLS、workload identity、RBAC / ABAC、table/column/row-level policy、
  privileged access management、encryption at rest、key rotation、field tokenisation、
  immutable audit log、secrets externalisation、signed artifacts / SBOM / dependency scan。

**驗收條件**

- [ ] 未授權存取回明確拒絕（唔會靜默）。
- [ ] row / column policy 生效且有測試。
- [ ] audit log 不可篡改。
- [ ] key rotation 過程可重啟、不丟資料。

---

## 13. E5 Multi-Tenant Isolation（P3.5）

- tenant ID 強制注入、memory/CPU/IO/storage quota、vector index tenant isolation、
  noisy-neighbour control、per-tenant encryption context、per-tenant usage / 成本統計。

**驗收條件**

- [ ] 跨 tenant 查詢永遠唔命中（沿用 B2-4 tenant 語意）。
- [ ] 單 tenant 無法用盡全域資源。
- [ ] per-tenant usage 可報表。

---

## 14. E6 Observability（P3.6）

在既有 `metrics` / Prometheus / `gtv-workload` 上補齊：ingestion lag、watermark lag、
source offset lag、query p50/p95/p99、rows/edges/vectors scanned、cache hit ratio、
partition pruning ratio、ANN candidate count、Recall@K sample、graph frontier size、
spill bytes、index build duration、DQ pass rate、scenario batch completion。

**驗收條件**

- [ ] 上述每個指標都有明確來源與單位。
- [ ] dashboard / 報表可重現（文字或 JSON 輸出，唔硬綁特定後端）。
- [ ] 指標與實際測試數字一致（抽驗）。

---

## 15. E7 High Availability / DR（P3.7）

- query service 多副本、catalog / manifest 備份、index snapshot 多副本、
  自動或受控 failover、regular restore test、degraded read mode、
  RTO/RPO 分業務定義、severe-but-plausible 演練。

**驗收條件**

- [ ] failover 後查詢恢復且資料一致。
- [ ] restore test 可重複執行且有報告。
- [ ] degraded read mode 明確降級語意。

---

## 16. 本批完成定義（Definition of Done）

- [ ] D1–D6、E1–E7 全部驗收條件通過。
- [ ] 端到端：scenario 建立 → hierarchy roll-up → ALM cube → FTP 定價 →
      Risk batch → reconciliation / explainability，全程 deterministic rerun。
- [ ] `cargo test --workspace` 全綠；**analysis engine 舊測試零回歸**。
- [ ] 依賴檢查：`gtv-core` / `gtv-engine` / `gtv-index` 唔依賴企業批 crate。
- [ ] 文件：`doc/prod_p4_design.md`、運維手冊、SLO / RTO / RPO 定義、
      安全與多租戶政策。

---

## 17. 明確唔喺本批做（非目標）

核心賬本雙重記賬、OLTP serializable transaction manager、跨 shard 在線交易 commit、
客戶餘額主檔、低延遲支付 authorization write path。若產品定位改變，須另開獨立架構
路線，**唔可以混入 analysis engine 主執行路徑**。
