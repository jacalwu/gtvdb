# gtvdb Large Exposure / Concentration Engine — Design

> **MA(BS)28 / HKMA Exposure Limits** implementation on gtvdb（Graph + Temporal + Columnar）
> Scope: **Large Exposure**、**Connected Parties**、**Concentration Risk**、
> **Pre-/Post-Trade Limit Check**、**Top-N 監管報表**、**增量重算**。
>
> 本文 consolidates 以下較早文件（內容已併入本檔）：
> `doc/large_exposure_review.md`（對照 MA(BS)28 之審查修正）、
> `doc/large_exposure_incremental.md`（增量重算）、
> `doc/large_exposure_reports.md`（Top-N／門檻報表）。

---

## 0. 文件來源與狀態

- **日期**：2026-09-14（v2，整合審查、增量、報表三份設計）
- **權威文件**：
  1. HKMA **MA(BS)28 Completion Instructions**（03/2026）— Return of Large Exposures
     <https://www.hkma.gov.hk/media/eng/doc/key-functions/banking-stability/banking-policy-and-supervision/regulatory-framework/MA(BS)28_CIs_(202603).pdf>
  2. HKMA **MA(BS)28 Template**
  3. HKMA **Exposure Limits** 頁（BELR Cap. 155S；**限額 25% Tier 1**）
     <https://www.hkma.gov.hk/eng/key-functions/banking/banking-legislation-policies-and-standards-implementation/exposure-limits/>
  4. BCBS “Supervisory framework for measuring and controlling large exposures”（bcbs283）
  5. gtvdb：`doc/parameter_externalization_audit.md`（P0–P3b config 模式）、
     `doc/prod_p3_design.md`（B3-2 bitemporal）、`crates/gtv-delta`。

---

## 1. 監管框架摘要（必須遵守）

- **Large exposure 定義**：對一個 **LC group**（或唔屬於任何 LC group 嘅獨立對手）
  嘅 aggregate exposure **≥10% Tier 1 capital**。
- **限額**：**不可超過 25% Tier 1**（BELR；G-SIB 對 G-SIB 另有 **15%**）。
  **報表門檻（5%/10%）≠ 限額（25%/15%）**。
- **分母**：**Tier 1 capital**，本地 AI 用**上季末**數字；外資行用總行最新數字。
- **報表幣**：HKD（可配置）；Tier 1 亦為 HKD；需 as-of FX 折算。
- **報表基礎**：本地 AI 需 **combined（HK offices + overseas branches）** 及
  **consolidated** 兩份。
- **MA(BS)28 Parts**：

| Part | ranking universe | 計量 | 門檻 / 排名 | 期內語意 |
|---|---|---|---|---|
| **I** | **connected party** | before CRM | ≥**5%** Tier1 | **期內最大** |
| **II** | **LC group**／獨立對手 | before CRM | **20 大** + 所有 ≥**10%** | **期內最大** |
| **III** | LC group／獨立對手 | **after CRM** | **20 大** + 所有 ≥**10%** | **期內最大** |
| **IV** | LC group／獨立對手 | **exempted before CRM** | ≥**10%** | **報告日**（非期內最大） |
| **V** | **group affiliate**（intragroup） | before CRM（豁免） | 本地 AI ≥**5%**；外資 AI **20 大** | **期內最大** |

- **三個唔同概念，必須分開**：
  1. **Connected party**：rule 85 BELR + 管理層/主要員工及親屬 + subsidiaries /
     fellow subsidiaries + controllers / directors + 利益衝突實體。
  2. **LC group**（group of linked counterparties）：rule 41 BELR（控制 + 經濟互賴）。
  3. **Group affiliate**（intragroup）：同一集團合併入賬嘅 affiliate。
- **曝險計量**（ASC exposure，依 BELR）分六組件：
  `on-balance sheet`、`trading book`、`off-balance sheet (CCF)`、
  `default risk exposures (derivatives & SFTs, SA-CCR)`、
  `investment with additional risk factor`、`indirect exposures`。
- **before-CRM** 含 **transferred-in indirect exposure**（rule 54 look-through 到
  保證人／抵押品發行人）；**after-CRM** 套 recognized CRM（rule 56/57）。
- **Exempted exposures**（rule 48(1)，Part IV）及 **Deductions**（rule 57）。
- **joint accounts**：連帶責任 → 按責任歸屬到**每位**持有人。
- **net short position**（銀行帳/交易帳）→ disregard。
- **Economic sector**（Part II col 13）：banks / NBFIs / others；**Relationship**
  （Part I col 14）：rule 85(1)(a)–(h) 段號。

> 每行 MA(BS)28 欄位：`Maximum exposure`、`On-balance sheet`、`Trading book`、
> `Off-balance sheet`、`Default risk (derivatives & SFTs)`、`Indirect exposures`、
> `Investment with additional risk factor`、`Total`、`Deductions`、
> `Economic sector`、`Relationship`、`As % of Tier 1`。

---

## 2. 設計原則

1. **新 enterprise crate** `gtv-largeexposure`（**唔可以**放入 `gtv-core` kernel；
   kernel crate 唔准依賴企業邏輯）。
2. **配置外部化**：**所有計算參數**（限額、報表門檻、warn 比例、`top_n`、控制股權門檻、
   CCF、FX、報告幣、Tier1 來源、basis 等）全部走**版本化 + effective-dated config table**
   （沿用 P0–P3b 模式）；**default = 法規值**，table 作 override。
3. **SQL 用 table function**（回 MemTable）；DataFusion scalar UDF **唔支援** `STRUCT`。
4. **時間用 i64**（與 gtvdb 一致），非 `chrono::NaiveDate`。
5. **圖 + 時序 + 列式**：關係用圖、曝險用 temporal event、聚合用 Arrow/SIMD。
6. **增量重算**：歷史更正只做 suffix update（見 §6），唔重跑全部日子。
7. **可重演**：每次計算綁 `execution_id` + snapshot（B2-3 lineage），bitemporal 支援
   「當時系統所知」版本。

---

## 3. 資料模型（Graph + Temporal + Columnar）

### 3.1 Core entities

```sql
-- 法人／客戶
CREATE TABLE le_entity (
  entity_id        STRING,
  entity_type      STRING,   -- bank, corporate, sovereign, FI, SPV, individual
  economic_sector  STRING,   -- banks | NBFIs | others（Part II col 13；可選 le_sector_map 自動對映）
  consolidation_scope STRING,-- combined | consolidated | both（報表基礎）
  is_g_sib         BOOL,     -- G-SIB 15% overlay 適用
  country_code     STRING,
  rating_provider  STRING,
  rating_grade     STRING,
  is_connected     BOOL,     -- connected party flag（Part I）
  connected_paragraph STRING,-- rule 85(1)(a)..(h) / 管理層 / 附屬 …（Part I col 14）
  PRIMARY KEY (entity_id)
);

-- 關係邊（時間序列）：控制 / 經濟互賴 / 集團 affiliate / 相連自然人
CREATE TABLE le_relationship (
  rel_id           STRING,
  parent_id        STRING,
  child_id         STRING,
  relation         STRING,   -- control | economic_dependence | group_affiliate | connected_person
  ownership_pct    DECIMAL(8,4),  -- control > 50%（可配置門檻）
  valid_from       BIGINT,
  valid_to         BIGINT,   -- 半開區間 [from, to)
  PRIMARY KEY (rel_id)
);

-- 曝險事件（append-only，六組件；時間序列 + bitemporal）
CREATE TABLE le_exposure_event (
  event_id         STRING,
  entity_id        STRING,
  product_type     STRING,   -- loan, bond, derivative, repo, OBS, ...
  on_balance       DECIMAL(20,4),
  trading_book     DECIMAL(20,4),
  off_balance_ccf  DECIMAL(20,4),  -- 已乘 CCF
  default_risk     DECIMAL(20,4),  -- SA-CCR default risk exposure
  additional_risk  DECIMAL(20,4),  -- investment with additional risk factor
  indirect         DECIMAL(20,4),  -- rule 54 transferred-in（指向 protection provider）
  protection_provider_id STRING NULL,  -- indirect 對象
  currency         STRING,
  net_short        BOOL,     -- true = disregard
  exempt           BOOL,     -- rule 48(1) 豁免
  exemption_provision STRING NULL,
  deduction        DECIMAL(20,4),  -- rule 57
  kind             STRING,   -- orig | adjustment | correction
  ref_event_id     STRING NULL,  -- correction/adjustment 指向原事件
  business_from    BIGINT,
  business_to      BIGINT,
  system_from      BIGINT,   -- bitemporal：系統知悉時間
  PRIMARY KEY (event_id)
);

-- CRM / 抵押品（versioned + effective dated）
CREATE TABLE le_crm (
  crm_id           STRING,
  exposure_event_id STRING,
  crm_type         STRING,   -- cash, securities, property, guarantee
  provider_id      STRING,   -- 保證人／發行人（indirect look-through）
  value            DECIMAL(20,4),
  haircut          DECIMAL(8,4),
  fx_haircut       DECIMAL(8,4),
  maturity_haircut DECIMAL(8,4),
  valid_from       BIGINT, valid_to BIGINT,
  PRIMARY KEY (crm_id)
);

-- 資本基礎（分母 = Tier 1；本地 AI 用上季末）
CREATE TABLE le_capital (
  as_of_date       BIGINT,
  basis            STRING,   -- combined | consolidated
  tier1            DECIMAL(20,4),
  PRIMARY KEY (as_of_date, basis)
);

-- 限額／門檻（versioned + effective dated）
CREATE TABLE le_limit_set (
  limit_set_id     STRING, version INT,
  effective_from   BIGINT, effective_to BIGINT,
  metric           STRING,   -- single_counterparty | lc_group | connected_party |
                             -- sector | country | rating | intragroup
  key              STRING,   -- '*' 或指定值
  report_threshold DECIMAL(8,4),  -- 0.05 / 0.10（報表門檻）
  warn_ratio       DECIMAL(8,4),  -- 預警線（例如 0.20）
  limit_ratio      DECIMAL(8,4),  -- 0.25（G-SIB 對 G-SIB 0.15）
  top_n            INT NULL,      -- 排名上限（預設 20，可配置；NULL = 用 le_config）
  applied_to       STRING,   -- tier1
  PRIMARY KEY (limit_set_id, version)
);

-- 一般純量參數（版本化 + effective dated）
CREATE TABLE le_config (
  scope            STRING,   -- global | basis | dimension | part
  key              STRING,   -- control_threshold | report_currency | tier1_source |
                             -- default_top_n | warn_ratio | g_sib_limit | ...
  value            STRING,   -- 解析為 f64 / bool / string / i64
  effective_from   BIGINT, effective_to BIGINT,
  PRIMARY KEY (scope, key, effective_from)
);

-- 可選：曝險類別 → 經濟行業對映（方便自動化；空則用 entity.economic_sector）
CREATE TABLE le_sector_map (
  exposure_class   STRING,   -- BCR STC/IRB 曝險類別
  economic_sector  STRING,   -- banks | NBFIs | others
  PRIMARY KEY (exposure_class)
);

-- OBS credit conversion factors（可配置）
CREATE TABLE le_ccf (
  product_type     STRING, ccf DECIMAL(8,4),
  effective_from   BIGINT, effective_to BIGINT,
  PRIMARY KEY (product_type, effective_from)
);

-- as-of FX（報表幣折算）
CREATE TABLE le_fx (
  as_of            BIGINT, currency STRING, to_hkd DECIMAL(20,8),
  PRIMARY KEY (as_of, currency)
);
```

### 3.2 三個聚合層次（唔可以混）

- `connected_party_closure(as_of)`：以 **rule 85 + 管理層/親屬 + 附屬** 建集合。
- `lc_group_closure(as_of)`：以 `relation ∈ {control, economic_dependence}` 建連通分量；
  控制門檻可配置（預設 >50% 表決權，或 rule 41 定義）。
- `group_affiliate(as_of)`：intragroup 集合（Part V）。

### 3.3 曝險計量

- **per entity**：`Σ` 六組件（按 as_of 生效；net_short disregard；exempt 另列）。
- **before-CRM**：包含被轉入嘅 indirect（protection provider 方向）。
- **after-CRM**：對每筆套 recognized CRM（haircut + FX/maturity mismatch），
  並把被轉出部分變為對 protection provider 嘅 indirect。
- **LC group / connected / intragroup**：對成員聚合，注意 **同一曝險只計一次**
  （rule 47(4)：同組內 CRM 唔重複計）。

### 3.4 時間模型

- 業務時間：`business_from/business_to`（半開區間）。
- 系統時間：`system_from`（B3-2 bitemporal）→ 更正**永不覆寫**，append 新版本。
- 重用 `gtv-refdata::EffectiveRange` / `gtv-core::BitemporalRange`。

---

### 3.5 配置參數一覽（全部可配置）

| 參數 | 表 / 欄 | Default（法規值） |
|---|---|---|
| 控制股權門檻 | `le_config.control_threshold` | `0.50` |
| 經濟互賴納入 | `le_config.include_economic_dependence` | `true` |
| 報表幣 | `le_config.report_currency` | `HKD` |
| Tier1 來源 | `le_config.tier1_source` | `prev_quarter_end` |
| 默認 `top_n` | `le_config.default_top_n` | `20` |
| 默認 warn 比例 | `le_config.warn_ratio` | `0.20` |
| G-SIB 對 G-SIB 限額 | `le_config.g_sib_limit` | `0.15` |
| 一般限額 / 報表門檻 | `le_limit_set.{report_threshold, limit_ratio, warn_ratio, top_n}` | `0.10` / `0.25` / `0.20` / `20` |
| OBS CCF | `le_ccf.ccf` | 依產品（法規 CCF） |
| FX 折算 | `le_fx.to_hkd` | as-of 匯率 |
| 報表基礎 | 呼叫參數 + `le_config.basis` | `combined` + `consolidated` |
| Derivative default-risk / CCF 演算法 | `le_config.{derivative_measure, obs_ccf_source}` | caller-provided / table |

> Loader：`le_config_load`、`le_ccf_load`、`le_fx_load`、`le_limit_load`（見 §7）。
> 所有 config table 可經 lineage 追溯，令報表結果可重演。

---

## 4. 計算模型

### 4.1 曝險 / 期內最大

```
LE_measure(key, t)        = Σ 事件（生效中，按 measure ∈ {before, after, exempt}）
period_max(key, [a,b])    = max_{t∈[a,b]} LE_measure(key, t)
```

### 4.2 比率與限額

```
ratio(key, t)  = LE_measure(key, t) / Tier1(basis, prev_quarter_end)
status         = OK | WARN(≥ warn_ratio) | REPORTABLE(≥ report_threshold)
                 | BREACH(≥ limit_ratio)
headroom       = limit_ratio × Tier1 − LE
```

**Pre-trade**：`proposed_delta` → post-trade `LE′ = LE + Δ`（同一 `t`）→ 對**所有**
適用 metric（single counterparty、LC group、connected、sector、country、rating、
intragroup）計 `projected_ratio`，回傳 `breached_limits` + `headroom`。

### 4.3 Concentration

- 維度：economic sector / country / rating / connected。
- 監管 ratio 用 **% Tier 1**；內部風險偏好可用 % of book（**必須標明**，唔可混）。

### 4.4 MA(BS)28 報表投影

見 §1 Parts I–V 表；每行欄位見 §1 尾。**Parts I/II/III/V 按 `period_max` 排名**，
**Part IV 用報告日 snapshot**。

---

## 5. 增量重算（歷史更正 / 調整）

### 5.1 問題本質

一次喺 `t0` 嘅更正 `Δ`，影響**所有 `t ≥ t0`** 嘅 LE／ratio／期內 max。
天真做法 `O((D−t0)·N)`；目標 `O(log D)`／聚合鍵。

### 5.2 資料結構

每個聚合鍵 `K` 維護：
- **Fenwick / BIT（range-add + 點查）**：`LE(K, t)` = `O(log D)`；
- **Segment tree（lazy range-add + range-max）**：`period_max(K,[a,b])` = `O(log D)`；
- **全域 Top-N**：bounded min-heap（或 order-statistic tree）keyed by `period_max`；
- **門檻集合**：`period_max ≥ threshold × Tier1`（sorted set / 標記）。

### 5.3 更新流程

```
更正(event X, entity E, business_from=t0, Δ = new − old, measure)
1. K_old / K_new = 更正前後 E 所屬群組 / 維度鍵（entity、LC group、connected、
   group affiliate、sector、country、rating；indirect 另加 protection provider）
2. 對 K_old ∪ K_new：BIT.range_add([t0,∞), Δ); SegTree.range_add([t0,∞), Δ)
3. 對每個 touched 鍵：new_pm = SegTree.range_max([a,b])；更新全域 Top-N
4. Part IV：exempted 用報告日 BIT 點查（非 period_max）
5. append 新 system version（bitemporal）+ lineage（execution_id, snapshot）
```

### 5.4 其他加速基建（gtvdb 現成）

- **Checkpoint + redo log**：每季末落 LE 快照，更正只重放「最近 checkpoint → now」。
- **Bitemporal（B3-2）**：更正 = append system version，**舊版本零重算**，可重演。
- **gtv-delta + 不可變 catalog snapshot**：更正落 delta，compaction 只重算被觸及
  partition（結構共享 / copy-on-write）。
- **關係圖增量**：**segment-tree-over-time + rollback DSU**（offline dynamic
  connectivity），單邊變更 `O(log² n)`；唔使全圖 closure 重做。
- **向量化 / 並行 fallback**：Arrow suffix slice 零拷貝 + SIMD parallel prefix-sum +
  rayon，全量重算 `O(N)` 但常數極細、可線性擴展。

### 5.5 Top-N 報表增量

- 更正只影響 **touched keys**：重算其 `period_max` → 更新全域 Top-N
  （`O(|K| log N)`）；唔使全體重排。
- 關係重組：rollback DSU 更新連通分量 → 只重算**新舊受影響群組**。

### 5.6 複雜度

| 操作 | 成本 |
|---|---|
| 新曝險（尾部 append） | `O(log D)`／鍵 |
| 歷史更正（suffix） | `O(|K|·log D)`，`|K|≈5–10` |
| 期內最大（單鍵） | `O(log D)` |
| 生成某 Part 報表 | `O(G log N + R log D)` |
| 關係重組 | `O(log² n)` + touched keys |
| 增量更新 Top-N | 只 touched keys，`O(|K| log N)` |

> **實測（release，本機；`gtv-largeexposure` LE-1/LE-2）**：D=3650 日、N=100k
> 事件、G=5000 對手、M=2000 更正：build 1.6s、增量更正 103ms、期內 max（全 entity）
> 5.3ms、naive 全量重算 4.56s → **44× 加速**。每鍵**坐標壓縮**（依該鍵事件邊界
> 建 Fenwick/SegTree）令記憶體 O(events) 而非 O(keys × days)；bottleneck 由 57s/2.9GB
> 降到 1.6s/O(N)。

---

## 6. Rust API（crate `gtv-largeexposure`）

```
crates/gtv-largeexposure/
  src/
    lib.rs
    entity.rs          # Entity, EconomicSector, ConnectedParagraph
    relationship.rs    # RelationshipKind { Control, EconomicDependence, GroupAffiliate, ConnectedPerson }
    exposure.rs        # ExposureMeasure { on_balance, trading_book, off_balance, default_risk, additional, indirect }
    group.rs           # connected_party_closure / lc_group_closure（union-find + 控制門檻）
    aggregate.rs       # entity / LC / connected / intragroup；before/after CRM；period-max
    temporal_index.rs  # Fenwick + segment tree（lazy range-add / range-max）
    event_store.rs     # append-only le_event + AdjustmentKind
    ranking.rs         # bounded heap / order-statistic tree + threshold set
    limit.rs           # LimitSet（版本 + effective dating）、status、headroom、pre-trade
    concentration.rs   # sector / country / rating / connected
    ma_bs28.rs         # Parts I–V 報表投影
  tests/               # closure oracle、before/after CRM、期內最大、Top-N、pre-trade、基準
```

核心 traits / structs（**已修正**：Tier 1 分母、before/after、門檻 vs 限額）：

```rust
pub enum EntityKey { EntityId(String), LcGroupId(String), ConnectedPartyId(String) }

pub struct ExposureResult {
    pub before_crm: f64,
    pub after_crm: f64,
    pub components: ExposureMeasure,
    pub currency: String,
}

pub struct LargeExposureRatio {
    pub key: String,
    pub measure: Measure,          // BeforeCrm | AfterCrm | Exempted
    pub exposure: f64,
    pub tier1: f64,                // 上季末
    pub ratio: f64,
    pub report_threshold: f64,     // 0.05 / 0.10
    pub limit_ratio: f64,          // 0.25（G-SIB 0.15）
    pub status: LimitStatus,       // Ok | Warn | Reportable | Breach
    pub headroom: f64,
}

pub struct PreTradeCheckResult {
    pub would_breach: bool,
    pub projected_ratios: Vec<(String /*metric*/, f64)>,
    pub breached_limits: Vec<String>,   // limit_set_id
    pub headroom: f64,
}

pub enum ConcentrationDimension { EconomicSector, Country, Rating, ConnectedParty }

pub enum Measure { BeforeCrm, AfterCrm, Exempted }
```

主要方法：
`exposure_at(as_of, &EntityKey, Measure) -> ExposureResult`、
`period_max(as_of_range, &EntityKey, Measure) -> f64`、
`large_exposure_ratio(as_of, group, Measure) -> LargeExposureRatio`、
`scan_large_exposures(period, top_n) -> Vec<LargeExposureRecord>`（`top_n` 可省略，
由 `le_limit_set.top_n` 或 `le_config.default_top_n` 提供）、
`pre_trade_check(as_of, &NewExposure) -> PreTradeCheckResult`、
`concentration_metrics(as_of, dim) -> Vec<ConcentrationRecord>`、
`ma_bs28_report(part, period, basis) -> Vec<MaBs28Row>`。

---

## 7. SQL UDF surface（DataFusion table functions）

```text
-- 曝險 / 分群
le_exposure(entity_or_group, as_of [, measure])         -- before/after/exempted
le_connected_parties(as_of)                             -- Part I 對象
le_lc_groups(as_of)                                     -- LC group 分群

-- 比率 / 掃描 / pre-trade
le_ratio(key, as_of [, measure])                        -- ratio, status, headroom
le_breach_scan(period_start, period_end, top_n)         -- 20 大 + 超限
le_pre_trade_check(entity_id, product_type, amount, currency, as_of)
le_period_max(key, period_start, period_end [, measure])

-- 集中度
le_concentration(dimension, as_of)                      -- sector/country/rating/connected

-- MA(BS)28 報表
le_ma_bs28(part, period_start, period_end, basis [, currency] [, top_n])
le_explain(part, key, period_start, period_end)         -- 逐 facility / CRM / indirect 審計
```

`le_ma_bs28` 回傳欄位對齊 MA(BS)28 template：
`rank, counterparty_id, lc_group_id, maximum_exposure, on_balance, trading_book,
off_balance, default_risk, indirect, additional_risk, total, deductions,
economic_sector, relationship_code, percent_of_tier1, exemption_provision`。

**CLI 配置命令**（沿用 P0–P3b）：`le_entity_load`、`le_relationship_load`、
`le_exposure_load`、`le_crm_load`、`le_capital_load`、`le_limit_load`、
`le_config_load`、`le_ccf_load`、`le_fx_load`。

> 命名慣例：唔用 `gtv_le_*` 前綴；唔用 scalar `RETURNS STRUCT`。

---

## 8. 與 IRRBB / stress 整合（延伸）

```text
le_exposure_scn(group_id, as_of, scenario_id)
  RETURNS TABLE<net_exposure_mv, eve_impact, ratio, breached>
```

- 用 `gtv-scenario::irrbb` 嘅 shock 曲線重估債券／貸款市值 → 映射到對手曝險
  （衍生品、債券持倉）；
- 觀察壓力情景下集團／國家／行業集中度是否惡化；
- 即 **IRRBB × Large Exposure × Concentration** 聯合視角。

---

## 9. 落地里程碑

| ID | 內容 | 依賴 |
|---|---|---|
| LE-0 | 本文（設計定稿）+ config table schema | — |
| LE-1 | `gtv-largeexposure` core：關係 closure、曝險聚合、限額、concentration | LE-0 |
| LE-2 | **增量**：`temporal_index`（Fenwick/segment tree）+ `event_store` + 基準測試 | LE-1 |
| LE-3 | **Top-N / 報表**：`ranking` + `ma_bs28`（Parts I–V） | LE-1, LE-2 |
| LE-4 | config loaders + SQL table functions + CLI + user menu | LE-1..3 |
| LE-5 | IRRBB × LE 情景 | LE-4 |

**建議次序**：LE-0 → LE-1 → LE-2 → LE-3 → LE-4 → LE-5。

> **進度**：LE-0（設計定稿）、LE-1（core）、LE-2（增量索引 + 基準）、LE-3
> （MA(BS)28 Parts I–V 報表投影）、**LE-5（IRRBB × LE 情景重估）**、
> **LE-6（SA-CCR 簡化版）** 已完成，實作喺 `crates/gtv-largeexposure`。
> 餘 LE-4（config loaders + SQL/CLI surface）。

---

## 10. 待決策 / 開放問題（已全部定案）

1. **Derivative 曝險** → **v1 接受 caller 提供 `default_risk`**；加 pluggable
   `DerivativeMeasure` strategy（`le_config.derivative_measure = provided | sa_ccr`）。
   **LE-6 已實作簡化 SA-CCR**（`gtv-largeexposure::sa_ccr`：RC + multiplier×AddOn、
   調整名義金額 / supervisory duration / 到期因子 / hedging-set 淨額；完整 CRE52
   maturity-bucket/correlation 聚合待補）。
2. ~~報表基礎：combined + consolidated 一齊做，定先做一種？~~ → **兩者一齊**，以
   `le_entity.consolidation_scope` 範圍過濾；所有聚合／報表 API 收 `basis` 參數。
3. ~~G-SIB 15% overlay：而家 encode 定留 config？~~ → **可配置**
   （`le_config.g_sib_limit`，default `0.15`；只喺 AI 與對手皆為 G-SIB 時適用）。
4. ~~限額集合：除 25% 外要唔要 encode 其他 BELR 限額？~~ → **通用 data-driven 限額
   引擎**：出廠預設核心（25% / G-SIB 15% / connected party / intragroup）；其餘（directors
   connected、特定資產、sector/country/internal）由 `le_limit_set` 加行，**唔改 code**。
5. ~~經濟行業分類？~~ → **第一版用 `le_entity.economic_sector`（banks/NBFIs/others）**
   ＋可選 `le_sector_map`；**BCR STC/IRB 整合延後**（capital engine 關注點）。
6. ~~`top_n`：固定 20~~ → **已決：`top_n` 可配置**（`le_limit_set.top_n`，
   否則 `le_config.default_top_n`；`le_ma_bs28` / `le_breach_scan` 可逐次 override）。
7. **SA-CCR / CCF / FX** 全部可配置：CCF 走 `le_ccf`，FX 走 `le_fx`，derivative
   default-risk 第一版接受 caller 提供（演算法選擇走 `le_config`）。

---

## 11. 附錄：為何用 gtvdb 而唔係傳統 SQL / DWH

- **Graph**：集團、SPV、擔保、關聯方多層關係 —— Temporal-CSR 天然支援。
- **Temporal + Bitemporal**：曝險隨時間變（drawdown/repayment/rollover/limit change），
  報表日只係一個 cut；更正 = append system version，**歷史零重算**。
- **Columnar + SIMD + rayon**：千萬級曝險事件嘅 group / sector / country 聚合、
  scenario revaluation 可做到毫秒–幾十毫秒級。
- **增量結構**（Fenwick/segment tree）：歷史更正由 `O(D·N)` 降到 `O(log D)`。
- 一句話版本：*「用一個 Graph+Temporal+Columnar 單引擎（gtvdb），把 IRRBB 利率風險場景、
  MA(BS)28 大額曝險聚合、HKMA exposure limits 集中度監控放喺同一底層；可喺 pre-trade
  即時判斷交易會否觸發 Large Exposure / Concentration limit，同時睇壓力情景下 IRRBB
  對集中度風險嘅放大效應。」*
