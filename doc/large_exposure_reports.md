# MA(BS)28 Top-N / 門檻報表 — 生成與增量設計

- **日期**：2026-09-14
- **背景**：HKMA **MA(BS)28 Return of Large Exposures** 係逐季提交，核心係
  **Top-N 及門檻清單**；而排名係按**報告期內最大曝險**，唔係季末快照。本文設計
  gtvdb 點樣生成呢啲報表，並同 `doc/large_exposure_incremental.md` 嘅增量重算配合。
- 權威：MA(BS)28 Completion Instructions (03/2026) 及 Template；BELR Cap. 155S。

---

## 1. MA(BS)28 Parts 一覽（報表對象、計量、門檻、期內語意）

| Part | 報表對象（ranking universe） | 計量 | 門檻 / 排名 |
|---|---|---|---|
| **Part I** | **connected party**（rule 85 + 管理層/親屬等） | before CRM | **≥5% Tier 1**（期內）+ memorandum 全體 aggregate |
| **Part II** | **LC group**（rule 41）或**獨立對手** | before CRM | **20 大** + 所有 **≥10% Tier 1** |
| **Part III** | LC group / 獨立對手 | **after CRM** | **20 大** + 所有 **≥10% Tier 1** |
| **Part IV** | LC group / 獨立對手 | **exempted before CRM** | **≥10% Tier 1**（**報告日**，非期內最大） |
| **Part V** | **group affiliate**（intragroup） | before CRM（豁免） | 本地 AI **≥5% Tier 1**；外資 AI **20 大** |

> 「Large exposure」定義 = 對 LC group（或獨立對手）曝險 **≥10% Tier 1**；
> **限額 = 25% Tier 1**（G-SIB 對 G-SIB 另 15%）。報表門檻（5%/10%）≠ 限額。

每行欄位（Parts I–V 共用）：
`Maximum exposure`、`On-balance sheet`、`Trading book`、`Off-balance sheet`、
`Default risk exposures (derivatives & SFTs)`、`Indirect exposures`、
`Exposures arising from investment with additional risk factor`、`Total`、
`Deductions`、`Economic sector (banks/NBFIs/others)`、`Relationship (Part I)`、
`As % of Tier 1`。

---

## 2. 關鍵：排名用「期內最大曝險」

Parts I、II、III、V 係按**報告期內最大曝險**排序／篩選；Part IV 用**報告日**。

```
period_max(key, [a,b]) = max_{t ∈ [a,b]} LE_measure(key, t)
rank(key)              = period_max 由大到小（tie-break: key 升序）
reportable(key)        = rank ≤ N  或  period_max ≥ threshold × Tier1(prev quarter)
```

所以：
- 唔可以只保留季末一個數；要保留**每個聚合鍵嘅期內 max**。
- 一次歷史更正會改變受影響鍵嘅 `period_max`，**Top-N 名單亦可能變**（有鍵升入／跌出）。
- 呢個正係 §3 資料結構要解決嘅事。

---

## 3. 增量資料結構

### 3.1 每鍵時序結構（見 incremental 設計）
每個聚合鍵 `K`（entity / LC group / connected / group affiliate / sector / country /
rating）維護：
- **Segment tree（lazy range-add + range-max）**：`period_max(K,[a,b])` = `O(log D)`；
- **Fenwick/BIT（range-add + 點查）**：`LE(K,t)` = `O(log D)`；
- 更正 `Δ` 喺 `t0`：`range_add(K,[t0,∞),Δ)` → `O(log D)`／鍵。

### 3.2 全域 Top-N（按 `period_max`）
- **Bounded min-heap（size N）** 或 **order-statistic tree** keyed by `period_max`：
  - 維護當前 Top-N 集合（N=20，或 Part I/V 嘅門檻清單）；
  - 每次更正：只需對**受影響鍵**重算 `period_max`，再更新佢喺全域結構嘅位置。
- **Lazy invalidation**：唔需要每次全體重排；只處理 touched keys，其餘節點嘅值唔變
  （因為更正只影響受影響鍵嘅 suffix）。
- **門檻清單**（≥5%/≥10%）：用「`period_max(K) ≥ threshold`」查詢；可維護一個
  **sorted set / interval index**，或對 Top-N heap 加「threshold-qualified」標記。

### 3.3 關係變更（LC group 重組）
- 關係邊變更 → rollback DSU 更新連通分量 → LC group 拆分/合併；
- 只重算**新舊受影響群組**嘅 `period_max`，再更新全域 Top-N；
- 唔使全圖 closure 重做（`O(log² n)` amortized）。

### 3.4 更正對 Top-N 嘅影響
```
更正(event X, entity E, t0, Δ, measure=before|after|exempted)
1. 舊 key 集合 K_old（更正前 E 所屬群組/維度）同 K_new（更正後）
2. 對 K_old ∪ K_new 每個鍵：BIT/SegTree.range_add([t0,∞), Δ)
3. 對每個 touched 鍵：new_pm = SegTree.range_max([a,b])；更新全域 Top-N
4. Part IV 例外：exempted 用報告日 snapshot（點查），唔用 period_max
5. append bitemporal system version + lineage
```
成本 = `O(|K| · log D + |K| · log N)`，`|K|` ≈ 5–10（除非關係重組）。

---

## 4. 報表 SQL surface

以 table function 生成整份報表（一個 Part 一次）：

```text
le_ma_bs28(
  part            STRING,   -- 'I' | 'II' | 'III' | 'IV' | 'V'
  period_start    BIGINT,   -- 報告期開始
  period_end      BIGINT,   -- 報告期末
  basis           STRING,   -- 'combined' | 'consolidated'
  currency        STRING,   -- 報表幣（預設 HKD）
  top_n           INT       -- 預設 20（Part I/V 可忽略，用門檻）
)
RETURNS TABLE<
  rank              INT,
  counterparty_id   STRING,
  lc_group_id       STRING,
  maximum_exposure  DOUBLE,
  on_balance        DOUBLE,
  trading_book      DOUBLE,
  off_balance       DOUBLE,
  default_risk      DOUBLE,
  indirect          DOUBLE,
  additional_risk   DOUBLE,
  total             DOUBLE,
  deductions        DOUBLE,
  economic_sector   STRING,
  relationship_code STRING,    -- Part I 用（rule 85 段號）
  percent_of_tier1  DOUBLE,
  exemption_provision STRING   -- Part IV 用
>
```

配套：
- `le_connected_parties(as_of)` → Part I 對象清單；
- `le_lc_groups(as_of)` → LC group 分群；
- `le_period_max(key, a, b)` → 單鍵期內最大（除錯用）；
- `le_explain(part, key, a, b)` → 逐 facility / CRM / indirect 路徑審計。

> SQL 全部用 **table function**（回 MemTable），符合 gtvdb/DataFusion 慣例
> （scalar UDF 唔支援 `STRUCT` 回傳）。

---

## 5. 複雜度總結

| 操作 | 成本 |
|---|---|
| 新曝險（尾部 append） | `O(log D)`／鍵 |
| 歷史更正（suffix） | `O(|K| · log D)`；`|K|≈5` |
| 期內最大（單鍵） | `O(log D)` |
| 生成某 Part 報表 | `O(G log N + R log D)`（G=群組數，R=入選行數） |
| 關係重組 | `O(log² n)`（DSU）+ touched keys |
| 更新後 Top-N | 只 touched keys，`O(|K| log N)` |

---

## 6. 正確性 / 合規細節

- **Tier 1 分母**：用**上季末** Tier 1（外資行用總行最新）；`as % of Tier 1` 每行計。
- **報告幣**：HKD（可配置）；as-of FX 折算。
- **before-CRM vs after-CRM**：Parts II/V 用 before；Part III 用 after；Part IV 用
  exempted before（報告日）。
- **indirect exposure**：before-CRM 要包含 transferred-in（rule 54）——所以更正
  保證人／抵押品發行人時，`K` 要包括佢。
- **joint accounts**：連帶責任 → 歸屬多位持有人（`K` 增加）。
- **net short position**：disregard（建模為事件旗標）。
- **排名 tie-break**：曝險降序，再 `counterparty_id` 升序，確保可重演。
- **重演**：每次報表生成綁 `execution_id` + snapshot（B2-3 lineage）；bitemporal
  可回答「某報表日、當時系統所知」嘅版本。

---

## 7. 建議實作次序（接 incremental 設計）

1. `temporal_index.rs`（Fenwick + segment tree）；
2. `event_store.rs`（`le_event` + correction/adjustment）；
3. `ranking.rs`（bounded heap / order-statistic tree + threshold set）；
4. `ma_bs28.rs`（Parts I–V 投影 + 欄位）；
5. `gtv-enterprise-sql`：`le_ma_bs28(...)` table function + `le_*_load`；
6. 基準測試：10 年 × 250 日、100 萬事件、每日更正 → top-N 更新 vs 全量重排。

> 待確認：`top_n` 固定 20；Part I/V 用**門檻**（≥5%）而唔係 Top-20（本地 AI）；
> 外資 AI Part V 才用 20 大。要唔要我照呢個次序開始實作？
