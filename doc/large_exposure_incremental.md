# Large Exposure 增量重算（Incremental Recomputation）設計

- **日期**：2026-09-14
- **背景**：Large Exposure 係**時序累積量**（累加曝險、期內最大曝險、季末比率）。
  當**過去某日**嘅曝險／CRM／關係／資本基礎有**更正（correction）**或
  **調整（adjustment）**，由該日起**之後所有日子**嘅 LE 都要重算。本文分析
  gtvdb 可以點樣加快。
- 相關：`doc/large_exposure_review.md`、`doc/prod_p3_design.md`（B3-2 bitemporal）、
  `doc/prod_p2_design.md`（B2-1 catalog / B2-3 lineage）、`crates/gtv-delta`。

---

## 1. 問題本質

設時間軸有 `D` 個結算日、`N` 筆曝險事件（drawdown / repayment / revaluation /
adjustment）。LE 係一個 **temporal aggregate**：

```
LE(group, t)      = Σ_{entity ∈ group(t)} Σ_{event e of entity, effective ≤ t} amount_e
maxLE(group,[a,b]) = max_{t∈[a,b]} LE(group, t)
ratio(group, t)   = LE(group, t) / Tier1(prev quarter)
```

一次喺 `t0` 嘅更正 `Δ`，會令**所有 `t ≥ t0`** 嘅 `LE`、`ratio`、期內 max 改變：

- 天真做法：每個更正重跑 `[t0, now]`，成本 `O((D − t0) × N)`；若每日都更正 → `O(D²·N)`。
- 目標：把更正成本降到 **`O(log D)` ×（受影響聚合鍵數）**，或至少 `O(segment length)`。

---

## 2. gtvdb 已有、可直接用嘅加速基建

| 基建 | 位置 | 對 LE 增量嘅作用 |
|---|---|---|
| **Temporal-CSR**（`valid_from/valid_to`） | `gtv-core/src/csr.rs` | 事件本身已係時序邊；`as_of` slice 零拷貝 |
| **Bitemporal（B3-2）** | `gtv-core/src/bitemporal.rs`、`gtv-storage::BitemporalStore` | 更正 = **append 新 system version**，舊歷史**零重算**；`as_of(business, system)` 可重演 |
| **Catalog 不可變 snapshot** | `gtv-catalog` | 新版本引用未變 partition（結構共享 / copy-on-write），重算只限變更 partition |
| **Delta 層 + compaction** | `gtv-delta` | 近實時更正落 mutable delta，定期 compact 入 immutable base；LE 讀 base⊕delta |
| **Columnar + SIMD** | `gtv-array` | suffix 重算時對 Arrow 欄做向量化前綴和 |
| **rayon 並行** | workspace | suffix 按時間 chunk / 按 group 並行重算 |
| **Window functions** | `msum` / `mavg` / rolling | 期內累積 / 移動最大嘅現成算子 |
| **Lineage（B2-3）** | `gtv-catalog::ExecutionRecord` | 每次重算可追溯 source snapshot + 版本，審計可重演 |

---

## 3. 五個加速機制（由最重要到輔助）

### 3.1 事件溯源 + 時序前綴結構（核心）

**唔好存「每日餘額」，只存「事件」**（append-only）：

```
le_event(event_id, entity_id, group_key?, measure_components{on_bs, trading_book,
         obs_ccf, default_risk, additional, indirect}, business_from, business_to,
         system_from, kind{orig|adjustment|correction}, ref_event_id?)
```

- 曝險 = 事件嘅**前綴和**；更正 = **移除舊事件值 + 加入新事件值**，即對
  `[t0, ∞)` 做一次 **suffix range-add `Δ`**。
- 對每個「聚合鍵」（entity、LC group、connected-party、sector、country、rating）
  維護一個**時序索引**：
  - **Fenwick / BIT（支持 range-add + 點查）**：`O(log D)` 更新、`O(log D)` 查某日 LE；
  - **Segment tree（lazy range-add + range-max）**：期內最大 `O(log D)`。
- 一次更正影響嘅鍵數 ≈ `1(entity) + 1(group) + 3(dim) ≈ 5` →
  **總成本 `O(5·log D)`**，與歷史長度無關。

> 直覺：`Δ` 對「entity 曝險時間序列」係一個**常數 suffix 加法**；BIT/segment tree
> 嘅 lazy range-add 正係為此而設。期內 max 只需 segment tree 嘅 range-max。

### 3.2 Checkpoint + redo log（界定重放範圍）

- 每季末（或每 `K` 日）落一個 **LE 快照 checkpoint**（各聚合鍵嘅累積值 + 期內 max）。
- 更正時：搵 `t0` 之前最近 checkpoint，**只重放 checkpoint…now** 嘅事件（`O(segment)`），
  而唔係由頭。
- Checkpoint 用 catalog 不可變 snapshot 存；新更正只 append 新 checkpoint（結構共享）。

### 3.3 Bitemporal 系統版本（歷史零重算、可重演）

- 更正**唔覆寫**舊版本，只 append 新 system version（B3-2）。
- 「截至 2026-03-31 系統所知嘅 LE」同「今日所知嘅同一 business date」係兩個版本；
  **舊版本唔需要重算**，直接重用。
- 重算只針對**新版本**嘅受影響 business-date 區間；監管報表可用指定 system time 重演。

### 3.4 Delta 層 + 結構共享（儲存與重算限於變更範圍）

- 近實時更正先落 `gtv-delta`（mutable），LE 增量索引即時更新；
- 定期 compaction 將 delta 合併入 base；base partition 未變則**零重算**。
- 配合 §3.1：compaction 只重算被觸及 partition 嘅聚合鍵。

### 3.5 向量化 / 並行 suffix 重算（fallback）

當需要「全量重算」時（例如首次建立、schema 變更）：
- 取 Arrow suffix slice（零拷貝）→ **SIMD parallel prefix-sum（scan）**；
- 按時間 chunk / 按 group 用 rayon 並行；
- 係 `O(N)` 但常數極細，且可線性擴展。

---

## 4. 關係圖（LC group / connected party）嘅增量

群組成員會隨時間變（收購、股權變動、控制權轉移）：

- 用 **segment-tree-over-time + rollback DSU（offline dynamic connectivity）**：
  每條關係邊 `(parent, child, pct, [from,to))` 插入時間線；某 `t` 嘅連通分量 =
  沿時間樹 DFS 時嘅 DSU 狀態。
- 單邊新增/刪除 = `O(log² n)` amortized；**唔需要**每次重新做全圖 closure。
- 更正「關係」→ 只重算受影響群組嘅曝險索引（suffix range-add）。
- 控制門檻（>50% 表決權 etc.）可配置；economic dependence 邊可選。

---

## 5. 具體更新流程（一次更正）

```
輸入：correction(event_id=X, entity=E, business_from=t0, Δ=new−old)
1. 定位舊事件值，計 Δ（before-CRM 同 after-CRM 各自）。
2. 對受影響聚合鍵 K = {E, LCgroup(E), connected(E), sector, country, rating}：
       BIT.range_add(K, [t0, ∞), Δ)
       SegTree.range_add(K, [t0, ∞), Δ)     # 期內 max 用
3. 若更正牽涉 CRM/擔保/間接曝險 → 對 after-CRM 鍵同樣做（可能多一批鍵）。
4. 若更正牽涉關係邊 → rollback DSU 更新連通分量，再對新/舊群組鍵各做 step 2。
5. append 新 system version（bitemporal）+ 記 lineage（execution_id, snapshot）。
6. 報表查詢：LE(t) = BIT.point_query；maxLE([a,b]) = SegTree.range_max。
```

**複雜度**：`O((|K| + |關係變更鍵|) · log D)`，與 `D`、`N` 無關（除首次建索引）。

---

## 6. 與「monotonic accumulate」嘅關係

LE 係單調累加（drawdown 增、repayment 減），但**更正令佢非單調**。策略：
- **保留單調累加嘅快速路徑**：正常新增曝險 = 尾部 append（`O(log D)`，只影響 `[t_now, ∞)`）；
- **歷史更正走路 §5**（suffix range-add）；
- **期內最大** 用 segment tree（唔可以只靠 tail append，因為更正可能抬高/降低歷史 max）。

---

## 7. 建議下一步

1. 喺 `gtv-largeexposure` 加 **incremental aggregate 子模組**：
   - `temporal_index.rs`：Fenwick + segment tree（lazy range-add、range-max），keyed by aggregate key；
   - `event_store.rs`：append-only `le_event` + `AdjustmentKind`；
   - `dirty.rs`：dirty-range 追蹤 + lazy materialization（只喺報表請求時物化）。
2. 基準測試：`D = 10 年 × 250 日`、`N = 100 萬事件`、每日一次更正；對比
   naive 重算 vs BIT/segment tree。
3. 整合 bitemporal：每個 system version 對應一組索引 snapshot；舊版本唯讀重用。
4. 關係圖增量：rollback DSU（可後做）。

> 待你確認：期內 max 係 `max` 定 `avg`／`sum`？報表需要嘅係**季度內最大 before-CRM
> 曝險**（Parts I/II/V），故建議 segment tree range-max + 尾 append 雙路徑。
