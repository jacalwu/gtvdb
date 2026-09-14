# ALM / FTP / CRM 計算參數外部化審查

- **日期**：2026-09-14
- **目的**：檢查所有 ALM / FTP / CRM（含 IRRBB、AML case）**計算參數**是否可經
  配置 table 提供，而非寫死於程式碼。
- **結論**：**部分已外部化、部分仍寫死**。FTP 同 CRM 治理規則本質上已係 data
  （catalog / `RuleSet`），但**冇 table loader**；IRRBB 監管常數同 CRM 評級對照表
  仍**寫死**。詳見下表。

圖例：✅ 已由 table / caller data 提供｜🟡 值可由 caller 提供但內建 default / 對照
寫死｜❌ 寫死於程式碼，無法配置。

---

## 1. 總覽

| 模組 | 參數外部化程度 | 主要問題 |
|---|---|---|
| ALM 現金流（`alm.rs`） | ✅ | `AlmConfig`（day-count / stress / decay）+ `load_alm_config` / `load_alm_cells` |
| IRRBB（`irrbb.rs`） | ✅ | `IrrbbConfig` + `ShockTable::from_params` + `load_irrbb_*` |
| FTP（`ftp.rs`） | ✅ | curve / policy data + `load_ftp_curves` / `load_ftp_policies` |
| CRM 治理（`gtv-governance`） | ✅ | `RuleSet` data + `load_crm_rulesets` |
| CRM kernel（`gtv-array` + `gtv-engine/crm.rs`） | ✅ | `CrmRatingMaps` + `load_rating_maps` + `crm_rating_map()` |
| AML case（`gtv-governance/aml.rs`） | 🟡 | `HybridWeights::default` 寫死 |
| CLI / SQL surface | 🟡 | IRRBB `irrbb_eve` + FTP `ftp_price` + 配置 load 命令已上；governed CRM `crm_alloc_v2` 未接 |

---

## 2. 逐項清單（含位置）

### 2.1 ALM（`crates/gtv-scenario/src/alm.rs`）

| 參數 | 位置 | 狀態 | 建議 |
|---|---|---|---|
| `LiquidityStress` default：`deposit_runoff=0.10`、`wholesale_outflow=0.0`、`inflow_haircut=0.0` | `AlmConfig::default` | ✅ `AlmConfig::liquidity_stress` + `load_alm_config` | — |
| `DepositDecay` `period_days=30` | `AlmConfig::deposit_decay_period_days` | ✅ `from_monthly_runoff_with` + `AlmConfig::deposit_decay` | — |
| `DiscountCurve` day-count `ACT/365` | `DayCount` | ✅ `DayCount{Act365,Act360}` + `from_zero_rates_dc` | — |
| `PrepaymentModel` smm / shift | — | ✅ 由 caller 傳入 | 可加 default（未做） |
| `AlmCell` / `AlmCube` 全部數值 | — | ✅ 由資料提供 | — |
| ALM table loader | — | ✅ `load_alm_config` / `load_alm_cells` | — |

### 2.2 IRRBB（`crates/gtv-scenario/src/irrbb.rs`）

| 參數 | 位置 | 狀態 | 建議 |
|---|---|---|---|
| Shock 公式：steepener `−0.65/0.9`、flattener `0.8/−0.6`、decay `t/4` | L229、L234、L237 | ❌ 常數寫死 | `irrbb_shock_formula` table（含 decay x，national discretion） |
| `DEFAULT_RATE_FLOOR = -0.02` | L246 | 🟡 可傳入，default 寫死 | `irrbb_floor` table / config |
| `ShockTable::current()` / `recalibrated_2026()` 幣種 bps | L~130–210 | 🟡 data 但寫死 code | `irrbb_shock_params(regulator, table_version, currency, parallel, short, long)` table |
| NMD caps `0.90/5.0`、`0.70/4.5`、`0.50/4.0` | `NmdCategory::caps` L396–401 | ❌ | `irrbb_nmd_caps(regulator, category, core_ratio_cap, maturity_cap_years)` |
| CPR γ 乘數 `0.8/1.2` | `prepayment_multiplier` L438–444 | ❌ | `irrbb_option_multipliers(scenario, cpr_gamma, tdrr_u)` |
| TDRR u 乘數 `1.2/0.8` | `tdrr_multiplier` L452–459 | ❌ | 同上 |
| 期權隱含波動 bump `1.25` | `option_risk_measure` L984 | ❌ | `irrbb_option_params(vol_bump)` |
| 19 時間帶及中點 | `standard_time_bands` L285–305 | ❌ | `irrbb_time_bands(index, label, start_years, end_years, midpoint_years)` |
| `Regulator::{Hkma,Mas}` + 2026 切換 | L~110–125 | ✅ 邏輯 | 保留；表改由 config |

### 2.3 FTP（`crates/gtv-scenario/src/ftp.rs`）

| 參數 | 位置 | 狀態 | 建議 |
|---|---|---|---|
| Curve tenors / zero rates / effective dating / version | `FtpCurve` L~60–90 | ✅ data | 加 `load_ftp_curve_points` |
| Liquidity premium / basis / optionality / behavioural | `FtpPolicy` L186–195 | ✅ data | 加 `load_ftp_policy_*` |
| `*` default product fallback | L~340、L628 | ✅ 邏輯 | — |
| `FtpEngine::price` 計算 | L~380–660 | ✅ 無數值寫死 | — |
| `variance_bps = variance × 10_000` | L664 | ✅ 單位換算 | — |
| FTP table loader | — | ❌ 冇 | `load_ftp_curves`、`load_ftp_policies` |

### 2.4 CRM 治理（`crates/gtv-governance`）

| 參數 | 位置 | 狀態 | 建議 |
|---|---|---|---|
| Collateral eligibility / priority / haircut / fx / maturity | `CollateralRule` `rules.rs` L10–60 | ✅ data | 加 `load_crm_rules` |
| Guarantee eligibility / jurisdiction | `GuaranteeRule` L63–95 | ✅ data | 同上 |
| Wrong-way / concentration limits | `rules.rs` L97–112 | ✅ data | 同上 |
| RuleSet 版本 + effective dating | `RuleSet` / `RuleRegistry` | ✅ data | 同上 |
| `netting_summary` 未設 netting set key `"__unrated__"` | `crm.rs` L232 | 🟡 寫死 string | 參數化 / `netting_unassigned_label` |
| `GovernedInputs` exposures / collateral / guarantors / pledges | `crm.rs` | ✅ data | 加 `load_crm_*` |
| CRM rules table loader | — | ❌ 冇 | `load_crm_rulesets` |

### 2.5 CRM kernel / engine（`gtv-array` + `gtv-engine/src/crm.rs`）

| 參數 | 位置 | 狀態 | 建議 |
|---|---|---|---|
| 借貸 rating → risk weight：`AAA/AA=0.2`、`A=0.5`、`BBB/BB=1.0`、`B=1.5`、else `1.0` | `loan_rating_rw` L275–284 | ❌ 寫死 | `crm_rating_map(map_name='loan_rw', key, value)` |
| 借貸 rating → priority：`BB=3.0`…`AAA=0.0` | `loan_rating_risk_priority` L286–296 | ❌ | `crm_rating_map('loan_priority', …)` |
| Collateral type → priority：`CASH=3.0`、`BOND=2.0`、`EQUITY=1.0`、else `0.0` | `collateral_type_priority` L298–306 | ❌ | `crm_rating_map('collateral_priority', …)` |
| Guarantor rating → priority：`AAA=3.0`…`BB=0.0` | `guarantor_rating_priority` L308–318 | ❌ | `crm_rating_map('guarantor_priority', …)` |
| 無 explicit priority / pd / rw 時 fallback `0.0` | `snapshot_from_batches` L~360 | 🟡 | 可保留，但 default 應可配置 |
| 實際 haircut / fx_haircut / amount / ratio / capacity | table 欄位 | ✅ | — |
| `method` 字串（`greedy`/`lp`/`haircut_efficiency`） | `snapshot_from_batches` | 🟡 寫死 dispatch | enum + 參數表（可接受） |
| `adjusted_collateral_value` 公式 | `gtv-array/src/crm.rs` L205–220 | ✅ 算法 | — |

### 2.6 AML case（`crates/gtv-governance/src/aml.rs`）

| 參數 | 位置 | 狀態 | 建議 |
|---|---|---|---|
| `HybridWeights` default `graph=0.5, vector=0.5` | L348–355 | 🟡 可傳入，default 寫死 | `aml_hybrid_weights` table |
| `FeedbackLedger` target/step | 由 caller | ✅ | — |

### 2.7 CLI / SQL surface

- ❌ 冇 `alm` / `ftp` / `irrbb` REPL 命令或 SQL table function；參數無法由使用者配置。
- `crm_alloc` / `crm_audit` table function（`gtv-engine/src/crm.rs`）用寫死評級對照表
  （見 2.5），且無 `rules` 參數。

---

## 3. 建議：統一配置 table 設計

沿用現有 Arrow table ingestion（`loadcsv` / `load` / catalog）同
`gtv-enterprise-sql::load` 模式，新增以下 table 及 loader：

```
# IRRBB（監管參數）
irrbb_shock_params(regulator, table_version, currency, parallel_bps, short_bps, long_bps)
irrbb_shock_formula(regulator, scenario, short_coeff, long_coeff, decay_x)
irrbb_floor(regulator, floor)
irrbb_nmd_caps(regulator, category, core_ratio_cap, maturity_cap_years)
irrbb_option_multipliers(regulator, scenario, cpr_gamma, tdrr_u)
irrbb_option_params(regulator, vol_bump)
irrbb_time_bands(index, label, start_years, end_years, midpoint_years)

# ALM
alm_cells(as_of_date, scenario_id, legal_entity, currency, product, time_bucket,
          cashflow_type, amount, discount_factor, repricing_date, assumption_version)
alm_stress_params(scenario_id, deposit_runoff, wholesale_outflow, inflow_haircut)
alm_decay_params(scenario_id, survival_csv, period_days, day_count)

# FTP
ftp_curve_points(curve_id, version, currency, effective_from, effective_to, tenor_days, zero_rate)
ftp_policy_liquidity(policy_id, version, product, tenor_days, spread)
ftp_policy_basis(policy_id, version, currency, tenor_days, spread)
ftp_policy_optionality(policy_id, version, product, charge)
ftp_policy_behavioural(policy_id, version, product, adjustment)

# CRM
crm_rules(ruleset_id, version, effective_from, effective_to, kind, key, eligible,
          priority, haircut, fx_haircut, maturity_haircut, currencies, jurisdictions, limit)
crm_wrong_way(ruleset_id, version, counterparty, collateral_type)
crm_rating_map(map_name, key, value)          # 取代 §2.5 四張寫死表
crm_inputs_*(...)                              # exposures / collateral / guarantors / pledges
```

**Loader**（放 `gtv-enterprise-sql/src/load.rs` 或各 crate 的 `load` 模組）：
`load_irrbb_shocks`、`load_irrbb_nmd_caps`、`load_irrbb_option_params`、
`load_irrbb_time_bands`、`load_ftp_curves`、`load_ftp_policies`、`load_crm_rulesets`、
`load_crm_rating_maps`、`load_alm_cells`、`load_alm_params`。

**SQL surface**：`irrbb_shocks('HKD', 2026)`、`irrbb_eve(...)`、`ftp_price(...)`、
`crm_alloc_v2(...)`，並更新 user menu。

---

## 4. 建議次序

1. **P0 — CRM 評級對照表**（`gtv-engine/src/crm.rs` 四張 map）＋ IRRBB 監管常數
   （caps / γ / u / vol bump / floor / shock 表 / time bands）：呢啲係「寫死喺程式」
   最明顯、亦最影響合規參數調整。
2. **P1 — FTP / CRM 規則 loader**：資料結構已好，只差由 table 載入。
3. **P2 — ALM 參數表 + day-count 參數化**。
4. **P3 — CLI / SQL surface + user menu**。

---

## 6. 執行進度

**P0 已完成**（commit 見 repo 記錄）：
- **CRM 評級對照表外部化**：`gtv_engine::crm::{CrmRatingMaps, load_rating_maps}`；
  `GtvContext::set_crm_rating_maps`；SQL `crm_rating_map()`；CLI `crm_rating_load <table>`。
  預設 = 原寫死值（零回歸），table 只覆蓋提供嘅 key。
- **IRRBB 監管常數外部化**：`gtv_scenario::irrbb::{IrrbbConfig, ShockFormula, NmdCaps}`
  （floor / vol_bump / shock 公式係數 / NMD caps / γ / u / time bands，預設 = 法規值）；
  config-aware API（`*_with`）；`ShockTable::from_params`；loader
  `load_irrbb_scalars` / `load_irrbb_nmd_caps` / `load_irrbb_scenario_multipliers` /
  `load_irrbb_time_bands` / `load_shock_table`（`gtv-enterprise-sql::load`）。

**P1 已完成**：
- **FTP loader**（`gtv-enterprise-sql::load`）：`load_ftp_curves`
  （`curve_id, version, currency, effective_from, effective_to, tenor_days, zero_rate`）、
  `load_ftp_policies`（header + liquidity / basis / optionality / behavioural 四張表；
  unknown-policy 詳細行會報錯）。
- **CRM 治理規則 loader**：`load_crm_rulesets`（header + collateral / guarantees /
  wrong-way / concentration 五張表；`eligible` 接受 true/1/yes；`currencies` /
  `jurisdictions` 逗號分隔），寫入 `gtv_governance::RuleRegistry`（版本 + effective dating）。

**P3 已完成（IRRBB / FTP）**：
- SQL surface（`gtv-enterprise-sql::register`）：`irrbb_eve(currency [, regulator]
  [, reporting_year])`（六大情景 ΔE，max = 風險值）、`ftp_price(curve_id, cv,
  policy_id, pv, product, ccy, value_date, maturity_date [, booking_date])`。
- CLI load 命令：`alm_load`、`irrbb_curve_load`、`irrbb_params_load`、
  `irrbb_nmd_load`、`irrbb_mult_load`、`irrbb_bands_load`、`irrbb_shocks_load`、
  `ftp_curves_load`、`ftp_policy_load`。
- 已以 REPL + 整合測試驗證。

**仍未做**：governed CRM `crm_alloc_v2`（需將 `GovernedInputs` /
`crm_rules_load` 接上 SQL surface）。

**P2 已完成**：
- **ALM 參數外部化**：`gtv_scenario::alm::{AlmConfig, DayCount}`（day-count ACT/365、
  ACT/360；`LiquidityStress` default；deposit-decay period），`DiscountCurve::from_zero_rates_dc`
  / `zero_rate_years` / `day_count`，`AlmConfig::{curve, deposit_decay}`。
- **ALM loader**：`load_alm_config`（`alm_params(key, value)`）、`load_alm_cells`
  （cube 行：scenario / legal_entity / currency / product / time_bucket /
  cashflow_type / amount / as_of_date / discount_factor / repricing_date /
  assumption_version）。

---

## 7. 注意

- IRRBB 監管常數（shock 公式係數、NMD caps、γ/u）係**法規規定**，外部化時應保留
  「default = 法規值」並記錄 override 來源，避免配置錯誤令報表不合規。
- 配置表需有 **version + effective dating**（FTP/CRM 已有模式），IRRBB 需加
  `table_version`（d368 vs d578）同 `regulator`。
- 配置表應可經 `lineage` 追溯，令報表結果可重演。
- 現有 `gtv-enterprise-sql::load` 已示範 table → registry 模式，可直接擴充。
