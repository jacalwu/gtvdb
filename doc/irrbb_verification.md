# IRRBB 邏輯驗證報告 — 對照 HKMA SPM IR-1 及 BCBS d368 / d578

- **日期**：2026-09-14
- **驗證對象**：`gtv-scenario`（`alm.rs` / `ftp.rs` / 新增 `irrbb.rs`）
- **權威文件**（已下載到 `/tmp/hkma/`，並可從以下 URL 重新取得）：
  1. HKMA SPM **IR-1 “Interest Rate Risk in the Banking Book”**（V.32 – Consultation）
     <https://brdr.hkma.gov.hk/eng/doc-ldg/docId/getPdf/20251024-3-EN/20251024-3-EN.pdf>
  2. HKMA circular **“BCBS recalibration of shocks for IRRBB”**（2024-07-22）
     <https://brdr.hkma.gov.hk/eng/doc-ldg/docId/getPdf/20240722-1-EN/20240722-1-EN.pdf>
  3. BCBS **d368** “Interest rate risk in the banking book”（SRP31 / SRP98）
     <https://www.bis.org/bcbs/publ/d368.pdf>
  4. BCBS **d578** “Recalibration of shocks for IRRBB”（2024-07）
     <https://www.bis.org/bcbs/publ/d578.pdf>

---

## 0. 結論摘要

`gtv-scenario` 原本只有**通用 ALM 工具**（現值、NII 求和、repricing gap、壓力參數），
**並未實現** IR-1 §5 的「本地標準化框架」。本輪新增 `irrbb.rs` 作為**標準化 IRRBB 的
權威參考實作**，已覆蓋可客觀驗證嘅核心（六大 shock、19 時間帶、標準化 ΔE、NMD caps、
CPR/TDRR 乘數），並以 BCBS 官方 worked example 做單元測試。餘下係**把 cube 現金流
按最早重定價日 slot 入時間帶**同 **KAO 期權定價**等接線工作。

| IR-1 條文 | 要求 | 現狀 |
|---|---|---|
| §5.34.1 | 六大 shock 參數化公式 | ✅ `shock_delta_bps` |
| §5.34.2 | 事後利率 `max(r+Δr, −2%)` | ✅ `post_shock_rate` |
| §5.34 | 各幣種 shock 大小（含 2026 重校準） | ✅ `ShockTable::{current,recalibrated_2026}` |
| §5.34.3/4 | MOP=HKD、未列幣種 default 400/500/300 | ✅ 測試覆蓋 |
| §5.1.1 | 19 時間帶及中點 t_k | ✅ `standard_time_bands` |
| §5.1.1 | `ΔE(k)=CF0·exp(−r0·t_k) − CFi·exp(−ri·t_k)` | ✅ `standardised_eve_scenario` |
| §5.1.1 | `ΔE_i,c=max(0, Σ_kΔE(k)+KAO)`、`ΔE=max_i Σ_c` | ✅ `aggregate_eve` |
| §5.1.1 | KAO 期權風險（+25% 隱含波動） | ❌ 只有 `option_risk` 輸入參數，未接期權定價模型 |
| §5.2.1 | 零售定息貸款 CPR：`min(1, γ_i·CPR_0)` | ✅ `cpr`（γ 乘數表） |
| §5.2.1 | `CF_i(k)=CF_S(k)+CPR_i·NO_i(k−1)` | ❌ 未接 notional-outstanding 滾動 |
| §5.2.2 | 零售定期存款 TDRR：`min(1, u_i·TDRR_0)`、早贖入 O/N 帶 | ✅ 乘數/TDRR；🟡 slot 入帶未接 |
| §5.3.1 | NMD 零售/非零售、core/non-core、caps 90/5、70/4.5、50/4 | ✅ `split_nmd`；🟡 behavioural maturity slot 未接 |
| §5.1.2 | 按**最早重定價日** slot（本金＋coupon） | ❌ 現有 `eve()` 用 `time_bucket`，非 `repricing_date`，亦無中點/netting |
| §4.4.3 | NII 兩個標準 shock（parallel up/down） | ❌ `nii()` 只係利息現金流求和，未 apply shock |
| §4.4.4 | Basis-risk 兩個假設情景 | ❌ 未實現 |
| §4.5.4 | Outlier 測試（ΔEVE > 15% Tier 1） | ❌ 未實現 |
| §5.34.2 | 風險無關折現曲線（swap curve） | 🟡 `DiscountCurve` 為連續複利；但 `eve()` 允許顯式 `discount_factor` 可繞過風險無關要求 |

---

## 1. 標準化 EVE 框架（IR-1 §5.1）

**條文**：對每幣種 c、時間帶 k，把所有**名義本金及票息**按**最早利率重定價日**
slot 入時間帶，計 net position `CF_0,c(k)` 及每個 shock 情景下嘅 `CF_i,c(k)`：

```
ΔE_i,c(k) = CF_0,c(k)·exp(−r_0,c(k)·t_k) − CF_i,c(k)·exp(−r_i,c(k)·t_k)
ΔE_i,c    = max(0, Σ_k ΔE_i,c(k) + KAO_i,c)
ΔE        = max_{i∈1..6} ( Σ_c ΔE_i,c )
```

`r_0/r_i` 為時間帶**中點**嘅風險無關即期利率，`t_k` 為中點（年）。

**實作**：`irrbb::standardised_eve_scenario` 逐帶計 `ΔE(k)` 並回傳
`max(0, Σ + KAO)`；`irrbb::aggregate_eve` 對六情景取 max。`TimeBand::midpoint_years`
明確使用 d368 Table 1 嘅中點（O/N 0.0028Y、1Y 0.875Y、4Y 3.5Y、>20Y 25Y…）。

**已用 BCBS 官方例子驗證**（d578 SRP31.92）：t_k=3.5Y、R=100bp →
short +41.7bp、steepener +25.4bp、flattener −1.6bp，測試全部通過。

**缺口**：`standardised_eve_scenario` 只接收**已 slot 好**嘅 `cf0` / `cf_shocked`。
現有 `AlmCube::eve()` 係 `Σ amount·df(time_bucket)`，**冇**按 `repricing_date`
slot、**冇**時間帶中點、**冇**逐情景重算，故不能直接用於監管報表。需要新嘅
`slot_notional_bands()` 將 `AlmCell`（Principals 按 `repricing_date`、coupons 按
`time_bucket`）映射到 19 帶（取最近中點）並帶內 netting。

---

## 2. 期權風險 KAO（IR-1 §5.1.1）

**條文**：`KAO_i,c = VAO_0,c − VAO_i,c`，其中 shock 情景期權淨值需用新曲線，
並假設**隱含波動相對上升 25%**。

**現狀**：`standardised_eve_scenario` 接受 `option_risk` 參數並正確加入 `ΔE_i,c`，
但 `AlmCube::optionality_charge()` 只係把 `CashflowType::Optionality` 嘅金額求和，
**唔係**期權重估。自動期權（caps/floors/swaptions）需要期權定價引擎（可用 D5
`DiscountCurve` + Black 模型），目前未接。

---

## 3. 零售定息貸款提前還款（IR-1 §5.2.1）

**條文**：`CPR_i = min(1, γ_i·CPR_0)`，γ 乘數：

| 情景 | Parallel up | Parallel down | Steepener | Flattener | Short up | Short down |
|---|---|---|---|---|---|---|
| γ_i | 0.8 | 1.2 | 0.8 | 1.2 | 0.8 | 1.2 |

`CF_i,c,p(k) = CF_i,c,S(k) + CPR_i,c,p·NO_i,c,p(k−1)`（NO = 上期期末名義餘額）。

**實作**：`irrbb::prepayment_multiplier` / `cpr` 已完全對應上表（測試覆蓋）。
`AlmCube::prepayment()` 則係「`smm` 比例提前 `shift_days`」嘅通用模型，
**唔等於**監管公式：無 γ 情景、無 rolling notional outstanding、無 `CF_S + CPR·NO(k−1)`。
需以 `irrbb::cpr` 為基礎重寫 / 新增監管版 prepayment schedule。

---

## 4. 零售定期存款早贖（IR-1 §5.2.2）

**條文**：`TDRR_i = min(1, u_i·TDRR_0)`，u 乘數：

| 情景 | Parallel up | Parallel down | Steepener | Flattener | Short up | Short down |
|---|---|---|---|---|---|---|
| u_i | 1.2 | 0.8 | 0.8 | 1.2 | 1.2 | 0.8 |

早贖名義 `CF_i,c,p(1) = TD_0,c,p·TDRR_i,c,p`，**slot 入隔夜帶 k=1**。

**實作**：`irrbb::tdrr_multiplier` / `tdrr` 已對應（測試覆蓋）。
`AlmCube::deposit_decay()` 係通用 survival 曲線，**唔係** TDRR；早贖 slot 入 O/N 帶
亦未接。注意 `deposit_decay` / `liquidity_stress` 屬**內部**資金風險工具，
唔應與監管標準化 TDRR 混用。

---

## 5. 非到期存款 NMD（IR-1 §5.3）

**條文**：
- 先分零售 / 非零售；零售再分 transactional / non-transactional（有定期交易或
  不計息 = transactional）。
- 先用 10 年觀察量找 stable NMD，再找 core deposits（穩定且大幅利率變動下亦不重定價）。
- caps：

| 類別 | core 佔比上限 | core 平均期限上限 |
|---|---|---|
| Retail/transactional | 90% | 5 年 |
| Retail/non-transactional | 70% | 4.5 年 |
| Non-retail | 50% | 4 年 |

core 按平均行為期限 slot；**non-core 視為隔夜**。

**實作**：`irrbb::split_nmd` + `NmdCategory::caps` 已完整編碼 caps 並測試
（例如 observed 95% transactional → 截 90%）。**未接**：core 部分按平均行為期限
分佈到時間帶（需 caller 提供 behavioural maturity profile）。

---

## 6. Shock 情景與重校準（IR-1 §5.34 / HKMA 2024 circular / BCBS d578）

**公式**（`irrbb::shock_delta_bps`）：

```
parallel up/down : ±R_parallel
steepener        : −0.65·R_short·e^(−t/4) + 0.9·R_long·(1−e^(−t/4))
flattener        :  0.8·R_short·e^(−t/4) − 0.6·R_long·(1−e^(−t/4))
short up/down    : ±R_short·e^(−t/4)
post-shock       : max(r0 + Δr, −2%)
```

**重校準**（BCBS d578，HKMA 目標 **2026-01-01** 實施；time series 2000–2023、
local shock factors、99.9th percentile、**25bp 取整**）：IR-1 consultation 同時列出
**兩張表** —— 第一張為重校準（例：HKD Parallel/Short/Long = **225/375/200**、
USD = 200/300/225），第二張為現行 d368（HKD = **200/250/100**、USD = 200/300/150）。
`ShockTable::recalibrated_2026()` 與 `current()` 分別編碼兩張表，並驗證重校準值
全部為 25bp 倍數。

> 註：IR-1 §5.34.3：MOP 跟 HKD；未列幣種 default 400/500/300 bps。已測試。

**缺口**：現行 `alm.rs` 完全冇 shock 情景，`eve()` 只計現時 EVE。需把
`ShockTable` + `post_shock_rate` 接上 cube，並在報表日 ≥ 2026-01-01 時切換到
重校準表。

---

## 7. 收益視角 NII（IR-1 §4.4）

**條文**：HKMA 用**兩個標準 shock**（parallel up/down）評估未來 12 個月 earnings；
另有 §4.4.4 兩個 basis-risk 情景（資產浮動/管理利率分別 shock，維持 1/3/6/12 個月）。

**現狀**：`AlmCube::nii()` 只係 `Σ Interest cash flow (time_bucket ≤ horizon)`，
**冇** apply shock、**冇** 12 個月重定價假設、**冇** basis 情景。需新增
`nii_shocked(base_curve, scenario, params, horizon)`。

---

## 8. 貨幣聚合及 outlier（IR-1 §4.5.4 / §5.1.1）

**條文**：`ΔE = max_i Σ_c ΔE_i,c`（HKMA 版本先在每幣種取 `max(0, ·)` 再按情景跨幣種
求和，最後對六情景取 max；BCBS d368 寫法係先 `+KAO` 後先跨幣種求和再 `max(0,·)`，
本報告以 HKMA 為準）。Outlier：ΔEVE > **15% Tier 1** 需特別關注。

**現狀**：`irrbb::aggregate_eve` 提供 max-over-scenarios；跨幣種 `Σ_c` 需 caller 提供。
`alm::currency_breakdown` 只係折算現值，唔係標準化 ΔE 聚合。Outlier 測試未實現
（只缺 Tier 1 輸入，屬 trivial 接線）。

---

## 9. 曲線與折現

- IR-1 §5.1.1：折現因子 `DF=exp(−R·t_k)`，`t_k` 為年中點；d368 §2.2：曲線須為
  **風險無關零息率**（如 secured swap curve）。
- `alm::DiscountCurve` 用連續複利 `exp(−r·t/365)`，方向正確；`irrbb::curve_zero`
  提供 `t(年)→curve.zero_rate(days)` 轉接。
- **風險點**：`AlmCube::eve()` 容許 cell 自帶 `discount_factor`，若該 DF 內含
  credit spread，會**違反**標準化框架（除非現金流本身亦已含 commercial margin，
  見 d368 註 29）。建議標準化路徑忽略 `discount_factor`，強制用風險無關曲線。

---

## 10. 現有 `alm.rs` 逐項評估

| 方法 | 是否 IRRBB 標準 | 說明 |
|---|---|---|
| `eve()` | ❌ | 通用現值，用 `time_bucket`，非 `repricing_date`／中點／情景 |
| `nii()` | 🟡 | 利息現金流求和，非 shocked NII |
| `repricing_gap()` | 🟡 | gap 分析（IR-1 舊版 / 內部管理），非 EVE 標準 |
| `optionality_charge()` | ❌ | 金額求和，非 KAO 期權重估 |
| `liquidity_stress()` | ❌ | 內部流動性壓力，唔屬 §5 |
| `deposit_decay()` | ❌ | 通用 survival，唔係 NMD caps / TDRR |
| `prepayment()` | ❌ | 固定 smm+shift，唔係 CPR 情景公式 |
| `aggregate_currency()` / `currency_breakdown()` | ❌ | 現值折算，唔係 ΔE 聚合 |
| `DiscountCurve` | ✅（方向） | `exp(−r·t)`；需確保風險無關 |

`ftp.rs` 屬定價（FTP）而非 IRRBB，**不受** IR-1 §5 約束；其 `DiscountCurve`
重用一致。

---

## 11. 新增參考實作（`crates/gtv-scenario/src/irrbb.rs`）

- `ShockScenario`（6，含編號 1–6）及 `as_str`。
- `ShockParams` / `ShockTable` / `ShockTableVersion`：現行與 2026 重校準表、
  MOP→HKD、未知幣種 default。
- `shock_delta_bps` / `post_shock_rate` / `DEFAULT_RATE_FLOOR`。
- `TimeBand` / `standard_time_bands`（19 帶 + 中點，d368 Table 1）。
- `standardised_eve_scenario` / `aggregate_eve` / `EveScenarioResult`。
- `NmdCategory` / `NmdSplit` / `split_nmd`（caps 90/5、70/4.5、50/4）。
- `prepayment_multiplier` / `cpr`、`tdrr_multiplier` / `tdrr`（γ / u 乘數）。
- `curve_zero`（`DiscountCurve` → `Fn(t_years)`）。
- **9 個單元測試**，包括 BCBS d578 worked examples、HKD/USD 兩版 shock 表、
  19 帶中點、ΔE 公式手算對比、floor、NMD caps、γ/u 乘數。

`cargo test -p gtv-scenario`：**36 passed / 0 failed**（原 27 → +9）。

---

## 12. 建議後續（完成 IR-1 合規）

1. **Slotting 引擎**：`AlmCube → [f64; 19]`，Principal 按 `repricing_date`、
   coupon 按 `time_bucket`，取最近中點；帶內 positive/negative netting。
2. **情景化現金流**：接 `irrbb::cpr`（含 `NO_i(k−1)` 滾動）及 `irrbb::tdrr`
   （早贖入 O/N 帶）；NMD core 依平均行為期限分佈、non-core 入 O/N。
3. **KAO 期權定價**：用 D5 `DiscountCurve` + Black 模型，shock 曲線 + 隱含波動 +25%。
4. **NII 情景**：parallel up/down 對重定價頭寸計 12 個月 ΔNII；加 §4.4.4 basis 情景。
5. **報表版本切換**：依報表日自動選 `Current2018` / `Recalibrated2026`（2026-01-01）。
6. **Outlier 測試**：`ΔEVE > 15% × Tier1`。
7. **風險無關折現**：標準化路徑禁止 `discount_factor` override。
8. 新增 CLI / SQL surface（例如 `irrbb_eve(...)`、`irrbb_shocks(<currency>)`）並更新
   user menu。

---

## 附錄：本輪驗證所用文件與位置

```
/tmp/hkma/20251024-3-EN.pdf   HKMA SPM IR-1 (V.32 Consultation, 37 pages)
/tmp/hkma/20240722-1-EN.pdf   HKMA circular: BCBS recalibration of shocks (2 pages)
/tmp/hkma/d368.pdf            BCBS IRRBB standard (51 pages)
/tmp/hkma/d578.pdf            BCBS recalibration of shocks (11 pages)
```

如要長期保存，可將上述 PDF 移入 `doc/irrbb/`（binary 較大，建議只保留連結與本報告）。
