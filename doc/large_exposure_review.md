# le.md 審查與下一步開發分析 — Large Exposure / Concentration Engine

- **日期**：2026-09-14
- **審查對象**：`le.md`（Large Exposure / Concentration Engine — API & SQL UDF Design 草稿）
- **權威文件**（已下載至 `/tmp/hkma/`）：
  1. **HKMA MA(BS)28 Completion Instructions**（03/2026 版，Return of Large Exposures，BELR Cap. 155S）
     <https://www.hkma.gov.hk/media/eng/doc/key-functions/banking-stability/banking-policy-and-supervision/regulatory-framework/MA(BS)28_CIs_(202603).pdf>
  2. **HKMA MA(BS)28 Template**
  3. HKMA **Exposure Limits** 頁：<https://www.hkma.gov.hk/eng/key-functions/banking/banking-legislation-policies-and-standards-implementation/exposure-limits/>
  4. BCBS “Supervisory framework for measuring and controlling large exposures”（bcbs283）
  5. 既有 `doc/parameter_externalization_audit.md`（P0–P3b 外部化模式）

---

## 1. le.md 正確 / 有價值嘅地方

- **Graph + Temporal + Columnar** 嘅定位正確：集團、SPV、擔保人、抵押品、關聯方本質係圖，
  而 `valid_from/valid_to` 正好用 Temporal-CSR 做切片。
- 資料模型覆蓋 entity / exposure_edge / collateral / capital_snapshot / limit_config。
- Rust trait（`exposure_at` / `large_exposure_ratio` / `scan` / `pre_trade_check` /
  `concentration_metrics`）方向正確。
- SQL UDF 用例（entity/group exposure、ratio、breach scan、pre-trade、concentration）
  同 IRRBB × LE 嘅延伸有實務價值。
- `limit_config` 用 table 配置 —— 同 gtvdb 嘅 P0–P3b 外部化方向一致。

## 2. 與 MA(BS)28 對照：需要修正 / 補完嘅位

> MA(BS)28 分 **Part I–V**：Part I 相連方 ≥5% Tier 1；Part II 20 大 **before-CRM** 曝險
> 及所有 ≥10% Tier 1；Part III 20 大 **after-CRM**；Part IV 豁免曝險 ≥10%；Part V 集團
> 內部（intragroup）≥5%。「Large exposure」定義 = 對 **LC group**（或獨立對手）曝險
> **≥10% Tier 1**；而**限額 = 25% Tier 1**（HKMA Exposure Limits 頁明文；G-SIB 對
> G-SIB 另有 15%，BELR 實施 BCBS LEX）。

| # | le.md 現狀 | 問題 | 應改為 |
|---|---|---|---|
| 1 | `capital_snapshot.total_capital` 用在 ratio 例 | 分母錯 | **Tier 1 capital**（且用**上季末**數字；外資行用總行最新數） |
| 2 | 無報表基礎（basis）概念 | MA(BS)28 要 **combined（HK offices + overseas branches）** 同 **consolidated** 兩份 | 加 `reporting_basis ∈ {combined, consolidated}` 貫穿全引擎 |
| 3 | 單一 `group_id` + `is_connected` flag | 混同三個監管概念：**connected party**（rule 85 + 管理層/親屬，Part I）、**LC group**（rule 41 linked counterparties，Parts II/III）、**group affiliate**（intragroup，Part V） | 分開三種關係類型同三套聚合 / 門檻 |
| 4 | `breached` 同報告門檻混用 | 報告門檻（≥10%、≥5%、Top-20）≠ 監管限額（25% / 15%） | 分開 `report_threshold` 同 `limit_ratio`；狀態 `OK/WARN/REPORTABLE/BREACH` |
| 5 | `net_exposure` 一個數 | MA(BS)28 要 **before-CRM** 同 **after-CRM** 兩套，且 before-CRM **含 transferred-in 嘅 indirect exposure**（rule 54 look-through 到保證人/抵押品發行人） | 每筆曝險同時輸出 before/after CRM；indirect 建邊到保護提供者 |
| 6 | `exposure_amount` 一個欄位 | 缺 MA(BS)28 六大組件：on-BS、trading book、OBS（CCF）、derivative/SFT default risk（SA-CCR）、investment w/ additional risk factor、indirect | `ExposureMeasure { on_bs, trading_book, obs_ccf, default_risk, additional_risk_factor, indirect }` |
| 7 | 無豁免 / 扣減 | Part IV（rule 48(1) 豁免）、rule 57 deductions 係報表必要 | 加 `ExemptedExposure` / `Deduction` 同 rule 代碼 |
| 8 | 無 joint account 處理 | 共同借款人要按連帶責任計俾每位持有人 | 曝險可歸屬多個 entity，附 attribution 規則 |
| 9 | 無 net short position 規則 | 銀行帳/交易帳 net short 要 disregard | 加 `net_short` 旗標 |
| 10 | 只用單一 `as_of` | Parts I/II/V 排名係按**報告期內最大曝險**（maximum during period），唔係季末快照 | 用 gtvdb temporal/rolling window 做**期內最大值**；`as_of` 只係其中一個 cut |
| 11 | 無經濟行業 / 關聯代碼 | Part II col 13（economic sector: banks/NBFIs/others）、Part I col 14（rule 85 段號） | 加 `economic_sector`、`connected_paragraph` |
| 12 | 多幣種無折算 | 報表以 **HKD** 計；Tier 1 亦係 HKD；需 as-of FX | 加 `fx_rate(as_of, ccy, HKD)` 同換算步驟 |
| 13 | `gtv_le_*` scalar `RETURNS STRUCT<...>` | **DataFusion scalar UDF 唔支援回傳 struct**；且 gtvdb 慣例係 table function 回 `MemTable`、時間用 i64 ns | 改用 **table function**（一行多欄）＋ CLI 慣例命名（`le_ratio(...)` 等） |
| 14 | 「Rust API design (gtvdb-core)」 | 放落 kernel `gtv-core` 會違反企業批邊界（kernel 不可依賴企業邏輯） | 新 enterprise crate **`gtv-largeexposure`** |
| 15 | `limit_config` 單表 | 同 P0–P3b 外部化模式未對齊（版本 + effective dating + loader） | 版本化 `le_limit_set` + `gtv-enterprise-sql::load` loader |
| 16 | 未重用現有元件 | 重覆造輪 | 重用 `gtv-refdata::EffectiveRange`/`Hierarchy`（生效日期）、`gtv-governance::BeneficialOwnership`（控制權閉包，D3 已有多路徑 % 乘積、cycle-safe）、`gtv-governance::RuleSet`（CRM）、`gtv-scenario::irrbb`（IRRBB × LE） |

**其他語意提醒**
- `share_of_total`（佔組合比例）≠ 監管 `ratio`（佔 Tier 1）。內部風險偏好可用前者，
  但報表必須後者。
- Derivative 曝險唔係 notional，而係 **default risk exposure**（SA-CCR：replacement
  cost + PFE）。第一版可接受 caller 提供嘅 default-risk-exposure。
- 分母 Tier 1 取**上季末**，即報表日同資本基礎時點唔一致，需明確對齊規則。
- 「ASC exposure」係 BELR 定義嘅曝險計量（本審查不展開其縮寫），引擎應以 BELR 為準。

## 3. 建議架構（對齊 gtvdb 既有模式）

```
crates/gtv-largeexposure/            # 新 enterprise crate（依賴 gtv-refdata；CRM 可選 gtv-governance）
  src/
    entity.rs          # Entity, EconomicSector, ConnectedParagraph
    relationship.rs     # Relationship { Control | EconomicDependence | ConnectedNaturalPerson | GroupAffiliate }
    exposure.rs         # ExposureMeasure { on_bs, trading_book, obs_ccf, default_risk, additional, indirect }
    group.rs            # LC group / connected-party closure（union-find + 控制權門檻）
    aggregate.rs        # entity / LC group / connected party / intragroup；before/after CRM；period-max
    limit.rs            # LimitSet（版本 + effective dating）、ratio、status、headroom、pre-trade
    concentration.rs    # sector / country / rating / connected
    ma_bs28.rs          # Parts I–V 報表投影
  tests/                # closure oracle、ratio、門檻、期內最大、pre-trade
```

- **配置 table**（延續 P0–P3b）：`le_entity`、`le_relationship`、`le_exposure`、
  `le_crm`、`le_capital`（Tier 1）、`le_limit_set`、`le_exempt`。
- **SQL surface**（`gtv-enterprise-sql::register`，全部 table function）：
  `le_exposure(entity_or_group, as_of)`、`le_ratio(group, as_of)`、
  `le_breach_scan(as_of, top_n)`、`le_pre_trade_check(...)`、
  `le_concentration(dimension, as_of)`、`le_ma_bs28(part, as_of)`、`le_explain(...)`。
- CLI `le_entity_load` / `le_relationship_load` / `le_exposure_load` / `le_capital_load` /
  `le_limit_load`；user menu。
- **邊界不變**：kernel crate 唔依賴 `gtv-largeexposure`。

## 4. 下一步開發建議

**Stage 0（設計定稿）**
- 將本審查併入 `doc/large_exposure_design.md`，並開 `prod_p5.md`（企業批新批次）列
  LE-1…LE-5 任務與驗收。

**Stage 1（首個可交付 increment，唔掂 SQL）— 建議即刻做**
- `gtv-largeexposure` core：
  1. 關係圖 + **LC group / connected-party closure**（控制門檻可配置；重用
     EffectiveRange；economic dependence 可選）；
  2. **曝險聚合**：entity / LC group / connected / intragroup × before/after CRM ×
     as-of 及**期內最大值**；indirect（保護提供者）建邊；
  3. **限額引擎**：Tier 1 分母、`report_threshold`（10%/5%）vs `limit_ratio`（25% /
     G-SIB 15%）、`OK/WARN/REPORTABLE/BREACH`、headroom、pre-trade（含 proposed delta）；
  4. **concentration**：economic sector / country / rating；
  5. 測試：closure 對 oracle、before/after CRM、期內最大、門檻狀態、pre-trade。

**Stage 2**：配置 table + loader（沿用 P0–P3b 模式）。
**Stage 3**：SQL table functions + CLI + user menu。
**Stage 4**：IRRBB × LE（用 `gtv-scenario::irrbb` 嘅 shock 曲線重估市值，映射到對手曝險）。

## 5. 待你拍板嘅決策

1. **首個 increment 範圍**：只做 Stage 1（core + tests），定連 Stage 2（loader）一齊？
2. **Derivative 曝險**：第一版**接受 caller 提供** default-risk-exposure，定要實作 SA-CCR？
3. **報表基礎**：`combined` 同 `consolidated` 都要，定先做一種？
4. **G-SIB 15% overlay**：而家 encode 定留待 config？
5. **分母**：確認用 **Tier 1**（非 CET1 / total capital）。
6. **新 crate 命名**：`gtv-largeexposure`（建議）定併入 `gtv-governance`？
