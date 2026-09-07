function3.md — 7天預測指標與算法全集

> **可行性評估（2026-09-06）** —— 三軸：可行性（現有基礎/資料可得）·價值（與 M1/pooled gate 實測證據一致：
> H=7 日線方向加特徵無顯著增益 → 「可實現」≠「會賺」，需 feature-gate 分批驗證）·外部依賴。
>
> | 類別 | 評級 |
> |---|---|
> | ema / macd_dif·dea·hist / atr / vwap / bb_mid·up·lo / align / cov / pca / var_hist / zscore·momentum | ✅ 已實現（Phase A）|
> | ema_slope·accel / bb_width·mid_slope / macd_distance·slope / atr_ratio / hv(+ratio) / vol_compress / vol_spike(_ratio) / obv(+slope) / vwap_dev / hh·ll / K線形態(大陽大陰·長影) / regime_trend·vol·event / gap_up·down | 🟢 Wave A | ✅ **已完成（2026-09-06）**：23 個 WindowUDF（ema_slope/accel、bb_width/mid_slope、macd_distance/slope、atr_ratio、hv/hv_ratio、vol_spike、obv/obv_slope、vwap_dev、hh/ll、big_green/red、long_lower/upper_shadow、regime_trend/vol/event、gap_up/down），engine 43 tests 過。**Feature-gate（10股×H=7, 4526決策）：knn_base .505 → knn_wave .511（+0.6pp，≈噪音，個股正負混雜）、gbdt_wave .486 → 未過閘 → Wave A 不入生產特徵，保留引擎研究用**（gate: doc/feature_gate2.py） |
> | adx·di± / kama(+slope) | 🟡 Wave C 純工程（中量）|
> | rs_index / beta_index / momentum_rank / pca1·pca2 | 🟡 Wave B | ✅ **部分完成（2026-09-06）**：`xrank`(=`rank_cs`，截面分位/同値均秩)、`rs_index`(=`rs`，個股−指數 n 日動能)、`beta_index`(=`beta`，滾動迴歸 20d，kernel `rolling_beta`) —— 需指數欄在同表（先 align 或 join 進 panel）。`momentum_rank` = 兩段組合：`rank_cs(momentum(close,n))`（無法單一窗 UDF，因需先時序後截面）。`pca1/pca2` = 沿用既有 `pca` table fn（align 出 returns panel 後做 PCA）。engine 45 tests 過。**待 feature-gate（面板/指數級）** |
> | rs_sector / beta_sector | 🟠 需板塊指數源（目前僅 HSI/恆生科技 cache）|
> | earnings_window | 🔴 現狀不可做：需財報日期曆外部資料 |
> | gap_return7 | 🟡 需前瞻/統計聚合 → 以 gate 工具做，不單獨引擎化 |
>
> 流程紀律：每波完成 → 以 doc/m1_gate.py 同款因果回放跑 **feature-gate（10 股×H=7，knn4 基準）**，
> 過閘才入生產特徵；不過閘則保留「研究用」。SIMD：遞迴類（EMA/KAMA/ADX）不適合 SIMD，窗口統計可向量化。

---

本文件定義 gtvdb Phase A + Phase C 中，用於 預測未來 7 天走勢 的所有指標與算法。所有指標均可在 Arrow + DataFusion + gtvdb-core 中落地，並可直接拆分為開發任務。

1. 趨勢類（Trend Indicators）

趨勢是 7 天預測最強的訊號。本節包含方向、強度、加速度三層。

1.1 EMA 系列

ema(n) — 指數移動平均

ema_slope(n) — EMA 斜率（趨勢強度）

ema_accel(n) — EMA 加速度（趨勢加速）

1.2 MACD 系列

macd_dif() — DIF

macd_dea() — DEA

macd_hist() — DIF - DEA

macd_distance() — DIF 與 DEA 距離（預測力強）

macd_slope() — MACD 斜率

1.3 ADX 系列（趨勢強度）

adx(n) — 趨勢強度

di_plus(n) — 多方力量

di_minus(n) — 空方力量

1.4 KAMA（自適應均線）

kama(n) — 自適應均線

kama_slope(n) — 趨勢強度

1.5 布林帶（Bollinger Bands）

bb_mid(n) — 中軌

bb_mid_slope(n) — 中軌斜率（預測力強）

bb_width(n) — 帶寬（波動率）

2. 波動率類（Volatility Indicators）

波動率 regime 是 7 天方向預測的核心。

2.1 ATR 系列

atr(n) — 真實波幅

atr_ratio(short,long) — 波動率 regime 判斷

2.2 歷史波動率（HV）

hv(n) — 年化歷史波動率

hv_ratio(short,long) — 波動率 regime 切換

2.3 波動率收縮（Volatility Compression）

vol_compress_bb() — 布林帶收縮

vol_compress_atr() — ATR 收縮

3. 量能類（Volume Indicators）

量能異常是 7 天方向最強的短中期訊號。

3.1 異常放量（Volume Spike）

vol_spike(n) — 異常放量

vol_spike_ratio() — 今日 vs MA20

3.2 OBV（On-Balance Volume）

obv() — 量能趨勢

obv_slope() — OBV 斜率

3.3 VWAP 偏離（VWAP Deviation）

vwap() — 加權平均價

vwap_dev() — 價格偏離 VWAP（機構成本）

4. 形態類（Pattern Indicators）

形態對 7 天方向有中等預測力，但在 regime 切換時非常有效。

4.1 高低點突破（HH/LL）

hh(n) — n 日最高價突破

ll(n) — n 日最低價跌破

4.2 K 線形態（簡化版）

long_lower_shadow() — 長下影線

long_upper_shadow() — 長上影線

big_green() — 大陽線

big_red() — 大陰線

5. 截面因子（Cross-sectional Factors）

多標的比較是 7 天預測最強的因子之一。

5.1 對齊（align）

align(freq) — 多標的時間對齊（Phase A 核心）

5.2 相對強弱（Relative Strength）

rs_index() — 個股 vs 指數

rs_sector() — 個股 vs 行業

5.3 截面動能（Cross-sectional Momentum）

momentum_cs(n) — 截面動能

momentum_rank() — 行業內排名

5.4 Beta（敏感度）

beta_index() — 對指數敏感度

beta_sector() — 對行業敏感度

6. 風險因子（Risk Factors）

風險上升通常預示未來 7 天方向性更強。

6.1 協方差矩陣（Covariance Matrix）

cov_matrix(window) — 批次版協方差矩陣

6.2 PCA（主成分分析）

pca1() — 市場主成分

pca2() — 行業主成分

6.3 VaR（歷史法）

var_hist(window) — 歷史 VaR

7. Regime 判斷（Market Regime）

市場狀態是 7 天預測最強的高階因子。

7.1 趨勢 regime

regime_trend() — EMA20 vs EMA60

7.2 波動率 regime

regime_vol() — HV20 vs HV60

7.3 事件 regime

regime_event() — Volume spike / Gap

8. 事件風險（Event Risk）

事件後的 7 天方向具有強統計效果。

8.1 跳空統計（Gap Stats）

gap_up() — 跳空上漲

gap_down() — 跳空下跌

gap_return7() — 跳空後 7 日報酬

8.2 財報事件（Earnings Window）

earnings_window() — 財報前後 7 日效應

9. 7 天預測最強指標（Top 10）

若需優先開發，建議以下 10 個：

ema_slope() — 趨勢強度

atr_ratio() — 波動率 regime

vol_spike() — 異常放量

vwap_dev() — VWAP 偏離

rs_index() — 相對強弱

momentum_cs() — 截面動能

pca1() — 市場主成分

cov_matrix() — 風險上升

gap_return7() — 跳空後方向

regime_trend() — 趨勢 regime

10. 開發要求（Implementation Requirements）

所有指標必須：

使用 Arrow Array 作為核心資料結構

支援 DataFusion SQL 調用

支援 Rust API 調用

支援 group-by symbol

支援 SIMD 加速（std::simd / AVX2 / AVX512）

與 Phase A 的 align()、OHLC、回測引擎整合

本文件為 gtvdb Phase A + Phase C 的完整 7 天預測指標規範，可直接用於開發排程、Jira 任務拆分與架構設計。