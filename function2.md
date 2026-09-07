function2.md — Phase A & Phase C Quant功能規範

> **實現狀態（2026-09-06）**：Phase A §1.1 指標 + vwap + §1.2 align + §1.4 回測引擎(phase1+2) 全落地；
> §1.5 cov/PCA、§1.6 Greeks(+rho)、§1.7 VaR 已有。**Phase C**：engine metrics（queries/latency histogram，REPL `metrics`
> Prometheus text）、`dq_report/dq_check`（NaN/重複/時間倒退/標的覆蓋）、`health_check`（freshness/rows/dup/nan/sorted）、
> `strategy_stats`（hit/Brier/ECE/PSI/前後期命中/高信心桶命中）已落地（gtv-engine monitor.rs，44+ tests）。
> **未做（原因）**：`/metrics` HTTP endpoint（需 HTTP 層，可用 metrics 文字直出；後補）；
> CPU/cache-miss/DRAM/SIMD 計數（需 OS/perf 硬體計數器，超出引擎範圍）；資料源更新狀態整合進每日報告（掛鉤處預留）。
> 所有新 SQL 需 `ALTER SESSION SET sqlmode = full`。

---

本文件為 gtvdb 下一階段（Phase A + Phase C）量化研究功能的正式規範，專注於「功能補齊」而非性能、交易或分佈式能力。所有內容均基於現有架構（Arrow + DataFusion + gtvdb-core），並可直接拆分為開發任務。

1. Phase A — 量化研究工具鏈（Quant Research Toolchain）

1.1 技術指標（Technical Indicators）

提供 Arrow WindowUDF 版本的常用技術指標，支援批次計算與 SQL 調用。

EMA(n) — 指數移動平均

RSI(n) — 相對強弱指標

MACD(12,26,9) — 雙層 EMA + DIF/DEA

ATR(n) — 真實波幅

Bollinger(n,k) — 中軌 + 上下軌

要求：

全部以 Arrow Array + SIMD 實作

支援 OVER (ORDER BY ts) 語意

支援多標的 group-by（symbol）

與現有 mavg/msum/deltas 一致的 API 風格

1.2 截面因子（Cross-sectional Factors）

補齊多標的量化研究必備的截面因子。

align() — 多標的時間對齊（核心必做）

rank() — 截面排序（已完成）

zscore() — 標準化（已完成）

momentum() — 動能（已完成）

要求：

align() 支援任意頻率（1m/5m/15m/1h/1d）

支援缺值填補（forward-fill / drop）

與 OHLC 重採樣整合

1.3 OHLCV 重採樣（Resampling）

補強批次版 K 線生成能力。

xbar(n) — 任意 bucket 重採樣

ohlc() — 開高低收聚合（已完成）

vwap() — 加權平均價（新增）

要求：

支援 group-by symbol

支援 volume 加總

與 align() 整合

1.4 回測引擎（Backtest Engine）

建立可用於量化研究的批次回測框架。

Position State Machine（持倉狀態機）

交易成本模型（slippage / commission）

停損 / 停利規則

多標的回測（portfolio）

再平衡（rebalance）

回測報表（收益、波動、Sharpe、maxDD）

要求：

Arrow RecordBatch 為核心資料結構

SQL + Rust API 雙介面

與技術指標、截面因子、OHLC 完整整合

1.5 協方差矩陣 / PCA（Quant Operators）

補齊量化風險與因子研究必備的矩陣算子。

covariance_matrix() — 批次版

pca() — 主成分分析

要求：

Arrow Array → Dense Matrix（f64）

SIMD + 多線程加速

與回測引擎整合（風險模型）

1.6 Greeks（批次版）

提供期權研究必備的希臘值計算。

delta / gamma / vega / theta / rho（Black-Scholes）

要求：

Arrow Array → SIMD 批次計算

支援多標的、多到期日

1.7 VaR（歷史法）

提供風險管理必備的 VaR 計算。

historical_var() — 基於歷史收益率

要求：

Arrow Array → quantile 計算

與回測引擎整合

2. Phase C — 監控與可觀察性（Observability）

2.1 引擎層監控（Engine Metrics）

提供量化研究員與開發者可用的系統監控。

查詢延遲（latency histogram）

吞吐量（TPS）

CPU 使用率

L1/L2/L3 cache miss

DRAM 帶寬

SIMD 使用率

要求：

Prometheus 格式輸出

gtvdb-server 內建 /metrics endpoint

2.2 策略層監控（Strategy Metrics）

補齊量化研究必備的訊號監控。

訊號命中率（hit rate）

訊號分布（p_up histogram）

門檻命中（threshold coverage）

漂移監控（EWMA / PSI）

要求：

與回測引擎整合

與每日決策報告整合

2.3 資料品質（Data Quality）

補齊量化研究必備的資料品質檢查。

缺值檢查（NaN / Inf）

重複資料檢查

時間戳錯誤檢查

標的對齊錯誤檢查

要求：

dq_check(table)

dq_report(table)

2.4 系統健康度（Health Check）

提供每日資料與系統健康度檢查。

資料源更新狀態

Parquet/HDB 完整性

指標是否 NaN

回測是否有空洞

要求：

health_check()

可整合至每日報告

3. 整體交付要求

3.1 Arrow + DataFusion 一致性

所有功能必須：

使用 Arrow Array 作為核心資料結構

支援 DataFusion SQL 調用

支援 Rust API 調用

3.2 SIMD / 多線程

所有批次計算必須：

支援 SIMD（std::simd 或 AVX2/AVX512）

支援 Rayon 或 tokio 多線程

3.3 與現有 gtvdb-core 完整整合

所有功能必須：

與 mavg/msum/deltas 一致的 API 風格

與 Temporal/Vector/Graph 模組不衝突

與回測引擎、技術指標、截面因子互通

4. 開發優先順序（建議）

align()（多標的研究核心）

EMA / RSI / MACD / ATR / Bollinger

OHLC + VWAP

回測引擎（Position / Cost / Rebalance）

covariance_matrix / PCA

Greeks / VaR

Engine Metrics / Strategy Metrics

Data Quality / Health Check

本文件作為 gtvdb Phase A + Phase C 的正式功能規範，可直接用於開發排程、Jira 任務拆分與架構設計。