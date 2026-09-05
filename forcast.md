
> **狀態：已落地。** gtvdb 已实现并接入上述四类诊断：
> 引擎 `fwd_walk`（严格 past-only，扩展输出 `actual_ret, q05..q95, bar_ret, vol20`，
> 并修复训练邻居标签的时序泄漏）＋ `./stock_diag.sh SYMBOL` 输出
> `calibration_points.csv / quantile_coverage.csv / tail_error_stats.csv /
> regime_performance.csv`（regime 默认 vol20 分桶，`REGIME=kmeans` 可选确定性 k-means）。
> HK.00700 H=10 实测：ECE=0.10、宽 band 欠覆盖(90%→82%)、左尾 bias -9.8pp/右尾 +13.7pp、
> 高波动 regime 命中率最高(57-58%) —— 详见下方各节解读与后续优化。
> **概率缩放已实现**：`fwd_walk(...,cal_frac)`（arg3/arg4 传 0~1 小数，如 `,0.3`）
> 用最早一段历史决策（outcome 已解析、跳过 H 根成果窗口保证因果）拟合
> isotonic(PAV) 与 Platt，只对后续行输出 `cal_p_up_iso/cal_p_up_platt`。
> HK.00700 H=10 因果留出区实测：ECE raw 8.5% → isotonic 3.5% → Platt 1.5%，
> Brier 0.260→0.249；原 70 次 p≥0.75 喊单命中仅 56%，校准后不再虚高。

> 目標：在現有 Rust 回測框架中，加入 4 類關鍵診斷：
> - calibration plot（p_up vs empirical）
> - quantile coverage plot
> - tail error decomposition
> - regime clustering（HMM / volatility states）

---

## 1. Calibration plot（p_up vs empirical）

### 1.1 目的與直覺
- **目的：**檢查模型輸出的 `p_up` 是否與真實上漲機率一致。
- **直覺：**如果模型說「p_up = 0.7」，那這一群樣本裡，實際上漲比例應該接近 70%。

### 1.2 數據結構（Rust）
建議在回測結果中保留：

```rust
struct PredictionRecord {
    asof: NaiveDate,
    close: f64,
    p_up: f64,
    actual_ret: f64,   // (close_t+K / close_t - 1.0)
}
1.3 分箱與統計邏輯
分箱（binning）：

例如以 0.05 為步長：[0.0,0.05), [0.05,0.10), ... [0.95,1.0]

每個 bin：

收集所有 PredictionRecord，滿足 p_up 落在該 bin。

計算：

mean_p_up：該 bin 內的平均 p_up

empirical_up_rate：actual_ret > 0 的比例

輸出：

生成一個 Vec<CalibrationPoint>，用於後續繪圖或導出 CSV。

rust
struct CalibrationPoint {
    bin_low: f64,
    bin_high: f64,
    mean_p_up: f64,
    empirical_up_rate: f64,
    count: usize,
}
1.4 視覺化（外部工具）
Rust 負責輸出 CSV / JSON。

用 Python / Vega-Lite / gnuplot 畫：

x 軸：mean_p_up

y 軸：empirical_up_rate

參考線：y = x（完美校準）

2. Quantile coverage plot
2.1 目的與直覺
目的：檢查預測區間（例如 10%–90% quantile）是否真的達到預期 coverage。

直覺：如果你宣稱「90% band」，那實際落在 band 內的比例應該接近 90%。

2.2 數據結構（Rust）
在每筆預測中保留：

rust
struct BandPredictionRecord {
    asof: NaiveDate,
    pred_lo: f64,      // 例如 10% quantile 預測報酬
    pred_hi: f64,      // 例如 90% quantile 預測報酬
    actual_ret: f64,
}
2.3 Coverage 計算
對所有樣本：

判斷 inside_band = (actual_ret >= pred_lo) && (actual_ret <= pred_hi)

統計：

coverage_rate = inside_count as f64 / total_count as f64

如果你有多組 quantile（例如 5%–95%、10%–90%），可以為每組 band 建一個：

rust
struct CoverageStats {
    band_name: String, // "10-90", "5-95"
    nominal_coverage: f64,
    empirical_coverage: f64,
    total_count: usize,
}
2.4 Coverage curve
進階：對不同時間區段 / regime 分別計算 coverage。

視覺化：

x 軸：nominal coverage（例如 0.8, 0.9）

y 軸：empirical coverage

3. Tail error decomposition
3.1 目的與直覺
目的：拆解模型在「尾部事件」（大漲 / 大跌）上的誤差。

直覺：你想知道模型是：

對大漲不敏感（右尾低估）

對大跌不敏感（左尾低估）

還是整體偏差但尾部 OK。

3.2 數據結構（Rust）
假設你有：

rust
struct ReturnPredictionRecord {
    asof: NaiveDate,
    pred_ret: f64,
    actual_ret: f64,
}
3.3 定義尾部
先計算所有 actual_ret 的分位數：

例如 10% 分位數 q10、90% 分位數 q90。

定義：

左尾樣本：actual_ret <= q10

右尾樣本：actual_ret >= q90

中間樣本：介於其間。

3.4 誤差分解
對每一類樣本計算：

rust
struct TailErrorStats {
    segment_name: String, // "left_tail", "right_tail", "middle"
    mae: f64,             // mean absolute error
    bias: f64,            // mean (actual_ret - pred_ret)
    count: usize,
}
比較：

左尾 MAE / bias

右尾 MAE / bias

中間 MAE / bias

3.5 結果解讀
若右尾 bias 明顯為正 → 模型低估大漲。

若左尾 bias 明顯為負 → 模型低估大跌。

若尾部 MAE 遠大於中間 → 模型 tail modeling 不足。

4. Regime clustering（HMM / volatility states）
4.1 目的與直覺
目的：將市場切分成不同 regime（例如：高波動、低波動、上升趨勢、下跌趨勢），再檢查模型在各 regime 的表現。

直覺：模型可能在「穩定上升期」很準，在「急跌期」完全失效。

4.2 特徵構建（Rust）
先為每個交易日構建 regime 特徵：

rust
struct RegimeFeatureRecord {
    date: NaiveDate,
    ret: f64,          // 當日或 K 日報酬
    vol: f64,          // rolling volatility
    vol_of_vol: f64,   // 波動的波動（可選）
    volume: f64,       // 成交量（可選）
}
4.3 HMM / clustering 流程（概念）
這部分可以：

在 Rust 內用現有 crate（如 linfa 做 clustering）

或輸出特徵到 Python，用 hmmlearn / sklearn 做 HMM / clustering，再把 regime label 回寫。

選擇方法：

HMM（隱馬可夫模型）：

狀態數例如 2–4（低波動 / 高波動 / 上升 / 下跌）

或 K-means / GMM clustering：

直接對 [ret, vol, volume, ...] 做聚類。

訓練：

用歷史特徵序列訓練 HMM / clustering。

標記 regime：

每個日期得到一個 regime_id。

rust
struct RegimeLabelRecord {
    date: NaiveDate,
    regime_id: usize, // 0,1,2,...
}
4.4 與預測結果結合
將 regime_id join 到你的預測記錄：

rust
struct LabeledPredictionRecord {
    asof: NaiveDate,
    regime_id: usize,
    p_up: f64,
    pred_ret: f64,
    actual_ret: f64,
}
4.5 分 regime 評估
對每個 regime_id 分別計算：

方向命中率（p_up vs sign(actual_ret)）

band coverage（若有 band）

MAE / bias（pred_ret vs actual_ret）

tail error stats（可再細分）

rust
struct RegimePerformanceStats {
    regime_id: usize,
    directional_hit_rate: f64,
    mae: f64,
    bias: f64,
    coverage_rate: Option<f64>,
    sample_count: usize,
}
5. 整體輸出與報告格式
5.1 建議輸出檔案
calibration_points.csv

quantile_coverage.csv

tail_error_stats.csv

regime_performance.csv

5.2 報告結構（可直接用本 md）
模型與數據說明

Calibration plot 結果與解讀

Quantile coverage 結果與解讀

Tail error decomposition 結果與解讀

Regime clustering 結果與解讀

後續優化方向（例如：校準、改分布、分 regime 模型）

