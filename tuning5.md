tuning5.md — 預測算法優化（review 2026-09-07）

> **逐項 review 與判決（有實驗證據，非照單全收）**：
> ① **標籤死區 → 實驗否決**：label-gate（10股×H=7、knn4、同因果回放，doc/label_gate.py）：
>    eps=0 / 0.2% / 0.5% → 0.505 / 0.504 / 0.500 —— 無增益甚至更差 → **不改 `is_up`**
>    （維持 平盤=非漲，D1 紀律）。
> ② **特徵去共線/換加速度 → 維持現狀**：feature-gate（Wave A 全測）已證 H7 特徵改動無增益。
> ③ **auto_features 前期保護 → 部分已存在**：above_sma20 分母為實際計數（非固定 20）；
>    早期失真列只在 train 窗內，影響可忽略。
> ④ **UI 門檻/中性帶 → ✅ 採納並已實作**：holdings_forecast.sh 中性帶 FLAT_BAND 預設 **0.12**
>    （p_cal∈[0.38,0.62]→FLAT），BUY/SELL 對稱門檻 **0.62**（仍疊加歷史模擬品質閘：多/空單
>    正期望才給動作）。K 保持 20 ∈ [15,30]。實測重跑：00100/02333/06613 弱訊號轉 FLAT/HOLD，
>    僅高信心+模擬正期望者給動作（00857/03668 BUY）。

---

一、 核心原因分析二元標籤（is_up）的邊界缺陷問題：analytics.rs 中將平盤（close_fwd == close_now）歸類為非漲（即跌）。在實際交易中，微幅震盪或微漲/微跌都會被強制硬切為 0 或 1。影響：模型在盤整期會產生大量噪聲訊號（Whipsaw）。距離度量（Euclidean Distance）缺乏權重與特徵正規化問題問題：kNN 在特徵空間計算歐氏距離時，預設的 4 個特徵（mom5, mom10, vol10, above_sma20）被賦予相同權重。mom10 與 mom5 存在極強的多重共線性（Multicollinearity），會雙重加權動量特徵，壓制了波動率特徵。滾動統計量（Auto Features）計算無邊界保護問題：auto_features 中計算 vol10 和 above_sma20 時，在數據前期的 saturating_sub 會導致滾動窗口不足（如第 2 天算 SMA20），產生失真特徵參與 kNN 匹配。UI 層門檻與動作邏輯過於簡單問題：app.py 中直接以 $p > 0.5$ 判定買入/賣出。kNN 機率本質上偏向中位數，未經置信度門檻（如 $p > 0.65$ 買入，$p < 0.35$ 賣出）篩選的訊號勝率非常低。二、 Rust 引擎（analytics.rs）優化修改1. 加入漲跌幅死區（Volatility-based Buffer / Deadband）引入標準差或最小變動百分比，排除微幅震盪的噪聲標籤。Rust// 修改 analytics.rs 中的 is_up 邏輯（可加上死區門檻，例如 0.2%）
#[inline]
fn is_up(close_now: f64, close_fwd: f64) -> bool {
    let ret = (close_fwd - close_now) / close_now;
    // 只有漲幅超過 0.2% 才算 UP，避免微小噪音干擾類比匹配
    ret > 0.002
}
2. 優化特徵計算（去除共線性並增加長短週期對比）調整 auto_features，引入 RSI 與 動量變化率（Acceleration），並對早期數據加上有效性保護：Rustfn auto_features(closes: &[f64]) -> Vec<Vec<f64>> {
    let n = closes.len();
    let mut out = vec![vec![0.0f64; 4]; n];
    
    for (i, row) in out.iter_mut().enumerate() {
        let a5 = i.saturating_sub(5);
        let a20 = i.saturating_sub(20);
        
        // 1. 短期動量 mom5
        row[0] = if i >= 5 { closes[i] / closes[a5] - 1.0 } else { 0.0 };
        
        // 2. 動量加速度 (mom5 - past_mom5) 替代高共線性的 mom10
        let a10 = i.saturating_sub(10);
        let past_mom5 = if a5 >= 5 { closes[a5] / closes[a10] - 1.0 } else { 0.0 };
        row[1] = row[0] - past_mom5; 
        
        // 3. 偏離 SMA20 程度 (均線乖離率)
        if i >= 19 {
            let s: f64 = closes[(i - 19)..=i].iter().sum();
            row[3] = closes[i] / (s / 20.0) - 1.0;
        } else {
            row[3] = 0.0;
        }
    }
    
    // 4. vol10: 波動率（至少需要 3 個 bar 才計算）
    for (i, row) in out.iter_mut().enumerate().skip(2) {
        let lo = i.saturating_sub(9).max(1);
        let seg = &closes[(lo - 1)..=i];
        let (mut sum, mut ss) = (0.0f64, 0.0f64);
        for w in seg.windows(2) {
            let r = w[1] / w[0] - 1.0;
            sum += r;
            ss += r * r;
        }
        let c = (seg.len() - 1) as f64;
        let mean = sum / c;
        row[2] = (ss / c - mean * mean).max(0.0).sqrt();
    }
    out
}
三、 Streamlit GUI (app.py) 訊號篩選優化在 UI 展示層引入 機率門檻（Probability Threshold） 與 動態動作過濾，避免將低置信度的 $p \approx 0.52$ 判斷為 BUY。修改 app.py 中的訊號判定邏輯：Python# 在 app.py 中加入機率門檻篩選與動作轉化函數
def assign_action(p_cal: float, upper_thr: float = 0.62, lower_thr: float = 0.38) -> tuple[str, str]:
    """
    根據校準後的機率 p_cal 產生高勝率的方向與動作指令
    """
    if pd.isna(p_cal):
        return "FLAT", "HOLD"
    
    if p_cal >= upper_thr:
        return "UP", "BUY"
    elif p_cal <= lower_thr:
        return "DOWN", "SELL"
    else:
        return "FLAT", "HOLD"  # 中性區間不操作，避免頻繁交易

# 讀取預測並套用過濾器
def process_forecast_with_thresholds(df: pd.DataFrame, upper_thr: float = 0.62, lower_thr: float = 0.38) -> pd.DataFrame:
    if df.empty or "p_cal" not in df.columns:
        return df
    
    # 重新計算置信度導向的方向與動作
    res = df.copy()
    actions = res["p_cal"].apply(lambda p: assign_action(p, upper_thr, lower_thr))
    res["direction"] = [a[0] for a in actions]
    res["action"] = [a[1] for a in actions]
    return res
四、 改進後的實施建議與驗證步驟調整超參數 $K$ 與 $H$：日線級別的歷史類比（Analog kNN）在 $K \in [15, 30]$ 表現較穩健。若 $K$ 過小（如 $K=5$），容易受到單一歷史極端值干擾。利用 fwd_walk 校準機率：確保在運行預測時使用 cal_p_up_iso（等凝迴歸校準後的機率），而非未校準的原始 p_up。回測驗證：修改 Rust 引擎與 UI 門檻後，進入 UI 中的 「指定日期回歸測試」 頁面，運行 多日回測統計，觀察 方向正確率 是否能穩定突破 55% ~ 60% 的有效閾值。