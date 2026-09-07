# forecast-function.md — 持倉趨勢預測：已實現 / 不做清單與原因

> 對照 `forcast1.md`（通用藍圖）與 `design.md`（落地設計）的最終功能盤點。
> 一口結論：**「日 bar → 特徵 → 標籤 → 模型 → 回測 → 實時」骨架全部落地；生產模型是
> 「歷史類比 kNN + 因果校準」，經三輪實驗閘（特徵/M1/pooled）證明換更強模型無顯著增益；
> 最後補齊的是報表決策層（方向/動作/倉位建議/事件風險），而非更複雜的模型。**

---

## 1. 已實現功能

### 1.1 資料與行情
| 功能 | 位置/用法 | 說明 |
|---|---|---|
| Futu OpenD 日 K（前復權 qfq）| `md klines futu`（`stock_*.sh` 預設 SOURCE=futu）| 需本機 OpenD 登入（port 11111）；本地 cache `data/market/futu/*/1d__qfq.parquet` 增量 |
| Yahoo 日 K 備援 | SOURCE=yahoo（`06613.HK` 形式）| 免 key，但 IP 限流(429)不穩 → 僅備援 |
| Parquet 離線 | SOURCE=parquet FILE=… | 免重抓、可回放 |
| 恆指/恆生科技日線快取 | `analytics_out/HK_INDEX_daily.parquet` | 逐年分頁拉取（HK.800000/800700），runner 過期自動補拉 |
| 每股 bars | `analytics_out/<SYM>_bars.parquet` | OHLCV + qfq close，決策/模擬共用 |

### 1.2 引擎函式（gtv-engine，嚴格因果）
| 功能 | 語法 | 說明 |
|---|---|---|
| 歷史類比方向機率 | `fwd_proba(name,H,k[,feats])` | 決策單行：t,close,p_up,p_down,n_analogs,hit_rate |
| **Walk-forward 回放** | `fwd_walk(name,H,k[,warmup\|cal_frac][,feats][,'model'])` | 嚴格 past-only；輸出 up/actual_ret/q05–q95/vol20 + 因果校準欄（Platt/iso）+ `model` 欄（D2） |
| As-of 回歸測試 | `fwd_regress(name,asof_ns,H,k[,feats][,'model'])` | 指定過去任一日重播生產決策 |
| Label 語意（D1）| `is_up(close_now,close_fwd)` | 二分類：`close[t+H]>close[t]`；平盤=非漲，單一真源 |
| 模型選擇 seam（D2）| 尾端 `'analog'|'gbdt'` token | `gbdt` 保留並明確報「M2 未建」；輸出加 additive `model` 欄（walk 19 / regress 14 欄），scripts 零改動 |

### 1.3 校準與診斷
| 功能 | 產出 | 說明 |
|---|---|---|
| 因果機率校準（Platt/isotonic PAV）| `fwd_walk` 內 `cal_p_up_*` + `stock_analysis` python fit | 只在擬合窗**之後**生效（gap≥H）；校準模型可存檔復用（CALMODEL） |
| 校準圖資料 | `calibration_points.csv`（`stock_diag.sh`）| 分桶 mean p vs 實測漲率 + ECE |
| Quantile coverage | `quantile_coverage.csv` | band 誠實性 |
| 尾部誤差拆解 | `tail_error_stats.csv` | 左右尾 MAE/bias |
| Regime 分桶表現 | `regime_performance.csv` | vol20 分桶 / kmeans（可選）|
| 門檻建議 | `stock_calib.sh` | 分桶命中 × 門檻表 → 建議 THR |
| As-of 迴歸 | `stock_regress.sh` | 過去多日決策 vs 真實 |

### 1.4 決策與報告（scripts）
| 功能 | 檔案 | 說明 |
|---|---|---|
| 單股每日分析+提醒 | `stock_analysis.sh` | 校準後機率 + 門檻 + exit code（0/1/3，cron 可判）|
| **持倉組合每日報告** | `holdings_forecast.sh` | 讀 `HOLDING.txt`；每股自動用 `HOLDING.cfg` 的 **HORIZON/THRESHOLD** 地圖（00857→H20@0.85…）|
| direction 三態 | ↑ ↓ —（FLAT）| `|p_cal−0.5|<FLAT_BAND(0.07)` → FLAT（機率無信心帶）|
| action 三態 | BUY/HOLD/SELL | 今日訊號 × 歷史模擬側期望（規則寫在 script）|
| action 佐證 | sim_L / sim_S 欄 | 該股該週期多/空側「n/平均bp」|
| 恆指情境欄 | hsi, hsi_ret5/20/60, stk_ret5/20, rel20 | 判斷訊號是 alpha 還是 beta |
| 事件風險覆蓋層 | `EVENT_MODE=1` | 門檻建議 ↑0.80、警示文案、不追新倉提示 |
| 週末/長假跳空附錄 | `holdings_weekend_gap.tsv` | 各股 avg\|gap\|/p2/p98/std（歷史實測）|

### 1.5 訊號紙上模擬（決定「訊號會不會賺」）
| 功能 | 檔案 | 說明 |
|---|---|---|
| 每股模擬網格 | `stock_sim.sh` | THR×成本掃描；方向感知（多/空分開）；`SIZING=5` 倉位層（strength .6→半倉、.7→全倉）；輸出 n/win/mean-net-bp/Sharpe/maxDD(sized vs full)/側均值 |
| 組合動作彙總 | `holdings_sim.sh` | 讀 H7/H20 模擬 → `holdings_action_7d.tsv` 動作欄；`--run` 自動補跑 |
| 每股最優門檻/H 掃描 | `holdings_thresh_scan.tsv` | H=7 vs 20 × THR，找出唯一強訊號：**00857 H20@0.85** |

### 1.6 實驗閘與決策證據（「用數據說不」的基礎）
| 閘 | 檔案 | 結論 |
|---|---|---|
| 特徵實驗 |（doc 內）| 15 維廣特徵對 kNN 無增益（hit ±0.05 混合）|
| M1 gate | `doc/m1_gate.py` | knn4 .507 / gbdt4 .512 / gbdt15 .507 / gbdt15ix(+恆指) .510 → 不啟動 M2 |
| Pooled gate | `doc/pooled_gate.py` | pooled hit 平手、Brier 較好(.266)；板塊互污染（油/科技）→ sector-group 是潛在下一步 |
| D1/D2 | analytics.rs | 三分類延後；單一 `fwd_*` 介面 + MODEL token（均含單元測試）|

---

## 2. 明確認定「不做」+ 原因

| 項目 | 原因（有實驗/設計證據）|
|---|---|
| **原生 GBDT / XGBoost / Random Forest 生產化（M2）** | M1 gate：同嚴格 time-CV 下 hit 全 ≈50%，GBDT 無顯著增益、raw Brier/ECE 反而更差（需多層校準）。設計原則：不為用 ML 而做 ML，閘不過就不投資 |
| **深度學習（LSTM/GRU/Temporal CNN）** | 單股僅 ~150–1000 根日 bar，樣本不足以支撐；forcast1.md 自己也同意先 XGBoost；且方向訊號本就 ≈ 噪音，DL 只會過擬合 |
| **引擎內廣特徵層 M0（RSI/MACD/BB/KDJ/ADX/量能/形態作為 engine fn）** | 兩次實測：加 15 維特徵對 kNN 無提升、對 GBDT 反更差；KDJ/ADX 從未實作 → 除非 sector-pooled/regime 續推否則不值得 |
| **真·三分類 label（±1% 橫行帶，D1）** | 會改變校準/門檻/診斷整套語意；報表已用「FLAT 機率帶」覆蓋「無信心」用途；ε(H) 規格已備但啟用收益低 |
| **自動交易 / 自動倉位下單（加倉/減倉/空倉執行）** | repo 無交易執行框架、且屬研究用途；已提供 **SIZING 倉位比例建議 + EVENT_MODE** 作為「人決策」的輸入，不代下單 |
| **新聞/NLP 事件「預測」特徵** | 事件發生於收盤後，歷史價格特徵物理上無法預測（殘差風險）。正確處理 = 風險管理層：EVENT_MODE + 跳空統計（已做），不是把新聞塞進模型 |
| **逐年 rolling block 回測** | 已被更嚴格的 cadence walk-forward（past-only + embargo + cal 窗）取代，語意超集 |
| **Yahoo 當主資料源** | 429/限流不穩；Futu OpenD（本機登入）為主，yahoo 只留備援 |
| **股票代碼即時/盤中預測** | 設計限收市後 daily（H 交易日），盤中無資料通道且非需求 |

---

## 3. 暫緩 / 有條件才做（未做但保留）

| 項目 | 觸發條件 |
|---|---|
| Sector-group pooled（AI/科技池 vs 能源池…）| 若想再推模型上限：pooled gate 顯示混池互相污染、Brier 已較好 → 分池有機會把 hit 也做實；**過閘才生產化** |
| 組合 vs 恆指 buy-hold 報酬基準模擬 | 想把 sim 的收益曲線對齊「同期買恆指」做 alpha 對比（資料齊，屬小活）|
| 漂移監控自動化（EWMA 命中/特徵 PSI，design §8 D6）| 每日決策累積夠多（目前決策日資料尚少）|
| 恆指波幅 VHSI / 布油期貨開盤前情境欄 | 需相應行情權限（現無確認）|
| 每股停損規則層 | 若把 SIZING 建議推進到「含停損的組合模擬」|

---

## 4. 尚未決策的開放項（design.md §11 D3–D6）
train 窗是否 cap、conformal band 來源、`features()` 放哪個 crate、漂移門檻參數 —— 皆與 M2 是否重啟綁定，目前無緊迫性。

## 5. 免責
全部為研究/教育用途，不構成投資建議；歷史模擬（含 00857 H20@0.85）不代表未來。
