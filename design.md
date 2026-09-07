# gtvdb 5–10 交易日走勢預測：ML 模型層設計（design.md）

> 目的：把 `forcast1.md` 的通用藍圖（日 bar → 特徵工程 → 標籤未來走勢 → 模型訓練 →
> 回測 → 實時預測）落地為 gtvdb 的具體設計，**並與既有的 kNN/校準/診斷基建對齊**。
>
> 核心洞察：現有整條 pipeline（`fwd_walk` 回放 → isotonic/Platt 校準 → 門檻決策 →
> 四份診斷 CSV）是 **model-agnostic** 的 —— 它只消費
> `(t, p_up, p_down, up/actual_ret, q05..q95)`。forcast1.md 要求的「模型訓練」其實
> 只是把 pipeline 最前面的 **scorer**（今日 = 歷史類比 kNN）換成「特徵更廣 + 樹集成」。
> 本設計最小化改動面：**只新增 scorer + 特徵層，下游原封不動**。

---

## 1. 現況與接縫（baseline，程式碼事實）

| 元件 | 位置 | 語義 |
|---|---|---|
| 統一 bar 表 `<SYM>_bars` | `HftRegistry`（registry-by-name） | `date(Int64 ns) open high low close volume`，來源 futu/yahoo/parquet |
| `fwd_proba(name,H,k[,feats])` | `analytics.rs` `compute()` | 只算最後一根：`t, close, p_up, p_down, n_used, hit_rate, m` |
| `fwd_walk(name,H,k[,warmup\|cal_frac][,feats])` | `analytics.rs` `walk_compute()` | 嚴格 past-only 逐 bar 回放；輸出 `idx,t,close,p_up,p_down,up,n_analogs,actual_ret,q05..q95,bar_ret,vol20,cal_iso,cal_platt`（18 欄） |
| `fwd_regress(name,asof_ns,H,k[,feats])` | `analytics.rs` `regress_compute()` | 指定 as-of 日期重播生產決策 → 生產 p_raw |
| auto features | `auto_features()` | mom5/mom10/vol10/above_sma20（只用 close） |
| 校準 | `stock_analysis.sh` + python | Platt/isotonic，fit 於 walk 最早 `CAL%` 行（因果留出 gap=H），存 `*_calib_model.tsv` |
| 診斷 | `stock_diag.sh` | `calibration_points.csv / quantile_coverage.csv / tail_error_stats.csv / regime_performance.csv` |
| 門檻/提醒 | `stock_analysis.sh` | `strength=max(p_cal,1-p_cal) ≥ THRESHOLD(0.75)` → exit 3 |
| 測試 | `analytics.rs` `#[cfg(test)]` | rising/walking series 因果單元測試 |

下游 scripts 全部**以欄名動態解析**（`"p_up" in cells`、`"bar_ret" in t[0]`），
所以只要新 scorer 輸出相同欄名的表，`stock_calib.sh` / `stock_diag.sh` /
`stock_analysis.sh` 的 python 解析層**零修改**即可復用。

---

## 2. 與 forcast1.md 的差異對照（逐節定案）

| forcast1.md 提議 | 本設計定案 | 原因 |
|---|---|---|
| 「未來 5 日」 | `HORIZON` 參數化，預設 **10**（交易日），H=5 為合法值 | repo 既有證據/校準/文件全以 H=10；5 日 ≈ 短 1 週，命中率預期更低（~50–55%），門檻需按 H 重新導出（機制已支援） |
| 二分類 vs 三分類(±1%) | **二分類 `up_i = close[i+H] > close[i]`**；三分類列 Phase B 擴展（D1） | 平盤算「非漲」與現有 `up/actual_ret>0` 語意一致；三分類改動校準/門檻/診斷語意，先不做 |
| 特徵清單（價格/量/TA/趨勢/形態） | 見 §4 特徵模組（因果窗口 + NaN 策略 + train-only 標準化） | 修補 warm-up 洩漏與 fit 紀律 |
| XGBoost/Random Forest | §5：先 benchmark 定 ceiling → **引擎內原生 GBDT**（Rust） | 單二進位、零 python 運行時、可稽核；資料量 1–2k×20d 任何樹實作皆秒級 |
| Rolling window 逐年回測 | §6：**cadence 重訓 walk-forward**（每 `C=63` bar 重訓一次，embargo=H） | kNN 可每 bar refit，GBDT 不可；逐 fold 特徵重算防穿越 |
| 年化報酬/Sharpe/maxDD/命中/成本 | §7：命中/ECE/Brier + 既有診斷 + 訊號紙上交易模擬 | Sharpe/maxDD/成本需「機率→倉位」規則，repo 現為提醒框架；模擬層放 M3 |
| 實時每日預測 | §8：沿用 `stock_analysis.sh`；新增 model artifact 版本化 + 漂移監控 | 決策日 p_raw 來自 `fwd_regress` 同構的 as-of scorer |

---

## 3. 總體架構

```text
<SYM>_bars (日K, 前復權)
   │  features() 引擎函式（§4，因果、一次算、共用）
   ▼
<SYM>_feat   ← 廣特徵（基準 + 價量 + TA + 形態 + regime）
   │
   ├── scorer: analog（既有 kNN，MODEL=analog 預設，輸出不變）
   └── scorer: gbdt（新，MODEL=gbdt）── 同一組 traits
          │  每個 decision bar 用「只在過去 fit 的模型」給 p_up/p_down/q05..q95
          ▼
   回放表（欄位 = fwd_walk 18 欄 ＋ model_id）   ← 下游不變
          │
   ├── stock_calib.sh   → 分桶命中 / 門檻建議        （零修改）
   ├── stock_diag.sh    → calibration/coverage/tail/regime（零修改）
   └── stock_analysis.sh→ 決策日 p_raw → Platt/iso → 提醒（零修改，新增 MODEL env）
```

**設計原則**
1. **Scorer 介面化**：`trait Scorer { fn predict(train, query) -> Score; }`；
   `AnalogScorer`（現有邏輯平移）與 `TreeScorer`（新）並存，由 cfg/`MODEL` 選。
2. **因果不變式**（`fwd_walk` 已強制，所有新路徑繼承）：decision bar `i` 只能用
   `j+H ≤ i` 的已解析標籤列；特徵只依賴 `≤i` 的歷史；標準化參數只在 train window 內 fit。
3. **下游 schema 相容**：gbdt 回放輸出欄位集合 ⊇ fwd_walk 現有 18 欄（+`model_id`），
   多餘欄不影響 scripts 的欄名解析。
4. **全域超參、每股只調門檻**：防 multiple-testing / 過度擬合（見 §10）。

---

## 4. 特徵模組（M0，最先做，全部 scorer 共用）

新引擎函式（`feature.rs`，registry-by-name，同 `knn/ohlc` 模式）：

```text
features('<SYM>_bars', mode) -> 註冊為 <SYM>_feat 的表
  mode = 'std'  （基準，取代現 auto_features 位置）
       = 'full' （基準 + 廣特徵，M2 gbdt 用）
```

**基準層（std，純 close 即可，向前相容）**：現 auto_features 四項保留，補
`ret_1, mom_20, dist_hi_lo_20`。

**廣特徵層（full，OHLCV）**：

| 類別 | 特徵 | 窗口/參數 |
|---|---|---|
| 動量 | `ret_1/5/10/20`（已含）、`mom_60` | 1/5/10/20/60 |
| 波動 | `vol_10/20`（ret std）、`atr_14/close`、`range_pct=(high-low)/close` | 14 |
| 均線結構 | `close/sma5,10,20,60 - 1`、`sma20>sma60`(0/1) | 5/10/20/60 |
| 技術指標 | `rsi_14`、`macd_hist`(12,26,9)、`bb_pos=(close-lb)/(ub-lb)`(20,2σ)、`adx_14` | 標準 |
| 量能 | `vol/ma_vol_5,20 - 1`、`vol_z20` | 5/20 |
| 形態/市場微觀 | 陰陽(0/1)、連陽/連陰天數、`high>prev 20d high` 突破(0/1)、`close` 於 20d 高低位置 | 20 |
| regime | `vol20` 分位（延續 stock_diag 的 vol regime 概念，Phase B2 才入 model） | 20 |

**紀律（寫死並測試）**
- 每欄 `i` 只用 `≤ i-H_feat` 的資料（因果 rolling/lag）；`RSI/MACD/ADX/BB` 的
  warm-up 前 `NaN` → 直接 drop 前 ~ max(warmup) 列（在回放裡屬 train 區即可，無洩漏）。
- 標準化（z）在**每個 train window 內** fit/apply（與 `walk_compute` 的
  `z_stats + z_apply` 相同，抽出共用）。
- split/dividend：一律吃已前復權收盤（futu `adjust=qfq` / yahoo `adjclose`），
  特徵用復權價算、volume 原始值（vol 比率特徵不受 scale 影響）。
- 實作為純 Rust 滑窗（可吃 `gtv-array` 的 mavg/msum 底層），單股 2k bar × ~30 欄 = 毫秒級。

---

## 5. 模型層（M1–M2）

### 5.1 決策：先 benchmark 定 ceiling，再做原生

| 選項 | 內容 | 優點 | 缺點 | 用途 |
|---|---|---|---|---|
| A | kNN + full 特徵（現有框架加 `feats` 參數即可） | 零新依賴、當場可跑 | 距離在高維易退化 | 新 baseline（M1 即產出） |
| B | python3 + xgboost 離線 benchmark（parquet 進出，不入生產） | 最快驗證「樹模型到底有沒有訊號天花板」 | 破壞單引擎、不可稽核 | **M1 訊號可行性閘** |
| C | **引擎內原生 GBDT（Rust，自研小模組）** | 單二進位、零外部運行時、完全稽核、與既有 UDF 文化一致 | 開發成本 ~600–900 LOC | **生產（M2）** |

> 若 B 在同等 walk-forward 回放上不顯著優於 A（非劣性檢定，§7 閘），則 M2 不做 C，
> 只收 M0 特徵層的增益，結論寫回文檔。**不要為了「用了 ML」而做 ML。**

### 5.2 原生 GBDT 規格（C，`gtv-engine/src/model/gbdt.rs`）

- 二元分類 GBDT：log-loss + 葉值牛頓更新（二階），tree 數 ≤ 200，靠
  **chronological 驗證段 early-stop**（見下）。單元輸出 `p_up ∈ (0,1)`。
- 同時 train 一條 `ret_H = close[i+H]/close[i]-1` 的 L2 迴歸樹鏈，給點預測 `mu`；
  不逐葉估 quantile，改用 **split-conformal residual band**：
  在每個 fold 的內部驗證段收集 `resid = actual_ret − mu`，取 emp. quantile，
  `q05..q95 = mu + resid_q05..q95`。小樣本、distribution-free、coverage 名義上誠實
  （可直接被現有 `quantile_coverage.csv` 檢查）。
- 全域預設（每種都寫死在 cfg，防 multiple-testing）：
  `lr=0.05, depth=3, min_child_w=5, subsample=0.8, colsample=0.8, n_est≤200`,
  internal validation = train 尾部最後 20% 時序段。1–2k×30d 下單次訓練 < 1s。
- 序列化：自訂二進位或 bincode；artifact 帶 `model_id = <sha256(feats schema+params)>-<trained_upto_date>`。

### 5.3 Scorer 對齊的引擎介面（**D2 已定案**：單一 `fwd_*` 家族 + 尾端 MODEL token）

**不做新 `ml_*` 家族。** `fwd_walk` / `fwd_regress` 接受可選尾端 `model` token：

```text
fwd_walk(name, H, k [, warmup|cal_frac]* [, feats] [, 'analog'|'gbdt'])
fwd_regress(name, asof_ns, H, k [, feats] [, 'analog'|'gbdt'])
```

- token 必須是**最後**一個位置參數的 bare string ∈ {`analog`,`gbdt`}（`split_model_token`）；
  其餘字串仍為 feats CSV，數字照舊 warmup/cal_frac —— 既有呼叫全部不受影響。
- `gbdt` 目前被 `ensure_available` 明確拒絕（error 指到 design.md M2）；M2 落地後只加一個分支。
- 輸出**新增尾端 `model` 欄（Utf8）**（additive，consumers 按欄名取用）：
  walk 18+1、regress 13+1 欄；scripts 的 python 解析層零修改。

> Scripts 層用 `MODEL=analog|gbdt`（env）選擇，預設 `analog` → 今日行為零變化
> （script 接線列 M3）。**已實作**：`analytics.rs` `split_model_token / ensure_available /
> append_model_col` + `is_up` label seam（2026-02-12，測試 `trailing_model_token_and_availability` 等）。

---

## 6. 回測協定（walk-forward with cadence）

```
對 gbdt：決策窗以 cadence 切塊。t=0 起：
  重訓點 r_k = k*C（C=63 交易日，≈季度）
  模型 k 用 train 區 j：j+H ≤ r_k 且 j ≥ r_k − MAX_TRAIN(1,500 列 cap? 見 D3)
  驗證/early-stop 段 = r_k 前最後 20% train 時序
  以模型 k 給 [r_k, r_k+C) 內每個 decision bar i（i+H ≤ n）打分
  產生與 fwd_walk 同構的列：idx,t,close,p_up,p_down,up,actual_ret,q05..q95,...
  （feature/標準化一律在各 fold 的 train 內重算 —— 無全域預算洩漏）
warmup 下限：~2y（~500 bar）→ 短歷史股自動退回 analog（並在輸出標 model_id=analog）
```

- **embargo**：label `j` 只算到 `j+H ≤ i−0`；校準 fit（§現有 isotonic/Platt）額外要求
  決策日 gap ≥ H（`fwd_walk` 已實作 `idx[r] >= last_train_idx + horizon`），繼承。
- 計算量：單股 ~50 次重訓 × <1s ≪ 現有 diag 成本，無效能風險。

---

## 7. 評估閘與指標（決定 MODEL 預設誰）

**主閘（每股、在 `fwd_walk(…,'MODEL')` 回放列上）**
1. 方向命中率 `hit = mean(up == (p_up>0.5))`；
2. 校準 `ECE(10 bins)` 與 `Brier`（現 `stock_calib.sh`/diag 邏輯，輸入表相同）；
3. **gbdt vs analog 非劣性**：`hit_gbdt ≥ hit_analog − 0.02`（n≥200 eval rows 時）；
   任一股不達標 → 該股 MODEL 預設回退 analog，log 理由。
4. 既有診斷自動跑：quantile coverage（band 誠實性）、tail bias 左右尾、
   regime 分桶命中 —— 全由 `stock_diag.sh` 直接吃 `fwd_walk(…,'MODEL')` 輸出。

**訊號經濟層（M3，不影響 M2 閘）**
- 「機率→動作」規則與 forcast1 一致但明確化：`strength ≥ THR` 才出手、`H` 日後平倉；
- 紙上模擬（repo 無倉位框架，不建實盤）：`<SYM>_signals_sim.tsv`：
  `mean ret_H, win%, n_sigs, maxDD, per-signal Sharpe`，成本 = round-trip bps（預設 20，可設），
  並輸出「成本掃描」（0/10/20/50 bps）→ THR 建議表（延續 stock_calib 的 threshold sweep 精神）。
- 注意 H=5 短窗：成本敏感、命中近 50%，THR 建議會比 H=10 高或無有效訊號 —— 屬預期。

---

## 8. 實時預測 / 每日運行（M3）

沿用 `stock_analysis.sh` 骨架，新增：

| 環境變數 | 預設 | 意義 |
|---|---|---|
| `MODEL` | `analog` | `analog`/`gbdt`（gbdt 時按 cadence 自動重訓/載入 artifact，M2/M3） |
| `MAX_AGE_DAYS` | `7` | model artifact 最大年齡，超過自動重訓（cadence 與回測一致） |
| `DRIFT_WATCH` | `on` | 見下 |

- **決策日**：`fwd_regress(last_bar, H, k [, 'MODEL'])` → p_raw → 既有 Platt/iso（model file
  加版本 tag，`*_calib_model.tsv` 表頭多一欄 `model_id`）→ THR 提醒，exit code 不變。
- **漂移監控（新）**：每日決策進 `*_decisions.tsv`；對最近 60 個已解析決策算
  EWMA hit-rate；若低於「該 confidence 桶歷史命中 − 0.15」連續 2 次 → stdout 警告 +
  exit code 8（可被 cron 接），建議重訓或回退 analog。特徵分佈 PSI 監控列 Phase B。
- 輸出檔（M3 後）：
  `analytics_out/<SYM>_feat.parquet`、
  `analytics_out/models/<SYM>_H<H>_gbdt_<model_id>.(model|meta)`、
  `analytics_out/<SYM>_walk.tsv`（= diag 輸入，含 model 欄）、
  `analytics_out/<SYM>_signals_sim.tsv`、既有 `*_analysis.tsv` 不變。

---

## 9. 里程碑與驗收

| 里程碑 | 內容 | 驗收 |
|---|---|---|
| **M0 特徵層** | `features()` 引擎函式 + std/full 模式 + 因果/NaN 單元測試 | 合成「單邊趨勢/regime 切換」序列：特徵值方向正確；warm-up 前無洩漏值；`fwd_walk(...,feats)` 吃 full 特徵即跑 |
| **M1 可行性閘** | A 新 baseline（kNN+full）；B xgboost 離線 benchmark，同一 walk 回放 | ✅ 已執行（2026-09-06）：8 隻持倉 × H=7、tail60% 評測窗、cadence=21、embargo=H，3334 決策。knn4 hit .507/Brier .279/ECE .149；gbdt4 .512/.286/.175；gbdt15 .507/.303/.202；gbdt15ix（完整恆指+恆生科技 2021→今）**.503/.309/.215** → **整體皆無顯著增益，閘不通過**。但 15ix 個股分化：AI/科技名受惠（06613 .619、02513 .581）、REIT/傳統股受損（00823 .460、01929 .456）→ 指數特徵值得在 pooled/regime（Phase B）再驗 |
| **M2 原生 GBDT** | ~~`gtv-engine` 內 `gbdt.rs` scorer…~~ | ❌ **閘未過 → 不啟動**（設計原則：不為用 ML 而做 ML）。保留 kNN+校準為生產 scorer。重啟條件：pooled 大樣本 / H≥20 / regime 條件訊號（Phase B） |
| **M3 接線** | `MODEL` env 貫穿三 scripts；artifact 生命週期 + 漂移監控；signals sim | 部分。`fwd_walk` MODEL token 已落地（D2）；M2 未啟動 → MODEL=gbdt 分支保留。**訊號紙上模擬已落地**：`stock_sim.sh`（THR×cost 掃描：n/win/mean-net-bp/per-signal Sharpe/maxDD，H=7 全部 10 持倉實測，見 §12） |
| **事件風險覆蓋層** | `EVENT_MODE=1` 開關（門檻建議 ↑0.80、警示文案）+ 週末/長假 gap 附錄（avg/p2/p98/std） | ✅ 已落地於 `holdings_forecast.sh`（2026-09-06）；gap 表存 `analytics_out/holdings_weekend_gap.tsv` |
| **Phase B** | 三分類(±ε, D1 已延後)、跨股 pooled、恆指/恆生科技參考對比、H≥20、regime 條件 | ✅ pooled gate 已執行（2026-09-06，10 隻持倉 × H=7、4526 決策、生產一致 knn4 基準）：knn4 hit .505/Brier .279；pooled-gbdt15ix .506/**0.266**；per-stock-gbdt15ix .497/.313。**方向命中持平 → M2 仍不啟動**；pooled 增益在機率品質(Brier)與特定組（AI/科技 .64/.65、消費/REIT .51）——能源/油股被混訓拖累 → 下一個候選：**sector-group pooling**（AI/科技 vs 能源分池，或加 code/sector 特徵）與 H≥20 |

**全域驗收（每 milestone）**：重跑冪等（同資料同 cfg 同輸出）；不加新重型依賴
（xgboost 只在 M1 benchmark 沙盒）；所有新增有 `#[cfg(test)]`（trend/regime/
leakage/coverage/artifact round-trip）。

---

## 10. 風險與「不做」清單

- **過度擬合紀律**：超參全域固定（§5.2），不許 per-stock 搜參；門檻每次從 walk 因果重導；
  ML 增益必須過 §7 非劣性閘，否則預設留在 analog。跨股反覆試 threshold 也是 multiple-testing —— 禁止。
- **穿越**：所有特徵/標準化/標籤的因果窗口是硬不變式，靠單元測試把關（M2 驗收）。
- **regime change**：GBDT 在政策/財報 regime 切換照樣失效 —— regime 分桶診斷
  （現成）拿來做「模型何時不可信」的監看，而不是假裝模型會自己適應。
- **不做**：深度學習（LSTM/GRU —— 資料量不足，forcast1 自己也同意）；
  實盤自動下單（repo 無此框架）；即時/盤中預測（本設計限收市後 daily）。
- 免責：研究/教育用途，不構成投資建議（沿用既有 docs 措辭）。

---

## 11. 開放決策（實作前定案，D）

**已定案：**

- **D1 三分類（±ε 橫行帶）—— 定案：不做（延後 Phase B），二分類語意收斂為單一
  `is_up(close_now, close_fwd)`（`analytics.rs`）**。理由：三分類改動校準/門檻/診斷
  語意且 Phase B 才有跨股資料支撐 ε。Phase B 啟用時 ε 依 H 縮放（H=5: 0.5% / H=10: 1%，
  見 `is_up` doc），把「平盤=非漲」的現有語意（含 `regress up=-1` 未足 H 日約定）保持不變。
  測試：`flat_close_counts_as_not_up`（全平盤序列 p_up=0）。
- **D2 引擎函式面 —— 定案：不做 `ml_walk/ml_regress/ml_train`；在既有
  `fwd_walk`/`fwd_regress` 加可選尾端 `model` token（`'analog'` 預設 / `'gbdt'` 保留），
  輸出加 additive `model` 欄**。理由：全面復用現有 warmup/cal_frac/feats 解析與嚴格因果
  回放框架，scripts 零改動、零行為變化；gbdt 尚未實作 → `ensure_available` 給明確 error
  （指到 M2），M2 落地時只加分支。`ml_train` 的 artifact 需求屆時由 script/引擎層視需要補。
  已實作：`split_model_token / ensure_available / append_model_col / ScorerKind` +
  `model` 欄（walk 18→19、regress 13→14）；測試
  `trailing_model_token_and_availability`、`walk_output_is_tagged_with_model`。

**仍開放：**

- **D3** train 窗是否 cap（1,500 列?）避免久遠 regime 拖累；還是全歷史（現 analog 全歷史）。
- **D4** conformal residual band 用內部驗證段 vs 每 fold 的 OOF 全段。
- **D5** `features()` 實作放 `gtv-engine`（如 `feature.rs`）還是 `gtv-array`（純 kernel）。
- **D6** 漂移監控的警告門檻（§8 的 −0.15 / 連續 2 次）與 exit code 8 的 cron 約定。

## 12. 訊號紙上模擬實測（2026-09-06，10 持倉 × H=7，Platt 校準）

> v2 修正：模擬改為**方向感知**（p≥0.5 做多、p<0.5 做空；先前把 DOWN 訊號當多單是錯的），
> 並分開**多/空兩側期望值**；動作規則（`holdings_sim.sh`）：
> **BUY = 今日 UP 訊號強度≥0.60 且歷史多單正期望**；**SELL = 今日 DOWN 訊號強度≥0.55 且歷史空單正期望**；其餘 HOLD。

THR=0.60、cost 0 的實測（多側 n/mean / 空側 n/mean，bp）：

| 持倉 | 今日 p_cal | 動作 | 多單 | 多mean | 空單 | 空mean |
|---|---|---|---|---|---|---|
| 中石油 00857 | 0.68 ↑ | **BUY** | 229 | +118 | 13 | −341 |
| 兗煤 03668 | 0.63 ↑ | **BUY** | 388 | +71 | 0 | – |
| 商湯 00020 | 0.36 ↓ | HOLD(別追空) | 0 | – | 483 | −69 |
| Link 00823 | 0.44 ↓ | HOLD | 0 | – | 171 | −36 |
| 周大福 01929 | 0.44 ↓ | HOLD(小樣本) | 1 | −728 | 17 | +172 |
| 中石化 00386 | 0.48 | HOLD | 56 | +31 | 0 | – |
| 藍思 06613 | 0.58 ↑ | HOLD(勿追) | 33 | −313 | 1 | −399 |
| MiniMax 00100 / 智譜 02513 | 0.61/0.72 ↑ | HOLD(未驗證) | – 短歷史 cal 窗不足 – |
| 長城 02333 | 0.55 | HOLD(無≥0.6訊號) | 0 | – | 0 | – |

解讀：**只有 00857/03668 的多頭訊號歷史上有正期望（BUY）；00857 空頭訊號大輸（勿反手空）**。
**00020 的空頭訊號也輸（AI 夾倉）→ 其高強度 DOWN 訊號不可追空**。01929 空單歷史 +172bp
（樣本 17，小）→ 會做空才留意。06613/00823 訊號側皆負期望 → 不照做。**maxDD 70–98% =
全倉逐訊號複利不可行** → 訊號只宜分批/小倉。成本(20bps)二階。

工具：`./stock_sim.sh HK.xxxxx`（THR×cost 掃描，含 n_buy/n_sell/buy_mean/sell_mean、SIZING=5 倉位）；
`./holdings_sim.sh`（→ `analytics_out/holdings_action_7d.tsv` 動作欄彙總）。

> **v3（2026-09-06）：倉位控制層已加** —— `SIZING=5` 預設：`pos=min(1, 5×(strength−0.5))`
> （strength .60→半倉、.70→全倉），全倉逐訊號問題得到緩解（如 00857 H7 maxDD 70%→60%）。
>
> **H=20 每股最優門檻掃描**（`holdings_thresh_scan.tsv`）：只有**00857 在長週期顯著**
> （H20 @THR 0.85：win 69%、+541bp/20d、Sharpe 0.56、DD 51%）；03668/00386 維持 H7 @0.60 小正期望；
> 00020 僅極高門檻(0.85)微正；其餘（00823/01929/02333/06613/00100/02513）任何 H/THR 都無正期望 → 只宜 HOLD/避開。

