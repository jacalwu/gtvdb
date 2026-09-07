# gtvdb Web UI（Streamlit）

持倉預測 + 量化研究儀表板，全部走既有 gtv 引擎（`webui/gtv_bridge.py` 以
`gtv` binary 驅動，與 `stock_*.sh` 同一模式），不需在 web 層重造引擎。

## 啟動

```bash
pip3 install --user -r webui/requirements.txt   # streamlit 已裝則可跳過
streamlit run webui/app.py
```

需要 `target/{release|debug}/gtv` 已建置（`cargo build -p gtv-cli --bin gtv`）。

## 頁面

| 功能 | 說明 |
|---|---|
| 📊 7日預測儀表板 | 讀 `analytics_out/holdings_forecast*.tsv`；「重新執行」＝呼叫 `holdings_forecast.sh`（需 Futu OpenD 登入）；顯示 買/持/賣 與 升/橫/跌 統計、P(校準) 長條圖 |
| 📝 持倉管理 | 直接編輯 `HOLDING.txt`（代碼+中文名）與 `HOLDING.cfg`（每股 HORIZON/門檻），即存即用 |
| 🔬 指定日期回歸測試 | `fwd_regress`：選股票+過去決策日 → P(升/跌)、預測區間 vs 實際區間、方向命中 |
| 📈 模擬/診斷/情境圖 | `*_sim_H*d.tsv` THR 掃描圖、`holdings_action`、週末跳空附錄、恆指/恆生科技走勢、`stock_diag.sh` 產出 calibration 圖 |
| 🩺 資料品質與健康 | `health_check` / `dq_report+dq_check` / `strategy_stats` |
| 🧪 SQL 控制台 | full mode 跑任意引擎 SQL（技術指標、align、backtest/pf、cov/pca/var…），可先 preload 一隻 bars |

## 備註
- 引擎呼叫為同步 subprocess；個別操作（如重跑每日預測、stock_diag）需數十秒至數分鐘。
- 資料源：futu 需本機 OpenD（11111）已登入；亦可改用 `SOURCE=yahoo`（見 scripts）。
- 研究/教育用途，不構成投資建議。
