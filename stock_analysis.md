# gtvdb 股票未來 2 週走勢分析(上漲/下跌機率 + 提醒)

> 使用 gtvdb 的資料整備、日線化、特徵/標籤與分析算子,對「指定股票代碼」的
> 歷史行情算出「未來 2 週」上漲/下跌的機率;機率 ≥ 70% 即提醒使用者。
> 設計成可重複執行的 shell script,供每日/定期使用。

---

## 1. 目標與定義

- 輸入:股票代碼(如 `AAPL`、`2513.HK`),歷史行情(日線或 tick)。
- 輸出:
  - 決策日(最後一根日 K)的 `p_up`、`p_down`,與模型摘要(hit-rate/樣本數);
  - 當 `max(p_up, p_down) ≥ THRESHOLD(預設 0.70)` 時觸發提醒(上漲/下跌方向)。
- 名詞定義:
  - **「未來 2 週」= 未來 10 個交易日**(日線只存在交易日,天然跳過週末)。
    日曆 14 天 vs 交易日 10 天,以交易日為準,避免假日誤差。
  - **漲** = `close[t+H] > close[t]`;跌 = `close[t+H] < close[t]`;平盤計入「非漲」。

---

## 2. 資料源與整備

| 來源 | 命令/表 | 需要 key | 欄位(節錄) | 用途 |
|---|---|---|---|---|
| Yahoo 日線 | `yahoo <table> SYM --range 5y` | 否 | symbol, ts(Int64 ns), open/high/low/close, adjclose, volume | 預設來源,無 key、日線直接可用 |
| LSE 歷史 tick | `fetch <table> SYM [--limit N]` | `LSE_API_KEY` | symbol, ts, ts_us(Int64 µs), price, bid/ask, volume | tick 級;需先日線化 |
| 既有樣本 | `testcase/data/stocks_{MCO,NVDA,TSLA}_tick.parquet` | 否 | 合成 tick(price…) | 離線開發/測試 |
| Futu OpenD 快照 | futuapi skill: `get_snapshot.py US.AAPL` | OpenD 登入+行情權限 | 最新價、開高低收、量、買賣盤 | 盤中即時/點位確認 |
| Futu OpenD 歷史 K 線 | futuapi skill: `get_kline.py US.AAPL --start …` | OpenD 登入(日K免額度?) | time_key, open/high/low/close, volume | 真實歷史日線(美股/港股/…最多20年) |


整備原則:
- 統一註冊為「tick 表」或「日線表」;`yahoo` 直接給日線(有 open/high/low/close),
  可直接建 bar 表;tick 來源需先日線化(§3)。
- 拉到後立即 `hdb_save <table> <date> …` 或寫入冷層,之後重跑用
  `hc_load`/`hdb_scan` 載入,避免每日重抓(見 §8 銜接)。

### 2.1 Futu/OpenD 前置條件與橋接

- **OpenD 需由使用者在有 GUI 的機器登入**(牛牛號 + 首次問卷/協議;futuapi skill
  明訂禁以 SDK `unlock_trade` 解鎖、一律用 GUI 手動);登入後監聽
  `127.0.0.1:11111`。headless server 無法完成登入 → 由使用者桌面端執行。
- 行情腳本已由 `futuapi` skill 提供(agent 全域 skills),例:
  - 快照:`python get_snapshot.py US.AAPL HK.00700` → JSON(最新價/開高低收/量/買賣盤)
  - 歷史日 K:`python get_kline.py US.AAPL --ktype K_DAY --start 2024-01-01 --end …` → JSON(K 線)
  - 代碼格式:`US.AAPL` / `HK.00700`(美股/港股前綴)。
- 橋接進 gtvdb:腳本輸出 JSON → 轉 CSV(ts, open, high, low, close, volume)→
  `loadcsv`/`load` 註冊;欄位對齊 §3 bar 表(Yahoo 同構)。
- `SOURCE=futu` 時分析流程與 yahoo 相同,只差「拉資料」這一段換成 OpenD。

---

## 3. 日線 bar 表(統一 schema)

來源是 tick(price/ts)時日線化;Yahoo 日線則僅欄位對齊。

統一 bar 表 schema(`<SYM>_bars`):

```
date(Int64 ns)  open  high  low  close  volume
```

SQL 範例(tick → 日 bar,資料需按 ts 升冪):
- 直接聚合:`SELECT trunc(ts,'DD') d, first(price) open, max(price) high,
  min(price) low, last(price) close, sum(volume) vol … GROUP BY d`
  (`first/last` 依 ORDER BY 語意,實作時以 `ohlc(name, bucket)` 表函數或
  DataFusion first_value/last_value 窗函數擇一)。
- Yahoo:直接 `SELECT ts date, open, high, low, close, volume … ORDER BY ts`。

> 實作注意:gtv `ohlc(name, bucket)` 需要 Int64 ts 欄(tick 表已具 `ts_us`/`t`);
> bucket 單位與日界(UTC)在 M1 決定;Yahoo 的 ts 已是日開盤 UTC。

---

## 4. 特徵與標籤表

對 `<SYM>_bars` 依交易日 `i` 計算(DataFusion window/既有算子可覆蓋大部分):

| 欄位 | 定義 | 用到的 gtvdb 能力 |
|---|---|---|
| `ret_1` | `close[i]/close[i-1]-1` | lag 窗函數 / deltas |
| `mom_5` / `mom_10` / `mom_20` | `close[i]/close[i-H]-1` | lag 窗函數 |
| `vol_10` | 近 10 日 `ret_1` 標準差 | stddev 窗函數(或新增小 UDF) |
| `above_sma20` | `close[i] / avg(close,20) - 1` | mavg / avg 窗 |
| `dist_hi_lo_10` | `(close[i]-min)/ (max-min)`(近10日高低位置) | min/max 窗 |
| **標籤** `fwd_ret_10` | `close[i+10]/close[i]-1`(需往後 10 根才可算) | lead/lag(向後)窗 |

- 只有「最後 10 根以前」的列有標籤(做驗證/校準);
- 決策日 = 最後一列(只有特徵、無標籤)。

---

## 5. 機率模型

**MVP(可解釋、免訓練框架):歷史類比(kNN)頻率**
- 對決策日特徵向量 `x`(正規化 mom_5/mom_10/vol_10/above_sma20/…),
  在歷史中找特徵最接近的 `K(預設 20)` 個有標籤日;
- `p_up = (#類比中 fwd_ret_10>0) / K`,`p_down = (#<0)/K`。
- 優點:只用 gtvdb 的查表/排序能力、結果可解釋、隨歷史增長自動更新。
- 建議以 gtv-engine 新增 table fn 實作:
  `fwd_proba('<bars表名>', horizon=10, k=20, '<feature_csv>')`
  回傳 `p_up, p_down, n_analogs, hit_rate(全歷史留一驗證)`。
  (DataFusion 純 SQL 做 kNN 較勉強 → 新 table fn 走 registry-by-name,
  與現有 `knn/ohlc/var_historical` 同模式。)

**後續(Phase B)**: ✅ 已实现 walk-forward 校准与 threshold 建议
(`fwd_walk` 严格 past-only 回放 + `stock_calib.sh` 分桶 p vs 實際漲率、
方向命中率×門檻表,自动建议阈值;HK.00700 H=10 实测建议 ≥0.75 → 68% 命中/
281 信号)。可選:logistic / 加權投票、多股聚合。

---

## 6. 決策與提醒

```
規則: max(p_up, p_down) >= THRESHOLD(0.70)
      → 提醒「SYM 未來 10 交易日 ↑ 機率 73%(n=20 analogs, 歷史hit 71%)」
      否則 → 「無明顯訊號(p_up=…, p_down=…)」
```
輸出:
- 機器可讀:`<SYM>_analysis.tsv/csv`(date, close, p_up, p_down, decision);
- 人類可讀 stdout;提醒掛鉤:通知列在 script 設定檔,`ALERT=telegram|mail|none`。

---

## 7. Shell script 設計(`stock_analysis.sh`)

```
用法:
  ./stock_analysis.sh AAPL                     # 全預設(Yahoo 5y, 門檻 0.7)
  SYMBOL=TSLA ./stock_analysis.sh
  SOURCE=yahoo RANGE=2y THRESHOLD=0.75 ./stock_analysis.sh TSLA
  DATA_DIR=/data ./stock_analysis.sh 2513.HK
```

環境變數(預設):

| 變數 | 預設 | 意義 |
|---|---|---|
| `SYMBOL` | (必填/參數) | 股票代碼 |
| `SOURCE` | `futu` | `futu`(OpenD,預設) / `yahoo`(免key) / `parquet`(已存 bars 檔,免重抓) |
| `RANGE` | `5y` | Yahoo 拉取長度 |
| `HORIZON` | `10` | 未來交易日(≈2 週) |
| `K` | `20` | 類比數 |
| `THRESHOLD` | `0.70` | 提醒門檻 |
| `DATA_ROOT` | `./analytics_data` | 冷層/快取 root |
| `OUT_DIR` | `./analytics_out` | 輸出目錄 |
| `ALERT` | `none` | `none`/`telegram`(webhook env)/`mail` |
| `GTV_PROFILE` | `release` | gtv binary profile |

流程(每階段 = 一次 `printf … | gtv` 呼叫,錯誤即停):
1. **拉資料**:`yahoo $SYM $DATA_ROOT/…` 或 `fetch`,註冊 `<SYM>_tick`;
2. **日線化**:產生/註冊 `<SYM>_bars`(§3);
3. **特徵**:產生特徵表(§4);
4. **機率**:`SELECT * FROM fwd_proba('<SYM>_bars', $HORIZON, $K, …)`;
5. **決策+提醒**(§6);
6. **存檔**:bar/特徵/結果寫入 `$OUT_DIR`,並 `hdb_save` 至 `$DATA_ROOT`
   (下次改由冷層載入 → 免重抓、可回放)。
退出碼:0 正常;1 資料/參數錯誤;2 無訊號;3 觸發提醒(方便 cron 判斷)。

---

## 8. 與 gtvdb 既有能力的銜接

- **每日增量**:yahoo 拉到當天 hot 表 → 換日用既有 `hdb_flush`(auto-rollover)
  封存到 `$DATA_ROOT`;分析時用 `hc_load`/`hdb_scan` 拉 `[start, 今天)` 冷層,
  不用每次全量重抓(與近期 hot/cold M1 直接串接)。
- 重現性:每次執行記 `git sha + 資料 root sha`(比照 run_tc_duration 的 header)。
- SQL/bar/特徵階段全部留在 REPL/DataFusion,script 只做參數、排序與提醒。

---

## 9. 驗收

1. **合成測試**:構造「已知單邊趨勢」的 tick/日線(前段多頭、後段空頭),
   驗證 `p_up/p_down` 方向正確、`>0.7` 時 script exit=3 且訊息含方向。
2. **真實資料 smoke**(無 key):`yahoo` 拉 `2513.HK`/`0100.HK`,全流程可跑,
   輸出含 p/決策與 hit_rate 欄位。
3. **hit-rate 報告**:全歷史 leave-one-out 的 `p≥0.7` 桶實際漲率 ≥ 0.65(容差),
   否則調門檻/特徵(Phase B)。
4. 重跑冪等:同一天重跑結果一致(資料有 cache),無重複抓取。

---

## 10. 限制與免責

- 歷史頻率 ≠ 未來保證;類比模型在 regime change(政策/財報/宏觀)時會失效;
- Yahoo 日線僅 daily;若需盤中/前後盤請改用 LSE tick 源;
- 此工具為研究/教育用途,不構成投資建議。

---

## 11. 里程碑

| 里程碑 | 內容 | 狀態 |
|---|---|---|
| **M1** | `yahoo/parquet` 拉資料 → bar 表 → SQL 特徵 → `fwd_proba` table fn(MVP kNN)→ script v1(決策+exit code) | ✅ 已实现(`stock_analysis.sh`,SOURCE=futu 預設) |
| **M2** | leave-one-out hit-rate/calibration 報告 + threshold 自動調整 + 特徵擴充 | ✅ 已实现 walk-forward 校准(`fwd_walk` + `stock_calib.sh`);logistic/加權可選 |
| **M3** | 每日排程(cron)+ 提醒(telegram/mail)+ hdb 增量累積(§8) | 待做 |

---

## 附錄 A:待新增/確認的 gtvdb 元件

| 元件 | 類型 | 狀態 |
|---|---|---|
| `fwd_proba(name, horizon, k, feats)` | engine table fn(registry-by-name) | ✅ 已实现(`analytics.rs`;feats 省略→自动特征 mom5/mom10/vol10/above_sma20) |
| 日線化 `first/last` 聚合或 `ohlc(name, bucket)` | SQL/既有 | 確認語意後選用(日線源直接可用) |
| `stddev` 窗 / lead 窗 | DataFusion 內建 | 可用(內置自動特徵已覆蓋) |
| 冷層每日累積(`hdb_flush` + `hc_load`) | 既有(M1 已完成) | 直接用 |
| 統一 provider 拉數(`klines('futu',…)` / REPL `md klines`) | market 框架 | ✅ 已实现(`market/` + `main.rs` `md` 命令) |
| `stock_analysis.sh`(SOURCE=futu 預設) | shell | ✅ 已实现(見 §7;退出碼 0/1/3) |

附錄 B:範例輸出(目標格式)

```csv
symbol,date,close,p_up,p_down,decision,hit_rate,n_analogs
AAPL,2026-09-05,231.20,0.73,0.27,UP_ALERT,0.71,20
```
