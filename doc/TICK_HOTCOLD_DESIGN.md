# gtvdb Tick 熱冷分層設計:當天本機 + 歷史分散式

> 單機引擎資料庫 gtvdb 的 tick 資料分層構想,對齊 kdb+ 的 tick 架構
> (tickerplant → RDB 當天在記憶體 / HDB 歷史在磁碟),只是把「磁碟」
> 換成「分散式儲存(物件儲存 / 遠端 root)」,使用時再載入本機。

---

## 1. 動機

tick 資料的生命週期極不均勻:

| 資料 | 存取型態 | 數量級 | 該放哪 |
|---|---|---|---|
| **當天 tick** | 即時 append、熱查詢(風控/訊號/撮合)、可容忍重建 | MB–GB | **本機記憶體**(熱層) |
| **歷史 tick** | 不常查、immutable、批次/研究/回測 | 日×sym 累積 | **分散式儲存**(冷層),用到再載 |

不該把「全部歷史」常駐本機記憶體(貴、載入慢),也不該讓「當天熱查詢」走遠端(延遲不可接受)。
因此:當天本機、歷史遠端、跨日查詢由引擎依日期拼接。

---

## 2. 分層總覽

```
                 live 串流 / 行情檔案 (LSE / Yahoo / CSV)
                          │  append
                          ▼
        ┌─────────────────────────────────────┐
        │  熱層:當天表(本機記憶體,單寫者)          │  ← 今日 SQL / asof / 算子全部命中
        │  gtv table (MemTable) + GTV_HOME catalog │     ns–µs
        └──────────────┬──────────────────────┘
            換日 rollover │ flush (immutable)
                        ▼
        ┌─────────────────────────────────────┐
        │  冷層:<root>/<date>/<table>/<sym>.parquet │  ← HdbStore 布局,root=遠端/物件儲存
        │  + per-table manifest(#16 雛形)           │
        └──────────────┬──────────────────────┘
            查詢需要 │ 首次使用:拉檔案到本機快取 → mmap(OS page cache)
                        ▼
        ┌─────────────────────────────────────┐
        │  讀路徑:date-aware provider(依日期拼接熱+冷) │  跨日 SQL 無感
        └─────────────────────────────────────┘
```

---

## 3. 寫路徑與 rollover

1. **當天**:`live`(LSE 串流)/ `loadcsv` / `bgload` 寫入本機表(沿用現有
   `register_batches` + GTV_HOME catalog 做 crash 後重建)。
2. **換日 rollover**:把當天表整批寫成 immutable 歷史 partition,寫完
   更新 manifest,再開新當天表。觸發見 §10 Q2:偵測到 UTC 日期鍵改變即自動
   flush(以現有 `hdb_flush` 背景 task 為基底),另提供手動 `rollover` 指令
   立即封存。→ M1 已整合:`hdb_flush` 背景自動換日、`rollover` 手動。
   - 現有 `hdb_flush` 背景 task 已做「定時 flush」;只差「換日時清空當天表並
     指到新日期」與「flush 到遠端 root」。
3. **當天資料易失性**:可接受數秒行情損失 → 不需 WAL;要零損失再引入
   MemSegment + WAL(issue #4)寫本機磁碟,rollover 只是把 WAL replay 完再封存。

**Rollover 原子性**:先寫冷層 partition(immutable 檔,寫完 rename 可見),
再更新 manifest,最後才清空本機當天表;任一步失敗都不會造成「半套歷史」。

---

## 4. 冷層布局與 manifest(issue #16 雛形)

**檔案布局**(沿用現有 `HdbStore::write_table` 產出):

```
<remote_root>/<date:YYYY.MM.DD>/<table>/
    <sym>.parquet          # 有 symbol 欄位時依 sym 拆檔
    __all.parquet          # 無 symbol 欄位時單檔(或 * 符號)
```

**manifest**(每 table 一個,TSV,和 catalog.tsv/tc_duration 同風格,免新
dependency)。**定位:derived 的索引/驗證物,不是 layout 的唯一事實來源**
(目錄樹才是,見 §10 Q3):

```
# gtv history manifest v1  table=trade  schema_sha=…  sorted_by=t
# date        sym   path                                  rows      bytes    sha256      created
2024.01.01    AAPL  2024.01.01/trade/AAPL.parquet         2_500_000 1.2e8    1a2b3c…    2024-01-02T00:00:00Z
2024.01.01    MSFT  2024.01.01/trade/MSFT.parquet         …
```

欄位意義:
- `schema_sha` — 歷史檔 schema 指紋;熱層換日時驗證與當天 schema 一致(asof 邊界才不會斷)。
- `sorted_by` — 宣告每檔按 `t` 遞增,跨熱冷 merge 可直接 two-pointer(現有 asof/aj kernel 前提)。
- `sha256` — 冷層檔不可變驗證(選配,拉檔後校驗)。

---

## 5. 讀路徑:依日期拼接熱 + 冷

**單機 M1(建議先做)**:date-aware loader 取代「手動 hdb_scan 全載」。
查詢某 range 時:
1. `[today …]` → 命中本機當天表(若有,同 table 名)。
2. `< today` → 依 manifest 找出所需 date×sym 切片 → 首次 `read_parquet_mmap`
   載入,之後命中 OS page cache。
3. 兩段皆按 `t` 排序 → 直接 concat(邊界無重疊),註冊成 session table。

**分散 M2+(issue #14/#15)**:coordinator 把跨日查詢切成 fragment
- 本機節點算當天;
- 各 partition 節點算各自歷史 partition(每個節點 = 今天的一台「小 HDB」);
- reduce 端 merge。
read routing(#15):當天=leader 強一致;歷史 immutable → follower/快取隨便讀。

**查詢下推**:先在 provider 層做 date/sym predicate pruning(只拉被 WHERE
命中的 partition);列/計數下推留待 DataFusion provider 化後做。

---

## 6. 載入 = mmap,不是整包進記憶體

- 歷史 immutable → 用現有 `read_parquet_mmap`(`gtv-storage::hdb`)零複製映射,
  由 OS page cache 當快取;記憶體壓力時 page 自動被回收,不必自做 LRU。
- 快取目錄沿用 **GTV_DATA_DIR read-through pattern**(Yahoo/LSE fetch 已實作):
  遠端檔首次使用 → pull 到本機快取 → 之後直接 mmap 本機檔。
- 「整段歷史分析」(回測/協方差)才需要暖身預載相關 date×sym 切片,用
  `SELECT … WHERE date IN (…)` 觸發即可,不要無差別全載。

---

## 7. 一致性與定義

| 項目 | 決定 |
|---|---|
| immutable | 歷史 partition 寫後不改;修正 = 新檔 + manifest 覆寫 |
| atomic 可見 | 檔案先寫 tmp → rename;manifest 先寫 tmp → rename |
| 換日基準 | 預設 UTC date key;可依 exchange calendar 覆寫(「當天」=交易日) |
| 跨日 asof | 熱層尾 + 冷層頭都按 `t` 排序,merge 用現有 aj/merge kernel |
| schema 一致 | rollover 與載入前都驗 `schema_sha`,不一致即報錯拒載 |

---

## 8. 對應現有積木(都已存在,只差組裝)

| 現有程式 | 用途 |
|---|---|
| `HdbStore::write_table / scan / read_partition` | 冷層寫讀(root 可指遠端/本機目錄) |
| `read_parquet_mmap`(gtv-storage) | 歷史零複製載入 |
| `hdb_flush` 背景 task | 定時/換日 flush(改 root 即可遠端) |
| GTV_DATA_DIR read-through cache | 遠端 → 本機快取 → mmap |
| GTV_HOME catalog(本 session 新做) | 當天表 crash 後重建 + rollover 前的臨時快照 |
| DataFusion `register_batches` + UDTF | 熱層當天表即查即用 |

---

## 9. 落地順序(對 GitHub issues)

| 里程碑 | 內容 | 關聯 issue | 新寫量 |
|---|---|---|---|
| M0 | 已具備:本機 hdb_save/scan/flush + mmap + cache | — | 0 |
| **M1** | ✅ 單機 hot+cold:`rollover`、`hc_load`(冷 range + 當天 hot union,`--sym/--root`,ORDER BY t);`hdb_flush` 整合 UTC 日期鍵改變自動 rollover + 同日期成長才 checkpoint | — | 已實作(gtv-cli) |
| M2 | per-table manifest v1 + schema_sha 驗證 | #16 | 小 |
| M3 | 遠端 root 真正掛 S3/MinIO;mmap 拉檔快取 | #7 | 中 |
| M4 | 分散:coordinator 跨日 map/reduce、各節點當天/歷史分工 | #14/#15 | 大 |
| M5 | MemSegment + WAL 取代「當天記憶體表」的易失性 | #4 | 大 |

---

## 10. 開放問題 → 決策記錄(已定案)

**Q1. 當天表是否「多來源共表」?(如 MCO+NVDA+TSLA 併成一張當天表)**

**決定:不共用,每個來源/每個 table name 各自一張當天表**;熱冷分層是
per-table 的。理由:

- SQL/算子/HFT registry(`tick_to_trade('mco')` 依名字查表)都建立在「表名」上,
  併表會破壞名字→資料的對應;
- 多 symbol 來源本來就自帶 `sym` 欄位,冷層也依 sym 拆檔,不需要跨表合併;
- 若要跨來源一起看,用查詢期 view(SELECT ... UNION)即可,那是讀端的事,
  不是儲存層該決定的。

**Q2. rollover 觸發時機(UTC 0 點 / exchange 收盤 / 手動)?**

**決定:日期鍵改變自動 + 手動兜底,預設 UTC 日期鍵。**

- 熱層每個 table 記一個 `date_key`(= partition 日期);當「下一次寫入或定時
  tick」偵測到本地 UTC 日期 ≠ `date_key` 時,先把舊日期當天表 flush 成
  immutable partition 再繼續寫新日期(先寫後寫皆可,寫完再開新表);
- 提供手動 `rollover <table>`(或 `rollover all`)立即封存,供測試/收盤後立刻
  落盤;`hdb_flush` 背景定時 flush 保留為「未換日也定期落盤」的保險;
- exchange 收盤行事曆**不做進 M1**:日期鍵預設 UTC,可被 `GTV_TZ` 環境變數
  覆寫;M4 分散化後再引入交易日曆。

**Q3. 遠端 root 用現有 HdbStore layout,還是 manifest 反過來當唯一事實來源?**

**決定:layout(目錄樹)是唯一事實來源;manifest 是 derived(可重建的索引/驗證物),不是**

- M1/M2 沿用 `HdbStore` layout:`<root>/<date>/<table>/<sym>.parquet`,kdb 風格
  ——「目錄本身即 catalog」,掃目錄就能列出有哪些 partition,不需 metadata 先存在;
- manifest v1(#16)定位為**派生的快取/驗證層**:schema_sha(換日與載入前驗證)、
  sorted_by、rows/bytes/sha256(prune 與拉檔校驗),可隨時由掃描目錄重建;
  manifest 與目錄不一致時以目錄為準並重建 manifest,而不是拒絕服務;
- 理由:把 metadata 當 source of truth 會產生第二份狀態、有 drift 風險,
  而 kdb 幾十年的 HDB 也證明「無 manifest、純目錄」是可靠的。

**仍延後(不阻塞 M1)**:exchange 交易日曆;跨來源 view 語法糖;遠端真正掛上
S3/MinIO(M3);manifest 成為協調器專用 metadata service 的輸入(#16 主體,
屆時由服務維護一份真正 authoritative 的 metadata)。

