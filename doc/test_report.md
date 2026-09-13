# gtvdb 整體測試報告

- **日期**：2026-09-13 HKT
- **代碼版本**：`677446f`（B3-1 streaming）—— 於 `f376987`（golden 測試修正）之上
- **環境**：`rustc 1.96.1 (31fca3adb 2026-06-26)`、`cargo 1.96.1`、`Linux 6.6.87.2-microsoft-standard-WSL2 x86_64`
- **執行方式**：單機 debug build（`target/debug/gtv`）

---

## 1. 執行摘要

| 測試套件 | 範圍 | 結果 |
|---|---|---|
| `cargo test --workspace` | 14 個 crate、43 個 test target（含 doc-test） | **332 passed / 0 failed / 0 ignored** ✅ |
| REPL golden（`testcase/run_tests.sh`） | TC-01…TC-11、signal | **10 passed / 0 failed / 1 skipped** ✅ |
| Day-rollover CI（`testcase/test_rollover.sh`） | hot→cold 自動換日 | **PASS**（5/5 checks） ✅ |
| **合計** | | **342 個斷言測試，0 失敗** |

> 對比上一輪（B3-2，307 tests）：新增 `gtv-ingest` **25** 個測試（17 unit + 8 integration）。
> 早前 golden 套件因 CLI preamble 漂移 + 一個舊 golden 未更新而報 9 FAIL，已於 `f376987` 修正。

---

## 2. `cargo test --workspace` 明細

| Crate | lib / bin | integration | 小計 |
|---|---:|---:|---:|
| gtv-array | 52 | – | 52 |
| gtv-catalog | 11 | 18 | 29 |
| gtv-cli | 3 | 1 | 4 |
| gtv-core | 39 | – | 39 |
| gtv-delta | 7 | – | 7 |
| gtv-engine | 63 | 26 | 89 |
| gtv-index | 41 | – | 41 |
| gtv-index-store | 3 | 14 | 17 |
| **gtv-ingest** | **17** | **8** | **25** |
| gtv-pattern | 6 | – | 6 |
| gtv-proto | 0 | – | 0 |
| gtv-server | 0 | 6 | 6 |
| gtv-storage | 13 | – | 13 |
| gtv-udf | 4 | – | 4 |
| **合計** | **242** | **90** | **332** |

Integration 分佈：

- `gtv-catalog`：`catalog.rs` 9、`crash.rs` 2、`dq.rs` 1、`embedding.rs` 6
- `gtv-index-store`：`embedding.rs` 6、`store.rs` 8
- `gtv-engine`：`ann_filter.rs` 6、`bitemporal.rs` 5、`dq.rs` 5、`embedding.rs` 5、`lineage.rs` 5
- `gtv-ingest`：`streaming.rs` 8
- `gtv-cli`：`prod_p2_e2e.rs` 1
- `gtv-server`：`p5_distributed.rs` 2、`smoke.rs` 4

全部 target 皆回報 `test result: ok`，無 `failed` / `ignored`。

---

## 3. prod_p3 近期功能覆蓋

| 任務 | 針對性測試 | 數量 |
|---|---|---:|
| B3-1 Streaming 攝取 | `gtv-ingest` unit（envelope / offsets / dedup / watermark / DLQ / file adapter）＋ `streaming.rs`（restart、冪等重播、原子可見性、DLQ 審計、backpressure、metrics） | 25 |
| B3-2 Bitemporal | `gtv-core::bitemporal::*`＋`gtv-storage::bitemporal::*`＋`gtv-engine/tests/bitemporal.rs`＋`gtv-catalog` `snapshot_as_of` | 16 |
| B3-3 Filter-aware ANN + rerank | `gtv-index::ann::tests::*`＋`gtv-engine/tests/ann_filter.rs` | 12 |
| B3-4 IVF k-means 量化器 | `gtv-index::ivf::tests::*` | 13 |

B3-1 驗收對應：

| 驗收條件 | 測試 |
|---|---|
| 重啟由 committed offset 續，無重複無漏 | `publish_then_restart_resumes_from_committed_offset` |
| 重播冪等（event_id dedup） | `replay_from_zero_is_idempotent_via_dedup` |
| 未完整發布 batch 不可見 | `failed_publish_is_invisible_and_retried` |
| lag / dropped 可量度 | `metrics_report_lag_and_progress` |
| late event 可處理 + 可審計 | `late_events_go_to_the_dlq_and_are_auditable`、`late_recompute_publishes_the_event` |
| backpressure 生效 | `backpressure_stops_polling_when_inflight_is_full` |

## 4. REPL golden 測試

`GTV_BIN=target/debug/gtv bash testcase/run_tests.sh`

| Test | 內容 | 結果 |
|---|---|---|
| `tc01_asof` | as-of join | PASS |
| `tc02_rolling` | mavg / msum / deltas | PASS |
| `tc03_temporal_slice` | 半開區間時間切片 | PASS |
| `tc04_graph_traversal` | neighbors / khop | PASS |
| `tc05_pattern` | 時序 pattern（ring / path / diamond） | PASS |
| `tc06_knn` | 向量 K-NN + bitmask filter | PASS |
| `tc07_tss` | temporal similarity search | PASS |
| `tc08_tt_save_load` | Parquet 持久化 + time-travel | PASS |
| `tc11_songs` | metadata-filtered K-NN | PASS |
| `tc_signal` | HFT signal SQL | PASS |
| `tc_hft_sql` | HFT SQL surface | SKIP（無 golden） |

## 5. Day-rollover CI

`GTV_PROFILE=debug bash testcase/test_rollover.sh`

```
ok: day-1 checkpoint partition exists (2024.01.31, 2 symbols)
ok: day-2 checkpoint partition exists (2024.02.01, 2 symbols)
ok: hdb_flush logged an automatic rollover on the date change
ok: cold range view returns 4 rows (2 per day)
ok: GTV_TODAY=2025.06.30 pins rollover's default date
PASS: automatic day rollover (fake-date hook)
```

---

## 6. 測試基礎設施修正（`f376987`）

1. **`testcase/run_tests.sh` preamble 剝除**：舊 harness 用 `tail -n +2` 假設第一行一定係 shell banner；CLI 其後喺 banner 之前多印一行 `catalog: …`，令 banner 洩漏入所有 diff。改為明確 `sed` 過濾 banner 同 `catalog: ` 行。
2. **`tc04` golden 補上 `(hops=… edges=… rows=… peak_frontier=…)`**：prod_p1 B1-3 加入嘅 khop 預算診斷輸出，屬預期行為。

修正後 golden 由 9 FAIL → 10 PASS。

---

## 7. 編譯警告

`cargo test` 仍餘 **2 個既有警告**（與 prod_p3 新代碼無關）：

| 位置 | 警告 |
|---|---|
| `crates/gtv-storage/src/cache.rs:112` | `unused import: StringArray` |
| `crates/gtv-engine/src/analytics.rs:1301` | `unnecessary parentheses around closure body` |

新增嘅 `gtv-ingest` 全 crate **零警告**。

---

## 8. 未執行 / 範圍外

| 項目 | 原因 |
|---|---|
| `testcase/run_tc_duration.sh` / `run_tc_duration1.sh` | 效能計時 harness（1M rows × `ITERS` 次），非 pass/fail；需要 release build 同長時間跑 |
| `tc_hft_sql` golden | 專案本身未提供 expected 檔，harness 按設計 SKIP |
| Kafka / Pulsar 實來源 | `gtv-ingest` 已預留 `kafka` / `pulsar` feature seam；預設 build 唔引入 rdkafka（編譯時間考量），以 `FileReplayAdapter` + `decode_json_envelope` 覆蓋測試與重演 |

---

## 9. 重現指令

```sh
# Rust workspace 全量測試
cargo test --workspace --no-fail-fast

# 建 REPL 二進位（golden / rollover 需要）
cargo build -p gtv-cli --bin gtv

# REPL golden
GTV_BIN="$PWD/target/debug/gtv" bash testcase/run_tests.sh

# Day-rollover CI
GTV_PROFILE=debug bash testcase/test_rollover.sh
```
