# B3-6 混合負載 SLO 報告（workload 隔離）

- **日期**：2026-09-14 HKT
- **範圍**：prod_p3 B3-6 驗收 —「index build 唔會令 interactive 查詢超時」、
  「超載時有明確 admission 決策 + 可觀測」、「preemption 可即時取消低優先級查詢」。
- **環境**：`rustc 1.96.1`、Linux 6.6 WSL2 x86_64、**debug build**（未開最佳化，
  數字偏保守）。
- **可重現指令**：

  ```sh
  cargo test -p gtv-engine --test cbo_workload mixed_load -- --nocapture
  ```

---

## 1. 方法

測試直接驅動 `gtv_engine::workload::WorkloadManager`（B3-6 的 admission 控制層），
構造一個 **永久超載** 的混合負載：

| 參數 | 值 |
|---|---|
| 全域併發預算 `global_max_active` | 2 |
| 佇列上限 `max_queue` | 256 |
| 負載 class | ingestion、risk_batch、alm_batch、index_build |
| 每 class `max_concurrency` | 2（佔滿全域預算） |
| 每 class worker | 2 條（共 8 條，互相搶 2 個 slot，持續重佔） |
| interactive 探測次數 | 500 |

流程：8 條混合負載 worker 先佔滿 2 個 slot → 量度 500 次 `interactive_aml` 的
`wait_admit` 延遲（admit → 立即 release）→ 收集 p50 / p99 / max、timeout 數、
以及各 class 被 preempt 的次數。

> **量度範圍**：此為 **admission / 排程控制面** 延遲，即「一個 interactive 查詢要等
> 幾耐才獲准執行」。未包含查詢本身嘅執行時間。單 process in-memory 架構下，
> CPU / 記憶體嘅硬隔離要留待 compute-storage 分離（設計文件已列為風險）。

---

## 2. 結果

```
B3-6 mixed-load interactive admission latency (n=500):
  p50 = 1.095µs
  p99 = 8.588µs
  max = 102.035µs
  timeouts = 0
  total preempted = 4
    ingestion        preempted=1
    risk_batch       preempted=1
    alm_batch        preempted=1
    ftp_batch        preempted=0
    index_build      preempted=1
```

（多次執行數字為微秒級波動：p50 958ns–1.6µs、p99 8.6–12µs、max 0.1–0.2ms。）

| 指標 | 量測值 | SLO 門檻 | 結果 |
|---|---|---|---|
| interactive admission p99 | ~8.6 µs | < 50 ms | ✅ 餘裕 ~5800× |
| interactive admission max | ~0.1 ms | < 200 ms | ✅ |
| interactive admission timeout | 0 / 500 | 0 | ✅ |
| 低優先級 class preemption | 4（分佈喺 4 個 class） | 需要時可搶佔 | ✅ |

---

## 3. 驗收結論

- **index build 唔會令 interactive 超時**：即使 ingestion / batch / index-build 混合負載
  永久佔滿全域併發預算，interactive admission 嘅 p99 仍喺 ~8.6µs，零 timeout。高優先級
  class 會即時 **preempt** 負載中最低 priority 嘅 class（`pick_victim` 只搶嚴格較低
  priority），令 interactive 永遠唔需要排喺 build 後面。
- **混合負載**：場景同時包含 ingestion（priority 50）、risk batch（40）、ALM batch
  （30）同 index build（10），符合 B3-6 驗收列出嘅 workload family。
- **超載有明確決策 + 可觀測**：admission 有三種結果（`Admit` / `Queue` / `Reject`），
  per-class 計數（admitted / queued / rejected / preempted / completed / active）
  經 `workload_status()` SQL 同 Prometheus（`gtv_workload_*{class=...}`）暴露。
- **preemption 即時生效**：victim 嘅 `CancelToken`（B1-3）被 trip，長查詢 operator
  可協作式中止；`WorkloadManager::preempt(id)` / `preempt_class` 支援定向同整 class 取消。

---

## 4. 對應測試

| 驗收條件 | 測試 |
|---|---|
| 混合負載下 interactive P99 符合 SLO | `cbo_workload::mixed_load_slo_report`＋`workload::mixed_load_keeps_interactive_admission_responsive` |
| index build 唔令 interactive 超時（隔離） | 同上（8 條 ingestion/batch/index-build worker 永久佔滿預算） |
| 超載有明確 admission 決策 | `workload::queue_overflow_is_rejected`、`workload::concurrency_quota_queues_then_admits`、`cbo_workload::workload_admission_rejects_and_executes` |
| preemption 即時取消低優先級查詢 | `workload::interactive_preempts_low_priority_batch`、`workload::explicit_preempt_and_prometheus`、`workload::same_priority_does_not_preempt` |
| admission 可觀測 | `cbo_workload::workload_status_surface_reports_admission`、`cbo_workload::prometheus_exposes_spill_and_workload_metrics` |
