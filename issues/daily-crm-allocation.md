# 每日 Credit Risk Mitigation 分配 pipeline（基於 gtvdb temporal-graph）

## 問題描述

- 目標：實作一個每日執行的 Credit Risk Mitigation (CRM) 分配 pipeline，將抵押品/擔保分配到曝險以計算淨曝險/資本，並支援每日變動的法律圖關係與抵押估值。
- 平台：基於 jacalwu/gtvdb（利用 temporal graph、as-of time-slice 與 Arrow 列式執行）。

## 要做的事（高階任務）

1. Snapshot inputs（每日/AS_OF）
   - exposures, collaterals (含 valuation_ts), edges (legal relations), risk_parameters
   - 將 snapshot 寫成 parquet 並以 run_id/AS_OF 標記
2. Netting-set 計算（graph）
   - 用 temporal edges（valid_from/valid_to 篩選）計算每日的 netting_set mapping（entity → pool_id / exposure_id → pool_id）
   - 支援全量與增量重算策略（先實作全量，未來做增量）
3. Pool 聚合與需求計算
   - 每 pool 計算 pool_net = SUM(market_value * (1 - haircut))
   - 每 exposure 計算 need（EAD 或 LEAST(EAD, outstanding, contractual)）
4. Allocation engine
   - 簡易：比例分配（SQL）
   - 進階：優先級/greedy 分配（WASM UDF 或 Python job）
5. 驗證與寫入結果
   - 審核規則：SUM(allocated) <= pool_net；allocated ≤ exposure.need
   - 把 allocation_results 寫入 append-only table（含 run_id、method、params）
6. 報表與審計
   - 產出每日報表與差異比較（可重算性：保留 inputs snapshot 與 params）

## Acceptance criteria（驗收條件）

- 每日 batch 可以在指定 AS_OF 時點完成 snapshot、netting_set 計算與比例分配（預估需在 N 小時完成，需由 stakeholder 提供規模與 SLA）。
- allocation_results 包含 run_id、as_of、method 且可用於重算比對。
- 驗證機制能檢查供需一致性 (no over-allocation)。
- 能以 graph 找出 exposure 的 reachable collateral pool（time-sliced）。

## Deliverables（交付物）

- SQL 腳本：snapshot、daily_netting_sets 計算、比例分配並寫入 allocation_results
- Python 範例（可選）：greedy allocation，含示例 dataset 與單元測試
- Run 與 audit 方案文件：包含 run_id 範例、parquet 路徑與重算指引

## 開發任務拆解（可直接勾選）

- [ ] 建立 allocation_runs 與 allocation_results 表 schema（DDL）
- [ ] 建立 snapshot job（寫入 parquet）
- [ ] 實作 daily_netting_sets（SQL 或 Python union‑find）
- [ ] 實作比例分配 SQL pipeline
- [ ] 實作 greedy allocation（Python 或 WASM UDF）
- [ ] 建立驗證/報表（sum checks, diff）
- [ ] 撰寫測試資料與範例（3 個 pool, 多個 edge 變動情境）
- [ ] 文件：操作與重算說明

## 問題 / 需確認

1. 請確認每日 batch 的 SLA（例如每日需在 N 小時內完成）與資料規模（exposures / collaterals / edges 大約筆數級別），以便調整設計（全量 vs 增量、並行度）
2. 是否接受先以「比例分配 SQL（每日批次）」作為快速上線方案，之後再補 greedy/WASM 實作？

---

> 此檔案由 GitHub Copilot Chat Assistant 代為建立。如需修改內容或改放其他路徑（例如 `.github/ISSUE_TEMPLATE/`），請回覆指示。
