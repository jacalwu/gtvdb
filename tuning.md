# Role & Task
你是一位頂尖的 Rust 系統效能專家與 Arrow/DataFusion 架構師。請針對 `gtvdb-core` 中的時序邊過濾 (Temporal Edge Filtering) 模組進行深入的效能分析與代碼重構，目標是在百萬級至千萬級圖數據環境下發揮極致吞吐量。

---

## 1. 專案背景與現狀 (Context)

`gtvdb-core` 使用 Apache Arrow 記憶體格式儲存動態圖邊資料 (`TemporalEdgeChunk`)。當前實作已使用 Rayon 進行 Chunk 切片平行計算，並透過 `arrow::compute` 進行 SIMD 比較。

### 當前關鍵代碼實作：
- **結構**：`TemporalEdgeChunk` 包含 `src_nodes`, `dst_nodes`, `valid_from`, `valid_to` (均為 Arrow Array)。
- **平行機制**：超過 100,000 筆資料時，按 `128,000` 筆一個 Chunk 切片，用 Rayon 平行計算 `lt_eq_scalar` 與 `gt_scalar`，最後執行 `arrow::compute::concat` 拼接。

---

## 2. 發現的瓶頸與優化目標 (Bottlenecks & Goals)

1. **記憶體配置與 Concat 開銷**：
   - 目前在各 Rayon 任務中分別生成子 `BooleanArray`，最後調用 `arrow::compute::concat` 進行大陣列拼接，帶來額外的記憶體 Alloc 與數據 Copy 開銷。
   - **目標**：實現零分配 (Zero-allocation) 或單次配置的預先配置 Buffer（如 `BooleanBufferBuilder`），讓各線程直接平行為未初始化記憶體區塊寫入位元遮罩 (Bitmask)。

2. **快取友好度與 Chunk 大小調優**：
   - 現有固定 128,000 筆 Chunk 大小未完全與 CPU L1d/L2 Cache Line 對齊。
   - **目標**：設計自適應 Chunk 分割機制，提高 CPU 指令與數據快取命中率。

3. **SIMD 運算融合 (Kernel Fusing)**：
   - 當前分兩步執行 `lt_eq` 與 `gt` 再做 `and`，產生中間 BooleanArray 暫存。
   - **目標**：實現或調用融合兩端點比較的單趟 (Single-pass) 向量化比對。

---

## 3. 重構要求 (Refactoring Requirements)

請對 `gtvdb-core/src/lib.rs` 進行優化，滿足以下規範：

### A. 效能優化 (Performance)
- **Direct Bit-builder 寫入**：探索直接操作 `arrow::buffer::MutableBuffer` 或 `BooleanBufferBuilder`，避免產生中間 `BooleanArray` 與最後的 `concat`。
- **無鎖/零拷貝拼接**：利用 Rayon 搭配 unsafe/raw buffer 切片（在保證 Rust 記憶體安全的前提下），實現平行寫入同一塊底層 `Buffer`。

### B. 安全與架構 (Safety & Engineering)
- 維持標籤 `pub fn create_temporal_mask_parallel(&self, valid_at: i64) -> Result<BooleanArray>` 的 API 相容性。
- 若使用 `unsafe` 區塊優化 Buffer 寫入，必須附帶詳細的 `// SAFETY:` 註解說明安全邊界條件。
- 完整保留並適配單線程退回閾值（Fallback logic）。

---

## 4. 期望產出 (Expected Deliverables)

1. **重構後的完整 Rust 代碼**：包含完整的依賴引用、註解與錯誤處理。
2. **優化原理與代碼比對說明**：說明本次重構如何減少記憶體配置（Allocation）與快取失誤（Cache Miss）。
3. **理論效能提升預估**：分析在百萬級數據下的記憶體開銷與 CPU 週期差異。