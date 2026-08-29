機器有内存帶寬限制盡量 0 copy 減少内存訪問的消耗。
零拷貝與 DRAM 流量極小化五大指令
算子融合 (Operator Fusion / Pipeline Fusion)

原則：嚴禁將中間結果寫回 DRAM。

作法：將 TC1 (As-Of Join) 與 TC2 (OFI) 或風控邏輯熔接到單一 Pass 迴圈內。TC1 產出的命中結果直接保留在 CPU 暫存器（Registers）與 L1 Cache 供 TC2 消耗，完全消除中間 16 MB 陣列的 DRAM 讀寫流量。

索引與 Payload 強制解耦 (Index-Only Processing)

原則：搜尋與過濾階段只處理 32 位元索引，不碰任何 64 位元浮點數據。

作法：雙指針 Sweep 或二分搜階段僅產生 u32 / i32 索引陣列，將記憶體讀取寬度從 24 bytes（TS + Price + Spread）直接壓縮至 4 bytes，記憶體 Bus 負載瞬間降低 83%。

旁路直寫消除 RFO (Non-Temporal Store)

原則：消滅 CPU 寫入主存時的 Read-For-Ownership (RFO) 隱形讀取開銷。

作法：最終寫入結果時採用 x86 _mm_stream_pd / _mm_stream_si128 指令，強制數據繞過 L1/L2/L3 直接寫入 Write-Combining Buffer 並推入 DRAM，寫入頻寬消耗直接砍半。

L1d/L2 快取切塊 (Cache-Tiling / Working Set Limitation)

原則：將單次處理數據量限制在 L1 Data Cache (通常為 32 KB) 內。

作法：設定 chunk_size = 4096 個元素（約 32 KB）。讓內部迴圈的所有熱數據 100% 常駐 L1/L2 Cache，完全不受外部 3.2 GB/s DRAM 頻寬拉垮。

零分配原地切片 (In-Place Slice & Zero-Allocation)

原則：運行期 0 次 malloc / 0 次 Vec::extend / 0 次 memcpy。

作法：全面採用 Rust 切片引用 &[T] 與 &mut [T]，或前置預分配定長平坦記憶體池 (Slab Allocator)，避免動態擴展引發的記憶體搬移。