# gtvdb HFT TC1–TC4 極致效能重構方案 (tuning2.md)

本文件針對 `RESULTS.md` 中未達標的 TC1 至 TC4 測試案例進行瓶頸診斷，並提供零記憶體配置 (Zero-Allocation)、SIMD 向量化與區塊平行化的重構實作代碼。

---

## 1. TC1: 跨資產 As-Of Join 重構方案

### 瓶頸診斷 (71.07 ms ➔ 目標 < 5 ms)
* 每次 Join 產出 `Vec<Option<f64>>`，包含額外的 Bitmap 開銷與高達 16 bytes/row 的記憶體分佈。
* 將 `price` 與 `spread` 分開執行了 2 次獨立的 As-Of Join 掃描。

### 核心優化策略
1. **單趟多欄投影 (Single-pass Multi-column)**：一次雙指針掃描同時鎖定 Price 與 Spread。
2. **Rayon 區塊平行二分掃描**：將左表切分為 64k 行一個 Chunk，右表透過 `partition_point` 二分搜尋定位起點，各 Thread 獨立執行無鎖 Sweep。
3. **緊密連續記憶體**：直接輸出 `Vec<f64>`（無效值填充 `f64::NAN`），徹底消滅 `Option` 開銷。

### 重構代碼 (`crates/gtv-cli/examples/hft_bench.rs`)

```rust
use rayon::prelude::*;

pub fn asof_join_multi_fast(
    left_ts: &[i64],
    right_ts: &[i64],
    right_price: &[f64],
    right_spread: &[f64],
    tolerance_ns: i64,
) -> (Vec<f64>, Vec<f64>) {
    let len = left_ts.len();
    let mut out_price = vec![f64::NAN; len];
    let mut out_spread = vec![f64::NAN; len];

    let chunk_size = 65536; // 64k 行 Chunk，適應 L2/L3 Cache Line
    out_price
        .par_chunks_mut(chunk_size)
        .zip(out_spread.par_chunks_mut(chunk_size))
        .enumerate()
        .for_each(|(chunk_idx, (p_chunk, s_chunk))| {
            let start_i = chunk_idx * chunk_size;
            
            // 二分搜尋尋找右表對應搜尋起點 O(log M)
            let left_start_ts = left_ts[start_i];
            let mut r_idx = right_ts.partition_point(|&ts| ts <= left_start_ts);
            if r_idx > 0 { r_idx -= 1; }

            // 區塊內單調雙指針 Sweep O(K)
            for i in 0..p_chunk.len() {
                let l_ts = left_ts[start_i + i];
                while r_idx + 1 < right_ts.len() && right_ts[r_idx + 1] <= l_ts {
                    r_idx += 1;
                }
                if r_idx < right_ts.len() {
                    let diff = l_ts - right_ts[r_idx];
                    if diff >= 0 && diff <= tolerance_ns {
                        p_chunk[i] = unsafe { *right_price.get_unchecked(r_idx) };
                        s_chunk[i] = unsafe { *right_spread.get_unchecked(r_idx) };
                    }
                }
            }
        });

    (out_price, out_spread)
}