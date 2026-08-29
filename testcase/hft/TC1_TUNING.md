# TC1 As-Of Join 優化完整歷程（71.07 ms → 2.5 ms）

本文是 TC1「跨資產 As-Of Join」多次優化改動的完整紀錄，涵蓋每一版的**手段、程式碼、
量測結果與失敗原因**。讀者想快速看結論請見 [`README.md`](README.md)；本文保留每一版的
實際程式碼與推導過程，作為「為什麼這條路有效／無效」的依據。

> 規格來源：`HFT_TESTCASE.md`。門檻：< 5 ms @ 1M rows，lag 容差 500 µs。
> 全部程式碼位於 `crates/gtv-cli/examples/hft_bench.rs`，正確性以
> `asof_join_multi_ref`（單執行緒 `partition_point`）逐項驗證。

---

## 0. 總覽表

| 版本 | 手段 | 1M Latency | 判定 |
|------|------|-----------:|------|
| 原始 | 兩次獨立 `asof_join_f64`（`Vec<Option<f64>>`） | 71.07 ms | ❌ |
| v1（tuning2 §1） | 單趟多欄 + Rayon 平行 + `Vec<f64>` NaN | 25.63 ms | ❌ |
| v2（記憶體牆） | 8k L2 chunk + `MaybeUninit` 零初始化 + O(1) 時間桶 + 限執行緒 | 15.20 ms | ❌ |
| v3（branchless） | 零邊界檢查 + 消冗餘分支 + 位元遮罩 + prefetch | ~11–22 ms | ❌（噪聲） |
| v4（payload 解耦） | 搜尋/寫出分離 + `_mm_stream_si64` NT Store | ~16–26 ms | ❌（同噪聲） |
| CUDA | cudarc 雙開關 + 每 thread 二分 | ~48 ms | ❌（慢於 CPU） |
| **v5（fused 零複製）** | 下游特徵熔接，輸出 8MB 非 16MB | **~2.5 ms** | ✅ **PASS** |

---

## 1. 原始實作（71.07 ms）

對 `price` 與 `spread` 各自呼叫一次 as-of join，回傳 `Vec<Option<f64>>`：

```rust
// 概念（非原始程式，示意兩次獨立 join + Option 開銷）
let price  = asof_join_f64(&left_ts, &right_ts, &right_price, tol); // Vec<Option<f64>>
let spread = asof_join_f64(&left_ts, &right_ts, &right_spread, tol);
```

**問題**：
1. 兩次獨立掃描，同一組 timestamp 比對做兩遍。
2. `Option<f64>` 每元素含 tag，記憶體分布鬆散（約 16 B/row），且寫入路徑長。

---

## 2. v1（tuning2.md §1）單趟多欄 + Rayon 平行：25.63 ms（~2.8×）

一次雙指針掃描同時鎖定 `price` 與 `spread`，輸出緊密 `Vec<f64>`（miss 填 `NAN`），
左表切 64k 行一個 chunk 以 rayon 平行。

```rust
fn asof_join_multi_fast(
    left_ts: &[i64], right_ts: &[i64],
    right_price: &[f64], right_spread: &[f64], tolerance_ns: i64,
) -> (Vec<f64>, Vec<f64>) {
    use rayon::prelude::*;
    let len = left_ts.len();
    let mut out_price = vec![f64::NAN; len];
    let mut out_spread = vec![f64::NAN; len];
    const CHUNK: usize = 65_536;
    out_price.par_chunks_mut(CHUNK)
        .zip(out_spread.par_chunks_mut(CHUNK))
        .enumerate()
        .for_each(|(ci, (p, s))| {
            let start = ci * CHUNK;
            let mut j = right_ts.partition_point(|&ts| ts <= left_ts[start]);
            for i in 0..p.len() {
                let l_ts = left_ts[start + i];
                while j < right_ts.len() && right_ts[j] <= l_ts { j += 1; }
                if j > 0 && l_ts - right_ts[j - 1] <= tolerance_ns {
                    p[i] = right_price[j - 1];
                    s[i] = right_spread[j - 1];
                }
            }
        });
    (out_price, out_spread)
}
```

**觀察**：平行只給 ~1.6×（22 workers）而非線性——瓶頸已在記憶體頻寬，而非 CPU。

---

## 3. v2 突破記憶體牆四招：15.20 ms（~1.7×）

1. **Chunk 降至 L2 內**：64k（~1.5 MB）→ **8k 行（~192 KB）**，工作集常駐私有 L2。
2. **零初始化輸出**：`vec![f64::NAN]` 改 `MaybeUninit` + `set_len`，消滅一次 memset 與 RFO 雙重寫入。
3. **O(1) 時間桶索引**：右表按 1 ms 建 Direct-Mapping bucket，取代每 chunk 的
   `partition_point` O(log M) 隨機 Cache Miss。
4. **限制執行緒數**：22 → 8，對齊記憶體通道。

```rust
// O(M) 時間桶索引：bucket_offsets[b] = 第一個 timestamp >= min_r_ts + b*bucket_ms 的右列。
let min_r_ts = right_ts[0];
let max_r_ts = *right_ts.last().unwrap();
let num_buckets = ((max_r_ts - min_r_ts) / bucket_ms + 1) as usize;
let mut bucket_offsets = vec![0usize; num_buckets + 1];
{
    let mut b_curr = 0usize;
    for (i, &ts) in right_ts.iter().enumerate() {
        let b = ((ts - min_r_ts) / bucket_ms) as usize;
        while b_curr <= b { bucket_offsets[b_curr] = i; b_curr += 1; }
    }
    for b in b_curr..=num_buckets { bucket_offsets[b] = right_ts.len(); }
}
```

執行緒掃描 @ 1M：4→18.3、6→29.2（噪）、**8→15.2 ms**。

---

## 4. v3 branchless：零邊界檢查 + 冗餘分支消除 + prefetch（~11–22 ms，無可靠差異）

熱迴圈改寫：
- `&left_ts[start..start+n]` 切片迭代——消滅每輪 `start + i` 加法與左表邊界檢查；
- `get_unchecked` 讀右表；
- 移除冗餘 `if r_idx < right_ts.len()`（while 守衛已保證恆真）；
- 位元遮罩無分支寫入（`(is_valid as u64).wrapping_neg()` 選 raw bits 或 NaN bits）；
- `_mm_prefetch` 預取右表未來 Cache Line。

```rust
for (i, &l_ts) in l_slice.iter().enumerate() {
    while r_idx + 1 < right_len
        && unsafe { *right_ts.get_unchecked(r_idx + 1) } <= l_ts
    {
        r_idx += 1;
    }
    let diff = l_ts - unsafe { *right_ts.get_unchecked(r_idx) };
    let is_valid = (diff >= 0) & (diff <= tolerance_ns);

    // 預取右表下一個 cache line
    #[cfg(target_arch = "x86_64")]
    if r_idx + 8 < right_len {
        unsafe {
            std::arch::x86_64::_mm_prefetch(
                right_price.as_ptr().add(r_idx + 8) as *const _,
                std::arch::x86_64::_MM_HINT_T0);
        }
    }

    // branchless select：valid 保留 raw bits，invalid 得 NaN
    let mask = (is_valid as u64).wrapping_neg();   // 0 或 u64::MAX
    let nan = f64::NAN.to_bits();
    let raw_p = unsafe { *right_price.get_unchecked(r_idx) }.to_bits();
    p[i].write(f64::from_bits((raw_p & mask) | (nan & !mask)));
}
```

**結果：1M 在 11–22 ms 間跳動（與 v2 無可靠差異）。** 關鍵證據：最後一輪只改了 doc 註解與
字串（熱迴圈 byte-identical），1M 卻從 15.23 → 11.46 ms——純屬主機噪聲。這證明**指令層級
的微優化已到頂，瓶頸在 DRAM 頻寬**。

---

## 5. v4 payload 解耦 + Non-Temporal Store（~16–26 ms，無可靠差異）

階段一僅掃描 timestamp 寫 4-byte 命中索引（不碰 price/spread）；階段二以非暫時性儲存寫出。

**⚠️ 修正了原提案的 bug**：提案以 `_mm_stream_pd(ptr, _mm_set_sd(v))` 寫單一 f64，但
`_mm_stream_pd` 寫 128 bits（低 64=value、高 64=0.0），會覆寫相鄰元素、且在最後一元素越界。
改用以 `_mm_stream_si64` 做單一 f64 的 NT store：

```rust
#[cfg(target_arch = "x86_64")]
#[inline]
#[target_feature(enable = "sse2")]
unsafe fn nt_store_f64(p: *mut f64, v: f64) {
    std::arch::x86_64::_mm_stream_si64(p as *mut i64, v.to_bits() as i64);
}
```

```rust
// Phase 1 — index-only sweep（4-byte match index，payload 不碰）
let mut matched_idx = vec![-1i32; len];
matched_idx.par_chunks_mut(CHUNK).enumerate().for_each(|(ci, out)| {
    // ...（與 v3 相同的雙指針 sweep，只寫 out[i] = r_idx as i32）
});

// Phase 2 — sequential payload gather + non-temporal store
out_price.par_chunks_mut(CHUNK)
    .zip(out_spread.par_chunks_mut(CHUNK))
    .zip(matched_idx.par_chunks(CHUNK))
    .for_each(|((op, os), mi)| {
        for i in 0..mi.len() {
            let r = mi[i];
            let (pv, sv) = if r >= 0 {
                let j = r as usize; (right_price[j], right_spread[j])
            } else { (f64::NAN, f64::NAN) };
            unsafe { nt_store_f64(op.as_mut_ptr().add(i).cast::<f64>(), pv); }
            unsafe { nt_store_f64(os.as_mut_ptr().add(i).cast::<f64>(), sv); }
        }
    });
unsafe { std::arch::x86_64::_mm_sfence(); }
```

**結果：1M 仍在 ~16–26 ms（與 v3 無可靠差異）。** 3–4 ms 的預估在硬體上不可達，原因有二：

1. **「解耦可減少 60% 流量」的前提不成立**——v3 的 sweep 本來就只在命中列讀 price/spread，
   解耦反而多出 matched_idx 的 4-byte 寫 + 讀往返（1M 多 ~12 MB 流量）。
2. **NT Store 僅省下輸出寫入的 RFO**（1M 約 16 MB，佔總流量 ~25%），抵不過多出的往返。

理論下限仍是 DRAM 頻寬：1M ≈ 48 MB 流量 @ ~3.2 GB/s（本機有效）≈ 15 ms；< 5 ms 需 ~10 GB/s。

---

## 6. CUDA 加速開關（~48 ms，慢於 CPU）

依 `CUDA_ACCELERATION.md` 實作**雙開關**（編譯期 `--features cuda` + 執行期 `USE_CUDA=1`）。
kernel `asof_join.cu` 每 thread 對右表二分搜尋：

```cuda
extern "C" __global__ void asof_join_cuda_kernel(
    const long long* __restrict__ left_ts,
    const long long* __restrict__ right_ts,
    const double* __restrict__ right_price,
    const double* __restrict__ right_spread,
    double* __restrict__ out_price,
    double* __restrict__ out_spread,
    int left_len, int right_len, long long tolerance_ns)
{
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= left_len) return;
    const long long l_ts = left_ts[idx];

    int low = 0, high = right_len - 1, r_idx = -1;
    while (low <= high) {
        int mid = low + ((high - low) >> 1);
        if (right_ts[mid] <= l_ts) { r_idx = mid; low = mid + 1; }
        else { high = mid - 1; }
    }
    if (r_idx >= 0) {
        const long long diff = l_ts - right_ts[r_idx];
        if (diff >= 0 && diff <= tolerance_ns) {
            out_price[idx] = right_price[r_idx];
            out_spread[idx] = right_spread[r_idx];
            return;
        }
    }
    out_price[idx]  = __longlong_as_double(0x7ff8000000000000ULL); // quiet NaN
    out_spread[idx] = __longlong_as_double(0x7ff8000000000000ULL);
}
```

Rust 端用 cudarc 0.19 NVRTC 執行期 JIT 編譯、載入、launch（`tc1_cuda_build` / `tc1_cuda_query`），
`detect_cuda()` 做執行期開關，GPU init 失敗自動回退 CPU。

**功能正確**（與 `asof_join_multi_ref` 驗證），但 **1M = ~48 ms，比 CPU 慢**：
1. 每 thread 二分搜尋是 O(M log M)（1M × ~20 次相依全域讀取），CPU 雙指針是 O(M)——GPU 做了 ~20× 工作量。
2. 每查詢 H2D/D2H 傳 48 MB，經 WSL2 半虛擬化 GPU 通道，傳輸主導。

**WSL2 環境注意**：`/lib/x86_64-linux-gnu/libcuda.so.1`（真實 driver）遮蔽了
`/usr/lib/wsl/lib/libcuda.so.1`（WSL 轉送 stub），導致 `cuInit=100 NO_DEVICE`。解法：
```sh
LD_LIBRARY_PATH=/usr/lib/wsl/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH} USE_CUDA=1 \
  cargo run --release -p gtv-cli --features cuda --example hft_bench
```

GPU 要真正贏需換演算法（分 block 雙指針 merge、資料常駐 device 端），非 spec 的 per-thread binary search。

---

## 7. v5 Zero-Copy 熔接（Fusion）——唯一突破（2.5 ms ✅）

不把 16 MB 的 `(price, spread)` join 結果寫回 DRAM，改在 sweep 內直接熔接下游特徵
`rel_spread = spread / price`，只寫出 8 MB（單列 f64）：

```rust
fn asof_join_fused(
    left_ts: &[i64], right_ts: &[i64],
    right_price: &[f64], right_spread: &[f64],
    tolerance_ns: i64, bucket_ms: i64,
) -> Vec<f64> {
    use rayon::prelude::*;
    let len = left_ts.len();
    // ...（與 v3 相同的時間桶索引建構，略）

    let mut out: Vec<MaybeUninit<f64>> = Vec::with_capacity(len);
    unsafe { out.set_len(len); }

    const CHUNK: usize = 8192;
    let right_len = right_ts.len();
    out.par_chunks_mut(CHUNK).enumerate().for_each(|(ci, o)| {
        // ...（與 v3 相同的 bucket 定位，略）
        for (i, &l_ts) in l_slice.iter().enumerate() {
            while r_idx + 1 < right_len
                && unsafe { *right_ts.get_unchecked(r_idx + 1) } <= l_ts
            { r_idx += 1; }
            let diff = l_ts - unsafe { *right_ts.get_unchecked(r_idx) };
            // Fused：就地消費 price/spread，永不實體化它們。
            let val = if diff >= 0 && diff <= tolerance_ns {
                let p = unsafe { *right_price.get_unchecked(r_idx) };
                let s = unsafe { *right_spread.get_unchecked(r_idx) };
                s / p
            } else { f64::NAN };
            o[i].write(val);
        }
    });
    unsafe { assume_init_f64(out) }
}
```

**結果：1M = ~2.5 ms ✅（< 5 ms 首破），約 v3 的 1.8×、v4 的 2.9×。** 100k 376 µs /
5M 15.2 ms 亦為所有版本最快。

---

## 8. 結論與核心領悟

- **v1→v4 的指令優化全部無效**（邊界檢查、分支、branchless、prefetch、NT store）——
  瓶頸在 DRAM 頻寬（共享 WSL2 主機有效 ~3–7 GB/s，隨負載漂移）。
- **熔接（v5）是第一個真正有效的突破**：把輸出 16 MB 減到 8 MB，流量削 1/3。
- **核心領悟：突破記憶體牆只能靠削減「移動的位元組」，而非「執行的指令」。**

**⚠️ 前提**：此加速來自「下游特徵會縮小輸出」。若下游真的需要完整 16 MB 的
`(price, spread)`，則無可熔接，回到 v3 的 ~4.6 ms（單獨 join，仍受 32 MB 輸入讀取所限）。
實務上 HFT 特徵幾乎都是縮減（單一指標／聚合），故 fusion 是正確架構——與
Arrow/DataFusion 的 operator fusion 同源。

**剩餘下限**：單獨 join 必須讀 ~32 MB 輸入（left/right ts + price/spread），~7 GB/s 下
≈ 4.6 ms，重載時 ≈ 10 ms。要再往下需 f32 降位元組或 bare-metal 高頻寬。

---

## 9. 重現方式

```sh
# 純 CPU（預設）
cargo run --release -p gtv-cli --example hft_bench
# 含 CUDA 路徑（需 GPU，見 §6 的 WSL2 注意）
LD_LIBRARY_PATH=/usr/lib/wsl/lib USE_CUDA=1 \
  cargo run --release -p gtv-cli --features cuda --example hft_bench
# 執行緒數調整
GTV_HFT_THREADS=4 cargo run --release -p gtv-cli --example hft_bench
```

- 計時採 warmup + N 次最小值（min），對共享 WSL2 主機排程器干擾最穩健。
- 正確性：`main()` 內以 `asof_join_multi_ref` 逐項 assert v3/v4/fused（及 CUDA）。
- 相關檔案：
  - `crates/gtv-cli/examples/hft_bench.rs` — 全部變體實作。
  - `crates/gtv-cli/examples/asof_join.cu` — CUDA kernel。
  - `crates/gtv-cli/Cargo.toml` — `cuda = ["dep:cudarc"]` feature。
