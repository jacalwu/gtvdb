# kdb+ (q) TC1–TC5 HFT 效能結果

本文件記錄在 kdb+ 5.0（`.z.K` 5f，release 2026.07.23，個人版 `kc.lic`）上，
以 `testcase/hft/hft_bench.q` 執行的 TC1–TC5 結果，與 Rust `hft_bench` 對照。

執行方式：

```bash
/home/jacal/.kx/bin/q testcase/hft/hft_bench.q
```

環境：
- 8 實體核心、15 GB RAM。
- **q 單執行緒**（`\s` = 0 slaves；free edition 未開啟 secondary threads）。
- 時間以 `.z.p`（奈秒）測量，取 min-of-N（與 Rust `hft_bench` 相同方法）。
- 資料為確定性合成資料（`\S` seed），規模/分佈與 Rust 版一致。

## 結果

| TC | 規模 | kdb+ Latency | 門檻 | 結果 |
|----|-----:|-------------:|------|------|
| TC1 as-of join (`aj`) | 100k | 20.5 ms | — | — |
| TC1 as-of join (`aj`) | 1M | **259.8 ms** | < 5 ms | ❌ FAIL |
| TC1 as-of join (`aj`) | 5M | 980 ms | — | — |
| TC2 OFI + `msum[100]` | 100k | 0.97 ms | — | — |
| TC2 OFI + `msum[100]` | 1M | **20.2 ms** | < 2 ms | ❌ FAIL |
| TC2 OFI + `msum[100]` | 5M | 107.2 ms | — | — |
| TC3 triangle join (`lj`) | 100k | 6.3 ms | — | — |
| TC3 triangle join (`lj`) | 500k | **51.6 ms** | < 10 ms | ❌ FAIL |
| TC4 brute-force KNN (`mmu`, chunked) | 100k | **381.8 ms** | < 8 ms | ❌ FAIL |
| TC4 brute-force KNN (`mmu`, chunked) | 1M | **3.71 s** | < 8 ms | ❌ FAIL |
| TC5 snapshot (`bin`) | 100k | 0.93 µs | — | — |
| TC5 snapshot (`bin`) | 1M | 0.89 µs | — | — |
| TC5 snapshot (`bin`) | 5M | **0.88 µs** | < 1 ms | ✅ PASS |

**門檻達成：1 / 6**（僅 TC5 通過）。

## 相同資源對照（兩者皆單執行緒）

以 kdb+（單執行緒）為基準，Rust `hft_bench` 以 `GTV_HFT_THREADS=1` 重跑（關閉 rayon
多執行緒，保留單執行緒內的 AVX2/FMA SIMD 與向量化 —— 與 q 內部 C primitive 同理）。

| TC | 規模 | kdb+ | Rust（1 執行緒） | kdb+/Rust |
|----|-----:|-----:|-----------------:|----------:|
| TC1 | 100k | 20.5 ms | 1.36 ms | 15.1× |
| TC1 | 1M | 259.8 ms | 23.71 ms | 11.0× |
| TC1 | 5M | 980 ms | 142.66 ms | 6.9× |
| TC2 | 100k | 0.97 ms | 534.8 µs | 1.8× |
| TC2 | 1M | 20.2 ms | 5.11 ms | 4.0× |
| TC2 | 5M | 107.2 ms | 47.38 ms | 2.3× |
| TC3 | 100k | 6.3 ms | 1.41 ms | 4.5× |
| TC3 | 500k | 51.6 ms | 7.13 ms | 7.2× |
| TC4 精確 | 100k | 381.8 ms | 25.32 ms | 15.1× |
| TC4 精確 | 1M | 3.71 s | 176.05 ms | 21.1× |
| TC4 IVF | 100k | — | 2.90 ms | — |
| TC4 IVF | 1M | — | 17.83 ms | — |
| **TC5** | 100k | 0.93 µs | 0.1 µs | **9.3×（Rust 反超）** |
| **TC5** | 1M | 0.89 µs | 0.3 µs | **3.0×** |
| **TC5** | 5M | 0.88 µs | 0.1 µs | **8.8×（Rust 反超）** |

> Rust 單執行緒欄位：TC1 取 `asof_join_multi_l2_bucket`（輸出 price+spread，與 `aj`
> 語意最接近；fused 版 1M 為 12.70 ms 更快但輸出為 rel-spread）；TC2 取 fused；TC3 取
> CSR 偵測器；TC4 精確取 FlatIndex、IVF 為次線性索引。

## 觀察（單執行緒）

1. **TC1–TC4：Rust 單執行緒仍快 2–21×**。差距不再來自多核，而是：
   - **資料型別**：Rust `f32`/`u16` 4/2 bytes vs q `float`/`long` 8 bytes —— 相同邏輯
     q 要多搬 1–2× 記憶體；TC4 512 維 q=8 B、Rust=4 B。
   - **SIMD 手寫核**：Rust 的 AVX2/FMA 四累加器距離核、無分支熱迴圈；q 的 primitive
     是通用 C 迴圈（非 BLAS）。
   - **演算法**：Rust TC1 用 O(1) time-bucket 索引 + 雙指針；q `aj` 是通用 merge-join。
     Rust TC3 用 CSR 排序欄 + binary search；q `lj` 是通用 hash join。
2. **TC5：Rust 重構後反超（~9×）**。將 zone-map 的 O(n) mask 掃描重構為
   `point_in_time_range`（兩次 `partition_point` 二分搜、零寫入、零拷貝切片），
   5M 由 51 µs 降至 ~0.1 µs（~500×），比 q `bin`（0.88 µs）再快 ~9×。
3. 門檻仍是以 Rust 引擎訂的；單執行緒下 Rust 通過 TC3（7.13 ms < 10）、TC4 IVF 100k
   （2.90 ms）、TC5（0.1 µs < 1 ms），共 3/10；kdb+ 僅 TC5（0.88 µs）。

## 檔案

- `testcase/hft/hft_bench.q` — kdb+ TC1–TC5 benchmark（含 smoke tests、chunked TC4、
  逐 case 釋放記憶體與 GC）。
