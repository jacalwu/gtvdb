# TC6–TC10 kdb+ vs GTVDB（單執行緒，相同資源）對照

以 kdb+（q 5.0，`\s`=0 單執行緒）為基準，GTVDB（Rust）以單執行緒重跑對應測試
（`hft_bench_tc6_tc10` 為純 scalar 單執行緒；未開 rayon/SIMD）。兩者皆為確定性
合成資料、min-of-N 計時、per-event = 批次總時間 / 事件數（攤銷單執行緒成本）。

執行方式：

```bash
# kdb+ baseline
/home/jacal/.kx/bin/q testcase/hft/hft_bench_tc6_tc10.q
# GTVDB (Rust)
cargo run --release -p gtv-cli --example hft_bench_tc6_tc10
```

## 結果

| TC | 規模 | kdb+ per-event | GTVDB per-event | GTVDB/kdb+ |
|----|-----:|---------------:|----------------:|-----------:|
| TC6 風控檢查 | 5M orders | 14.4 ns/order | 3.4 ns/order | **4.3× 快** |
| TC7 tick-to-trade | 1M packets | 18.0 ns/packet | 1.1 ns/packet | **16.8× 快** |
| TC8 L2 OBI+micro | 10k symbols | 162 ns/symbol | 11.1 ns/symbol | **14.7× 快** |
| TC8 | 100k symbols | 175 ns/symbol | 17.7 ns/symbol | **9.9× 快** |
| TC8 | 1M symbols | 219 ns/symbol | 20.6 ns/symbol | **10.6× 快** |
| TC9 500×500 協方差 | 100 ticks | 890 µs/tick | 130 µs/tick | **6.9× 快** |
| TC10 撮合引擎 | 200k orders | 3.74 µs/order | 119 ns/order | **31.4× 快** |

## 門檻達成（hft_tc6-tc10.md）

| TC | 門檻 | kdb+ | GTVDB |
|----|------|------|-------|
| TC6 | < 200 ns/order | ✅ 14.4 ns | ✅ 3.4 ns |
| TC7 | < 1.5 µs/packet | ✅ 18 ns | ✅ 1.1 ns |
| TC8 | < 1 µs（10k symbols 批次） | ❌ 1.62 ms | ❌ 111 µs |
| TC9 | < 2 ms/tick | ✅ 890 µs | ✅ 130 µs |
| TC10 | < 1 µs/order | ❌ 3.74 µs | ✅ 119 ns |

> TC8 的「10k symbols < 1 µs」門檻在此硬體上不可達：10k × 10 檔 × 4 欄 ≈ 400k
> flops，需 > 400 GFLOPs 才能壓進 1 µs（單核 AVX2 理論峰值 ~50 GFLOPs）。即使 SIMD
> 也落在 ~8–40 µs。q（1.62 ms）與 Rust（111 µs）皆未能達標，屬門檻本身超硬體上限。

## 觀察

1. **TC6–TC10 全部由 GTVDB（Rust）勝出，單執行緒下 4–31×**。且 Rust 版尚未用
   AVX2/FMA/rayon —— 僅是「型別寬度（f64 vs 泛用 8B）+ 緊湊 scalar 迴圈 + 無分支
   CMOV 化」的差異。TC8/TC9 若加 SIMD 可再拉開。
2. **TC6 的無分支寫法**：`pass += (band & maxq & maxn & smp_ok) as u64` 完全消除
   條件跳轉，5M 筆 16.8 ms（3.4 ns/order，298 M orders/s），超過規格「5M orders/s」
   吞吐 60×。
3. **TC10 的差距最大（31×）**：q 的 per-order dict 操作（`min key`、`_` 刪 key、
   dict assign）每筆 ~3.7 µs；Rust `BTreeMap` 每筆 ~119 ns。若改用規格建議的
   Slab Allocator + IntMap/陣列檔位定址，Rust 可再進 sub-100 ns。
4. **TC9**：q `mmu`（500×500 外積）每 tick 890 µs；Rust 純 scalar 雙重迴圈
   130 µs。規格建議的「下三角平坦化」可再省一半（125k vs 250k 元素），rayon 並行
   可再壓一個量級。

## 檔案

- `testcase/hft/hft_bench_tc6_tc10.q` — kdb+ TC6–TC10 baseline
- `crates/gtv-cli/examples/hft_bench_tc6_tc10.rs` — GTVDB TC6–TC10 benchmark
