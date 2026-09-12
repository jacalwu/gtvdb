# prod_p1 設計文件 — Kernel 快速改進

> 對應批次清單：`prod_p1.md`
> 上層路線圖：`BANKING_ANALYTICS_GAP_ROADMAP.md`
> 範圍：B1-1 Distance metric、B1-2 TemporalCSR 自適應索引、
> B1-3 AML 走訪安全、B1-4 HNSW 連續佈局。

---

## 1. 總覽

本批全部改動集中喺 **已存在嘅 kernel crate**（`gtv-core` / `gtv-index` /
`gtv-pattern`），唔引入新 crate、唔依賴 catalog。目標係：

1. 修正 metric 語意缺失（銀行 embedding 必需 Cosine）。
2. 消除 `csr.rs` 已標記嘅 `TODO(P1)` 線性掃描。
3. 令圖走訪有硬性資源上限（避免 AML 查詢 OOM）。
4. 重構 HNSW 記憶體佈局，並預留版本化序列化供第二批用。

```
B1-1 Metric ─┐
             ├─> B1-4 HNSW（序列化格式 freeze）
B1-2 CSR ────┴─> B1-3 AML 走訪
```

---

## 2. B1-1 Distance metric contract

### 2.1 設計原則

- **全系統統一「越小越近」（lower = closer）**：排序、top-K、heap 全部用同一方向。
- 索引建構時**鎖定** `metric` 同 `dim`，查詢唔可以靜默用第二個 metric。
- Cosine 嘅 normalization policy 必須**明確記錄**，避免同一索引內混合 normalized
  同 raw 向量。

### 2.2 新增 `gtv-core/src/metric.rs`

```rust
/// Distance metric fixed at index build time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Metric {
    /// Squared Euclidean: sum((a-b)^2). Lower = closer.
    L2,
    /// Cosine distance = 1 - cos(a,b). Lower = closer. Requires normalization
    /// policy (see `normalized` flag on the index/collection).
    Cosine,
    /// Inner product. Stored/sorted as NEGATIVE inner product so that
    /// "lower = closer" holds uniformly. NOTE: not a metric (no triangle
    /// inequality); ANN recall may degrade — see §2.7.
    Ip,
}

impl Metric {
    /// Lower is closer, for every variant.
    #[inline]
    pub fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        match self {
            Metric::L2 => a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum(),
            Metric::Cosine => 1.0 - cosine_similarity(a, b),
            Metric::Ip => -dot(a, b), // negative → lower = closer
        }
    }

    /// Cosine (and optionally Ip) want unit-norm vectors for a stable policy.
    pub fn requires_normalization(&self) -> bool {
        matches!(self, Metric::Cosine)
    }

    /// Human name for SQL / manifest.
    pub fn as_str(&self) -> &'static str {
        match self { Metric::L2 => "l2", Metric::Cosine => "cosine", Metric::Ip => "dot" }
    }

    pub fn parse(s: &str) -> Option<Metric> { /* case-insensitive l2|cosine|dot|ip */ }
}

/// Roadmap alias.
pub type DistanceMetric = Metric;

pub fn dot(a: &[f32], b: &[f32]) -> f32 { a.iter().zip(b).map(|(x, y)| x * y).sum() }
pub fn norm(a: &[f32]) -> f32 { dot(a, a).sqrt() }
pub fn normalize(a: &mut [f32]) { let n = norm(a); if n > 0.0 { for x in a { *x /= n; } } }
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let na = norm(a); let nb = norm(b);
    if na == 0.0 || nb == 0.0 { return 0.0; }
    dot(a, b) / (na * nb)
}
```

### 2.3 `VectorIndex` trait 演進（`gtv-core/src/traits.rs`）

現況只有：

```rust
fn search_knn(&self, query: &[f32], k: usize, filter_mask: Option<&BooleanArray>) -> Result<UInt64Array>;
```

問題：唔回距離，無法做 rerank / telemetry；亦冇 metric 資訊。

新增：

```rust
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VectorHit { pub id: u64, pub distance: f32 }

pub trait VectorIndex: Send + Sync {
    /// Metric fixed at build time.
    fn metric(&self) -> Metric;
    /// Vector dimension.
    fn dim(&self) -> usize;

    /// Ranked hits (nearest first), **exact or approximate per index type**.
    fn search(
        &self,
        query: &[f32],
        k: usize,
        filter_mask: Option<&BooleanArray>,
    ) -> Result<Vec<VectorHit>>;

    /// Backward-compatible convenience: ids only.
    fn search_knn(
        &self,
        query: &[f32],
        k: usize,
        filter_mask: Option<&BooleanArray>,
    ) -> Result<UInt64Array> {
        let hits = self.search(query, k, filter_mask)?;
        Ok(UInt64Array::from(hits.into_iter().map(|h| h.id).collect::<Vec<_>>()))
    }
}
```

> `search_knn` 變成 provided method，所有現有 caller（`graph.rs` 等）零改動。

### 2.4 `gtv-core/src/error.rs` 新增

```rust
#[error("metric mismatch: index={index:?}, query={query:?}")]
MetricMismatch { index: Metric, query: Metric },

#[error("dimension mismatch: index={index}, query={query}")]
DimensionMismatch { index: usize, query: usize },
```

### 2.5 三個索引改動

**FlatIndex**（`flat.rs`）：
- 新增欄位 `metric: Metric`、`normalized: bool`。
- `new` / `from_flat` 增 `metric` 參數（或用 builder 保留舊 signature + `with_metric`）。
- `data()` 保持回 raw；若 `metric == Cosine && !normalized`，於建構時 normalize
  並將 `normalized = true`。
- `search()` 用 `metric.distance` 取代寫死 `squared_l2`；AVX2 kernel 保留 L2，
  新增 `dot` kernel；Cosine normalize 後可復用 L2 kernel 做
  `cos_dist = 0.5 * l2_sq`（見 §2.6）。
- `impl VectorIndex` 回 `Vec<VectorHit>`。

**IvfIndex**（`ivf.rs`）：
- 加 `metric` / `normalized`；centroid assignment 同 cell 內掃描都改用 `metric`。
- 注意：Ip（負內積）下 centroid 應為 **mean 而非 normalized mean**；
  Cosine 下 centroid 需 normalize。

**HnswIndex**（`hnsw.rs`）：
- 加 `metric` / `normalized`；`squared_l2` 私有函式改為 `self.metric.distance`。
- greedy descent / search_layer / top-K 全部用同一距離。
- 見 §2.7 對 Ip 嘅限制。

### 2.6 Cosine 實作選擇

- **若向量已 unit-norm**：`cos_dist = 1 - dot(a,b)`；亦等於 `0.5 * L2²`，
  可直接用現有 AVX2 L2 kernel。
- **若未 normalize**：build/insert 時 normalize 並記 `normalized = true`；
  對外 `distance` 回 cosine distance。
- 呢個令 Cosine 無需新 SIMD kernel，直接復用 L2，效能等同 L2。

### 2.7 Inner product 嘅限制（必讀）

Inner product **唔滿足三角不等式**，即唔係 metric。對 HNSW 嘅圖導航同 IVF 嘅
centroid 分區都會令 recall 下降。緩解：

1. **首選**：要求用戶用 Cosine（normalize 後等價於 L2）代替 raw Ip。
2. 如必須 raw Ip：提供 norm-augmentation（加一維 `sqrt(max_norm² - ||x||²)`）
   將 MIPS 化為 L2，並在 manifest 記錄 augmentation 參數。
3. 對 Ip 索引，`search()` 必須仍正確排序（呢點可保證），但 recall 需經
   Recall@K 量度，唔可以當 L2 咁預期。

文件需明確寫明呢點。

### 2.8 Engine / SQL 整合

- `gtv-engine/src/knn.rs`：`KnnCollection` 加 `metric`、`dim`、`normalized`；
  `search()` 用 `metric.distance`。
- `GtvContext::register_knn(name, ids, vectors, labels)` 加 optional metric
  （或新增 `register_knn_metric`）。
- `gtv-cli`：`knn <node> [k] [--mask ...] [--metric l2|cosine|dot]`。
- UDTF `knn(name, query, k [, label])` 保持。

### 2.9 測試

| 測試 | 方法 |
|---|---|
| Flat/HNSW/IVF 三者一致 | 同一 corpus + metric，Top-K 對比獨立參考（手算 / numpy 生成 golden） |
| Cosine normalize policy | 已 normalize vs 未 normalize 輸入結果一致 |
| Metric mismatch | 查詢 metric ≠ 索引 → `MetricMismatch` |
| Dimension mismatch | → `DimensionMismatch` |
| L2 零回歸 | 現有所有測試須過 |
| Ip 排序 | 對少量資料手算負內積排序 |
| Recall | HNSW cosine(normalized) recall == HNSW l2 recall（數學等價） |

---

## 3. B1-2 TemporalCSR 自適應時間索引

### 3.1 現況與不變量

- `csr.rs`：edges 按 `(src, valid_from, valid_to, dst)` 排序；
  `offsets` 切分每個 source 嘅連續 run。
- 半開區間：edge active at T ⇔ `valid_from <= T < valid_to`。
- `all_active_at(T)` 用全域 bounding box（`max_valid_from` / `min_valid_to`）做 O(1) 快路。
- **`valid_to` 喺 run 內非單調** → 唔可以單純 binary search `valid_to`。

### 3.2 策略分派

```
degree = offsets[s+1] - offsets[s]

degree < 16            -> LinearScan（現狀）
16 <= degree < 4096    -> BinarySearch + ChunkZoneMap
degree >= 4096         -> TimeBucket + ChunkZoneMap
degree >= HIGH_PARALLEL (config, e.g. 1<<20)
                       -> ParallelChunkScan
```

回傳 `NeighborStrategy` 供 telemetry / 測試。

### 3.3 新增資料結構（`TemporalCSR`）

```rust
pub struct TemporalCSR {
    // 現有欄位 ...
    node_count: usize,
    offsets: Vec<u32>,
    dst: Vec<u64>,
    valid_from: Vec<i64>,
    valid_to: Vec<i64>,
    edge_type: Vec<u16>,
    max_valid_from: i64,
    min_valid_to: i64,

    // B1-2 新增
    chunk_size: usize,                 // 預設 64
    /// 每個 edge chunk 嘅 max_valid_to，concatenated。
    /// chunk c of source s 位於 chunk_index[s] + c。
    chunk_max_valid_to: Vec<i64>,
    /// 每個 source 嘅 chunk 起點（長度 node_count + 1）。
    chunk_index: Vec<u32>,
    /// 只有高 degree source 才有 bucket index。
    buckets: Option<Box<BucketIndex>>,
    degree_histogram: Vec<u64>,        // 建構時算一次
}
```

`BucketIndex`（針對高 degree source）：

```rust
struct BucketIndex {
    /// 每個 source 一個 bucket 區（可 sparse，用 HashMap<u32, PerSource>）。
    per_source: HashMap<u32, PerSourceBuckets>,
}
struct PerSourceBuckets {
    t0: i64,          // run 內最小 valid_from
    width: i64,       // bucket 寬度（ns）
    /// bucket b 對應 edge 區間 [bucket_start[b], bucket_start[b+1])
    bucket_start: Vec<u32>,
    /// 每個 bucket 嘅 max_valid_to（zone map）
    bucket_max_valid_to: Vec<i64>,
}
```

> 建構成本：`chunk_max_valid_to` 約每 64 edges 一個 i64（≈ 0.2% 額外記憶體）。
> BucketIndex 只對 `degree >= 4096` 嘅 source 建。

### 3.4 查詢演算法

```
fn neighbors(src, T):
    (start, end) = edge_range(src)
    if all_active_at(T):              # O(1) 快路，整個 run 有效
        return run[ start..end ]
    n = end - start
    match strategy(n):
      LinearScan:
        scan start..end, keep if active
      BinarySearch + ChunkZoneMap:
        hi = partition_point(valid_from[start..end], |vf| vf <= T)   # 只剪 valid_from
        for each chunk c in [start, start+hi) step chunk_size:
            if chunk_max_valid_to[c] <= T: continue                 # 整 chunk 過期
            scan chunk: keep if valid_from <= T < valid_to          # SIMD valid_to
      TimeBucket:
        for each bucket b with bucket.t0 + b*width <= T:
            if bucket_max_valid_to[b] <= T: continue
            scan bucket edges, keep if active
```

- binary search 只剪 `valid_from`，**必須**配合 chunk zone map 剪已過期區段，
  否則仍有大量有效 but 已過期嘅前綴被掃。
- SIMD：`valid_to` 比較用 Arrow / 手寫 AVX2 mask（可選，先 scalar 正確再優化）。

### 3.5 對外 API

- `neighbors()` **簽名不變**，內部走 `neighbors_planned()`。
- 新增：
  ```rust
  pub fn neighbors_planned(&self, src: u64, valid_at: i64)
      -> Result<(impl Iterator<Item = Neighbor> + '_, NeighborStrategy)>;
  pub fn degree(&self, src: u64) -> Result<u32>;
  pub fn degree_histogram(&self) -> &[u64];
  pub fn stats(&self) -> TemporalCsrStats;
  ```
- `khop` 改用 `neighbors_planned`（但實際 visited / budget 由 B1-3 接手）。

### 3.6 測試

| 測試 | 方法 |
|---|---|
| 結果一致 | 隨機 temporal edges，`neighbors` vs 純線性 oracle 逐位元比較 |
| 半開區間 | 邊界 `T == valid_to`、`T == valid_from` 測試 |
| all_active_at | 快路與慢路結果一致 |
| 高 degree 效能 | 建 1M-degree node，量測延遲下降（example/bench） |
| khop 回歸 | 現有 khop 測試零回歸 |

---

## 4. B1-3 AML / 圖走訪安全與資源預算

### 4.1 動機

`khop` 現時每個 hop `sort + dedup`；高 degree + k 大會令 frontier 爆炸，
冇任何上限，尾延遲不可預測。AML 走訪必須有硬性資源上限同確定性。

### 4.2 VisitedSet（generation-stamped）

```rust
pub struct VisitedSet {
    stamp: Vec<u32>,   // len = node_count, 4 bytes/node
    gen: u32,
}
impl VisitedSet {
    pub fn new(node_count: usize) -> Self { /* stamp = vec![0; n], gen = 1 */ }
    /// true if newly marked (i.e. not seen this generation).
    #[inline]
    pub fn mark(&mut self, id: u32) -> bool {
        if self.stamp[id as usize] == self.gen { false }
        else { self.stamp[id as usize] = self.gen; true }
    }
    #[inline]
    pub fn seen(&self, id: u32) -> bool { self.stamp[id as usize] == self.gen }
    pub fn reset(&mut self) {
        self.gen = self.gen.wrapping_add(1);
        if self.gen == 0 { self.stamp.iter_mut().for_each(|s| *s = 0); self.gen = 1; }
    }
}
```

- O(1) reset（只 bump gen），遠優於 `HashSet::clear`。
- 4 bytes/node（稀疏圖可改用 sparse variant，見下）。

### 4.3 資源預算

```rust
pub struct TraversalBudget {
    pub max_hops: usize,
    pub max_edges: usize,          // 累計掃描 edge 數
    pub max_frontier: usize,       // 單一 hop frontier 上限
    pub max_rows: usize,           // 累計輸出節點
    pub max_memory_bytes: usize,   // visited + frontier 估算
    pub deadline: Option<std::time::Instant>,
}
pub struct TraversalStats {
    pub hops: usize, pub edges_scanned: u64, pub rows: u64,
    pub peak_frontier: usize, pub strategy: NeighborStrategy,
}
```

超出 → `GtvError::BudgetExceeded { stage: &'static str, limit: u64, observed: u64 }`。

### 4.4 取消

```rust
pub type CancelToken = std::sync::Arc<std::sync::atomic::AtomicBool>;
```

每掃 N 個 edge（例如 4096）檢查一次 `load(Relaxed)`；set → 回
`GtvError::Cancelled`。

### 4.5 direction-optimizing BFS

- dense frontier（`|frontier| * avg_degree > threshold`）改用 pull：
  遍歷候選節點，檢查是否有鄰居喺 frontier。
- 需要 **reverse CSR**：`TemporalCSR::transpose() -> TemporalCSR`（lazy、可選、
  cache 於 `TemporalGraph` 層）。
- 注意 temporal 語意：pull 時要查「當前節點 → 前一層 frontier」嘅 active edges，
  即 reverse CSR 上做同樣嘅 temporal 過濾。

### 4.6 predicate pushdown

```rust
pub trait EdgePredicate: Send + Sync {
    /// Return false to skip this edge before frontier expansion.
    fn keep(&self, edge: &Neighbor) -> bool;
}
```

內建 builder：amount / currency / channel / jurisdiction / edge_type / time。
predicate 喺 `neighbors_planned` 過濾，減少 frontier 膨脹。

### 4.7 高 degree guard

- `high_degree_guard: usize`（例如 100_000）。
- degree 超門檻且冇 predicate → 回
  `GtvError::HighDegreeNode { node, degree, hint }`，提示加 predicate 或調高預算。
- 避免「一撳就爆」。

### 4.8 API

```rust
pub struct KhopResult { pub frontiers: Vec<UInt64Array>, pub stats: TraversalStats }

impl TemporalCSR {
    pub fn khop_bounded(
        &self,
        seeds: &UInt64Array,
        k: usize,
        valid_at: i64,
        budget: &TraversalBudget,
        predicate: Option<&dyn EdgePredicate>,
        cancel: Option<&CancelToken>,
    ) -> Result<KhopResult>;
}
```

- 舊 `khop()` = `khop_bounded` 用無限預算 + 無 predicate，保持兼容。
- 輸出 frontier 一律**按 node id 升序**（確定性）。

### 4.9 `gtv-pattern` 整合

- `find()` / `find_from()` / `find_ring3()` 加 `budget` / `cancel` / visited。
- DFS backtracking 亦要遵守 `max_edges` / `deadline`。
- 結果排序確定化（現時註解講明 ordering unspecified，需改）。

### 4.10 SQL / CLI

- 新增 UDTF `khop(src, k, valid_at [, max_hops, max_edges])`。
- CLI：`khop <src> <k> <valid_at> [--max-edges N] [--max-frontier N]`。

### 4.11 測試

| 測試 | 方法 |
|---|---|
| 記憶體上限 | 稠密高 degree 圖，`BudgetExceeded` 而非 OOM |
| 確定性 | 同查詢多次執行結果完全一致 |
| cancel / timeout | 毫秒級中斷 |
| predicate pushdown | 掃描 edge 數 / 記憶體下降 |
| 向後兼容 | 無預算 `khop` 零回歸 |
| pattern 零回歸 | ring/path/diamond 結果不變 |

---

## 5. B1-4 HNSW 連續記憶體佈局重構

### 5.1 現況反模式

```rust
struct Node { id: u64, vector: Vec<f32>, layers: Vec<Vec<usize>> }
struct HnswIndex { nodes: Vec<Node>, entry: usize, dim, m, m0, ... }
```

每個 node：1 次 vector alloc + (level+1) 次 layer alloc；查詢 pointer chasing，
cache miss 高。

### 5.2 目標佈局

```rust
pub struct HnswIndex {
    ids: Vec<u64>,            // n
    vectors: Vec<f32>,        // n * dim, row-major
    dim: usize,
    levels: Vec<u8>,          // per node top level
    /// node_offset[i] = start index into `neighbours` for node i.
    node_offset: Vec<u32>,    // n + 1
    /// neighbours: for node i, level 0 has m0 slots, levels 1..=levels[i] have m slots.
    neighbours: Vec<u32>,     // total = sum_i (m0 + levels[i] * m)
    entry: u32,
    max_level: u8,
    m: usize, m0: usize,
    ef_construction: usize, ef_search: usize,
    metric: Metric, normalized: bool,
    deleted: Vec<bool>,       // tombstones
    tombstone_count: usize,
}
```

**鄰居尋址**（node i, level l）：

```
base = node_offset[i]
if l == 0 { slice = neighbours[base .. base + m0] }
else      { off = base + m0 + (l-1) * m; slice = neighbours[off .. off + m] }
```

每 node 只有一次分配（`vectors` / `neighbours` 各一大塊），
所有 node 連續。

### 5.3 查詢 scratch（per-thread，零 allocation）

```rust
struct SearchScratch {
    visited: Vec<u32>,   // generation stamps, len = n
    gen: u32,
    candidates: BinaryHeap<Reverse<OrdF32U32>>,  // min-heap
    results: BinaryHeap<OrdF32U32>,              // bounded max-heap (size <= ef)
}
```

- `reset()` bump gen（同 §4.2）。
- 批次 / 並行查詢時每 thread 一個 scratch（rayon `map_init`）。

### 5.4 搜尋演算法

```
search(query, k, filter):
    ef = max(ef_search, k)
    (d0, entry) = greedy_descend(query, entry, max_level)
    for level in (0..max_level].rev():
        candidates = search_layer(query, [(d0, entry)], ef, level, filter)
    results = search_layer(query, candidates, ef, 0, filter)
    return top-k(results)   # skip deleted
```

- `OrdF32U32`：`f32` 用 `total_cmp` 包裝成 `Ord`。
- `search_layer` 用 min-heap 展開、bounded max-heap 收結果。

### 5.5 Tombstone 與 compaction

- `delete(id)`：設 `deleted[pos] = true`（需 id→pos 映射）。
- search 跳過 deleted；若有效結果不足 k，擴大 ef 重試。
- `tombstone_count / len > threshold`（例如 20%）→ 觸發 `compact()`：
  重建只保留未刪節點（用同一 RNG seed 保證可重現，或重新 insert）。
- `compact()` 產生新 `HnswIndex`，交給 B2-2 atomic swap。

### 5.6 批次 / 並行查詢

```rust
pub fn search_batch(&self, queries: &[Vec<f32>], k: usize,
                    filter: Option<&BooleanArray>) -> Result<Vec<Vec<VectorHit>>>;
```

- 用 rayon `par_iter().map_init(|| SearchScratch::new(n), ...)`。
- 確定性：每查詢結果只依自身 query（無共享可變狀態），故並行 == 逐條。

### 5.7 動態 ef_search 與 telemetry

```rust
pub fn search_with_ef(&self, query, k, ef, filter) -> Result<Vec<VectorHit>>;

pub struct SearchReport {
    pub candidates_visited: u64,
    pub filtered_out: u64,
    pub exact_distance: bool,   // false for HNSW
}
// search_report() 可回傳最後一次（或經參數 out）
```

### 5.8 filter-aware routing（只留 hook）

- 若 `filter` 存在且 selective，B1-4 只提供 `filtered_exact_fallback()`：
  對允許嘅 id 走 FlatIndex 精確掃描。
- 真正策略分派（pre/traversal/post）喺第三批 B3-3。

### 5.9 版本化序列化（供 B2-2）

```
[magic "GTVIDX\0"][u16 version][u32 manifest_len][manifest JSON][payload]
```

HNSW payload（全部 little-endian, 連續）：

```
u64 n, u32 dim, u8 max_level, u32 entry, u32 m, u32 m0,
u8 metric_code, u8 normalized, u64 tombstone_count,
ids:        n * u64
vectors:    n * dim * f32
levels:     n * u8
node_offset:(n+1) * u32
neighbours: total * u32
deleted:    ceil(n/8) bytes
```

API：

```rust
pub fn to_bytes(&self) -> Vec<u8>;
pub fn from_bytes(bytes: &[u8]) -> Result<Self>;
pub fn checksum(&self) -> [u8; 32];   // blake3 over payload
```

- version 一旦 freeze，B2-2 直接用；升級時 `from_bytes` 需向後兼容或明確要求 rebuild。

### 5.10 兼容性

- `HnswIndex::build(ids, Vec<Vec<f32>>, m, efc, efs)`、`insert(id, Vec<f32>)`、
  `len/is_empty/dim`、`impl VectorIndex` 全部保留。
- `insert` 需要處理 `neighbours` 重新配置（可能重新分配；或預留 capacity）。

### 5.11 測試

| 測試 | 方法 |
|---|---|
| Recall 不回歸 | 現有 `build_matches_flat_recall_on_random_data` + 更大隨機集 |
| 記憶體 | bytes/vector 前後對比（目標 ≥ 2× 改善） |
| 批次一致性 | `search_batch` vs 逐條 `search` 完全一致 |
| tombstone | 刪除後唔回傳；compaction 後仍正確 |
| round-trip | `to_bytes` → `from_bytes` recall/latency 一致 |
| ef 曲線 | `ef_search` 對 recall/latency 單調 |

---

## 6. 交付順序與里程碑

| 里程碑 | 內容 | 出口 |
|---|---|---|
| M1 | B1-1 metric + trait 演進 | 三索引 Cosine/Dot 正確；L2 零回歸 |
| M2 | B1-2 CSR 自適應 | 高 degree 延遲 ≥ 10× 改善；結果逐位元一致 |
| M3 | B1-3 AML 安全 | 高 degree 查詢有硬上限；確定性；cancel |
| M4 | B1-4 HNSW 重構 | recall 不回歸；記憶體 ≥ 2× 改善；序列化 round-trip |

## 7. 風險登記

| 風險 | 影響 | 緩解 |
|---|---|---|
| Ip 非 metric 導致 recall 下降 | 中 | 文件明示 + 推 Cosine + norm-augmentation 選項 |
| HNSW 重構引入 recall 回歸 | 高 | FlatIndex oracle property test；逐步重構 |
| CSR chunk metadata 增記憶體 | 低 | 量測；chunk_size 可配 |
| 序列化格式未 freeze 而 B2-2 開工 | 中 | M4 先 freeze v1 格式 |
| reverse CSR 記憶體 | 中 | lazy + 可選；只在 dense pull 時建 |
| 核心 API 改動破壞 caller | 中 | `search_knn` 保持 provided method；舊測試零回歸 |
