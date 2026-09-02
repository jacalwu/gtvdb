# Storage Design

本文件為 **gtvdb** 的磁碟存儲設計規格，目標是達到 **kdb 等級的極致讀寫性能**，同時保留 **Graph、Temporal、Columnar、In‑Memory** 的單引擎一致性。內容以可直接在 Rust 中實作的資料結構、檔案格式、IO 流程與操作 API 為主，並包含 recovery、並發與 benchmark 建議。

---

## 設計目標與原則

- **單一記憶體模型**  
  所有 in‑memory 結構（Arrow RecordBatch、Temporal‑CSR、Vector arrays）能直接對應到磁碟格式，支援 zero‑copy `mmap` 或極低成本序列化。

- **讀優先、順序寫**  
  寫入採順序 append（WAL + delta），讀取以 columnar block + zone map 為主，最大化掃描效率並最小化隨機 IO。

- **時間分區化**  
  以時間為第一級分區，支援快速 time‑range pruning 與 time‑travel。

- **可映射 CSR**  
  Graph 邊表以 CSR 分檔存放，能直接 `mmap` 成為 in‑memory CSR，支援零拷貝 traversal。

- **向量索引側車**  
  向量索引（Flat / IVF / HNSW）以 sidecar 檔案存放，支援快速載入與增量合併。

- **可恢復性與原子性**  
  WAL 可重放，metadata 原子更新，flush/compaction 支援 crash‑safe 操作。

- **可擴展性**  
  本地單節點優化為主，未來在 metadata 層擴展為分散式目錄與分片，不改變本地格式。

---

## 物理佈局與檔案格式

### 目錄範例
/data/
/table=trades/partition=2024-09-01/
meta.json
wal.log
delta.arrow
blocks/
block_0001.col
block_0002.col
edges.csr/
row_ptr.bin
col_idx.bin
edge_ts.col
vectors.idx/
flat.f32
flat.ids
ivf.centroids
ivf.postings

Code

### 檔案類型總覽
- **meta.json**：分區級 metadata（schema version、block list、zone maps、compaction state、checksums）。  
- **wal.log**：append‑only binary log，記錄操作序列（op_type, table, partition, txn_id, payload）。順序寫入，定期切檔。  
- **delta.arrow**：in‑memory delta 的持久化快照（Arrow IPC），用於快速恢復與 crash 後重建。  
- **block_*.col**：列式 block，固定大小（建議 64MB），內含多個 column 的原始 bytes 或 Arrow‑compatible chunk。每個 block 附帶 header（min/max per column、row_count、offsets、checksum）。  
- **edges.csr/**：CSR 分檔  
  - `row_ptr.bin`：連續 u64 array（row pointer）  
  - `col_idx.bin`：連續 u32/u64 array（neighbor ids）  
  - `edge_ts.col`：對應 timestamp column（Arrow 或 raw u64）  
- **vectors.idx/**：向量索引 sidecar  
  - Flat: `flat.f32`（連續 f32 vectors），`flat.ids`（u64 ids）  
  - IVF: `ivf.centroids`（centroid vectors），`ivf.postings`（posting lists，連續 id arrays）

### Block header 範例
```text
struct BlockHeader {
  u32 magic;
  u16 version;
  u64 block_id;
  u64 row_count;
  u64 byte_size;
  // per-column metadata: min,max,offset
  // checksum
}
```

## 寫入路徑 WAL Delta Flush Compaction

### 寫入高階流程

1. 接收寫入請求（insert/update/delete）
2. 同步 append 到 WAL（順序寫，fsync 策略可配置）
3. apply 到 in‑memory DeltaBuffer（Arrow RecordBatch 或自家 columnar buffer）
4. 回應 client（可選同步或異步 commit）
5. 背景 flush：當 delta 達到閾值或時間到，觸發 flush → 產生新的 block 並更新 meta.json

### WAL 格式建議
Record = [txn_id:u64][op:u8][table_id:u32][partition_id:u32][payload_len:u32][payload_bytes]

payload：列式序列化（列名→values）或 Arrow IPC chunk。

切檔策略：當 wal.log > 256MB 或每 N 秒切檔；切檔時產生 wal.log.1、wal.log.2，並在成功 flush 對應 delta 後刪除已 replay 的 wal 範圍。

## DeltaBuffer 設計

每個 table/partition 一個 DeltaBuffer，內含多個列的可寫 buffer（SoA），並維護 row_count。

使用 preallocated pages、SIMD‑aligned buffers、可選的整數壓縮（u16/u32）以降低記憶體帶寬。

定期將 delta 序列化為 delta.arrow 作為快速恢復點。

## Flush 與 Compaction 流程

### Flush 步驟：

- 將 DeltaBuffer 排序（若需要按 timestamp 或主鍵）
- 切分為固定大小 block（例如 64MB）
- 為每個 block 計算 zone map（per column min/max）、row_count、checksum
- 寫入 block_*.col（先寫 temp 檔，寫完後原子 rename）
- 更新 meta.json（原子寫入或使用 write‑ahead meta）
- 刪除或截斷 WAL 中已包含的記錄

### Compaction：

類似 LSM compaction，但目標為讀優化：合併多個小 block 成大 block，合併 zone map，重建 bloom filter，並做刪除/更新的物理清理。

分為 minor（合併小 block）與 major（重寫整個 partition）。

## 讀取路徑 與 索引策略

### Metadata 與 Pruning
meta.json 保存每個 block 的 zone map（每列 min/max）、row_count、offset、bloom filter 指標。

Query flow：

1. 解析 predicate（time range、symbol、id）
2. partition pruning（time）定位 candidate partitions
3. block level zone map + bloom 篩掉不命中的 block
4. 對剩餘 block 採用 zero‑copy mmap 或直接 memory map + pointer arithmetic 讀取需要的列
5. 在 memory 層用 bitmask 做 predicate filter 與 operator fusion

### Zero‑copy 與 mmap 策略

對冷 block 使用 mmap，對熱 block 使用 madvise(MADV_WILLNEED) 與 prefetch。

block 內 column 以連續 bytes 存放，header 提供 offsets，讀取端直接建立 Arrow ArrayData 指向 mmap memory。

可變長字串或 dictionary encoded column提供 sidecar offset table 以支援 direct pointer。

### Bloom Bitmask Zone Map

Zone map：每 block 每列儲存 min/max，支援 range pruning。

Bloom filter：對高基數列（symbol、id）建立 bloom，快速排除不存在的 block。

Bitmask：在 in‑memory 運算中使用 bitmask（u8/u64 位元陣列）做批次過濾與向量化運算。

## Vector Search 整合

- Flat scan：對小規模或精確查詢，直接 mmap flat.f32 並用 SIMD 做 top‑K。
- IVF：先用 centroids 篩選 candidate posting lists，再在 posting lists 上做精確 top‑K。
- Sidecar metadata：每 posting list 附帶 block id / offset，能直接定位到原始 row id 與 column 值。

## Graph Temporal 存儲整合 CSR on disk

### CSR 分檔格式
- row_ptr.bin：u64 array，長度 = num_nodes + 1
- col_idx.bin：u32/u64 array，連續 neighbor ids
- edge_ts.col：對應 timestamp column（Arrow 或 raw u64）
- edge_attr.col：可選屬性列（weight、type），以 columnar block 存放

### 時間分區化 與 版本

以 valid_from 或 event time 做分區（例如 daily/hourly），每個分區有獨立 CSR 檔案。

查詢時先選擇時間分區，再在 CSR 中用 edge_ts 做篩選以支援 time‑travel。

新邊先寫入 delta CSR（appendable adjacency lists），定期合併到主 CSR（compaction）。

### mmap 與 traversal

最近分區在啟動時 mmap，提供零拷貝 traversal。

冷分區按需 mmap，載入後放入 LRU cache。

提供 edge_slices(node_id, time_range) -> &[EdgeSlice]，EdgeSlice 直接指向 mmap memory，避免 allocation。

## 實作細節 API 與 Rust 範例結構

### meta.json schema 範例
```json
{
  "partition": "2024-09-01",
  "schema_version": 3,
  "blocks": [
    {
      "id": 1,
      "file": "blocks/block_0001.col",
      "row_count": 123456,
      "byte_size": 67108864,
      "zone_map": {
        "ts": [1693526400000, 1693612799000],
        "price": [10.5, 99.9]
      },
      "bloom": {
        "symbol": { "offset": 12345, "len": 8192 }
      },
      "checksum": "sha256:..."
    }
  ],
  "wal_files": ["wal.log", "wal.log.1"],
  "version": 42
}
```

### Rust 結構建議
```rust
pub struct BlockHeader {
  pub magic: u32,
  pub version: u16,
  pub block_id: u64,
  pub row_count: u64,
  pub byte_size: u64,
  // offsets and per-column metadata stored separately
}

pub struct DeltaBuffer {
  pub columns: HashMap<String, WritableColumn>,
  pub row_count: usize,
}

pub struct PartitionMeta {
  pub blocks: Vec<BlockMeta>,
  pub wal_files: Vec<String>,
  pub version: u64,
}

pub struct BlockMeta {
  pub id: u64,
  pub row_count: u64,
  pub offsets: HashMap<String, u64>,
  pub zone_map: HashMap<String, (Scalar, Scalar)>,
  pub bloom_offset: Option<u64>,
}
```

## WAL replay 與 flush 原子性

- meta.json 原子更新：寫入 temp file → fsync → rename。
- WAL replay：啟動時先 replay 最近未 flush 的 wal，然後載入 delta.arrow。
- Flush 兩階段 commit：寫 block temp → fsync → rename → 更新 meta → fsync meta → 刪除 wal 範圍。

## 並發與鎖策略

- partition‑level concurrency：每 partition 一把輕量鎖（RwLock），讀多寫少情況下讀不阻塞。
- background workers：flush、compaction、index build 在獨立 thread pool 執行，使用 channel 與 backpressure 控制。
- traversal API 返回引用型 slice，避免分配。

## 測試 Benchmark 與目標指標

### 測試項目
- WAL 順序寫入吞吐（append throughput）
- Cold mmap load time（首次 mmap 並建立 Arrow view）
- Hot scan throughput（columnar scan throughput）
- CSR traversal latency（time‑travel traversal）
- Vector search latency（Flat / IVF top‑K）

### Benchmark 目標
- WAL 順序寫入吞吐 ≥ 1 GB/s（視硬體）
- Columnar scan throughput 接近 DRAM 帶寬（memory bound）
- CSR traversal latency 符合現有 TC3 結果（ms 級）
- Vector top‑K 在 100k–1M 規模下達到單查詢低毫秒級

### 測試方法
建立可重現的 TC1–TC15 benchmark binary（hft_bench），輸出 CSV/Markdown 結果。

在不同硬體（NVMe、SATA、RAM）與不同 fsync 策略下測試吞吐與延遲 tradeoff。

使用 perf / VTune / flamegraph 定位 CPU 與 memory bottleneck。

## 漸進實作路線與優先順序

1. 定義 block binary format 與 meta.json schema
2. 實作 WAL + DeltaBuffer + simple flush（無 compaction）
3. 實作 block level zone map pruning + mmap zero‑copy read
4. 實作 CSR 分檔與 mmap traversal
5. 加入 compaction、bloom filter、IVF sidecar
6. 優化：SIMD top‑K、operator fusion、non‑temporal store、prefetch hints
7. 擴展：metadata sharding 與分散式目錄

## 風險 與 權衡

- 格式穩定性：一旦格式確定，升級成本高，建議設計 versioned header 與向後相容策略。
- 記憶體 vs IO tradeoff：為了極速讀取會增加 mmap 常駐頁面，需設計 LRU 與 memory budget 控制。
- 寫入延遲 vs durability：fsync 策略可配置，測試不同場景下的吞吐與延遲平衡。
- 分散式延伸：本地單節點優化後，分散式需在 metadata 層做 shard/replica 設計，不應改變本地格式。

---

## 附錄 A: WAL + DeltaBuffer Rust 範例（可編譯）

下面是一個精簡但可編譯的 Rust 範例，展示：
- 將寫入以順序 append 到 wal.log
- 將變更應用到 in‑memory DeltaBuffer
- 在閾值到達時 flush 為一個簡單的 block 檔案，並更新 meta.json（簡化版）
- 啟動時 replay wal 並重建 DeltaBuffer

此範例使用 serde + bincode 做簡單序列化。示範用於學習與原型，並非生產就緒。

Cargo.toml (最小依賴):

```toml
[package]
name = "gtvdb_wal_demo"
version = "0.1.0"
edition = "2021"

[dependencies]
serde = { version = "1.0", features = ["derive"] }
bincode = "1.3"
serde_json = "1.0"
```

main.rs:

```rust
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

const WAL_PATH: &str = "data/partition=demo/wal.log";
const META_PATH: &str = "data/partition=demo/meta.json";
const BLOCK_DIR: &str = "data/partition=demo/blocks";

#[derive(Serialize, Deserialize, Debug)]
enum Op {
    Insert { id: u64, ts: u64, price: f64 },
    Delete { id: u64 },
}

#[derive(Serialize, Deserialize, Debug)]
struct WalRecord {
    txn_id: u64,
    op: Op,
}

#[derive(Debug, Default)]
struct DeltaBuffer {
    // simple column-store: vectors for each column
    ids: Vec<u64>,
    tss: Vec<u64>,
    prices: Vec<f64>,
}

impl DeltaBuffer {
    fn apply(&mut self, rec: &WalRecord) {
        match &rec.op {
            Op::Insert { id, ts, price } => {
                self.ids.push(*id);
                self.tss.push(*ts);
                self.prices.push(*price);
            }
            Op::Delete { id: _ } => {
                // naive: no physical delete in delta; production would mark tombstone
            }
        }
    }

    fn row_count(&self) -> usize {
        self.ids.len()
    }

    fn clear(&mut self) {
        self.ids.clear();
        self.tss.clear();
        self.prices.clear();
    }
}

fn ensure_dirs() -> std::io::Result<()> {
    std::fs::create_dir_all(BLOCK_DIR)?;
    Ok(())
}

fn append_wal(record: &WalRecord) -> std::io::Result<()> {
    let mut f = OpenOptions::new()
        .create(true)
        .append(true)
        .open(WAL_PATH)?;
    let bytes = bincode::serialize(record).unwrap();
    // write length prefix then payload (simple framing)
    let len = bytes.len() as u32;
    f.write_all(&len.to_le_bytes())?;
    f.write_all(&bytes)?;
    f.sync_all()?; // configurable in real system
    Ok(())
}

fn replay_wal(delta: &mut DeltaBuffer) -> std::io::Result<()> {
    if !Path::new(WAL_PATH).exists() {
        return Ok(());
    }
    let mut f = File::open(WAL_PATH)?;
    loop {
        let mut len_buf = [0u8; 4];
        match f.read_exact(&mut len_buf) {
            Ok(_) => {}
            Err(_) => break, // EOF
        }
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut payload = vec![0u8; len];
        f.read_exact(&mut payload)?;
        let rec: WalRecord = bincode::deserialize(&payload).unwrap();
        delta.apply(&rec);
    }
    Ok(())
}

fn flush_delta(delta: &mut DeltaBuffer) -> std::io::Result<()> {
    // create a simple block file: write columns as binary arrays with length prefix
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let block_name = format!("{}/block_{}.col", BLOCK_DIR, ts);
    let mut f = File::create(&block_name)?;

    // write ids
    f.write_all(&(delta.ids.len() as u64).to_le_bytes())?;
    for v in &delta.ids {
        f.write_all(&v.to_le_bytes())?;
    }
    // write tss
    f.write_all(&(delta.tss.len() as u64).to_le_bytes())?;
    for v in &delta.tss {
        f.write_all(&v.to_le_bytes())?;
    }
    // write prices
    f.write_all(&(delta.prices.len() as u64).to_le_bytes())?;
    for v in &delta.prices {
        f.write_all(&v.to_le_bytes())?;
    }
    f.sync_all()?;

    // update meta.json (very simplified)
    let meta = serde_json::json!({
        "blocks": [ { "file": block_name, "row_count": delta.row_count() } ],
        "wal_files": ["wal.log"],
        "version": 1u64,
    });
    let tmp_meta = format!("{}.tmp", META_PATH);
    let mut mf = File::create(&tmp_meta)?;
    mf.write_all(serde_json::to_string_pretty(&meta).unwrap().as_bytes())?;
    mf.sync_all()?;
    std::fs::rename(tmp_meta, META_PATH)?;

    // truncate wal after flush (simple strategy: remove file)
    std::fs::remove_file(WAL_PATH).ok();

    delta.clear();
    Ok(())
}

fn demo_workflow() -> std::io::Result<()> {
    ensure_dirs()?;
    let mut delta = DeltaBuffer::default();

    // replay existing WAL on startup
    replay_wal(&mut delta)?;
    println!("After replay, delta rows = {}", delta.row_count());

    // simulate incoming writes
    for i in 0..10u64 {
        let rec = WalRecord {
            txn_id: i,
            op: Op::Insert { id: i + 1000, ts: 1_700_000_000 + i, price: 10.0 + (i as f64) },
        };
        append_wal(&rec)?; // durable append
        delta.apply(&rec); // apply to in‑memory delta
    }
    println!("Before flush, delta rows = {}", delta.row_count());

    // flush when delta grows beyond threshold (here we just flush)
    flush_delta(&mut delta)?;
    println!("Flushed delta to block and updated meta.json");

    Ok(())
}

fn main() {
    if let Err(e) = demo_workflow() {
        eprintln!("Error: {}", e);
    }
}
```

說明與延伸：
- 這個範例在實作上非常簡化：不處理 tombstones、更新、並發鎖、atomic rename 的邊際狀況，或 WAL 部分針對 replay 範圍裁剪。生產系統應該使用更健全的 WAL 切檔、segment 管理、以及元資料版本控制。
- 在實際系統中，block 會包含 header（magic/version/offsets/zone map/checksum），並以原子 rename 與 meta 更新確保 crash safety。

---

如果你想，我可以：
- 直接把這份 docs/storage_design.md 提交到 repo（我已經準備好），或
- 改成只提交 Markdown（不附範例程式碼），或
- 把 Rust 範例拆成獨立檔案（Cargo project）並一併提交。

請告訴我是否要我現在把這個檔案提交到 jacalwu/gtvdb 的預設分支（我將在預設分支上建立 commit）。
