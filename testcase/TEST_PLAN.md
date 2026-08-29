# gtvdb P5 Test Plan — kdb+ / KDB.AI feature parity

This document defines the acceptance test cases that verify **gtvdb** (after
Phase 5 — distributed query dispatch via `tonic` gRPC + Arrow Flight) delivers
the time-series and vector capabilities that **kdb+ / KDB.AI** are known for.

Each test case is anchored to a concrete use case from the
[KxSystems/kdbai-samples](https://github.com/KxSystems/kdbai-samples) repository,
mapped onto the corresponding gtvdb primitive, with an exact expected result
(golden file) and a pass/fail criterion.

---

## 1. Mapping: kdbai-samples → gtvdb feature → test case

| kdbai-samples use case (notebook)                | kdb+ / KDB.AI capability                         | gtvdb primitive                          | Test case |
|--------------------------------------------------|--------------------------------------------------|------------------------------------------|-----------|
| `TSS_non_transformed` (`createHDB.q`)            | as-of alignment of trade prices (`aj`)           | `asof_join` / `asof_join` table fn       | TC-01     |
| `TSS_non_transformed` (technical analysis)       | rolling `mavg`/`msum`/`deltas` on prices         | `mavg`/`msum`/`deltas` WindowUDFs        | TC-02     |
| HDB time-travel (`as of` partitioning)           | half-open `[valid_from, valid_to)` slicing       | Temporal-CSR + SQL `WHERE`               | TC-03     |
| money-transfer graphs (graph `G` in gtvdb)       | k-hop / neighbor traversal                       | `neighbors()` / `khop`                   | TC-04     |
| `pattern_matching` (sensor data)                 | temporal pattern matching (ring/path/diamond)    | `gtv-pattern`                            | TC-05     |
| `music_recommendation`, `metadata_filtering`     | filtered vector K-NN (bitmask pruning)           | `HnswIndex::search_knn` + `BooleanArray` | TC-06     |
| `TSS_non_transformed` / `TSS_transformed`        | temporal similarity search (window → vector)     | window normalize + `VectorIndex`         | TC-07     |
| `qFlat` / `qHnsw` on-disk + HDB persistence      | Parquet persistence + time-travel store          | `gtv-storage` (Parquet + SnapshotStore)  | TC-08     |
| *any SQL sample, replicated over nodes*          | distributed query dispatch (gRPC)                | P5 gRPC `ExecuteSql`                     | TC-09     |
| *large result sets*                              | zero-copy data streaming (Arrow Flight)          | P5 Flight `DoGet`/`DoPut`                | TC-10     |

---

## 2. Test data (canonical)

The REPL ships with a hardcoded demo dataset; `data/gen_data.py` regenerates the
same data as CSV so a P5 server can load it from disk. Run:

```sh
python3 testcase/data/gen_data.py --out-dir testcase/data/generated
```

Dataset summary:

- **`nodes`** — 6 nodes `id 0..5`, attribute `value 1..6`.
- **`edges`** — 6 temporal edges, half-open `[valid_from, valid_to)` (ns).
- **`prices`** — `t = [0,10,20,30,40,50]`, `price = [100,101,99,102,103,104]`.
- **`transfers_*`** — 4-node money-transfer graph with distinct event times
  (`10,15,20,25,30,40`), used by TC-05.
- **`songs`** — 10 songs with `genre` + 2-dim embeddings, used by TC-06/07.
- **`tss_series`** — 4 reference price windows + 1 query, used by TC-07.

---

## 3. Test case matrix

| ID    | Name                                     | Runnable now (REPL) | P5 required | Golden file                                   |
|-------|------------------------------------------|---------------------|-------------|-----------------------------------------------|
| TC-01 | As-of join (`aj`)                        | ✅                  | ✅ (SQL)    | `expected/tc01_asof.txt`                      |
| TC-02 | Rolling window aggregates                | ✅                  | ✅ (SQL)    | `expected/tc02_rolling.txt`                   |
| TC-03 | Temporal slice (half-open interval)      | ✅                  | ✅ (SQL)    | `expected/tc03_temporal_slice.txt`            |
| TC-04 | Graph traversal (neighbors / k-hop)      | ✅                  | ✅ (SQL)    | `expected/tc04_graph_traversal.txt`           |
| TC-05 | Temporal pattern matching                | ✅                  | —           | `expected/tc05_pattern.txt`                   |
| TC-06 | Vector K-NN with bitmask filter          | ✅                  | —           | `expected/tc06_knn.txt`                       |
| TC-07 | Temporal similarity search (TSS)         | ✅                  | ✅ (SQL)    | `expected/tc07_tss.txt`                       |
| TC-08 | Parquet persistence + time-travel        | ✅                  | ✅ (SQL)    | `expected/tc08_tt_save_load.txt`              |
| TC-09 | Distributed SQL dispatch (gRPC) parity   | —                   | ✅          | compare vs TC-01..04, TC-08 golden            |
| TC-10 | Arrow IPC transport integrity            | —                   | ✅          | row-count / checksum, see §6                  |
| TC-11 | Metadata-filtered K-NN (songs)           | ✅                  | ✅ (SQL)    | `expected/tc11_songs.txt`                     |

> "Runnable now" means the single-node `gtv` REPL already produces the golden
> output; "P5 required" means the capability must also be reachable through the
> distributed P5 surface (SQL over gRPC/Flight, or the vector-search RPC).

---

## 4. Detailed test cases

### TC-01 — As-of join (kdb `aj`)
- **Reference**: `TSS_non_transformed/createHDB.q` (trades aligned to a clock).
- **Input**: `scripts/tc01_asof.gtv`
  ```sql
  SELECT * FROM asof_join(0, 5, 15, 25, 35, 45, 55, 60);
  ```
- **Expected**: `expected/tc01_asof.txt`
  - `t=0→100, 5→100, 15→101, 25→99, 35→102, 45→103, 55→104, 60→104`.
  - Left times before the first right time would be `NULL` (not exercised here).
- **Pass**: output equals golden exactly.

### TC-02 — Rolling window aggregates (`mavg`/`msum`/`deltas`)
- **Reference**: `TSS_non_transformed` technical-analysis cells.
- **Input**: `scripts/tc02_rolling.gtv`
- **Expected**: `expected/tc02_rolling.txt`
  - `mavg[3] = [100.0, 100.5, 100.0, 100.666…, 101.333…, 103.0]` (kdb prefix-average
    semantics for the first `n-1` rows).
  - `msum[3] = [100.0, 201.0, 300.0, 302.0, 304.0, 309.0]`.
  - `deltas = [100.0, 1.0, -2.0, 3.0, 1.0, 1.0]`.
  - Same values via `OVER (ORDER BY t)` WindowUDFs.
- **Pass**: output equals golden exactly.

### TC-03 — Temporal slice (half-open interval)
- **Reference**: kdb HDB `as-of` partition reads.
- **Input**: `scripts/tc03_temporal_slice.gtv`
- **Expected**: `expected/tc03_temporal_slice.txt`
  - At `T=150` → `(0,2), (1,4), (2,5), (3,5)`.
  - At `T=0`   → `(0,1), (1,3), (2,5)`.
  - At `T=100` → `(0,2), (1,4), (2,5)`.
  - `valid_to` is exclusive: edge `0→1` `[0,100)` is absent at `T=100`.
- **Pass**: row sets equal golden (order-insensitive).

### TC-04 — Graph traversal
- **Reference**: gtvdb `G` tier; money-transfer graph.
- **Input**: `scripts/tc04_graph_traversal.gtv`
- **Expected**: `expected/tc04_graph_traversal.txt`
  - `neighbors 0 0` → `[1]`; `neighbors 0 100` → `[2]` (0→1 expired at 100).
  - `neighbors 3 399` → `[5]`; `neighbors 3 400` → `[]` (exclusive `valid_to`).
  - `khop 0 2 0` → hop1 `[1]`, hop2 `[3]`.
  - `SELECT * FROM neighbors(0,100)` returns `(0,2)` with `Int64` timestamps.
- **Pass**: output equals golden exactly.

### TC-05 — Temporal pattern matching
- **Reference**: `pattern_matching` (sensor windows) + `TSS_transformed` pattern cells.
- **Input**: `scripts/tc05_pattern.gtv` (uses the 4-node `transfers` graph).
- **Expected**: `expected/tc05_pattern.txt`
  - At `T=500`:
    - `ring(4)`  → 1 match `[0,1,2,3]`.
    - `path(3)`  → 2 matches `[0,1,2,3]`, `[1,2,3,0]`.
    - `diamond`  → 2 matches `[0,1,2,3]`, `[0,2,1,3]`.
  - At `T=1500` (all edges expired) → 0 matches each.
- **Pass**: match counts + node bindings equal golden (order-insensitive).

### TC-06 — Vector K-NN with bitmask filter
- **Reference**: `music_recommendation` / `metadata_filtering`.
- **Input**: `scripts/tc06_knn.gtv` (6 demo embeddings, 4-dim)
- **Expected**: `expected/tc06_knn.txt`
  - `knn 0 3` → `[0, 1, 2]` (exact nearest of node 0).
  - `knn 0 3 --mask 2,3,4,5` → `[2, 4, 3]` (nodes 0,1 pruned by the mask).
- **Pass**: output equals golden (top-1 MUST equal; full ordering should match on
  this small deterministic build).

### TC-07 — Temporal similarity search (TSS)
- **Reference**: `TSS_non_transformed` + `TSS_transformed` (sliding window →
  normalize → L2 search).
- **Data**: `data/generated/tss_series.csv` — 4 reference windows (6 dims) and one
  noisy-uptrend query; registered as the `tss` K-NN collection.
- **Input**: `scripts/tc07_tss.gtv`
  ```sql
  SELECT id FROM knn('tss', '1.1,2.0,3.1,4.0,5.1,6.0', 4);
  SELECT id FROM knn('tss', '1.1,2.0,3.1,4.0,5.1,6.0', 1);
  ```
- **Expected**: `expected/tc07_tss.txt`
  - `knn(q=noisy_uptrend, k=4) = [0, 2, 1, 3]` (uptrend, flat, dip, downtrend).
  - `knn(q=noisy_uptrend, k=1) = [0]`.
- **Pass**: top-1 equals `0`; top-4 equals `{0,2,1,3}` in that order.

### TC-08 — Parquet persistence + time-travel store
- **Reference**: `qFlat` / `qHnsw` on-disk indexes; HDB snapshot reads.
- **Input**: `scripts/tc08_tt_save_load.gtv`
- **Expected**: `expected/tc08_tt_save_load.txt`
  - `tt edges 0/100/200` → 3 active edges each (per the half-open filter).
  - `save prices …` → `load prices2 …` → `SELECT * FROM prices2` reproduces `prices`.
- **Pass**: output equals golden; loaded table row count = 6 and values match.

### TC-09 — Distributed SQL dispatch parity (P5 gRPC)
- **Goal**: any SQL query routed through the P5 `QueryService.Execute` RPC returns
  the **same result** as the single-node golden for TC-01..04 and TC-08.
- **Setup**: 1 coordinator + `N ≥ 2` workers. Tables `nodes`/`edges`/`prices` are
  registered (sharded or replicated) across workers.
- **Procedure**: for each SQL statement in `scripts/tc0*.gtv`, send it via
  `QueryService.Execute(QueryRequest{ sql })` (helper: `gtv_proto::query_remote`),
  decode the Arrow IPC response, and diff against the corresponding golden.
- **Pass**: 100% parity. Un-ordered queries are compared after `ORDER BY` (or as
  multisets); ordered queries must match exactly.

### TC-10 — Arrow IPC transport integrity (P5)
- **Goal**: result batches round-trip over the gRPC `QueryResponse.arrow_ipc`
  (Arrow IPC stream, the Flight wire format) without truncation or corruption,
  and shard results merge correctly.
- **Procedure**:
  1. Generate a table of ≥ 1M rows (e.g., cross join of `prices × prices`).
  2. Execute it via `Execute` and decode the `arrow_ipc` bytes.
  3. Assert: schema matches; total row count == expected; a column checksum
     (e.g., `SUM(t)` / `SUM(price)`) matches the single-node computation.
- **Pass**: row count + checksum match; decode succeeds without error.
- **Note**: if the P5 coordinator later upgrades to true Arrow Flight `DoGet`
  streaming (rather than a single IPC payload), this test is unchanged in intent —
  it still verifies lossless transport of large result sets.

### TC-11 — Metadata-filtered K-NN on songs
- **Reference**: `music_recommendation` (genre filter over embeddings).
- **Data**: `data/generated/songs.csv` (10 songs, 2-dim embeddings + genre);
  registered as the `songs` K-NN collection with a genre label column.
- **Input**: `scripts/tc11_songs.gtv`
  ```sql
  SELECT id FROM knn('songs', '0.1,0.1', 3);
  SELECT id FROM knn('songs', '0.1,0.1', 3, 'pop');
  SELECT id FROM knn('songs', '5.0,5.0', 3, 'rock');
  ```
- **Expected**: `expected/tc11_songs.txt`
  - `knn(q=[0.1,0.1], k=3, no filter) = [8, 0, 1]`.
  - `knn(q=[0.1,0.1], k=3, mask=pop)  = [8, 0, 1]`.
  - `knn(q=[5.0,5.0], k=3, mask=rock) = [2, 9, 3]`.
- **Pass**: results equal oracle (same tie-break: ascending id).

---

## 5. P5 surface (contract the tests target)

The P5 gRPC contract lives in `crates/gtv-proto/proto/gtvquery.proto`:

```proto
service QueryService {
  rpc Execute (QueryRequest) returns (QueryResponse);
}

message QueryRequest { string sql = 1; }

message QueryResponse {
  bytes arrow_ipc = 1;   // Arrow IPC stream encoding the result RecordBatches
  string error = 2;      // non-empty on failure
}
```

`gtv_proto::encode_batches` / `decode_batches` serialize Arrow `RecordBatch`es
using the Arrow IPC stream format (the Flight wire format), and
`gtv_proto::query_remote(addr, sql)` is the client helper. The SQL-only tests
(TC-01..04, TC-08, TC-09, TC-10) target this service directly.

**Vector search** (TC-07 / TC-11) is exposed as a SQL table function registered
in the server's DataFusion context (`GtvContext::register_knn`):

```sql
SELECT id, distance FROM knn('songs', '0.1,0.1', 3);           -- no filter
SELECT id, distance FROM knn('songs', '0.1,0.1', 3, 'pop');    -- metadata filter
SELECT id, distance FROM knn('tss', '1.1,2.0,3.1,4.0,5.1,6.0', 4);
```

It performs an exact brute-force L2 search over a named collection (ids +
vectors + optional per-vector label), returning `(id, distance)` rows ordered
nearest-first with ties broken by ascending id — matching the `gen_data.py`
oracle. Collections are registered by name; a `SearchKnn` RPC remains a possible
future addition but is not required for parity.

---

## 6. How to run

```sh
# 1. build the single-node binary (already done in CI)
cargo build -p gtv-cli --bin gtv

# 2. (optional) regenerate CSV data + oracles
python3 testcase/data/gen_data.py

# 3. run the REPL golden tests
./testcase/run_tests.sh
```

For P5, set the client in `run_tests.sh` (see `GTV_P5_CLIENT` hook) so the same
scripts are executed against the distributed endpoint instead of the local REPL.

---

## 7. Pass criteria summary

| TC | Criterion |
|----|-----------|
| 01, 02, 04, 05, 06 | stdout equals golden (order-insensitive where noted) |
| 03 | row set equals golden at each `T` |
| 07, 11 | K-NN ranking equals oracle (top-1 always exact) |
| 08 | persistence round-trip reproduces source table |
| 09 | distributed SQL ≡ single-node golden |
| 10 | row count + column checksum match |
