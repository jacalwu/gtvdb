# GTV Catalog & Index Store — on-disk schema (prod_p2)

Metadata control plane introduced in batch 2 (`gtv-catalog`, `gtv-index-store`).
All identities are UUIDs; all logs are append-only JSONL; every published object
is immutable. Root is `$GTV_HOME` (catalog) and the `--root` passed to
`index_save` / `embedding_build` (index store).

## 1. Directory layout

```text
$GTV_HOME/
  metadata/
    tables.json                     # registry: name -> TableMeta
    <table_id>/
      schemas.json                  # schema version history (SchemaRecord[])
      snapshots.jsonl               # committed Snapshot log (append-only)
      files.jsonl                   # DataFile log (append-only)
      manifests/<snapshot_id>.json  # immutable manifest for one snapshot
      latest                        # version hint: current SnapshotId (atomic)
    lineage.jsonl                   # ExecutionRecord log (B2-3)
    dq_decisions.jsonl              # GateDecisionRecord log (B2-5)
    dq_overrides.jsonl              # OverrideRecord log (B2-5)
  data/<table_id>/<spec_version>/<partition>/<file_id>.parquet
  tmp/                              # staging for atomic writes
```

Index store (`--root`):

```text
<root>/
  indexes.json                      # name -> IndexId
  <index_id>/
    CURRENT                         # active version number (atomic)
    v<n>/index.gtvidx               # container: magic + version + manifest + payload
    v<n>/manifest.json              # IndexManifest
```

## 2. Catalog records

### TableMeta (`tables.json`)
`table_id`, `name`, `created_at`, `schema_version`, `spec` (PartitionSpec),
`event_time_column`, `renames` (applied to historical files).

### Schema registry (`schemas.json`)
Ordered `SchemaRecord[]`: each has `version`, Arrow `schema`, and a rename map.
Compatibility is enforced on `evolve_schema` (add column / rename / widening
only); incompatible evolution is rejected with `SchemaIncompatible`.

### DataFile (`files.jsonl`)

| field | meaning |
|---|---|
| `file_id` | immutable file identity |
| `path` | absolute path (managed Parquet or external CSV/Parquet ref) |
| `format` | `parquet` / `csv` |
| `managed` | `true` when the catalog wrote it |
| `row_count`, `size_bytes` | physical stats |
| `column_stats` | per-column `null_count` / `min` / `max` / `distinct_est` |
| `event_time_min/max` | event-time bounding box for pruning |
| `schema_version` | schema in force when written |
| `partition` | partition values (identity / date_trunc / hash_bucket) |
| `checksum` | blake3 (hex) of the file bytes |
| `source_offsets` | upstream position (streaming/CDC) |
| `commit_id` | commit that produced the file |

### Snapshot (`manifests/<snapshot_id>.json`, mirrored to `snapshots.jsonl`)
`snapshot_id`, `parent`, `table_id`, `schema_version`, `spec_version`, `files`
(list of DataFileId), `op` (`append` / `overwrite` / `delete`), `summary`
(includes `idempotency_key`), `created_at`.

## 3. Commit protocol (crash-safe)

1. Write each data file atomically (`tmp` → `fsync` → rename).
2. Append its `DataFile` record to `files.jsonl`.
3. Write the snapshot manifest atomically.
4. Append the `Snapshot` to `snapshots.jsonl`.
5. Atomically update `latest` (the **publish point**).

Readers resolve `latest → manifest → files`, so a crash at any point before
step 5 leaves the previous committed snapshot visible; orphan files/manifests
are never reached. `commit` is idempotent when `idempotency_key` is set
(re-commit returns the existing `SnapshotId`).

## 4. Index store records

### IndexManifest (`v<n>/manifest.json`)
`index_id`, `table_id`, `index_type` (`flat`/`ivf`/`hnsw`),
`corpus_snapshot_id`, `source_file_ids`, `model_id`, `model_version`,
`embedding_model`, `feature_version`, `normalized`, `dim`, `metric`,
`build_params`, `build_ts`, `checksum`, `engine_version`, `row_count`,
`tombstone_count`.

### Container (`index.gtvidx`)
`magic | version | manifest_json_len | manifest_json | payload`. The payload is
the versioned binary encoding from `gtv-index` (HNSW flat buffers, IVF
centroids/list offsets, or Flat ids+data). `load` verifies the container and
payload blake3 checksums and refuses corrupted files. `CURRENT` is swapped
atomically, so shadow builds never disturb live readers and `rollback` is just
an activation of an older version.

## 5. Ledgers queried from SQL / CLI

| ledger | record | surface |
|---|---|---|
| `lineage.jsonl` | `ExecutionRecord` | `lineage` / `lineage_show` / `gtv_lineage` table |
| `dq_decisions.jsonl` | `GateDecisionRecord` | `dq_audit` |
| `dq_overrides.jsonl` | `OverrideRecord` | `dq_audit` |
