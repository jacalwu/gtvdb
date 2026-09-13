//! Filesystem catalog with an atomic commit protocol.
//!
//! Layout under the catalog root:
//!
//! ```text
//! root/
//!   metadata/tables.json                     # table registry
//!   metadata/<table_id>/schemas.json         # versioned schema history
//!   metadata/<table_id>/snapshots.jsonl       # append-only snapshot log
//!   metadata/<table_id>/files.jsonl           # append-only data-file index
//!   metadata/<table_id>/manifests/<snap>.json # full manifest per snapshot
//!   metadata/<table_id>/latest                # version hint (snapshot id)
//!   data/<table_id>/<spec_ver>/<partition>/<file>.parquet
//!   tmp/                                      # scratch
//! ```
//!
//! Commit protocol: write each Parquet file to a `*.tmp`, fsync, checksum,
//! atomic-rename, fsync the directory; then write the manifest atomically,
//! append the snapshot log and finally update `latest`. Readers only ever open
//! committed manifests, so a half-written batch is never visible.
//!
//! Single-writer assumption: the catalog does not take inter-process locks
//! (B2-1 MVP; distributed ownership is a P3 concern).

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use gtv_storage::{write_atomic, write_batch_atomic};

use crate::dq::{GateDecisionRecord, OverrideRecord};
use crate::error::{CatalogError, Result};
use crate::id::{CommitId, DataFileId, SnapshotId, TableId};
use crate::lineage::{ExecutionId, ExecutionRecord};
use crate::manifest::{CommitOp, DataFile, FileFormat, Snapshot};
use crate::partition::{partition_dir, PartitionSpec, PartitionValue};
use crate::schema::{
    apply_change, check_compatible, now_ns, SchemaChange, SchemaRecord, SchemaVersion,
};
use crate::stats::column_stats;

/// A table's registry entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMeta {
    pub table_id: TableId,
    pub name: String,
    pub created_at: i64,
    pub schema_version: u32,
    pub spec: PartitionSpec,
    /// Default event-time column used for file bounding boxes / pruning.
    #[serde(default)]
    pub event_time_column: Option<String>,
    /// Rename history (`old -> new`) applied when reading historical files.
    #[serde(default)]
    pub renames: BTreeMap<String, String>,
}

/// A batch to be written as one data file.
pub struct NewFile {
    pub batch: RecordBatch,
    pub partition: Vec<PartitionValue>,
}

impl NewFile {
    /// An unpartitioned data file.
    pub fn unpartitioned(batch: RecordBatch) -> Self {
        Self {
            batch,
            partition: Vec::new(),
        }
    }
}

/// Options for [`FsCatalog::commit`].
#[derive(Debug, Clone, Default)]
pub struct CommitOptions {
    /// Replay guard: committing the same key twice returns the original snapshot.
    pub idempotency_key: Option<String>,
    /// Override the table's event-time column for this commit.
    pub event_time_column: Option<String>,
}

/// Predicate used to prune data files by event time.
#[derive(Debug, Clone, Default)]
pub struct ScanFilter {
    pub event_time_min: Option<i64>,
    pub event_time_max: Option<i64>,
}

impl ScanFilter {
    fn matches(&self, file: &DataFile) -> bool {
        if let Some(lo) = self.event_time_min {
            if file.event_time_max < lo {
                return false;
            }
        }
        if let Some(hi) = self.event_time_max {
            if file.event_time_min > hi {
                return false;
            }
        }
        true
    }
}

/// A filesystem-backed catalog.
#[derive(Debug, Clone)]
pub struct FsCatalog {
    root: PathBuf,
}

impl FsCatalog {
    /// Open (or create) a catalog rooted at `root`.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        fs::create_dir_all(root.join("metadata"))?;
        fs::create_dir_all(root.join("data"))?;
        fs::create_dir_all(root.join("tmp"))?;
        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    // -- paths --------------------------------------------------------------

    fn tables_path(&self) -> PathBuf {
        self.root.join("metadata/tables.json")
    }
    fn table_dir(&self, table: TableId) -> PathBuf {
        self.root.join("metadata").join(table.to_string())
    }
    fn schemas_path(&self, table: TableId) -> PathBuf {
        self.table_dir(table).join("schemas.json")
    }
    fn snapshots_path(&self, table: TableId) -> PathBuf {
        self.table_dir(table).join("snapshots.jsonl")
    }
    fn files_path(&self, table: TableId) -> PathBuf {
        self.table_dir(table).join("files.jsonl")
    }
    fn manifest_path(&self, table: TableId, snapshot: SnapshotId) -> PathBuf {
        self.table_dir(table)
            .join("manifests")
            .join(format!("{snapshot}.json"))
    }
    fn latest_path(&self, table: TableId) -> PathBuf {
        self.table_dir(table).join("latest")
    }
    fn lineage_path(&self) -> PathBuf {
        self.root.join("metadata/lineage.jsonl")
    }
    fn dq_decisions_path(&self) -> PathBuf {
        self.root.join("metadata/dq_decisions.jsonl")
    }
    fn dq_overrides_path(&self) -> PathBuf {
        self.root.join("metadata/dq_overrides.jsonl")
    }

    // -- generic io ---------------------------------------------------------

    fn read_json<T: DeserializeOwned>(&self, path: &Path) -> Result<Option<T>> {
        match fs::read(path) {
            Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn write_json<T: Serialize>(&self, path: &Path, value: &T) -> Result<()> {
        let bytes = serde_json::to_vec_pretty(value)?;
        write_atomic(path, &bytes)?;
        Ok(())
    }

    fn append_jsonl<T: Serialize>(&self, path: &Path, value: &T) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        serde_json::to_writer(&mut file, value)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        Ok(())
    }

    fn read_jsonl<T: DeserializeOwned>(&self, path: &Path) -> Result<Vec<T>> {
        let Ok(text) = fs::read_to_string(path) else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        for line in text.lines() {
            if line.trim().is_empty() {
                continue;
            }
            out.push(serde_json::from_str(line)?);
        }
        Ok(out)
    }

    // -- table registry -----------------------------------------------------

    fn load_tables(&self) -> Result<Vec<TableMeta>> {
        Ok(self.read_json(&self.tables_path())?.unwrap_or_default())
    }

    fn save_tables(&self, tables: &[TableMeta]) -> Result<()> {
        self.write_json(&self.tables_path(), &tables)
    }

    /// Create a new table with schema version 1.
    pub fn create_table(
        &self,
        name: &str,
        schema: SchemaRef,
        spec: PartitionSpec,
    ) -> Result<TableId> {
        let mut tables = self.load_tables()?;
        if tables.iter().any(|t| t.name == name) {
            return Err(CatalogError::TableExists(name.to_string()));
        }
        let table_id = TableId::new();
        let meta = TableMeta {
            table_id,
            name: name.to_string(),
            created_at: now_ns(),
            schema_version: 1,
            spec,
            event_time_column: None,
            renames: BTreeMap::new(),
        };
        fs::create_dir_all(self.table_dir(table_id))?;
        let record = SchemaRecord::new(1, &schema, None)?;
        self.write_json(&self.schemas_path(table_id), &vec![record])?;
        tables.push(meta);
        self.save_tables(&tables)?;
        Ok(table_id)
    }

    /// Look a table up by name.
    pub fn table(&self, name: &str) -> Result<TableMeta> {
        self.load_tables()?
            .into_iter()
            .find(|t| t.name == name)
            .ok_or_else(|| CatalogError::TableNotFound(name.to_string()))
    }

    /// Look a table up by id.
    pub fn table_by_id(&self, table: TableId) -> Result<TableMeta> {
        self.load_tables()?
            .into_iter()
            .find(|t| t.table_id == table)
            .ok_or_else(|| CatalogError::TableNotFound(table.to_string()))
    }

    /// Update the event-time column used for a table's file bounding boxes.
    pub fn set_event_time_column(&self, table: TableId, column: Option<String>) -> Result<()> {
        let mut tables = self.load_tables()?;
        let meta = tables
            .iter_mut()
            .find(|t| t.table_id == table)
            .ok_or_else(|| CatalogError::TableNotFound(table.to_string()))?;
        meta.event_time_column = column;
        self.save_tables(&tables)
    }

    /// All registered tables.
    pub fn list_tables(&self) -> Result<Vec<TableMeta>> {
        self.load_tables()
    }

    /// All registered table names.
    pub fn table_names(&self) -> Result<Vec<String>> {
        Ok(self.load_tables()?.into_iter().map(|t| t.name).collect())
    }

    /// Register an existing external CSV/Parquet file as a table (or replace the
    /// current version of it). The file is never copied, so the source stays
    /// authoritative; the schema is inferred from the file.
    pub fn register_external(
        &self,
        name: &str,
        path: &str,
        format: FileFormat,
    ) -> Result<TableId> {
        let schema = match format {
            FileFormat::Csv => gtv_storage::infer_csv_schema(path, 1024)?,
            FileFormat::Parquet => gtv_storage::parquet_schema(path)?,
        };
        let table = match self.table(name) {
            Ok(t) => t.table_id,
            Err(CatalogError::TableNotFound(_)) => {
                self.create_table(name, schema.clone(), PartitionSpec::single())?
            }
            Err(e) => return Err(e),
        };
        let meta = self.table_by_id(table)?;

        let file = DataFile {
            file_id: DataFileId::new(),
            path: path.to_string(),
            format,
            managed: false,
            row_count: 0,
            size_bytes: fs::metadata(path).map(|m| m.len()).unwrap_or(0),
            column_stats: Vec::new(),
            event_time_min: i64::MIN,
            event_time_max: i64::MAX,
            schema_version: SchemaVersion(meta.schema_version),
            partition: Vec::new(),
            checksum: String::new(),
            source_offsets: Vec::new(),
            commit_id: CommitId::new(),
        };
        let mut summary = serde_json::json!({});
        summary["external"] = serde_json::Value::Bool(true);
        let snapshot = Snapshot {
            snapshot_id: SnapshotId::new(),
            parent: self.latest(table)?,
            table_id: table,
            schema_version: SchemaVersion(meta.schema_version),
            spec_version: meta.spec.version,
            files: vec![file.file_id],
            op: CommitOp::Overwrite,
            summary,
            created_at: now_ns(),
        };
        self.append_jsonl(&self.files_path(table), &file)?;
        self.write_json(&self.manifest_path(table, snapshot.snapshot_id), &snapshot)?;
        self.append_jsonl(&self.snapshots_path(table), &snapshot)?;
        write_atomic(
            &self.latest_path(table),
            snapshot.snapshot_id.to_string().as_bytes(),
        )?;
        Ok(table)
    }

    /// Remove a table from the registry. The metadata directory is deleted;
    /// managed data files are left on disk (they may be shared by snapshots).
    pub fn drop_table(&self, name: &str) -> Result<Option<TableMeta>> {
        let mut tables = self.load_tables()?;
        let Some(pos) = tables.iter().position(|t| t.name == name) else {
            return Ok(None);
        };
        let meta = tables.remove(pos);
        self.save_tables(&tables)?;
        let _ = fs::remove_dir_all(self.table_dir(meta.table_id));
        Ok(Some(meta))
    }

    // -- lineage ------------------------------------------------------------

    /// Append an execution record to the append-only lineage log.
    pub fn append_lineage(&self, record: &ExecutionRecord) -> Result<()> {
        self.append_jsonl(&self.lineage_path(), record)
    }

    /// Look up one execution record by id.
    pub fn lineage(&self, id: ExecutionId) -> Result<Option<ExecutionRecord>> {
        Ok(self
            .read_jsonl::<ExecutionRecord>(&self.lineage_path())?
            .into_iter()
            .find(|r| r.execution_id == id))
    }

    /// Every recorded execution, oldest first.
    pub fn lineage_records(&self) -> Result<Vec<ExecutionRecord>> {
        self.read_jsonl(&self.lineage_path())
    }

    // -- data-quality gate ledger -------------------------------------------

    /// Append a publish-gate decision to the append-only DQ ledger.
    pub fn append_gate_decision(&self, record: &GateDecisionRecord) -> Result<()> {
        self.append_jsonl(&self.dq_decisions_path(), record)
    }

    /// Every recorded gate decision, oldest first.
    pub fn gate_decisions(&self) -> Result<Vec<GateDecisionRecord>> {
        self.read_jsonl(&self.dq_decisions_path())
    }

    /// Append an override to the append-only override ledger.
    pub fn append_override(&self, record: &OverrideRecord) -> Result<()> {
        self.append_jsonl(&self.dq_overrides_path(), record)
    }

    /// Every recorded override, oldest first.
    pub fn overrides(&self) -> Result<Vec<OverrideRecord>> {
        self.read_jsonl(&self.dq_overrides_path())
    }

    // -- schemas ------------------------------------------------------------

    /// Fetch a specific schema version.
    pub fn schema(&self, table: TableId, version: SchemaVersion) -> Result<SchemaRef> {
        let records: Vec<SchemaRecord> = self
            .read_json(&self.schemas_path(table))?
            .ok_or_else(|| CatalogError::TableNotFound(table.to_string()))?;
        records
            .into_iter()
            .find(|r| r.version == version)
            .ok_or_else(|| {
                CatalogError::Corrupt(format!("schema version {version:?} not found"))
            })?
            .arrow()
    }

    /// Evolve the schema, validating compatibility with the current version.
    pub fn evolve_schema(&self, table: TableId, change: SchemaChange) -> Result<SchemaVersion> {
        let mut tables = self.load_tables()?;
        let meta = tables
            .iter_mut()
            .find(|t| t.table_id == table)
            .ok_or_else(|| CatalogError::TableNotFound(table.to_string()))?;

        let mut records: Vec<SchemaRecord> = self
            .read_json(&self.schemas_path(table))?
            .unwrap_or_default();
        let current = records
            .last()
            .ok_or_else(|| CatalogError::Corrupt("no schema versions".into()))?;
        let current_schema = current.arrow()?;
        let (new_schema, rename) = apply_change(&current_schema, &change)?;
        check_compatible(&current_schema, &new_schema)?;

        let next_version = current.version.0 + 1;
        let mut record = SchemaRecord::new(next_version, &new_schema, None)?;
        record.renames = current.renames.clone();
        if let Some((from, to)) = rename {
            record.renames.insert(from, to);
        }
        meta.renames = record.renames.clone();
        meta.schema_version = next_version;
        records.push(record);
        self.write_json(&self.schemas_path(table), &records)?;
        self.save_tables(&tables)?;
        Ok(SchemaVersion(next_version))
    }

    // -- snapshots ----------------------------------------------------------

    /// The latest committed snapshot id, if any.
    pub fn latest(&self, table: TableId) -> Result<Option<SnapshotId>> {
        match fs::read_to_string(self.latest_path(table)) {
            Ok(s) => s
                .trim()
                .parse::<SnapshotId>()
                .map(Some)
                .map_err(|_| CatalogError::Corrupt("bad version hint".into())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Load a committed snapshot manifest.
    pub fn snapshot(&self, table: TableId, snapshot: SnapshotId) -> Result<Snapshot> {
        self.read_json(&self.manifest_path(table, snapshot))?
            .ok_or_else(|| CatalogError::SnapshotNotFound(snapshot.to_string()))
    }

    /// Resolve the full data-file records of a snapshot.
    pub fn files(&self, table: TableId, snapshot: SnapshotId) -> Result<Vec<DataFile>> {
        let snap = self.snapshot(table, snapshot)?;
        let index: HashMap<DataFileId, DataFile> = self
            .read_jsonl::<DataFile>(&self.files_path(table))?
            .into_iter()
            .map(|f| (f.file_id, f))
            .collect();
        let mut out = Vec::with_capacity(snap.files.len());
        for id in &snap.files {
            let file = index
                .get(id)
                .ok_or_else(|| CatalogError::Corrupt(format!("missing data file {id}")))?
                .clone();
            out.push(file);
        }
        Ok(out)
    }

    /// Files of a snapshot that overlap the event-time filter.
    pub fn scan(
        &self,
        table: TableId,
        snapshot: SnapshotId,
        filter: &ScanFilter,
    ) -> Result<Vec<DataFile>> {
        Ok(self
            .files(table, snapshot)?
            .into_iter()
            .filter(|f| filter.matches(f))
            .collect())
    }

    /// All committed snapshots of a table, oldest first (append-only log order).
    pub fn snapshots(&self, table: TableId) -> Result<Vec<Snapshot>> {
        self.read_jsonl(&self.snapshots_path(table))
    }

    /// The newest snapshot committed at or before `system_ts` (system time).
    ///
    /// This is the catalog half of `AS OF SYSTEM TIME`: system time is versioned
    /// by immutable snapshots, so the returned id pins exactly what the system
    /// knew at `system_ts`.
    pub fn snapshot_as_of(&self, table: TableId, system_ts: i64) -> Result<Option<SnapshotId>> {
        let mut best: Option<Snapshot> = None;
        for snap in self.snapshots(table)? {
            if snap.created_at > system_ts {
                continue;
            }
            if best.as_ref().map_or(true, |b| snap.created_at >= b.created_at) {
                best = Some(snap);
            }
        }
        Ok(best.map(|s| s.snapshot_id))
    }

    fn find_by_key(&self, table: TableId, key: &str) -> Result<Option<SnapshotId>> {
        for snap in self.read_jsonl::<Snapshot>(&self.snapshots_path(table))? {
            if snap.summary.get("idempotency_key").and_then(|v| v.as_str()) == Some(key) {
                return Ok(Some(snap.snapshot_id));
            }
        }
        Ok(None)
    }

    // -- commit -------------------------------------------------------------

    /// Atomically commit `files` to `table`.
    pub fn commit(
        &self,
        table: TableId,
        op: CommitOp,
        files: Vec<NewFile>,
        opts: &CommitOptions,
    ) -> Result<SnapshotId> {
        let meta = self.table_by_id(table)?;

        if let Some(key) = &opts.idempotency_key {
            if let Some(existing) = self.find_by_key(table, key)? {
                return Ok(existing);
            }
        }

        let parent = self.latest(table)?;
        let parent_snapshot = match parent {
            Some(id) => Some(self.snapshot(table, id)?),
            None => None,
        };
        let commit_id = CommitId::new();

        // 1. write data files (each atomically).
        let mut written = Vec::with_capacity(files.len());
        for nf in &files {
            written.push(self.write_data_file(table, &meta, nf, commit_id, opts)?);
        }
        for df in &written {
            self.append_jsonl(&self.files_path(table), df)?;
        }

        // 2. assemble the new snapshot.
        let mut file_ids: Vec<DataFileId> = match op {
            CommitOp::Append => parent_snapshot
                .as_ref()
                .map(|s| s.files.clone())
                .unwrap_or_default(),
            CommitOp::Overwrite | CommitOp::Delete => Vec::new(),
        };
        file_ids.extend(written.iter().map(|d| d.file_id));
        if op == CommitOp::Delete {
            file_ids.clear();
        }

        let mut summary = serde_json::json!({});
        if let Some(key) = &opts.idempotency_key {
            summary["idempotency_key"] = serde_json::Value::String(key.clone());
        }
        let snapshot = Snapshot {
            snapshot_id: SnapshotId::new(),
            parent,
            table_id: table,
            schema_version: SchemaVersion(meta.schema_version),
            spec_version: meta.spec.version,
            files: file_ids,
            op,
            summary,
            created_at: now_ns(),
        };

        // 3. manifest → snapshot log → version hint.
        self.write_json(&self.manifest_path(table, snapshot.snapshot_id), &snapshot)?;
        self.append_jsonl(&self.snapshots_path(table), &snapshot)?;
        write_atomic(
            &self.latest_path(table),
            snapshot.snapshot_id.to_string().as_bytes(),
        )?;
        Ok(snapshot.snapshot_id)
    }

    fn write_data_file(
        &self,
        table: TableId,
        meta: &TableMeta,
        nf: &NewFile,
        commit_id: CommitId,
        opts: &CommitOptions,
    ) -> Result<DataFile> {
        let file_id = DataFileId::new();
        let dir = self
            .root
            .join("data")
            .join(table.to_string())
            .join(meta.spec.version.to_string())
            .join(partition_dir(&nf.partition));
        let path = dir.join(format!("{file_id}.parquet"));
        write_batch_atomic(&path, &nf.batch)?;

        let bytes = fs::read(&path)?;
        let checksum = blake3::hash(&bytes).to_hex().to_string();
        let size_bytes = bytes.len() as u64;
        let event_col = opts
            .event_time_column
            .as_deref()
            .or(meta.event_time_column.as_deref());
        let (event_time_min, event_time_max) = event_bounds(&nf.batch, event_col);

        Ok(DataFile {
            file_id,
            path: path.to_string_lossy().into_owned(),
            format: FileFormat::Parquet,
            managed: true,
            row_count: nf.batch.num_rows() as u64,
            size_bytes,
            column_stats: column_stats(&nf.batch),
            event_time_min,
            event_time_max,
            schema_version: SchemaVersion(meta.schema_version),
            partition: nf.partition.clone(),
            checksum,
            source_offsets: Vec::new(),
            commit_id,
        })
    }
}

/// Event-time bounding box of `batch` using `column` (Int64 / UInt64 /
/// Timestamp(ns)). Falls back to an unbounded box.
fn event_bounds(batch: &RecordBatch, column: Option<&str>) -> (i64, i64) {
    let Some(name) = column else {
        return (i64::MIN, i64::MAX);
    };
    let Ok(idx) = batch.schema().index_of(name) else {
        return (i64::MIN, i64::MAX);
    };
    let arr = batch.column(idx);
    let stat = column_stats(batch)
        .into_iter()
        .find(|s| s.name == name)
        .unwrap_or(crate::manifest::ColumnStat {
            name: name.to_string(),
            null_count: arr.null_count() as u64,
            min: None,
            max: None,
            distinct_est: None,
        });
    let to_i64 = |s: &Option<crate::manifest::Scalar>| match s {
        Some(crate::manifest::Scalar::Int(v)) => Some(*v),
        Some(crate::manifest::Scalar::UInt(v)) => Some(*v as i64),
        _ => None,
    };
    (
        to_i64(&stat.min).unwrap_or(i64::MIN),
        to_i64(&stat.max).unwrap_or(i64::MAX),
    )
}
