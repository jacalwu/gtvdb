//! Table catalog: persistence of registered tables across REPL restarts.
//!
//! Enabled by setting `GTV_HOME` to a directory. Since B2-1 this is a thin
//! façade over [`gtv_catalog::FsCatalog`]: the old `catalog.tsv` TSV manifest is
//! replaced by versioned table/snapshot/manifest metadata with an atomic commit
//! protocol. Existing `catalog.tsv` files are imported once, on first open.
//!
//! Two kinds of persistence are supported:
//!
//! * **external reference** (`csv` / `parquet`) — the table's rows come from an
//!   existing file on disk; the source stays authoritative and is re-read on
//!   restart. Schema is inferred from the file.
//! * **managed snapshot** (`snapshot`) — the catalog owns the Parquet file
//!   (e.g. `CREATE TABLE … AS SELECT …`), written under `<home>/data/`.
//!
//! Loads on startup are best-effort: a missing source file logs a warning and is
//! skipped rather than aborting the session.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context as _, Result};
use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use chrono::Local;
use gtv_catalog::{
    CommitOp, CommitOptions, FileFormat, FsCatalog, NewFile, PartitionSpec, TableMeta, TableRef,
};
use gtv_engine::GtvContext;

/// Where a persisted table's rows come from on the next startup.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Kind {
    Csv,
    Parquet,
    Snapshot,
}

/// One persisted table (a display view over the catalog metadata).
#[derive(Clone, Debug)]
pub struct Entry {
    pub name: String,
    pub kind: Kind,
    pub path: PathBuf,
    pub rows: u64,
    pub created: String,
}

/// A persisted-table catalog rooted at a directory.
#[derive(Debug)]
pub struct Catalog {
    home: PathBuf,
    fs: FsCatalog,
    entries: BTreeMap<String, Entry>,
}

const LEGACY_MANIFEST: &str = "catalog.tsv";

fn fmt_time(ns: i64) -> String {
    chrono::DateTime::from_timestamp_nanos(ns)
        .with_timezone(&Local)
        .format("%Y-%m-%dT%H:%M:%S")
        .to_string()
}

fn abs_path(p: &str) -> PathBuf {
    let path = Path::new(p);
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map(|cwd| cwd.join(path))
            .unwrap_or_else(|_| path.to_path_buf())
    }
}

impl Catalog {
    /// Open (or create) the catalog under `home`. `home` must be writable.
    pub fn open(home: &Path) -> Result<Catalog> {
        fs::create_dir_all(home)
            .with_context(|| format!("create catalog dir {}", home.display()))?;
        let fs = FsCatalog::open(home).map_err(|e| anyhow!("{e}"))?;
        let mut cat = Catalog {
            home: home.to_path_buf(),
            fs,
            entries: BTreeMap::new(),
        };
        cat.import_legacy()?;
        cat.refresh()?;
        Ok(cat)
    }

    pub fn home(&self) -> &Path {
        &self.home
    }

    /// The underlying filesystem catalog (lineage, pinned snapshot reads).
    pub fn fs(&self) -> &FsCatalog {
        &self.fs
    }

    /// Append one execution record to the catalog's lineage log.
    pub fn append_lineage(&self, record: &gtv_catalog::ExecutionRecord) -> Result<()> {
        self.fs.append_lineage(record).map_err(|e| anyhow!("{e}"))
    }

    /// Fetch one execution record by id.
    pub fn lineage(&self, id: gtv_catalog::ExecutionId) -> Result<Option<gtv_catalog::ExecutionRecord>> {
        self.fs.lineage(id).map_err(|e| anyhow!("{e}"))
    }

    /// Every recorded execution, oldest first.
    pub fn lineage_records(&self) -> Result<Vec<gtv_catalog::ExecutionRecord>> {
        self.fs.lineage_records().map_err(|e| anyhow!("{e}"))
    }

    /// The pinned catalog reference for a table's latest snapshot, if any.
    /// Used to stamp lineage records with the exact version a query read.
    pub fn table_ref(&self, name: &str) -> Result<Option<TableRef>> {
        let meta = match self.fs.table(name) {
            Ok(m) => m,
            Err(gtv_catalog::CatalogError::TableNotFound(_)) => return Ok(None),
            Err(e) => return Err(anyhow!("{e}")),
        };
        let Some(snapshot_id) = self.fs.latest(meta.table_id).map_err(|e| anyhow!("{e}"))? else {
            return Ok(None);
        };
        Ok(Some(TableRef {
            table_id: meta.table_id,
            snapshot_id,
            schema_version: meta.schema_version,
            table_name: meta.name,
        }))
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.entries.keys().map(String::as_str)
    }

    pub fn get(&self, name: &str) -> Option<&Entry> {
        self.entries.get(name)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Entry> {
        self.entries.values()
    }

    /// Record a table whose rows come from a CSV file on disk.
    pub fn record_csv(&mut self, name: &str, path: &str) -> Result<()> {
        let abs = abs_path(path);
        self.fs
            .register_external(name, abs.to_str().unwrap(), FileFormat::Csv)
            .map_err(|e| anyhow!("{e}"))?;
        self.refresh()
    }

    /// Record a table whose rows come from a Parquet file on disk.
    pub fn record_parquet(&mut self, name: &str, path: &str) -> Result<()> {
        let abs = abs_path(path);
        self.fs
            .register_external(name, abs.to_str().unwrap(), FileFormat::Parquet)
            .map_err(|e| anyhow!("{e}"))?;
        self.refresh()
    }

    /// Materialize `batches` as a managed snapshot Parquet file and record it.
    pub fn record_snapshot(
        &mut self,
        name: &str,
        schema: &SchemaRef,
        batches: &[RecordBatch],
    ) -> Result<u64> {
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        let combined = if batches.is_empty() {
            RecordBatch::new_empty(schema.clone())
        } else {
            concat_batches(schema, batches).with_context(|| format!("concat snapshot `{name}`"))?
        };

        let table = match self.fs.table(name) {
            Ok(t) => t.table_id,
            Err(gtv_catalog::CatalogError::TableNotFound(_)) => self
                .fs
                .create_table(name, schema.clone(), PartitionSpec::single())
                .map_err(|e| anyhow!("{e}"))?,
            Err(e) => return Err(anyhow!("{e}")),
        };
        let op = if self
            .fs
            .latest(table)
            .map_err(|e| anyhow!("{e}"))?
            .is_some()
        {
            CommitOp::Overwrite
        } else {
            CommitOp::Append
        };
        self.fs
            .commit(
                table,
                op,
                vec![NewFile::unpartitioned(combined)],
                &CommitOptions::default(),
            )
            .map_err(|e| anyhow!("{e}"))?;
        self.refresh()?;
        Ok(rows as u64)
    }

    /// Remove a table from the catalog. Returns the removed entry.
    pub fn remove(&mut self, name: &str) -> Result<Option<Entry>> {
        let removed = self.fs.drop_table(name).map_err(|e| anyhow!("{e}"))?;
        if removed.is_none() {
            return Ok(None);
        }
        Ok(self.entries.remove(name))
    }

    /// Replay every table into `ctx`, best-effort: tables whose source file is
    /// missing are skipped with a warning. Returns the number restored.
    pub fn replay(&self, ctx: &GtvContext) -> usize {
        let mut restored = 0usize;
        let tables = self.fs.list_tables().unwrap_or_default();
        for meta in tables {
            match self.replay_one(ctx, &meta) {
                Ok(true) => restored += 1,
                Ok(false) => eprintln!("catalog: skip `{}` — empty/unreadable", meta.name),
                Err(err) => eprintln!("catalog: skip `{}` — {err}", meta.name),
            }
        }
        restored
    }

    fn replay_one(&self, ctx: &GtvContext, meta: &TableMeta) -> Result<bool> {
        let Some(snap) = self.fs.latest(meta.table_id).map_err(|e| anyhow!("{e}"))? else {
            return Ok(false);
        };
        let files = self
            .fs
            .files(meta.table_id, snap)
            .map_err(|e| anyhow!("{e}"))?;
        let Some(first) = files.first() else {
            return Ok(false);
        };

        // Pin the snapshot this table was restored from so lineage records can
        // reference the exact version replay should use.
        let source = TableRef {
            table_id: meta.table_id,
            snapshot_id: snap,
            schema_version: meta.schema_version,
            table_name: meta.name.clone(),
        };

        if !first.managed {
            let res = match first.format {
                FileFormat::Csv => ctx.register_csv(&first.path, &meta.name),
                FileFormat::Parquet => ctx.register_parquet(&first.path, &meta.name),
            };
            if res.is_ok() {
                ctx.set_table_source(&meta.name, source);
            }
            return Ok(res.is_ok());
        }

        // Managed: concat every Parquet file of the snapshot.
        let mut schema: Option<SchemaRef> = None;
        let mut batches: Vec<RecordBatch> = Vec::new();
        for f in &files {
            let bs = gtv_storage::read_batches(&f.path)
                .map_err(|e| anyhow!("read {}: {e}", f.path))?;
            if schema.is_none() {
                schema = bs.first().map(|b| b.schema());
            }
            batches.extend(bs);
        }
        let Some(schema) = schema else {
            return Ok(false);
        };
        let ok = ctx.register_batches(&meta.name, schema, batches).is_ok();
        if ok {
            ctx.set_table_source(&meta.name, source);
        }
        Ok(ok)
    }

    /// Import a legacy `catalog.tsv` manifest once (only when the new catalog is
    /// empty). The legacy file is left in place.
    fn import_legacy(&mut self) -> Result<()> {
        if !self
            .fs
            .table_names()
            .map_err(|e| anyhow!("{e}"))?
            .is_empty()
        {
            return Ok(());
        }
        let path = self.home.join(LEGACY_MANIFEST);
        let Ok(text) = fs::read_to_string(&path) else {
            return Ok(());
        };
        let mut imported = 0usize;
        for (lineno, raw) in text.lines().enumerate() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let mut cols = line.split('\t');
            let (Some(name), Some(kind), Some(p)) = (cols.next(), cols.next(), cols.next()) else {
                eprintln!("catalog: malformed legacy line {}", lineno + 1);
                continue;
            };
            let rel = Path::new(p);
            let src = if rel.is_absolute() {
                rel.to_path_buf()
            } else {
                self.home.join(rel)
            };
            let src = src.to_string_lossy().into_owned();
            let res = match kind {
                "csv" => self.fs.register_external(name, &src, FileFormat::Csv),
                "parquet" | "snapshot" => {
                    self.fs.register_external(name, &src, FileFormat::Parquet)
                }
                other => {
                    eprintln!("catalog: unknown legacy kind `{other}` (line {})", lineno + 1);
                    continue;
                }
            };
            match res {
                Ok(_) => imported += 1,
                Err(e) => eprintln!("catalog: legacy import `{name}` failed: {e}"),
            }
        }
        if imported > 0 {
            eprintln!(
                "catalog: imported {imported} table(s) from legacy {LEGACY_MANIFEST}; \
                 future state is stored under metadata/"
            );
        }
        Ok(())
    }

    /// Rebuild the display cache from the catalog metadata.
    fn refresh(&mut self) -> Result<()> {
        self.entries.clear();
        for meta in self.fs.list_tables().map_err(|e| anyhow!("{e}"))? {
            let (kind, path, rows) = self.describe(&meta);
            self.entries.insert(
                meta.name.clone(),
                Entry {
                    name: meta.name.clone(),
                    kind,
                    path,
                    rows,
                    created: fmt_time(meta.created_at),
                },
            );
        }
        Ok(())
    }

    fn describe(&self, meta: &TableMeta) -> (Kind, PathBuf, u64) {
        let Ok(Some(snap)) = self.fs.latest(meta.table_id) else {
            return (Kind::Snapshot, PathBuf::new(), 0);
        };
        let Ok(files) = self.fs.files(meta.table_id, snap) else {
            return (Kind::Snapshot, PathBuf::new(), 0);
        };
        let rows: u64 = files.iter().map(|f| f.row_count).sum();
        let path = files
            .first()
            .map(|f| PathBuf::from(&f.path))
            .unwrap_or_default();
        if let Some(first) = files.first() {
            if !first.managed {
                let kind = match first.format {
                    FileFormat::Csv => Kind::Csv,
                    FileFormat::Parquet => Kind::Parquet,
                };
                return (kind, path, rows);
            }
        }
        (Kind::Snapshot, path, rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn sample_batch() -> (SchemaRef, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1i64, 2, 3])) as ArrayRef],
        )
        .unwrap();
        (schema, batch)
    }

    #[test]
    fn roundtrip_managed_snapshot() {
        let dir = std::env::temp_dir().join(format!("gtv_cat_facade_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        {
            let mut cat = Catalog::open(&dir).unwrap();
            let (schema, batch) = sample_batch();
            let n = cat
                .record_snapshot("myt", &schema, std::slice::from_ref(&batch))
                .unwrap();
            assert_eq!(n, 3);
            assert_eq!(cat.names().count(), 1);
            let e = cat.get("myt").unwrap();
            assert_eq!(e.kind, Kind::Snapshot);
            assert_eq!(e.rows, 3);
        }
        {
            let cat = Catalog::open(&dir).unwrap();
            let e = cat.get("myt").expect("myt survives");
            assert_eq!(e.kind, Kind::Snapshot);
            assert_eq!(e.rows, 3);
        }
        {
            let mut cat = Catalog::open(&dir).unwrap();
            assert!(cat.remove("myt").unwrap().is_some());
            assert!(cat.get("myt").is_none());
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn external_csv_reference_is_registered() {
        let dir = std::env::temp_dir().join(format!("gtv_cat_csv_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let csv = dir.join("ticks.csv");
        fs::write(&csv, "t,p\n1,10.0\n2,11.0\n").unwrap();

        {
            let mut cat = Catalog::open(&dir).unwrap();
            cat.record_csv("ticks", csv.to_str().unwrap()).unwrap();
            let e = cat.get("ticks").unwrap();
            assert_eq!(e.kind, Kind::Csv);
            assert_eq!(e.path, csv);
        }
        {
            let cat = Catalog::open(&dir).unwrap();
            assert_eq!(cat.get("ticks").unwrap().kind, Kind::Csv);
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn legacy_manifest_is_imported() {
        let dir = std::env::temp_dir().join(format!("gtv_cat_legacy_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let csv = dir.join("legacy.csv");
        fs::write(&csv, "a\n1\n2\n").unwrap();
        fs::write(
            dir.join("catalog.tsv"),
            format!(
                "# gtv table catalog v1\n# name\tkind\tpath\trows\tcreated\nlegacy\tcsv\t{}\t0\t2024-01-01T00:00:00\n",
                csv.display()
            ),
        )
        .unwrap();

        let cat = Catalog::open(&dir).unwrap();
        let e = cat.get("legacy").expect("legacy table imported");
        assert_eq!(e.kind, Kind::Csv);
        let _ = fs::remove_dir_all(&dir);
    }
}
