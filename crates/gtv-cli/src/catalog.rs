//! Table catalog: persistence of registered tables across REPL restarts.
//!
//! Enabled by setting `GTV_HOME` to a directory. The catalog keeps one TSV
//! manifest (`catalog.tsv`) at the home root:
//!
//! ```text
//! name  kind    path          rows  created
//! ticks csv     /abs/ticks.csv       2025-..
//! myt   snapshot snap/myt.parquet  6  2025-..
//! ```
//!
//! `kind` is one of:
//! * `csv` / `parquet` — a reference to the original data file. On restart the
//!   file is re-read (no data duplication; the source remains authoritative).
//! * `snapshot` — a materialized Parquet copy under `<home>/snap/`, written for
//!   tables with no backing file (e.g. `CREATE TABLE … AS SELECT …`).
//!
//! Loads on startup are best-effort: a missing source file logs a warning and
//! is skipped rather than aborting the session.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result};
use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use chrono::Local;
use gtv_engine::GtvContext;

/// Where a persisted table's rows come from on the next startup.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Kind {
    Csv,
    Parquet,
    Snapshot,
}

impl Kind {
    fn as_str(self) -> &'static str {
        match self {
            Kind::Csv => "csv",
            Kind::Parquet => "parquet",
            Kind::Snapshot => "snapshot",
        }
    }

    fn parse(s: &str) -> Option<Kind> {
        match s {
            "csv" => Some(Kind::Csv),
            "parquet" => Some(Kind::Parquet),
            "snapshot" => Some(Kind::Snapshot),
            _ => None,
        }
    }
}

/// One persisted table.
#[derive(Clone, Debug)]
pub struct Entry {
    pub name: String,
    pub kind: Kind,
    /// Absolute path (source file for csv/parquet, snapshot file for snapshot).
    pub path: PathBuf,
    pub rows: u64,
    pub created: String,
}

/// A persisted-table manifest rooted at a directory.
#[derive(Debug)]
pub struct Catalog {
    home: PathBuf,
    entries: BTreeMap<String, Entry>,
}

const MANIFEST: &str = "catalog.tsv";
const SNAP_DIR: &str = "snap";

fn now() -> String {
    Local::now().format("%Y-%m-%dT%H:%M:%S").to_string()
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

fn slug(name: &str) -> String {
    let s: String = name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect();
    if s.is_empty() { "table".into() } else { s }
}

impl Catalog {
    /// Open (or create) the catalog under `home`. `home` must be writable.
    pub fn open(home: &Path) -> Result<Catalog> {
        fs::create_dir_all(home).with_context(|| format!("create catalog dir {}", home.display()))?;
        let mut cat = Catalog { home: home.to_path_buf(), entries: BTreeMap::new() };
        cat.load()?;
        Ok(cat)
    }

    pub fn home(&self) -> &Path {
        &self.home
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
        self.upsert(Entry {
            name: name.to_string(),
            kind: Kind::Csv,
            path: abs_path(path),
            rows: 0,
            created: now(),
        })
    }

    /// Record a table whose rows come from a Parquet file on disk.
    pub fn record_parquet(&mut self, name: &str, path: &str) -> Result<()> {
        self.upsert(Entry {
            name: name.to_string(),
            kind: Kind::Parquet,
            path: abs_path(path),
            rows: 0,
            created: now(),
        })
    }

    /// Materialize `batches` as a snapshot Parquet file and record it.
    pub fn record_snapshot(&mut self, name: &str, schema: &arrow::datatypes::SchemaRef, batches: &[RecordBatch]) -> Result<u64> {
        let snap = self.home.join(SNAP_DIR);
        fs::create_dir_all(&snap).with_context(|| format!("create {}", snap.display()))?;
        let file = snap.join(format!("{}.parquet", slug(name)));
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        let combined = if batches.is_empty() {
            RecordBatch::new_empty(schema.clone())
        } else {
            concat_batches(schema, batches).with_context(|| format!("concat snapshot `{name}`"))?
        };
        gtv_storage::write_batch(file.to_str().unwrap(), &combined)
            .with_context(|| format!("write snapshot `{name}`"))?;
        self.upsert(Entry {
            name: name.to_string(),
            kind: Kind::Snapshot,
            path: file,
            rows: rows as u64,
            created: now(),
        })?;
        Ok(rows as u64)
    }

    /// Remove a table from the manifest; deletes the snapshot file (but never
    /// a referenced csv/parquet source file). Returns the removed entry.
    pub fn remove(&mut self, name: &str) -> Result<Option<Entry>> {
        let Some(entry) = self.entries.remove(name) else {
            return Ok(None);
        };
        if entry.kind == Kind::Snapshot {
            let _ = fs::remove_file(&entry.path);
        }
        self.save()?;
        Ok(Some(entry))
    }

    /// Replay every entry into `ctx`, best-effort: tables whose source file is
    /// missing are skipped with a warning. Returns the number restored.
    pub fn replay(&self, ctx: &GtvContext) -> usize {
        let mut restored = 0usize;
        for e in self.entries.values() {
            match e.kind {
                Kind::Csv => match ctx.register_csv(e.path.to_str().unwrap(), &e.name) {
                    Ok(()) => restored += 1,
                    Err(err) => eprintln!(
                        "catalog: skip `{}` ({}) — {}",
                        e.name,
                        e.path.display(),
                        err
                    ),
                },
                Kind::Parquet => match ctx.register_parquet(e.path.to_str().unwrap(), &e.name) {
                    Ok(()) => restored += 1,
                    Err(err) => eprintln!(
                        "catalog: skip `{}` ({}) — {}",
                        e.name,
                        e.path.display(),
                        err
                    ),
                },
                Kind::Snapshot => match gtv_storage::read_batches(e.path.to_str().unwrap()) {
                    Ok(batches) => {
                        let first = batches.first().cloned();
                        match first {
                            Some(b) if ctx.register_batches(&e.name, b.schema(), batches).is_ok() => {
                                restored += 1;
                            }
                            _ => eprintln!("catalog: skip `{}` ({}) — empty/unreadable", e.name, e.path.display()),
                        }
                    }
                    Err(err) => eprintln!(
                        "catalog: skip `{}` ({}) — {}",
                        e.name,
                        e.path.display(),
                        err
                    ),
                },
            }
        }
        restored
    }

    /// Persist the in-memory manifest to `<home>/catalog.tsv` (atomic replace).
    pub fn save(&self) -> Result<()> {
        let path = self.home.join(MANIFEST);
        let tmp = self.home.join(format!("{MANIFEST}.tmp"));
        let mut out = String::new();
        out.push_str("# gtv table catalog v1\n");
        out.push_str("# name\tkind\tpath\trows\tcreated\n");
        for e in self.entries.values() {
            out.push_str(&format!(
                "{}\t{}\t{}\t{}\t{}\n",
                e.name,
                e.kind.as_str(),
                e.path.display(),
                e.rows,
                e.created
            ));
        }
        fs::write(&tmp, out).with_context(|| format!("write {}", tmp.display()))?;
        fs::rename(&tmp, &path).with_context(|| format!("rename to {}", path.display()))?;
        Ok(())
    }

    fn load(&mut self) -> Result<()> {
        let path = self.home.join(MANIFEST);
        if !path.exists() {
            return Ok(());
        }
        let text = fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        for (lineno, raw) in text.lines().enumerate() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let mut cols = line.split('\t');
            let (Some(name), Some(kind), Some(p)) = (cols.next(), cols.next(), cols.next()) else {
                eprintln!("catalog: malformed line {} in {}", lineno + 1, path.display());
                continue;
            };
            let rows = cols.next().and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
            let created = cols.next().unwrap_or("").to_string();
            let Some(kind) = Kind::parse(kind) else {
                eprintln!("catalog: unknown kind `{kind}` on line {}", lineno + 1);
                continue;
            };
            let path = Path::new(p);
            let path = if path.is_absolute() {
                path.to_path_buf()
            } else {
                self.home.join(path)
            };
            self.entries.insert(
                name.to_string(),
                Entry { name: name.to_string(), kind, path, rows, created },
            );
        }
        Ok(())
    }

    fn upsert(&mut self, entry: Entry) -> Result<()> {
        self.entries.insert(entry.name.clone(), entry);
        self.save()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, ArrayRef};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn sample_batch() -> (arrow::datatypes::SchemaRef, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1i64, 2, 3])) as ArrayRef],
        )
        .unwrap();
        (schema, batch)
    }

    #[test]
    fn roundtrip_manifest() {
        let dir = std::env::temp_dir().join(format!("gtv_cat_test_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        {
            let mut cat = Catalog::open(&dir).unwrap();
            cat.record_csv("ticks", "/tmp/ticks.csv").unwrap();
            let (schema, batch) = sample_batch();
            let n = cat
                .record_snapshot("myt", &schema, std::slice::from_ref(&batch))
                .unwrap();
            assert_eq!(n, 3);
            assert_eq!(cat.names().count(), 2);
        }
        {
            // Reopen: entries survive.
            let cat = Catalog::open(&dir).unwrap();
            let ticks = cat.get("ticks").expect("ticks entry");
            assert_eq!(ticks.kind, Kind::Csv);
            assert_eq!(ticks.path, Path::new("/tmp/ticks.csv"));
            let myt = cat.get("myt").expect("myt entry");
            assert_eq!(myt.kind, Kind::Snapshot);
            assert_eq!(myt.rows, 3);
            assert!(myt.path.exists(), "snapshot file written");
        }
        {
            let mut cat = Catalog::open(&dir).unwrap();
            cat.remove("myt").unwrap();
            assert!(cat.get("myt").is_none());
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn slug_sanitizes() {
        assert_eq!(slug("my.table/1"), "my_table_1");
        assert_eq!(slug("ticks"), "ticks");
    }
}
