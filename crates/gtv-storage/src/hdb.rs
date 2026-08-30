//! kdb+-style HDB partition store (roadmap phase 1).
//!
//! Physical layout:
//! ```text
//! <root>/sym_dictionary.parquet   # global symbol enum (id <-> symbol)
//! <root>/<date>/<table>/<symbol>.parquet
//! ```
//! Each date/table/symbol partition is a standalone Parquet file. On write, a
//! `symbol` column is *enumerated* (replaced by a compact `sym_id` int) and the
//! global dictionary is persisted; on read the id is decoded back to the symbol
//! string, so the round-trip is transparent to the caller. `scan` performs
//! *partition pruning*: it only touches the `date/` directories that fall in the
//! requested range and the `symbol` files that were requested.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{as_primitive_array, as_string_array, ArrayRef, Int64Array, StringArray, UInt64Array};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, Int64Type, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;

use crate::error::{Result, StorageError};

/// HDB root directory with `date/table/symbol.parquet` partitioning.
#[derive(Debug, Clone)]
pub struct HdbStore {
    root: PathBuf,
}

impl HdbStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn sym_dictionary_path(&self) -> PathBuf {
        self.root.join("sym_dictionary.parquet")
    }

    pub fn load_sym_dictionary(&self) -> SymDictionary {
        SymDictionary::load(&self.sym_dictionary_path()).unwrap_or_default()
    }

    pub fn save_sym_dictionary(&self, dict: &SymDictionary) -> Result<()> {
        dict.save(&self.sym_dictionary_path())
    }

    /// `<root>/<date>/<table>/<symbol>.parquet`.
    pub fn partition_path(&self, date: &str, table: &str, symbol: &str) -> PathBuf {
        self.root.join(date).join(table).join(format!("{symbol}.parquet"))
    }

    /// Write one partition (creates parent dirs).
    pub fn write_partition(
        &self,
        date: &str,
        table: &str,
        symbol: &str,
        batch: &RecordBatch,
    ) -> Result<PathBuf> {
        let path = self.partition_path(date, table, symbol);
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir)?;
        }
        let file = fs::File::create(&path)?;
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
        writer.write(batch)?;
        writer.close()?;
        Ok(path)
    }

    /// Persist a table into `date/table/`, splitting by a `symbol` column when
    /// one is present (otherwise a single `<table>.parquet`). Symbols are
    /// enumerated into `sym_id` and the global dictionary is updated. Returns
    /// the number of partitions written.
    pub fn write_table(&self, date: &str, table: &str, batch: &RecordBatch) -> Result<usize> {
        let mut dict = self.load_sym_dictionary();
        let has_sym = batch
            .column_by_name("symbol")
            .map(|c| matches!(c.data_type(), DataType::Utf8 | DataType::LargeUtf8))
            .unwrap_or(false);
        if !has_sym {
            self.write_partition(date, table, table, batch)?;
            return Ok(1);
        }
        let mut count = 0;
        for (sym, sub) in split_by_symbol(batch) {
            let encoded = encode_symbol_column(&sub, &mut dict);
            self.write_partition(date, table, &sym, &encoded)?;
            count += 1;
        }
        self.save_sym_dictionary(&dict)?;
        Ok(count)
    }

    /// Read a single partition (all row groups), decoding `sym_id` back to
    /// `symbol` via the global dictionary.
    pub fn read_partition(&self, date: &str, table: &str, symbol: &str) -> Result<Vec<RecordBatch>> {
        let path = self.partition_path(date, table, symbol);
        let dict = self.load_sym_dictionary();
        Ok(read_parquet_mmap(&path)?
            .into_iter()
            .map(|b| decode_symbol_column(&b, &dict))
            .collect())
    }

    /// Dates present for a table (sorted), derived from the directory listing.
    pub fn list_dates(&self, table: &str) -> Result<Vec<String>> {
        let mut dates = Vec::new();
        if !self.root.is_dir() {
            return Ok(dates);
        }
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let date = entry.file_name().to_string_lossy().to_string();
            if entry.path().join(table).is_dir() {
                dates.push(date);
            }
        }
        dates.sort();
        Ok(dates)
    }

    /// Symbols present for a table on a date (sorted).
    pub fn list_symbols(&self, date: &str, table: &str) -> Result<Vec<String>> {
        let dir = self.root.join(date).join(table);
        let mut syms = Vec::new();
        if !dir.is_dir() {
            return Ok(syms);
        }
        for entry in fs::read_dir(&dir)? {
            let name = entry?.file_name().to_string_lossy().to_string();
            if let Some(stem) = name.strip_suffix(".parquet") {
                syms.push(stem.to_string());
            }
        }
        syms.sort();
        Ok(syms)
    }

    /// Partition pruning: list the partition files matching a date range
    /// (inclusive, string-compared on `YYYY.MM.DD`) and a symbol set. When
    /// `syms` is empty, all symbols match.
    pub fn prune(
        &self,
        table: &str,
        start_date: &str,
        end_date: &str,
        syms: &[String],
    ) -> Result<Vec<PathBuf>> {
        let mut out = Vec::new();
        for date in self.list_dates(table)? {
            if date.as_str() < start_date || date.as_str() > end_date {
                continue;
            }
            for sym in self.list_symbols(&date, table)? {
                if !syms.is_empty() && !syms.iter().any(|s| s == &sym) {
                    continue;
                }
                out.push(self.partition_path(&date, table, &sym));
            }
        }
        Ok(out)
    }

    /// Prune + read + concatenate every matching partition (decoded).
    pub fn scan(
        &self,
        table: &str,
        start_date: &str,
        end_date: &str,
        syms: &[String],
    ) -> Result<Vec<RecordBatch>> {
        let dict = self.load_sym_dictionary();
        let mut batches = Vec::new();
        for path in self.prune(table, start_date, end_date, syms)? {
            for b in read_parquet_mmap(&path)? {
                batches.push(decode_symbol_column(&b, &dict));
            }
        }
        Ok(batches)
    }
}

// ---------------------------------------------------------------------------
// Global symbol enumeration
// ---------------------------------------------------------------------------

/// Global symbol dictionary (`id <-> symbol`), persisted as
/// `<root>/sym_dictionary.parquet`.
#[derive(Debug, Default)]
pub struct SymDictionary {
    pub id_to_sym: Vec<String>,
    sym_to_id: HashMap<String, i64>,
}

impl SymDictionary {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::new());
        }
        let batches = read_parquet_mmap(path)?;
        let mut dict = Self::new();
        for b in batches {
            let ids = as_primitive_array::<Int64Type>(b.column_by_name("id").ok_or_else(|| {
                StorageError::Msg("sym_dictionary missing `id` column".into())
            })?);
            let syms = as_string_array(b.column_by_name("symbol").ok_or_else(|| {
                StorageError::Msg("sym_dictionary missing `symbol` column".into())
            })?);
            for i in 0..b.num_rows() {
                dict.insert(ids.value(i), syms.value(i).to_string());
            }
        }
        Ok(dict)
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir)?;
        }
        let ids: Vec<i64> = (0..self.id_to_sym.len() as i64).collect();
        let syms: Vec<String> = self.id_to_sym.clone();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("symbol", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)) as ArrayRef,
                Arc::new(StringArray::from(syms)) as ArrayRef,
            ],
        )?;
        let file = fs::File::create(path)?;
        let mut writer = ArrowWriter::try_new(file, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(())
    }

    pub fn insert(&mut self, id: i64, symbol: String) {
        if self.id_to_sym.len() <= id as usize {
            self.id_to_sym.resize(id as usize + 1, String::new());
        }
        self.id_to_sym[id as usize] = symbol.clone();
        self.sym_to_id.insert(symbol, id);
    }

    pub fn get_or_insert(&mut self, symbol: &str) -> i64 {
        if let Some(&id) = self.sym_to_id.get(symbol) {
            return id;
        }
        let id = self.id_to_sym.len() as i64;
        self.insert(id, symbol.to_string());
        id
    }

    pub fn symbol(&self, id: i64) -> Option<&str> {
        self.id_to_sym.get(id as usize).map(|s| s.as_str())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Split a batch into per-symbol sub-batches by its `symbol` column.
fn split_by_symbol(batch: &RecordBatch) -> Vec<(String, RecordBatch)> {
    let sym = as_string_array(batch.column_by_name("symbol").expect("symbol column"));
    let mut groups: HashMap<String, Vec<u64>> = HashMap::new();
    for i in 0..batch.num_rows() {
        groups
            .entry(sym.value(i).to_string())
            .or_default()
            .push(i as u64);
    }
    let mut out = Vec::with_capacity(groups.len());
    for (sym, idxs) in groups {
        let indices = UInt64Array::from(idxs);
        let cols: Vec<ArrayRef> = batch
            .columns()
            .iter()
            .map(|c| take(c.as_ref(), &indices, None).expect("take column"))
            .collect();
        let sub = RecordBatch::try_new(batch.schema(), cols).expect("build sub batch");
        out.push((sym, sub));
    }
    out
}

/// Replace a `symbol` (Utf8) column with an enumerated `sym_id` (Int64) column.
fn encode_symbol_column(batch: &RecordBatch, dict: &mut SymDictionary) -> RecordBatch {
    let Ok(idx) = batch.schema().index_of("symbol") else {
        return batch.clone();
    };
    if !matches!(
        batch.column(idx).data_type(),
        DataType::Utf8 | DataType::LargeUtf8
    ) {
        return batch.clone();
    }
    let sym = as_string_array(batch.column(idx));
    let ids: Vec<i64> = (0..batch.num_rows())
        .map(|i| dict.get_or_insert(sym.value(i)))
        .collect();

    let mut fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields[idx] = Field::new("sym_id", DataType::Int64, false);

    let mut cols: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    for (i, c) in batch.columns().iter().enumerate() {
        if i == idx {
            cols.push(Arc::new(Int64Array::from(ids.clone())));
        } else {
            cols.push(c.clone());
        }
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).expect("encode batch")
}

/// Replace a `sym_id` (Int64) column with the decoded `symbol` (Utf8) column.
fn decode_symbol_column(batch: &RecordBatch, dict: &SymDictionary) -> RecordBatch {
    let Ok(idx) = batch.schema().index_of("sym_id") else {
        return batch.clone();
    };
    let ids = as_primitive_array::<Int64Type>(batch.column(idx));
    let syms: Vec<String> = ids
        .values()
        .iter()
        .map(|id| dict.symbol(*id).unwrap_or("").to_string())
        .collect();

    let mut fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields[idx] = Field::new("symbol", DataType::Utf8, false);

    let mut cols: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    for (i, c) in batch.columns().iter().enumerate() {
        if i == idx {
            cols.push(Arc::new(StringArray::from(syms.clone())));
        } else {
            cols.push(c.clone());
        }
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).expect("decode batch")
}

/// Memory-map a Parquet file and read it through the mapped bytes (the OS page
/// cache is accessed directly, avoiding an explicit user-space `read` copy of
/// the raw file).
pub fn read_parquet_mmap(path: &Path) -> Result<Vec<RecordBatch>> {
    let file = fs::File::open(path)?;
    // SAFETY: the mapping is read-only and the file is not modified while mapped.
    let mmap = unsafe { memmap2::Mmap::map(&file).map_err(StorageError::from)? };
    // Zero-copy: `Bytes::from_owner` wraps the mmap (no user-space file copy).
    let bytes = bytes::Bytes::from_owner(mmap);
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|e| StorageError::Arrow(e.into()))?;
    let reader = builder.build().map_err(|e| StorageError::Arrow(e.into()))?;
    let mut batches = Vec::new();
    for b in reader {
        batches.push(b?);
    }
    Ok(batches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Float64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["AAPL", "MSFT", "AAPL"])) as ArrayRef,
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])) as ArrayRef,
            ],
        )
        .unwrap()
    }

    #[test]
    fn write_table_splits_and_scan_prunes() {
        let root = std::env::temp_dir().join("gtvdb_hdb_test");
        let _ = fs::remove_dir_all(&root);
        let hdb = HdbStore::new(&root);

        let b = sample_batch();
        let n1 = hdb.write_table("2026.08.28", "trade", &b).unwrap();
        let n2 = hdb.write_table("2026.08.29", "trade", &b).unwrap();
        assert_eq!(n1, 2);
        assert_eq!(n2, 2);

        // dictionary was persisted with 2 symbols
        let dict = hdb.load_sym_dictionary();
        assert_eq!(dict.id_to_sym.len(), 2);

        // list symbols on one date (file names stay readable symbol strings)
        let syms = hdb.list_symbols("2026.08.28", "trade").unwrap();
        assert_eq!(syms, vec!["AAPL", "MSFT"]);

        // prune to one date + one symbol
        let paths = hdb
            .prune("trade", "2026.08.28", "2026.08.28", &["AAPL".to_string()])
            .unwrap();
        assert_eq!(paths.len(), 1);

        // scan a single date + single symbol -> 2 rows (two AAPL), decoded
        let batches = hdb
            .scan("trade", "2026.08.28", "2026.08.28", &["AAPL".to_string()])
            .unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2);
        // symbol column is decoded back to strings
        let sym = as_string_array(batches[0].column_by_name("symbol").unwrap());
        assert!(sym.iter().all(|s| s == Some("AAPL")));

        // scan all dates, all symbols -> 6 rows
        let batches = hdb
            .scan("trade", "2026.08.28", "2026.08.29", &[])
            .unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 6);

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn sym_dictionary_roundtrip() {
        let mut dict = SymDictionary::new();
        let a = dict.get_or_insert("AAPL");
        let b = dict.get_or_insert("MSFT");
        let a2 = dict.get_or_insert("AAPL");
        assert_eq!(a, a2);
        assert_ne!(a, b);
        assert_eq!(dict.symbol(a), Some("AAPL"));
        assert_eq!(dict.symbol(b), Some("MSFT"));
    }
}
