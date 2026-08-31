//! Per-day static data cache (read-through).
//!
//! Layout: `<root>/<source>/<date>/<symbol>.parquet`, e.g.
//! `data/static/yahoo/2026-05-01/0700.HK.parquet`. Higher-level operators check
//! this cache first and only hit the web API when the data is missing, so the
//! original function signatures are unchanged.

use std::fs;
use std::path::PathBuf;

use arrow::record_batch::RecordBatch;
use chrono::{TimeZone, Utc};

use crate::error::Result;
use crate::parquet::{read_batches, write_batch};

#[derive(Debug, Clone)]
pub struct StaticCache {
    root: PathBuf,
}

impl StaticCache {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &PathBuf {
        &self.root
    }

    /// `<root>/<source>/<date>/<symbol>.parquet`.
    pub fn day_path(&self, source: &str, date: &str, symbol: &str) -> PathBuf {
        self.root.join(source).join(date).join(format!("{symbol}.parquet"))
    }

    /// Write one day's data (creates `source/date/` dirs).
    pub fn write_day(&self, source: &str, date: &str, symbol: &str, batch: &RecordBatch) -> Result<()> {
        let path = self.day_path(source, date, symbol);
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir)?;
        }
        write_batch(path.to_str().unwrap(), batch)
    }

    /// Read every cached day of `symbol` with `date ∈ [start, end]` (inclusive,
    /// `YYYY-MM-DD` string compare).
    pub fn read_days(&self, source: &str, symbol: &str, start: &str, end: &str) -> Result<Vec<RecordBatch>> {
        let dir = self.root.join(source);
        let mut out = Vec::new();
        if !dir.is_dir() {
            return Ok(out);
        }
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let date = entry.file_name().to_string_lossy().to_string();
            if date.as_str() < start || date.as_str() > end {
                continue;
            }
            let path = entry.path().join(format!("{symbol}.parquet"));
            if path.exists() {
                out.extend(read_batches(path.to_str().unwrap())?);
            }
        }
        Ok(out)
    }

    /// Most recent cached date for a symbol (or `None`).
    pub fn latest_date(&self, source: &str, symbol: &str) -> Option<String> {
        let dir = self.root.join(source);
        if !dir.is_dir() {
            return None;
        }
        let mut best: Option<String> = None;
        for entry in fs::read_dir(&dir).ok()? {
            let entry = entry.ok()?;
            if !entry.file_type().ok()?.is_dir() {
                continue;
            }
            let date = entry.file_name().to_string_lossy().to_string();
            if entry.path().join(format!("{symbol}.parquet")).exists()
                && best.as_ref().map_or(true, |b| &date > b)
            {
                best = Some(date);
            }
        }
        best
    }

    /// `YYYY-MM-DD` from epoch seconds.
    pub fn date_from_secs(secs: i64) -> String {
        Utc.timestamp_opt(secs, 0)
            .single()
            .map(|dt| dt.date_naive().format("%Y-%m-%d").to_string())
            .unwrap_or_default()
    }

    /// `YYYY-MM-DD` from epoch microseconds.
    pub fn date_from_us(us: i64) -> String {
        Utc.timestamp_micros(us)
            .single()
            .map(|dt| dt.date_naive().format("%Y-%m-%d").to_string())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn write_and_read_days() {
        let root = std::env::temp_dir().join("gtv_cache_test");
        let _ = fs::remove_dir_all(&root);
        let c = StaticCache::new(&root);
        let schema = Arc::new(Schema::new(vec![Field::new("close", DataType::Float64, false)]));
        let b1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(Float64Array::from(vec![1.0]))]).unwrap();
        let b2 = RecordBatch::try_new(schema.clone(), vec![Arc::new(Float64Array::from(vec![2.0, 3.0]))]).unwrap();
        c.write_day("yahoo", "2026-05-01", "AAA", &b1).unwrap();
        c.write_day("yahoo", "2026-05-02", "AAA", &b2).unwrap();
        c.write_day("yahoo", "2026-05-02", "BBB", &b2).unwrap();

        assert_eq!(c.latest_date("yahoo", "AAA").as_deref(), Some("2026-05-02"));
        let rows: usize = c
            .read_days("yahoo", "AAA", "2026-05-01", "2026-05-02")
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 3);
        // date conversion
        assert_eq!(StaticCache::date_from_secs(1787875200), "2026-08-28");
        let _ = fs::remove_dir_all(&root);
    }
}
