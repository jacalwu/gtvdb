//! Crash-safe file publication helpers.
//!
//! Every metadata / data file is written to a sibling `*.tmp`, flushed to disk,
//! then `rename`d into place (atomic on POSIX). The containing directory is
//! fsync'd so the rename itself is durable. Readers must only ever open the
//! final path, never the temporary one.

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use arrow::record_batch::RecordBatch;

use crate::error::{Result, StorageError};

/// fsync a directory so a rename/create inside it is durable.
pub fn fsync_dir(dir: &Path) -> Result<()> {
    let f = File::open(dir).map_err(StorageError::Io)?;
    f.sync_all().map_err(StorageError::Io)
}

/// Temporary sibling path used for atomic writes (`<path>.tmp`).
pub fn tmp_path(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_os_string();
    s.push(".tmp");
    PathBuf::from(s)
}

/// Write `bytes` to `path` atomically: `path.tmp` → fsync → rename → fsync dir.
pub fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = tmp_path(path);
    {
        let mut f = File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        fsync_dir(parent)?;
    }
    Ok(())
}

/// Write a Parquet file atomically (`<path>.tmp` → fsync → rename → fsync dir).
pub fn write_batch_atomic(path: &Path, batch: &RecordBatch) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Err(StorageError::Msg("path has no parent directory".into()));
    };
    fs::create_dir_all(parent)?;
    let tmp = tmp_path(path);
    {
        let file = File::create(&tmp)?;
        let mut writer = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None)?;
        writer.write(batch)?;
        let file = writer.into_inner()?;
        file.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    fsync_dir(parent)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::UInt64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("x", DataType::UInt64, false)])),
            vec![Arc::new(UInt64Array::from(vec![1u64, 2, 3]))],
        )
        .unwrap()
    }

    #[test]
    fn atomic_write_then_read() {
        let dir = std::env::temp_dir().join(format!("gtv_atomic_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let path = dir.join("sub").join("data.parquet");
        write_batch_atomic(&path, &batch()).unwrap();
        assert!(path.exists());
        assert!(!tmp_path(&path).exists());
        let out = crate::read_batches(path.to_str().unwrap()).unwrap();
        assert_eq!(out.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_bytes_atomic() {
        let dir = std::env::temp_dir().join(format!("gtv_atomic_bytes_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let path = dir.join("meta.json");
        write_atomic(&path, b"{\"a\":1}").unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), "{\"a\":1}");
        let _ = fs::remove_dir_all(&dir);
    }
}
