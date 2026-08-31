//! gtv-storage: Arrow ↔ Parquet persistence plus a multi-versioned in-memory
//! time-travel store.

pub mod cache;
pub mod csv;
pub mod error;
pub mod hdb;
pub mod parquet;
pub mod snapshot;

pub use cache::StaticCache;
pub use csv::read_csv;
pub use error::{Result, StorageError};
pub use hdb::{read_parquet_mmap, HdbStore};
pub use parquet::{read_batches, write_batch};
pub use snapshot::{Snapshot, SnapshotStore};
