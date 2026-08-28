//! gtv-storage: Arrow ↔ Parquet persistence plus a multi-versioned in-memory
//! time-travel store.

pub mod error;
pub mod parquet;
pub mod snapshot;

pub use error::{Result, StorageError};
pub use parquet::{read_batches, write_batch};
pub use snapshot::{Snapshot, SnapshotStore};
