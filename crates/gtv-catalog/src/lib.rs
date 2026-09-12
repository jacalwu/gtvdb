//! gtv-catalog — the metadata control plane for gtvdb.
//!
//! Provides stable table/snapshot/file identities, a versioned schema registry,
//! partition specs, immutable snapshot manifests and an atomic commit protocol
//! over the local filesystem. Everything downstream (index lifecycle, execution
//! lineage, embedding governance, DQ gates) references `SnapshotId`s produced
//! here.

pub mod error;
pub mod id;
pub mod manifest;
pub mod partition;
pub mod schema;
pub mod stats;
pub mod store;

pub use error::{CatalogError, Result};
pub use id::{CommitId, DataFileId, IndexId, SnapshotId, TableId};
pub use manifest::{
    ColumnStat, CommitOp, DataFile, FileFormat, Scalar, Snapshot, SourceOffset,
};
pub use partition::{partition_dir, PartitionColumn, PartitionSpec, PartitionValue, Transform};
pub use schema::{
    apply_change, check_compatible, is_widening, SchemaChange, SchemaRecord, SchemaVersion,
};
pub use stats::column_stats;
pub use store::{CommitOptions, FsCatalog, NewFile, ScanFilter, TableMeta};
