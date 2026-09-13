//! gtv-catalog — the metadata control plane for gtvdb.
//!
//! Provides stable table/snapshot/file identities, a versioned schema registry,
//! partition specs, immutable snapshot manifests and an atomic commit protocol
//! over the local filesystem. Everything downstream (index lifecycle, execution
//! lineage, embedding governance, DQ gates) references `SnapshotId`s produced
//! here.

pub mod dq;
pub mod error;
pub mod embedding;
pub mod id;
pub mod lineage;
pub mod manifest;
pub mod partition;
pub mod schema;
pub mod stats;
pub mod store;

pub use error::{CatalogError, Result};
pub use dq::{
    overridden_rules, parse_rules, DqFailure, DqRule, GateDecision, GateDecisionRecord,
    OverrideRecord,
};
pub use embedding::{
    active_entity_ids, embedding_schema, filter_active, read_embeddings, schema_dimension,
    validate_embedding_batch, EmbeddingGovernance, EmbeddingProvenance, EMBEDDING_COLUMN,
};
pub use id::{CommitId, DataFileId, IndexId, SnapshotId, TableId};
pub use lineage::{
    query_hash, ExecutionId, ExecutionRecord, IndexRef, ModelRef, TableRef, UdfRef,
};
pub use manifest::{
    ColumnStat, CommitOp, DataFile, FileFormat, Scalar, Snapshot, SourceOffset,
};
pub use partition::{partition_dir, PartitionColumn, PartitionSpec, PartitionValue, Transform};
pub use schema::{
    apply_change, check_compatible, is_widening, SchemaChange, SchemaRecord, SchemaVersion,
};
pub use stats::column_stats;
pub use store::{CommitOptions, FsCatalog, NewFile, ScanFilter, TableMeta};
