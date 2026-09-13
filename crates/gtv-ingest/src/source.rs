//! The [`SourceAdapter`] contract shared by every streaming source.
//!
//! An adapter owns the *read side* of a feed and the source-side commit of
//! offsets. The pipeline never trusts an offset until the corresponding data
//! batch has been durably published (see [`crate::batch`]).

use serde::{Deserialize, Serialize};

use crate::envelope::Envelope;
use crate::error::Result;

/// Committed position of one `(source, partition)`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PartitionOffset {
    pub source: String,
    pub partition: i32,
    /// Last processed offset (inclusive); the next read starts at `offset + 1`.
    pub offset: i64,
}

impl PartitionOffset {
    pub fn new(source: impl Into<String>, partition: i32, offset: i64) -> Self {
        Self {
            source: source.into(),
            partition,
            offset,
        }
    }
}

impl From<&PartitionOffset> for gtv_catalog::SourceOffset {
    fn from(o: &PartitionOffset) -> Self {
        gtv_catalog::SourceOffset {
            source: o.source.clone(),
            partition: o.partition,
            offset: o.offset,
        }
    }
}

/// Per-partition lag relative to the end of the source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionLag {
    pub partition: i32,
    /// Last committed offset (`-1` when nothing committed).
    pub committed: i64,
    /// Highest available offset (`-1` when the source is empty).
    pub high: i64,
}

impl PartitionLag {
    /// Number of unprocessed events (`0` when caught up).
    pub fn lag(&self) -> i64 {
        (self.high - self.committed).max(0)
    }
}

/// A read-side streaming source.
pub trait SourceAdapter: Send {
    /// Stable source name (used as the offset / DLQ key).
    fn name(&self) -> &str;

    /// Poll up to `max` events. Must not block indefinitely; an empty return is
    /// a valid "no data right now".
    fn poll(&mut self, max: usize) -> Result<Vec<Envelope>>;

    /// Source-side offset commit, called only after a successful publish.
    fn commit(&mut self, offsets: &[PartitionOffset]) -> Result<()>;

    /// Resume from committed offsets (restart / replay).
    fn seek(&mut self, offsets: &[PartitionOffset]) -> Result<()>;

    /// Per-partition lag for health / metrics.
    fn lag(&self) -> Vec<PartitionLag>;
}
