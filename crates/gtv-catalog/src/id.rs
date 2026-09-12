//! Immutable identifiers used across the catalog.
//!
//! Every table gets a [`TableId`] once and keeps it forever (renames only change
//! the display name). Snapshots, data files, commits and indexes are likewise
//! UUID-keyed so metadata can reference them without ambiguity.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

macro_rules! id_type {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(pub Uuid);

        impl $name {
            /// Generate a fresh random id.
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl std::str::FromStr for $name {
            type Err = uuid::Error;
            fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
                Ok(Self(Uuid::parse_str(s)?))
            }
        }
    };
}

id_type!(
    /// Stable identity of a table.
    TableId
);
id_type!(
    /// A committed, immutable table snapshot.
    SnapshotId
);
id_type!(
    /// A single immutable data file.
    DataFileId
);
id_type!(
    /// One atomic metadata commit.
    CommitId
);
id_type!(
    /// A vector index built over a corpus snapshot.
    IndexId
);
