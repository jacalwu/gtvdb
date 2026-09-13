//! Bitemporal time model (prod_p3 B3-2).
//!
//! A bitemporal fact has two independent time axes:
//!
//! * **business time** — when the fact is true in the modelled world
//!   (`business_valid_from <= t < business_valid_to`);
//! * **system time** — when the system knew it (`system_valid_from <= t <
//!   system_valid_to`).
//!
//! The temporal CSR keeps indexing *business* time only (two axes in one index
//! would blow up the structure); system time is versioned by the storage /
//! catalog layer, where each system write appends a new immutable version.
//!
//! [`BitemporalRange`] is the value type; [`migrate_legacy_edges`] upgrades a
//! single-timeline edge table (legacy `valid_from/valid_to`) into the canonical
//! bitemporal schema while keeping the legacy columns for compatibility;
//! [`find_overlaps`] detects contradictory versions for the same entity.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Date32Array, RecordBatch, TimestampNanosecondArray, UInt16Array,
    UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

use crate::error::{GtvError, Result};

/// Upper-bound sentinel for business / system time ("still true", "still known").
pub const OPEN_ENDED: i64 = i64::MAX;

/// Nanoseconds in one day, for the `business_date` projection.
pub const NS_PER_DAY: i64 = 86_400_000_000_000;

/// Canonical column names of a bitemporal edge table.
pub mod bitemporal_cols {
    /// Legacy / business-valid start.
    pub const VALID_FROM: &str = "valid_from";
    /// Legacy / business-valid end (exclusive).
    pub const VALID_TO: &str = "valid_to";
    /// Business-valid start.
    pub const BUSINESS_FROM: &str = "business_valid_from";
    /// Business-valid end (exclusive).
    pub const BUSINESS_TO: &str = "business_valid_to";
    /// System-known start.
    pub const SYSTEM_FROM: &str = "system_valid_from";
    /// System-known end (exclusive).
    pub const SYSTEM_TO: &str = "system_valid_to";
    /// Event time (usually equal to `business_valid_from`).
    pub const EVENT_TIME: &str = "event_time";
    /// Ingest time (usually equal to `system_valid_from`).
    pub const INGEST_TIME: &str = "ingest_time";
    /// Business date (days since epoch).
    pub const BUSINESS_DATE: &str = "business_date";
}

fn timestamp_ns() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, None)
}

/// A half-open interval `[from, to)` on one time axis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BitemporalRange {
    pub business_from: i64,
    pub business_to: i64,
    pub system_from: i64,
    pub system_to: i64,
}

impl BitemporalRange {
    pub fn new(business_from: i64, business_to: i64, system_from: i64, system_to: i64) -> Self {
        Self {
            business_from,
            business_to,
            system_from,
            system_to,
        }
    }

    /// Upgrade a legacy single-timeline range: business time keeps
    /// `valid_from/valid_to`, the row becomes known at `system_from` and stays
    /// current forever.
    pub fn from_legacy(valid_from: i64, valid_to: i64, system_from: i64) -> Self {
        Self {
            business_from: valid_from,
            business_to: valid_to,
            system_from,
            system_to: OPEN_ENDED,
        }
    }

    #[inline]
    pub fn contains_business(&self, t: i64) -> bool {
        self.business_from <= t && t < self.business_to
    }

    #[inline]
    pub fn contains_system(&self, t: i64) -> bool {
        self.system_from <= t && t < self.system_to
    }

    /// True when the fact is visible for the `(business, system)` cut.
    #[inline]
    pub fn contains(&self, business: i64, system: i64) -> bool {
        self.contains_business(business) && self.contains_system(system)
    }

    #[inline]
    pub fn business_overlaps(&self, other: &Self) -> bool {
        self.business_from < other.business_to && other.business_from < self.business_to
    }

    #[inline]
    pub fn system_overlaps(&self, other: &Self) -> bool {
        self.system_from < other.system_to && other.system_from < self.system_to
    }

    #[inline]
    pub fn overlaps(&self, other: &Self) -> bool {
        self.business_overlaps(other) && self.system_overlaps(other)
    }

    /// A version with no known end yet.
    #[inline]
    pub fn is_current(&self) -> bool {
        self.system_to == OPEN_ENDED
    }
}

/// A pair of contradictory rows for the same entity (overlapping business *and*
/// system intervals).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Overlap {
    pub key: u64,
    pub first: usize,
    pub second: usize,
    pub business_from: i64,
    pub business_to: i64,
}

/// Detect contradictory versions: two rows with the same `key` whose business
/// and system intervals both overlap. `ranges` is aligned with `keys`.
pub fn find_overlaps(keys: &[u64], ranges: &[BitemporalRange]) -> Vec<Overlap> {
    debug_assert_eq!(keys.len(), ranges.len());
    let mut by_key: HashMap<u64, Vec<usize>> = HashMap::new();
    for (i, &k) in keys.iter().enumerate() {
        by_key.entry(k).or_default().push(i);
    }
    let mut out = Vec::new();
    for (key, idxs) in by_key {
        for a in 0..idxs.len() {
            for b in (a + 1)..idxs.len() {
                let (i, j) = (idxs[a], idxs[b]);
                if ranges[i].overlaps(&ranges[j]) {
                    let business_from = ranges[i].business_from.max(ranges[j].business_from);
                    let business_to = ranges[i].business_to.min(ranges[j].business_to);
                    out.push(Overlap {
                        key,
                        first: i,
                        second: j,
                        business_from,
                        business_to,
                    });
                }
            }
        }
    }
    // Deterministic ordering (HashMap iteration is not).
    out.sort_by(|a, b| {
        a.key
            .cmp(&b.key)
            .then_with(|| a.first.cmp(&b.first))
            .then_with(|| a.second.cmp(&b.second))
    });
    out
}

/// Business date in days since the Unix epoch (floor division).
#[inline]
pub fn business_date(ts: i64) -> i32 {
    ts.div_euclid(NS_PER_DAY) as i32
}

/// Schema of the canonical bitemporal edge table: the legacy `valid_from/to`
/// columns are retained (compatibility view), followed by the two explicit
/// axes plus `event_time` / `ingest_time` / `business_date`.
pub fn bitemporal_edge_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("src", DataType::UInt64, false),
        Field::new("dst", DataType::UInt64, false),
        Field::new("edge_type", DataType::UInt16, false),
        Field::new(bitemporal_cols::VALID_FROM, timestamp_ns(), false),
        Field::new(bitemporal_cols::VALID_TO, timestamp_ns(), false),
        Field::new(bitemporal_cols::BUSINESS_FROM, timestamp_ns(), false),
        Field::new(bitemporal_cols::BUSINESS_TO, timestamp_ns(), false),
        Field::new(bitemporal_cols::SYSTEM_FROM, timestamp_ns(), false),
        Field::new(bitemporal_cols::SYSTEM_TO, timestamp_ns(), false),
        Field::new(bitemporal_cols::EVENT_TIME, timestamp_ns(), false),
        Field::new(bitemporal_cols::INGEST_TIME, timestamp_ns(), false),
        Field::new(bitemporal_cols::BUSINESS_DATE, DataType::Date32, false),
    ]))
}

fn column<'a, T>(batch: &'a RecordBatch, name: &str) -> Result<&'a T>
where
    T: Array + 'static,
{
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|_| GtvError::Schema(format!("missing column `{name}`")))?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| GtvError::Schema(format!("column `{name}` has unexpected type")))
}

/// Migrate a legacy single-timeline edge table into the bitemporal schema.
///
/// Business time takes over `valid_from/valid_to`, `system_valid_from` is the
/// supplied `system_from`, `system_valid_to` is [`OPEN_ENDED`], `event_time =
/// valid_from`, `ingest_time = system_from` and `business_date` is derived.
/// The legacy columns are copied verbatim, so every existing query keeps its
/// result.
pub fn migrate_legacy_edges(batch: &RecordBatch, system_from: i64) -> Result<RecordBatch> {
    let src = column::<UInt64Array>(batch, "src")?;
    let dst = column::<UInt64Array>(batch, "dst")?;
    let edge_type = column::<UInt16Array>(batch, "edge_type")?;
    let valid_from = column::<TimestampNanosecondArray>(batch, bitemporal_cols::VALID_FROM)?;
    let valid_to = column::<TimestampNanosecondArray>(batch, bitemporal_cols::VALID_TO)?;
    let n = batch.num_rows();

    let from: Vec<i64> = valid_from.values().to_vec();
    let to: Vec<i64> = valid_to.values().to_vec();
    let system_from_col: Vec<i64> = vec![system_from; n];
    let system_to_col: Vec<i64> = vec![OPEN_ENDED; n];
    let business_date_col: Vec<i32> = from.iter().copied().map(business_date).collect();

    let cols: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(src.values().to_vec())),
        Arc::new(UInt64Array::from(dst.values().to_vec())),
        Arc::new(UInt16Array::from(edge_type.values().to_vec())),
        Arc::new(TimestampNanosecondArray::from(from.clone())),
        Arc::new(TimestampNanosecondArray::from(to)),
        Arc::new(TimestampNanosecondArray::from(from.clone())),
        Arc::new(TimestampNanosecondArray::from(
            valid_to.values().to_vec(),
        )),
        Arc::new(TimestampNanosecondArray::from(system_from_col.clone())),
        Arc::new(TimestampNanosecondArray::from(system_to_col)),
        Arc::new(TimestampNanosecondArray::from(from)),
        Arc::new(TimestampNanosecondArray::from(system_from_col)),
        Arc::new(Date32Array::from(business_date_col)),
    ];
    Ok(RecordBatch::try_new(bitemporal_edge_schema(), cols)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn legacy_batch() -> RecordBatch {
        crate::table::EdgeTable::from_vecs(
            vec![1, 2, 3],
            vec![10, 20, 30],
            vec![0, 0, 0],
            vec![0, 100, 200],
            vec![100, 200, 300],
        )
        .unwrap()
        .batch()
        .clone()
    }

    #[test]
    fn range_half_open_and_axes() {
        let r = BitemporalRange::new(10, 20, 100, 200);
        assert!(r.contains_business(10));
        assert!(r.contains_business(19));
        assert!(!r.contains_business(20));
        assert!(r.contains(15, 150));
        assert!(!r.contains(15, 200));
        assert!(r.contains_system(199));
        assert!(!r.contains_system(1000));
        let current = BitemporalRange::from_legacy(0, 10, 5);
        assert!(current.is_current());
    }

    #[test]
    fn overlap_requires_both_axes() {
        let a = BitemporalRange::new(0, 10, 0, 10);
        let b = BitemporalRange::new(5, 15, 5, 15);
        let c = BitemporalRange::new(5, 15, 20, 30); // business overlaps, system doesn't
        assert!(a.overlaps(&b));
        assert!(!a.overlaps(&c));
        assert!(a.business_overlaps(&c));
        assert!(!a.system_overlaps(&c));
    }

    #[test]
    fn find_overlaps_groups_by_key() {
        let keys = vec![1u64, 1, 1, 2];
        let ranges = vec![
            BitemporalRange::new(0, 10, 0, 100),
            BitemporalRange::new(5, 15, 0, 100),
            BitemporalRange::new(20, 30, 0, 100),
            BitemporalRange::new(0, 10, 0, 100),
        ];
        let found = find_overlaps(&keys, &ranges);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].key, 1);
        assert_eq!((found[0].first, found[0].second), (0, 1));
        assert_eq!((found[0].business_from, found[0].business_to), (5, 10));
    }

    #[test]
    fn migration_is_result_preserving() {
        let legacy = legacy_batch();
        let migrated = migrate_legacy_edges(&legacy, 1_000).unwrap();
        assert_eq!(migrated.num_rows(), 3);
        // Legacy columns are byte-for-byte identical, so old queries match.
        for name in ["src", "dst", "edge_type", "valid_from", "valid_to"] {
            assert_eq!(
                migrated.column_by_name(name).unwrap(),
                legacy.column_by_name(name).unwrap(),
                "legacy column {name} changed"
            );
        }
        let sys_from = migrated
            .column_by_name(bitemporal_cols::SYSTEM_FROM)
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(sys_from.value(0), 1_000);
        let bus_from = migrated
            .column_by_name(bitemporal_cols::BUSINESS_FROM)
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(bus_from.value(0), 0);
        assert_eq!(bus_from.value(1), 100);
        let bdate = migrated
            .column_by_name(bitemporal_cols::BUSINESS_DATE)
            .unwrap()
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        assert_eq!(bdate.value(0), 0);
    }

    #[test]
    fn business_date_floors_for_negative_times() {
        assert_eq!(business_date(0), 0);
        assert_eq!(business_date(NS_PER_DAY), 1);
        assert_eq!(business_date(-1), -1);
    }
}
