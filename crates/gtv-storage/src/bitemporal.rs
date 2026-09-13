//! Bitemporal store (prod_p3 B3-2).
//!
//! A table is a sequence of append-only **system versions**: each
//! [`record`](BitemporalStore::record) call captures the full table as known at
//! a system timestamp. Rows inside a version carry their own **business**
//! interval (`business_valid_from/business_valid_to`, or the legacy
//! `valid_from/valid_to`).
//!
//! * `as_of_system(t)` replays "what the system knew at `t`" — the newest
//!   version with `system_from <= t`;
//! * `as_of(business, system)` additionally keeps only rows active at the
//!   business instant (two independent axes);
//! * corrections append a new version and never mutate an older one, so a
//!   query pinned to an old system time is stable forever.
//!
//! The temporal CSR keeps indexing business time only; this store provides the
//! system-time axis by versioning whole tables.

use std::collections::HashMap;

use arrow::array::{BooleanArray, RecordBatch, TimestampNanosecondArray, UInt64Array};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{Schema, SchemaRef};
use gtv_core::bitemporal::{bitemporal_cols, find_overlaps, BitemporalRange, Overlap, OPEN_ENDED};

use crate::error::{Result, StorageError};

/// One immutable system-time version of a table.
#[derive(Debug, Clone)]
struct SystemVersion {
    system_from: i64,
    batches: Vec<RecordBatch>,
}

#[derive(Debug, Clone)]
struct TableVersions {
    schema: SchemaRef,
    versions: Vec<SystemVersion>,
}

/// Append-only, multi-versioned, bitemporal table store.
#[derive(Debug, Default, Clone)]
pub struct BitemporalStore {
    tables: HashMap<String, TableVersions>,
}

/// Which pair of columns carries business time for a schema.
fn business_columns(schema: &Schema) -> Result<(&'static str, &'static str)> {
    if schema.index_of(bitemporal_cols::BUSINESS_FROM).is_ok()
        && schema.index_of(bitemporal_cols::BUSINESS_TO).is_ok()
    {
        Ok((
            bitemporal_cols::BUSINESS_FROM,
            bitemporal_cols::BUSINESS_TO,
        ))
    } else if schema.index_of(bitemporal_cols::VALID_FROM).is_ok()
        && schema.index_of(bitemporal_cols::VALID_TO).is_ok()
    {
        Ok((bitemporal_cols::VALID_FROM, bitemporal_cols::VALID_TO))
    } else {
        Err(StorageError::Msg(format!(
            "table has no business-time columns (`{}`/`{}` or legacy `{}`/`{}`)",
            bitemporal_cols::BUSINESS_FROM,
            bitemporal_cols::BUSINESS_TO,
            bitemporal_cols::VALID_FROM,
            bitemporal_cols::VALID_TO,
        )))
    }
}

fn timestamp_col<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a TimestampNanosecondArray> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|_| StorageError::Msg(format!("missing column `{name}`")))?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .ok_or_else(|| StorageError::Msg(format!("column `{name}` is not a timestamp")))
}

impl BitemporalStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append the system version effective at `system_from`.
    ///
    /// Re-recording the *same* `system_from` replaces only that version
    /// (idempotent replay); every other version is untouched. This is the
    /// correction path: corrections always append a new system time.
    pub fn record(
        &mut self,
        table: &str,
        system_from: i64,
        batches: Vec<RecordBatch>,
    ) -> Result<()> {
        let first = batches
            .first()
            .ok_or_else(|| StorageError::Msg(format!("system version of `{table}` is empty")))?;
        let schema = first.schema();
        let versions = self
            .tables
            .entry(table.to_string())
            .or_insert_with(|| TableVersions {
                schema: schema.clone(),
                versions: Vec::new(),
            });
        if versions.schema.as_ref() != schema.as_ref() {
            return Err(StorageError::Msg(format!(
                "schema mismatch for `{table}` at system_from={system_from}"
            )));
        }
        match versions
            .versions
            .iter_mut()
            .find(|v| v.system_from == system_from)
        {
            Some(v) => v.batches = batches,
            None => {
                versions.versions.push(SystemVersion {
                    system_from,
                    batches,
                });
                versions.versions.sort_by_key(|v| v.system_from);
            }
        }
        Ok(())
    }

    fn versions(&self, table: &str) -> Result<&TableVersions> {
        self.tables
            .get(table)
            .ok_or_else(|| StorageError::Msg(format!("unknown bitemporal table `{table}`")))
    }

    /// Newest version with `system_from <= system_ts`.
    fn version_at(&self, table: &str, system_ts: i64) -> Result<&SystemVersion> {
        self.versions(table)?
            .versions
            .iter()
            .rev()
            .find(|v| v.system_from <= system_ts)
            .ok_or_else(|| {
                StorageError::Msg(format!(
                    "no system version of `{table}` at or before system_ts={system_ts}"
                ))
            })
    }

    /// "What the system knew at `system_ts`" (business time untouched).
    pub fn as_of_system(&self, table: &str, system_ts: i64) -> Result<Vec<RecordBatch>> {
        Ok(self.version_at(table, system_ts)?.batches.clone())
    }

    /// Both axes: pick the system version known at `system_ts`, then keep only
    /// rows whose business interval contains `business_ts`.
    pub fn as_of(&self, table: &str, business_ts: i64, system_ts: i64) -> Result<Vec<RecordBatch>> {
        let version = self.version_at(table, system_ts)?;
        filter_business(&version.batches, business_ts)
    }

    /// Business slice on the latest known version.
    pub fn business_at(&self, table: &str, business_ts: i64) -> Result<Vec<RecordBatch>> {
        let versions = self.versions(table)?;
        let latest = versions
            .versions
            .last()
            .ok_or_else(|| StorageError::Msg(format!("`{table}` has no versions")))?;
        filter_business(&latest.batches, business_ts)
    }

    /// System timestamps recorded for a table, ascending.
    pub fn timestamps(&self, table: &str) -> Result<Vec<i64>> {
        Ok(self
            .versions(table)?
            .versions
            .iter()
            .map(|v| v.system_from)
            .collect())
    }

    pub fn schema(&self, table: &str) -> Option<SchemaRef> {
        self.tables.get(table).map(|v| v.schema.clone())
    }

    pub fn table_names(&self) -> impl Iterator<Item = &str> {
        self.tables.keys().map(|s| s.as_str())
    }

    /// Contradictory rows for the same `key_column`: overlapping business *and*
    /// system intervals inside one system version. A correction at a later
    /// system time does not trip this (its system interval starts later), so
    /// only genuinely inconsistent data is flagged.
    pub fn overlaps(&self, table: &str, key_column: &str) -> Result<Vec<Overlap>> {
        let versions = self.versions(table)?;
        let mut out = Vec::new();
        for version in &versions.versions {
            let next_from = versions
                .versions
                .iter()
                .find(|v| v.system_from > version.system_from)
                .map(|v| v.system_from)
                .unwrap_or(OPEN_ENDED);
            for batch in &version.batches {
                let key_idx = batch.schema().index_of(key_column).map_err(|_| {
                    StorageError::Msg(format!("missing key column `{key_column}`"))
                })?;
                let keys: Vec<u64> = batch
                    .column(key_idx)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| {
                        StorageError::Msg(format!("key column `{key_column}` is not UInt64"))
                    })?
                    .values()
                    .to_vec();
                let (bf, bt) = business_columns(batch.schema().as_ref())?;
                let bf = timestamp_col(batch, bf)?;
                let bt = timestamp_col(batch, bt)?;
                let ranges: Vec<BitemporalRange> = (0..batch.num_rows())
                    .map(|i| {
                        BitemporalRange::new(
                            bf.value(i),
                            bt.value(i),
                            version.system_from,
                            next_from,
                        )
                    })
                    .collect();
                out.extend(find_overlaps(&keys, &ranges));
            }
        }
        Ok(out)
    }
}

/// Keep only rows whose business interval contains `business_ts`.
fn filter_business(batches: &[RecordBatch], business_ts: i64) -> Result<Vec<RecordBatch>> {
    let mut out = Vec::new();
    for batch in batches {
        let (from_name, to_name) = business_columns(batch.schema().as_ref())?;
        let from = timestamp_col(batch, from_name)?;
        let to = timestamp_col(batch, to_name)?;
        let mask = BooleanArray::from(
            (0..batch.num_rows())
                .map(|i| from.value(i) <= business_ts && business_ts < to.value(i))
                .collect::<Vec<_>>(),
        );
        let filtered = filter_record_batch(batch, &mask)?;
        if filtered.num_rows() > 0 {
            out.push(filtered);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use gtv_core::bitemporal::migrate_legacy_edges;
    use gtv_core::EdgeTable;

    fn legacy(src: Vec<u64>, dst: Vec<u64>, from: Vec<i64>, to: Vec<i64>) -> RecordBatch {
        EdgeTable::from_vecs(
            src,
            dst,
            vec![0; from.len()],
            from,
            to,
        )
        .unwrap()
        .batch()
        .clone()
    }

    fn srcs(batches: &[RecordBatch]) -> Vec<u64> {
        let mut out = Vec::new();
        for b in batches {
            let a = b.column_by_name("src").unwrap().as_any().downcast_ref::<UInt64Array>().unwrap();
            out.extend((0..b.num_rows()).map(|i| a.value(i)));
        }
        out
    }

    #[test]
    fn system_slice_is_replayable_after_correction() {
        let mut store = BitemporalStore::new();
        store
            .record("edges", 1_000, vec![migrate_legacy_edges(&legacy(vec![1], vec![2], vec![0], vec![100]), 1_000).unwrap()])
            .unwrap();
        let before = store.as_of_system("edges", 1_500).unwrap();
        assert_eq!(srcs(&before), vec![1]);

        // Correction: a new system version (same key, different target).
        store
            .record("edges", 2_000, vec![migrate_legacy_edges(&legacy(vec![1], vec![9], vec![0], vec![100]), 2_000).unwrap()])
            .unwrap();

        // The old system cut is unchanged...
        let before_again = store.as_of_system("edges", 1_500).unwrap();
        assert_eq!(srcs(&before_again), vec![1]);
        assert_eq!(
            before_again[0].column_by_name("dst").unwrap(),
            before[0].column_by_name("dst").unwrap()
        );
        // ...while the new cut sees the correction.
        let after = store.as_of_system("edges", 2_500).unwrap();
        let dst = after[0].column_by_name("dst").unwrap().as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(dst.value(0), 9);
    }

    #[test]
    fn business_and_system_axes_are_independent() {
        let mut store = BitemporalStore::new();
        store
            .record("edges", 1_000, vec![migrate_legacy_edges(&legacy(vec![1, 2], vec![10, 20], vec![0, 500], vec![500, 1000]), 1_000).unwrap()])
            .unwrap();

        // Business 100 hits only edge 1 in either system cut.
        assert_eq!(srcs(&store.as_of("edges", 100, 1_500).unwrap()), vec![1]);
        assert_eq!(srcs(&store.as_of("edges", 100, 9_999).unwrap()), vec![1]);
        // Business 600 hits only edge 2.
        assert_eq!(srcs(&store.as_of("edges", 600, 1_500).unwrap()), vec![2]);
        // A business cut before any row is empty.
        assert!(store.as_of("edges", 4_000, 1_500).unwrap().is_empty());
    }

    #[test]
    fn corrections_do_not_erase_history() {
        let mut store = BitemporalStore::new();
        for t in [1_000i64, 2_000, 3_000] {
            store
                .record("edges", t, vec![migrate_legacy_edges(&legacy(vec![1], vec![t as u64], vec![0], vec![100]), t).unwrap()])
                .unwrap();
        }
        assert_eq!(store.timestamps("edges").unwrap(), vec![1_000, 2_000, 3_000]);
        for t in [1_000i64, 2_000, 3_000] {
            let dst = store.as_of_system("edges", t).unwrap()[0]
                .column_by_name("dst")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0);
            assert_eq!(dst, t as u64);
        }
    }

    #[test]
    fn overlaps_detects_contradictory_versions() {
        let mut store = BitemporalStore::new();
        // Same key, overlapping business intervals, same system version.
        let b = migrate_legacy_edges(&legacy(vec![1, 1], vec![10, 20], vec![0, 5], vec![100, 100]), 1_000).unwrap();
        store.record("edges", 1_000, vec![b]).unwrap();
        let found = store.overlaps("edges", "src").unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].key, 1);
    }

    #[test]
    fn correction_at_new_system_time_is_not_an_overlap() {
        let mut store = BitemporalStore::new();
        store
            .record("edges", 1_000, vec![migrate_legacy_edges(&legacy(vec![1], vec![10], vec![0], vec![100]), 1_000).unwrap()])
            .unwrap();
        store
            .record("edges", 2_000, vec![migrate_legacy_edges(&legacy(vec![1], vec![20], vec![0], vec![100]), 2_000).unwrap()])
            .unwrap();
        assert!(store.overlaps("edges", "src").unwrap().is_empty());
    }
}
