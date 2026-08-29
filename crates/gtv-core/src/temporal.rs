//! Zone-map index for temporal edge filtering.
//!
//! The "active at `T`" predicate (`valid_from <= T < valid_to`) over a large
//! edge table is O(n) as a full scan. When edges carry temporal locality, a
//! per-chunk zone map (min `valid_from`, max `valid_to`) lets us skip any chunk
//! whose interval does not contain `T`, dropping the work to O(chunks + active)
//! without changing the result.

use std::ops::Range;

use arrow::array::BooleanArray;
use arrow::buffer::{BooleanBuffer, MutableBuffer};

/// Per-chunk temporal bounds used to prune the active-at-`T` scan.
///
/// A chunk covers a contiguous `[offset, offset + len)` run of edges; `min_from`
/// is the smallest `valid_from` and `max_to` the largest `valid_to` in it.
#[derive(Clone, Copy, Debug)]
pub struct ZoneMap {
    pub offset: usize,
    pub len: usize,
    pub min_from: i64,
    pub max_to: i64,
}

impl ZoneMap {
    /// True when no edge in this chunk can be active at `valid_at`: either
    /// every edge starts after `valid_at`, or every edge has already ended.
    #[inline]
    pub fn excludes(&self, valid_at: i64) -> bool {
        valid_at < self.min_from || valid_at >= self.max_to
    }
}

/// Build one [`ZoneMap`] per fixed-size chunk over the parallel slices.
///
/// `valid_from` and `valid_to` must be the same length. The chunk size trades
/// zone-map memory (smaller chunks = finer pruning but more zones) against the
/// per-query zone scan; a power-of-two around the cache line (e.g. 128) is a
/// reasonable default.
pub fn build_zone_maps(
    valid_from: &[i64],
    valid_to: &[i64],
    chunk_size: usize,
) -> Vec<ZoneMap> {
    assert_eq!(valid_from.len(), valid_to.len());
    assert!(chunk_size > 0, "chunk_size must be positive");
    let n = valid_from.len();
    let num_chunks = n / chunk_size + usize::from(n % chunk_size != 0);
    let mut zones = Vec::with_capacity(num_chunks);
    let mut offset = 0;
    while offset < n {
        let len = chunk_size.min(n - offset);
        let mut min_from = i64::MAX;
        let mut max_to = i64::MIN;
        for &v in &valid_from[offset..offset + len] {
            min_from = min_from.min(v);
        }
        for &v in &valid_to[offset..offset + len] {
            max_to = max_to.max(v);
        }
        zones.push(ZoneMap {
            offset,
            len,
            min_from,
            max_to,
        });
        offset += len;
    }
    zones
}

/// Active-at-`T` mask, pruning whole chunks whose bounds exclude `T`.
///
/// Produces a zero-allocation `BooleanArray` from a single pre-zeroed bit
/// buffer. Bits are only written for chunks that may contain active edges;
/// skipped chunks remain `false` without ever reading their `valid_from` /
/// `valid_to` values.
pub fn temporal_mask_pruned(
    valid_from: &[i64],
    valid_to: &[i64],
    valid_at: i64,
    zones: &[ZoneMap],
) -> BooleanArray {
    let n = valid_from.len();
    let byte_len = n / 8 + usize::from(n % 8 != 0);
    let mut bytes = MutableBuffer::new(byte_len);
    bytes.resize(byte_len, 0u8);
    let bits = bytes.as_slice_mut();

    for z in zones {
        if z.excludes(valid_at) {
            continue;
        }
        for i in z.offset..z.offset + z.len {
            if valid_from[i] <= valid_at && valid_at < valid_to[i] {
                bits[i >> 3] |= 1u8 << (i & 7);
            }
        }
    }

    BooleanArray::new(BooleanBuffer::new(bytes.into(), 0, n), None)
}

/// O(log N) point-in-time snapshot for temporally-sorted inputs.
///
/// When `valid_from` and `valid_to` are both sorted ascending (the common HFT
/// case: time-ordered records), the set of edges active at `valid_at`
/// (`valid_from <= valid_at < valid_to`) is a *contiguous* index range:
/// `valid_from <= valid_at` selects a prefix `[0, hi)` and `valid_to > valid_at`
/// selects a suffix `[lo, n)`. Two branchless [`slice::partition_point`] calls
/// locate the boundaries with **zero writes** — no bitmask is materialized and
/// only ~2·log₂(n) cache lines are touched (memory0copy.md: index-only,
/// zero-copy).
///
/// Returns the half-open `[lo, hi)` range of active rows (empty when `lo >= hi`).
pub fn point_in_time_range(
    valid_from: &[i64],
    valid_to: &[i64],
    valid_at: i64,
) -> Range<usize> {
    debug_assert_eq!(valid_from.len(), valid_to.len());
    // Exclusive upper bound: first index where valid_from > valid_at.
    let hi = valid_from.partition_point(|&f| f <= valid_at);
    // Exclusive lower bound: rows with valid_to <= valid_at are inactive; the
    // active rows (valid_to > valid_at) begin at this index.
    let lo = valid_to.partition_point(|&v| v <= valid_at);
    lo..hi
}

/// Zero-copy payload slice for the active-at-`valid_at` window.
///
/// Same sortedness requirement as [`point_in_time_range`]. `payloads` is
/// indexed by row (aligned with `valid_from`/`valid_to`); the returned slice
/// borrows the active rows — no allocation, no copy, no write-back.
pub fn point_in_time_slice<'a, T>(
    valid_from: &[i64],
    valid_to: &[i64],
    payloads: &'a [T],
    valid_at: i64,
) -> &'a [T] {
    debug_assert_eq!(valid_from.len(), payloads.len());
    let range = point_in_time_range(valid_from, valid_to, valid_at);
    &payloads[range]
}

/// Generic zero-copy slice over monotonic keys.
///
/// Returns the contiguous `payloads` run whose keys fall in the inclusive
/// `[start_key, end_key]` range, located with two `partition_point` binary
/// searches (O(log n), ~2·log₂(n) cache-line reads, zero writes). `keys` must
/// be sorted ascending and aligned with `payloads`.
pub fn binary_slice<'a, K: Ord, T>(
    keys: &[K],
    payloads: &'a [T],
    start_key: &K,
    end_key: &K,
) -> &'a [T] {
    debug_assert_eq!(keys.len(), payloads.len());
    let start = keys.partition_point(|x| x < start_key);
    let end = keys[start..].partition_point(|x| x <= end_key) + start;
    &payloads[start..end]
}

/// Unpruned baseline: the same predicate over every edge, no zone map.
pub fn temporal_mask_full(valid_from: &[i64], valid_to: &[i64], valid_at: i64) -> BooleanArray {
    let n = valid_from.len();
    let byte_len = n / 8 + usize::from(n % 8 != 0);
    let mut bytes = MutableBuffer::new(byte_len);
    bytes.resize(byte_len, 0u8);
    let bits = bytes.as_slice_mut();
    for i in 0..n {
        if valid_from[i] <= valid_at && valid_at < valid_to[i] {
            bits[i >> 3] |= 1u8 << (i & 7);
        }
    }
    BooleanArray::new(BooleanBuffer::new(bytes.into(), 0, n), None)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn count_true(mask: &BooleanArray) -> usize {
        (0..mask.len()).filter(|&i| mask.value(i)).count()
    }

    #[test]
    fn pruned_matches_full_across_queries() {
        // Ascending starts, fixed duration -> strong temporal locality.
        let n = 10_000i64;
        let duration = 50i64;
        let valid_from: Vec<i64> = (0..n).collect();
        let valid_to: Vec<i64> = valid_from.iter().map(|&f| f + duration).collect();
        let zones = build_zone_maps(&valid_from, &valid_to, 128);

        for t in [-10, 0, 1, 49, 50, 51, n / 2, n + duration + 5] {
            let full = temporal_mask_full(&valid_from, &valid_to, t);
            let pruned = temporal_mask_pruned(&valid_from, &valid_to, t, &zones);
            assert_eq!(full, pruned, "masks diverge at T={t}");
        }
    }

    #[test]
    fn half_open_boundaries() {
        // A single edge [10, 20): active at 10..=19, gone at 20.
        let from = vec![10i64];
        let to = vec![20i64];
        let zones = build_zone_maps(&from, &to, 1);
        assert_eq!(count_true(&temporal_mask_pruned(&from, &to, 10, &zones)), 1);
        assert_eq!(count_true(&temporal_mask_pruned(&from, &to, 19, &zones)), 1);
        assert_eq!(count_true(&temporal_mask_pruned(&from, &to, 20, &zones)), 0);
        assert_eq!(count_true(&temporal_mask_pruned(&from, &to, 9, &zones)), 0);
    }

    #[test]
    fn empty_input() {
        let from: Vec<i64> = vec![];
        let to: Vec<i64> = vec![];
        let zones = build_zone_maps(&from, &to, 128);
        assert!(zones.is_empty());
        assert_eq!(temporal_mask_pruned(&from, &to, 0, &zones).len(), 0);
        assert_eq!(temporal_mask_full(&from, &to, 0).len(), 0);
    }

    #[test]
    fn zone_map_bounds_are_min_from_max_to() {
        let from = vec![5i64, 1, 9, 3];
        let to = vec![10i64, 20, 15, 30];
        let zones = build_zone_maps(&from, &to, 2);
        assert_eq!(zones.len(), 2);
        assert_eq!((zones[0].min_from, zones[0].max_to), (1, 20));
        assert_eq!((zones[1].min_from, zones[1].max_to), (3, 30));
    }

    #[test]
    fn point_in_time_range_matches_full_mask() {
        let n = 10_000i64;
        let duration = 100i64;
        let from: Vec<i64> = (0..n).collect();
        let to: Vec<i64> = from.iter().map(|&f| f + duration).collect();
        let payloads: Vec<i64> = (1000..1000 + n).collect();

        for t in [-10, 0, 1, 99, 100, 101, n / 2, n + duration + 5] {
            let full = temporal_mask_full(&from, &to, t);
            let (f_lo, f_hi) = (0..full.len() as usize).fold(
                (usize::MAX, 0usize),
                |(lo, hi), i| {
                    if full.value(i) {
                        (lo.min(i), hi.max(i + 1))
                    } else {
                        (lo, hi)
                    }
                },
            );
            let range = point_in_time_range(&from, &to, t);
            if f_lo == usize::MAX {
                assert!(range.is_empty(), "range not empty at T={t}");
            } else {
                assert_eq!(range, f_lo..f_hi, "range mismatch at T={t}");
            }
            // Zero-copy slice must match the contiguous payload run.
            let slice = point_in_time_slice(&from, &to, &payloads, t);
            assert_eq!(slice.len(), range.len());
            assert_eq!(slice, &payloads[range.clone()]);
        }
    }

    #[test]
    fn binary_slice_locates_inclusive_bounds() {
        let keys = vec![0i64, 5, 10, 15, 20, 25];
        let payloads = vec!["a", "b", "c", "d", "e", "f"];
        assert_eq!(binary_slice(&keys, &payloads, &10, &20), &["c", "d", "e"]);
        assert_eq!(binary_slice(&keys, &payloads, &6, &14), &["c"]);
        assert_eq!(binary_slice(&keys, &payloads, &21, &30), &["f"]);
        assert!(binary_slice(&keys, &payloads, &26, &30).is_empty());
    }
}
