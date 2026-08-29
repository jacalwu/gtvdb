//! Zone-map index for temporal edge filtering.
//!
//! The "active at `T`" predicate (`valid_from <= T < valid_to`) over a large
//! edge table is O(n) as a full scan. When edges carry temporal locality, a
//! per-chunk zone map (min `valid_from`, max `valid_to`) lets us skip any chunk
//! whose interval does not contain `T`, dropping the work to O(chunks + active)
//! without changing the result.

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
}
