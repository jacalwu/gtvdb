//! Immutable in-memory Temporal-CSR index.
//!
//! # Adaptive temporal lookup (B1-2)
//!
//! Edges are sorted by `(src, valid_from, valid_to, dst)`, so each source's run
//! is ordered by `valid_from`. Neighbor lookup uses two prunings:
//!
//! 1. **Binary search on `valid_from`** — only edges with `valid_from <= T` can
//!    be active, so everything after that point is skipped outright.
//! 2. **Chunk zone maps** — the run is split into [`CHUNK_SIZE`] chunks, each
//!    storing the maximum `valid_to`. A chunk whose max `valid_to <= T` is
//!    entirely expired and skipped without touching its edges. (`valid_to` is
//!    not monotonic inside a run, which is why a zone map is needed instead of a
//!    second binary search.)
//!
//! Low-degree nodes keep the simple linear scan (fastest for tiny runs).

use std::sync::Arc;

use arrow::array::{ArrayRef, TimestampNanosecondArray, UInt16Array, UInt64Array};
use arrow::record_batch::RecordBatch;

use crate::error::{GtvError, Result};
use crate::table::edge_schema;
use crate::traits::TemporalGraphIndex;
use crate::traversal::{
    BudgetTracker, CancelToken, EdgePredicate, KhopResult, TraversalBudget, VisitedSet,
};

/// Edges per zone-map chunk.
pub const CHUNK_SIZE: usize = 64;
/// Runs below this degree use the plain linear scan.
pub const LINEAR_SCAN_MAX_DEGREE: u32 = 16;

/// A single neighbor edge resolved at a point in time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Neighbor {
    pub dst: u64,
    pub edge_type: u16,
    pub valid_from: i64,
    pub valid_to: i64,
}

/// Which lookup strategy a `(src, T)` query used (for telemetry / tests).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NeighborStrategy {
    /// Linear scan over the whole source run.
    LinearScan,
    /// Binary search on `valid_from` + chunk zone maps on `valid_to`.
    BinarySearchZoneMap,
}

/// BFS expansion direction for [`TemporalCSR::khop_directed`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectionMode {
    /// Expand the frontier edge by edge (`|frontier| × degree`).
    Push,
    /// Scan unvisited nodes and pull from their in-neighbours (`V + E_rev`).
    Pull,
    /// Pick the cheaper direction per hop (needs the transposed graph).
    Auto,
}

/// Aggregate index statistics.
#[derive(Debug, Clone)]
pub struct TemporalCsrStats {
    pub node_count: usize,
    pub edge_count: usize,
    pub chunk_count: usize,
    pub max_degree: u32,
    /// `hist[k]` = number of nodes whose degree falls in the bucket with
    /// `floor(log2(degree)) == k`; `hist[0]` also holds degree 0 and 1.
    pub degree_histogram: Vec<u64>,
}

#[derive(Debug, Clone, Copy)]
struct EdgeRow {
    src: u64,
    dst: u64,
    valid_from: i64,
    valid_to: i64,
    edge_type: u16,
}

/// Immutable in-memory Temporal-CSR.
///
/// Edges are sorted by `(src, valid_from, valid_to, dst)`; `offsets` partitions
/// the parallel edge arrays into contiguous runs per source node, so a neighbor
/// lookup scans only the run belonging to the queried source node.
#[derive(Debug, Clone)]
pub struct TemporalCSR {
    node_count: usize,
    offsets: Vec<u32>,
    dst: Vec<u64>,
    valid_from: Vec<i64>,
    valid_to: Vec<i64>,
    edge_type: Vec<u16>,
    /// Global temporal bounding box: `max_valid_from` is the largest `valid_from`
    /// over all edges, `min_valid_to` the smallest `valid_to`. Together they let a
    /// caller skip per-edge activity checks when every edge is active at a time.
    max_valid_from: i64,
    min_valid_to: i64,
    /// Edges per chunk (zone-map granularity).
    chunk_size: usize,
    /// `chunk_index[s]..chunk_index[s + 1]` = global chunk ids of source `s`.
    chunk_index: Vec<u32>,
    /// `chunk_max_valid_to[c]` = max `valid_to` over the edges of global chunk `c`.
    chunk_max_valid_to: Vec<i64>,
}

impl TemporalCSR {
    /// Build the index from parallel edge column arrays.
    pub fn from_arrays(
        src: &UInt64Array,
        dst: &UInt64Array,
        valid_from: &TimestampNanosecondArray,
        valid_to: &TimestampNanosecondArray,
        edge_type: &UInt16Array,
        node_count: usize,
    ) -> Result<Self> {
        let n = src.len();
        if dst.len() != n
            || valid_from.len() != n
            || valid_to.len() != n
            || edge_type.len() != n
        {
            return Err(GtvError::InvalidArgument(
                "edge arrays have mismatched lengths".into(),
            ));
        }

        let src_v = src.values().as_ref();
        let dst_v = dst.values().as_ref();
        let vf_v = valid_from.values().as_ref();
        let vt_v = valid_to.values().as_ref();
        let et_v = edge_type.values().as_ref();

        let mut rows: Vec<EdgeRow> = (0..n)
            .map(|i| EdgeRow {
                src: src_v[i],
                dst: dst_v[i],
                valid_from: vf_v[i],
                valid_to: vt_v[i],
                edge_type: et_v[i],
            })
            .collect();

        if let Some(bad) = rows.iter().map(|r| r.src).find(|s| *s >= node_count as u64) {
            return Err(GtvError::NodeOutOfRange(bad));
        }

        rows.sort_unstable_by_key(|r| (r.src, r.valid_from, r.valid_to, r.dst));

        let mut offsets = vec![0u32; node_count + 1];
        for r in &rows {
            offsets[r.src as usize + 1] += 1;
        }
        for i in 0..node_count {
            offsets[i + 1] += offsets[i];
        }

        let mut dst_vec = Vec::with_capacity(n);
        let mut vf_vec = Vec::with_capacity(n);
        let mut vt_vec = Vec::with_capacity(n);
        let mut et_vec = Vec::with_capacity(n);
        let mut max_valid_from = i64::MIN;
        let mut min_valid_to = i64::MAX;
        for r in &rows {
            dst_vec.push(r.dst);
            vf_vec.push(r.valid_from);
            vt_vec.push(r.valid_to);
            et_vec.push(r.edge_type);
            max_valid_from = max_valid_from.max(r.valid_from);
            min_valid_to = min_valid_to.min(r.valid_to);
        }

        // Chunk zone maps: one max-valid_to per CHUNK_SIZE edges of each run.
        let chunk_size = CHUNK_SIZE;
        let mut chunk_index = Vec::with_capacity(node_count + 1);
        let mut chunk_max_valid_to = Vec::new();
        chunk_index.push(0u32);
        for s in 0..node_count {
            let start = offsets[s] as usize;
            let end = offsets[s + 1] as usize;
            let mut c = start;
            while c < end {
                let ce = (c + chunk_size).min(end);
                let mut mx = i64::MIN;
                for &v in &vt_vec[c..ce] {
                    mx = mx.max(v);
                }
                chunk_max_valid_to.push(mx);
                c = ce;
            }
            chunk_index.push(chunk_max_valid_to.len() as u32);
        }

        Ok(Self {
            node_count,
            offsets,
            dst: dst_vec,
            valid_from: vf_vec,
            valid_to: vt_vec,
            edge_type: et_vec,
            max_valid_from,
            min_valid_to,
            chunk_size,
            chunk_index,
            chunk_max_valid_to,
        })
    }

    pub fn node_count(&self) -> usize {
        self.node_count
    }

    pub fn edge_count(&self) -> usize {
        self.dst.len()
    }

    /// Number of zone-map chunks (telemetry).
    pub fn chunk_count(&self) -> usize {
        self.chunk_max_valid_to.len()
    }

    /// True when *every* edge is active at `t` (`valid_from <= t < valid_to`).
    ///
    /// Uses the global temporal bounding box, so this is O(1) and lets hot loops
    /// skip the per-edge activity test — which reads two columns and compares
    /// twice per edge — when the answer is known up front.
    #[inline]
    pub fn all_active_at(&self, t: i64) -> bool {
        t >= self.max_valid_from && t < self.min_valid_to
    }

    #[inline]
    fn edge_range(&self, src: u64) -> Result<(usize, usize)> {
        if src >= self.node_count as u64 {
            return Err(GtvError::NodeOutOfRange(src));
        }
        let s = src as usize;
        Ok((self.offsets[s] as usize, self.offsets[s + 1] as usize))
    }

    /// Degree (out-edge count) of `src`.
    #[inline]
    pub fn degree(&self, src: u64) -> Result<u32> {
        let (start, end) = self.edge_range(src)?;
        Ok((end - start) as u32)
    }

    /// Maximum out-degree over all nodes.
    pub fn max_degree(&self) -> u32 {
        (0..self.node_count)
            .map(|s| self.offsets[s + 1] - self.offsets[s])
            .max()
            .unwrap_or(0)
    }

    /// Power-of-two degree histogram: `hist[k]` counts nodes with
    /// `floor(log2(degree)) == k` (degree 0 and 1 both land in bucket 0).
    pub fn degree_histogram(&self) -> Vec<u64> {
        let mut hist = vec![0u64; 65];
        for s in 0..self.node_count {
            let d = self.offsets[s + 1] - self.offsets[s];
            let bucket = if d <= 1 {
                0
            } else {
                (32 - d.leading_zeros()) as usize
            };
            hist[bucket] += 1;
        }
        hist
    }

    /// Index statistics for telemetry / planning.
    pub fn stats(&self) -> TemporalCsrStats {
        TemporalCsrStats {
            node_count: self.node_count,
            edge_count: self.dst.len(),
            chunk_count: self.chunk_max_valid_to.len(),
            max_degree: self.max_degree(),
            degree_histogram: self.degree_histogram(),
        }
    }

    /// Global position of the chunk containing edge `pos` of source `src`.
    #[inline]
    fn chunk_of(&self, src: usize, pos: usize) -> u32 {
        let start = self.offsets[src] as usize;
        let within = (pos - start) / self.chunk_size;
        self.chunk_index[src] + within as u32
    }

    /// First edge position after the chunk containing `chunk` of source `src`.
    #[inline]
    fn chunk_end(&self, src: usize, chunk: u32) -> usize {
        let base = self.chunk_index[src];
        let within = (chunk - base) as usize;
        let start = self.offsets[src] as usize;
        (start + (within + 1) * self.chunk_size).min(self.offsets[src + 1] as usize)
    }

    /// Parallel raw edge slices for `src`: `(dst, valid_from, valid_to, edge_type)`
    /// covering only that source's contiguous run.
    ///
    /// Pattern matching reads these directly to skip the per-edge [`Neighbor`]
    /// struct and iterator closures of [`neighbors`], which dominate a
    /// multi-million-node scan. The slices are sorted by `(valid_from, valid_to,
    /// dst)` (the build sort key), so callers may also binary-search the active
    /// range.
    #[inline]
    pub fn edge_slices(&self, src: u64) -> Result<(&[u64], &[i64], &[i64], &[u16])> {
        let (start, end) = self.edge_range(src)?;
        Ok((
            &self.dst[start..end],
            &self.valid_from[start..end],
            &self.valid_to[start..end],
            &self.edge_type[start..end],
        ))
    }

    /// First position in `[start, end)` with `valid_from > t` (the run is sorted
    /// by `valid_from`). Everything at or after it is inactive at `t`.
    #[inline]
    fn valid_from_upper_bound(&self, start: usize, end: usize, t: i64) -> usize {
        let mut lo = start;
        let mut hi = end;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.valid_from[mid] <= t {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// Neighbors of `src` active at `valid_at`.
    ///
    /// Uses binary search on `valid_from` plus chunk zone maps on `valid_to`;
    /// low-degree nodes fall back to a linear scan.
    pub fn neighbors(&self, src: u64, valid_at: i64) -> Result<Neighbors<'_>> {
        let (start, end) = self.edge_range(src)?;
        let all_active = self.all_active_at(valid_at);
        let hi = if all_active {
            end
        } else {
            self.valid_from_upper_bound(start, end, valid_at)
        };
        Ok(Neighbors {
            csr: self,
            src: src as usize,
            pos: start,
            hi,
            valid_at,
            all_active,
        })
    }

    /// Like [`neighbors`], also reporting which strategy was selected.
    pub fn neighbors_planned(
        &self,
        src: u64,
        valid_at: i64,
    ) -> Result<(Neighbors<'_>, NeighborStrategy)> {
        let degree = self.degree(src)?;
        let strategy = if degree <= LINEAR_SCAN_MAX_DEGREE {
            NeighborStrategy::LinearScan
        } else {
            NeighborStrategy::BinarySearchZoneMap
        };
        Ok((self.neighbors(src, valid_at)?, strategy))
    }

    /// Fetch neighbors for a batch of source nodes as a `RecordBatch` with the
    /// canonical edge schema.
    pub fn neighbors_record_batch(
        &self,
        src_nodes: &UInt64Array,
        valid_at: i64,
    ) -> Result<RecordBatch> {
        let mut src_out: Vec<u64> = Vec::new();
        let mut dst_out: Vec<u64> = Vec::new();
        let mut et_out: Vec<u16> = Vec::new();
        let mut vf_out: Vec<i64> = Vec::new();
        let mut vt_out: Vec<i64> = Vec::new();

        for &s in src_nodes.values().as_ref() {
            for nb in self.neighbors(s, valid_at)? {
                src_out.push(s);
                dst_out.push(nb.dst);
                et_out.push(nb.edge_type);
                vf_out.push(nb.valid_from);
                vt_out.push(nb.valid_to);
            }
        }

        let batch = RecordBatch::try_new(
            edge_schema(),
            vec![
                Arc::new(UInt64Array::from(src_out)) as ArrayRef,
                Arc::new(UInt64Array::from(dst_out)) as ArrayRef,
                Arc::new(UInt16Array::from(et_out)) as ArrayRef,
                Arc::new(TimestampNanosecondArray::from(vf_out)) as ArrayRef,
                Arc::new(TimestampNanosecondArray::from(vt_out)) as ArrayRef,
            ],
        )?;
        Ok(batch)
    }

    /// k-hop traversal: one frontier per hop (1..=k), each a deduplicated,
    /// sorted array of reached nodes at snapshot time `valid_at`.
    ///
    /// > Resource-bounded traversal (visited bitmap, budgets, cancellation) is
    /// > B1-3 in `prod_p1.md`; this remains the unbounded convenience form.
    pub fn khop(&self, seeds: &UInt64Array, k: usize, valid_at: i64) -> Result<Vec<UInt64Array>> {
        let mut frontiers = Vec::with_capacity(k);
        let mut current: Vec<u64> = seeds.values().as_ref().to_vec();
        for _ in 0..k {
            let mut next: Vec<u64> = Vec::new();
            for &s in &current {
                if s >= self.node_count as u64 {
                    return Err(GtvError::NodeOutOfRange(s));
                }
                for nb in self.neighbors(s, valid_at)? {
                    next.push(nb.dst);
                }
            }
            next.sort_unstable();
            next.dedup();
            frontiers.push(UInt64Array::from(next.clone()));
            current = next;
        }
        Ok(frontiers)
    }

    /// Resource-bounded, push-direction k-hop BFS (B1-3).
    ///
    /// Unlike [`TemporalCSR::khop`], every node is visited at most once via a
    /// global visited bitmap, so the frontier and total work are deterministic
    /// and bounded. Seeds are marked visited but not emitted; each frontier is
    /// sorted ascending.
    ///
    /// * `budget` caps hops / edges / frontier / rows / memory / wall clock;
    /// * `predicate` filters edges before the frontier grows;
    /// * `cancel` aborts cooperatively from another thread.
    pub fn khop_bounded(
        &self,
        seeds: &UInt64Array,
        k: usize,
        valid_at: i64,
        budget: &TraversalBudget,
        predicate: Option<&dyn EdgePredicate>,
        cancel: Option<&CancelToken>,
    ) -> Result<KhopResult> {
        self.khop_directed(
            seeds,
            k,
            valid_at,
            budget,
            predicate,
            cancel,
            None,
            DirectionMode::Push,
        )
    }

    /// Direction-optimizing k-hop BFS (B1-3).
    ///
    /// With a dense frontier, expanding it edge-by-edge (`push`) touches
    /// `|frontier| × degree` edges. The `pull` step instead scans every
    /// unvisited node and stops at its first in-neighbour that is in the
    /// frontier, which costs `V + E_rev` but is independent of the frontier
    /// size. Provide the [`TemporalCSR::transpose`] as `reverse` to enable it.
    ///
    /// [`DirectionMode::Auto`] picks the cheaper direction per hop. Predicates
    /// are evaluated on the *forward* edge (`src -> dst`) in both directions.
    #[allow(clippy::too_many_arguments)]
    pub fn khop_directed(
        &self,
        seeds: &UInt64Array,
        k: usize,
        valid_at: i64,
        budget: &TraversalBudget,
        predicate: Option<&dyn EdgePredicate>,
        cancel: Option<&CancelToken>,
        reverse: Option<&TemporalCSR>,
        mode: DirectionMode,
    ) -> Result<KhopResult> {
        let tracker = BudgetTracker::new(budget.clone(), cancel.cloned());
        tracker.check_time()?;
        let k = k.min(budget.max_hops);
        let mut visited = VisitedSet::new(self.node_count);
        let mut in_frontier = VisitedSet::new(self.node_count);

        let mut current: Vec<u64> = seeds.values().as_ref().to_vec();
        for &s in &current {
            if s >= self.node_count as u64 {
                return Err(GtvError::NodeOutOfRange(s));
            }
        }
        current.sort_unstable();
        current.dedup();
        for &s in &current {
            visited.mark(s);
        }

        let avg_degree = if self.node_count == 0 {
            0.0
        } else {
            self.edge_count() as f64 / self.node_count as f64
        };

        let mut frontiers = Vec::with_capacity(k);
        let mut pending_edges = 0u64;
        for _hop in 0..k {
            tracker.check_time()?;
            if current.is_empty() {
                break;
            }

            if let Some(guard) = budget.max_degree {
                for &s in &current {
                    let deg = self.degree(s)?;
                    if deg > guard && predicate.is_none() {
                        return Err(GtvError::HighDegreeNode {
                            node: s,
                            degree: deg as u64,
                            hint: "add an edge predicate or raise max_degree",
                        });
                    }
                }
            }

            let p = current.len() as f64 / self.node_count.max(1) as f64;
            let use_pull = match mode {
                DirectionMode::Push => false,
                DirectionMode::Pull => true,
                // Direction-optimizing trigger (random-graph model): with a
                // frontier fraction `p`, pull is expected to scan
                // `V(1-p)/p` edges (early exit) vs push's `pV*avg`. Pull wins
                // when `(1-p) < p^2 * avg_degree`.
                DirectionMode::Auto => {
                    reverse.is_some() && (1.0 - p) < p * p * avg_degree.max(1.0)
                }
            };

            let mut next = if use_pull {
                let rev = reverse.ok_or_else(|| {
                    GtvError::InvalidArgument(
                        "pull traversal requires the transposed graph (reverse)".into(),
                    )
                })?;
                self.pull_frontier(
                    rev,
                    &current,
                    valid_at,
                    predicate,
                    &mut visited,
                    &mut in_frontier,
                    &tracker,
                    &mut pending_edges,
                )?
            } else {
                self.push_frontier(
                    &current,
                    valid_at,
                    predicate,
                    &mut visited,
                    &tracker,
                    &mut pending_edges,
                )?
            };

            tracker.add_edges(pending_edges)?;
            pending_edges = 0;
            next.sort_unstable();
            next.dedup();
            if next.is_empty() {
                break;
            }
            tracker.record_frontier(next.len())?;
            tracker.add_rows(next.len() as u64)?;
            let mem = visited.memory_bytes()
                + in_frontier.memory_bytes()
                + (next.len() as u64 + current.len() as u64) * std::mem::size_of::<u64>() as u64;
            tracker.check_memory(mem)?;
            frontiers.push(UInt64Array::from(next.clone()));
            current = next;
        }
        let stats = tracker.stats(frontiers.len());
        Ok(KhopResult { frontiers, stats })
    }

    /// Push step: expand `current` edge by edge into the next frontier.
    #[allow(clippy::too_many_arguments)]
    fn push_frontier(
        &self,
        current: &[u64],
        valid_at: i64,
        predicate: Option<&dyn EdgePredicate>,
        visited: &mut VisitedSet,
        tracker: &BudgetTracker,
        pending_edges: &mut u64,
    ) -> Result<Vec<u64>> {
        let mut next: Vec<u64> = Vec::new();
        for &s in current {
            for nb in self.neighbors(s, valid_at)? {
                *pending_edges += 1;
                if *pending_edges >= 4096 {
                    tracker.add_edges(*pending_edges)?;
                    *pending_edges = 0;
                    tracker.check_time()?;
                }
                if let Some(p) = predicate {
                    if !p.keep(&nb) {
                        continue;
                    }
                }
                if visited.mark(nb.dst) {
                    next.push(nb.dst);
                }
            }
        }
        Ok(next)
    }

    /// Pull step: scan every unvisited node and stop at its first in-neighbour
    /// that is in `current` (requires the transposed graph).
    #[allow(clippy::too_many_arguments)]
    fn pull_frontier(
        &self,
        reverse: &TemporalCSR,
        current: &[u64],
        valid_at: i64,
        predicate: Option<&dyn EdgePredicate>,
        visited: &mut VisitedSet,
        in_frontier: &mut VisitedSet,
        tracker: &BudgetTracker,
        pending_edges: &mut u64,
    ) -> Result<Vec<u64>> {
        in_frontier.reset();
        for &s in current {
            in_frontier.mark(s);
        }
        let mut next: Vec<u64> = Vec::new();
        for u in 0..self.node_count as u64 {
            if visited.seen(u) {
                continue;
            }
            for nb in reverse.neighbors(u, valid_at)? {
                *pending_edges += 1;
                if *pending_edges >= 4096 {
                    tracker.add_edges(*pending_edges)?;
                    *pending_edges = 0;
                    tracker.check_time()?;
                }
                // `nb.dst` is the original source; must be in the frontier.
                if !in_frontier.seen(nb.dst) {
                    continue;
                }
                // Evaluate the predicate on the forward edge (src=nb.dst -> dst=u).
                let edge = Neighbor {
                    dst: u,
                    edge_type: nb.edge_type,
                    valid_from: nb.valid_from,
                    valid_to: nb.valid_to,
                };
                if let Some(p) = predicate {
                    if !p.keep(&edge) {
                        continue;
                    }
                }
                visited.mark(u);
                next.push(u);
                break;
            }
        }
        Ok(next)
    }

    /// Build the transposed (reverse) CSR: every edge `s -> d` becomes `d -> s`
    /// with the same `valid_from` / `valid_to` / `edge_type`. Used by the pull /
    /// direction-optimizing traversal and as a general reverse-index primitive.
    pub fn transpose(&self) -> Result<TemporalCSR> {
        let n = self.dst.len();
        let mut src = Vec::with_capacity(n);
        let mut dst = Vec::with_capacity(n);
        let mut vf = Vec::with_capacity(n);
        let mut vt = Vec::with_capacity(n);
        let mut et = Vec::with_capacity(n);
        for s in 0..self.node_count {
            let (a, b) = self.edge_range(s as u64)?;
            for e in a..b {
                src.push(self.dst[e]);
                dst.push(s as u64);
                vf.push(self.valid_from[e]);
                vt.push(self.valid_to[e]);
                et.push(self.edge_type[e]);
            }
        }
        TemporalCSR::from_arrays(
            &UInt64Array::from(src),
            &UInt64Array::from(dst),
            &TimestampNanosecondArray::from(vf),
            &TimestampNanosecondArray::from(vt),
            &UInt16Array::from(et),
            self.node_count,
        )
    }
}

/// Iterator over the neighbors of one source that are active at `valid_at`.
///
/// Yielded in `(valid_from, valid_to, dst)` order (the run's sort order).
pub struct Neighbors<'a> {
    csr: &'a TemporalCSR,
    src: usize,
    pos: usize,
    hi: usize,
    valid_at: i64,
    all_active: bool,
}

impl Iterator for Neighbors<'_> {
    type Item = Neighbor;

    #[inline]
    fn next(&mut self) -> Option<Neighbor> {
        while self.pos < self.hi {
            if !self.all_active {
                let chunk = self.csr.chunk_of(self.src, self.pos);
                if self.csr.chunk_max_valid_to[chunk as usize] <= self.valid_at {
                    self.pos = self.csr.chunk_end(self.src, chunk).min(self.hi);
                    continue;
                }
            }
            let e = self.pos;
            self.pos += 1;
            if self.all_active
                || (self.csr.valid_from[e] <= self.valid_at
                    && self.valid_at < self.csr.valid_to[e])
            {
                return Some(Neighbor {
                    dst: self.csr.dst[e],
                    edge_type: self.csr.edge_type[e],
                    valid_from: self.csr.valid_from[e],
                    valid_to: self.csr.valid_to[e],
                });
            }
        }
        None
    }
}

impl TemporalGraphIndex for TemporalCSR {
    fn fetch_temporal_neighbors(
        &self,
        src_nodes: &UInt64Array,
        valid_at: i64,
    ) -> Result<RecordBatch> {
        self.neighbors_record_batch(src_nodes, valid_at)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> TemporalCSR {
        TemporalCSR::from_arrays(
            &UInt64Array::from(vec![0u64, 0, 1, 1, 2, 3]),
            &UInt64Array::from(vec![1u64, 2, 3, 4, 5, 5]),
            &TimestampNanosecondArray::from(vec![0i64, 50, 0, 100, 0, 150]),
            &TimestampNanosecondArray::from(vec![100i64, 200, 100, 300, 300, 400]),
            &UInt16Array::from(vec![1u16, 1, 2, 2, 1, 3]),
            6,
        )
        .unwrap()
    }

    #[test]
    fn neighbors_sliced_by_time() {
        let csr = sample();
        let at_t0: Vec<u64> = csr.neighbors(0, 0).unwrap().map(|n| n.dst).collect();
        assert_eq!(at_t0, vec![1]);

        let at_t150: Vec<u64> = csr.neighbors(0, 150).unwrap().map(|n| n.dst).collect();
        assert_eq!(at_t150, vec![2]);
    }

    #[test]
    fn half_open_interval_semantics() {
        let csr = sample();
        // valid_to is exclusive: at exactly valid_to the edge is gone.
        assert_eq!(csr.neighbors(3, 399).unwrap().count(), 1); // 3 -> 5 active
        assert_eq!(csr.neighbors(3, 400).unwrap().count(), 0); // 3 -> 5 expired
        // node 0's first edge (0->1, [0,100)) expires at T=100 while 0->2 persists.
        let nbs: Vec<u64> = csr.neighbors(0, 100).unwrap().map(|n| n.dst).collect();
        assert_eq!(nbs, vec![2]);
    }

    #[test]
    fn khop_frontiers() {
        let csr = sample();
        let f = csr.khop(&UInt64Array::from(vec![0u64]), 2, 0).unwrap();
        assert_eq!(f.len(), 2);
        assert_eq!(f[0].values().as_ref(), &[1u64]);
        assert_eq!(f[1].values().as_ref(), &[3u64]);
    }

    #[test]
    fn node_out_of_range_is_error() {
        let csr = sample();
        assert!(csr.neighbors(99, 0).is_err());
    }

    #[test]
    fn degree_and_strategy() {
        let csr = sample();
        assert_eq!(csr.degree(0).unwrap(), 2);
        assert_eq!(csr.degree(3).unwrap(), 1);
        let (_, strategy) = csr.neighbors_planned(0, 0).unwrap();
        assert_eq!(strategy, NeighborStrategy::LinearScan);
    }

    // ---- Property test: adaptive path == linear-scan oracle ----------------

    struct Lcg(u64);
    impl Lcg {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            self.0
        }
        fn range(&mut self, lo: i64, hi: i64) -> i64 {
            lo + (self.next() % ((hi - lo) as u64)) as i64
        }
    }

    fn random_csr(nodes: usize, edges: usize, seed: u64) -> (TemporalCSR, Vec<(u64, u64, i64, i64)>) {
        let mut rng = Lcg(seed);
        let mut src = Vec::new();
        let mut dst = Vec::new();
        let mut vf = Vec::new();
        let mut vt = Vec::new();
        let mut rows = Vec::new();
        for _ in 0..edges {
            let s = (rng.next() % nodes as u64) as u64;
            let d = (rng.next() % nodes as u64) as u64;
            let a = rng.range(0, 10_000);
            let b = a + rng.range(1, 5_000);
            src.push(s);
            dst.push(d);
            vf.push(a);
            vt.push(b);
            rows.push((s, d, a, b));
        }
        let csr = TemporalCSR::from_arrays(
            &UInt64Array::from(src),
            &UInt64Array::from(dst),
            &TimestampNanosecondArray::from(vf),
            &TimestampNanosecondArray::from(vt),
            &UInt16Array::from(vec![0u16; edges]),
            nodes,
        )
        .unwrap();
        (csr, rows)
    }

    /// Reference: brute-force filter over the original edge rows.
    fn oracle(rows: &[(u64, u64, i64, i64)], src: u64, t: i64) -> Vec<u64> {
        let mut out: Vec<u64> = rows
            .iter()
            .filter(|(s, _, a, b)| *s == src && *a <= t && t < *b)
            .map(|(_, d, _, _)| *d)
            .collect();
        out.sort_unstable();
        out
    }

    #[test]
    fn adaptive_matches_linear_oracle() {
        let (csr, rows) = random_csr(64, 4_000, 0xC0FFEE);
        // Enough edges on some nodes to exercise binary search + zone maps.
        for src in 0..64u64 {
            for &t in &[-1i64, 0, 1, 1_000, 3_000, 5_000, 9_999, 20_000] {
                let mut got: Vec<u64> = csr.neighbors(src, t).unwrap().map(|n| n.dst).collect();
                got.sort_unstable();
                assert_eq!(got, oracle(&rows, src, t), "src={src} t={t}");
            }
        }
    }

    #[test]
    fn binary_search_used_for_high_degree() {
        // 1000 edges all from node 0 -> degree 1000 > LINEAR_SCAN_MAX_DEGREE.
        let mut src = vec![0u64; 1000];
        src.push(1);
        let mut dst: Vec<u64> = (0..1000).collect();
        dst.push(2);
        let mut vf: Vec<i64> = (0..1000).map(|i| i * 10).collect();
        vf.push(0);
        let mut vt: Vec<i64> = (0..1000).map(|i| i * 10 + 5).collect();
        vt.push(100);
        let csr = TemporalCSR::from_arrays(
            &UInt64Array::from(src),
            &UInt64Array::from(dst),
            &TimestampNanosecondArray::from(vf),
            &TimestampNanosecondArray::from(vt),
            &UInt16Array::from(vec![0u16; 1001]),
            3,
        )
        .unwrap();
        assert_eq!(csr.degree(0).unwrap(), 1000);
        let (nbs, strategy) = csr.neighbors_planned(0, 5004).unwrap();
        assert_eq!(strategy, NeighborStrategy::BinarySearchZoneMap);
        // 5000 <= t < 5005 -> exactly one edge (index 500).
        let got: Vec<u64> = nbs.map(|n| n.dst).collect();
        assert_eq!(got, vec![500]);
    }

    #[test]
    fn zone_map_skips_expired_chunks() {
        let (csr, _) = random_csr(8, 5_000, 7);
        assert!(csr.chunk_count() > 0);
        assert!(csr.stats().max_degree > LINEAR_SCAN_MAX_DEGREE);
    }

    // ---- B1-3: bounded traversal ------------------------------------------

    fn cyclic() -> TemporalCSR {
        // 0->1, 1->2, 2->0, 2->3, all active at T=0.
        TemporalCSR::from_arrays(
            &UInt64Array::from(vec![0u64, 1, 2, 2]),
            &UInt64Array::from(vec![1u64, 2, 0, 3]),
            &TimestampNanosecondArray::from(vec![0i64, 0, 0, 0]),
            &TimestampNanosecondArray::from(vec![100i64, 100, 100, 100]),
            &UInt16Array::from(vec![1u16, 1, 1, 2]),
            4,
        )
        .unwrap()
    }

    #[test]
    fn khop_bounded_visits_once() {
        let csr = cyclic();
        let r = csr
            .khop_bounded(
                &UInt64Array::from(vec![0u64]),
                5,
                0,
                &TraversalBudget::unlimited(),
                None,
                None,
            )
            .unwrap();
        let f: Vec<Vec<u64>> = r
            .frontiers
            .iter()
            .map(|a| a.values().to_vec())
            .collect();
        // 0 is never re-emitted (visited bitmap).
        assert_eq!(f, vec![vec![1], vec![2], vec![3]]);
        assert_eq!(r.stats.rows, 3);
    }

    #[test]
    fn khop_bounded_is_deterministic() {
        let (csr, _) = random_csr(32, 2_000, 99);
        let seeds = UInt64Array::from(vec![0u64, 1, 2, 3]);
        let run = || {
            csr.khop_bounded(&seeds, 4, 5_000, &TraversalBudget::unlimited(), None, None)
                .unwrap()
                .frontiers
                .iter()
                .map(|a| a.values().to_vec())
                .collect::<Vec<_>>()
        };
        let a = run();
        let b = run();
        assert_eq!(a, b);
        // Every frontier is sorted + unique.
        for f in &a {
            let mut s = f.clone();
            s.sort_unstable();
            s.dedup();
            assert_eq!(&s, f);
        }
    }

    #[test]
    fn khop_bounded_enforces_edge_budget() {
        // Node 0 has degree 100; max_edges=5 must trip during the scan.
        let src = vec![0u64; 100];
        let dst: Vec<u64> = (1..=100).collect();
        let csr = TemporalCSR::from_arrays(
            &UInt64Array::from(src),
            &UInt64Array::from(dst),
            &TimestampNanosecondArray::from(vec![0i64; 100]),
            &TimestampNanosecondArray::from(vec![100i64; 100]),
            &UInt16Array::from(vec![0u16; 100]),
            101,
        )
        .unwrap();
        let err = csr
            .khop_bounded(
                &UInt64Array::from(vec![0u64]),
                1,
                0,
                &TraversalBudget::unlimited().with_max_edges(5),
                None,
                None,
            )
            .unwrap_err();
        assert!(matches!(err, GtvError::BudgetExceeded { stage: "edges", .. }));
    }

    #[test]
    fn khop_bounded_predicate_narrows() {
        let csr = cyclic();
        // Only edge_type == 2 (2 -> 3) may be traversed.
        let pred = |nb: &Neighbor| nb.edge_type == 2;
        let pred: &dyn EdgePredicate = &pred;
        let r = csr
            .khop_bounded(
                &UInt64Array::from(vec![2u64]),
                1,
                0,
                &TraversalBudget::unlimited(),
                Some(pred),
                None,
            )
            .unwrap();
        assert_eq!(r.frontiers[0].values().as_ref(), &[3u64]);
    }

    #[test]
    fn khop_bounded_cancel() {
        use std::sync::atomic::AtomicBool;
        use std::sync::Arc;
        let csr = cyclic();
        let token = Arc::new(AtomicBool::new(true));
        assert!(matches!(
            csr.khop_bounded(
                &UInt64Array::from(vec![0u64]),
                2,
                0,
                &TraversalBudget::unlimited(),
                None,
                Some(&token),
            ),
            Err(GtvError::Cancelled)
        ));
    }

    #[test]
    fn khop_bounded_high_degree_guard() {
        // Node 0 has degree 100.
        let src = vec![0u64; 100];
        let dst: Vec<u64> = (1..=100).collect();
        let vf = vec![0i64; 100];
        let vt = vec![100i64; 100];
        let csr = TemporalCSR::from_arrays(
            &UInt64Array::from(src),
            &UInt64Array::from(dst),
            &TimestampNanosecondArray::from(vf),
            &TimestampNanosecondArray::from(vt),
            &UInt16Array::from(vec![0u16; 100]),
            101,
        )
        .unwrap();
        let err = csr
            .khop_bounded(
                &UInt64Array::from(vec![0u64]),
                1,
                0,
                &TraversalBudget::unlimited().with_max_degree(1),
                None,
                None,
            )
            .unwrap_err();
        assert!(matches!(err, GtvError::HighDegreeNode { .. }));
    }

    // ---- B1-3b: direction-optimizing BFS ----------------------------------

    #[test]
    fn transpose_reverses_edges() {
        let csr = cyclic();
        let rev = csr.transpose().unwrap();
        // 0 -> 1 exists in the forward graph, so 1 -> 0 exists in the reverse.
        let fwd: Vec<u64> = csr.neighbors(0, 0).unwrap().map(|n| n.dst).collect();
        assert_eq!(fwd, vec![1]);
        let back: Vec<u64> = rev.neighbors(1, 0).unwrap().map(|n| n.dst).collect();
        assert_eq!(back, vec![0]);
    }

    #[test]
    fn pull_and_auto_match_push() {
        let (csr, _) = random_csr(64, 4_000, 0xBEEF);
        let rev = csr.transpose().unwrap();
        let seeds = UInt64Array::from(vec![0u64, 1, 2, 3, 4, 5]);
        let budget = TraversalBudget::unlimited();
        let push = csr
            .khop_directed(&seeds, 4, 5_000, &budget, None, None, None, DirectionMode::Push)
            .unwrap();
        let pull = csr
            .khop_directed(
                &seeds,
                4,
                5_000,
                &budget,
                None,
                None,
                Some(&rev),
                DirectionMode::Pull,
            )
            .unwrap();
        let auto = csr
            .khop_directed(
                &seeds,
                4,
                5_000,
                &budget,
                None,
                None,
                Some(&rev),
                DirectionMode::Auto,
            )
            .unwrap();
        let fronts = |r: &KhopResult| -> Vec<Vec<u64>> {
            r.frontiers.iter().map(|a| a.values().to_vec()).collect()
        };
        assert_eq!(fronts(&push), fronts(&pull));
        assert_eq!(fronts(&push), fronts(&auto));
    }

    #[test]
    fn pull_honours_predicate_on_forward_edge() {
        // 2 -> 3 has edge_type 2; reverse pull from seed 2 must respect it.
        let csr = cyclic();
        let rev = csr.transpose().unwrap();
        let pred = |nb: &Neighbor| nb.edge_type == 2;
        let pred: &dyn EdgePredicate = &pred;
        let r = csr
            .khop_directed(
                &UInt64Array::from(vec![2u64]),
                1,
                0,
                &TraversalBudget::unlimited(),
                Some(pred),
                None,
                Some(&rev),
                DirectionMode::Pull,
            )
            .unwrap();
        assert_eq!(r.frontiers[0].values().as_ref(), &[3u64]);
    }

    #[test]
    fn pull_requires_reverse() {
        let csr = cyclic();
        let err = csr
            .khop_directed(
                &UInt64Array::from(vec![0u64]),
                2,
                0,
                &TraversalBudget::unlimited(),
                None,
                None,
                None,
                DirectionMode::Pull,
            )
            .unwrap_err();
        assert!(matches!(err, GtvError::InvalidArgument(_)));
    }
}
