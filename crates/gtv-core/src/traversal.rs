//! Resource-bounded graph traversal primitives (B1-3).
//!
//! [`crate::TemporalCSR::khop_bounded`] and the `gtv-pattern` matcher share
//! these types so that a single budget / cancellation contract applies to every
//! graph query:
//!
//! * [`VisitedSet`] — O(1)-reset, generation-stamped bitmap (4 bytes/node).
//! * [`TraversalBudget`] — hard caps on hops, edges, frontier, rows, memory and
//!   wall-clock time.
//! * [`CancelToken`] — cooperative cancellation from another thread.
//! * [`EdgePredicate`] — push edge filters into the expansion loop.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crate::csr::Neighbor;
use crate::error::{GtvError, Result};

/// Cooperative cancellation flag shared with a traversal.
pub type CancelToken = Arc<AtomicBool>;

/// Hard limits for a graph traversal. `Default` is effectively unlimited.
#[derive(Debug, Clone)]
pub struct TraversalBudget {
    /// Maximum number of hops (frontiers) to expand.
    pub max_hops: usize,
    /// Maximum cumulative edges scanned across all hops.
    pub max_edges: u64,
    /// Maximum size of a single frontier.
    pub max_frontier: usize,
    /// Maximum cumulative output nodes emitted.
    pub max_rows: u64,
    /// Maximum estimated frontier + visited memory (bytes).
    pub max_memory_bytes: u64,
    /// When set, a node with a higher degree requires an [`EdgePredicate`] (or
    /// the query is rejected) to avoid one-node blowups.
    pub max_degree: Option<u32>,
    /// Wall-clock deadline.
    pub deadline: Option<Instant>,
}

impl Default for TraversalBudget {
    fn default() -> Self {
        Self {
            max_hops: usize::MAX,
            max_edges: u64::MAX,
            max_frontier: usize::MAX,
            max_rows: u64::MAX,
            max_memory_bytes: u64::MAX,
            max_degree: None,
            deadline: None,
        }
    }
}

impl TraversalBudget {
    /// No limits (equivalent to [`Default`]).
    pub fn unlimited() -> Self {
        Self::default()
    }

    pub fn with_max_hops(mut self, n: usize) -> Self {
        self.max_hops = n;
        self
    }
    pub fn with_max_edges(mut self, n: u64) -> Self {
        self.max_edges = n;
        self
    }
    pub fn with_max_frontier(mut self, n: usize) -> Self {
        self.max_frontier = n;
        self
    }
    pub fn with_max_rows(mut self, n: u64) -> Self {
        self.max_rows = n;
        self
    }
    pub fn with_max_memory_bytes(mut self, n: u64) -> Self {
        self.max_memory_bytes = n;
        self
    }
    pub fn with_max_degree(mut self, n: u32) -> Self {
        self.max_degree = Some(n);
        self
    }
    pub fn with_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }
}

/// What a traversal actually did (for telemetry / tests).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TraversalStats {
    pub hops: usize,
    pub edges_scanned: u64,
    pub rows: u64,
    pub peak_frontier: usize,
}

/// Push an edge filter into the expansion loop so an over-broad traversal can be
/// narrowed *before* the frontier grows.
pub trait EdgePredicate: Send + Sync {
    /// Return `false` to drop this edge from the traversal.
    fn keep(&self, edge: &Neighbor) -> bool;
}

impl<F> EdgePredicate for F
where
    F: Fn(&Neighbor) -> bool + Send + Sync,
{
    #[inline]
    fn keep(&self, edge: &Neighbor) -> bool {
        self(edge)
    }
}

/// O(1)-reset, generation-stamped visited bitmap over dense node ids.
///
/// `mark` returns `true` the first time a node is seen in the current
/// generation; `reset` just bumps the generation counter, so clearing is O(1)
/// instead of O(n).
#[derive(Debug, Clone)]
pub struct VisitedSet {
    stamp: Vec<u32>,
    gen: u32,
}

impl VisitedSet {
    pub fn new(node_count: usize) -> Self {
        Self {
            stamp: vec![0u32; node_count],
            gen: 1,
        }
    }

    /// Mark `id`; returns `true` if it was not already visited this generation.
    #[inline]
    pub fn mark(&mut self, id: u64) -> bool {
        let i = id as usize;
        if self.stamp[i] == self.gen {
            false
        } else {
            self.stamp[i] = self.gen;
            true
        }
    }

    #[inline]
    pub fn seen(&self, id: u64) -> bool {
        self.stamp[id as usize] == self.gen
    }

    /// Clear in O(1) by advancing the generation.
    pub fn reset(&mut self) {
        self.gen = self.gen.wrapping_add(1);
        if self.gen == 0 {
            self.stamp.iter_mut().for_each(|s| *s = 0);
            self.gen = 1;
        }
    }

    /// Estimated resident bytes.
    pub fn memory_bytes(&self) -> u64 {
        (self.stamp.len() * std::mem::size_of::<u32>()) as u64
    }
}

/// Thread-safe budget accountant, usable from the rayon-parallel pattern
/// matcher as well as the sequential BFS.
pub struct BudgetTracker {
    budget: TraversalBudget,
    edges: AtomicU64,
    rows: AtomicU64,
    peak_frontier: AtomicU64,
    cancel: Option<CancelToken>,
    start: Instant,
}

impl BudgetTracker {
    pub fn new(budget: TraversalBudget, cancel: Option<CancelToken>) -> Self {
        Self {
            budget,
            edges: AtomicU64::new(0),
            rows: AtomicU64::new(0),
            peak_frontier: AtomicU64::new(0),
            cancel,
            start: Instant::now(),
        }
    }

    pub fn budget(&self) -> &TraversalBudget {
        &self.budget
    }

    #[inline]
    pub fn cancel(&self) {
        if let Some(c) = &self.cancel {
            c.store(true, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn is_cancelled(&self) -> bool {
        self.cancel
            .as_ref()
            .map(|c| c.load(Ordering::Relaxed))
            .unwrap_or(false)
    }

    /// Check cancellation + deadline. Cheap enough for inner loops at a coarse
    /// cadence.
    #[inline]
    pub fn check_time(&self) -> Result<()> {
        if self.is_cancelled() {
            return Err(GtvError::Cancelled);
        }
        if let Some(deadline) = self.budget.deadline {
            if Instant::now() >= deadline {
                return Err(GtvError::BudgetExceeded {
                    stage: "deadline",
                    limit: 0,
                    observed: self.start.elapsed().as_millis() as u64,
                });
            }
        }
        Ok(())
    }

    /// Account `n` scanned edges and enforce `max_edges`.
    #[inline]
    pub fn add_edges(&self, n: u64) -> Result<()> {
        let total = self.edges.fetch_add(n, Ordering::Relaxed) + n;
        if total > self.budget.max_edges {
            return Err(GtvError::BudgetExceeded {
                stage: "edges",
                limit: self.budget.max_edges,
                observed: total,
            });
        }
        Ok(())
    }

    /// Account `n` emitted rows and enforce `max_rows`.
    #[inline]
    pub fn add_rows(&self, n: u64) -> Result<()> {
        let total = self.rows.fetch_add(n, Ordering::Relaxed) + n;
        if total > self.budget.max_rows {
            return Err(GtvError::BudgetExceeded {
                stage: "rows",
                limit: self.budget.max_rows,
                observed: total,
            });
        }
        Ok(())
    }

    /// Enforce `max_frontier` and record the peak frontier size.
    #[inline]
    pub fn record_frontier(&self, n: usize) -> Result<()> {
        if n as u64 > self.budget.max_frontier as u64 {
            return Err(GtvError::BudgetExceeded {
                stage: "frontier",
                limit: self.budget.max_frontier as u64,
                observed: n as u64,
            });
        }
        self.peak_frontier.fetch_max(n as u64, Ordering::Relaxed);
        Ok(())
    }

    /// Enforce an estimated memory figure against `max_memory_bytes`.
    #[inline]
    pub fn check_memory(&self, bytes: u64) -> Result<()> {
        if bytes > self.budget.max_memory_bytes {
            return Err(GtvError::BudgetExceeded {
                stage: "memory",
                limit: self.budget.max_memory_bytes,
                observed: bytes,
            });
        }
        Ok(())
    }

    pub fn stats(&self, hops: usize) -> TraversalStats {
        TraversalStats {
            hops,
            edges_scanned: self.edges.load(Ordering::Relaxed),
            rows: self.rows.load(Ordering::Relaxed),
            peak_frontier: self.peak_frontier.load(Ordering::Relaxed) as usize,
        }
    }
}

/// Result of a bounded k-hop traversal.
#[derive(Debug, Clone)]
pub struct KhopResult {
    /// One frontier per hop; `frontiers[i]` holds the nodes first reached at hop
    /// `i + 1` (sorted ascending, unique).
    pub frontiers: Vec<arrow::array::UInt64Array>,
    pub stats: TraversalStats,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn visited_set_generation_reset() {
        let mut v = VisitedSet::new(4);
        assert!(v.mark(2));
        assert!(!v.mark(2));
        assert!(v.seen(2));
        v.reset();
        assert!(!v.seen(2));
        assert!(v.mark(2));
    }

    #[test]
    fn budget_edges_enforced() {
        let t = BudgetTracker::new(TraversalBudget::unlimited().with_max_edges(10), None);
        assert!(t.add_edges(8).is_ok());
        assert!(matches!(
            t.add_edges(5),
            Err(GtvError::BudgetExceeded { stage: "edges", .. })
        ));
    }

    #[test]
    fn cancel_token_trips() {
        let token: CancelToken = Arc::new(AtomicBool::new(false));
        let t = BudgetTracker::new(TraversalBudget::unlimited(), Some(token.clone()));
        assert!(t.check_time().is_ok());
        token.store(true, Ordering::Relaxed);
        assert!(matches!(t.check_time(), Err(GtvError::Cancelled)));
    }

    #[test]
    fn frontier_and_memory_enforced() {
        let t = BudgetTracker::new(
            TraversalBudget::unlimited()
                .with_max_frontier(3)
                .with_max_memory_bytes(100),
            None,
        );
        assert!(t.record_frontier(3).is_ok());
        assert!(t.record_frontier(4).is_err());
        assert!(t.check_memory(50).is_ok());
        assert!(t.check_memory(200).is_err());
    }
}
