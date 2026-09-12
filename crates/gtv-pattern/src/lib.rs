//! gtv-pattern: temporal graph pattern matching (GQL/Cypher-lite) over a
//! [`TemporalCSR`].
//!
//! A [`Pattern`] is a set of typed edges between *variables* plus temporal
//! ordering constraints between edges. Matching is a backtracking DFS that
//! binds each variable to a node, requires every matched edge to be active at
//! the reference time `valid_at`, and enforces the event-time (edge
//! `valid_from`) ordering constraints.

use gtv_core::{BudgetTracker, CancelToken, Result, TemporalCSR, TraversalBudget};

/// A pattern edge from variable `from` to variable `to`.
///
/// The matcher processes edges in declaration order and assumes each edge's
/// `from` variable is bound before the edge is reached (all built-in
/// constructors guarantee this "forward" ordering).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PatternEdge {
    pub from: usize,
    pub to: usize,
    /// If `Some`, only edges of this type match.
    pub edge_type: Option<u16>,
}

/// Temporal ordering: the event time (`valid_from`) of edge `before` must be
/// strictly less than that of edge `after`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventOrder {
    pub before: usize,
    pub after: usize,
}

/// A temporal graph pattern.
#[derive(Debug, Clone)]
pub struct Pattern {
    pub num_vars: usize,
    pub edges: Vec<PatternEdge>,
    pub event_orders: Vec<EventOrder>,
}

impl Pattern {
    /// A temporal path of `len` hops: `v0 -> v1 -> ... -> v_{len}`, with
    /// strictly increasing event times along the chain (`T1 < T2 < ...`).
    pub fn temporal_path(len: usize) -> Self {
        assert!(len >= 1, "path length must be >= 1");
        let edges: Vec<PatternEdge> = (0..len)
            .map(|i| PatternEdge {
                from: i,
                to: i + 1,
                edge_type: None,
            })
            .collect();
        let event_orders = (0..len.saturating_sub(1))
            .map(|i| EventOrder {
                before: i,
                after: i + 1,
            })
            .collect();
        Self {
            num_vars: len + 1,
            edges,
            event_orders,
        }
    }

    /// A diamond: `a -> b`, `a -> c`, `b -> d`, `c -> d`, with each arm ordered
    /// in time (`T(a->b) < T(b->d)` and `T(a->c) < T(c->d)`).
    pub fn diamond() -> Self {
        Self {
            num_vars: 4,
            edges: vec![
                PatternEdge { from: 0, to: 1, edge_type: None },
                PatternEdge { from: 0, to: 2, edge_type: None },
                PatternEdge { from: 1, to: 3, edge_type: None },
                PatternEdge { from: 2, to: 3, edge_type: None },
            ],
            event_orders: vec![
                EventOrder { before: 0, after: 2 },
                EventOrder { before: 1, after: 3 },
            ],
        }
    }

    /// A temporal ring (cycle) of `len` hops back to the start:
    /// `v0 -> v1 -> ... -> v_{len-1} -> v0`, with increasing event times.
    pub fn ring(len: usize) -> Self {
        assert!(len >= 2, "ring length must be >= 2");
        let edges: Vec<PatternEdge> = (0..len)
            .map(|i| PatternEdge {
                from: i,
                to: (i + 1) % len,
                edge_type: None,
            })
            .collect();
        let event_orders = (0..len.saturating_sub(1))
            .map(|i| EventOrder {
                before: i,
                after: i + 1,
            })
            .collect();
        Self {
            num_vars: len,
            edges,
            event_orders,
        }
    }
}

/// One matched edge within a [`Match`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MatchedEdge {
    pub src: u64,
    pub dst: u64,
    pub edge_type: u16,
    pub valid_from: i64,
    pub valid_to: i64,
}

/// A complete pattern match: node bindings per variable plus the matched edges.
#[derive(Debug, Clone)]
pub struct Match {
    /// `nodes[var]` is the bound node id for variable `var`.
    pub nodes: Vec<u64>,
    pub edges: Vec<MatchedEdge>,
}

/// Find up to `limit` matches anchoring variable 0 at `start`, at time `valid_at`.
pub fn find_from(
    csr: &TemporalCSR,
    pattern: &Pattern,
    start: u64,
    valid_at: i64,
    limit: usize,
) -> Result<Vec<Match>> {
    if limit == 0 || pattern.num_vars == 0 {
        return Ok(Vec::new());
    }
    let mut state = DfsState::new(pattern);
    state.nodes[0] = start;
    let mut matches = Vec::new();
    dfs(csr, pattern, valid_at, 0, &mut state, &mut matches, limit, None)?;
    Ok(matches)
}

/// True when `pattern` is the 3-cycle `v0 → v1 → v2 → v0` (the wash-trade ring).
fn is_ring3(pattern: &Pattern) -> bool {
    pattern.num_vars == 3
        && pattern.edges.len() == 3
        && pattern.edges[0]
            == PatternEdge {
                from: 0,
                to: 1,
                edge_type: None,
            }
        && pattern.edges[1]
            == PatternEdge {
                from: 1,
                to: 2,
                edge_type: None,
            }
        && pattern.edges[2]
            == PatternEdge {
                from: 2,
                to: 0,
                edge_type: None,
            }
}

/// Recursion-free 3-cycle matcher (`A→B→C→A` with strictly increasing event
/// times).
///
/// The generic DFS pays a function call and a `Result` unwind per level; across
/// a 500k-node scan that overhead dominates. This runs three tight loops,
/// parallelizes the independent start-node scan with rayon, and — when every
/// edge is active at `valid_at` — drops the per-edge validity test entirely.
fn find_ring3(csr: &TemporalCSR, valid_at: i64, limit: usize) -> Result<Vec<Match>> {
    use rayon::prelude::*;

    let n = csr.node_count();
    if limit == 0 {
        return Ok(Vec::new());
    }
    let all_active = csr.all_active_at(valid_at);

    // Start nodes are scanned independently, so split them across the pool. Each
    // chunk collects its own matches; the results are concatenated and truncated
    // to `limit` (the ordering is unspecified, matching the generic `find`).
    const CHUNK: usize = 8192;
    let starts: Vec<u64> = (0..n as u64).collect();
    let parts: Result<Vec<Vec<Match>>> = starts
        .par_chunks(CHUNK)
        .map(|chunk| {
            let mut local = Vec::new();
            for &a in chunk {
                if local.len() >= limit {
                    break;
                }
                ring3_from(csr, a, valid_at, all_active, limit, &mut local)?;
            }
            Ok(local)
        })
        .collect();

    let mut out = Vec::new();
    for mut local in parts? {
        out.append(&mut local);
        if out.len() >= limit {
            out.truncate(limit);
            break;
        }
    }
    Ok(out)
}

/// Scan one start node `a` for a 3-cycle rooted there, pushing matches into `out`.
#[inline]
fn ring3_from(
    csr: &TemporalCSR,
    a: u64,
    valid_at: i64,
    all_active: bool,
    limit: usize,
    out: &mut Vec<Match>,
) -> Result<()> {
    let (da, vfa, vta, eta) = csr.edge_slices(a)?;
    for i in 0..da.len() {
        let b = da[i];
        let t0 = vfa[i];
        if b == a || (!all_active && (t0 > valid_at || valid_at >= vta[i])) {
            continue;
        }
        let (db, vfb, vtb, etb) = csr.edge_slices(b)?;
        for j in 0..db.len() {
            let c = db[j];
            if c == a || c == b {
                continue;
            }
            let t1 = vfb[j];
            if t1 <= t0 || (!all_active && (t1 > valid_at || valid_at >= vtb[j])) {
                continue;
            }
            let (dc, vfc, vtc, etc_) = csr.edge_slices(c)?;
            for k in 0..dc.len() {
                if dc[k] != a {
                    continue;
                }
                let t2 = vfc[k];
                if t2 <= t1 || (!all_active && (t2 > valid_at || valid_at >= vtc[k])) {
                    continue;
                }
                out.push(Match {
                    nodes: vec![a, b, c],
                    edges: vec![
                        MatchedEdge {
                            src: a,
                            dst: b,
                            edge_type: eta[i],
                            valid_from: t0,
                            valid_to: vta[i],
                        },
                        MatchedEdge {
                            src: b,
                            dst: c,
                            edge_type: etb[j],
                            valid_from: t1,
                            valid_to: vtb[j],
                        },
                        MatchedEdge {
                            src: c,
                            dst: a,
                            edge_type: etc_[k],
                            valid_from: t2,
                            valid_to: vtc[k],
                        },
                    ],
                });
                if out.len() >= limit {
                    return Ok(());
                }
            }
        }
    }
    Ok(())
}

/// Find up to `limit` matches over *all* possible start nodes, at time `valid_at`.
///
/// Allocation-free hot path: a single [`DfsState`] is reused across every start
/// node, and the 3-cycle (wash-trade) shape is dispatched to a dedicated
/// recursion-free matcher. [`find_from`] allocates a fresh state (three `Vec`s)
/// per start, which turns a 500k-node scan into ~1.5M heap allocations.
pub fn find(
    csr: &TemporalCSR,
    pattern: &Pattern,
    valid_at: i64,
    limit: usize,
) -> Result<Vec<Match>> {
    if limit == 0 || pattern.num_vars == 0 {
        return Ok(Vec::new());
    }
    if is_ring3(pattern) {
        return find_ring3(csr, valid_at, limit);
    }
    let mut out = Vec::new();
    let mut state = DfsState::new(pattern);
    for start in 0..csr.node_count() as u64 {
        if out.len() >= limit {
            break;
        }
        state.nodes[0] = start;
        dfs(csr, pattern, valid_at, 0, &mut state, &mut out, limit, None)?;
    }
    Ok(out)
}

/// Resource-bounded variant of [`find`] (B1-3).
///
/// Enforces the traversal [`TraversalBudget`] (edges / rows / deadline) and
/// cooperatively honours a [`CancelToken`]. The optimized parallel 3-cycle
/// matcher is intentionally bypassed here so that every scanned edge is
/// accounted for; use [`find`] when no budget is needed.
///
/// Results are produced in the same order as [`find`] for the generic path.
pub fn find_bounded(
    csr: &TemporalCSR,
    pattern: &Pattern,
    valid_at: i64,
    limit: usize,
    budget: &TraversalBudget,
    cancel: Option<&CancelToken>,
) -> Result<Vec<Match>> {
    if limit == 0 || pattern.num_vars == 0 {
        return Ok(Vec::new());
    }
    let tracker = BudgetTracker::new(budget.clone(), cancel.cloned());
    let mut out = Vec::new();
    let mut state = DfsState::new(pattern);
    for start in 0..csr.node_count() as u64 {
        tracker.check_time()?;
        if out.len() >= limit {
            break;
        }
        state.nodes[0] = start;
        dfs(
            csr,
            pattern,
            valid_at,
            0,
            &mut state,
            &mut out,
            limit,
            Some(&tracker),
        )?;
        tracker.add_rows(out.len() as u64)?;
    }
    Ok(out)
}

struct DfsState {
    nodes: Vec<u64>,
    valid_from: Vec<i64>,
    valid_to: Vec<i64>,
    edge_type: Vec<u16>,
}

impl DfsState {
    fn new(pattern: &Pattern) -> Self {
        Self {
            nodes: vec![u64::MAX; pattern.num_vars],
            valid_from: vec![0; pattern.edges.len()],
            valid_to: vec![0; pattern.edges.len()],
            edge_type: vec![0; pattern.edges.len()],
        }
    }

    fn to_match(&self, pattern: &Pattern) -> Match {
        let edges = pattern
            .edges
            .iter()
            .enumerate()
            .map(|(i, e)| MatchedEdge {
                src: self.nodes[e.from],
                dst: self.nodes[e.to],
                edge_type: self.edge_type[i],
                valid_from: self.valid_from[i],
                valid_to: self.valid_to[i],
            })
            .collect();
        Match {
            nodes: self.nodes.clone(),
            edges,
        }
    }

    fn event_orders_satisfied(&self, pattern: &Pattern) -> bool {
        pattern
            .event_orders
            .iter()
            .all(|o| self.valid_from[o.before] < self.valid_from[o.after])
    }
}

fn dfs(
    csr: &TemporalCSR,
    pattern: &Pattern,
    valid_at: i64,
    ei: usize,
    state: &mut DfsState,
    matches: &mut Vec<Match>,
    limit: usize,
    tracker: Option<&BudgetTracker>,
) -> Result<()> {
    if matches.len() >= limit {
        return Ok(());
    }
    if ei == pattern.edges.len() {
        if state.event_orders_satisfied(pattern) {
            matches.push(state.to_match(pattern));
        }
        return Ok(());
    }

    let e = pattern.edges[ei];
    let from_node = state.nodes[e.from];
    if from_node == u64::MAX {
        return Ok(());
    }

    let to_bound = state.nodes[e.to] != u64::MAX;

    // Direct slice access — skips the `neighbors` iterator's per-edge `Neighbor`
    // struct and closure overhead, which dominates a multi-million-node scan.
    let (dst, vf, vt, et) = csr.edge_slices(from_node)?;
    for idx in 0..dst.len() {
        if let Some(t) = tracker {
            t.add_edges(1)?;
            if idx & 0x3ff == 0 {
                t.check_time()?;
            }
        }
        // Active at `valid_at`: `valid_from <= valid_at < valid_to`.
        let vf_i = vf[idx];
        if vf_i > valid_at || valid_at >= vt[idx] {
            continue;
        }
        if let Some(t) = e.edge_type {
            if et[idx] != t {
                continue;
            }
        }
        let dst_i = dst[idx];
        if to_bound {
            // The destination is already fixed: only edges into it count.
            if dst_i != state.nodes[e.to] {
                continue;
            }
            state.valid_from[ei] = vf_i;
            state.valid_to[ei] = vt[idx];
            state.edge_type[ei] = et[idx];
            dfs(csr, pattern, valid_at, ei + 1, state, matches, limit, tracker)?;
        } else {
            // Bind a fresh variable: keep node assignments distinct.
            let mut dup = false;
            for k in 0..pattern.num_vars {
                if state.nodes[k] == dst_i {
                    dup = true;
                    break;
                }
            }
            if dup {
                continue;
            }
            state.nodes[e.to] = dst_i;
            state.valid_from[ei] = vf_i;
            state.valid_to[ei] = vt[idx];
            state.edge_type[ei] = et[idx];
            dfs(csr, pattern, valid_at, ei + 1, state, matches, limit, tracker)?;
            state.nodes[e.to] = u64::MAX;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, UInt64Array};
    use gtv_core::{EdgeTable, NodeTable, TemporalGraph};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    /// Transfer graph with distinct event times:
    ///   0 -> 1 @10, 1 -> 2 @20, 2 -> 3 @30, 3 -> 0 @40  (a temporal ring)
    ///   0 -> 4 @15, 4 -> 3 @25 (a diamond with the ring's left half)
    fn transfer_graph() -> TemporalGraph {
        let nodes = NodeTable::new(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::UInt64, false),
                    Field::new("value", DataType::Float64, false),
                ])),
                vec![
                    Arc::new(UInt64Array::from(vec![0u64, 1, 2, 3, 4])) as _,
                    Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0])) as _,
                ],
            )
            .unwrap(),
        )
        .unwrap();
        let edges = EdgeTable::from_vecs(
            vec![0, 1, 2, 3, 0, 4],
            vec![1, 2, 3, 0, 4, 3],
            vec![1u16, 1, 1, 1, 1, 1],
            vec![10, 20, 30, 40, 15, 25],
            vec![1000, 1000, 1000, 1000, 1000, 1000],
        )
        .unwrap();
        TemporalGraph::new(nodes, edges).unwrap()
    }

    #[test]
    fn temporal_path_finds_increasing_chain() {
        let g = transfer_graph();
        let pat = Pattern::temporal_path(3);
        let m = find_from(g.csr(), &pat, 0, 500, 10).unwrap();
        // Only 0 -> 1 -> 2 -> 3 has strictly increasing times (10 < 20 < 30).
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].nodes, vec![0, 1, 2, 3]);
    }

    #[test]
    fn ring_finds_cycle_back_to_start() {
        let g = transfer_graph();
        let pat = Pattern::ring(4);
        let m = find_from(g.csr(), &pat, 0, 500, 10).unwrap();
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].nodes, vec![0, 1, 2, 3]);
        assert_eq!(m[0].edges[3].dst, 0);
    }

    #[test]
    fn diamond_finds_two_arm_structure() {
        let g = transfer_graph();
        let pat = Pattern::diamond();
        let m = find_from(g.csr(), &pat, 0, 500, 10).unwrap();
        // Diamond: 0->1 (10), 0->4 (15), 1->3 (20 via? no 1->3 is not an edge).
        // Our graph has 0->1,0->4 and 4->3, but no 1->3, so no diamond from 0.
        assert!(m.is_empty());
    }

    #[test]
    fn event_order_prunes_decreasing_times() {
        // Build a chain whose times DECREASE: 0->1 @50, 1->2 @40.
        let g = {
            let nodes = NodeTable::new(
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)])),
                    vec![Arc::new(UInt64Array::from(vec![0u64, 1, 2])) as _],
                )
                .unwrap(),
            )
            .unwrap();
            let edges = EdgeTable::from_vecs(
                vec![0, 1],
                vec![1, 2],
                vec![1u16, 1],
                vec![50, 40],
                vec![2000, 2000],
            )
            .unwrap();
            TemporalGraph::new(nodes, edges).unwrap()
        };
        let pat = Pattern::temporal_path(2);
        // Edges are active at T=1500, but their event times DECREASE (50 > 40),
        // so the temporal path (which requires increasing times) must not match.
        let m = find_from(g.csr(), &pat, 0, 1500, 10).unwrap();
        assert!(m.is_empty());
    }

    #[test]
    fn find_bounded_matches_find_and_enforces_budget() {
        let g = transfer_graph();
        let pat = Pattern::temporal_path(3);
        let unbounded = find(g.csr(), &pat, 500, 10).unwrap();
        let bounded =
            find_bounded(g.csr(), &pat, 500, 10, &TraversalBudget::unlimited(), None).unwrap();
        assert_eq!(unbounded.len(), bounded.len());

        let err = find_bounded(
            g.csr(),
            &pat,
            500,
            10,
            &TraversalBudget::unlimited().with_max_edges(1),
            None,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            gtv_core::GtvError::BudgetExceeded { stage: "edges", .. }
        ));
    }

    #[test]
    fn find_bounded_honours_cancel() {
        let g = transfer_graph();
        let pat = Pattern::temporal_path(3);
        let token: CancelToken = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let got = find_bounded(
            g.csr(),
            &pat,
            500,
            10,
            &TraversalBudget::unlimited(),
            Some(&token),
        );
        assert!(matches!(got, Err(gtv_core::GtvError::Cancelled)));
    }
}
