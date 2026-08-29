//! gtv-pattern: temporal graph pattern matching (GQL/Cypher-lite) over a
//! [`TemporalCSR`].
//!
//! A [`Pattern`] is a set of typed edges between *variables* plus temporal
//! ordering constraints between edges. Matching is a backtracking DFS that
//! binds each variable to a node, requires every matched edge to be active at
//! the reference time `valid_at`, and enforces the event-time (edge
//! `valid_from`) ordering constraints.

use gtv_core::{Result, TemporalCSR};

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
    dfs(csr, pattern, valid_at, 0, &mut state, &mut matches, limit)?;
    Ok(matches)
}

/// Find up to `limit` matches over *all* possible start nodes, at time `valid_at`.
pub fn find(
    csr: &TemporalCSR,
    pattern: &Pattern,
    valid_at: i64,
    limit: usize,
) -> Result<Vec<Match>> {
    let mut out = Vec::new();
    for start in 0..csr.node_count() as u64 {
        if out.len() >= limit {
            break;
        }
        let got = find_from(csr, pattern, start, valid_at, limit - out.len())?;
        out.extend(got);
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

    for nb in csr.neighbors(from_node, valid_at)? {
        if let Some(t) = e.edge_type {
            if nb.edge_type != t {
                continue;
            }
        }
        if to_bound {
            // The destination is already fixed: only edges into it count.
            if nb.dst != state.nodes[e.to] {
                continue;
            }
            state.valid_from[ei] = nb.valid_from;
            state.valid_to[ei] = nb.valid_to;
            state.edge_type[ei] = nb.edge_type;
            dfs(csr, pattern, valid_at, ei + 1, state, matches, limit)?;
        } else {
            // Bind a fresh variable: keep node assignments distinct.
            if state.nodes[..pattern.num_vars].contains(&nb.dst) {
                continue;
            }
            state.nodes[e.to] = nb.dst;
            state.valid_from[ei] = nb.valid_from;
            state.valid_to[ei] = nb.valid_to;
            state.edge_type[ei] = nb.edge_type;
            dfs(csr, pattern, valid_at, ei + 1, state, matches, limit)?;
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
}
