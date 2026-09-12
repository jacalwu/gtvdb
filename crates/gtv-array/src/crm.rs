//! Greedy CRM (credit-risk-mitigation) allocation kernels.
//!
//! Implements the allocator specified in `crm-allocation.md` (repo root):
//!
//! * one loan may carry many collaterals **and** many guarantors; one
//!   collateral / guarantor may cover many loans (multi-to-multi graph);
//! * **specified** edges (`allocation_mode = 'specified'`) are contractually
//!   locked and are allocated **first** — they never enter optimisation;
//! * remaining **optimizable** edges are allocated by a deterministic greedy /
//!   priority allocator (sources & loans ranked by priority, ties broken by
//!   ascending id) so runs are auditable, explainable and reproducible;
//! * collateral capacity is haircut-adjusted up-front:
//!   `C_adj = max(0, C × (1 − Hc − Hfx − Hmm))` — see
//!   [`adjusted_collateral_value`];
//! * guarantee coverage respects `CRMg = min(G, E − CRMc)` because guarantee
//!   phases always run after the collateral phases have reduced each loan's
//!   remaining exposure;
//! * every allocation is recorded in an audit trail
//!   ([`CrmResult::allocations`]) for later validation / replay.
//!
//! Entities are identified by caller-supplied `u64` ids, so sparse or
//! non-contiguous database ids are fine — the kernel remaps to dense
//! work arrays internally and returns results keyed by the original ids.

use std::collections::HashMap;
use std::error::Error;
use std::fmt;

/// Whether an edge participates in the greedy optimisation pool.
///
/// `Specified` edges mirror contractually locked CRM arrangements — they are
/// allocated first (in edge-list order) and never optimised.
/// `Optimizable` edges are available to the greedy / priority allocator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AllocationMode {
    Specified,
    Optimizable,
}

/// A loan that needs credit-risk-mitigation cover.
///
/// `exposure` is the gross exposure to mitigate (EAD / PV); `priority` ranks
/// which loans are covered first by a shared CRM source (larger = earlier).
#[derive(Debug, Clone, Copy)]
pub struct Loan {
    pub id: u64,
    pub exposure: f64,
    pub priority: f64,
}

/// A collateral source.
///
/// `capacity` must already be haircut-adjusted (see
/// [`adjusted_collateral_value`]); `priority` ranks which sources are consumed
/// first (larger = earlier).
#[derive(Debug, Clone, Copy)]
pub struct Collateral {
    pub id: u64,
    pub capacity: f64,
    pub priority: f64,
}

/// A guarantor. `capacity` is the guarantee amount, `priority` ranks it.
#[derive(Debug, Clone, Copy)]
pub struct Guarantor {
    pub id: u64,
    pub capacity: f64,
    pub priority: f64,
}

/// Pledge edge from one collateral to one loan.
///
/// `ratio` is the contractual share of the collateral earmarked for the loan.
/// It is retained for proration weighting and for the future LP solver; the
/// greedy allocator itself treats every edge as giving access to the source's
/// remaining capacity (exactly the semantics of `crm-allocation.md` §5/§6).
#[derive(Debug, Clone, Copy)]
pub struct CollateralEdge {
    pub col_id: u64,
    pub loan_id: u64,
    pub ratio: f64,
    pub mode: AllocationMode,
}

/// Guarantee edge from one guarantor to one loan.
///
/// `amount` is the contractual guarantee line for that loan; it is retained for
/// the future LP solver while the greedy allocator consumes the guarantor's
/// remaining capacity (see [`crm_alloc_greedy`]).
#[derive(Debug, Clone, Copy)]
pub struct GuaranteeEdge {
    pub guarantor_id: u64,
    pub loan_id: u64,
    pub amount: f64,
    pub mode: AllocationMode,
}

/// Which kind of CRM source produced an audit record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrmSourceKind {
    Collateral,
    Guarantee,
}

/// Which allocation phase produced an audit record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrmStage {
    /// Contract-locked edges (phase 1) — allocated first, in edge-list order.
    Specified,
    /// Greedy / priority allocator over optimizable edges (phase 2).
    Greedy,
    /// Exact LP optimizer over optimizable edges (phase 3, optional `crm-lp`
    /// feature) — may enforce per-edge caps / a different objective.
    Lp,
}

/// One auditable allocation step.
///
/// Every non-zero cover writes one record so results can be replayed and
/// checked: `Σ amount` per source must not exceed its capacity, `Σ amount` per
/// loan must not exceed its exposure, and `net_exposure = exposure − Σ covers`.
#[derive(Debug, Clone, Copy)]
pub struct CrmAllocation {
    pub source_kind: CrmSourceKind,
    pub stage: CrmStage,
    pub source_id: u64,
    pub loan_id: u64,
    pub amount: f64,
    /// Remaining source capacity **after** this allocation.
    pub source_remaining: f64,
    /// Remaining loan exposure **after** this allocation.
    pub loan_remaining: f64,
}

/// Per-loan allocation outcome, aligned with the input `loans` order.
#[derive(Debug, Clone)]
pub struct CrmResult {
    pub loan_id: Vec<u64>,
    /// Original exposure (== `Loan.exposure` of the input row).
    pub exposure: Vec<f64>,
    /// Total collateral cover per loan (specified + greedy).
    pub collateral_cover: Vec<f64>,
    /// Total guarantee cover per loan (specified + greedy).
    pub guarantee_cover: Vec<f64>,
    /// Audited residual exposure after all covers (≥ 0; equals the
    /// `loan_remaining` of the last audit record for the loan).
    pub net_exposure: Vec<f64>,
    /// Full audit trail of every non-zero allocation, in allocation order.
    pub allocations: Vec<CrmAllocation>,
}

/// Input-validation errors from [`crm_alloc_greedy`].
#[derive(Debug, Clone, PartialEq)]
pub enum CrmError {
    DuplicateLoan(u64),
    DuplicateCollateral(u64),
    DuplicateGuarantor(u64),
    DanglingCollateralEdge {
        col_id: u64,
        loan_id: u64,
    },
    DanglingGuaranteeEdge {
        guarantor_id: u64,
        loan_id: u64,
    },
    /// The optional LP solver could not solve the model (message from the
    /// solver backend).
    LpSolver(String),
}

impl fmt::Display for CrmError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CrmError::DuplicateLoan(id) => write!(f, "duplicate loan_id {id}"),
            CrmError::DuplicateCollateral(id) => write!(f, "duplicate col_id {id}"),
            CrmError::DuplicateGuarantor(id) => write!(f, "duplicate guarantor_id {id}"),
            CrmError::DanglingCollateralEdge { col_id, loan_id } => {
                write!(
                    f,
                    "collateral edge ({col_id} -> {loan_id}) references an unknown node"
                )
            }
            CrmError::DanglingGuaranteeEdge {
                guarantor_id,
                loan_id,
            } => {
                write!(
                    f,
                    "guarantee edge ({guarantor_id} -> {loan_id}) references an unknown node"
                )
            }
            CrmError::LpSolver(msg) => write!(f, "lp solver failed: {msg}"),
        }
    }
}

impl Error for CrmError {}

/// Haircut-adjusted collateral value.
///
/// `C_adj = max(0, C × (1 − Hc − Hfx − Hmm))` where `haircut` is the standard
/// supervisory haircut, `fx_haircut` the currency-mismatch haircut and
/// `maturity_haircut` the maturity-mismatch adjustment. Results are floored at
/// zero — collateral fully wiped out by haircuts contributes no capacity.
pub fn adjusted_collateral_value(
    value: f64,
    haircut: f64,
    fx_haircut: f64,
    maturity_haircut: f64,
) -> f64 {
    if !(value > 0.0) {
        return 0.0;
    }
    let d = 1.0 - haircut - fx_haircut - maturity_haircut;
    if d <= 0.0 {
        0.0
    } else {
        value * d
    }
}

/// Deterministic source / loan ordering: priority descending, then id
/// ascending (NaN priorities sort as ties and fall back to id order), so runs
/// with equal priorities are still reproducible.
fn rank_desc(a_priority: f64, a_id: u64, b_priority: f64, b_id: u64) -> std::cmp::Ordering {
    b_priority
        .partial_cmp(&a_priority)
        .unwrap_or(std::cmp::Ordering::Equal)
        .then_with(|| a_id.cmp(&b_id))
}

/// Run the two-phase greedy CRM allocator (Phase 1 + Phase 2).
///
/// Phase 1 — *specified* edges, in edge-list order:
/// each edge covers `min(source.remaining, loan.remaining)`.
///
/// Phase 2 — *optimizable* edges, greedy / priority:
/// sources are consumed in (priority desc, id asc) order and each source walks
/// its optimizable edges in loan (priority desc, id asc) order, allocating
/// `min(source.remaining, loan.remaining)` per edge until the source or the
/// loan is exhausted.
///
/// Guarantee phases always follow their collateral counterparts, which realises
/// `CRMg = min(G, E − CRMc)` on the *residual* exposure after collateral.
///
/// Returns an error for duplicate ids or edges that reference unknown nodes —
/// a CRM allocator must never silently drop a contractual edge.
///
/// The optional `crm-lp` feature adds [`crate::crm_lp::crm_alloc_lp`] (Phase 3),
/// which shares this Phase-1 front end and replaces the greedy Phase 2 with an
/// exact LP optimizer.
pub fn crm_alloc_greedy(
    loans: &[Loan],
    collaterals: &[Collateral],
    guarantors: &[Guarantor],
    coll_edges: &[CollateralEdge],
    guar_edges: &[GuaranteeEdge],
) -> Result<CrmResult, CrmError> {
    let mut state = specified_phase(loans, collaterals, guarantors, coll_edges, guar_edges)?;
    greedy_phase(&mut state);
    Ok(state.finish())
}

// ---------------------------------------------------------------------------
// Shared Phase-1 front end + Phase-2 greedy engine.
//
// Both `crm_alloc_greedy` (phase 2) and the optional LP solver (phase 3,
// `crm_lp`) consume [`SpecifiedState`]: validation, densification and the
// contract-locked specified allocation are byte-identical regardless of which
// allocator runs afterwards.
// ---------------------------------------------------------------------------

/// A dense optimizable-pool edge (positions into the node arrays of a
/// [`SpecifiedState`]).
///
/// `cap` is the per-edge contractual upper bound used by the LP solver:
/// * collateral edges — `ratio × haircut-adjusted capacity`,
/// * guarantee edges — the guarantee-line `amount`,
/// * `f64::INFINITY` when the edge carries no cap (no ratio / no amount).
///
/// The greedy phase 2 ignores caps (it allocates `min(source, loan)` per edge,
/// matching `crm-allocation.md` §5/§6).
#[derive(Debug, Clone, Copy)]
pub(crate) struct OptEdge {
    pub src: usize,
    pub loan: usize,
    /// Per-edge cap consumed by the LP solver (`crm-lp` feature); the greedy
    /// engine never reads it, so it is dead code in feature-off builds.
    #[allow(dead_code)]
    pub cap: f64,
}

/// Shared trait over collateral and guarantor sources (id / priority used for
/// the deterministic greedy ordering and for the LP objective weights).
pub(crate) trait SourceNode {
    fn source_id(&self) -> u64;
    fn source_priority(&self) -> f64;
}

impl SourceNode for Collateral {
    fn source_id(&self) -> u64 {
        self.id
    }
    fn source_priority(&self) -> f64 {
        self.priority
    }
}

impl SourceNode for Guarantor {
    fn source_id(&self) -> u64 {
        self.id
    }
    fn source_priority(&self) -> f64 {
        self.priority
    }
}

/// Deterministic source ordering: priority desc, then id asc.
pub(crate) fn source_order<S: SourceNode>(sources: &[S]) -> Vec<usize> {
    let mut order: Vec<usize> = (0..sources.len()).collect();
    order.sort_by(|&a, &b| {
        rank_desc(
            sources[a].source_priority(),
            sources[a].source_id(),
            sources[b].source_priority(),
            sources[b].source_id(),
        )
    });
    order
}

/// Position of each loan in the (priority desc, id asc) ordering, shared by the
/// greedy engine and the LP phase for a deterministic allocation order.
pub(crate) fn loan_order_rank(loans: &[Loan]) -> Vec<usize> {
    let mut order: Vec<usize> = (0..loans.len()).collect();
    order.sort_by(|&a, &b| {
        rank_desc(
            loans[a].priority,
            loans[a].id,
            loans[b].priority,
            loans[b].id,
        )
    });
    let mut rank = vec![0usize; loans.len()];
    for (i, &l) in order.iter().enumerate() {
        rank[l] = i;
    }
    rank
}

/// State after validation + densification + Phase 1 (specified edges).
///
/// `*_remaining` are residual capacities / exposures *after* the specified
/// phase; the cover and audit fields hold the Phase-1 results. Optimizable
/// edges are dense (`src` / `loan` are positions) so the greedy engine and the
/// LP solver both operate without re-hashing ids.
#[derive(Debug, Clone)]
pub(crate) struct SpecifiedState {
    pub loans: Vec<Loan>,
    pub loan_remaining: Vec<f64>,
    pub collaterals: Vec<Collateral>,
    pub col_remaining: Vec<f64>,
    pub guarantors: Vec<Guarantor>,
    pub guar_remaining: Vec<f64>,
    pub collateral_cover: Vec<f64>,
    pub guarantee_cover: Vec<f64>,
    pub audit: Vec<CrmAllocation>,
    pub opt_coll_edges: Vec<OptEdge>,
    pub opt_guar_edges: Vec<OptEdge>,
}

/// Validate + densify inputs, then run Phase 1 (specified edges, edge-list
/// order). The returned state is consumed by the greedy Phase 2 or the LP
/// Phase 3.
pub(crate) fn specified_phase(
    loans: &[Loan],
    collaterals: &[Collateral],
    guarantors: &[Guarantor],
    coll_edges: &[CollateralEdge],
    guar_edges: &[GuaranteeEdge],
) -> Result<SpecifiedState, CrmError> {
    // dense position maps + duplicate / dangling validation
    let mut loan_pos: HashMap<u64, usize> = HashMap::with_capacity(loans.len());
    for (i, l) in loans.iter().enumerate() {
        if loan_pos.insert(l.id, i).is_some() {
            return Err(CrmError::DuplicateLoan(l.id));
        }
    }
    let mut col_pos: HashMap<u64, usize> = HashMap::with_capacity(collaterals.len());
    for (i, c) in collaterals.iter().enumerate() {
        if col_pos.insert(c.id, i).is_some() {
            return Err(CrmError::DuplicateCollateral(c.id));
        }
    }
    let mut guar_pos: HashMap<u64, usize> = HashMap::with_capacity(guarantors.len());
    for (i, g) in guarantors.iter().enumerate() {
        if guar_pos.insert(g.id, i).is_some() {
            return Err(CrmError::DuplicateGuarantor(g.id));
        }
    }
    for e in coll_edges {
        if !col_pos.contains_key(&e.col_id) || !loan_pos.contains_key(&e.loan_id) {
            return Err(CrmError::DanglingCollateralEdge {
                col_id: e.col_id,
                loan_id: e.loan_id,
            });
        }
    }
    for e in guar_edges {
        if !guar_pos.contains_key(&e.guarantor_id) || !loan_pos.contains_key(&e.loan_id) {
            return Err(CrmError::DanglingGuaranteeEdge {
                guarantor_id: e.guarantor_id,
                loan_id: e.loan_id,
            });
        }
    }

    let mut state = SpecifiedState {
        loans: loans.to_vec(),
        loan_remaining: loans.iter().map(|l| l.exposure).collect(),
        collaterals: collaterals.to_vec(),
        col_remaining: collaterals.iter().map(|c| c.capacity).collect(),
        guarantors: guarantors.to_vec(),
        guar_remaining: guarantors.iter().map(|g| g.capacity).collect(),
        collateral_cover: vec![0.0f64; loans.len()],
        guarantee_cover: vec![0.0f64; loans.len()],
        audit: Vec::new(),
        opt_coll_edges: Vec::new(),
        opt_guar_edges: Vec::new(),
    };

    // Phase 1a: specified collateral, in edge-list order.
    for e in coll_edges {
        if e.mode != AllocationMode::Specified {
            continue;
        }
        let (c, l) = (col_pos[&e.col_id], loan_pos[&e.loan_id]);
        if state.loan_remaining[l] <= 0.0 || state.col_remaining[c] <= 0.0 {
            continue;
        }
        let cover = state.col_remaining[c].min(state.loan_remaining[l]);
        state.col_remaining[c] -= cover;
        state.loan_remaining[l] -= cover;
        state.collateral_cover[l] += cover;
        state.audit.push(CrmAllocation {
            source_kind: CrmSourceKind::Collateral,
            stage: CrmStage::Specified,
            source_id: e.col_id,
            loan_id: e.loan_id,
            amount: cover,
            source_remaining: state.col_remaining[c],
            loan_remaining: state.loan_remaining[l],
        });
    }

    // Phase 1b: specified guarantee, in edge-list order.
    for e in guar_edges {
        if e.mode != AllocationMode::Specified {
            continue;
        }
        let (g, l) = (guar_pos[&e.guarantor_id], loan_pos[&e.loan_id]);
        if state.loan_remaining[l] <= 0.0 || state.guar_remaining[g] <= 0.0 {
            continue;
        }
        let cover = state.guar_remaining[g].min(state.loan_remaining[l]);
        state.guar_remaining[g] -= cover;
        state.loan_remaining[l] -= cover;
        state.guarantee_cover[l] += cover;
        state.audit.push(CrmAllocation {
            source_kind: CrmSourceKind::Guarantee,
            stage: CrmStage::Specified,
            source_id: e.guarantor_id,
            loan_id: e.loan_id,
            amount: cover,
            source_remaining: state.guar_remaining[g],
            loan_remaining: state.loan_remaining[l],
        });
    }

    // Dense optimizable-pool edges (+ per-edge caps for the LP solver).
    for e in coll_edges {
        if e.mode != AllocationMode::Optimizable {
            continue;
        }
        let (c, l) = (col_pos[&e.col_id], loan_pos[&e.loan_id]);
        let cap = if e.ratio > 0.0 {
            e.ratio * collaterals[c].capacity
        } else {
            f64::INFINITY
        };
        state.opt_coll_edges.push(OptEdge {
            src: c,
            loan: l,
            cap,
        });
    }
    for e in guar_edges {
        if e.mode != AllocationMode::Optimizable {
            continue;
        }
        let (g, l) = (guar_pos[&e.guarantor_id], loan_pos[&e.loan_id]);
        let cap = if e.amount > 0.0 {
            e.amount
        } else {
            f64::INFINITY
        };
        state.opt_guar_edges.push(OptEdge {
            src: g,
            loan: l,
            cap,
        });
    }

    Ok(state)
}

/// Phase 2: greedy / priority allocation over the optimizable pool, in place.
///
/// Collateral sources are consumed first, then guarantors (so guarantee covers
/// only the residual after *all* collateral); within a pool, sources in
/// (priority desc, id asc) order and each source walks its edges in loan
/// (priority desc, id asc) order. Allocates `min(source, loan)` per edge —
/// per-edge caps are deliberately ignored here (greedy reference semantics).
fn greedy_phase(state: &mut SpecifiedState) {
    let loan_rank = loan_order_rank(&state.loans);
    apply_greedy_pool(
        &state.loans,
        &mut state.loan_remaining,
        &mut state.collateral_cover,
        &state.collaterals,
        &mut state.col_remaining,
        &state.opt_coll_edges,
        &loan_rank,
        CrmSourceKind::Collateral,
        &mut state.audit,
    );
    apply_greedy_pool(
        &state.loans,
        &mut state.loan_remaining,
        &mut state.guarantee_cover,
        &state.guarantors,
        &mut state.guar_remaining,
        &state.opt_guar_edges,
        &loan_rank,
        CrmSourceKind::Guarantee,
        &mut state.audit,
    );
}

/// Greedy allocation over one source pool (collateral or guarantee). Only the
/// optimizable edges of [`SpecifiedState`] participate, so it never re-uses
/// capacity already locked by the specified phase.
#[allow(clippy::too_many_arguments)]
fn apply_greedy_pool<S: SourceNode>(
    loans: &[Loan],
    loan_remaining: &mut [f64],
    cover: &mut [f64],
    sources: &[S],
    source_remaining: &mut [f64],
    edges: &[OptEdge],
    loan_rank: &[usize],
    kind: CrmSourceKind,
    audit: &mut Vec<CrmAllocation>,
) {
    // per-source adjacency of optimizable loans, walked in loan-priority order
    let mut adj: Vec<Vec<usize>> = vec![Vec::new(); sources.len()];
    for e in edges {
        adj[e.src].push(e.loan);
    }
    for list in adj.iter_mut() {
        list.sort_unstable_by_key(|&l| loan_rank[l]);
        list.dedup();
    }
    let src_order = source_order(sources);
    for s in src_order {
        if source_remaining[s] <= 0.0 {
            continue;
        }
        for &l in &adj[s] {
            if loan_remaining[l] <= 0.0 {
                continue;
            }
            let cover_amount = source_remaining[s].min(loan_remaining[l]);
            if cover_amount <= 0.0 {
                continue;
            }
            source_remaining[s] -= cover_amount;
            loan_remaining[l] -= cover_amount;
            cover[l] += cover_amount;
            audit.push(CrmAllocation {
                source_kind: kind,
                stage: CrmStage::Greedy,
                source_id: sources[s].source_id(),
                loan_id: loans[l].id,
                amount: cover_amount,
                source_remaining: source_remaining[s],
                loan_remaining: loan_remaining[l],
            });
            if source_remaining[s] <= 0.0 {
                break;
            }
        }
    }
}

impl SpecifiedState {
    /// Consume the (fully allocated) state into a per-loan result. Net exposure
    /// is the audited residual remaining per loan, kept ≥ 0.
    pub(crate) fn finish(self) -> CrmResult {
        let loan_id: Vec<u64> = self.loans.iter().map(|l| l.id).collect();
        let exposure: Vec<f64> = self.loans.iter().map(|l| l.exposure).collect();
        let net_exposure: Vec<f64> = self.loan_remaining.iter().map(|r| r.max(0.0)).collect();
        CrmResult {
            loan_id,
            exposure,
            collateral_cover: self.collateral_cover,
            guarantee_cover: self.guarantee_cover,
            net_exposure,
            allocations: self.audit,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const fn loan(id: u64, exposure: f64) -> Loan {
        Loan {
            id,
            exposure,
            priority: 0.0,
        }
    }
    const fn col(id: u64, capacity: f64) -> Collateral {
        Collateral {
            id,
            capacity,
            priority: 0.0,
        }
    }
    const fn guar(id: u64, capacity: f64) -> Guarantor {
        Guarantor {
            id,
            capacity,
            priority: 0.0,
        }
    }

    #[test]
    fn haircut_adjusted_value() {
        // C_adj = C x (1 - Hc - Hfx - Hmm)
        assert!((adjusted_collateral_value(1000.0, 0.1, 0.05, 0.05) - 800.0).abs() < 1e-9);
        assert_eq!(adjusted_collateral_value(1000.0, 0.6, 0.6, 0.0), 0.0); // floored
        assert_eq!(adjusted_collateral_value(-5.0, 0.0, 0.0, 0.0), 0.0);
        assert_eq!(adjusted_collateral_value(0.0, 0.0, 0.0, 0.0), 0.0);
        assert_eq!(adjusted_collateral_value(100.0, 0.0, 0.0, 0.0), 100.0);
    }

    #[test]
    fn specified_collateral_is_allocated_before_optimizable() {
        // L1: specified C1(60) + optimizable C2(100). L2: optimizable C2(100).
        // C1 must be consumed first by contract, only then C2 is optimised.
        let loans = [loan(1, 100.0), loan(2, 100.0)];
        let cols = [col(10, 60.0), col(20, 100.0)];
        let ce = [
            CollateralEdge {
                col_id: 10,
                loan_id: 1,
                ratio: 1.0,
                mode: AllocationMode::Specified,
            },
            CollateralEdge {
                col_id: 20,
                loan_id: 1,
                ratio: 1.0,
                mode: AllocationMode::Optimizable,
            },
            CollateralEdge {
                col_id: 20,
                loan_id: 2,
                ratio: 1.0,
                mode: AllocationMode::Optimizable,
            },
        ];
        let r = crm_alloc_greedy(&loans, &cols, &[], &ce, &[]).unwrap();
        assert_eq!(r.loan_id, vec![1, 2]);
        assert!((r.collateral_cover[0] - 100.0).abs() < 1e-9); // 60 specified + 40 greedy
        assert!((r.collateral_cover[1] - 60.0).abs() < 1e-9);
        assert!((r.net_exposure[0]).abs() < 1e-9);
        assert!((r.net_exposure[1] - 40.0).abs() < 1e-9);
        assert_eq!(r.allocations.len(), 3);
        assert_eq!(r.allocations[0].stage, CrmStage::Specified);
        assert_eq!(r.allocations[0].source_id, 10);
        assert_eq!(r.allocations[0].amount, 60.0);
        // first greedy alloc is still the residual on L1, not L2
        assert_eq!(r.allocations[1].stage, CrmStage::Greedy);
        assert_eq!(r.allocations[1].loan_id, 1);
        assert_eq!(r.allocations[2].loan_id, 2);
    }

    #[test]
    fn guarantee_netting_formula_min_g_e_minus_crmc() {
        // L1 exp 100 with specified collateral 70 -> residual 30; guarantee
        // (capacity 100) must cover exactly min(G, 30) = 30.
        let loans = [loan(1, 100.0)];
        let cols = [col(10, 70.0)];
        let guars = [guar(100, 100.0)];
        let ce = [CollateralEdge {
            col_id: 10,
            loan_id: 1,
            ratio: 1.0,
            mode: AllocationMode::Specified,
        }];
        let ge = [GuaranteeEdge {
            guarantor_id: 100,
            loan_id: 1,
            amount: 100.0,
            mode: AllocationMode::Optimizable,
        }];
        let r = crm_alloc_greedy(&loans, &cols, &guars, &ce, &ge).unwrap();
        assert!((r.collateral_cover[0] - 70.0).abs() < 1e-9);
        assert!((r.guarantee_cover[0] - 30.0).abs() < 1e-9);
        assert!(r.net_exposure[0].abs() < 1e-9);
        assert_eq!(r.allocations.len(), 2);
        // guarantee record shows the residual loan state
        assert_eq!(r.allocations[1].source_kind, CrmSourceKind::Guarantee);
        assert_eq!(r.allocations[1].amount, 30.0);
        assert_eq!(r.allocations[1].source_remaining, 70.0);
        assert_eq!(r.allocations[1].loan_remaining, 0.0);
    }

    #[test]
    fn riskier_loan_is_covered_first() {
        // Single collateral C(100) shared by L2 (priority 1.0, riskier) and
        // L1 (priority 0.0). Loan priority must beat id order.
        let loans = [
            Loan {
                id: 1,
                exposure: 100.0,
                priority: 0.0,
            },
            Loan {
                id: 2,
                exposure: 100.0,
                priority: 1.0,
            },
        ];
        let cols = [col(10, 100.0)];
        let ce = [
            CollateralEdge {
                col_id: 10,
                loan_id: 1,
                ratio: 1.0,
                mode: AllocationMode::Optimizable,
            },
            CollateralEdge {
                col_id: 10,
                loan_id: 2,
                ratio: 1.0,
                mode: AllocationMode::Optimizable,
            },
        ];
        let r = crm_alloc_greedy(&loans, &cols, &[], &ce, &[]).unwrap();
        assert!(
            (r.collateral_cover[0]).abs() < 1e-9,
            "low-risk loan uncovered"
        );
        assert!(
            (r.collateral_cover[1] - 100.0).abs() < 1e-9,
            "risky loan fully covered"
        );
        assert_eq!(r.allocations[0].loan_id, 2);
    }

    #[test]
    fn source_priority_controls_consumption_order() {
        // Two collaterals (id 10 low priority 0, id 20 high priority 1) both
        // optimizable to L(150). Higher-priority source is consumed first and
        // fully drains before the next source is touched.
        let loans = [loan(1, 150.0)];
        let cols = [
            Collateral {
                id: 10,
                capacity: 100.0,
                priority: 0.0,
            },
            Collateral {
                id: 20,
                capacity: 100.0,
                priority: 1.0,
            },
        ];
        let ce = [
            CollateralEdge {
                col_id: 10,
                loan_id: 1,
                ratio: 1.0,
                mode: AllocationMode::Optimizable,
            },
            CollateralEdge {
                col_id: 20,
                loan_id: 1,
                ratio: 1.0,
                mode: AllocationMode::Optimizable,
            },
        ];
        let r = crm_alloc_greedy(&loans, &cols, &[], &ce, &[]).unwrap();
        assert_eq!(r.allocations[0].source_id, 20);
        assert_eq!(r.allocations[0].amount, 100.0);
        assert_eq!(r.allocations[1].source_id, 10);
        assert_eq!(r.allocations[1].amount, 50.0);
        assert!(r.net_exposure[0].abs() < 1e-9);
    }

    #[test]
    fn one_collateral_many_loans_shares_capacity() {
        // C(100) optimizable to three equal-priority loans of 60 each:
        // deterministic id-ascending walk covers 60 + 40 then stops.
        let loans = [loan(1, 60.0), loan(2, 60.0), loan(3, 60.0)];
        let cols = [col(10, 100.0)];
        let ce: Vec<CollateralEdge> = [1, 2, 3]
            .iter()
            .map(|&lid| CollateralEdge {
                col_id: 10,
                loan_id: lid,
                ratio: 1.0,
                mode: AllocationMode::Optimizable,
            })
            .collect();
        let r = crm_alloc_greedy(&loans, &cols, &[], &ce, &[]).unwrap();
        assert_eq!(r.collateral_cover, vec![60.0, 40.0, 0.0]);
        assert_eq!(r.net_exposure, vec![0.0, 20.0, 60.0]);
    }

    #[test]
    fn specified_guarantee_phase_runs_before_greedy_collateral() {
        // crm-allocation.md phase order: specified col -> specified guar ->
        // greedy col -> greedy guar. A specified guarantee(80) locks first, the
        // optimizable collateral(100) then only covers the residual 20.
        let loans = [loan(1, 100.0)];
        let cols = [col(10, 100.0)];
        let guars = [guar(100, 80.0)];
        let ce = [CollateralEdge {
            col_id: 10,
            loan_id: 1,
            ratio: 1.0,
            mode: AllocationMode::Optimizable,
        }];
        let ge = [GuaranteeEdge {
            guarantor_id: 100,
            loan_id: 1,
            amount: 80.0,
            mode: AllocationMode::Specified,
        }];
        let r = crm_alloc_greedy(&loans, &cols, &guars, &ce, &ge).unwrap();
        assert_eq!(r.allocations.len(), 2);
        assert_eq!(r.allocations[0].source_kind, CrmSourceKind::Guarantee);
        assert_eq!(r.allocations[0].stage, CrmStage::Specified);
        assert_eq!(r.allocations[0].amount, 80.0);
        assert_eq!(r.allocations[1].source_kind, CrmSourceKind::Collateral);
        assert_eq!(r.allocations[1].stage, CrmStage::Greedy);
        assert_eq!(r.allocations[1].amount, 20.0);
        assert!(r.net_exposure[0].abs() < 1e-9);
        assert!((r.guarantee_cover[0] - 80.0).abs() < 1e-9);
        assert!((r.collateral_cover[0] - 20.0).abs() < 1e-9);
    }

    #[test]
    fn sparse_ids_are_supported_and_results_aligned_to_input() {
        let loans = [loan(1000, 80.0), loan(2000, 50.0)];
        let cols = [col(9000, 120.0)];
        let ce = [
            CollateralEdge {
                col_id: 9000,
                loan_id: 2000,
                ratio: 1.0,
                mode: AllocationMode::Optimizable,
            },
            CollateralEdge {
                col_id: 9000,
                loan_id: 1000,
                ratio: 1.0,
                mode: AllocationMode::Optimizable,
            },
        ];
        // loan 2000 (id 2000) is riskier? equal priority -> id ascending:
        // 1000 covered first (80), then 2000 (40 of remaining 40).
        let r = crm_alloc_greedy(&loans, &cols, &[], &ce, &[]).unwrap();
        assert_eq!(r.loan_id, vec![1000, 2000]);
        assert_eq!(r.collateral_cover, vec![80.0, 40.0]);
        assert_eq!(r.net_exposure, vec![0.0, 10.0]);
    }

    #[test]
    fn deterministic_across_reruns() {
        let loans = [
            Loan {
                id: 3,
                exposure: 90.0,
                priority: 0.4,
            },
            Loan {
                id: 1,
                exposure: 70.0,
                priority: 0.9,
            },
            Loan {
                id: 2,
                exposure: 50.0,
                priority: 0.9,
            },
        ];
        let cols = [
            Collateral {
                id: 30,
                capacity: 80.0,
                priority: 0.5,
            },
            Collateral {
                id: 20,
                capacity: 80.0,
                priority: 0.5,
            },
        ];
        let guars = [Guarantor {
            id: 5,
            capacity: 100.0,
            priority: 2.0,
        }];
        let ce: Vec<CollateralEdge> = (1..=3)
            .flat_map(|lid| {
                [20, 30].iter().map(move |&cid| CollateralEdge {
                    col_id: cid,
                    loan_id: lid,
                    ratio: 1.0,
                    mode: if lid % 2 == 0 {
                        AllocationMode::Specified
                    } else {
                        AllocationMode::Optimizable
                    },
                })
            })
            .collect();
        let ge: Vec<GuaranteeEdge> = (1..=3)
            .map(|lid| GuaranteeEdge {
                guarantor_id: 5,
                loan_id: lid,
                amount: 40.0,
                mode: AllocationMode::Optimizable,
            })
            .collect();

        let a = crm_alloc_greedy(&loans, &cols, &guars, &ce, &ge).unwrap();
        let b = crm_alloc_greedy(&loans, &cols, &guars, &ce, &ge).unwrap();
        assert_eq!(a.loan_id, b.loan_id);
        assert_eq!(a.collateral_cover, b.collateral_cover);
        assert_eq!(a.guarantee_cover, b.guarantee_cover);
        assert_eq!(a.net_exposure, b.net_exposure);
        assert_eq!(a.allocations.len(), b.allocations.len());
        for (x, y) in a.allocations.iter().zip(b.allocations.iter()) {
            assert_eq!(x.source_id, y.source_id);
            assert_eq!(x.loan_id, y.loan_id);
            assert_eq!(x.amount, y.amount);
            assert_eq!(x.stage, y.stage);
            assert_eq!(x.source_kind, y.source_kind);
        }
    }

    #[test]
    fn audit_invariants_no_source_overallocated_no_loan_overcovered() {
        // Random-ish connected instance: after allocation, sum per source in
        // the audit must never exceed capacity and per-loan total covers must
        // never exceed exposure.
        let loans: Vec<Loan> = (1..=8).map(|i| loan(i, 10.0 * i as f64)).collect();
        let cols: Vec<Collateral> = (1..=4).map(|i| col(100 + i, 25.0 * i as f64)).collect();
        let guars: Vec<Guarantor> = (1..=3).map(|i| guar(200 + i, 20.0 * i as f64)).collect();
        let ce: Vec<CollateralEdge> = (1..=4)
            .flat_map(move |ci| {
                (1..=8)
                    .filter(move |li| (ci + li) % 2 == 0)
                    .map(move |li| CollateralEdge {
                        col_id: 100 + ci,
                        loan_id: li,
                        ratio: 0.5,
                        mode: if li % 3 == 0 {
                            AllocationMode::Specified
                        } else {
                            AllocationMode::Optimizable
                        },
                    })
            })
            .collect();
        let ge: Vec<GuaranteeEdge> = (1..=3)
            .flat_map(move |gi| {
                (1..=8)
                    .filter(move |li| (gi * li) % 2 == 1)
                    .map(move |li| GuaranteeEdge {
                        guarantor_id: 200 + gi,
                        loan_id: li,
                        amount: 5.0,
                        mode: if li % 2 == 0 {
                            AllocationMode::Specified
                        } else {
                            AllocationMode::Optimizable
                        },
                    })
            })
            .collect();

        let r = crm_alloc_greedy(&loans, &cols, &guars, &ce, &ge).unwrap();
        let mut per_source: std::collections::HashMap<u64, f64> = std::collections::HashMap::new();
        let mut per_loan: std::collections::HashMap<u64, f64> = std::collections::HashMap::new();
        for a in &r.allocations {
            assert!(a.amount > 0.0);
            *per_source.entry(a.source_id).or_insert(0.0) += a.amount;
            *per_loan.entry(a.loan_id).or_insert(0.0) += a.amount;
        }
        for c in &cols {
            assert!(per_source.get(&c.id).copied().unwrap_or(0.0) <= c.capacity + 1e-9);
        }
        for g in &guars {
            assert!(per_source.get(&g.id).copied().unwrap_or(0.0) <= g.capacity + 1e-9);
        }
        for l in &loans {
            assert!(per_loan.get(&l.id).copied().unwrap_or(0.0) <= l.exposure + 1e-9);
        }
        // net exposure consistency
        for i in 0..r.loan_id.len() {
            let net = r.exposure[i] - r.collateral_cover[i] - r.guarantee_cover[i];
            assert!((net - r.net_exposure[i]).abs() < 1e-9);
            assert!(r.net_exposure[i] > -1e-9);
        }
    }

    #[test]
    fn rejects_duplicate_and_dangling_ids() {
        assert!(matches!(
            crm_alloc_greedy(&[loan(1, 10.0), loan(1, 20.0)], &[], &[], &[], &[]),
            Err(CrmError::DuplicateLoan(1))
        ));
        assert!(matches!(
            crm_alloc_greedy(&[], &[col(5, 10.0), col(5, 9.0)], &[], &[], &[]),
            Err(CrmError::DuplicateCollateral(5))
        ));
        assert!(matches!(
            crm_alloc_greedy(&[], &[], &[guar(7, 1.0), guar(7, 2.0)], &[], &[]),
            Err(CrmError::DuplicateGuarantor(7))
        ));
        // edge to an unknown collateral
        let err = crm_alloc_greedy(
            &[loan(1, 10.0)],
            &[col(5, 10.0)],
            &[],
            &[CollateralEdge {
                col_id: 999,
                loan_id: 1,
                ratio: 1.0,
                mode: AllocationMode::Specified,
            }],
            &[],
        )
        .unwrap_err();
        assert!(matches!(
            err,
            CrmError::DanglingCollateralEdge {
                col_id: 999,
                loan_id: 1
            }
        ));
        // edge to an unknown loan
        let err = crm_alloc_greedy(
            &[loan(1, 10.0)],
            &[],
            &[guar(5, 10.0)],
            &[],
            &[GuaranteeEdge {
                guarantor_id: 5,
                loan_id: 777,
                amount: 1.0,
                mode: AllocationMode::Specified,
            }],
        )
        .unwrap_err();
        assert!(matches!(
            err,
            CrmError::DanglingGuaranteeEdge {
                guarantor_id: 5,
                loan_id: 777
            }
        ));
    }
}
