//! Phase 3 — exact LP allocator over the optimizable CRM pool (optional).
//!
//! `crm_alloc_lp` shares the Phase-1 front end of [`crate::crm`] (specified /
//! contract-locked edges are allocated first and are byte-identical to the
//! greedy path) and replaces the greedy Phase 2 with an exact linear program:
//!
//! ```text
//! variables   x_e ≥ 0                          for each optimizable edge e
//! objective   max  Σ_e  weight(loan(e)) · x_e   (weight = loan risk priority)
//! subject to  ∀ loan l        Σ_{e→l} x_e ≤ demand_l          (residual after Phase 1)
//!             ∀ source s      Σ_{e←s} x_e ≤ capacity_s        (residual after Phase 1)
//!             ∀ edge e with cap:  x_e ≤ cap_e
//! ```
//!
//! `cap_e` is the *contractual per-edge bound* carried by the tables:
//! collateral edges cap at `ratio × haircut-adjusted capacity`, guarantee edges
//! at the guarantee-line `amount` (`∞` when the edge has no cap data). The
//! greedy phase 2 deliberately ignores those caps (reference semantics); the LP
//! phase honours them — that is exactly where the "mathematical optimum" can
//! differ from the greedy heuristic, because the LP sees the whole pool at once
//! instead of consuming sources one at a time.
//!
//! The solver backend is abstracted by `good_lp`: the optional `crm-lp` feature
//! currently builds the pure-Rust `microlp` backend (no C/C++ toolchain
//! needed); swap it for e.g. `highs` in `gtv-array/Cargo.toml` for larger
//! models — the modelling code below is unchanged.
//!
//! Results (covers / net exposure / audit trail) use the same schema as the
//! greedy allocator, so `crm_alloc` (`method='lp'`) results can be diffed
//! loan-by-loan against `method='greedy'`.

#![cfg(feature = "crm-lp")]

use good_lp::Variable;
use good_lp::{default_solver, variable, Expression, ProblemVariables, Solution, SolverModel};

use crate::crm::{
    source_order, specified_phase, Collateral, CollateralEdge, CrmAllocation, CrmError, CrmResult,
    CrmSourceKind, CrmStage, GuaranteeEdge, Guarantor, Loan, OptEdge, SpecifiedState,
};

/// Minimal positive amount to keep from a solved LP variable.
const EPS: f64 = 1e-9;

/// Phase-3 entry point: Phase 1 (specified) + exact LP over the optimizable
/// pool. See the module docs for the model.
pub fn crm_alloc_lp(
    loans: &[Loan],
    collaterals: &[Collateral],
    guarantors: &[Guarantor],
    coll_edges: &[CollateralEdge],
    guar_edges: &[GuaranteeEdge],
) -> Result<CrmResult, CrmError> {
    let mut state = specified_phase(loans, collaterals, guarantors, coll_edges, guar_edges)?;
    solve_optimizable_pool(&mut state)?;
    Ok(state.finish())
}

/// One candidate optimizable edge fed to the LP (dense positions).
struct Cand {
    /// `true` = collateral source pool, `false` = guarantor pool.
    collateral: bool,
    src: usize,
    loan: usize,
    /// Contractual per-edge upper bound (finite) or unbounded.
    cap: f64,
}

/// Candidate index groupings for constraint assembly.
struct Cands {
    items: Vec<Cand>,
    by_loan: Vec<Vec<usize>>,
    by_coll: Vec<Vec<usize>>,
    by_guar: Vec<Vec<usize>>,
}

impl Cands {
    fn new(n_loans: usize, n_coll: usize, n_guar: usize) -> Self {
        Cands {
            items: Vec::new(),
            by_loan: vec![Vec::new(); n_loans],
            by_coll: vec![Vec::new(); n_coll],
            by_guar: vec![Vec::new(); n_guar],
        }
    }

    fn push(&mut self, c: Cand) -> usize {
        let i = self.items.len();
        self.by_loan[c.loan].push(i);
        if c.collateral {
            self.by_coll[c.src].push(i);
        } else {
            self.by_guar[c.src].push(i);
        }
        self.items.push(c);
        i
    }
}

fn solve_optimizable_pool(state: &mut SpecifiedState) -> Result<(), CrmError> {
    // ------------------------------------------------------------------
    // Candidate edges (only endpoints with positive residual demand/capacity
    // and a positive per-edge cap can receive cover).
    // ------------------------------------------------------------------
    let mut cands = Cands::new(
        state.loans.len(),
        state.collaterals.len(),
        state.guarantors.len(),
    );
    let mut collect = |pool_collateral: bool, edges: &[OptEdge], src_remaining: &[f64]| {
        for e in edges {
            if e.cap.is_finite() && e.cap <= 0.0 {
                continue;
            }
            if state.loan_remaining[e.loan] <= 0.0 || src_remaining[e.src] <= 0.0 {
                continue;
            }
            cands.push(Cand {
                collateral: pool_collateral,
                src: e.src,
                loan: e.loan,
                cap: e.cap,
            });
        }
    };
    collect(true, &state.opt_coll_edges, &state.col_remaining);
    collect(false, &state.opt_guar_edges, &state.guar_remaining);

    if cands.items.is_empty() {
        return Ok(()); // nothing optimizable left — Phase 1 only
    }

    // ------------------------------------------------------------------
    // Build the model.
    // ------------------------------------------------------------------
    let mut vars = ProblemVariables::new();
    let mut x: Vec<Variable> = Vec::with_capacity(cands.items.len());
    for c in &cands.items {
        let mut def = variable().min(0.0);
        if c.cap.is_finite() {
            def = def.max(c.cap);
        }
        x.push(vars.add(def));
    }

    // Objective weight = loan risk priority (pd-derived). If the slice carries
    // no priority at all, fall back to uniform weights (maximise total cover).
    let weights: Vec<f64> = state.loans.iter().map(|l| l.priority).collect();
    let max_w = weights.iter().cloned().fold(0.0f64, f64::max);
    let mut objective = Expression::default();
    for (i, c) in cands.items.iter().enumerate() {
        let w = if max_w > 0.0 { weights[c.loan] } else { 1.0 };
        objective += w * x[i];
    }
    let mut model = vars.maximise(objective).using(default_solver);

    for (l, members) in cands.by_loan.iter().enumerate() {
        if state.loan_remaining[l] <= 0.0 || members.is_empty() {
            continue;
        }
        let mut e = Expression::default();
        for &i in members {
            e += x[i];
        }
        model.add_constraint(e.leq(state.loan_remaining[l]));
    }
    for (s, members) in cands.by_coll.iter().enumerate() {
        if state.col_remaining[s] <= 0.0 || members.is_empty() {
            continue;
        }
        let mut e = Expression::default();
        for &i in members {
            e += x[i];
        }
        model.add_constraint(e.leq(state.col_remaining[s]));
    }
    for (s, members) in cands.by_guar.iter().enumerate() {
        if state.guar_remaining[s] <= 0.0 || members.is_empty() {
            continue;
        }
        let mut e = Expression::default();
        for &i in members {
            e += x[i];
        }
        model.add_constraint(e.leq(state.guar_remaining[s]));
    }

    // ------------------------------------------------------------------
    // Solve.
    // ------------------------------------------------------------------
    let solution = model
        .solve()
        .map_err(|err| CrmError::LpSolver(err.to_string()))?;

    let mut pending: Vec<(usize, f64)> = cands
        .items
        .iter()
        .enumerate()
        .filter_map(|(i, _)| {
            let v = solution.value(x[i]);
            if v > EPS {
                Some((i, v))
            } else {
                None
            }
        })
        .collect();
    if pending.is_empty() {
        return Ok(());
    }

    // ------------------------------------------------------------------
    // Apply the solution in a deterministic order (collateral pool first,
    // then guarantors — mirroring the greedy phase order — with sources in
    // priority order and loans in risk order). The LP already respects every
    // constraint; re-clamping guards against solver tolerance overshoot and
    // keeps `net_exposure` exact from the audit trail.
    // ------------------------------------------------------------------
    let loan_rank = crate::crm::loan_order_rank(&state.loans);
    let coll_pos: Vec<usize> = invert(source_order(&state.collaterals));
    let guar_pos: Vec<usize> = invert(source_order(&state.guarantors));
    pending.sort_by_key(|&(i, _)| {
        let c = &cands.items[i];
        let pool = if c.collateral { 0usize } else { 1usize };
        let src = if c.collateral {
            coll_pos[c.src]
        } else {
            guar_pos[c.src]
        };
        (pool, src, loan_rank[c.loan], i)
    });

    for (i, amount) in pending {
        let c = &cands.items[i];
        let cap = if c.cap.is_finite() {
            c.cap
        } else {
            f64::INFINITY
        };
        let src_rem = if c.collateral {
            state.col_remaining[c.src]
        } else {
            state.guar_remaining[c.src]
        };
        let amt = amount
            .min(src_rem)
            .min(state.loan_remaining[c.loan])
            .min(cap);
        if amt <= EPS {
            continue;
        }
        let kind = if c.collateral {
            CrmSourceKind::Collateral
        } else {
            CrmSourceKind::Guarantee
        };
        let source_id = if c.collateral {
            state.collaterals[c.src].id
        } else {
            state.guarantors[c.src].id
        };
        // mutate residual / covers (the sorted + clamped order keeps the audit
        // deterministic and net_exposure exact from the trail)
        state.loan_remaining[c.loan] -= amt;
        if c.collateral {
            state.col_remaining[c.src] -= amt;
            state.collateral_cover[c.loan] += amt;
        } else {
            state.guar_remaining[c.src] -= amt;
            state.guarantee_cover[c.loan] += amt;
        }
        state.audit.push(CrmAllocation {
            source_kind: kind,
            stage: CrmStage::Lp,
            source_id,
            loan_id: state.loans[c.loan].id,
            amount: amt,
            source_remaining: if c.collateral {
                state.col_remaining[c.src]
            } else {
                state.guar_remaining[c.src]
            },
            loan_remaining: state.loan_remaining[c.loan],
        });
    }
    Ok(())
}

/// `order[i] = source index at rank i` → `pos[source index] = rank i`.
fn invert(order: Vec<usize>) -> Vec<usize> {
    let mut pos = vec![0usize; order.len()];
    for (i, &s) in order.iter().enumerate() {
        pos[s] = i;
    }
    pos
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crm::AllocationMode;

    const fn loan(id: u64, exposure: f64, priority: f64) -> Loan {
        Loan {
            id,
            exposure,
            priority,
        }
    }
    const fn col(id: u64, capacity: f64, priority: f64) -> Collateral {
        Collateral {
            id,
            capacity,
            priority,
        }
    }
    const fn col_edge(col_id: u64, loan_id: u64, ratio: f64) -> CollateralEdge {
        CollateralEdge {
            col_id,
            loan_id,
            ratio,
            mode: AllocationMode::Optimizable,
        }
    }

    fn total_cover(r: &CrmResult) -> f64 {
        r.collateral_cover.iter().sum::<f64>() + r.guarantee_cover.iter().sum::<f64>()
    }

    #[test]
    fn lp_respects_per_edge_caps_that_greedy_ignores() {
        // One collateral (cap 100). Two loans each ratio 0.4 -> per-edge cap 40.
        // Greedy (no caps) pours 80 into the riskier loan then 20 into the other;
        // the LP is constrained to 40/40 by the contractual ratio caps.
        let loans = [loan(1, 80.0, 2.0), loan(2, 80.0, 1.0)];
        let cols = [col(10, 100.0, 3.0)];
        let ce = [col_edge(10, 1, 0.4), col_edge(10, 2, 0.4)];

        let greedy = crate::crm::crm_alloc_greedy(&loans, &cols, &[], &ce, &[]).unwrap();
        assert_eq!(greedy.collateral_cover, vec![80.0, 20.0]); // no cap awareness

        let lp = crm_alloc_lp(&loans, &cols, &[], &ce, &[]).unwrap();
        assert!(
            lp.collateral_cover[0] <= 40.0 + 1e-6,
            "LP must honour the 0.4 ratio cap"
        );
        assert!(lp.collateral_cover[1] <= 40.0 + 1e-6);
        assert!(
            (total_cover(&lp) - (40.0 + 40.0)).abs() < 1e-4,
            "caps leave supply unused: {}",
            total_cover(&lp)
        );
        assert!(lp.net_exposure.iter().all(|&n| n >= -1e-9));
    }

    #[test]
    fn lp_without_binding_caps_matches_greedy_objective() {
        // ratio = 1.0 -> per-edge cap = full capacity (never binding): the LP
        // feasible set is the greedy one, and with a separable objective the
        // greedy water-filling is optimal, so both must use the full supply.
        let loans = [
            loan(1, 100.0, 0.9),
            loan(2, 100.0, 0.5),
            loan(3, 100.0, 0.2),
        ];
        let cols = [col(10, 100.0, 0.0), col(20, 100.0, 0.0)];
        let ce = [
            col_edge(10, 1, 1.0),
            col_edge(10, 2, 1.0),
            col_edge(10, 3, 1.0),
            col_edge(20, 1, 1.0),
            col_edge(20, 2, 1.0),
            col_edge(20, 3, 1.0),
        ];
        let greedy = crate::crm::crm_alloc_greedy(&loans, &cols, &[], &ce, &[]).unwrap();
        let lp = crm_alloc_lp(&loans, &cols, &[], &ce, &[]).unwrap();
        assert!((total_cover(&greedy) - 200.0).abs() < 1e-9);
        assert!((total_cover(&lp) - 200.0).abs() < 1e-9);
        for i in 0..3 {
            assert!(lp.net_exposure[i] >= -1e-9);
        }
        // audit trail is complete + deterministic
        let again = crm_alloc_lp(&loans, &cols, &[], &ce, &[]).unwrap();
        assert_eq!(lp.collateral_cover, again.collateral_cover);
        assert_eq!(lp.guarantee_cover, again.guarantee_cover);
        assert_eq!(lp.net_exposure, again.net_exposure);
    }

    #[test]
    fn lp_shares_specified_phase_with_greedy() {
        // Contract-locked edge (40) is identical under both engines; only the
        // remaining optimizable pool differs (greedy vs LP).
        let loans = [loan(1, 100.0, 2.0), loan(2, 100.0, 1.0)];
        let cols = [col(10, 60.0, 3.0), col(20, 100.0, 3.0)];
        let ce = [
            CollateralEdge {
                col_id: 10,
                loan_id: 1,
                ratio: 1.0,
                mode: AllocationMode::Specified, // locks 60 of loan 1
            },
            col_edge(20, 1, 0.5), // LP cap = 50
            col_edge(20, 2, 0.5),
        ];
        let greedy = crate::crm::crm_alloc_greedy(&loans, &cols, &[], &ce, &[]).unwrap();
        let lp = crm_alloc_lp(&loans, &cols, &[], &ce, &[]).unwrap();
        // both respect the specified 60
        assert!((greedy.collateral_cover[0].min(lp.collateral_cover[0])) >= 60.0 - 1e-9);
        assert_eq!(
            greedy
                .allocations
                .iter()
                .filter(|a| a.stage == CrmStage::Specified)
                .count(),
            lp.allocations
                .iter()
                .filter(|a| a.stage == CrmStage::Specified)
                .count()
        );
        assert!(
            lp.allocations.iter().any(|a| a.stage == CrmStage::Lp),
            "LP records its own audit stage"
        );
        assert!(lp.net_exposure.iter().all(|&n| n >= -1e-9));
    }
}
