#!/usr/bin/env python3
"""Generate CRM (credit-risk-mitigation) test data for gtvdb.

Schema + usage follow `crm-allocation.md` (repo root) — five CSV files:

    loan_exposure.csv   (loan_id, ead, pd, lgd, ccr_ead, pv, rating, scenario_id, valid_from, valid_to)
    collateral.csv      (col_id, value, haircut, fx_haircut, maturity_mm, type, valid_from, valid_to)
    guarantee.csv       (guarantor_id, amount, rating, valid_from, valid_to)
    collateral_edges.csv(col_id, loan_id, ratio, allocation_mode, valid_from, valid_to)
    guarantee_edges.csv (guarantor_id, loan_id, amount, allocation_mode, valid_from, valid_to)

Load them into the gtv shell with `LOAD CSV '<file>' INTO <table>` and run the
greedy allocator, e.g.:

    gtv> SELECT * FROM crm_alloc('loan_exposure','collateral','guarantee',
              'collateral_edges','guarantee_edges','BASE',-1);

Defaults are sized for a scale / perf test:
    50,000 loans and 100,000 collaterals (override with --loans / --collateral).
Guarantors default to loans/10 (5,000) and each source keeps the fan-out of the
reference spec (10 loans per collateral, 15 loans per guarantor).

Example:
    python3 crm_data.py                       # 50k loans / 100k collateral
    python3 crm_data.py --loans 5000 --collateral 10000 --dir out --seed 7
"""

import argparse
import csv
import os
import random
import time


# ---------------------------------------------------------------------------
# Value distributions.
#
# Loan / guarantor amounts keep the ranges of crm-allocation.md §2.  Collateral
# values are RE-BASED for the large defaults (100k collateral vs 50k loans): at
# the reference 10–50M range, 100k items would supply ~3T against ~625B of loan
# EAD and the allocator would saturate (collateral cover everything, guarantee
# unused).  Here gross collateral value is tuned to the same order as loan EAD
# so the scale run exercises specified/greedy ordering, guarantee netting and
# positive residual exposure.  Tweak the tuples below to change the mix.
# ---------------------------------------------------------------------------
LOAN_EAD = (5e6, 20e6)
LOAN_PD = (0.01, 0.05)
LOAN_LGD = (0.3, 0.6)
LOAN_CCR_EAD = (5e6, 20e6)
LOAN_PV = (5e6, 20e6)
LOAN_RATINGS = ["A", "BBB", "BB"]

COLLATERAL_VALUE = (1e6, 6e6)          # gross; haircuts reduce usable capacity
COLLATERAL_HAIRCUT = (0.05, 0.15)
COLLATERAL_FX_HAIRCUT = (0.0, 0.05)
COLLATERAL_MATURITY = (0.0, 0.05)
COLLATERAL_TYPES = ["CASH", "BOND", "EQUITY"]

GUARANTOR_AMOUNT = (20e6, 80e6)
GUARANTOR_RATINGS = ["AAA", "AA", "A"]

EDGE_RATIO = (0.05, 0.3)               # collateral edge ratio (informational)
GUARANTEE_EDGE_AMOUNT = (1e6, 10e6)


def _u(rng, lo, hi):
    return round(rng.uniform(lo, hi), 2)  # monetary amounts: 2 dp


def loan_fields(rng):
    return [
        _u(rng, *LOAN_EAD),               # ead
        round(rng.uniform(*LOAN_PD), 6),   # pd
        round(rng.uniform(*LOAN_LGD), 6),  # lgd
        _u(rng, *LOAN_CCR_EAD),            # ccr_ead
        _u(rng, *LOAN_PV),                 # pv
        rng.choice(LOAN_RATINGS),          # rating
    ]


def collateral_fields(rng):
    return [
        _u(rng, *COLLATERAL_VALUE),                          # value
        round(rng.uniform(*COLLATERAL_HAIRCUT), 4),          # haircut
        round(rng.uniform(*COLLATERAL_FX_HAIRCUT), 4),       # fx_haircut
        round(rng.uniform(*COLLATERAL_MATURITY), 4),         # maturity_mm
        rng.choice(COLLATERAL_TYPES),                        # type
    ]


def guarantee_fields(rng):
    return [
        _u(rng, *GUARANTOR_AMOUNT),        # amount
        rng.choice(GUARANTOR_RATINGS),     # rating
    ]


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--loans", type=int, default=50_000,
                    help="number of loan_exposure rows (default 50_000)")
    ap.add_argument("--collateral", type=int, default=100_000,
                    help="number of collateral rows (default 100_000)")
    ap.add_argument("--guarantors", type=int, default=None,
                    help="number of guarantor rows (default loans//10)")
    ap.add_argument("--coll-fanout", type=int, default=10,
                    help="optimizable/specified loan edges per collateral (default 10)")
    ap.add_argument("--guar-fanout", type=int, default=15,
                    help="loan edges per guarantor (default 15)")
    ap.add_argument("--dir", default=".",
                    help="output directory (default: current directory)")
    ap.add_argument("--seed", type=int, default=None,
                    help="random seed for reproducible output")
    args = ap.parse_args()

    n_loans = max(1, args.loans)
    n_cols = max(1, args.collateral)
    n_guars = max(
        1, args.guarantors if args.guarantors is not None else n_loans // 10
    )
    coll_fanout = min(max(1, args.coll_fanout), n_loans)
    guar_fanout = min(max(1, args.guar_fanout), n_loans)
    n_coll_edges = n_cols * coll_fanout
    n_guar_edges = n_guars * guar_fanout

    rng = random.Random(args.seed)
    now = int(time.time() * 1_000_000_000)      # valid window: T = now (±10 µs)
    out = os.path.abspath(args.dir)
    os.makedirs(out, exist_ok=True)

    # aggregate counters (for the mix summary at the end)
    loan_ead_total = 0.0
    coll_value_total = 0.0
    guar_amount_total = 0.0

    def path(name):
        return os.path.join(out, name)

    # 1. loan_exposure.csv — 50k loans
    with open(path("loan_exposure.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["loan_id", "ead", "pd", "lgd", "ccr_ead", "pv",
                    "rating", "scenario_id", "valid_from", "valid_to"])
        for i in range(1, n_loans + 1):
            fields = loan_fields(rng)
            loan_ead_total += fields[0]
            w.writerow([i, *fields, "BASE", now - 10_000, now + 10_000])

    # 2. collateral.csv — 100k collateral
    with open(path("collateral.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["col_id", "value", "haircut", "fx_haircut", "maturity_mm",
                    "type", "valid_from", "valid_to"])
        for i in range(1, n_cols + 1):
            fields = collateral_fields(rng)
            coll_value_total += fields[0]
            w.writerow([i, *fields, now - 10_000, now + 10_000])

    # 3. guarantee.csv — defaults to loans/10 guarantors
    with open(path("guarantee.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["guarantor_id", "amount", "rating", "valid_from", "valid_to"])
        for i in range(1, n_guars + 1):
            fields = guarantee_fields(rng)
            guar_amount_total += fields[0]
            w.writerow([i, *fields, now - 10_000, now + 10_000])

    # 4. collateral_edges.csv — each collateral pledges to `coll-fanout` loans
    loan_ids = range(1, n_loans + 1)
    with open(path("collateral_edges.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["col_id", "loan_id", "ratio", "allocation_mode",
                    "valid_from", "valid_to"])
        for col in range(1, n_cols + 1):
            for loan in rng.sample(loan_ids, coll_fanout):
                w.writerow([
                    col,
                    loan,
                    round(rng.uniform(0.05, 0.3), 4),
                    rng.choice(["specified", "optimizable"]),
                    now - 10_000,
                    now + 10_000,
                ])

    # 5. guarantee_edges.csv — each guarantor guarantees `guar-fanout` loans
    with open(path("guarantee_edges.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["guarantor_id", "loan_id", "amount", "allocation_mode",
                    "valid_from", "valid_to"])
        for g in range(1, n_guars + 1):
            for loan in rng.sample(loan_ids, guar_fanout):
                w.writerow([
                    g,
                    loan,
                    round(rng.uniform(1e6, 10e6), 2),
                    rng.choice(["specified", "optimizable"]),
                    now - 10_000,
                    now + 10_000,
                ])

    print(f"wrote CRM test data to {out}:")
    print(f"  loan_exposure.csv     {n_loans:>10,} rows")
    print(f"  collateral.csv        {n_cols:>10,} rows")
    print(f"  guarantee.csv         {n_guars:>10,} rows")
    print(f"  collateral_edges.csv  {n_coll_edges:>10,} rows")
    print(f"  guarantee_edges.csv   {n_guar_edges:>10,} rows")
    print(
        f"  aggregate: loan EAD ≈ {loan_ead_total / 1e9:.1f}B | "
        f"collateral gross value ≈ {coll_value_total / 1e9:.1f}B | "
        f"guarantee amount ≈ {guar_amount_total / 1e9:.1f}B"
    )
    print("hint: pass as-of T=-1 (no temporal filter) to crm_alloc/crm_audit")


if __name__ == "__main__":
    main()
