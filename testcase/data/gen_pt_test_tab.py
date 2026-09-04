#!/usr/bin/env python3
"""Regenerate the FIXED standard benchmark dataset `pt_test_tab.csv`.

This is a one-time generator, not run by the benchmark script. The output is a
committed file so every machine tests against the identical 1M-row table.

Schema (loaded into the REPL table `pt_test_tab`):
    t          Int64    tick timestamp (ns), ascending
    bid        Float64
    ask        Float64
    bid_sz     Float64  (written with a ".0" so CSV inference keeps it f64)
    ask_sz     Float64
    valid_from Int64    point-in-time half-open interval start
    valid_to   Int64    point-in-time half-open interval end (exclusive)

Usage:
    python3 testcase/data/gen_pt_test_tab.py [rows] [out.csv]
"""
import random
import sys


def main() -> None:
    rows = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000
    out = sys.argv[2] if len(sys.argv) > 2 else "testcase/data/pt_test_tab.csv"
    rng = random.Random(20240904)  # pinned seed -> identical bytes every run
    mid = 100.0
    with open(out, "w") as f:
        f.write("t,bid,ask,bid_sz,ask_sz,valid_from,valid_to\n")
        for i in range(rows):
            mid += (rng.random() - 0.5) * 0.2
            spread = 0.01 + rng.random() * 0.02
            f.write(
                f"{i*1000},{mid-spread/2:.6f},{mid+spread/2:.6f},"
                f"{1+rng.randrange(10000):.1f},{1+rng.randrange(10000):.1f},"
                f"{i},{i+100}\n"
            )


if __name__ == "__main__":
    main()
