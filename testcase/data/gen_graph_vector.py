#!/usr/bin/env python3
"""Regenerate the FIXED graph & vector benchmark datasets.

One-time generators (not run by the benchmark script). Outputs are committed so
every machine tests against identical files:

    edges_1m.csv     — 1M temporal transfer edges (chain spine + planted A→B→C→A
                       wash-trading cycles), for TC3 `wash_from`.
    vectors_1m.csv   — 1M × 8-dim random embeddings, for TC4 `knn_from`.

Usage:
    python3 testcase/data/gen_graph_vector.py [out_dir]
"""
import random
import os
import sys


def gen_edges(path: str, chain_edges: int = 970_000, cycles: int = 10_000) -> None:
    BIG = 1_000_000_000  # every edge is active at the query time T=1_000_000
    with open(path, "w") as f:
        f.write("src,dst,edge_type,valid_from,valid_to\n")
        # Chain spine i -> i+1: no back edges, so no accidental 3-cycles.
        for i in range(chain_edges):
            f.write(f"{i},{i+1},1,{i},{i+BIG}\n")
        # Planted A -> B -> C -> A cycles with strictly increasing event times.
        base = chain_edges + 1
        for k in range(cycles):
            a = base + k * 3
            b = a + 1
            c = a + 2
            f.write(f"{a},{b},1,1000,{1000+BIG}\n")
            f.write(f"{b},{c},1,2000,{2000+BIG}\n")
            f.write(f"{c},{a},1,3000,{3000+BIG}\n")


def gen_vectors(path: str, n: int = 1_000_000, dim: int = 8, seed: int = 20240904) -> None:
    rng = random.Random(seed)
    with open(path, "w") as f:
        f.write("id," + ",".join(f"v{d}" for d in range(dim)) + "\n")
        for i in range(n):
            vals = ",".join(f"{rng.random():.6f}" for _ in range(dim))
            f.write(f"{i},{vals}\n")


def main() -> None:
    out_dir = sys.argv[1] if len(sys.argv) > 1 else "testcase/data"
    os.makedirs(out_dir, exist_ok=True)
    gen_edges(os.path.join(out_dir, "edges_1m.csv"))
    gen_vectors(os.path.join(out_dir, "vectors_1m.csv"))
    print(f"wrote {out_dir}/edges_1m.csv and {out_dir}/vectors_1m.csv")


if __name__ == "__main__":
    main()
