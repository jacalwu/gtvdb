#!/usr/bin/env python3
"""Deterministic dataset generator + reference oracle for gtvdb P5 test cases.

The CSV files mirror the exact demo data hardcoded in `gtv-cli` so that a P5
server can load the same canonical dataset the single-node REPL ships with.

It also prints the *reference* K-NN rankings for the two vector test cases
(TSS and metadata filtering), computed with plain L2 — these are the oracles
the P5 vector-search path must reproduce.

Usage:
    python3 gen_data.py [--out-dir DIR]
"""
from __future__ import annotations

import argparse
import csv
import os
from dataclasses import dataclass
from typing import List, Tuple


def sq_l2(a: List[float], b: List[float]) -> float:
    return sum((x - y) ** 2 for x, y in zip(a, b))


def knn_oracle(query: List[float], vectors: List[Tuple[int, List[float]]],
               k: int, allowed: List[int] | None = None) -> List[int]:
    allowed = set(allowed) if allowed is not None else None
    scored = [
        (sq_l2(query, v), i)
        for i, v in vectors
        if allowed is None or i in allowed
    ]
    scored.sort(key=lambda t: (t[0], t[1]))
    return [i for _, i in scored[:k]]


# ---------------------------------------------------------------------------
# Canonical demo data (identical to gtv-cli/src/main.rs)
# ---------------------------------------------------------------------------
NODES = [(0, 1.0), (1, 2.0), (2, 3.0), (3, 4.0), (4, 5.0), (5, 6.0)]
EDGES = [
    (0, 1, 1, 0, 100),
    (0, 2, 1, 50, 200),
    (1, 3, 2, 0, 100),
    (1, 4, 2, 100, 300),
    (2, 5, 1, 0, 300),
    (3, 5, 3, 150, 400),
]
PRICES = [(0, 100.0), (10, 101.0), (20, 99.0), (30, 102.0), (40, 103.0), (50, 104.0)]

# Transfer graph used by `pattern` (gtv-cli build_transfers): 4 nodes.
TRANSFER_NODES = [(0, 1.0), (1, 2.0), (2, 3.0), (3, 4.0)]
TRANSFER_EDGES = [
    (0, 1, 1, 10, 1000),
    (1, 2, 1, 20, 1000),
    (2, 3, 1, 30, 1000),
    (3, 0, 1, 40, 1000),
    (0, 2, 1, 15, 1000),
    (1, 3, 1, 25, 1000),
]

# Music-recommendation / metadata-filtering sample (10 songs, 2-dim embedding).
SONGS = [
    (0, "pop",       [0.0, 0.0]),
    (1, "pop",       [0.5, 0.5]),
    (2, "rock",      [5.0, 5.0]),
    (3, "rock",      [5.2, 5.1]),
    (4, "jazz",      [1.0, 1.0]),
    (5, "jazz",      [1.1, 1.0]),
    (6, "classical", [9.0, 9.0]),
    (7, "classical", [9.1, 9.0]),
    (8, "pop",       [0.2, 0.1]),
    (9, "rock",      [4.9, 5.0]),
]

# Temporal-similarity-search sample (4 reference windows + 1 query, 6 dims).
TSS_SERIES = [
    (0, "uptrend",   [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]),
    (1, "dip",       [5.0, 4.0, 2.0, 2.0, 4.0, 5.0]),
    (2, "flat",      [3.0, 3.0, 3.0, 3.0, 3.0, 3.0]),
    (3, "downtrend", [6.0, 5.0, 4.0, 3.0, 2.0, 1.0]),
]
TSS_QUERY = [1.1, 2.0, 3.1, 4.0, 5.1, 6.0]


def write_csv(path: str, header: List[str], rows: List[tuple]) -> None:
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def generate(out_dir: str) -> None:
    os.makedirs(out_dir, exist_ok=True)
    write_csv(os.path.join(out_dir, "nodes.csv"),
              ["id", "value"], NODES)
    write_csv(os.path.join(out_dir, "edges.csv"),
              ["src", "dst", "edge_type", "valid_from", "valid_to"], EDGES)
    write_csv(os.path.join(out_dir, "prices.csv"), ["t", "price"], PRICES)
    write_csv(os.path.join(out_dir, "transfers_nodes.csv"),
              ["id", "value"], TRANSFER_NODES)
    write_csv(os.path.join(out_dir, "transfers_edges.csv"),
              ["src", "dst", "edge_type", "valid_from", "valid_to"], TRANSFER_EDGES)
    write_csv(os.path.join(out_dir, "songs.csv"),
              ["song_id", "genre", "x", "y"],
              [(i, g, *v) for i, g, v in SONGS])
    write_csv(os.path.join(out_dir, "tss_series.csv"),
              ["series_id", "label", "v0", "v1", "v2", "v3", "v4", "v5"],
              [(i, l, *v) for i, l, v in TSS_SERIES]
              + [("query", "noisy_uptrend", *TSS_QUERY)])
    print(f"wrote CSVs to {out_dir}")


def print_oracle() -> None:
    print("\n== Reference oracle (L2, tie-break by id) ==")
    songs = [(i, v) for i, _, v in SONGS]
    print("songs  knn(q=[0.1,0.1], k=3, no filter)   =",
          knn_oracle([0.1, 0.1], songs, 3))
    pop = [i for i, g, _ in SONGS if g == "pop"]
    rock = [i for i, g, _ in SONGS if g == "rock"]
    print("songs  knn(q=[0.1,0.1], k=3, mask=pop)    =",
          knn_oracle([0.1, 0.1], songs, 3, pop))
    print("songs  knn(q=[5.0,5.0], k=3, mask=rock)   =",
          knn_oracle([5.0, 5.0], songs, 3, rock))
    series = [(i, v) for i, _, v in TSS_SERIES]
    print("tss    knn(q=noisy_uptrend, k=4, no mask) =",
          knn_oracle(TSS_QUERY, series, 4))
    print("tss    knn(q=noisy_uptrend, k=1, no mask) =",
          knn_oracle(TSS_QUERY, series, 1))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default=os.path.join(os.path.dirname(__file__), "generated"))
    args = ap.parse_args()
    generate(args.out_dir)
    print_oracle()


if __name__ == "__main__":
    main()
