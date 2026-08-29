# gtvdb — kdb+ / KDB.AI feature-parity test suite

Acceptance tests that verify **gtvdb P5** (distributed query dispatch via gRPC +
Arrow Flight) reproduces the time-series / vector capabilities that kdb+ and
KDB.AI offer. Use cases are drawn from
[`KxSystems/kdbai-samples`](https://github.com/KxSystems/kdbai-samples).

## Contents

```
testcase/
├── README.md                      # this file
├── TEST_PLAN.md                   # full test-case matrix + expected results
├── run_tests.sh                   # runs REPL tests against the gtv binary
├── data/
│   ├── gen_data.py                # deterministic CSV generator + L2 oracle
│   └── generated/                 # (output) nodes/edges/prices/... CSV
├── scripts/                       # one .gtv REPL script per test case
│   ├── tc01_asof.gtv
│   ├── tc02_rolling.gtv
│   ├── tc03_temporal_slice.gtv
│   ├── tc04_graph_traversal.gtv
│   ├── tc05_pattern.gtv
│   ├── tc06_knn.gtv
│   ├── tc07_tss.gtv
│   ├── tc08_tt_save_load.gtv
│   └── tc11_songs.gtv
└── expected/                      # golden outputs (banner stripped)
    ├── tc01_asof.txt
    ├── tc02_rolling.txt
    ├── tc03_temporal_slice.txt
    ├── tc04_graph_traversal.txt
    ├── tc05_pattern.txt
    ├── tc06_knn.txt
    ├── tc07_tss.txt
    ├── tc07_tss_reference.txt     # oracle for temporal similarity search
    ├── tc08_tt_save_load.txt
    ├── tc11_songs.txt
    └── tc_meta_knn_reference.txt  # oracle for metadata-filtered K-NN
```

## Quick start

```sh
# build the single-node REPL binary
cargo build -p gtv-cli --bin gtv

# run all golden tests against the local binary
./testcase/run_tests.sh
```

Each script in `scripts/` is piped into the `gtv` REPL; the runner strips the
one-line startup banner and diffs stdout against the matching file in `expected/`.

## Data generator & oracles

```sh
python3 testcase/data/gen_data.py --out-dir testcase/data/generated
```

This regenerates the canonical CSV datasets (identical to the demo data shipped
in `gtv-cli`) and prints the L2 K-NN reference rankings used as oracles for the
vector test cases (TC-07, TC-11).

## P5 (distributed) hook

The P5 gRPC contract already exists in `crates/gtv-proto/proto/gtvquery.proto`
(`QueryService.Execute` returning Arrow IPC bytes). TC-09 and TC-10 (and the SQL
variants of TC-01..04, TC-08) target that service once the server binary exists.
Point `run_tests.sh` at the P5 client by setting `GTV_P5_CLIENT`; the runner then
replays the same scripts through the distributed endpoint and diffs against the
same golden files (see `TEST_PLAN.md` §5).

`gtv_proto::query_remote(addr, sql)` is a ready-made client helper for replaying
the SQL test cases over the network.
