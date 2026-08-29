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

## P5 (distributed) tests — TC-09 / TC-10

The single-node golden tests (TC-01..08, TC-11) run via `./run_tests.sh`. The two
distributed cases are Rust integration tests that spin up a `gtv-server` on an
ephemeral port and drive it through `gtv_proto::query_remote`:

```sh
cargo test -p gtv-server --test p5_distributed
```

- `tc09_distributed_sql_parity` — replays the SQL variants of TC-01..04 (+ vector
  K-NN) over gRPC and asserts `RecordBatch` parity with a local `GtvContext`.
- `tc10_arrow_ipc_transport_integrity` — transports a 1,000,000-row result over
  the Arrow IPC payload and verifies row count + column checksum survive
  losslessly.

The gRPC contract lives in `crates/gtv-proto/proto/gtvquery.proto`
(`QueryService.Execute` returning Arrow IPC bytes); `gtv_proto::query_remote` is
the client helper. Both client and server lift tonic's default 4 MiB message cap
so large result sets can be shipped as a single payload.
