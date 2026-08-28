# gtvdb

Temporal-Columnar Graph-Vector Engine — a single-engine database combining
kdb+-style array computation, temporal graph traversal, vector search, and
columnar OLAP scans.

See [`init.md`](./init.md) for the architecture spec and [`ROADMAP.md`](./ROADMAP.md)
for the phased plan.

## Build & test

```sh
cargo build --workspace
cargo test --workspace
```

## Run the CLI

```sh
cargo run -p gtv-cli --bin gtv
```

Phase 1 CLI commands: `help`, `tables`, `neighbors`, `khop`, `mavg`, `msum`,
`deltas`, `asof`, `quit`.
