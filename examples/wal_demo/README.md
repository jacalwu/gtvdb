# wal_demo

This is a minimal demo project showing a simple WAL + DeltaBuffer workflow used by the storage design docs.

Build & run

From the repository root you can build the workspace or the example directly:

- Build the whole workspace:

  cargo build --release

- Run the demo:

  cargo run -p wal_demo --release

Behavior & data

- The demo writes files to `data/partition=demo/` under the repository root.
- It demonstrates WAL append, WAL replay on startup, applying records to an in-memory DeltaBuffer, and flushing a simple block file plus updating `meta.json`.

Notes

- This is a simplified educational demo: it omits production features like robust segment management, tombstones, concurrent writer safety, and advanced crash-safety guarantees.
