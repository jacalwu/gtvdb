//! TC-09 / TC-10 — distributed query dispatch parity and Arrow IPC transport
//! integrity, exercised end-to-end over the P5 gRPC surface.
//!
//! - TC-09 replays the SQL variants of TC-01..04 (+ vector K-NN) through
//!   `QueryService.Execute` and asserts byte-identical parity with the same
//!   query run against a local `GtvContext`.
//! - TC-10 transports a ≥1M-row result over the Arrow IPC (Flight wire format)
//!   payload and verifies row count + column checksum survive losslessly.

use std::net::SocketAddr;

use arrow::array::Int64Array;

/// Pick a free loopback port by asking the OS for one.
fn free_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind free port");
    listener.local_addr().expect("local addr")
}

/// Start a server on an ephemeral port and return its address plus a join handle.
fn spawn_server() -> (String, tokio::task::JoinHandle<()>) {
    let addr = free_addr();
    let handle = tokio::spawn(async move {
        let _ = gtv_server::serve(addr).await;
    });
    (addr.to_string(), handle)
}

#[tokio::test]
async fn tc09_distributed_sql_parity() {
    let (addr, handle) = spawn_server();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let local = gtv_server::build_ctx().expect("build local ctx");

    // SQL variants of the single-node golden cases (TC-01..04 + vector K-NN).
    let sqls = [
        // TC-01 as-of join
        "SELECT * FROM asof_join(0, 5, 15, 25, 35, 45, 55, 60)",
        // TC-02 rolling window aggregates
        "SELECT t, mavg(price, 3) OVER (ORDER BY t) FROM prices",
        "SELECT t, msum(price, 2) OVER (ORDER BY t), deltas(price) OVER (ORDER BY t) FROM prices",
        // TC-03 temporal slice
        "SELECT src, dst FROM edges WHERE valid_from <= 150 AND 150 < valid_to",
        "SELECT src, dst, edge_type FROM edges WHERE valid_from <= 0 AND 0 < valid_to",
        "SELECT * FROM edges WHERE valid_from <= 100 AND 100 < valid_to",
        // TC-04 graph traversal
        "SELECT * FROM neighbors(0, 100)",
        // TC-08 source table (load target parity)
        "SELECT * FROM prices ORDER BY t",
        // vector K-NN
        "SELECT id FROM knn('songs', '0.1,0.1', 3)",
        "SELECT id FROM knn('tss', '1.1,2.0,3.1,4.0,5.1,6.0', 4)",
    ];

    for sql in sqls {
        let expected = local.sql(sql).await.expect("local sql");
        let got = gtv_proto::query_remote(&addr, sql)
            .await
            .expect("remote sql");
        assert_eq!(expected, got, "distributed parity mismatch for: {sql}");
    }

    handle.abort();
}

#[tokio::test]
async fn tc10_arrow_ipc_transport_integrity() {
    let (addr, handle) = spawn_server();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // 10^6 rows via a six-way self cross join of the 10-row `numbers` table.
    let sql = "SELECT a.n AS x, b.n AS y \
               FROM numbers a CROSS JOIN numbers b CROSS JOIN numbers c \
               CROSS JOIN numbers d CROSS JOIN numbers e CROSS JOIN numbers f";
    let batches = gtv_proto::query_remote(&addr, sql)
        .await
        .expect("remote cross join");

    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 1_000_000, "1M rows must survive IPC transport");

    assert!(!batches.is_empty());
    assert_eq!(batches[0].num_columns(), 2, "two projected columns");

    // Column checksum over the transported batches: SUM(x) over `numbers`
    // (1..=10, each repeated 10^5 times) = 55 * 100_000 = 5_500_000.
    let sum_x: i64 = batches
        .iter()
        .flat_map(|b| {
            let arr = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("x is Int64");
            (0..arr.len()).map(|i| arr.value(i)).collect::<Vec<i64>>()
        })
        .sum();
    assert_eq!(sum_x, 5_500_000, "column checksum must be lossless");

    handle.abort();
}
