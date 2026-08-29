//! End-to-end smoke test: spin up a `gtv-server` on an ephemeral port and run a
//! query through the gRPC client (`gtv_proto::query_remote`).

use std::net::SocketAddr;

use arrow::array::{Int64Array, UInt64Array};

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
async fn query_numbers_over_grpc() {
    let (addr, handle) = spawn_server();
    // Give the server a moment to bind.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let batches = gtv_proto::query_remote(&addr, "SELECT n, square FROM numbers ORDER BY n")
        .await
        .expect("remote query");

    assert_eq!(batches.len(), 1, "single batch expected");
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 10);
    assert_eq!(batch.num_columns(), 2);

    let n = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("n is Int64");
    assert_eq!(n.value(0), 1);
    assert_eq!(n.value(9), 10);

    handle.abort();
}

#[tokio::test]
async fn remote_error_is_surfaced() {
    let (addr, handle) = spawn_server();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let err = gtv_proto::query_remote(&addr, "SELECT * FROM does_not_exist")
        .await
        .expect_err("bad query should error");

    let msg = format!("{err:#}");
    assert!(msg.contains("remote error"), "unexpected message: {msg}");

    handle.abort();
}

#[tokio::test]
async fn knn_songs_matches_oracle_over_grpc() {
    let (addr, handle) = spawn_server();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // TC-11: metadata-filtered K-NN over song embeddings.
    let batches = gtv_proto::query_remote(
        &addr,
        "SELECT id FROM knn('songs', '0.1,0.1', 3)",
    )
    .await
    .expect("knn songs");
    let ids = ids_of(&batches);
    assert_eq!(ids, vec![8, 0, 1]);

    let batches = gtv_proto::query_remote(
        &addr,
        "SELECT id FROM knn('songs', '5.0,5.0', 3, 'rock')",
    )
    .await
    .expect("knn songs rock");
    let ids = ids_of(&batches);
    assert_eq!(ids, vec![2, 9, 3]);

    handle.abort();
}

#[tokio::test]
async fn knn_tss_matches_oracle_over_grpc() {
    let (addr, handle) = spawn_server();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // TC-07: temporal similarity search (6-dim windows).
    let batches = gtv_proto::query_remote(
        &addr,
        "SELECT id FROM knn('tss', '1.1,2.0,3.1,4.0,5.1,6.0', 4)",
    )
    .await
    .expect("knn tss");
    let ids = ids_of(&batches);
    assert_eq!(ids, vec![0, 2, 1, 3]);

    handle.abort();
}

/// Extract the single `id` (UInt64) column from a query result as a `Vec<u64>`.
fn ids_of(batches: &[arrow::array::RecordBatch]) -> Vec<u64> {
    batches
        .iter()
        .flat_map(|b| {
            let arr = b
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("id is UInt64");
            (0..arr.len()).map(|i| arr.value(i)).collect::<Vec<u64>>()
        })
        .collect()
}
