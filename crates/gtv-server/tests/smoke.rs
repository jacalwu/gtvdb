//! End-to-end smoke test: spin up a `gtv-server` on an ephemeral port and run a
//! query through the gRPC client (`gtv_proto::query_remote`).

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
