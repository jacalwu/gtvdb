//! `gtv-server` — gRPC query dispatch endpoint for the gtv engine.
//!
//! Serves a DataFusion-backed `QueryService`. A client sends SQL; the server
//! executes it against a local [`GtvContext`] seeded with the demo tables and
//! returns the result as an Arrow IPC stream (the Flight wire format) in
//! `QueryResponse.arrow_ipc`.
//!
//! The binary binds `0.0.0.0:50051` (configurable via `GTV_ADDR`); [`serve`]
//! exposes the same server for embedding and tests.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, UInt64Array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use gtv_core::{EdgeTable, NodeTable, TemporalGraph};
use gtv_engine::GtvContext;
use gtv_proto::gtvquery::query_service_server::{QueryService, QueryServiceServer};
use gtv_proto::gtvquery::{QueryRequest, QueryResponse};
use gtv_proto::encode_batches;

pub const DEFAULT_ADDR: &str = "0.0.0.0:50051";

/// The server-side query service: runs SQL against the shared `GtvContext`.
#[derive(Clone)]
pub struct GtvQueryService {
    ctx: Arc<GtvContext>,
}

#[tonic::async_trait]
impl QueryService for GtvQueryService {
    async fn execute(
        &self,
        request: tonic::Request<QueryRequest>,
    ) -> std::result::Result<tonic::Response<QueryResponse>, tonic::Status> {
        let sql = request.into_inner().sql;
        match self.ctx.sql(&sql).await {
            Ok(batches) => match encode_batches(&batches) {
                Ok(arrow_ipc) => Ok(tonic::Response::new(QueryResponse {
                    arrow_ipc,
                    error: String::new(),
                })),
                Err(e) => Ok(tonic::Response::new(QueryResponse {
                    arrow_ipc: Vec::new(),
                    error: format!("encode: {e}"),
                })),
            },
            Err(e) => Ok(tonic::Response::new(QueryResponse {
                arrow_ipc: Vec::new(),
                error: format!("query: {e}"),
            })),
        }
    }
}

/// A ready-to-serve query service backed by the demo tables.
pub fn service() -> Result<GtvQueryService> {
    Ok(GtvQueryService {
        ctx: Arc::new(build_ctx()?),
    })
}

/// Serve the gtv `QueryService` on `addr` until the process is shut down.
pub async fn serve(addr: SocketAddr) -> Result<()> {
    let svc = service()?;
    println!("gtv-server listening on {addr}");
    tonic::transport::Server::builder()
        .add_service(QueryServiceServer::new(svc))
        .serve(addr)
        .await?;
    Ok(())
}

/// Build a `GtvContext` seeded with the demo tables (`nodes`, `edges`, `prices`
/// and `numbers`), plus the `neighbors` / `asof_join` table functions.
pub fn build_ctx() -> Result<GtvContext> {
    let ctx = GtvContext::new();

    let nodes = nodes_table()?;
    let graph = TemporalGraph::new(nodes, edges_table()?)?;
    ctx.register_batches(
        "nodes",
        graph.nodes().batch().schema(),
        vec![graph.nodes().batch().clone()],
    )?;
    let (edge_schema, edges_batch) = edges_int64_batch(graph.edges())?;
    ctx.register_batches("edges", edge_schema, vec![edges_batch])?;

    let times = vec![0i64, 10, 20, 30, 40, 50];
    let prices = vec![100.0, 101.0, 99.0, 102.0, 103.0, 104.0];
    let (price_schema, price_batch) = prices_batch(&times, &prices)?;
    ctx.register_batches("prices", price_schema, vec![price_batch])?;

    let (number_schema, number_batch) = numbers_batch()?;
    ctx.register_batches("numbers", number_schema, vec![number_batch])?;

    ctx.register_neighbors(graph.csr());
    ctx.register_asof_join(times, prices);
    Ok(ctx)
}

fn nodes_table() -> Result<NodeTable> {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("value", DataType::Float64, false),
        ])),
        vec![
            Arc::new(UInt64Array::from(vec![0u64, 1, 2, 3, 4, 5])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0])) as ArrayRef,
        ],
    )?;
    Ok(NodeTable::new(batch)?)
}

fn edges_table() -> Result<EdgeTable> {
    Ok(EdgeTable::from_vecs(
        vec![0, 0, 1, 1, 2, 3],
        vec![1, 2, 3, 4, 5, 5],
        vec![1u16, 1, 2, 2, 1, 3],
        vec![0, 50, 0, 100, 0, 150],
        vec![100, 200, 100, 300, 300, 400],
    )?)
}

/// Edge table with temporal columns as `Int64` nanoseconds (ergonomic for SQL).
fn edges_int64_batch(edges: &EdgeTable) -> Result<(SchemaRef, RecordBatch)> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("src", DataType::UInt64, false),
        Field::new("dst", DataType::UInt64, false),
        Field::new("edge_type", DataType::UInt16, false),
        Field::new("valid_from", DataType::Int64, false),
        Field::new("valid_to", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(edges.src().clone()) as ArrayRef,
            Arc::new(edges.dst().clone()) as ArrayRef,
            Arc::new(edges.edge_type().clone()) as ArrayRef,
            cast(edges.valid_from(), &DataType::Int64)?,
            cast(edges.valid_to(), &DataType::Int64)?,
        ],
    )?;
    Ok((schema, batch))
}

/// Price series as a two-column `(t, price)` batch.
fn prices_batch(times: &[i64], prices: &[f64]) -> Result<(SchemaRef, RecordBatch)> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("t", DataType::Int64, false),
        Field::new("price", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(times.to_vec())) as ArrayRef,
            Arc::new(Float64Array::from(prices.to_vec())) as ArrayRef,
        ],
    )?;
    Ok((schema, batch))
}

/// A simple `(n, square)` table for connectivity smoke tests.
fn numbers_batch() -> Result<(SchemaRef, RecordBatch)> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("n", DataType::Int64, false),
        Field::new("square", DataType::Float64, false),
    ]));
    let n: Vec<i64> = (1..=10).collect();
    let square: Vec<f64> = n.iter().map(|x| (*x as f64).powi(2)).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(n)) as ArrayRef,
            Arc::new(Float64Array::from(square)) as ArrayRef,
        ],
    )?;
    Ok((schema, batch))
}
