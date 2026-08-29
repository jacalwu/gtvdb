//! gtv-proto: gRPC query dispatch + Arrow IPC (Flight wire format) transport.
//!
//! Defines the [`gtvquery`] service and a tiny client/server helper that
//! serializes result [`RecordBatch`]es to/from an Arrow IPC stream, so query
//! results can cross a node boundary as bytes.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;

pub mod gtvquery {
    tonic::include_proto!("gtvquery");
}

pub use gtvquery::{QueryRequest, QueryResponse};

/// Encode a sequence of batches into a single Arrow IPC stream.
pub fn encode_batches(batches: &[RecordBatch]) -> arrow::error::Result<Vec<u8>> {
    let schema = batches
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| Arc::new(Schema::empty()));
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema)?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.finish()?;
    }
    Ok(buf)
}

/// Decode an Arrow IPC stream back into batches.
pub fn decode_batches(data: &[u8]) -> arrow::error::Result<Vec<RecordBatch>> {
    if data.is_empty() {
        return Ok(Vec::new());
    }
    let cursor = std::io::Cursor::new(data);
    let reader = StreamReader::try_new(cursor, None)?;
    reader.collect()
}

/// Execute `sql` on a remote gtv-server at `addr` (e.g. `127.0.0.1:50051`).
pub async fn query_remote(addr: &str, sql: &str) -> anyhow::Result<Vec<RecordBatch>> {
    let client = gtvquery::query_service_client::QueryServiceClient::connect(format!(
        "http://{addr}"
    ))
    .await?;
    // Lift tonic's default 4 MiB message cap so large result sets (TC-10) can
    // be transported as a single Arrow IPC payload.
    let mut client = client
        .max_decoding_message_size(usize::MAX)
        .max_encoding_message_size(usize::MAX);
    let response = client
        .execute(QueryRequest {
            sql: sql.to_string(),
        })
        .await?
        .into_inner();
    if !response.error.is_empty() {
        anyhow::bail!("remote error: {}", response.error);
    }
    Ok(decode_batches(&response.arrow_ipc)?)
}
