//! Unified streaming event envelope.
//!
//! Every source (Kafka, Pulsar, CDC, file replay) is normalised into an
//! [`Envelope`] carrying the source position, a stable `event_id` (the dedup
//! key), event / ingest time, and the raw payload. [`encode_envelopes`] turns a
//! batch of envelopes into the Arrow batch the catalog persists.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{
    ArrayRef, BinaryArray, Int32Array, Int64Array, RecordBatch, TimestampNanosecondArray,
    UInt32Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bytes::Bytes;
use serde::Deserialize;

use crate::error::{IngestError, Result};
use crate::source::PartitionOffset;

/// Wall-clock nanoseconds since the Unix epoch (0 before the epoch).
pub fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// A single event from a streaming source, normalised across adapters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope {
    pub source: String,
    pub partition: i32,
    pub offset: i64,
    /// Stable 16-byte dedup key (uuid / hash).
    pub event_id: [u8; 16],
    /// Business / event time in ns.
    pub event_time: i64,
    /// Wall-clock time the event entered the pipeline, in ns.
    pub ingest_time: i64,
    /// Schema version of `payload`.
    pub schema_version: u32,
    /// Raw bytes (Arrow IPC / JSON / Avro).
    pub payload: Bytes,
}

impl Envelope {
    /// Build an envelope, stamping `ingest_time` with [`now_ns`].
    pub fn new(
        source: impl Into<String>,
        partition: i32,
        offset: i64,
        event_id: [u8; 16],
        event_time: i64,
        payload: impl Into<Bytes>,
    ) -> Self {
        Self {
            source: source.into(),
            partition,
            offset,
            event_id,
            event_time,
            ingest_time: now_ns(),
            schema_version: 1,
            payload: payload.into(),
        }
    }

    /// Explicit `ingest_time` (deterministic replay / tests).
    #[allow(clippy::too_many_arguments)]
    pub fn with_ingest_time(
        source: impl Into<String>,
        partition: i32,
        offset: i64,
        event_id: [u8; 16],
        event_time: i64,
        ingest_time: i64,
        schema_version: u32,
        payload: impl Into<Bytes>,
    ) -> Self {
        Self {
            source: source.into(),
            partition,
            offset,
            event_id,
            event_time,
            ingest_time,
            schema_version,
            payload: payload.into(),
        }
    }

    /// Deterministic id from an arbitrary key (`blake3`, first 16 bytes).
    pub fn id_from_key(key: &str) -> [u8; 16] {
        let hash = blake3::hash(key.as_bytes());
        let mut id = [0u8; 16];
        id.copy_from_slice(&hash.as_bytes()[..16]);
        id
    }

    /// Deterministic id from a source position (fallback when a source has no
    /// native event id). Kafka partition offsets are stable across replays.
    pub fn id_from_offset(source: &str, partition: i32, offset: i64) -> [u8; 16] {
        Self::id_from_key(&format!("{source}:{partition}:{offset}"))
    }

    pub fn partition_offset(&self) -> PartitionOffset {
        PartitionOffset {
            source: self.source.clone(),
            partition: self.partition,
            offset: self.offset,
        }
    }
}

/// Arrow schema of an encoded envelope batch.
pub fn envelope_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("source", DataType::Utf8, false),
        Field::new("partition", DataType::Int32, false),
        Field::new("offset", DataType::Int64, false),
        Field::new("event_id", DataType::Binary, false),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "ingest_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("schema_version", DataType::UInt32, false),
        Field::new("payload", DataType::Binary, false),
    ]))
}

/// Encode envelopes into one Arrow [`RecordBatch`] (envelope schema).
pub fn encode_envelopes(envs: &[Envelope]) -> Result<RecordBatch> {
    let schema = envelope_schema();
    let cols: Vec<ArrayRef> = vec![
        Arc::new(arrow::array::StringArray::from(
            envs.iter().map(|e| e.source.as_str()).collect::<Vec<_>>(),
        )),
        Arc::new(Int32Array::from(
            envs.iter().map(|e| e.partition).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            envs.iter().map(|e| e.offset).collect::<Vec<_>>(),
        )),
        Arc::new(BinaryArray::from_iter_values(
            envs.iter().map(|e| e.event_id),
        )),
        Arc::new(TimestampNanosecondArray::from(
            envs.iter().map(|e| e.event_time).collect::<Vec<_>>(),
        )),
        Arc::new(TimestampNanosecondArray::from(
            envs.iter().map(|e| e.ingest_time).collect::<Vec<_>>(),
        )),
        Arc::new(UInt32Array::from(
            envs.iter().map(|e| e.schema_version).collect::<Vec<_>>(),
        )),
        Arc::new(BinaryArray::from_iter_values(
            envs.iter().map(|e| e.payload.as_ref()),
        )),
    ];
    Ok(RecordBatch::try_new(schema, cols)?)
}

/// The JSON line shape accepted by [`crate::file::FileReplayAdapter`] and the
/// protocol-agnostic decoder shared with future Kafka / CDC adapters.
#[derive(Debug, Clone, Deserialize)]
pub struct JsonEnvelope {
    pub event_time: i64,
    #[serde(default)]
    pub payload: String,
    /// Optional explicit id (hex). Derived from the source position when absent.
    #[serde(default)]
    pub event_id: Option<String>,
    /// Optional business / event-time schema version.
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
}

fn default_schema_version() -> u32 {
    1
}

/// Decode one JSON event body into an [`Envelope`]. Used by the file replay
/// adapter and (without changes) by a Kafka/CDC adapter that receives the same
/// JSON payload.
pub fn decode_json_envelope(
    source: &str,
    partition: i32,
    offset: i64,
    bytes: &[u8],
) -> Result<Envelope> {
    let rec: JsonEnvelope = serde_json::from_slice(bytes)?;
    let event_id = match rec.event_id.as_deref() {
        Some(hex) => parse_event_id(hex)?,
        None => Envelope::id_from_offset(source, partition, offset),
    };
    Ok(Envelope::with_ingest_time(
        source,
        partition,
        offset,
        event_id,
        rec.event_time,
        now_ns(),
        rec.schema_version,
        rec.payload,
    ))
}

/// Parse a 32-char hex event id.
pub fn parse_event_id(hex: &str) -> Result<[u8; 16]> {
    let hex = hex.trim();
    if hex.len() != 32 {
        return Err(IngestError::Msg(format!(
            "event_id must be 32 hex chars, got `{hex}`"
        )));
    }
    let mut id = [0u8; 16];
    for (i, byte) in id.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
            .map_err(|_| IngestError::Msg(format!("invalid hex event_id `{hex}`")))?;
    }
    Ok(id)
}

/// Lower-case hex encoding of an event id (for JSON / offset logs).
pub fn event_id_hex(id: &[u8; 16]) -> String {
    let mut out = String::with_capacity(32);
    for b in id {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn id_is_deterministic_and_keyed() {
        assert_eq!(Envelope::id_from_key("a"), Envelope::id_from_key("a"));
        assert_ne!(Envelope::id_from_key("a"), Envelope::id_from_key("b"));
        assert_eq!(
            Envelope::id_from_offset("s", 0, 7),
            Envelope::id_from_offset("s", 0, 7)
        );
        assert_ne!(
            Envelope::id_from_offset("s", 0, 7),
            Envelope::id_from_offset("s", 0, 8)
        );
    }

    #[test]
    fn encode_round_trips_shape() {
        let envs = vec![
            Envelope::with_ingest_time("s", 0, 0, Envelope::id_from_key("0"), 100, 200, 1, "x"),
            Envelope::with_ingest_time("s", 0, 1, Envelope::id_from_key("1"), 101, 201, 2, "yy"),
        ];
        let batch = encode_envelopes(&envs).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.schema(), envelope_schema());
    }

    #[test]
    fn json_decode_derives_id() {
        let bytes = br#"{"event_time": 42, "payload": "hi"}"#;
        let e = decode_json_envelope("feed", 1, 5, bytes).unwrap();
        assert_eq!(e.event_time, 42);
        assert_eq!(e.payload.as_ref(), b"hi");
        assert_eq!(e.event_id, Envelope::id_from_offset("feed", 1, 5));
    }

    #[test]
    fn hex_id_round_trip() {
        let id = Envelope::id_from_key("z");
        assert_eq!(parse_event_id(&event_id_hex(&id)).unwrap(), id);
        assert!(parse_event_id("nope").is_err());
    }
}
