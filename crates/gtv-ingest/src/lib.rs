//! gtv-ingest — streaming ingestion contract (prod_p3 B3-1).
//!
//! A source-agnostic, at-least-once **micro-batch** ingestion layer on top of
//! the B2-1 atomic catalog commit:
//!
//! ```text
//! poll ──▶ dedup(event_id) ──▶ watermark / late policy ──▶ encode Arrow
//!      ──▶ Sink (atomic catalog commit, offsets in snapshot.summary)
//!      ──▶ commit offsets (offset store + source) ──▶ persist dedup
//! ```
//!
//! * [`Envelope`] — the unified event type (source position, stable
//!   `event_id`, event / ingest time, payload);
//! * [`SourceAdapter`] — the read/commit contract (Kafka / Pulsar / CDC would
//!   implement this; the always-on [`FileReplayAdapter`] covers tests, backfills
//!   and disaster replay);
//! * [`OffsetStore`] — durable committed offsets (append-only JSONL);
//! * [`DedupStore`] — bounded `event_id` window making replays idempotent;
//! * [`Watermark`] / [`LatePolicy`] — event-time completeness + late handling;
//! * [`DeadLetterQueue`] — auditable rejects with reprocessing;
//! * [`Pipeline`] — the micro-batch executor with backpressure + metrics;
//! * [`CatalogSink`] — atomic publish (data + offsets in one snapshot).
//!
//! **[`FileReplayAdapter`]** is always available. Kafka (`feature = "kafka"`) and
//! Pulsar (`feature = "pulsar"`) adapters are reserved seams: the features exist
//! so enabling them is an explicit build decision (rdkafka dominates compile
//! time), while the protocol-agnostic [`decode_json_envelope`] already covers
//! Kafka/CDC JSON payloads.

pub mod batch;
pub mod dedup;
pub mod dlq;
pub mod envelope;
pub mod error;
pub mod file;
pub mod offsets;
pub mod sink;
pub mod source;
pub mod watermark;

pub use batch::{
    idempotency_key, Pipeline, PollOutcome, PublishOutcome, RunOutcome, StreamConfig, StreamHealth,
    StreamMetrics,
};
pub use dedup::DedupStore;
pub use dlq::{dlq_date, dlq_schema, DeadLetterQueue};
pub use envelope::{
    decode_json_envelope, encode_envelopes, envelope_schema, event_id_hex, now_ns, parse_event_id,
    Envelope, JsonEnvelope,
};
pub use error::{IngestError, Result};
pub use file::FileReplayAdapter;
pub use offsets::OffsetStore;
pub use sink::{CatalogSink, FailingSink, MemorySink, Sink};
pub use source::{PartitionLag, PartitionOffset, SourceAdapter};
pub use watermark::{LatePolicy, Watermark, WatermarkConfig};
