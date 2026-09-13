//! Micro-batch execution: poll → dedup → watermark → atomic publish → commit.
//!
//! The pipeline is deliberately synchronous so the crash/restart story is easy
//! to reason about and test:
//!
//! 1. `poll` reads up to `max_batch` events and advances the *read cursor*
//!    (not the committed offset);
//! 2. duplicates (`event_id`) and late events are classified — late events are
//!    recomputed, routed to the DLQ or dropped per policy;
//! 3. the surviving batch is published through the [`Sink`] (catalog atomic
//!    commit, offsets recorded in the snapshot);
//! 4. only then are the offsets committed (offset store + source adapter) and
//!    the dedup window persisted.
//!
//! A failure anywhere before step 4 leaves the batch invisible and the offset
//! uncommitted, so a restart re-reads it (at-least-once). Dedup + the catalog
//! idempotency key make the replay effectively-once.

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;

use crate::dedup::DedupStore;
use crate::dlq::{dlq_date, DeadLetterQueue};
use crate::envelope::{encode_envelopes, now_ns, Envelope};
use crate::error::Result;
use crate::offsets::OffsetStore;
use crate::sink::Sink;
use crate::source::{PartitionOffset, SourceAdapter};
use crate::watermark::{LatePolicy, Watermark, WatermarkConfig};

/// Pipeline tuning / backpressure knobs.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamConfig {
    /// Maximum events polled per micro-batch.
    pub max_batch: usize,
    /// Maximum buffered (polled-but-unpublished) batches before backpressure.
    pub max_inflight_batches: usize,
    /// Backoff between empty polls.
    pub poll_interval_ms: u64,
    /// Optional events/second rate limit (source-side backpressure).
    pub rate_limit_eps: Option<u64>,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            max_batch: 1024,
            max_inflight_batches: 4,
            poll_interval_ms: 100,
            rate_limit_eps: None,
        }
    }
}

/// Cumulative pipeline counters (lag fields are point-in-time).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamMetrics {
    pub events_polled: u64,
    pub events_published: u64,
    pub events_duplicate: u64,
    pub events_late: u64,
    pub events_dropped: u64,
    pub events_dlq: u64,
    pub batches_polled: u64,
    pub batches_published: u64,
    pub last_event_time: Option<i64>,
    pub last_ingest_time: Option<i64>,
    pub last_poll_at: Option<i64>,
    pub last_publish_at: Option<i64>,
    /// Sum of unprocessed events across source partitions.
    pub offset_lag: i64,
}

impl StreamMetrics {
    /// Event-time lag: wall clock now minus the newest event time seen.
    pub fn event_time_lag_ns(&self, now: i64) -> i64 {
        self.last_event_time.map(|t| (now - t).max(0)).unwrap_or(0)
    }

    /// End-to-end lag: ingest time minus event time of the newest event.
    pub fn end_to_end_lag_ns(&self) -> i64 {
        match (self.last_ingest_time, self.last_event_time) {
            (Some(i), Some(e)) => (i - e).max(0),
            _ => 0,
        }
    }
}

/// Snapshot of pipeline + source health.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamHealth {
    pub metrics: StreamMetrics,
    pub buffered: usize,
}

/// Outcome of polling a batch into the buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PollOutcome {
    /// Source has no data right now.
    Empty,
    /// In-flight buffer is full; callers must publish before polling.
    Backpressure,
    Polled {
        events: usize,
        published: usize,
        duplicates: usize,
        late: usize,
    },
}

/// Outcome of publishing one buffered batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishOutcome {
    Nothing,
    Published { rows: usize, snapshot: String },
}

/// Outcome of [`Pipeline::run_once`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunOutcome {
    /// Source has no data right now.
    Empty,
    /// In-flight buffer is full; publish before polling.
    Backpressure,
    /// A batch was consumed but nothing needed publishing (all duplicate or
    /// late-handled). Offsets were committed; more data may still be pending.
    Skipped { events: usize },
    Published { rows: usize, snapshot: String },
}

struct BufferedBatch {
    batch: RecordBatch,
    offsets: Vec<PartitionOffset>,
    /// Fresh ids handled by this batch (inserted into dedup only after publish).
    ids: Vec<[u8; 16]>,
    idempotency_key: String,
}

/// The micro-batch pipeline.
pub struct Pipeline<A: SourceAdapter, S: Sink> {
    adapter: A,
    sink: S,
    dedup: DedupStore,
    watermark: Watermark,
    offsets: OffsetStore,
    dlq: DeadLetterQueue,
    cfg: StreamConfig,
    policy: LatePolicy,
    metrics: StreamMetrics,
    buffer: VecDeque<BufferedBatch>,
    last_poll: Option<Instant>,
}

impl<A: SourceAdapter, S: Sink> Pipeline<A, S> {
    pub fn new(
        adapter: A,
        sink: S,
        offsets: OffsetStore,
        dlq: DeadLetterQueue,
        dedup: DedupStore,
        cfg: StreamConfig,
        wm: WatermarkConfig,
    ) -> Self {
        let mut metrics = StreamMetrics::default();
        metrics.offset_lag = adapter.lag().iter().map(|l| l.lag()).sum();
        Self {
            adapter,
            sink,
            dedup,
            watermark: Watermark::from_config(&wm),
            offsets,
            dlq,
            cfg,
            policy: wm.policy,
            metrics,
            buffer: VecDeque::new(),
            last_poll: None,
        }
    }

    pub fn adapter(&self) -> &A {
        &self.adapter
    }

    pub fn metrics(&self) -> &StreamMetrics {
        &self.metrics
    }

    pub fn health(&self) -> StreamHealth {
        StreamHealth {
            metrics: self.metrics.clone(),
            buffered: self.buffer.len(),
        }
    }

    pub fn buffered_batches(&self) -> usize {
        self.buffer.len()
    }

    /// Poll one batch into the in-flight buffer. Returns [`PollOutcome::Backpressure`]
    /// without touching the source when the buffer is full.
    pub fn poll_once(&mut self) -> Result<PollOutcome> {
        if self.buffer.len() >= self.cfg.max_inflight_batches.max(1) {
            return Ok(PollOutcome::Backpressure);
        }
        self.throttle();

        let max = self.cfg.max_batch.max(1);
        let polled = self.adapter.poll(max)?;
        self.last_poll = Some(Instant::now());
        self.metrics.last_poll_at = Some(now_ns());
        self.metrics.offset_lag = self.adapter.lag().iter().map(|l| l.lag()).sum();
        if polled.is_empty() {
            return Ok(PollOutcome::Empty);
        }
        let polled_len = polled.len();
        self.metrics.batches_polled += 1;
        self.metrics.events_polled += polled_len as u64;
        if let Some(t) = polled.iter().map(|e| e.event_time).max() {
            self.metrics.last_event_time = Some(
                self.metrics
                    .last_event_time
                    .map_or(t, |cur| cur.max(t)),
            );
        }
        if let Some(t) = polled.iter().map(|e| e.ingest_time).max() {
            self.metrics.last_ingest_time = Some(t);
        }

        // Offsets cover *every* consumed event so a restart never re-reads a
        // duplicate the dedup window has already forgotten.
        let offsets = max_offsets(&polled);

        // Non-mutating duplicate check (in-batch + across the persisted window).
        let mut batch_seen: HashSet<[u8; 16]> = HashSet::new();
        let mut fresh: Vec<Envelope> = Vec::with_capacity(polled.len());
        let mut duplicates = 0usize;
        for e in polled {
            if self.dedup.contains(&e.event_id) || !batch_seen.insert(e.event_id) {
                duplicates += 1;
            } else {
                fresh.push(e);
            }
        }
        self.metrics.events_duplicate += duplicates as u64;

        let ids: Vec<[u8; 16]> = fresh.iter().map(|e| e.event_id).collect();
        let (on_time, late) = self.watermark.advance_and_split(fresh);
        let late_count = late.len();
        self.metrics.events_late += late_count as u64;

        let mut to_publish = on_time;
        match self.policy {
            LatePolicy::Recompute => to_publish.extend(late),
            LatePolicy::Dlq => {
                if !late.is_empty() {
                    let source = self.adapter.name().to_string();
                    let date = dlq_date(late.iter().map(|e| e.event_time).min().unwrap_or(0));
                    let partition = late[0].partition;
                    self.dlq.write(
                        &source,
                        &date,
                        partition,
                        &late,
                        "LATE_EVENT",
                        "event_time below watermark",
                    )?;
                    self.metrics.events_dlq += late.len() as u64;
                }
            }
            LatePolicy::Drop => self.metrics.events_dropped += late.len() as u64,
        }

        let published = to_publish.len();
        if published == 0 {
            // Everything was duplicate / late-handled: advance the offset
            // without emitting an empty snapshot.
            self.commit_progress(&offsets, &ids)?;
            return Ok(PollOutcome::Polled {
                events: polled_len,
                published: 0,
                duplicates,
                late: late_count,
            });
        }

        let batch = encode_envelopes(&to_publish)?;
        self.buffer.push_back(BufferedBatch {
            batch,
            idempotency_key: idempotency_key(&offsets),
            offsets,
            ids,
        });
        Ok(PollOutcome::Polled {
            events: polled_len,
            published,
            duplicates,
            late: late_count,
        })
    }

    /// Publish the oldest buffered batch, then commit offsets + dedup.
    pub fn publish_once(&mut self) -> Result<PublishOutcome> {
        let Some(b) = self.buffer.pop_front() else {
            return Ok(PublishOutcome::Nothing);
        };
        let outcome = match self
            .sink
            .publish(&b.batch, &b.offsets, Some(&b.idempotency_key))
        {
            Ok(o) => o,
            Err(e) => {
                // Nothing was published: drop all in-flight work and rewind the
                // source to the last durable offset so the next poll re-reads it.
                self.buffer.clear();
                let committed = self.offsets.all();
                let _ = self.adapter.seek(&committed);
                return Err(e);
            }
        };
        self.commit_progress(&b.offsets, &b.ids)?;
        self.metrics.events_published += outcome.rows as u64;
        self.metrics.batches_published += 1;
        self.metrics.last_publish_at = Some(now_ns());
        Ok(PublishOutcome::Published {
            rows: outcome.rows,
            snapshot: outcome.snapshot,
        })
    }

    /// Poll then publish (one micro-batch worth of work).
    pub fn run_once(&mut self) -> Result<RunOutcome> {
        if !self.buffer.is_empty() {
            return match self.publish_once()? {
                PublishOutcome::Nothing => Ok(RunOutcome::Empty),
                PublishOutcome::Published { rows, snapshot } => {
                    Ok(RunOutcome::Published { rows, snapshot })
                }
            };
        }
        match self.poll_once()? {
            PollOutcome::Empty => Ok(RunOutcome::Empty),
            PollOutcome::Backpressure => Ok(RunOutcome::Backpressure),
            PollOutcome::Polled {
                events,
                published: 0,
                ..
            } => Ok(RunOutcome::Skipped { events }),
            PollOutcome::Polled { .. } => match self.publish_once()? {
                PublishOutcome::Nothing => Ok(RunOutcome::Empty),
                PublishOutcome::Published { rows, snapshot } => {
                    Ok(RunOutcome::Published { rows, snapshot })
                }
            },
        }
    }

    /// Drain up to `max_batches` micro-batches (for bounded tests / backfills).
    /// Stops on [`RunOutcome::Empty`] / [`RunOutcome::Backpressure`]; skipped
    /// (duplicate / late) batches do not terminate the drain.
    pub fn run_bounded(&mut self, max_batches: usize) -> Result<Vec<RunOutcome>> {
        let mut out = Vec::new();
        for _ in 0..max_batches {
            let outcome = self.run_once()?;
            let stop = matches!(outcome, RunOutcome::Empty | RunOutcome::Backpressure);
            out.push(outcome);
            if stop {
                break;
            }
        }
        Ok(out)
    }

    fn throttle(&mut self) {
        if let Some(eps) = self.cfg.rate_limit_eps {
            if eps == 0 {
                return;
            }
            if let Some(last) = self.last_poll {
                let per_batch = self.cfg.max_batch.max(1) as f64 / eps as f64;
                let min = Duration::from_secs_f64(per_batch);
                let elapsed = last.elapsed();
                if elapsed < min {
                    std::thread::sleep(min - elapsed);
                }
            }
        }
    }

    fn commit_progress(&mut self, offsets: &[PartitionOffset], ids: &[[u8; 16]]) -> Result<()> {
        self.offsets.commit(offsets)?;
        self.adapter.commit(offsets)?;
        for id in ids {
            self.dedup.insert(*id);
        }
        self.dedup.persist()?;
        Ok(())
    }
}

/// Max offset per `(source, partition)` over a batch (deterministic order).
fn max_offsets(envs: &[Envelope]) -> Vec<PartitionOffset> {
    let mut map: BTreeMap<(String, i32), i64> = BTreeMap::new();
    for e in envs {
        let entry = map
            .entry((e.source.clone(), e.partition))
            .or_insert(e.offset);
        *entry = (*entry).max(e.offset);
    }
    map.into_iter()
        .map(|((source, partition), offset)| PartitionOffset::new(source, partition, offset))
        .collect()
}

/// Stable idempotency key for a set of offsets (same batch ⇒ same key ⇒ replay
/// resolves to the original snapshot).
pub fn idempotency_key(offsets: &[PartitionOffset]) -> String {
    let joined = offsets
        .iter()
        .map(|o| format!("{}:{}:{}", o.source, o.partition, o.offset))
        .collect::<Vec<_>>()
        .join("|");
    blake3::hash(joined.as_bytes()).to_hex().to_string()
}
