//! B3-1 streaming acceptance tests: restart/resume, idempotent replay, atomic
//! publish visibility, late-event audit, metrics and backpressure.

use std::fs;
use std::path::PathBuf;

use gtv_ingest::{
    idempotency_key, CatalogSink, DeadLetterQueue, DedupStore, FileReplayAdapter, LatePolicy,
    MemorySink, OffsetStore, Pipeline, PollOutcome, RunOutcome, SourceAdapter, StreamConfig,
    WatermarkConfig,
};

/// Unique temp dir per test (removed at the end).
fn tmp(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "gtv_stream_{tag}_{}_{}",
        std::process::id(),
        gtv_ingest::now_ns()
    ));
    let _ = fs::remove_dir_all(&dir);
    dir
}

fn body(times: &[i64]) -> String {
    times
        .iter()
        .enumerate()
        .map(|(i, t)| format!("{{\"event_time\":{t},\"payload\":\"p{i}\"}}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn catalog_rows(sink: &CatalogSink) -> u64 {
    let cat = sink.catalog();
    let Some(snap) = cat.latest(sink.table_id()).unwrap() else {
        return 0;
    };
    cat.files(sink.table_id(), snap)
        .unwrap()
        .iter()
        .map(|f| f.row_count)
        .sum()
}

fn cfg(max_batch: usize) -> StreamConfig {
    StreamConfig {
        max_batch,
        max_inflight_batches: 4,
        poll_interval_ms: 1,
        rate_limit_eps: None,
    }
}

#[test]
fn publish_then_restart_resumes_from_committed_offset() {
    let dir = tmp("restart");
    let text = body(&[0, 1, 2, 3, 4]);

    // First run: 2 + 2 + 1 events published, offsets committed after each batch.
    let sink = CatalogSink::open(&dir, "events").unwrap();
    let adapter = FileReplayAdapter::from_str("feed", &text).unwrap();
    let offsets = OffsetStore::open(&dir).unwrap();
    let dedup = DedupStore::open(dir.join("metadata/dedup.txt"), 1000).unwrap();
    let mut p = Pipeline::new(
        adapter,
        sink.clone(),
        offsets,
        DeadLetterQueue::new(&dir),
        dedup,
        cfg(2),
        WatermarkConfig::default(),
    );
    let outcomes = p.run_bounded(10).unwrap();
    assert_eq!(
        outcomes
            .iter()
            .filter(|o| matches!(o, RunOutcome::Published { .. }))
            .count(),
        3
    );
    assert_eq!(p.metrics().events_published, 5);
    assert_eq!(catalog_rows(&sink), 5);

    // The catalog snapshot summary carries the source offsets (atomic record).
    let snap = sink.catalog().latest(sink.table_id()).unwrap().unwrap();
    let summary = &sink.catalog().snapshot(sink.table_id(), snap).unwrap().summary;
    assert!(summary["source_offsets"].is_array(), "summary={summary}");
    let files = sink.catalog().files(sink.table_id(), snap).unwrap();
    assert!(!files[0].source_offsets.is_empty());

    // Restart: a fresh adapter resyncs to the durable offsets and reads nothing.
    let mut adapter2 = FileReplayAdapter::from_str("feed", &text).unwrap();
    let offsets2 = OffsetStore::open(&dir).unwrap();
    assert_eq!(offsets2.committed("feed", 0), Some(4));
    adapter2.seek(&offsets2.all()).unwrap();
    let mut p2 = Pipeline::new(
        adapter2,
        sink.clone(),
        offsets2,
        DeadLetterQueue::new(&dir),
        DedupStore::new(1000),
        cfg(2),
        WatermarkConfig::default(),
    );
    assert_eq!(p2.run_once().unwrap(), RunOutcome::Empty);
    assert_eq!(catalog_rows(&sink), 5, "restart must not duplicate rows");
    assert_eq!(p2.metrics().events_duplicate, 0);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn replay_from_zero_is_idempotent_via_dedup() {
    let dir = tmp("replay");
    let text = body(&[10, 11, 12, 13]);
    let dedup_path = dir.join("metadata/dedup.txt");

    let sink = CatalogSink::open(&dir, "events").unwrap();
    let mut p = Pipeline::new(
        FileReplayAdapter::from_str("feed", &text).unwrap(),
        sink.clone(),
        OffsetStore::open(&dir).unwrap(),
        DeadLetterQueue::new(&dir),
        DedupStore::open(&dedup_path, 1000).unwrap(),
        cfg(2),
        WatermarkConfig::default(),
    );
    p.run_bounded(10).unwrap();
    assert_eq!(catalog_rows(&sink), 4);

    // Replay the whole file from offset 0 with the persisted dedup window.
    let mut adapter = FileReplayAdapter::from_str("feed", &text).unwrap();
    adapter.rewind();
    let mut p2 = Pipeline::new(
        adapter,
        sink.clone(),
        OffsetStore::open(&dir).unwrap(),
        DeadLetterQueue::new(&dir),
        DedupStore::open(&dedup_path, 1000).unwrap(),
        cfg(2),
        WatermarkConfig::default(),
    );
    // Nothing new is published; every event is recognised as a duplicate.
    let outcomes = p2.run_bounded(10).unwrap();
    assert!(outcomes.iter().all(|o| !matches!(o, RunOutcome::Published { .. })));
    assert_eq!(p2.metrics().events_polled, 4);
    assert_eq!(p2.metrics().events_duplicate, 4);
    assert_eq!(p2.metrics().events_published, 0);
    assert_eq!(catalog_rows(&sink), 4, "replay must not duplicate rows");
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn failed_publish_is_invisible_and_retried() {
    let dir = tmp("atomic");
    let text = body(&[1, 2]);

    // A sink that fails: the batch must stay invisible and offsets uncommitted.
    let failing = gtv_ingest::FailingSink;
    let offsets = OffsetStore::open(&dir).unwrap();
    let mut p = Pipeline::new(
        FileReplayAdapter::from_str("feed", &text).unwrap(),
        failing,
        offsets,
        DeadLetterQueue::new(&dir),
        DedupStore::new(1000),
        cfg(10),
        WatermarkConfig::default(),
    );
    let err = p.run_once().unwrap_err();
    assert!(err.to_string().contains("sink unavailable"), "{err}");
    // No offset was committed and the source was rewound for a retry.
    let offsets = OffsetStore::open(&dir).unwrap();
    assert_eq!(offsets.committed("feed", 0), None);
    assert_eq!(p.adapter().lag()[0].lag(), 2);

    // Retry with a working sink: the same events publish exactly once.
    let sink = CatalogSink::open(&dir, "events").unwrap();
    let mut adapter2 = FileReplayAdapter::from_str("feed", &text).unwrap();
    adapter2.seek(&offsets.all()).unwrap(); // empty commit -> rewind to start
    let mut p2 = Pipeline::new(
        adapter2,
        sink.clone(),
        offsets,
        DeadLetterQueue::new(&dir),
        DedupStore::new(1000),
        cfg(10),
        WatermarkConfig::default(),
    );
    let out = p2.run_once().unwrap();
    assert!(matches!(out, RunOutcome::Published { rows: 2, .. }), "{out:?}");
    assert_eq!(catalog_rows(&sink), 2);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn late_events_go_to_the_dlq_and_are_auditable() {
    let dir = tmp("dlq");
    let text = body(&[100, 50]); // second event is below the watermark

    let sink = CatalogSink::open(&dir, "events").unwrap();
    let dlq = DeadLetterQueue::new(&dir);
    let mut p = Pipeline::new(
        FileReplayAdapter::from_str("feed", &text).unwrap(),
        sink.clone(),
        OffsetStore::open(&dir).unwrap(),
        dlq.clone(),
        DedupStore::new(1000),
        cfg(10),
        WatermarkConfig {
            allowed_lateness_ns: 0,
            policy: LatePolicy::Dlq,
        },
    );
    p.run_bounded(5).unwrap();
    assert_eq!(p.metrics().events_late, 1);
    assert_eq!(p.metrics().events_dlq, 1);
    assert_eq!(p.metrics().events_published, 1);
    assert_eq!(catalog_rows(&sink), 1);

    let files = dlq.list("feed").unwrap();
    assert_eq!(files.len(), 1);
    let batches = dlq.read(&files[0]).unwrap();
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(
        batches[0]
            .schema()
            .index_of("error_code")
            .expect("error_code column"),
        6
    );
    // The consumed offset still advances past the DLQ'd event.
    assert_eq!(
        OffsetStore::open(&dir).unwrap().committed("feed", 0),
        Some(1)
    );
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn late_recompute_publishes_the_event() {
    let dir = tmp("recompute");
    let text = body(&[100, 50]);
    let sink = CatalogSink::open(&dir, "events").unwrap();
    let mut p = Pipeline::new(
        FileReplayAdapter::from_str("feed", &text).unwrap(),
        sink.clone(),
        OffsetStore::open(&dir).unwrap(),
        DeadLetterQueue::new(&dir),
        DedupStore::new(1000),
        cfg(10),
        WatermarkConfig {
            allowed_lateness_ns: 0,
            policy: LatePolicy::Recompute,
        },
    );
    p.run_bounded(5).unwrap();
    assert_eq!(p.metrics().events_late, 1);
    assert_eq!(p.metrics().events_published, 2);
    assert_eq!(catalog_rows(&sink), 2);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn backpressure_stops_polling_when_inflight_is_full() {
    let dir = tmp("backpressure");
    let text = body(&[0, 1, 2, 3, 4, 5]);
    let mut p = Pipeline::new(
        FileReplayAdapter::from_str("feed", &text).unwrap(),
        MemorySink::new(),
        OffsetStore::open(&dir).unwrap(),
        DeadLetterQueue::new(&dir),
        DedupStore::new(1000),
        StreamConfig {
            max_batch: 2,
            max_inflight_batches: 2,
            poll_interval_ms: 0,
            rate_limit_eps: None,
        },
        WatermarkConfig::default(),
    );
    assert!(matches!(p.poll_once().unwrap(), PollOutcome::Polled { .. }));
    assert!(matches!(p.poll_once().unwrap(), PollOutcome::Polled { .. }));
    // Buffer is full -> backpressure, source untouched.
    assert_eq!(p.poll_once().unwrap(), PollOutcome::Backpressure);
    assert_eq!(p.buffered_batches(), 2);
    // Draining makes room and polling resumes.
    assert!(matches!(
        p.publish_once().unwrap(),
        gtv_ingest::PublishOutcome::Published { .. }
    ));
    assert!(matches!(p.poll_once().unwrap(), PollOutcome::Polled { .. }));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn metrics_report_lag_and_progress() {
    let dir = tmp("metrics");
    let text = body(&[1000, 2000, 3000]);
    let sink = CatalogSink::open(&dir, "events").unwrap();
    let mut p = Pipeline::new(
        FileReplayAdapter::from_str("feed", &text).unwrap(),
        sink.clone(),
        OffsetStore::open(&dir).unwrap(),
        DeadLetterQueue::new(&dir),
        DedupStore::new(1000),
        cfg(2),
        WatermarkConfig::default(),
    );
    p.run_bounded(10).unwrap();
    let m = p.metrics();
    assert_eq!(m.events_polled, 3);
    assert_eq!(m.events_published, 3);
    assert_eq!(m.last_event_time, Some(3000));
    assert_eq!(m.offset_lag, 0);
    assert!(m.end_to_end_lag_ns() >= 0);
    assert!(m.event_time_lag_ns(gtv_ingest::now_ns()) > 0);
    assert_eq!(p.health().buffered, 0);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn idempotency_key_is_stable_for_the_same_offsets() {
    let dir = tmp("key");
    let a = gtv_ingest::PartitionOffset::new("feed", 0, 9);
    let b = gtv_ingest::PartitionOffset::new("feed", 1, 3);
    assert_eq!(
        idempotency_key(&[a.clone(), b.clone()]),
        idempotency_key(&[a.clone(), b.clone()])
    );
    assert_ne!(idempotency_key(&[a]), idempotency_key(&[b]));
    let _ = fs::remove_dir_all(&dir);
}
