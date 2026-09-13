//! Event-time watermark and late-event policy.
//!
//! `watermark = max_event_time_seen - allowed_lateness`. An event with
//! `event_time < watermark` is *late*; the policy decides what happens to it.

use crate::envelope::Envelope;

/// What to do with an event that arrives below the watermark.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LatePolicy {
    /// Publish it anyway so downstream can recompute the affected window.
    Recompute,
    /// Route it to the dead-letter queue.
    Dlq,
    /// Count and discard (not recommended for banking).
    Drop,
}

/// Watermark configuration.
#[derive(Debug, Clone, PartialEq)]
pub struct WatermarkConfig {
    /// Allowed lateness in ns (0 = strictly monotonic event time).
    pub allowed_lateness_ns: i64,
    pub policy: LatePolicy,
}

impl Default for WatermarkConfig {
    fn default() -> Self {
        Self {
            allowed_lateness_ns: 0,
            policy: LatePolicy::Recompute,
        }
    }
}

/// Tracks the maximum event time seen and classifies arrivals.
#[derive(Debug, Clone)]
pub struct Watermark {
    allowed_lateness_ns: i64,
    max_event_time: i64,
    seen: bool,
}

impl Watermark {
    pub fn new(allowed_lateness_ns: i64) -> Self {
        Self {
            allowed_lateness_ns: allowed_lateness_ns.max(0),
            max_event_time: i64::MIN,
            seen: false,
        }
    }

    pub fn from_config(cfg: &WatermarkConfig) -> Self {
        Self::new(cfg.allowed_lateness_ns)
    }

    /// Fold one event time into the max.
    pub fn observe(&mut self, event_time: i64) {
        if !self.seen {
            self.max_event_time = event_time;
            self.seen = true;
        } else {
            self.max_event_time = self.max_event_time.max(event_time);
        }
    }

    /// Current watermark (`i64::MIN` before the first observation).
    pub fn watermark(&self) -> i64 {
        if !self.seen {
            i64::MIN
        } else {
            self.max_event_time - self.allowed_lateness_ns
        }
    }

    /// Highest event time observed.
    pub fn max_event_time(&self) -> Option<i64> {
        self.seen.then_some(self.max_event_time)
    }

    /// True when `event_time` is below the current watermark.
    pub fn is_late(&self, event_time: i64) -> bool {
        self.seen && event_time < self.watermark()
    }

    /// Advance over the whole batch, then split it into `(on_time, late)`.
    ///
    /// The batch's own maximum participates in the watermark, so a batch is
    /// classified against the max event time it (and all prior batches)
    /// contained — standard watermark semantics.
    pub fn advance_and_split(&mut self, envs: Vec<Envelope>) -> (Vec<Envelope>, Vec<Envelope>) {
        for e in &envs {
            self.observe(e.event_time);
        }
        let wm = self.watermark();
        let mut on_time = Vec::new();
        let mut late = Vec::new();
        for e in envs {
            if e.event_time < wm {
                late.push(e);
            } else {
                on_time.push(e);
            }
        }
        (on_time, late)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn env(t: i64) -> Envelope {
        Envelope::with_ingest_time(
            "s",
            0,
            t,
            Envelope::id_from_key(&t.to_string()),
            t,
            0,
            1,
            Bytes::from_static(b"x"),
        )
    }

    #[test]
    fn watermark_boundary_is_half_open() {
        let mut w = Watermark::new(10);
        w.observe(100);
        assert_eq!(w.watermark(), 90);
        // Exactly on the watermark is on time; one below is late.
        assert!(!w.is_late(90));
        assert!(w.is_late(89));
    }

    #[test]
    fn batch_max_advances_the_watermark() {
        let mut w = Watermark::new(5);
        let (on_time, late) = w.advance_and_split(vec![env(100), env(96), env(95)]);
        // max=100 -> watermark=95; 95 is on time, 96 on time, 100 on time.
        assert_eq!(on_time.len(), 3);
        assert!(late.is_empty());
        let (on_time, late) = w.advance_and_split(vec![env(200), env(94)]);
        // max=200 -> watermark=195; 94 is late.
        assert_eq!(on_time.len(), 1);
        assert_eq!(late.len(), 1);
        assert_eq!(late[0].event_time, 94);
    }

    #[test]
    fn empty_batch_keeps_watermark() {
        let mut w = Watermark::new(1);
        w.observe(50);
        let (on_time, late) = w.advance_and_split(vec![]);
        assert!(on_time.is_empty() && late.is_empty());
        assert_eq!(w.watermark(), 49);
    }
}
