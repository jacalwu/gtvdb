//! B3-6 workload management / isolation.
//!
//! A single-process, best-effort implementation of workload classes, resource
//! groups, admission control and cooperative preemption (reusing the B1-3
//! [`CancelToken`]). One big graph query or index build can no longer starve
//! interactive AML: when the global concurrency budget is full a
//! higher-priority class preempts a lower-priority one, and otherwise the
//! request is explicitly queued or rejected.
//!
//! Real CPU/memory *enforcement* needs process isolation (enterprise batch);
//! here the manager decides and records admission, exposes telemetry as a
//! `workload_status()` SQL table and Prometheus text, and lets long-running
//! operators observe their cancel token.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl};
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use gtv_core::CancelToken;
use thiserror::Error;

/// Workload classes (design §7.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WorkloadClass {
    Ingestion,
    InteractiveAml,
    RiskBatch,
    AlmBatch,
    FtpBatch,
    IndexBuild,
}

impl WorkloadClass {
    pub const ALL: [WorkloadClass; 6] = [
        WorkloadClass::Ingestion,
        WorkloadClass::InteractiveAml,
        WorkloadClass::RiskBatch,
        WorkloadClass::AlmBatch,
        WorkloadClass::FtpBatch,
        WorkloadClass::IndexBuild,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            WorkloadClass::Ingestion => "ingestion",
            WorkloadClass::InteractiveAml => "interactive_aml",
            WorkloadClass::RiskBatch => "risk_batch",
            WorkloadClass::AlmBatch => "alm_batch",
            WorkloadClass::FtpBatch => "ftp_batch",
            WorkloadClass::IndexBuild => "index_build",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "ingestion" => Some(WorkloadClass::Ingestion),
            "interactive" | "interactive_aml" | "aml_interactive" => {
                Some(WorkloadClass::InteractiveAml)
            }
            "risk" | "risk_batch" => Some(WorkloadClass::RiskBatch),
            "alm" | "alm_batch" => Some(WorkloadClass::AlmBatch),
            "ftp" | "ftp_batch" => Some(WorkloadClass::FtpBatch),
            "index" | "index_build" => Some(WorkloadClass::IndexBuild),
            _ => None,
        }
    }

    /// Default priority (higher wins).
    pub fn default_priority(&self) -> u8 {
        match self {
            WorkloadClass::InteractiveAml => 100,
            WorkloadClass::Ingestion => 50,
            WorkloadClass::RiskBatch => 40,
            WorkloadClass::AlmBatch => 30,
            WorkloadClass::FtpBatch => 20,
            WorkloadClass::IndexBuild => 10,
        }
    }

    pub fn default_max_concurrency(&self) -> usize {
        match self {
            WorkloadClass::InteractiveAml => 4,
            WorkloadClass::Ingestion => 2,
            WorkloadClass::RiskBatch => 2,
            _ => 1,
        }
    }
}

/// Resource group (design §7.2). CPU/IO figures are relative shares.
#[derive(Debug, Clone, PartialEq)]
pub struct ResourceGroup {
    pub class: WorkloadClass,
    pub cpu_quota: f64,
    pub max_concurrency: usize,
    pub memory_limit_bytes: u64,
    pub io_limit_bps: Option<u64>,
    pub priority: u8,
}

impl ResourceGroup {
    pub fn new(class: WorkloadClass) -> Self {
        Self {
            class,
            cpu_quota: 0.1,
            max_concurrency: class.default_max_concurrency(),
            memory_limit_bytes: 512 * 1024 * 1024,
            io_limit_bps: None,
            priority: class.default_priority(),
        }
    }
}

/// Admission decision.
#[derive(Debug, Clone)]
pub enum Admission {
    Admit { id: u64, token: CancelToken },
    Queue { position: usize },
    Reject { reason: String },
}

/// Admission / preemption failures surfaced to the caller.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum WorkloadError {
    #[error("admission rejected: {0}")]
    Rejected(String),
    #[error("timed out waiting for admission in workload class `{0}`")]
    TimedOut(&'static str),
    #[error("query was preempted")]
    Preempted,
}

#[derive(Debug)]
struct ActiveEntry {
    id: u64,
    class: WorkloadClass,
    priority: u8,
    token: CancelToken,
}

#[derive(Debug, Clone, Default)]
struct ClassCounters {
    admitted: u64,
    queued: u64,
    rejected: u64,
    preempted: u64,
    completed: u64,
}

/// Per-class telemetry snapshot.
#[derive(Debug, Clone, PartialEq)]
pub struct ClassStatus {
    pub class: WorkloadClass,
    pub priority: u8,
    pub cpu_quota: f64,
    pub max_concurrency: usize,
    pub memory_limit_bytes: u64,
    pub io_limit_bps: Option<u64>,
    pub active: usize,
    pub admitted: u64,
    pub queued: u64,
    pub rejected: u64,
    pub preempted: u64,
    pub completed: u64,
}

/// Process-wide workload manager.
#[derive(Debug)]
pub struct WorkloadManager {
    groups: RwLock<HashMap<WorkloadClass, ResourceGroup>>,
    counters: RwLock<HashMap<WorkloadClass, ClassCounters>>,
    active: Mutex<Vec<ActiveEntry>>,
    next_id: AtomicU64,
    queue_depth: AtomicU64,
    global_max_active: usize,
    max_queue: usize,
}

impl Default for WorkloadManager {
    fn default() -> Self {
        Self::with_limits(8, 256)
    }
}

impl WorkloadManager {
    /// Manager with explicit global concurrency and queue caps.
    pub fn with_limits(global_max_active: usize, max_queue: usize) -> Self {
        let mut groups = HashMap::new();
        let mut counters = HashMap::new();
        for c in WorkloadClass::ALL {
            groups.insert(c, ResourceGroup::new(c));
            counters.insert(c, ClassCounters::default());
        }
        Self {
            groups: RwLock::new(groups),
            counters: RwLock::new(counters),
            active: Mutex::new(Vec::new()),
            next_id: AtomicU64::new(0),
            queue_depth: AtomicU64::new(0),
            global_max_active: global_max_active.max(1),
            max_queue: max_queue,
        }
    }

    pub fn global_max_active(&self) -> usize {
        self.global_max_active
    }

    /// Replace a class's resource group.
    pub fn configure(&self, group: ResourceGroup) {
        self.groups.write().unwrap().insert(group.class, group);
    }

    pub fn group(&self, class: WorkloadClass) -> ResourceGroup {
        self.groups
            .read()
            .unwrap()
            .get(&class)
            .cloned()
            .unwrap_or_else(|| ResourceGroup::new(class))
    }

    fn bump<F: FnOnce(&mut ClassCounters)>(&self, class: WorkloadClass, f: F) {
        let mut c = self.counters.write().unwrap();
        f(c.entry(class).or_default());
    }

    /// Request admission for one query of `class`.
    pub fn admit(&self, class: WorkloadClass) -> Admission {
        let g = self.group(class);
        let mut active = self.active.lock().unwrap();
        let class_active = active.iter().filter(|e| e.class == class).count();

        if class_active < g.max_concurrency {
            if active.len() < self.global_max_active {
                let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
                let token: CancelToken = Arc::new(std::sync::atomic::AtomicBool::new(false));
                active.push(ActiveEntry {
                    id,
                    class,
                    priority: g.priority,
                    token: token.clone(),
                });
                drop(active);
                self.bump(class, |c| c.admitted += 1);
                return Admission::Admit { id, token };
            }
            // Global budget full: preempt a strictly lower-priority class.
            if let Some(idx) = pick_victim(&active, class, g.priority) {
                let victim = active.remove(idx);
                victim
                    .token
                    .store(true, Ordering::Relaxed);
                let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
                let token: CancelToken = Arc::new(std::sync::atomic::AtomicBool::new(false));
                active.push(ActiveEntry {
                    id,
                    class,
                    priority: g.priority,
                    token: token.clone(),
                });
                drop(active);
                self.bump(victim.class, |c| c.preempted += 1);
                self.bump(class, |c| c.admitted += 1);
                return Admission::Admit { id, token };
            }
        }
        drop(active);

        let depth = self.queue_depth.fetch_add(1, Ordering::Relaxed) + 1;
        if depth as usize > self.max_queue {
            self.queue_depth.fetch_sub(1, Ordering::Relaxed);
            self.bump(class, |c| c.rejected += 1);
            return Admission::Reject {
                reason: format!("admission queue full ({} waiting)", self.max_queue),
            };
        }
        self.bump(class, |c| c.queued += 1);
        Admission::Queue {
            position: depth as usize,
        }
    }

    /// Block (bounded) until admitted, or fail with a clear reason.
    pub fn wait_admit(&self, class: WorkloadClass, timeout: Duration) -> Result<Admission, WorkloadError> {
        let deadline = Instant::now() + timeout;
        let mut queued = false;
        loop {
            match self.admit(class) {
                Admission::Admit { id, token } => {
                    if queued {
                        self.queue_depth.fetch_sub(1, Ordering::Relaxed);
                    }
                    return Ok(Admission::Admit { id, token });
                }
                Admission::Queue { .. } => {
                    queued = true;
                    if Instant::now() >= deadline {
                        self.queue_depth.fetch_sub(1, Ordering::Relaxed);
                        return Err(WorkloadError::TimedOut(class.as_str()));
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
                Admission::Reject { reason } => return Err(WorkloadError::Rejected(reason)),
            }
        }
    }

    /// Release a granted admission slot.
    pub fn release(&self, id: u64) {
        let mut active = self.active.lock().unwrap();
        if let Some(pos) = active.iter().position(|e| e.id == id) {
            let entry = active.remove(pos);
            drop(active);
            self.bump(entry.class, |c| c.completed += 1);
        }
    }

    /// Cancel one specific in-flight query (cooperative).
    pub fn preempt(&self, id: u64) -> bool {
        let mut active = self.active.lock().unwrap();
        if let Some(pos) = active.iter().position(|e| e.id == id) {
            let entry = active.remove(pos);
            entry.token.store(true, Ordering::Relaxed);
            drop(active);
            self.bump(entry.class, |c| c.preempted += 1);
            return true;
        }
        false
    }

    /// Cancel every in-flight query of a class; returns how many were signalled.
    pub fn preempt_class(&self, class: WorkloadClass) -> usize {
        let mut active = self.active.lock().unwrap();
        let mut n = 0;
        let mut kept = Vec::with_capacity(active.len());
        for entry in active.drain(..) {
            if entry.class == class {
                entry.token.store(true, Ordering::Relaxed);
                n += 1;
            } else {
                kept.push(entry);
            }
        }
        *active = kept;
        drop(active);
        if n > 0 {
            self.bump(class, |c| c.preempted += n as u64);
        }
        n
    }

    /// Run `f` under `class`, admitting first and releasing afterwards.
    pub fn execute<F, T>(&self, class: WorkloadClass, f: F) -> Result<T, WorkloadError>
    where
        F: FnOnce(&CancelToken) -> T,
    {
        match self.wait_admit(class, Duration::from_secs(5))? {
            Admission::Admit { id, token } => {
                let out = f(&token);
                self.release(id);
                Ok(out)
            }
            _ => Err(WorkloadError::Rejected("unexpected admission".into())),
        }
    }

    /// Snapshot every class's telemetry.
    pub fn status(&self) -> Vec<ClassStatus> {
        let active = self.active.lock().unwrap();
        let groups = self.groups.read().unwrap();
        let counters = self.counters.read().unwrap();
        WorkloadClass::ALL
            .iter()
            .map(|&class| {
                let g = groups.get(&class).cloned().unwrap_or_else(|| ResourceGroup::new(class));
                let c = counters.get(&class).cloned().unwrap_or_default();
                ClassStatus {
                    class,
                    priority: g.priority,
                    cpu_quota: g.cpu_quota,
                    max_concurrency: g.max_concurrency,
                    memory_limit_bytes: g.memory_limit_bytes,
                    io_limit_bps: g.io_limit_bps,
                    active: active.iter().filter(|e| e.class == class).count(),
                    admitted: c.admitted,
                    queued: c.queued,
                    rejected: c.rejected,
                    preempted: c.preempted,
                    completed: c.completed,
                }
            })
            .collect()
    }

    /// Prometheus text exposition of the workload counters.
    pub fn prometheus(&self) -> String {
        let status = self.status();
        let mut s = String::new();
        for metric in ["active", "admitted", "queued", "rejected", "preempted", "completed"] {
            s.push_str(&format!("# TYPE gtv_workload_{metric} gauge\n"));
            for st in &status {
                let v = match metric {
                    "active" => st.active as u64,
                    "admitted" => st.admitted,
                    "queued" => st.queued,
                    "rejected" => st.rejected,
                    "preempted" => st.preempted,
                    _ => st.completed,
                };
                s.push_str(&format!(
                    "gtv_workload_{metric}{{class=\"{}\"}} {v}\n",
                    st.class.as_str()
                ));
            }
        }
        s
    }
}

/// Pick the lowest-priority (oldest-first) active entry strictly below the
/// requester's priority.
fn pick_victim(active: &[ActiveEntry], requester: WorkloadClass, priority: u8) -> Option<usize> {
    active
        .iter()
        .enumerate()
        .filter(|(_, e)| e.class != requester && e.priority < priority)
        .min_by_key(|(_, e)| e.priority)
        .map(|(i, _)| i)
}

/// `workload_status()` — one row per workload class.
#[derive(Debug)]
pub struct WorkloadStatusTableFunction {
    manager: Arc<WorkloadManager>,
}

impl WorkloadStatusTableFunction {
    pub fn new(manager: Arc<WorkloadManager>) -> Self {
        Self { manager }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("class", DataType::Utf8, false),
            Field::new("priority", DataType::UInt64, false),
            Field::new("cpu_quota", DataType::Float64, false),
            Field::new("max_concurrency", DataType::UInt64, false),
            Field::new("memory_limit_bytes", DataType::UInt64, false),
            Field::new("io_limit_bps", DataType::Int64, true),
            Field::new("active", DataType::UInt64, false),
            Field::new("admitted", DataType::UInt64, false),
            Field::new("queued", DataType::UInt64, false),
            Field::new("rejected", DataType::UInt64, false),
            Field::new("preempted", DataType::UInt64, false),
            Field::new("completed", DataType::UInt64, false),
        ]))
    }
}

impl TableFunctionImpl for WorkloadStatusTableFunction {
    fn call_with_args(&self, _args: TableFunctionArgs) -> Result<Arc<dyn datafusion::datasource::TableProvider>> {
        let rows = self.manager.status();
        let schema = Self::schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.class.as_str()).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(UInt64Array::from(
                    rows.iter().map(|r| r.priority as u64).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Float64Array::from(
                    rows.iter().map(|r| r.cpu_quota).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(UInt64Array::from(
                    rows.iter().map(|r| r.max_concurrency as u64).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(UInt64Array::from(
                    rows.iter().map(|r| r.memory_limit_bytes).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Int64Array::from(
                    rows.iter()
                        .map(|r| r.io_limit_bps.map(|v| v as i64))
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(UInt64Array::from(
                    rows.iter().map(|r| r.active as u64).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(UInt64Array::from(
                    rows.iter().map(|r| r.admitted).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(UInt64Array::from(
                    rows.iter().map(|r| r.queued).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(UInt64Array::from(
                    rows.iter().map(|r| r.rejected).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(UInt64Array::from(
                    rows.iter().map(|r| r.preempted).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(UInt64Array::from(
                    rows.iter().map(|r| r.completed).collect::<Vec<_>>(),
                )) as ArrayRef,
            ],
        )?;
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn concurrency_quota_queues_then_admits() {
        let m = WorkloadManager::with_limits(8, 8);
        m.configure(ResourceGroup {
            class: WorkloadClass::AlmBatch,
            max_concurrency: 1,
            ..ResourceGroup::new(WorkloadClass::AlmBatch)
        });
        let first = m.admit(WorkloadClass::AlmBatch);
        assert!(matches!(first, Admission::Admit { .. }));
        let second = m.admit(WorkloadClass::AlmBatch);
        assert!(matches!(second, Admission::Queue { position: 1 }));
        if let Admission::Admit { id, .. } = first {
            m.release(id);
        }
        assert!(matches!(
            m.admit(WorkloadClass::AlmBatch),
            Admission::Admit { .. }
        ));
    }

    #[test]
    fn queue_overflow_is_rejected() {
        let m = WorkloadManager::with_limits(8, 1);
        m.configure(ResourceGroup {
            class: WorkloadClass::FtpBatch,
            max_concurrency: 1,
            ..ResourceGroup::new(WorkloadClass::FtpBatch)
        });
        let held = m.admit(WorkloadClass::FtpBatch);
        assert!(matches!(held, Admission::Admit { .. }));
        assert!(matches!(
            m.admit(WorkloadClass::FtpBatch),
            Admission::Queue { .. }
        ));
        assert!(matches!(
            m.admit(WorkloadClass::FtpBatch),
            Admission::Reject { .. }
        ));
    }

    #[test]
    fn interactive_preempts_low_priority_batch() {
        let m = WorkloadManager::with_limits(1, 8);
        let batch = m.admit(WorkloadClass::IndexBuild);
        let token = match &batch {
            Admission::Admit { token, .. } => token.clone(),
            other => panic!("expected admit, got {other:?}"),
        };
        assert!(!token.load(Ordering::Relaxed));
        let interactive = m.admit(WorkloadClass::InteractiveAml);
        assert!(matches!(interactive, Admission::Admit { .. }));
        // The index build was cancelled and recorded.
        assert!(token.load(Ordering::Relaxed), "victim token not tripped");
        let st = m.status();
        let idx = st.iter().find(|s| s.class == WorkloadClass::IndexBuild).unwrap();
        assert_eq!(idx.preempted, 1);
        assert_eq!(idx.active, 0);
    }

    #[test]
    fn same_priority_does_not_preempt() {
        let m = WorkloadManager::with_limits(1, 8);
        let a = m.admit(WorkloadClass::RiskBatch);
        assert!(matches!(a, Admission::Admit { .. }));
        let b = m.admit(WorkloadClass::AlmBatch); // lower priority than risk
        assert!(matches!(b, Admission::Queue { .. }));
    }

    #[test]
    fn mixed_load_keeps_interactive_admission_responsive() {
        use std::sync::atomic::AtomicBool;

        let m = Arc::new(WorkloadManager::with_limits(2, 64));
        // Allow two index builds to fill the whole global budget.
        m.configure(ResourceGroup {
            class: WorkloadClass::IndexBuild,
            max_concurrency: 2,
            ..ResourceGroup::new(WorkloadClass::IndexBuild)
        });
        let stop = Arc::new(AtomicBool::new(false));
        let mut workers = Vec::new();
        for _ in 0..4 {
            let m = m.clone();
            let stop = stop.clone();
            workers.push(std::thread::spawn(move || {
                if let Ok(Admission::Admit { id, token }) =
                    m.wait_admit(WorkloadClass::IndexBuild, Duration::from_secs(2))
                {
                    while !token.load(Ordering::Relaxed) && !stop.load(Ordering::Relaxed) {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    m.release(id);
                }
            }));
        }

        // Wait until the index builds actually occupy the global budget.
        let ready = Instant::now() + Duration::from_secs(2);
        while Instant::now() < ready {
            let st = m.status();
            let active = st
                .iter()
                .find(|s| s.class == WorkloadClass::IndexBuild)
                .map(|s| s.active)
                .unwrap_or(0);
            if active >= 2 {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }

        // Interactive queries must never stall behind the index builds.
        let mut worst = Duration::ZERO;
        for _ in 0..20 {
            let t0 = Instant::now();
            let admitted = m.wait_admit(WorkloadClass::InteractiveAml, Duration::from_millis(500));
            worst = worst.max(t0.elapsed());
            if let Ok(Admission::Admit { id, .. }) = admitted {
                m.release(id);
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        stop.store(true, Ordering::Relaxed);
        for w in workers {
            let _ = w.join();
        }

        assert!(
            worst < Duration::from_millis(200),
            "interactive admission stalled: {worst:?}"
        );
        let st = m.status();
        let idx = st
            .iter()
            .find(|s| s.class == WorkloadClass::IndexBuild)
            .unwrap();
        assert!(
            idx.preempted > 0,
            "expected index-build preemption under mixed load"
        );
    }

    #[test]
    fn explicit_preempt_and_prometheus() {
        let m = WorkloadManager::with_limits(4, 8);
        let a = m.admit(WorkloadClass::IndexBuild);
        let id = match a {
            Admission::Admit { id, token } => {
                assert!(!token.load(Ordering::Relaxed));
                id
            }
            _ => unreachable!(),
        };
        assert!(m.preempt(id));
        assert!(!m.preempt(id));
        let text = m.prometheus();
        assert!(text.contains("gtv_workload_preempted{class=\"index_build\"} 1"));
        assert!(text.contains("gtv_workload_active{class=\"interactive_aml\"} 0"));
    }
}
