//! B3-5 / B3-6 SQL-surface tests: `cbo_explain(...)` routing decisions and
//! `workload_status()` observability.

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use gtv_catalog::{ColumnStat, Scalar, TableStats};
use gtv_core::Metric;
use gtv_engine::cbo::GraphStats;
use gtv_engine::workload::{Admission, WorkloadClass, WorkloadError};
use gtv_engine::GtvContext;
use gtv_index::{AnyIndex, BuildOptions};

fn corpus(n: usize, dim: usize) -> (Vec<u64>, Vec<Vec<f32>>) {
    let ids: Vec<u64> = (0..n as u64).collect();
    let vs: Vec<Vec<f32>> = (0..n)
        .map(|i| {
            let x = i as f32;
            (0..dim).map(|d| ((x + d as f32) * 0.001).sin()).collect()
        })
        .collect();
    (ids, vs)
}

fn flat(n: usize) -> AnyIndex {
    let (ids, vs) = corpus(n, 8);
    AnyIndex::build(ids, vs, Metric::L2, &BuildOptions::Flat).unwrap()
}

fn q(dim: usize) -> String {
    (0..dim)
        .map(|d| format!("{}", (d as f32 * 0.01).sin()))
        .collect::<Vec<_>>()
        .join(",")
}

struct Plan {
    strategy: String,
    index_type: String,
    filter_first: bool,
    temporal_bitmap: bool,
    prune_sources: bool,
    enabled: bool,
    estimated_cost: f64,
    selectivity: f64,
}

async fn explain(ctx: &GtvContext, sql: &str) -> Plan {
    let out = ctx.sql(sql).await.unwrap();
    let b = &out[0];
    let s = |name: &str| {
        b.column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_string()
    };
    let bl = |name: &str| {
        b.column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    };
    let f = |name: &str| {
        b.column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
    };
    Plan {
        strategy: s("strategy"),
        index_type: s("index_type"),
        filter_first: bl("filter_first"),
        temporal_bitmap: bl("temporal_bitmap_first"),
        prune_sources: bl("prune_graph_sources"),
        enabled: bl("enabled"),
        estimated_cost: f("estimated_cost"),
        selectivity: f("selectivity"),
    }
}

#[tokio::test]
async fn cbo_explain_pure_vector_tiny_corpus_is_flat() {
    let ctx = GtvContext::new();
    ctx.register_any_index("idx", flat(500));
    let plan = explain(
        &ctx,
        &format!("SELECT * FROM cbo_explain('idx', '{}', 5)", q(8)),
    )
    .await;
    assert_eq!(plan.index_type, "flat");
    assert!(plan.filter_first);
    assert!(plan.enabled);
    assert!(plan.estimated_cost > 0.0);
}

#[tokio::test]
async fn cbo_explain_filter_plus_vector_switches_to_ivf_prefilter() {
    let ctx = GtvContext::new();
    ctx.register_any_index("big", flat(20_000));

    // Very selective filter (1 id out of 20k) -> filter first, IVF.
    let selective = explain(
        &ctx,
        &format!("SELECT * FROM cbo_explain('big', '{}', 5, 'l2', '0')", q(8)),
    )
    .await;
    assert!(selective.selectivity < 0.01);
    assert_eq!(selective.index_type, "ivf");
    assert!(selective.filter_first);
    assert_eq!(selective.strategy, "pre_filter_exact");

    // No filter -> high selectivity, HNSW recommendation, ANN first.
    let unfiltered = explain(
        &ctx,
        &format!("SELECT * FROM cbo_explain('big', '{}', 5)", q(8)),
    )
    .await;
    assert_eq!(unfiltered.index_type, "hnsw");
    assert!(!unfiltered.filter_first);
    assert_eq!(unfiltered.strategy, "post_filter_rerank");
}

#[tokio::test]
async fn cbo_explain_graph_plus_vector_applies_stats_hints() {
    let ctx = GtvContext::new();
    ctx.register_any_index("gv", flat(20_000));
    let sql = format!("SELECT * FROM cbo_explain('gv', '{}', 5)", q(8));

    // Without stats there is no temporal / graph signal.
    let before = explain(&ctx, &sql).await;
    assert!(!before.temporal_bitmap);
    assert!(!before.prune_sources);

    // Installing stats (a table with an event_time column + a high-degree,
    // mostly-expired graph) must change the plan.
    ctx.set_table_stats(
        "gv",
        TableStats {
            table: "gv".into(),
            row_count: 20_000,
            file_count: 1,
            event_time_min: 0,
            event_time_max: 1_000,
            columns: vec![ColumnStat {
                name: "event_time".into(),
                null_count: 0,
                min: None,
                max: None,
                distinct_est: None,
            }],
        },
    )
    .unwrap();
    ctx.set_graph_stats(
        "gv",
        GraphStats {
            node_count: 1_000,
            edge_count: 500_000,
            max_degree: 5_000,
            degree_histogram: vec![1, 2, 3],
            temporal_active_ratio: 0.05,
        },
    )
    .unwrap();

    let after = explain(&ctx, &sql).await;
    assert!(after.temporal_bitmap, "temporal bitmap hint not applied");
    assert!(after.prune_sources, "graph pruning hint not applied");
}

#[tokio::test]
async fn cbo_explain_disabled_optimizer_falls_back() {
    let ctx = GtvContext::new();
    ctx.register_any_index("h", flat(20_000));
    let mut model = gtv_engine::cbo::CostModel::default();
    model.enabled = false;
    ctx.set_cost_model(model).unwrap();
    let plan = explain(
        &ctx,
        &format!("SELECT * FROM cbo_explain('h', '{}', 5)", q(8)),
    )
    .await;
    assert!(!plan.enabled);
    assert_eq!(plan.index_type, "flat", "must use the live index when disabled");
}

#[tokio::test]
async fn stats_refresh_from_catalog_changes_plan_inputs() {
    let ctx = GtvContext::new();
    ctx.register_any_index("s", flat(20_000));
    let sql = format!("SELECT * FROM cbo_explain('s', '{}', 5)", q(8));
    assert!(!explain(&ctx, &sql).await.temporal_bitmap);

    // A stale-free catalog refresh is what the engine calls after a commit.
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let dir = std::env::temp_dir().join(format!("gtv_cbo_{}_{}", std::process::id(), stamp));
    let _ = std::fs::remove_dir_all(&dir);
    let cat = gtv_catalog::FsCatalog::open(&dir).unwrap();
    let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("event_time", arrow::datatypes::DataType::Int64, false),
    ]));
    let table = cat
        .create_table("s", schema, gtv_catalog::PartitionSpec::single())
        .unwrap();
    let batch = arrow::record_batch::RecordBatch::try_new(
        std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("event_time", arrow::datatypes::DataType::Int64, false),
        ])),
        vec![std::sync::Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3]))
            as arrow::array::ArrayRef],
    )
    .unwrap();
    cat.commit(
        table,
        gtv_catalog::CommitOp::Append,
        vec![gtv_catalog::NewFile::unpartitioned(batch)],
        &gtv_catalog::CommitOptions::default(),
    )
    .unwrap();
    ctx.refresh_table_stats("s", &cat, table).unwrap();
    ctx.set_graph_stats(
        "s",
        GraphStats {
            node_count: 10,
            edge_count: 100,
            max_degree: 5_000,
            degree_histogram: vec![1],
            temporal_active_ratio: 0.1,
        },
    )
    .unwrap();
    let after = explain(&ctx, &sql).await;
    assert!(after.temporal_bitmap);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn workload_status_surface_reports_admission() {
    let ctx = GtvContext::new();
    let w = ctx.workload();

    let held = match w.admit(WorkloadClass::IndexBuild) {
        Admission::Admit { id, .. } => id,
        other => panic!("expected admit, got {other:?}"),
    };

    let out = ctx
        .sql("SELECT class, active, max_concurrency FROM workload_status() WHERE class = 'index_build'")
        .await
        .unwrap();
    let b = &out[0];
    assert_eq!(b.num_rows(), 1);
    let class = b
        .column_by_name("class")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(class.value(0), "index_build");
    let active = b
        .column_by_name("active")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(active.value(0), 1);

    // All six classes are always reported (observability surface).
    let all = ctx.sql("SELECT count(*) AS n FROM workload_status()").await.unwrap();
    let n = all[0]
        .column_by_name("n")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, 6);

    w.release(held);
    let st = w.status();
    let idx = st
        .iter()
        .find(|s| s.class == WorkloadClass::IndexBuild)
        .unwrap();
    assert_eq!(idx.active, 0);
    assert!(idx.admitted >= 1);
}

/// Render every row of the `plan` column of an `EXPLAIN` result.
fn plan_text(batches: &[RecordBatch]) -> String {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of("plan").unwrap();
        let col = b.column(idx).as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..col.len() {
            out.push(col.value(i).to_string());
        }
    }
    out.join("\n")
}

#[tokio::test]
async fn explain_surfaces_catalog_statistics() {
    let ctx = GtvContext::new();
    let schema = std::sync::Arc::new(Schema::new(vec![Field::new(
        "id",
        DataType::UInt64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![std::sync::Arc::new(UInt64Array::from(vec![1u64, 2, 3])) as arrow::array::ArrayRef],
    )
    .unwrap();
    ctx.register_batches("stat_t", schema, vec![batch]).unwrap();
    ctx.set_table_stats(
        "stat_t",
        TableStats {
            table: "stat_t".into(),
            row_count: 99,
            file_count: 1,
            event_time_min: 0,
            event_time_max: 0,
            columns: vec![ColumnStat {
                name: "id".into(),
                null_count: 0,
                min: Some(Scalar::UInt(1)),
                max: Some(Scalar::UInt(3)),
                distinct_est: Some(3),
            }],
        },
    )
    .unwrap();

    // The row count comes from the registered catalog stats (99), not from the
    // 3 in-memory rows, proving the custom statistics provider is on the path.
    let text = plan_text(&ctx.sql("EXPLAIN SELECT * FROM stat_t").await.unwrap());
    assert!(text.contains("Rows=Exact(99)"), "EXPLAIN missing stats:\n{text}");
    assert!(
        text.contains("Min=Exact(UInt64(1))") && text.contains("Max=Exact(UInt64(3))"),
        "EXPLAIN missing column bounds:\n{text}"
    );
}

#[tokio::test]
async fn workload_admission_rejects_and_executes() {
    let ctx = GtvContext::new();
    // Interactive queries are admitted and produce rows.
    let out = ctx
        .sql_as(
            WorkloadClass::InteractiveAml,
            "SELECT 1 AS one",
            std::time::Duration::from_millis(500),
        )
        .await
        .unwrap();
    assert_eq!(out[0].num_rows(), 1);

    // Saturate the ftp class, then a second admission must time out.
    let w = ctx.workload();
    let held = match w.admit(WorkloadClass::FtpBatch) {
        Admission::Admit { id, .. } => id,
        other => panic!("expected admit, got {other:?}"),
    };
    let err = ctx
        .sql_as(
            WorkloadClass::FtpBatch,
            "SELECT 1",
            std::time::Duration::from_millis(20),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, WorkloadError::TimedOut(_)), "got {err:?}");
    w.release(held);
}

#[tokio::test]
async fn prometheus_exposes_spill_and_workload_metrics() {
    let ctx = GtvContext::new();
    let text = ctx.prometheus();
    assert!(text.contains("gtv_spill_bytes"), "{text}");
    assert!(text.contains("gtv_workload_active{class=\"interactive_aml\"}"), "{text}");
}

/// B3-6 acceptance: under a mixed load (interactive + index build) the
/// interactive class must stay within its admission SLO; the index build can
/// never stall it. Prints a report when run with `--nocapture`.
#[tokio::test]
async fn mixed_load_slo_report() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use gtv_engine::workload::{ResourceGroup, WorkloadManager};

    let m = Arc::new(WorkloadManager::with_limits(2, 256));
    // Load classes: ingestion + interactive + batch (risk/alm) + index build,
    // i.e. the four families in the B3-6 acceptance scenario. Let every
    // non-interactive class take up to the whole global budget so the
    // contention is real; interactive (priority 100) must preempt them.
    let load_classes = [
        WorkloadClass::Ingestion,
        WorkloadClass::RiskBatch,
        WorkloadClass::AlmBatch,
        WorkloadClass::IndexBuild,
    ];
    for class in load_classes {
        m.configure(ResourceGroup {
            class,
            max_concurrency: 2,
            ..ResourceGroup::new(class)
        });
    }
    let stop = Arc::new(AtomicBool::new(false));

    // Two workers per load class compete for the two global slots, so the
    // budget is permanently saturated by mixed load.
    let mut workers = Vec::new();
    for class in load_classes {
        for _ in 0..2 {
            let m = m.clone();
            let stop = stop.clone();
            workers.push(std::thread::spawn(move || {
                if let Ok(Admission::Admit { id, token }) =
                    m.wait_admit(class, Duration::from_secs(5))
                {
                    while !token.load(Ordering::Relaxed) && !stop.load(Ordering::Relaxed) {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    m.release(id);
                }
            }));
        }
    }

    // Wait until the global budget is genuinely occupied.
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        let active: usize = m.status().iter().map(|s| s.active).sum();
        if active >= 2 {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }

    // Measure interactive admission latency while the budget is saturated.
    const N: usize = 500;
    let mut lat = Vec::with_capacity(N);
    let mut timeouts = 0usize;
    for _ in 0..N {
        let t0 = Instant::now();
        match m.wait_admit(WorkloadClass::InteractiveAml, Duration::from_millis(200)) {
            Ok(Admission::Admit { id, .. }) => m.release(id),
            _ => timeouts += 1,
        }
        lat.push(t0.elapsed());
    }
    stop.store(true, Ordering::Relaxed);
    for w in workers {
        let _ = w.join();
    }

    lat.sort();
    let pct = |p: f64| lat[((lat.len() as f64 - 1.0) * p) as usize];
    let p50 = pct(0.50);
    let p99 = pct(0.99);
    let max = *lat.last().unwrap();
    let status = m.status();
    let total_preempted: u64 = status.iter().map(|s| s.preempted).sum();

    // Report (visible with `cargo test -p gtv-engine --test cbo_workload mixed_load -- --nocapture`).
    println!("B3-6 mixed-load interactive admission latency (n={N}):");
    println!("  p50 = {:?}", p50);
    println!("  p99 = {:?}", p99);
    println!("  max = {:?}", max);
    println!("  timeouts = {timeouts}");
    println!("  total preempted = {total_preempted}");
    for st in &status {
        if st.class != WorkloadClass::InteractiveAml {
            println!("    {:<16} preempted={}", st.class.as_str(), st.preempted);
        }
    }

    // SLO: interactive admission stays well under 50ms even while ingestion,
    // batch and index-build load saturate the global budget.
    assert_eq!(timeouts, 0, "interactive admission timed out under mixed load");
    assert!(p99 < Duration::from_millis(50), "interactive p99 SLO breach: {p99:?}");
    assert!(max < Duration::from_millis(200), "interactive max SLO breach: {max:?}");
    assert!(total_preempted > 0, "expected preemption of lower-priority load");
}
