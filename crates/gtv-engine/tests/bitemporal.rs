//! Bitemporal SQL surface tests (prod_p3 B3-2): two independent time axes,
//! system-time replayability after corrections, and overlap governance.

use arrow::array::{Array, RecordBatch, UInt64Array};
use gtv_core::bitemporal::migrate_legacy_edges;
use gtv_core::EdgeTable;
use gtv_engine::GtvContext;

/// A legacy edge batch upgraded to the canonical bitemporal schema.
fn version(
    src: &[u64],
    dst: &[u64],
    from: &[i64],
    to: &[i64],
    system_from: i64,
) -> RecordBatch {
    let legacy = EdgeTable::from_vecs(
        src.to_vec(),
        dst.to_vec(),
        vec![0u16; src.len()],
        from.to_vec(),
        to.to_vec(),
    )
    .unwrap();
    migrate_legacy_edges(legacy.batch(), system_from).unwrap()
}

fn dst_of(batches: &[RecordBatch]) -> Vec<u64> {
    let mut out = Vec::new();
    for b in batches {
        let a = b
            .column_by_name("dst")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        out.extend((0..b.num_rows()).map(|i| a.value(i)));
    }
    out
}

fn rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

async fn q(ctx: &GtvContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql).await.unwrap()
}

#[tokio::test]
async fn system_time_replays_what_the_system_knew() {
    let ctx = GtvContext::new();
    // System version 1: 1->10 valid [0,100), 2->20 valid [500,1000).
    ctx.register_bitemporal_version(
        "edges",
        1_000,
        vec![version(&[1, 2], &[10, 20], &[0, 500], &[100, 1000], 1_000)],
    )
    .unwrap();
    // Correction at system 2_000: the full snapshot with 1->11 corrected
    // (the old version is never overwritten).
    ctx.register_bitemporal_version(
        "edges",
        2_000,
        vec![version(
            &[1, 2],
            &[11, 20],
            &[0, 500],
            &[100, 1000],
            2_000,
        )],
    )
    .unwrap();

    // Business 50 at the old system cut sees the original target 10.
    let old = q(&ctx, "SELECT dst FROM as_of('edges', 50, 1500)").await;
    assert_eq!(dst_of(&old), vec![10]);
    // The same business instant at the new cut sees the correction.
    let new = q(&ctx, "SELECT dst FROM as_of('edges', 50, 2500)").await;
    assert_eq!(dst_of(&new), vec![11]);
    // Omitted system_ts means "latest known".
    let latest = q(&ctx, "SELECT dst FROM as_of('edges', 50)").await;
    assert_eq!(dst_of(&latest), vec![11]);
    // Replaying the old cut after the correction is byte-stable.
    let replay = q(&ctx, "SELECT dst FROM as_of('edges', 50, 1500)").await;
    assert_eq!(dst_of(&replay), vec![10]);
}

#[tokio::test]
async fn business_and_system_axes_are_independent() {
    let ctx = GtvContext::new();
    ctx.register_bitemporal_version(
        "edges",
        1_000,
        vec![version(&[1, 2], &[10, 20], &[0, 500], &[100, 1000], 1_000)],
    )
    .unwrap();

    // Business 600 selects edge 2 regardless of the system cut.
    assert_eq!(dst_of(&q(&ctx, "SELECT dst FROM as_of('edges', 600, 1500)").await), vec![20]);
    assert_eq!(dst_of(&q(&ctx, "SELECT dst FROM as_of('edges', 600, 9999)").await), vec![20]);
    // A business instant in no interval returns an empty, schema-typed result.
    let empty = q(&ctx, "SELECT dst FROM as_of('edges', 4000, 1500)").await;
    assert_eq!(rows(&empty), 0);
    assert!(empty[0].schema().index_of("dst").is_ok());
}

#[tokio::test]
async fn system_time_before_first_version_errors() {
    let ctx = GtvContext::new();
    ctx.register_bitemporal_version(
        "edges",
        1_000,
        vec![version(&[1], &[10], &[0], &[100], 1_000)],
    )
    .unwrap();
    let err = ctx
        .sql("SELECT * FROM as_of('edges', 50, 500)")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("no system version"), "{err}");
}

#[tokio::test]
async fn overlaps_report_contradictory_versions() {
    let ctx = GtvContext::new();
    // Same key 1 twice with overlapping business intervals in one version.
    ctx.register_bitemporal_version(
        "edges",
        1_000,
        vec![version(&[1, 1], &[10, 20], &[0, 5], &[100, 100], 1_000)],
    )
    .unwrap();
    let out = q(&ctx, "SELECT * FROM bitemporal_overlaps('edges', 'src')").await;
    assert_eq!(rows(&out), 1);
    let key = out[0]
        .column_by_name("key")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(key.value(0), 1);
}

#[tokio::test]
async fn corrections_do_not_create_overlaps() {
    let ctx = GtvContext::new();
    ctx.register_bitemporal_version(
        "edges",
        1_000,
        vec![version(&[1], &[10], &[0], &[100], 1_000)],
    )
    .unwrap();
    ctx.register_bitemporal_version(
        "edges",
        2_000,
        vec![version(&[1], &[20], &[0], &[100], 2_000)],
    )
    .unwrap();
    let out = q(&ctx, "SELECT * FROM bitemporal_overlaps('edges', 'src')").await;
    assert_eq!(rows(&out), 0);
}
