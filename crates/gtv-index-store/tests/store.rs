//! Integration tests for the versioned index store (B2-2).

use std::path::PathBuf;

use gtv_core::{Metric, VectorIndex};
use gtv_index::{AnyIndex, BuildOptions};
use gtv_index_store::{IndexMeta, IndexStore};

struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }
    fn f32(&mut self) -> f32 {
        (self.next() >> 40) as f32 / (1u64 << 24) as f32
    }
}

fn vectors(n: usize, dim: usize, seed: u64) -> (Vec<u64>, Vec<Vec<f32>>) {
    let mut rng = Lcg(seed);
    let ids = (0..n as u64).collect();
    let vs = (0..n)
        .map(|_| (0..dim).map(|_| rng.f32()).collect())
        .collect();
    (ids, vs)
}

fn root(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("gtv_idx_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn meta() -> IndexMeta {
    IndexMeta {
        model_id: "bge".into(),
        model_version: "1".into(),
        embedding_model: "bge-small".into(),
        ..Default::default()
    }
}

fn save_load_roundtrip(tag: &str, options: BuildOptions, metric: Metric) {
    let store = IndexStore::open(root(tag)).unwrap();
    let (ids, vs) = vectors(128, 8, 7);
    let index = AnyIndex::build(ids, vs.clone(), metric, &options).unwrap();
    let v = store.save(tag, &index, &options, &meta()).unwrap();
    assert_eq!(v.version, 1);
    assert_eq!(store.current_version(tag).unwrap(), Some(1));

    let loaded = store.load(tag, None).unwrap();
    assert_eq!(loaded.manifest.row_count, 128);
    assert_eq!(loaded.manifest.metric, metric.as_str());
    assert_eq!(loaded.index.metric(), metric);
    assert_eq!(loaded.index.len(), 128);

    let q = vs[3].clone();
    let a: Vec<u64> = index
        .search(&q, 5, None)
        .unwrap()
        .iter()
        .map(|h| h.id)
        .collect();
    let b: Vec<u64> = loaded
        .index
        .search(&q, 5, None)
        .unwrap()
        .iter()
        .map(|h| h.id)
        .collect();
    assert_eq!(a, b);
    // Exact indexes must recover the query vector itself.
    if !matches!(options, BuildOptions::Hnsw { .. }) {
        assert_eq!(b[0], 3);
    }
}

#[test]
fn flat_round_trip() {
    save_load_roundtrip("flat", BuildOptions::Flat, Metric::L2);
}

#[test]
fn ivf_round_trip() {
    save_load_roundtrip(
        "ivf",
        BuildOptions::Ivf {
            nlist: 8,
            nprobe: 8,
        },
        Metric::L2,
    );
}

#[test]
fn hnsw_round_trip() {
    save_load_roundtrip(
        "hnsw",
        BuildOptions::Hnsw {
            m: 16,
            ef_construction: 100,
            ef_search: 100,
        },
        Metric::Cosine,
    );
}

#[test]
fn shadow_build_then_swap_and_rollback() {
    let tag = "swap";
    let store = IndexStore::open(root(tag)).unwrap();
    let (ids, vs) = vectors(64, 4, 1);
    let v1_index = AnyIndex::build(ids.clone(), vs.clone(), Metric::L2, &BuildOptions::Flat).unwrap();
    store
        .save(tag, &v1_index, &BuildOptions::Flat, &meta())
        .unwrap();

    // Shadow build v2 (different corpus size) without touching CURRENT.
    let (ids2, vs2) = vectors(96, 4, 2);
    let v2_index = AnyIndex::build(ids2, vs2, Metric::L2, &BuildOptions::Flat).unwrap();
    let v2 = store
        .build(tag, &v2_index, &BuildOptions::Flat, &meta())
        .unwrap();
    assert_eq!(v2.version, 2);
    assert_eq!(store.current_version(tag).unwrap(), Some(1));
    assert_eq!(store.load(tag, None).unwrap().manifest.row_count, 64);

    // Swap to v2, then roll back to v1.
    store.activate(tag, 2).unwrap();
    assert_eq!(store.current_version(tag).unwrap(), Some(2));
    assert_eq!(store.load(tag, None).unwrap().manifest.row_count, 96);
    store.rollback(tag, 1).unwrap();
    assert_eq!(store.load(tag, None).unwrap().manifest.row_count, 64);

    assert_eq!(store.versions(tag).unwrap(), vec![1, 2]);
}

#[test]
fn rebuild_from_vectors_matches_direct_build() {
    let store = IndexStore::open(root("rebuild")).unwrap();
    let (ids, vs) = vectors(200, 6, 5);
    let opts = BuildOptions::Ivf {
        nlist: 16,
        nprobe: 4,
    };
    let built = AnyIndex::build(ids.clone(), vs.clone(), Metric::L2, &opts).unwrap();
    store.save("rb", &built, &opts, &meta()).unwrap();
    let loaded = store.load("rb", None).unwrap();

    // "Rebuild" = build again from the same authoritative vectors.
    let rebuilt = AnyIndex::build(ids, vs.clone(), Metric::L2, &opts).unwrap();
    let q = vs[10].clone();
    let a: Vec<u64> = loaded.index.search(&q, 5, None).unwrap().iter().map(|h| h.id).collect();
    let b: Vec<u64> = rebuilt.search(&q, 5, None).unwrap().iter().map(|h| h.id).collect();
    assert_eq!(a, b);
}

#[test]
fn corruption_is_detected() {
    let tag = "corrupt";
    let store = IndexStore::open(root(tag)).unwrap();
    let (ids, vs) = vectors(32, 4, 9);
    let index = AnyIndex::build(ids, vs, Metric::L2, &BuildOptions::Flat).unwrap();
    store.save(tag, &index, &BuildOptions::Flat, &meta()).unwrap();

    let id = store.resolve(tag).unwrap();
    let path = store
        .root()
        .join(id.to_string())
        .join("v1")
        .join("index.gtvidx");
    let mut bytes = std::fs::read(&path).unwrap();
    let n = bytes.len();
    bytes[n - 40] ^= 0xff;
    std::fs::write(&path, &bytes).unwrap();

    assert!(store.load(tag, None).is_err());
    assert!(store.verify(tag, 1).is_err());
}

#[test]
fn unknown_version_and_name_are_typed_errors() {
    let store = IndexStore::open(root("missing")).unwrap();
    assert!(store.resolve("nope").is_err());
    let (ids, vs) = vectors(8, 2, 1);
    let index = AnyIndex::build(ids, vs, Metric::L2, &BuildOptions::Flat).unwrap();
    store.save("x", &index, &BuildOptions::Flat, &meta()).unwrap();
    assert!(store.load("x", Some(99)).is_err());
    assert!(store.activate("x", 99).is_err());
}
