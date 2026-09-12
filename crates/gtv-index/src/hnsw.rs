//! Approximate nearest-neighbor search via a Hierarchical Navigable Small
//! World (HNSW) graph, built from scratch with a deterministic PRNG so builds
//! are reproducible (and testable without external randomness).
//!
//! # Contiguous memory layout (B1-4)
//!
//! Every node's vector and neighbour lists live in flat `Vec`s instead of
//! `Vec<f32>` / `Vec<Vec<usize>>` per node, which removes the per-node
//! allocations and pointer chasing:
//!
//! * `vectors` — `n * dim` row-major f32 buffer;
//! * `node_offset[i]` — start of node `i`'s neighbour block;
//! * inside a block: level 0 occupies `m0` slots, level `l >= 1` occupies `m`;
//! * `block_start[i] + l` indexes `counts`, the live neighbour count of
//!   `(node i, level l)`.
//!
//! Queries use a generation-stamped visited scratch (zero allocation, safe for
//! parallel search) plus a min-heap candidate set and a bounded max-heap result
//! set. Tombstone deletion and a versioned `to_bytes` / `from_bytes` round-trip
//! are provided for the index-lifecycle work (B2-2).
//!
//! Supports [`Metric::L2`], [`Metric::Cosine`] and [`Metric::Ip`]; Cosine rows
//! (and the query) are unit-normalized, after which the score is `1 - dot`.

use std::borrow::Cow;
use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap};

use arrow::array::BooleanArray;
use gtv_core::{GtvError, Metric, Result, VectorHit, VectorIndex};

/// Deterministic 64-bit splitmix PRNG (seeded) — keeps HNSW builds reproducible.
#[derive(Debug, Clone)]
struct SplitMix64(u64);

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        SplitMix64(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform float in [0, 1).
    fn next_f32(&mut self) -> f32 {
        (self.next_u64() >> 40) as f32 / (1u64 << 24) as f32
    }
}

/// `f32` wrapper with a total order (NaN-safe) for heap keys.
#[derive(Debug, Clone, Copy, PartialEq)]
struct OrdF32(f32);

impl Eq for OrdF32 {}
impl PartialOrd for OrdF32 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for OrdF32 {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// Per-query visited scratch (generation-stamped; reset is O(1)).
struct Scratch {
    visited: Vec<u32>,
    gen: u32,
}

impl Scratch {
    fn new(capacity: usize) -> Self {
        Self {
            visited: vec![0u32; capacity],
            gen: 1,
        }
    }

    #[inline]
    fn reset(&mut self) {
        self.gen = self.gen.wrapping_add(1);
        if self.gen == 0 {
            self.visited.iter_mut().for_each(|v| *v = 0);
            self.gen = 1;
        }
    }

    #[inline]
    fn visit(&mut self, i: u32) -> bool {
        let idx = i as usize;
        if idx >= self.visited.len() {
            self.visited.resize(idx + 1, 0);
        }
        if self.visited[idx] == self.gen {
            false
        } else {
            self.visited[idx] = self.gen;
            true
        }
    }
}

/// Telemetry from one approximate search.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchReport {
    pub candidates_visited: u64,
    pub filtered_out: u64,
    pub ef: usize,
}

const MAGIC: &[u8; 8] = b"GHNSWv1\0";
const FORMAT_VERSION: u16 = 1;

/// Approximate K-NN over an HNSW graph.
///
/// The bitmask is indexed by node *position* (0..n), identical to
/// [`FlatIndex`](crate::FlatIndex).
pub struct HnswIndex {
    ids: Vec<u64>,
    /// Row-major: vector `i` occupies `vectors[i * dim .. (i + 1) * dim]`.
    vectors: Vec<f32>,
    dim: usize,
    metric: Metric,
    /// Top level of each node.
    levels: Vec<u8>,
    /// `node_offset[i]` = start of node `i`'s neighbour block (len `n + 1`).
    node_offset: Vec<u32>,
    /// Fixed-capacity neighbour slots (see module docs).
    neighbours: Vec<u32>,
    /// `block_start[i]` = index of node `i`'s level-0 count (len `n + 1`).
    block_start: Vec<u32>,
    /// Live neighbour count per `(node, level)` block.
    counts: Vec<u8>,
    entry: u32,
    max_level: u8,
    /// Max neighbours per node at layer >= 1.
    m: usize,
    /// Max neighbours per node at layer 0 (usually 2 * `m`).
    m0: usize,
    ef_construction: usize,
    ef_search: usize,
    rng: SplitMix64,
    deleted: Vec<bool>,
    tombstone_count: usize,
    pos_of: HashMap<u64, u32>,
}

impl HnswIndex {
    /// New L2 index.
    pub fn new(m: usize, ef_construction: usize, ef_search: usize) -> Self {
        Self::with_metric(m, ef_construction, ef_search, Metric::L2)
    }

    /// New index with an explicit metric.
    pub fn with_metric(m: usize, ef_construction: usize, ef_search: usize, metric: Metric) -> Self {
        let m = m.max(1);
        HnswIndex {
            ids: Vec::new(),
            vectors: Vec::new(),
            dim: 0,
            metric,
            levels: Vec::new(),
            node_offset: vec![0],
            neighbours: Vec::new(),
            block_start: vec![0],
            counts: Vec::new(),
            entry: 0,
            max_level: 0,
            m,
            m0: (m * 2).max(1),
            ef_construction: ef_construction.max(1),
            ef_search: ef_search.max(1),
            rng: SplitMix64::new(0x243F_6A88_85A3_08D3),
            deleted: Vec::new(),
            tombstone_count: 0,
            pos_of: HashMap::new(),
        }
    }

    /// Convenience: build an L2 index from scratch, inserting each vector in order.
    pub fn build(
        ids: Vec<u64>,
        vectors: Vec<Vec<f32>>,
        m: usize,
        ef_construction: usize,
        ef_search: usize,
    ) -> Result<Self> {
        Self::build_with_metric(ids, vectors, m, ef_construction, ef_search, Metric::L2)
    }

    /// Build an index with an explicit metric.
    pub fn build_with_metric(
        ids: Vec<u64>,
        vectors: Vec<Vec<f32>>,
        m: usize,
        ef_construction: usize,
        ef_search: usize,
        metric: Metric,
    ) -> Result<Self> {
        if ids.len() != vectors.len() {
            return Err(GtvError::InvalidArgument(
                "ids and vectors length mismatch".into(),
            ));
        }
        let mut index = Self::with_metric(m, ef_construction, ef_search, metric);
        let mut scratch = Scratch::new(ids.len());
        for (id, v) in ids.into_iter().zip(vectors) {
            index.insert_with_scratch(id, v, &mut scratch)?;
        }
        Ok(index)
    }

    pub fn len(&self) -> usize {
        self.ids.len()
    }

    /// Number of non-deleted nodes.
    pub fn live_len(&self) -> usize {
        self.ids.len() - self.tombstone_count
    }

    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    pub fn metric(&self) -> Metric {
        self.metric
    }

    pub fn tombstone_count(&self) -> usize {
        self.tombstone_count
    }

    // -- contiguous-layout accessors ---------------------------------------

    #[inline]
    fn vector(&self, i: u32) -> &[f32] {
        let i = i as usize;
        &self.vectors[i * self.dim..(i + 1) * self.dim]
    }

    #[inline]
    fn level_capacity(&self, level: u8) -> usize {
        if level == 0 {
            self.m0
        } else {
            self.m
        }
    }

    #[inline]
    fn block_index(&self, i: u32, level: u8) -> usize {
        self.block_start[i as usize] as usize + level as usize
    }

    #[inline]
    fn level_base(&self, i: u32, level: u8) -> usize {
        let node = self.node_offset[i as usize] as usize;
        if level == 0 {
            node
        } else {
            node + self.m0 + (level as usize - 1) * self.m
        }
    }

    #[inline]
    fn level_count(&self, i: u32, level: u8) -> usize {
        self.counts[self.block_index(i, level)] as usize
    }

    #[inline]
    fn level_slice(&self, i: u32, level: u8) -> &[u32] {
        let base = self.level_base(i, level);
        let cnt = self.level_count(i, level);
        &self.neighbours[base..base + cnt]
    }

    fn write_level(&mut self, i: u32, level: u8, nbs: &[u32]) {
        let cap = self.level_capacity(level);
        let take = nbs.len().min(cap);
        let base = self.level_base(i, level);
        self.neighbours[base..base + take].copy_from_slice(&nbs[..take]);
        let bi = self.block_index(i, level);
        self.counts[bi] = take as u8;
    }

    fn add_and_prune(&mut self, node: u32, level: u8, new_nb: u32) {
        let cap = self.level_capacity(level);
        let mut cur: Vec<u32> = self.level_slice(node, level).to_vec();
        if cur.contains(&new_nb) {
            return;
        }
        cur.push(new_nb);
        if cur.len() > cap {
            let target = self.vector(node).to_vec();
            let mut scored: Vec<(f32, u32)> = cur
                .into_iter()
                .map(|nb| (self.dist(&target, self.vector(nb)), nb))
                .collect();
            scored.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
            cur = scored.into_iter().take(cap).map(|(_, n)| n).collect();
        }
        self.write_level(node, level, &cur);
    }

    #[inline]
    fn dist(&self, a: &[f32], b: &[f32]) -> f32 {
        self.metric.distance(a, b)
    }

    fn prepare_query<'a>(&self, query: &'a [f32]) -> Cow<'a, [f32]> {
        if self.metric.requires_normalization() {
            let mut q = query.to_vec();
            self.metric.normalize_in_place(&mut q);
            Cow::Owned(q)
        } else {
            Cow::Borrowed(query)
        }
    }

    /// Sample a level from the geometric-ish distribution `floor(-ln(u) * mL)`.
    fn random_level(&mut self) -> usize {
        let ml = 1.0 / (self.m as f32).ln();
        let u = self.rng.next_f32().max(1e-6);
        (-u.ln() * ml) as usize
    }

    /// Greedy (ef = 1) descent to the nearest node at `level` from `start`.
    fn greedy_descend(&self, query: &[f32], mut cur: u32, level: u8) -> (f32, u32) {
        let mut cur_d = self.dist(query, self.vector(cur));
        loop {
            let mut best = (cur_d, cur);
            for &nb in self.level_slice(cur, level) {
                let d = self.dist(query, self.vector(nb));
                if d < best.0 {
                    best = (d, nb);
                }
            }
            if best.1 == cur {
                return best;
            }
            cur = best.1;
            cur_d = best.0;
        }
    }

    /// Beam search at `level`: min-heap candidates + bounded max-heap results.
    #[allow(clippy::too_many_arguments)]
    fn search_layer(
        &self,
        query: &[f32],
        entry_points: &[(f32, u32)],
        ef: usize,
        level: u8,
        mask: Option<&BooleanArray>,
        scratch: &mut Scratch,
        report: &mut SearchReport,
    ) -> Vec<(f32, u32)> {
        let allowed = |i: u32| -> bool {
            if self.deleted.get(i as usize).copied().unwrap_or(false) {
                return false;
            }
            mask.map_or(true, |m| m.value(i as usize))
        };

        let mut candidates: BinaryHeap<Reverse<(OrdF32, u32)>> = BinaryHeap::new();
        let mut results: BinaryHeap<(OrdF32, u32)> = BinaryHeap::new();

        for &(d, i) in entry_points {
            scratch.visit(i);
            candidates.push(Reverse((OrdF32(d), i)));
            if allowed(i) {
                results.push((OrdF32(d), i));
            } else {
                report.filtered_out += 1;
            }
        }

        while let Some(Reverse((cd, ci))) = candidates.pop() {
            let farthest = results.peek().map(|(d, _)| d.0).unwrap_or(f32::INFINITY);
            if results.len() >= ef && cd.0 > farthest {
                break;
            }
            report.candidates_visited += 1;
            for &nb in self.level_slice(ci, level) {
                if !scratch.visit(nb) {
                    continue;
                }
                let d = self.dist(query, self.vector(nb));
                let farthest = results.peek().map(|(d, _)| d.0).unwrap_or(f32::INFINITY);
                if results.len() < ef || d < farthest {
                    candidates.push(Reverse((OrdF32(d), nb)));
                    if allowed(nb) {
                        results.push((OrdF32(d), nb));
                        if results.len() > ef {
                            results.pop();
                        }
                    } else {
                        report.filtered_out += 1;
                    }
                }
            }
        }
        results
            .into_sorted_vec()
            .into_iter()
            .map(|(d, i)| (d.0, i))
            .collect()
    }

    /// Wire a new node into every layer from `min(level, max_level)` down.
    fn connect(&mut self, new_idx: u32, scratch: &mut Scratch) {
        let level = self.levels[new_idx as usize] as usize;
        let query = self.vector(new_idx).to_vec();
        let mut cur = self.entry;
        let mut cur_d = self.dist(&query, self.vector(cur));
        for l in ((level + 1)..=self.max_level as usize).rev() {
            let (d, n) = self.greedy_descend(&query, cur, l as u8);
            cur = n;
            cur_d = d;
        }

        let top = level.min(self.max_level as usize);
        for l in (0..=top).rev() {
            let l8 = l as u8;
            let mut report = SearchReport::default();
            let mut candidates = self.search_layer(
                &query,
                &[(cur_d, cur)],
                self.ef_construction,
                l8,
                None,
                scratch,
                &mut report,
            );
            candidates.retain(|&(_, i)| i != new_idx);
            let max_deg = self.level_capacity(l8);
            let selected: Vec<u32> = candidates
                .iter()
                .take(max_deg)
                .map(|&(_, i)| i)
                .collect();

            self.write_level(new_idx, l8, &selected);
            for &nb in &selected {
                self.add_and_prune(nb, l8, new_idx);
            }
            if let Some(&(d, n)) = candidates.first() {
                cur = n;
                cur_d = d;
            }
        }
    }

    fn insert_with_scratch(&mut self, id: u64, mut vector: Vec<f32>, scratch: &mut Scratch) -> Result<()> {
        if self.dim == 0 {
            self.dim = vector.len();
        } else if vector.len() != self.dim {
            return Err(GtvError::DimensionMismatch {
                index: self.dim,
                query: vector.len(),
            });
        }
        self.metric.normalize_in_place(&mut vector);

        let level = self.random_level();
        let new_idx = self.ids.len() as u32;

        // Append vector + metadata.
        self.ids.push(id);
        self.vectors.extend_from_slice(&vector);
        self.levels.push(level as u8);
        self.deleted.push(false);
        self.pos_of.insert(id, new_idx);

        // Reserve the neighbour block for this node.
        let prev = *self.node_offset.last().unwrap();
        let add = (self.m0 + level * self.m) as u32;
        self.node_offset.push(prev + add);
        self.neighbours.resize((prev + add) as usize, 0);
        let bprev = *self.block_start.last().unwrap();
        self.block_start.push(bprev + level as u32 + 1);
        self.counts.resize((bprev + level as u32 + 1) as usize, 0);

        if new_idx == 0 {
            self.entry = 0;
            self.max_level = level as u8;
            return Ok(());
        }
        self.connect(new_idx, scratch);
        if level > self.max_level as usize {
            self.max_level = level as u8;
            self.entry = new_idx;
        }
        Ok(())
    }

    /// Insert a single node, wiring it into the graph bidirectionally.
    pub fn insert(&mut self, id: u64, vector: Vec<f32>) -> Result<()> {
        let mut scratch = Scratch::new(self.ids.len() + 1);
        self.insert_with_scratch(id, vector, &mut scratch)
    }

    /// Mark `id` deleted (tombstone). Its neighbours stay in place; searches skip
    /// it. Run [`HnswIndex::compact`] once tombstones dominate.
    pub fn delete(&mut self, id: u64) -> bool {
        if let Some(&pos) = self.pos_of.get(&id) {
            if !self.deleted[pos as usize] {
                self.deleted[pos as usize] = true;
                self.tombstone_count += 1;
                return true;
            }
        }
        false
    }

    /// Rebuild the graph from the live nodes only (deterministic).
    pub fn compact(&self) -> Result<HnswIndex> {
        let mut ids = Vec::with_capacity(self.live_len());
        let mut vectors = Vec::with_capacity(self.live_len());
        for i in 0..self.ids.len() {
            if !self.deleted[i] {
                ids.push(self.ids[i]);
                vectors.push(self.vector(i as u32).to_vec());
            }
        }
        HnswIndex::build_with_metric(
            ids,
            vectors,
            self.m,
            self.ef_construction,
            self.ef_search,
            self.metric,
        )
    }

    /// Full search with an explicit `ef`, returning hits + telemetry.
    pub fn search_with_report(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        mask: Option<&BooleanArray>,
    ) -> Result<(Vec<VectorHit>, SearchReport)> {
        let mut scratch = Scratch::new(self.ids.len());
        self.search_with_scratch(query, k, ef, mask, &mut scratch)
    }

    /// Core search using a caller-provided scratch (so batched/parallel callers
    /// reuse one visited buffer instead of allocating per query).
    fn search_with_scratch(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        mask: Option<&BooleanArray>,
        scratch: &mut Scratch,
    ) -> Result<(Vec<VectorHit>, SearchReport)> {
        if query.len() != self.dim {
            return Err(GtvError::DimensionMismatch {
                index: self.dim,
                query: query.len(),
            });
        }
        if let Some(m) = mask {
            if m.len() != self.ids.len() {
                return Err(GtvError::InvalidArgument(
                    "filter mask length mismatch".into(),
                ));
            }
        }
        let mut report = SearchReport {
            ef: ef.max(k),
            ..Default::default()
        };
        if k == 0 || self.ids.is_empty() || self.live_len() == 0 {
            return Ok((Vec::new(), report));
        }
        let query = self.prepare_query(query);
        let ef = ef.max(k);

        let mut cur = self.entry;
        let mut cur_d = self.dist(&query, self.vector(cur));
        for l in (1..=self.max_level).rev() {
            let (d, n) = self.greedy_descend(&query, cur, l);
            cur = n;
            cur_d = d;
        }
        let mut results = self.search_layer(
            &query,
            &[(cur_d, cur)],
            ef,
            0,
            mask,
            scratch,
            &mut report,
        );
        results.truncate(k);
        Ok((
            results
                .into_iter()
                .map(|(distance, i)| VectorHit {
                    id: self.ids[i as usize],
                    distance,
                })
                .collect(),
            report,
        ))
    }

    /// Search with an explicit `ef`.
    pub fn search_with_ef(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        mask: Option<&BooleanArray>,
    ) -> Result<Vec<VectorHit>> {
        self.search_with_report(query, k, ef, mask).map(|(h, _)| h)
    }

    /// Parallel batch search (one reused scratch per rayon worker; deterministic).
    pub fn search_batch(&self, queries: &[Vec<f32>], k: usize) -> Result<Vec<Vec<VectorHit>>> {
        use rayon::prelude::*;
        let n = self.ids.len();
        let ef = self.ef_search;
        queries
            .par_iter()
            .map_init(
                || Scratch::new(n),
                |scratch, q| {
                    scratch.reset();
                    let (hits, _) = self.search_with_scratch(q, k, ef, None, scratch)?;
                    Ok(hits)
                },
            )
            .collect()
    }

    /// Exact-distance fallback over the allowed positions (used by the
    /// filter-aware router: selective filters are cheaper as an exact scan).
    pub fn filtered_exact(
        &self,
        query: &[f32],
        k: usize,
        mask: &BooleanArray,
    ) -> Result<Vec<VectorHit>> {
        if query.len() != self.dim {
            return Err(GtvError::DimensionMismatch {
                index: self.dim,
                query: query.len(),
            });
        }
        if mask.len() != self.ids.len() {
            return Err(GtvError::InvalidArgument(
                "filter mask length mismatch".into(),
            ));
        }
        let query = self.prepare_query(query);
        let mut scored: Vec<(f32, u64)> = (0..self.ids.len())
            .filter(|&i| mask.value(i) && !self.deleted[i])
            .map(|i| (self.dist(&query, self.vector(i as u32)), self.ids[i]))
            .collect();
        scored.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        scored.truncate(k);
        Ok(scored
            .into_iter()
            .map(|(distance, id)| VectorHit { id, distance })
            .collect())
    }

    // -- versioned serialization (consumed by B2-2) -------------------------

    /// Serialize to the versioned `GHNSWv1` container.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes()); // flags
        out.extend_from_slice(&(self.ids.len() as u64).to_le_bytes());
        out.extend_from_slice(&(self.dim as u32).to_le_bytes());
        out.extend_from_slice(&(self.m as u32).to_le_bytes());
        out.extend_from_slice(&(self.m0 as u32).to_le_bytes());
        out.extend_from_slice(&(self.ef_construction as u32).to_le_bytes());
        out.extend_from_slice(&(self.ef_search as u32).to_le_bytes());
        out.push(self.max_level);
        out.push(metric_code(self.metric));
        out.extend_from_slice(&self.entry.to_le_bytes());
        out.extend_from_slice(&(self.tombstone_count as u64).to_le_bytes());
        for id in &self.ids {
            out.extend_from_slice(&id.to_le_bytes());
        }
        for v in &self.vectors {
            out.extend_from_slice(&v.to_le_bytes());
        }
        out.extend_from_slice(&self.levels);
        for x in &self.node_offset {
            out.extend_from_slice(&x.to_le_bytes());
        }
        for x in &self.neighbours {
            out.extend_from_slice(&x.to_le_bytes());
        }
        for x in &self.block_start {
            out.extend_from_slice(&x.to_le_bytes());
        }
        out.extend_from_slice(&self.counts);
        out.extend(self.deleted.iter().map(|&d| d as u8));
        out
    }

    /// Blak3-style checksum over the serialized bytes (FNV-1a here to avoid a
    /// new dependency; replaceable by blake3 when B2-2 lands).
    pub fn checksum(&self) -> u64 {
        let bytes = self.to_bytes();
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for b in bytes {
            hash ^= b as u64;
            hash = hash.wrapping_mul(0x100_0000_01b3);
        }
        hash
    }

    /// Deserialize a `to_bytes` payload.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut r = Reader::new(bytes);
        if r.take(8)? != &MAGIC[..] {
            return Err(GtvError::InvalidArgument("hnsw: bad magic".into()));
        }
        let version = r.u16()?;
        if version != FORMAT_VERSION {
            return Err(GtvError::InvalidArgument(format!(
                "hnsw: unsupported format version {version}"
            )));
        }
        let _flags = r.u16()?;
        let n = r.u64()? as usize;
        let dim = r.u32()? as usize;
        let m = r.u32()? as usize;
        let m0 = r.u32()? as usize;
        let ef_construction = r.u32()? as usize;
        let ef_search = r.u32()? as usize;
        let max_level = r.u8()?;
        let metric = metric_from_code(r.u8()?)?;
        let entry = r.u32()?;
        let tombstone_count = r.u64()? as usize;

        let mut ids = Vec::with_capacity(n);
        for _ in 0..n {
            ids.push(r.u64()?);
        }
        let mut vectors = Vec::with_capacity(n * dim);
        for _ in 0..n * dim {
            vectors.push(r.f32()?);
        }
        let levels = r.take(n)?.to_vec();
        let mut node_offset = Vec::with_capacity(n + 1);
        for _ in 0..=n {
            node_offset.push(r.u32()?);
        }
        let neigh_len = *node_offset.last().unwrap() as usize;
        let mut neighbours = Vec::with_capacity(neigh_len);
        for _ in 0..neigh_len {
            neighbours.push(r.u32()?);
        }
        let mut block_start = Vec::with_capacity(n + 1);
        for _ in 0..=n {
            block_start.push(r.u32()?);
        }
        let block_len = *block_start.last().unwrap() as usize;
        let counts = r.take(block_len)?.to_vec();
        let deleted: Vec<bool> = r.take(n)?.iter().map(|&b| b != 0).collect();

        let mut pos_of = HashMap::with_capacity(n);
        for (i, &id) in ids.iter().enumerate() {
            pos_of.insert(id, i as u32);
        }

        Ok(HnswIndex {
            ids,
            vectors,
            dim,
            metric,
            levels,
            node_offset,
            neighbours,
            block_start,
            counts,
            entry,
            max_level,
            m,
            m0,
            ef_construction,
            ef_search,
            rng: SplitMix64::new(0x243F_6A88_85A3_08D3),
            deleted,
            tombstone_count,
            pos_of,
        })
    }
}

impl VectorIndex for HnswIndex {
    fn metric(&self) -> Metric {
        self.metric
    }

    fn dim(&self) -> usize {
        self.dim
    }

    fn search(
        &self,
        query: &[f32],
        k: usize,
        filter_mask: Option<&BooleanArray>,
    ) -> Result<Vec<VectorHit>> {
        self.search_with_report(query, k, self.ef_search, filter_mask)
            .map(|(h, _)| h)
    }
}

fn metric_code(metric: Metric) -> u8 {
    match metric {
        Metric::L2 => 0,
        Metric::Cosine => 1,
        Metric::Ip => 2,
    }
}

fn metric_from_code(code: u8) -> Result<Metric> {
    match code {
        0 => Ok(Metric::L2),
        1 => Ok(Metric::Cosine),
        2 => Ok(Metric::Ip),
        other => Err(GtvError::InvalidArgument(format!(
            "hnsw: unknown metric code {other}"
        ))),
    }
}

/// Minimal little-endian reader for [`HnswIndex::from_bytes`].
struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or_else(|| GtvError::InvalidArgument("hnsw: length overflow".into()))?;
        if end > self.bytes.len() {
            return Err(GtvError::InvalidArgument("hnsw: truncated payload".into()));
        }
        let s = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn f32(&mut self) -> Result<f32> {
        Ok(f32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn random_vectors(n: usize, dim: usize, seed: u64) -> (Vec<u64>, Vec<Vec<f32>>) {
        let mut rng = SplitMix64::new(seed);
        let ids: Vec<u64> = (0..n as u64).collect();
        let vectors = (0..n)
            .map(|_| (0..dim).map(|_| rng.next_f32()).collect())
            .collect();
        (ids, vectors)
    }

    #[test]
    fn insert_and_search_recovers_nearest() {
        let mut index = HnswIndex::new(4, 20, 20);
        index.insert(0, vec![0.0, 0.0]).unwrap();
        index.insert(1, vec![1.0, 1.0]).unwrap();
        index.insert(2, vec![5.0, 5.0]).unwrap();

        let got = index.search_knn(&[0.1, 0.1], 3, None).unwrap();
        assert_eq!(got.values().as_ref(), &[0, 1, 2]);
    }

    #[test]
    fn knn_respects_bitmask() {
        let mut index = HnswIndex::new(4, 20, 20);
        index.insert(0, vec![0.0, 0.0]).unwrap();
        index.insert(1, vec![1.0, 1.0]).unwrap();
        index.insert(2, vec![5.0, 5.0]).unwrap();

        let mask = BooleanArray::from(vec![false, true, true]);
        let got = index.search_knn(&[5.0, 5.0], 3, Some(&mask)).unwrap();
        assert_eq!(got.values().as_ref(), &[2, 1]);
    }

    #[test]
    fn build_matches_flat_recall_on_random_data() {
        use crate::FlatIndex;
        let (ids, vectors) = random_vectors(50, 4, 42);
        let flat = FlatIndex::new(ids.clone(), vectors.clone()).unwrap();
        let hnsw = HnswIndex::build(ids, vectors, 8, 40, 40).unwrap();

        let query = vec![0.5, 0.5, 0.5, 0.5];
        let exact = flat.search_knn(&query, 5, None).unwrap();
        let approx = hnsw.search_knn(&query, 5, None).unwrap();
        let exact: Vec<u64> = exact.values().to_vec();
        let approx: Vec<u64> = approx.values().to_vec();
        assert_eq!(exact[0], approx[0]);
    }

    #[test]
    fn cosine_reports_and_orders() {
        let ids = vec![0u64, 1, 2];
        let vectors = vec![vec![1.0f32, 0.0], vec![0.9, 0.1], vec![0.0, 1.0]];
        let index = HnswIndex::build_with_metric(ids, vectors, 4, 20, 20, Metric::Cosine).unwrap();
        assert_eq!(index.metric(), Metric::Cosine);
        let hits = index.search(&[1.0, 0.0], 3, None).unwrap();
        assert_eq!(hits[0].id, 0);
    }

    #[test]
    fn dimension_mismatch_is_typed() {
        let mut index = HnswIndex::new(4, 20, 20);
        index.insert(0, vec![0.0, 0.0]).unwrap();
        assert!(matches!(
            index.search(&[1.0], 1, None),
            Err(GtvError::DimensionMismatch { .. })
        ));
    }

    #[test]
    fn contiguous_layout_has_no_per_node_allocs() {
        let (ids, vectors) = random_vectors(64, 8, 7);
        let index = HnswIndex::build(ids, vectors.clone(), 8, 40, 40).unwrap();
        // vectors is exactly n*dim (single flat buffer).
        assert_eq!(index.vectors.len(), 64 * 8);
        // neighbour capacity matches m0 + levels*m per node.
        let expected: usize = (0..64)
            .map(|i| index.m0 + index.levels[i] as usize * index.m)
            .sum();
        assert_eq!(index.neighbours.len(), expected);
        // counts/blocks line up.
        let blocks: usize = (0..64).map(|i| index.levels[i] as usize + 1).sum();
        assert_eq!(index.counts.len(), blocks);
    }

    #[test]
    fn batch_matches_sequential() {
        let (ids, vectors) = random_vectors(200, 8, 11);
        let index = HnswIndex::build(ids, vectors.clone(), 16, 100, 100).unwrap();
        let queries: Vec<Vec<f32>> = vectors.iter().take(10).cloned().collect();
        let batch = index.search_batch(&queries, 5).unwrap();
        for (q, b) in queries.iter().zip(batch.iter()) {
            let seq = index.search(q, 5, None).unwrap();
            let a: Vec<u64> = b.iter().map(|h| h.id).collect();
            let s: Vec<u64> = seq.iter().map(|h| h.id).collect();
            assert_eq!(a, s);
        }
    }

    #[test]
    fn tombstone_hides_and_compaction_restores() {
        let (ids, vectors) = random_vectors(120, 6, 5);
        let mut index = HnswIndex::build(ids, vectors, 12, 80, 80).unwrap();
        let query = vec![0.3, 0.3, 0.3, 0.3, 0.3, 0.3];

        let before = index.search(&query, 10, None).unwrap();
        let victim = before[0].id;
        assert!(index.delete(victim));
        assert!(!index.delete(victim)); // idempotent
        assert_eq!(index.tombstone_count(), 1);
        assert_eq!(index.live_len(), 119);

        let after = index.search(&query, 10, None).unwrap();
        assert!(after.iter().all(|h| h.id != victim));

        let compacted = index.compact().unwrap();
        assert_eq!(compacted.live_len(), 119);
        assert!(compacted.search(&query, 10, None).unwrap().iter().all(|h| h.id != victim));
    }

    #[test]
    fn round_trip_preserves_results() {
        let (ids, vectors) = random_vectors(150, 5, 21);
        let index = HnswIndex::build(ids, vectors, 12, 80, 80).unwrap();
        let bytes = index.to_bytes();
        let restored = HnswIndex::from_bytes(&bytes).unwrap();
        assert_eq!(restored.len(), index.len());
        assert_eq!(restored.dim(), index.dim());
        assert_eq!(restored.metric(), index.metric());

        let query = vec![0.2, 0.4, 0.6, 0.8, 1.0];
        let a: Vec<u64> = index.search(&query, 10, None).unwrap().iter().map(|h| h.id).collect();
        let b: Vec<u64> = restored.search(&query, 10, None).unwrap().iter().map(|h| h.id).collect();
        assert_eq!(a, b);
        assert_eq!(index.checksum(), restored.checksum());
    }

    #[test]
    fn from_bytes_rejects_truncated() {
        let (ids, vectors) = random_vectors(20, 4, 1);
        let index = HnswIndex::build(ids, vectors, 8, 40, 40).unwrap();
        let bytes = index.to_bytes();
        assert!(HnswIndex::from_bytes(&bytes[..bytes.len() - 3]).is_err());
    }

    #[test]
    fn dynamic_ef_is_monotone_non_decreasing_recall() {
        use crate::FlatIndex;
        let (ids, vectors) = random_vectors(300, 8, 33);
        let flat = FlatIndex::new(ids.clone(), vectors.clone()).unwrap();
        let hnsw = HnswIndex::build(ids, vectors.clone(), 8, 60, 10).unwrap();
        let recall = |ef: usize| {
            let mut hit = 0usize;
            for q in vectors.iter().take(20) {
                let exact: Vec<u64> =
                    flat.search(q, 10, None).unwrap().iter().map(|h| h.id).collect();
                let approx: Vec<u64> = hnsw
                    .search_with_ef(q, 10, ef, None)
                    .unwrap()
                    .iter()
                    .map(|h| h.id)
                    .collect();
                hit += exact.iter().filter(|id| approx.contains(id)).count();
            }
            hit
        };
        let low = recall(10);
        let high = recall(100);
        assert!(high >= low, "recall(ef=100)={high} < recall(ef=10)={low}");
    }

    #[test]
    fn filtered_exact_matches_bruteforce() {
        let (ids, vectors) = random_vectors(80, 4, 9);
        let index = HnswIndex::build(ids.clone(), vectors.clone(), 8, 40, 40).unwrap();
        let mask = BooleanArray::from((0..80).map(|i| i % 3 == 0).collect::<Vec<bool>>());
        let query = vec![0.5, 0.5, 0.5, 0.5];
        let exact = index.filtered_exact(&query, 5, &mask).unwrap();
        let mut brute: Vec<(f32, u64)> = (0..80)
            .filter(|i| i % 3 == 0)
            .map(|i| (Metric::L2.distance(&query, &vectors[i]), ids[i]))
            .collect();
        brute.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        let want: Vec<u64> = brute.into_iter().take(5).map(|(_, id)| id).collect();
        let got: Vec<u64> = exact.iter().map(|h| h.id).collect();
        assert_eq!(got, want);
    }
}
