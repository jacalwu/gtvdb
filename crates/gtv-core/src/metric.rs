//! Distance metric contract shared by every vector index.
//!
//! Every variant is expressed so that **lower means closer**:
//!
//! * [`Metric::L2`] — squared Euclidean distance `sum((a-b)^2)`.
//! * [`Metric::Cosine`] — cosine distance `1 - cos(a,b)`. Indexes normalize
//!   their rows (and the query) so the kernel reduces to `1 - dot(a,b)`.
//! * [`Metric::Ip`] — inner product, returned as **negative** inner product so
//!   that the uniform "lower = closer" ordering still holds.
//!
//! # Inner product is not a metric
//!
//! `Ip` violates the triangle inequality, so graph (HNSW) and cell (IVF)
//! navigation can lose recall on raw, un-normalized vectors. Prefer
//! [`Metric::Cosine`] for embedding search. If raw `Ip` is required, see the
//! design note in `doc/prod_p1_design.md` (norm-augmentation reduces MIPS to
//! L2) — a Recall@K measurement is mandatory before trusting it.

/// Distance metric fixed at index build time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Metric {
    /// Squared Euclidean distance: `sum((a-b)^2)`. Lower = closer.
    L2,
    /// Cosine distance `1 - cos(a,b)`. Lower = closer.
    Cosine,
    /// Negative inner product `-dot(a,b)`. Lower = closer.
    ///
    /// Not a metric; see the module docs.
    Ip,
}

/// Roadmap alias (`prod_p1.md` / `prod_p2.md` call it `DistanceMetric`).
pub type DistanceMetric = Metric;

impl Default for Metric {
    fn default() -> Self {
        Metric::L2
    }
}

impl Metric {
    /// Canonical distance; **lower means closer** for every variant.
    ///
    /// This is the reference (unoptimized) implementation: it handles raw,
    /// un-normalized vectors. Index kernels use normalized rows for
    /// [`Metric::Cosine`] and dispatch to SIMD kernels for speed.
    #[inline]
    pub fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        match self {
            Metric::L2 => squared_l2(a, b),
            Metric::Cosine => 1.0 - cosine_similarity(a, b),
            Metric::Ip => -dot(a, b),
        }
    }

    /// Whether the index should unit-normalize rows (and queries) up front.
    ///
    /// Only [`Metric::Cosine`] does: after normalization `1 - cos(a,b)` becomes
    /// `1 - dot(a,b)`, which is one fused multiply-add per element.
    #[inline]
    pub fn requires_normalization(&self) -> bool {
        matches!(self, Metric::Cosine)
    }

    /// Normalize `v` in place when this metric requires it. Zero vectors are
    /// left untouched (their cosine is undefined; `cosine_similarity` treats
    /// them as orthogonal).
    #[inline]
    pub fn normalize_in_place(&self, v: &mut [f32]) {
        if self.requires_normalization() {
            let n = norm(v);
            if n > 0.0 && n.is_finite() {
                for x in v.iter_mut() {
                    *x /= n;
                }
            }
        }
    }

    /// Lower-case name for SQL / manifests / CLI flags.
    pub fn as_str(&self) -> &'static str {
        match self {
            Metric::L2 => "l2",
            Metric::Cosine => "cosine",
            Metric::Ip => "dot",
        }
    }

    /// Parse a user-supplied metric name (case-insensitive).
    ///
    /// Accepts `l2`, `euclidean`, `cosine`, `cos`, `dot`, `ip`, `inner_product`.
    pub fn parse(s: &str) -> Option<Metric> {
        match s.trim().to_ascii_lowercase().as_str() {
            "l2" | "euclidean" => Some(Metric::L2),
            "cosine" | "cos" => Some(Metric::Cosine),
            "dot" | "ip" | "inner_product" | "innerproduct" => Some(Metric::Ip),
            _ => None,
        }
    }
}

impl std::fmt::Display for Metric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Dot product (scalar reference).
#[inline]
pub fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

/// Squared Euclidean distance (scalar reference).
#[inline]
pub fn squared_l2(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
}

/// Euclidean norm (scalar reference).
#[inline]
pub fn norm(a: &[f32]) -> f32 {
    dot(a, a).sqrt()
}

/// Unit-normalize `v` in place (no-op for zero vectors).
#[inline]
pub fn normalize(v: &mut [f32]) {
    let n = norm(v);
    if n > 0.0 && n.is_finite() {
        for x in v.iter_mut() {
            *x /= n;
        }
    }
}

/// Cosine similarity in `[-1, 1]`; zero vectors are treated as orthogonal.
#[inline]
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let (na, nb) = (norm(a), norm(b));
    if na == 0.0 || nb == 0.0 {
        return 0.0;
    }
    dot(a, b) / (na * nb)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn l2_orders_near_first() {
        let q = [0.0, 0.0];
        assert!(Metric::L2.distance(&q, &[1.0, 0.0]) < Metric::L2.distance(&q, &[2.0, 0.0]));
    }

    #[test]
    fn cosine_is_scale_invariant() {
        let a = [1.0, 2.0, 3.0];
        let b = [2.0, 4.0, 6.0];
        assert!((Metric::Cosine.distance(&a, &b) - 0.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_orthogonal_is_one() {
        assert!((Metric::Cosine.distance(&[1.0, 0.0], &[0.0, 1.0]) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn ip_larger_product_is_closer() {
        let q = [1.0, 1.0];
        let near = [2.0, 2.0]; // dot = 4 -> dist = -4
        let far = [1.0, 0.0]; // dot = 1 -> dist = -1
        assert!(Metric::Ip.distance(&q, &near) < Metric::Ip.distance(&q, &far));
    }

    #[test]
    fn normalization_makes_cosine_equal_dot() {
        let mut a = [3.0, 4.0];
        let mut b = [1.0, 0.0];
        Metric::Cosine.normalize_in_place(&mut a);
        Metric::Cosine.normalize_in_place(&mut b);
        let cos_dist = 1.0 - dot(&a, &b);
        assert!((Metric::Cosine.distance(&a, &b) - cos_dist).abs() < 1e-6);
    }

    #[test]
    fn parse_names() {
        assert_eq!(Metric::parse("L2"), Some(Metric::L2));
        assert_eq!(Metric::parse("cosine"), Some(Metric::Cosine));
        assert_eq!(Metric::parse("ip"), Some(Metric::Ip));
        assert_eq!(Metric::parse("nope"), None);
    }
}
