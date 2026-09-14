//! Temporal aggregates for incremental recomputation: a Fenwick tree
//! (range-add / point-query) and a lazy segment tree (range-add / range-max)
//! over a fixed day axis.

/// A day axis mapping dates to dense indices `[0, len)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeAxis {
    origin: i64,
    len: usize,
}

impl TimeAxis {
    pub fn new(origin: i64, len: usize) -> Self {
        Self { origin, len }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn origin(&self) -> i64 {
        self.origin
    }

    #[inline]
    pub fn index(&self, date: i64) -> Option<usize> {
        if date < self.origin {
            return None;
        }
        let i = (date - self.origin) as usize;
        (i < self.len).then_some(i)
    }

    #[inline]
    pub fn date(&self, index: usize) -> i64 {
        self.origin + index as i64
    }
}

/// Fenwick tree over a difference array: `range_add(l, r, delta)` and
/// `point_query(i)` in `O(log n)`.
#[derive(Debug, Clone)]
pub struct Fenwick {
    n: usize,
    tree: Vec<f64>,
}

impl Fenwick {
    pub fn new(n: usize) -> Self {
        Self {
            n,
            tree: vec![0.0; n + 1],
        }
    }

    fn bit_add(&mut self, mut i: usize, delta: f64) {
        // 1-indexed
        while i <= self.n {
            self.tree[i] += delta;
            i += i & i.wrapping_neg();
        }
    }

    fn bit_sum(&self, mut i: usize) -> f64 {
        let mut s = 0.0;
        while i > 0 {
            s += self.tree[i];
            i -= i & i.wrapping_neg();
        }
        s
    }

    /// Add `delta` to every index in `[l, r)`.
    pub fn range_add(&mut self, l: usize, r: usize, delta: f64) {
        if l >= r || l >= self.n {
            return;
        }
        let r = r.min(self.n);
        self.bit_add(l + 1, delta);
        if r < self.n {
            self.bit_add(r + 1, -delta);
        }
    }

    /// Value at index `i`.
    pub fn point_query(&self, i: usize) -> f64 {
        if i >= self.n {
            return 0.0;
        }
        self.bit_sum(i + 1)
    }
}

/// Lazy segment tree: `range_add(l, r, delta)` and `range_max(l, r)` in
/// `O(log n)`.
#[derive(Debug, Clone)]
pub struct SegTree {
    n: usize,
    max: Vec<f64>,
    lazy: Vec<f64>,
}

impl SegTree {
    pub fn new(n: usize) -> Self {
        Self {
            n,
            // additive range-max: the baseline is 0 (no exposure / no data),
            // so `0 + delta == delta` (unlike NEG_INFINITY + delta).
            max: vec![0.0; 4 * n.max(1)],
            lazy: vec![0.0; 4 * n.max(1)],
        }
    }

    fn push(&mut self, node: usize) {
        let z = self.lazy[node];
        if z != 0.0 {
            for child in [node * 2, node * 2 + 1] {
                self.max[child] += z;
                self.lazy[child] += z;
            }
            self.lazy[node] = 0.0;
        }
    }

    fn add(&mut self, node: usize, nl: usize, nr: usize, l: usize, r: usize, v: f64) {
        if r <= nl || nr <= l {
            return;
        }
        if l <= nl && nr <= r {
            self.max[node] += v;
            self.lazy[node] += v;
            return;
        }
        self.push(node);
        let mid = (nl + nr) / 2;
        self.add(node * 2, nl, mid, l, r, v);
        self.add(node * 2 + 1, mid, nr, l, r, v);
        self.max[node] = self.max[node * 2].max(self.max[node * 2 + 1]);
    }

    /// Add `delta` to every index in `[l, r)`.
    pub fn range_add(&mut self, l: usize, r: usize, delta: f64) {
        if self.n == 0 || l >= r || l >= self.n {
            return;
        }
        let r = r.min(self.n);
        self.add(1, 0, self.n, l, r, delta);
    }

    fn query(&mut self, node: usize, nl: usize, nr: usize, l: usize, r: usize) -> f64 {
        if r <= nl || nr <= l {
            return f64::NEG_INFINITY;
        }
        if l <= nl && nr <= r {
            return self.max[node];
        }
        self.push(node);
        let mid = (nl + nr) / 2;
        let a = self.query(node * 2, nl, mid, l, r);
        let b = self.query(node * 2 + 1, mid, nr, l, r);
        a.max(b)
    }

    /// Maximum over `[l, r)` (`0.0`-safe empty/degenerate range).
    pub fn range_max(&mut self, l: usize, r: usize) -> f64 {
        if self.n == 0 || l >= r || l >= self.n {
            return 0.0;
        }
        let r = r.min(self.n);
        let m = self.query(1, 0, self.n, l, r);
        if m == f64::NEG_INFINITY {
            0.0
        } else {
            m
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_axis_maps_dates() {
        let ax = TimeAxis::new(100, 10);
        assert_eq!(ax.index(100), Some(0));
        assert_eq!(ax.index(109), Some(9));
        assert_eq!(ax.index(99), None);
        assert_eq!(ax.index(110), None);
        assert_eq!(ax.date(3), 103);
    }

    #[test]
    fn fenwick_range_add_point_query() {
        let mut f = Fenwick::new(10);
        f.range_add(2, 5, 3.0); // [2,5)
        for i in 0..10 {
            let expect = if (2..5).contains(&i) { 3.0 } else { 0.0 };
            assert_eq!(f.point_query(i), expect, "i={i}");
        }
        f.range_add(0, 10, -1.0);
        assert_eq!(f.point_query(0), -1.0);
        assert_eq!(f.point_query(3), 2.0);
    }

    #[test]
    fn segtree_range_add_range_max() {
        let mut s = SegTree::new(10);
        s.range_add(2, 6, 5.0);
        assert_eq!(s.range_max(0, 1), 0.0);
        assert_eq!(s.range_max(0, 10), 5.0);
        assert_eq!(s.range_max(2, 3), 5.0);
        assert_eq!(s.range_max(6, 10), 0.0);
        s.range_add(4, 8, 3.0);
        assert_eq!(s.range_max(4, 5), 8.0);
        assert_eq!(s.range_max(2, 4), 5.0);
        s.range_add(0, 10, -10.0);
        assert_eq!(s.range_max(4, 5), -2.0);
    }

    #[test]
    fn segtree_matches_naive_after_random_updates() {
        // deterministic pseudo-random updates
        let mut s = SegTree::new(32);
        let mut naive = vec![0.0f64; 32];
        let mut x: u64 = 12345;
        for _ in 0..200 {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let l = (x >> 33) as usize % 32;
            let r = l + ((x >> 40) as usize % (32 - l)) + 1;
            let v = ((x >> 20) % 21) as f64 - 10.0;
            s.range_add(l, r, v);
            for x in &mut naive[l..r] {
                *x += v;
            }
            let ql = (x >> 5) as usize % 32;
            let qr = ql + ((x >> 8) as usize % (32 - ql)) + 1;
            let expected = naive[ql..qr].iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            assert!((s.range_max(ql, qr) - expected).abs() < 1e-9);
        }
    }
}
