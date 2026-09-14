//! Top-N ranking.

/// A ranked item.
#[derive(Debug, Clone, PartialEq)]
pub struct Ranked {
    pub id: String,
    pub value: f64,
}

/// Rank `(id, value)` pairs by value descending, ties broken by id ascending,
/// and keep the top `n`.
pub fn rank_top_n<I>(items: I, n: usize) -> Vec<Ranked>
where
    I: IntoIterator<Item = (String, f64)>,
{
    let mut v: Vec<Ranked> = items
        .into_iter()
        .map(|(id, value)| Ranked { id, value })
        .collect();
    v.sort_by(|a, b| {
        b.value
            .partial_cmp(&a.value)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.id.cmp(&b.id))
    });
    v.truncate(n);
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sorts_desc_with_deterministic_ties() {
        let items = vec![
            ("B".to_string(), 10.0),
            ("A".to_string(), 10.0),
            ("C".to_string(), 30.0),
            ("D".to_string(), 5.0),
        ];
        let top = rank_top_n(items, 3);
        assert_eq!(top[0], Ranked { id: "C".into(), value: 30.0 });
        assert_eq!(top[1].id, "A"); // tie 10.0 -> id ascending
        assert_eq!(top[2].id, "B");
        assert_eq!(top.len(), 3);
    }
}
