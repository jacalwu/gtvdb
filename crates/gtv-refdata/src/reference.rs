//! Master data (accounts / customers / instruments / counterparties) and
//! effective-dated reference data (curves / calendars / currencies /
//! jurisdictions).

use std::collections::BTreeMap;

use crate::error::{DanglingReference, RefDataError};
use crate::hierarchy::EffectiveRange;

/// Master-data entity kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum MasterKind {
    Account,
    Customer,
    Instrument,
    Counterparty,
}

impl MasterKind {
    pub const ALL: [MasterKind; 4] = [
        MasterKind::Account,
        MasterKind::Customer,
        MasterKind::Instrument,
        MasterKind::Counterparty,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            MasterKind::Account => "account",
            MasterKind::Customer => "customer",
            MasterKind::Instrument => "instrument",
            MasterKind::Counterparty => "counterparty",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "account" => Some(MasterKind::Account),
            "customer" => Some(MasterKind::Customer),
            "instrument" => Some(MasterKind::Instrument),
            "counterparty" => Some(MasterKind::Counterparty),
            _ => None,
        }
    }
}

/// One effective-dated master-data version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MasterRecord {
    pub kind: MasterKind,
    pub id: String,
    pub effective: EffectiveRange,
    pub attributes: BTreeMap<String, String>,
}

impl MasterRecord {
    pub fn new(kind: MasterKind, id: impl Into<String>, effective: EffectiveRange) -> Self {
        Self {
            kind,
            id: id.into(),
            effective,
            attributes: BTreeMap::new(),
        }
    }

    pub fn with_attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }

    pub fn attribute(&self, key: &str) -> Option<&str> {
        self.attributes.get(key).map(String::as_str)
    }

    #[inline]
    pub fn active_at(&self, t: i64) -> bool {
        self.effective.contains(t)
    }
}

/// A referential constraint: `kind.field` must name an existing `target` entity
/// that is itself effective at the same instant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceConstraint {
    pub kind: MasterKind,
    pub field: String,
    pub target: MasterKind,
}

impl ReferenceConstraint {
    pub fn new(kind: MasterKind, field: impl Into<String>, target: MasterKind) -> Self {
        Self {
            kind,
            field: field.into(),
            target,
        }
    }
}

/// A dangling reference found by [`MasterData::check_references`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceViolation {
    pub kind: MasterKind,
    pub id: String,
    pub field: String,
    pub target: MasterKind,
    pub target_id: String,
    pub as_of: i64,
}

/// Append-only, effective-dated master data.
///
/// The key `(kind, id)` is unique **over time**: a new version may only be
/// inserted where it does not overlap an existing version's interval.
#[derive(Debug, Default, Clone)]
pub struct MasterData {
    records: Vec<MasterRecord>,
    constraints: Vec<ReferenceConstraint>,
}

impl MasterData {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn records(&self) -> &[MasterRecord] {
        &self.records
    }

    pub fn constraints(&self) -> &[ReferenceConstraint] {
        &self.constraints
    }

    /// Insert one version, enforcing the time-unique key.
    pub fn put(&mut self, record: MasterRecord) -> Result<(), RefDataError> {
        if record.kind.as_str().is_empty() || record.id.trim().is_empty() {
            return Err(RefDataError::EmptyId);
        }
        let overlaps = self.records.iter().any(|r| {
            r.kind == record.kind
                && r.id == record.id
                && r.effective.overlaps(&record.effective)
        });
        if overlaps {
            return Err(RefDataError::DuplicateKey {
                kind: record.kind.as_str().to_string(),
                key: record.id,
            });
        }
        self.records.push(record);
        Ok(())
    }

    /// The version active at `as_of`, if any.
    pub fn get(&self, kind: MasterKind, id: &str, as_of: i64) -> Option<&MasterRecord> {
        self.records
            .iter()
            .find(|r| r.kind == kind && r.id == id && r.active_at(as_of))
    }

    /// All versions of one key, ordered by effective start.
    pub fn versions(&self, kind: MasterKind, id: &str) -> Vec<&MasterRecord> {
        let mut out: Vec<&MasterRecord> = self
            .records
            .iter()
            .filter(|r| r.kind == kind && r.id == id)
            .collect();
        out.sort_by_key(|r| (r.effective.from, r.effective.to));
        out
    }

    /// All records of `kind` active at `as_of`, ordered by id.
    pub fn records_at(&self, kind: MasterKind, as_of: i64) -> Vec<&MasterRecord> {
        let mut out: Vec<&MasterRecord> = self
            .records
            .iter()
            .filter(|r| r.kind == kind && r.active_at(as_of))
            .collect();
        out.sort_by(|a, b| a.id.cmp(&b.id));
        out
    }

    pub fn add_reference(&mut self, constraint: ReferenceConstraint) {
        self.constraints.push(constraint);
    }

    /// Check every referential constraint at each record's effective start.
    pub fn check_references(&self) -> Vec<ReferenceViolation> {
        let mut out = Vec::new();
        for record in &self.records {
            for c in self
                .constraints
                .iter()
                .filter(|c| c.kind == record.kind)
            {
                let Some(target_id) = record.attribute(&c.field) else {
                    continue;
                };
                let as_of = record.effective.from;
                if self.get(c.target, target_id, as_of).is_none() {
                    out.push(ReferenceViolation {
                        kind: record.kind,
                        id: record.id.clone(),
                        field: c.field.clone(),
                        target: c.target,
                        target_id: target_id.to_string(),
                        as_of,
                    });
                }
            }
        }
        out.sort_by(|a, b| {
            (a.kind, &a.id, &a.field, &a.target_id).cmp(&(b.kind, &b.id, &b.field, &b.target_id))
        });
        out
    }

    /// Like [`check_references`](Self::check_references) but returns the first
    /// violation as an error.
    pub fn check_references_strict(&self) -> Result<(), RefDataError> {
        match self.check_references().into_iter().next() {
            None => Ok(()),
            Some(v) => Err(RefDataError::DanglingReference(Box::new(DanglingReference {
                kind: v.kind.as_str().to_string(),
                id: v.id,
                field: v.field,
                target: v.target.as_str().to_string(),
                target_id: v.target_id,
                as_of: v.as_of,
            }))),
        }
    }
}

/// One effective-dated reference value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceEntry {
    pub domain: String,
    pub key: String,
    pub effective: EffectiveRange,
    pub value: String,
}

impl ReferenceEntry {
    #[inline]
    pub fn active_at(&self, t: i64) -> bool {
        self.effective.contains(t)
    }
}

/// Effective-dated reference data (curve / calendar / currency / jurisdiction).
///
/// Key `(domain, key)` is time-unique, mirroring [`MasterData`].
#[derive(Debug, Default, Clone)]
pub struct ReferenceData {
    entries: Vec<ReferenceEntry>,
}

impl ReferenceData {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn entries(&self) -> &[ReferenceEntry] {
        &self.entries
    }

    /// Insert one effective-dated value, enforcing the time-unique key.
    pub fn set(
        &mut self,
        domain: impl Into<String>,
        key: impl Into<String>,
        effective: EffectiveRange,
        value: impl Into<String>,
    ) -> Result<(), RefDataError> {
        let domain = domain.into();
        let key = key.into();
        if domain.trim().is_empty() || key.trim().is_empty() {
            return Err(RefDataError::EmptyId);
        }
        let overlaps = self.entries.iter().any(|e| {
            e.domain == domain && e.key == key && e.effective.overlaps(&effective)
        });
        if overlaps {
            return Err(RefDataError::DuplicateKey {
                kind: domain,
                key,
            });
        }
        self.entries.push(ReferenceEntry {
            domain,
            key,
            effective,
            value: value.into(),
        });
        Ok(())
    }

    pub fn get(&self, domain: &str, key: &str, as_of: i64) -> Option<&str> {
        self.entries
            .iter()
            .find(|e| e.domain == domain && e.key == key && e.active_at(as_of))
            .map(|e| e.value.as_str())
    }

    /// All versions of one key, ordered by effective start.
    pub fn versions(&self, domain: &str, key: &str) -> Vec<&ReferenceEntry> {
        let mut out: Vec<&ReferenceEntry> = self
            .entries
            .iter()
            .filter(|e| e.domain == domain && e.key == key)
            .collect();
        out.sort_by_key(|e| (e.effective.from, e.effective.to));
        out
    }

    /// All keys active in a domain at `as_of`, ordered by key.
    pub fn keys_at(&self, domain: &str, as_of: i64) -> Vec<&str> {
        let mut seen: Vec<&str> = self
            .entries
            .iter()
            .filter(|e| e.domain == domain && e.active_at(as_of))
            .map(|e| e.key.as_str())
            .collect();
        seen.sort_unstable();
        seen.dedup();
        seen
    }

    /// Distinct domains, sorted.
    pub fn domains(&self) -> Vec<&str> {
        let mut out: Vec<&str> = self.entries.iter().map(|e| e.domain.as_str()).collect();
        out.sort_unstable();
        out.dedup();
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hierarchy::EffectiveRange;

    fn open(from: i64) -> EffectiveRange {
        EffectiveRange::from_now_on(from)
    }

    #[test]
    fn master_key_is_unique_over_time() {
        let mut md = MasterData::new();
        md.put(MasterRecord::new(MasterKind::Customer, "C1", EffectiveRange::new(0, 100).unwrap()))
            .unwrap();
        // non-overlapping version is fine
        md.put(MasterRecord::new(MasterKind::Customer, "C1", open(100)))
            .unwrap();
        assert_eq!(md.versions(MasterKind::Customer, "C1").len(), 2);
        // overlapping version is rejected
        assert!(matches!(
            md.put(MasterRecord::new(MasterKind::Customer, "C1", EffectiveRange::new(50, 150).unwrap())),
            Err(RefDataError::DuplicateKey { .. })
        ));
        assert_eq!(md.get(MasterKind::Customer, "C1", 50).unwrap().effective.from, 0);
        assert_eq!(md.get(MasterKind::Customer, "C1", 150).unwrap().effective.from, 100);
    }

    #[test]
    fn referrals_are_checked_against_effective_targets() {
        let mut md = MasterData::new();
        md.put(
            MasterRecord::new(MasterKind::Customer, "C1", open(0))
                .with_attribute("segment", "retail"),
        )
        .unwrap();
        md.put(
            MasterRecord::new(MasterKind::Account, "A1", open(0))
                .with_attribute("customer", "C1"),
        )
        .unwrap();
        md.put(
            MasterRecord::new(MasterKind::Account, "A2", open(0))
                .with_attribute("customer", "GHOST"),
        )
        .unwrap();
        md.add_reference(ReferenceConstraint::new(
            MasterKind::Account,
            "customer",
            MasterKind::Customer,
        ));

        let violations = md.check_references();
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].id, "A2");
        assert_eq!(violations[0].target_id, "GHOST");
        assert!(md.check_references_strict().is_err());
    }

    #[test]
    fn referral_respects_effective_dating() {
        let mut md = MasterData::new();
        // customer only exists from t=100
        md.put(MasterRecord::new(MasterKind::Customer, "C1", open(100)))
            .unwrap();
        md.put(
            MasterRecord::new(MasterKind::Account, "A1", EffectiveRange::new(0, 50).unwrap())
                .with_attribute("customer", "C1"),
        )
        .unwrap();
        md.add_reference(ReferenceConstraint::new(
            MasterKind::Account,
            "customer",
            MasterKind::Customer,
        ));
        // A1 is effective at t=0, when C1 is not yet active -> dangling
        assert_eq!(md.check_references().len(), 1);
    }

    #[test]
    fn reference_values_are_effective_dated() {
        let mut rd = ReferenceData::new();
        rd.set("curve", "USD.5Y", EffectiveRange::new(0, 100).unwrap(), "0.02")
            .unwrap();
        rd.set("curve", "USD.5Y", open(100), "0.03").unwrap();
        assert_eq!(rd.get("curve", "USD.5Y", 50), Some("0.02"));
        assert_eq!(rd.get("curve", "USD.5Y", 150), Some("0.03"));
        assert_eq!(rd.get("curve", "USD.5Y", 100), Some("0.03"));
        assert_eq!(rd.versions("curve", "USD.5Y").len(), 2);
        assert!(matches!(
            rd.set("curve", "USD.5Y", EffectiveRange::new(50, 150).unwrap(), "0.99"),
            Err(RefDataError::DuplicateKey { .. })
        ));
    }

    #[test]
    fn reference_domains_and_keys() {
        let mut rd = ReferenceData::new();
        rd.set("calendar", "HKEX", open(0), "HK").unwrap();
        rd.set("currency", "HKD", open(0), "344").unwrap();
        rd.set("currency", "USD", open(0), "840").unwrap();
        assert_eq!(rd.domains(), vec!["calendar", "currency"]);
        assert_eq!(rd.keys_at("currency", 0), vec!["HKD", "USD"]);
        assert!(rd.get("calendar", "NYSE", 0).is_none());
    }

    #[test]
    fn empty_identifiers_are_rejected() {
        let mut md = MasterData::new();
        assert!(matches!(
            md.put(MasterRecord::new(MasterKind::Account, "  ", open(0))),
            Err(RefDataError::EmptyId)
        ));
        let mut rd = ReferenceData::new();
        assert!(matches!(
            rd.set("", "k", open(0), "v"),
            Err(RefDataError::EmptyId)
        ));
    }
}
