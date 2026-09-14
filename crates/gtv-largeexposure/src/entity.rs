//! Counterparty entities.

/// The three-way economic-sector split used by MA(BS)28 Part II (column 13).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum EconomicSector {
    Banks,
    Nbfis,
    Others,
}

impl EconomicSector {
    pub fn as_str(self) -> &'static str {
        match self {
            EconomicSector::Banks => "banks",
            EconomicSector::Nbfis => "NBFIs",
            EconomicSector::Others => "others",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().replace([' ', '-', '_'], "").as_str() {
            "banks" | "bank" => Some(EconomicSector::Banks),
            "nbfis" | "nbfi" | "nonbankfinancialinstitutions" => Some(EconomicSector::Nbfis),
            "others" | "other" => Some(EconomicSector::Others),
            _ => None,
        }
    }
}

/// Legal-entity kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EntityKind {
    Bank,
    Corporate,
    Sovereign,
    Fi,
    Spv,
    Individual,
}

impl EntityKind {
    pub fn as_str(self) -> &'static str {
        match self {
            EntityKind::Bank => "bank",
            EntityKind::Corporate => "corporate",
            EntityKind::Sovereign => "sovereign",
            EntityKind::Fi => "fi",
            EntityKind::Spv => "spv",
            EntityKind::Individual => "individual",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "bank" => Some(EntityKind::Bank),
            "corporate" | "corp" => Some(EntityKind::Corporate),
            "sovereign" => Some(EntityKind::Sovereign),
            "fi" | "financial" => Some(EntityKind::Fi),
            "spv" => Some(EntityKind::Spv),
            "individual" | "person" => Some(EntityKind::Individual),
            _ => None,
        }
    }
}

/// Reporting basis a counterparty belongs to (MA(BS)28 requires both).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Scope {
    Combined,
    Consolidated,
    Both,
}

impl Scope {
    pub fn as_str(self) -> &'static str {
        match self {
            Scope::Combined => "combined",
            Scope::Consolidated => "consolidated",
            Scope::Both => "both",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "combined" => Some(Scope::Combined),
            "consolidated" => Some(Scope::Consolidated),
            "both" | "*" => Some(Scope::Both),
            _ => None,
        }
    }

    /// Whether this entity is included under `basis`.
    pub fn includes(self, basis: Scope) -> bool {
        matches!(self, Scope::Both)
            || self == basis
            || matches!(basis, Scope::Both)
    }
}

/// A counterparty / legal entity.
#[derive(Debug, Clone, PartialEq)]
pub struct Entity {
    pub entity_id: String,
    pub kind: EntityKind,
    /// Raw economic-sector label (banks / NBFIs / others).
    pub economic_sector: Option<String>,
    pub country_code: Option<String>,
    pub rating_grade: Option<String>,
    /// Explicit connected-party flag (MA(BS)28 Part I / rule 85).
    pub is_connected: bool,
    /// Which connected-party paragraph applies (Part I column 14), if known.
    pub connected_paragraph: Option<String>,
    pub scope: Scope,
    pub is_g_sib: bool,
}

impl Entity {
    pub fn new(entity_id: impl Into<String>, kind: EntityKind) -> Self {
        Self {
            entity_id: entity_id.into(),
            kind,
            economic_sector: None,
            country_code: None,
            rating_grade: None,
            is_connected: false,
            connected_paragraph: None,
            scope: Scope::Both,
            is_g_sib: false,
        }
    }

    pub fn with_economic_sector(mut self, v: impl Into<String>) -> Self {
        self.economic_sector = Some(v.into());
        self
    }

    pub fn with_country(mut self, v: impl Into<String>) -> Self {
        self.country_code = Some(v.into());
        self
    }

    pub fn with_rating(mut self, v: impl Into<String>) -> Self {
        self.rating_grade = Some(v.into());
        self
    }

    pub fn connected(mut self) -> Self {
        self.is_connected = true;
        self
    }

    pub fn with_connected_paragraph(mut self, v: impl Into<String>) -> Self {
        self.connected_paragraph = Some(v.into());
        self
    }

    pub fn with_scope(mut self, scope: Scope) -> Self {
        self.scope = scope;
        self
    }

    pub fn g_sib(mut self) -> Self {
        self.is_g_sib = true;
        self
    }

    /// The three-way economic sector (unknown labels fall back to `Others`).
    pub fn sector(&self) -> EconomicSector {
        self.economic_sector
            .as_deref()
            .and_then(EconomicSector::parse)
            .unwrap_or(EconomicSector::Others)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sector_and_scope_parsing() {
        assert_eq!(EconomicSector::parse("NBFIs"), Some(EconomicSector::Nbfis));
        assert_eq!(EconomicSector::parse("banks"), Some(EconomicSector::Banks));
        assert_eq!(EconomicSector::parse("???"), None);
        assert!(Scope::Both.includes(Scope::Combined));
        assert!(Scope::Combined.includes(Scope::Combined));
        assert!(!Scope::Combined.includes(Scope::Consolidated));
    }

    #[test]
    fn entity_builder_and_sector_fallback() {
        let e = Entity::new("E1", EntityKind::Corporate)
            .with_economic_sector("NBFIs")
            .with_country("HK")
            .connected()
            .g_sib();
        assert_eq!(e.sector(), EconomicSector::Nbfis);
        assert!(e.is_connected && e.is_g_sib);
        let e2 = Entity::new("E2", EntityKind::Bank);
        assert_eq!(e2.sector(), EconomicSector::Others); // unset -> Others
    }
}
