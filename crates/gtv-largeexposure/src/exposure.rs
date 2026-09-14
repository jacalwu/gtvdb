//! Exposure events: the six MA(BS)28 components, before/after CRM.

/// Which exposure measure to aggregate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Measure {
    /// Before CRM (includes transferred-in indirect exposures).
    BeforeCrm,
    /// After recognized CRM.
    AfterCrm,
    /// Exempted before CRM (rule 48(1)).
    Exempted,
}

impl Measure {
    pub const ALL: [Measure; 3] = [Measure::BeforeCrm, Measure::AfterCrm, Measure::Exempted];

    pub fn as_str(self) -> &'static str {
        match self {
            Measure::BeforeCrm => "before_crm",
            Measure::AfterCrm => "after_crm",
            Measure::Exempted => "exempted",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().replace([' ', '-'], "_").as_str() {
            "before_crm" | "before" | "gross" => Some(Measure::BeforeCrm),
            "after_crm" | "after" | "net" => Some(Measure::AfterCrm),
            "exempted" | "exempt" => Some(Measure::Exempted),
            _ => None,
        }
    }
}

/// The six MA(BS)28 exposure components.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct ExposureMeasure {
    pub on_balance: f64,
    pub trading_book: f64,
    pub off_balance: f64,
    pub default_risk: f64,
    pub additional_risk: f64,
    pub indirect: f64,
}

impl ExposureMeasure {
    pub fn zero() -> Self {
        Self::default()
    }

    pub fn on_balance(mut self, v: f64) -> Self {
        self.on_balance = v;
        self
    }

    pub fn trading_book(mut self, v: f64) -> Self {
        self.trading_book = v;
        self
    }

    pub fn off_balance(mut self, v: f64) -> Self {
        self.off_balance = v;
        self
    }

    pub fn default_risk(mut self, v: f64) -> Self {
        self.default_risk = v;
        self
    }

    pub fn additional_risk(mut self, v: f64) -> Self {
        self.additional_risk = v;
        self
    }

    pub fn indirect(mut self, v: f64) -> Self {
        self.indirect = v;
        self
    }

    /// Sum of the six components.
    pub fn total(&self) -> f64 {
        self.on_balance
            + self.trading_book
            + self.off_balance
            + self.default_risk
            + self.additional_risk
            + self.indirect
    }

    pub fn add(&mut self, other: &Self) {
        self.on_balance += other.on_balance;
        self.trading_book += other.trading_book;
        self.off_balance += other.off_balance;
        self.default_risk += other.default_risk;
        self.additional_risk += other.additional_risk;
        self.indirect += other.indirect;
    }

    pub fn scale(&self, k: f64) -> Self {
        Self {
            on_balance: self.on_balance * k,
            trading_book: self.trading_book * k,
            off_balance: self.off_balance * k,
            default_risk: self.default_risk * k,
            additional_risk: self.additional_risk * k,
            indirect: self.indirect * k,
        }
    }
}

/// Whether an event is the original booking, an adjustment, or a correction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    Orig,
    Adjustment,
    Correction,
}

impl EventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            EventKind::Orig => "orig",
            EventKind::Adjustment => "adjustment",
            EventKind::Correction => "correction",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "orig" | "original" => Some(EventKind::Orig),
            "adjustment" | "adjust" => Some(EventKind::Adjustment),
            "correction" | "correct" => Some(EventKind::Correction),
            _ => None,
        }
    }
}

/// An append-only exposure event over a business-time interval.
///
/// `crm_reduction` is the haircut-adjusted recognized-CRM amount reducing the
/// exposure; `after_crm = max(0, before_crm − crm_reduction)`.
#[derive(Debug, Clone, PartialEq)]
pub struct ExposureEvent {
    pub event_id: String,
    pub entity_id: String,
    pub measure: ExposureMeasure,
    pub crm_reduction: f64,
    pub currency: String,
    /// Net short position → disregarded (MA(BS)28 §14).
    pub net_short: bool,
    /// Exempted exposure (rule 48(1)).
    pub exempt: bool,
    pub exemption_provision: Option<String>,
    /// Deduction under rule 57 (memorandum item).
    pub deduction: f64,
    pub kind: EventKind,
    pub ref_event_id: Option<String>,
    pub business_from: i64,
    pub business_to: i64,
    pub system_from: i64,
}

impl ExposureEvent {
    pub fn new(
        event_id: impl Into<String>,
        entity_id: impl Into<String>,
        measure: ExposureMeasure,
        business_from: i64,
        business_to: i64,
    ) -> Self {
        Self {
            event_id: event_id.into(),
            entity_id: entity_id.into(),
            measure,
            crm_reduction: 0.0,
            currency: "HKD".to_string(),
            net_short: false,
            exempt: false,
            exemption_provision: None,
            deduction: 0.0,
            kind: EventKind::Orig,
            ref_event_id: None,
            business_from,
            business_to,
            system_from: 0,
        }
    }

    pub fn with_crm_reduction(mut self, v: f64) -> Self {
        self.crm_reduction = v;
        self
    }

    pub fn with_currency(mut self, v: impl Into<String>) -> Self {
        self.currency = v.into();
        self
    }

    pub fn net_short(mut self) -> Self {
        self.net_short = true;
        self
    }

    pub fn exempt(mut self, provision: impl Into<String>) -> Self {
        self.exempt = true;
        self.exemption_provision = Some(provision.into());
        self
    }

    pub fn with_deduction(mut self, v: f64) -> Self {
        self.deduction = v;
        self
    }

    pub fn with_kind(mut self, kind: EventKind, ref_event_id: impl Into<String>) -> Self {
        self.kind = kind;
        self.ref_event_id = Some(ref_event_id.into());
        self
    }

    pub fn with_system_from(mut self, v: i64) -> Self {
        self.system_from = v;
        self
    }

    #[inline]
    pub fn active_at(&self, t: i64) -> bool {
        self.business_from <= t && t < self.business_to
    }

    /// Before-CRM exposure (net-short disregarded).
    pub fn before_crm(&self) -> f64 {
        if self.net_short {
            0.0
        } else {
            self.measure.total()
        }
    }

    /// After-CRM exposure.
    pub fn after_crm(&self) -> f64 {
        (self.before_crm() - self.crm_reduction).max(0.0)
    }

    /// Exposure under a given measure.
    pub fn value(&self, measure: Measure) -> f64 {
        match measure {
            Measure::BeforeCrm => self.before_crm(),
            Measure::AfterCrm => self.after_crm(),
            Measure::Exempted => {
                if self.exempt {
                    self.before_crm()
                } else {
                    0.0
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn components_sum_and_crm() {
        let m = ExposureMeasure::zero()
            .on_balance(100.0)
            .off_balance(20.0)
            .default_risk(5.0);
        assert_eq!(m.total(), 125.0);
        let e = ExposureEvent::new("X", "E1", m, 0, 100).with_crm_reduction(25.0);
        assert_eq!(e.before_crm(), 125.0);
        assert_eq!(e.after_crm(), 100.0);
        // CRM cannot make the exposure negative
        assert_eq!(e.clone().with_crm_reduction(999.0).after_crm(), 0.0);
    }

    #[test]
    fn net_short_and_exempt_measures() {
        let m = ExposureMeasure::zero().on_balance(100.0);
        let ns = ExposureEvent::new("X", "E1", m, 0, 10).net_short();
        assert_eq!(ns.before_crm(), 0.0);
        let ex = ExposureEvent::new("Y", "E1", m, 0, 10).exempt("rule_48(1)(a)");
        assert_eq!(ex.value(Measure::Exempted), 100.0);
        assert_eq!(ex.value(Measure::BeforeCrm), 100.0);
        let normal = ExposureEvent::new("Z", "E1", m, 0, 10);
        assert_eq!(normal.value(Measure::Exempted), 0.0);
    }

    #[test]
    fn active_interval_is_half_open() {
        let e = ExposureEvent::new(
            "X",
            "E1",
            ExposureMeasure::zero(),
            10,
            20,
        );
        assert!(!e.active_at(9) && e.active_at(10) && e.active_at(19) && !e.active_at(20));
    }
}
