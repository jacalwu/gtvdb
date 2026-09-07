//! Technical-indicator WindowUDFs (function2.md Phase A §1.1 / §1.3 vwap).
//!
//! All operators are **causal / trailing-window** over the (pre-sorted)
//! partition — same convention as the existing `mavg`-style kernels — so they
//! compose with `OVER (PARTITION BY symbol ORDER BY ts)`. Rows before the
//! window fills (`NaN` warm-up) are returned as `NaN` rather than spurious
//! values. Kernels live in `gtv_array::quant`; the wrappers are thin.
//!
//! SQL surface (function2.md Phase A):
//! ```sql
//! SELECT t, ema(close,5)          OVER (ORDER BY t),
//!        rsi(close,14)            OVER (ORDER BY t),
//!        macd_hist(close,12,26,9) OVER (ORDER BY t),
//!        atr(high,low,close,14)   OVER (ORDER BY t),
//!        boll_up(close,20,2.0)    OVER (ORDER BY t),
//!        vwap(high,low,close,volume,20) OVER (PARTITION BY symbol ORDER BY t)
//! FROM bars;
//! ```

use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::{as_primitive_array, ArrayRef, Float64Array};
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
use datafusion::logical_expr::{
    PartitionEvaluator, Signature, Volatility, WindowUDF, WindowUDFImpl,
};
use datafusion::scalar::ScalarValue;

fn f64_values(array: &ArrayRef) -> &[f64] {
    as_primitive_array::<Float64Type>(array.as_ref()).values().as_ref()
}

fn int_value(array: &ArrayRef) -> usize {
    ScalarValue::try_from_array(array, 0)
        .ok()
        .and_then(|s| s.cast_to(&DataType::Int64).ok())
        .and_then(|s| match s {
            ScalarValue::Int64(Some(n)) => Some(n.max(1) as usize),
            _ => None,
        })
        .unwrap_or(14)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum IndOp {
    Ema,
    Rsi,
    MacdDif,
    MacdDea,
    MacdHist,
    Atr,
    BollMid,
    BollUp,
    BollLo,
    Vwap,
    // function3.md Wave A
    EmaSlope,
    EmaAccel,
    BbWidth,
    BbMidSlope,
    MacdDistance,
    MacdSlope,
    AtrRatio,
    Hv,
    HvRatio,
    VolSpike,
    Obv,
    ObvSlope,
    VwapDev,
    Hh,
    Ll,
    BigGreen,
    BigRed,
    LowShadow,
    UpShadow,
    RegimeTrend,
    RegimeVol,
    RegimeEvent,
    GapOp,
    // function3.md Wave B (cross-sectional / index-relative)
    XRank,
    RsIndex,
    BetaIndex,
}

impl IndOp {
    /// (number of float columns, number of trailing Int64 literal params)
    fn layout(self) -> (usize, usize) {
        match self {
            IndOp::Ema | IndOp::Rsi | IndOp::EmaSlope | IndOp::EmaAccel => (1, 1),
            IndOp::BbWidth | IndOp::BbMidSlope => (1, 1),
            IndOp::MacdDif | IndOp::MacdDea | IndOp::MacdHist => (1, 3),
            IndOp::MacdDistance | IndOp::MacdSlope => (1, 2),
            IndOp::Atr => (3, 1),
            IndOp::AtrRatio => (3, 2),
            IndOp::BollMid | IndOp::BollUp | IndOp::BollLo => (1, 2),
            IndOp::Vwap | IndOp::VwapDev => (4, 1),
            IndOp::Hv => (1, 2),
            IndOp::HvRatio | IndOp::RegimeVol => (1, 3),
            IndOp::VolSpike | IndOp::Hh | IndOp::Ll => (1, 1),
            IndOp::Obv | IndOp::GapOp => (2, 0),
            IndOp::ObvSlope => (2, 1),
            IndOp::BigGreen | IndOp::BigRed | IndOp::LowShadow | IndOp::UpShadow => (4, 0),
            IndOp::RegimeTrend => (1, 2),
            IndOp::RegimeEvent => (5, 1),
            IndOp::XRank => (1, 0),
            IndOp::RsIndex => (2, 1),
            IndOp::BetaIndex => (2, 1),
        }
    }

    fn compute(self, v: &[&[f64]], p: &[usize]) -> Vec<f64> {
        use gtv_array::quant as q;
        match self {
            IndOp::Ema => q::ema(v[0], p[0]),
            IndOp::Rsi => q::rsi(v[0], p[0]),
            IndOp::MacdDif | IndOp::MacdDea | IndOp::MacdHist => {
                let (dif, dea, hist) = q::macd(v[0], p[0], p[1], p[2]);
                match self {
                    IndOp::MacdDif => dif,
                    IndOp::MacdDea => dea,
                    _ => hist,
                }
            }
            IndOp::Atr => q::atr(v[0], v[1], v[2], p[0]),
            IndOp::BollMid | IndOp::BollUp | IndOp::BollLo => {
                let (mid, up, lo) = q::bollinger(v[0], p[0], p[1] as f64);
                match self {
                    IndOp::BollMid => mid,
                    IndOp::BollUp => up,
                    _ => lo,
                }
            }
            IndOp::Vwap => q::vwap(v[0], v[1], v[2], v[3], p[0]),
            // ---- function3.md Wave A ----
            IndOp::EmaSlope => q::ema_slope(v[0], p[0]),
            IndOp::EmaAccel => q::ema_accel(v[0], p[0]),
            IndOp::BbWidth => q::boll_width(v[0], p[0]),
            IndOp::BbMidSlope => q::boll_mid_slope(v[0], p[0]),
            IndOp::MacdDistance => q::macd_distance(v[0], p[0], p[1]),
            IndOp::MacdSlope => q::macd_slope(v[0], p[0], p[1]),
            IndOp::AtrRatio => q::atr_ratio(v[0], v[1], v[2], p[0], p[1]),
            IndOp::Hv => q::hv(v[0], p[0], p[1]),
            IndOp::HvRatio => q::hv_ratio(v[0], p[0], p[1], p[2]),
            IndOp::VolSpike => q::vol_spike(v[0], p[0]),
            IndOp::Obv => q::obv(v[0], v[1]),
            IndOp::ObvSlope => q::obv_slope(v[0], v[1], p[0]),
            IndOp::VwapDev => q::vwap_dev(v[0], v[1], v[2], v[3], p[0]),
            IndOp::Hh => q::hh(v[0], p[0]),
            IndOp::Ll => q::ll(v[0], p[0]),
            IndOp::BigGreen => q::candle_green(v[0], v[1], v[2], v[3]),
            IndOp::BigRed => q::candle_red(v[0], v[1], v[2], v[3]),
            IndOp::LowShadow => q::candle_lower_shadow(v[0], v[1], v[2], v[3]),
            IndOp::UpShadow => q::candle_upper_shadow(v[0], v[1], v[2], v[3]),
            IndOp::RegimeTrend => q::regime_trend(v[0], p[0], p[1]),
            IndOp::RegimeVol => q::regime_vol(v[0], p[0], p[1], p[2]),
            IndOp::RegimeEvent => q::regime_event(v[0], v[1], v[2], v[3], v[4], p[0]),
            IndOp::GapOp => q::gap(v[0], v[1]),
            IndOp::XRank => q::xrank(v[0]),
            IndOp::RsIndex => {
                // relative strength: stock n-bar momentum minus index n-bar momentum
                let s = q::momentum(v[0], p[0]);
                let m = q::momentum(v[1], p[0]);
                s.into_iter().zip(m).map(|(a, b)| a - b).collect()
            }
            IndOp::BetaIndex => q::rolling_beta(v[0], v[1], p[0]),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct IndicatorWindowUdf {
    name: &'static str,
    signature: Signature,
    op: IndOp,
}

impl IndicatorWindowUdf {
    fn new(name: &'static str, op: IndOp) -> Self {
        let (ncols, nint) = op.layout();
        let mut args = Vec::with_capacity(ncols + nint);
        for _ in 0..ncols {
            args.push(DataType::Float64);
        }
        for _ in 0..nint {
            args.push(DataType::Int64);
        }
        Self {
            name,
            signature: Signature::exact(args, Volatility::Immutable),
            op,
        }
    }
}

impl WindowUDFImpl for IndicatorWindowUdf {
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn partition_evaluator(
        &self,
        _args: PartitionEvaluatorArgs,
    ) -> DfResult<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(IndicatorEvaluator { op: self.op }))
    }
    fn field(&self, field_args: WindowUDFFieldArgs) -> DfResult<FieldRef> {
        Ok(Field::new(field_args.name(), DataType::Float64, true).into())
    }
}

#[derive(Debug)]
struct IndicatorEvaluator {
    op: IndOp,
}

impl PartitionEvaluator for IndicatorEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], _num_rows: usize) -> DfResult<ArrayRef> {
        let (ncols, nint) = self.op.layout();
        if values.len() < ncols + nint {
            return Ok(Arc::new(Float64Array::from(Vec::<f64>::new())));
        }
        let n = values[0].len();
        let cols: Vec<&[f64]> = values[..ncols].iter().map(f64_values).collect();
        let params: Vec<usize> = values[ncols..ncols + nint].iter().map(int_value).collect();
        let out = self.op.compute(&cols, &params);
        debug_assert_eq!(out.len(), n);
        Ok(Arc::new(Float64Array::from(out)))
    }
}

pub fn indicator_window_udfs() -> Vec<WindowUDF> {
    vec![
        WindowUDF::from(IndicatorWindowUdf::new("ema", IndOp::Ema)),
        WindowUDF::from(IndicatorWindowUdf::new("rsi", IndOp::Rsi)),
        WindowUDF::from(IndicatorWindowUdf::new("macd_dif", IndOp::MacdDif)),
        WindowUDF::from(IndicatorWindowUdf::new("macd_dea", IndOp::MacdDea)),
        WindowUDF::from(IndicatorWindowUdf::new("macd_hist", IndOp::MacdHist)),
        WindowUDF::from(IndicatorWindowUdf::new("atr", IndOp::Atr)),
        WindowUDF::from(IndicatorWindowUdf::new("boll_mid", IndOp::BollMid)),
        WindowUDF::from(IndicatorWindowUdf::new("boll_up", IndOp::BollUp)),
        WindowUDF::from(IndicatorWindowUdf::new("boll_lo", IndOp::BollLo)),
        WindowUDF::from(IndicatorWindowUdf::new("vwap", IndOp::Vwap)),
        // function3.md Wave A
        WindowUDF::from(IndicatorWindowUdf::new("ema_slope", IndOp::EmaSlope)),
        WindowUDF::from(IndicatorWindowUdf::new("ema_accel", IndOp::EmaAccel)),
        WindowUDF::from(IndicatorWindowUdf::new("bb_width", IndOp::BbWidth)),
        WindowUDF::from(IndicatorWindowUdf::new("bb_mid_slope", IndOp::BbMidSlope)),
        WindowUDF::from(IndicatorWindowUdf::new("macd_distance", IndOp::MacdDistance)),
        WindowUDF::from(IndicatorWindowUdf::new("macd_slope", IndOp::MacdSlope)),
        WindowUDF::from(IndicatorWindowUdf::new("atr_ratio", IndOp::AtrRatio)),
        WindowUDF::from(IndicatorWindowUdf::new("hv", IndOp::Hv)),
        WindowUDF::from(IndicatorWindowUdf::new("hv_ratio", IndOp::HvRatio)),
        WindowUDF::from(IndicatorWindowUdf::new("vol_spike", IndOp::VolSpike)),
        WindowUDF::from(IndicatorWindowUdf::new("obv", IndOp::Obv)),
        WindowUDF::from(IndicatorWindowUdf::new("obv_slope", IndOp::ObvSlope)),
        WindowUDF::from(IndicatorWindowUdf::new("vwap_dev", IndOp::VwapDev)),
        WindowUDF::from(IndicatorWindowUdf::new("hh", IndOp::Hh)),
        WindowUDF::from(IndicatorWindowUdf::new("ll", IndOp::Ll)),
        WindowUDF::from(IndicatorWindowUdf::new("big_green", IndOp::BigGreen)),
        WindowUDF::from(IndicatorWindowUdf::new("big_red", IndOp::BigRed)),
        WindowUDF::from(IndicatorWindowUdf::new("long_lower_shadow", IndOp::LowShadow)),
        WindowUDF::from(IndicatorWindowUdf::new("long_upper_shadow", IndOp::UpShadow)),
        WindowUDF::from(IndicatorWindowUdf::new("regime_trend", IndOp::RegimeTrend)),
        WindowUDF::from(IndicatorWindowUdf::new("regime_vol", IndOp::RegimeVol)),
        WindowUDF::from(IndicatorWindowUdf::new("regime_event", IndOp::RegimeEvent)),
        WindowUDF::from(IndicatorWindowUdf::new("gap_up", IndOp::GapOp)),
        WindowUDF::from(IndicatorWindowUdf::new("gap_down", IndOp::GapOp)),
        // function3.md Wave B
        WindowUDF::from(IndicatorWindowUdf::new("xrank", IndOp::XRank)),
        WindowUDF::from(IndicatorWindowUdf::new("rank_cs", IndOp::XRank)),
        WindowUDF::from(IndicatorWindowUdf::new("rs_index", IndOp::RsIndex)),
        WindowUDF::from(IndicatorWindowUdf::new("rs", IndOp::RsIndex)),
        WindowUDF::from(IndicatorWindowUdf::new("beta_index", IndOp::BetaIndex)),
        WindowUDF::from(IndicatorWindowUdf::new("beta", IndOp::BetaIndex)),
    ]
}

#[cfg(test)]
mod tests {
    use gtv_array::quant as q;

    #[test]
    fn rsi_is_bounded_and_directional() {
        let up: Vec<f64> = (0..100).map(|i| 100.0 + i as f64 * 1.0).collect();
        let r = q::rsi(&up, 14);
        for i in 14..r.len() {
            assert_eq!(r[i], 100.0, "pure uptrend -> RSI 100 at {i}");
        }
        let down: Vec<f64> = (0..100).map(|i| 500.0 - i as f64 * 1.0).collect();
        let r2 = q::rsi(&down, 14);
        for i in 14..r2.len() {
            assert!(r2[i] >= 0.0 && r2[i] <= 100.0);
        }
        assert_eq!(r2[99], 0.0, "pure downtrend -> RSI 0");
        assert!(r2[..14].iter().all(|x| x.is_nan()), "warm-up must be NaN");
    }

    #[test]
    fn bollinger_and_ema_on_constant_series() {
        let c = vec![5.0f64; 30];
        let (mid, up, lo) = q::bollinger(&c, 20, 2.0);
        assert_eq!(mid[19], 5.0);
        assert!((up[19] - 5.0).abs() < 1e-12 && (lo[19] - 5.0).abs() < 1e-12);
        assert!(mid[..19].iter().all(|x| x.is_nan()));
        let e = q::ema(&c, 5);
        assert_eq!(e[4], 5.0);
        assert!((e[29] - 5.0).abs() < 1e-12);
        assert!(e[..4].iter().all(|x| x.is_nan()));
    }

    #[test]
    fn atr_and_macd_on_trending_series() {
        let h: Vec<f64> = (0..80).map(|i| i as f64 + 1.0).collect();
        let l: Vec<f64> = (0..80).map(|i| i as f64 - 0.5).collect();
        let c: Vec<f64> = (0..80).map(|i| i as f64 + 0.5).collect();
        let a = q::atr(&h, &l, &c, 14);
        assert!((a[70] - 1.5).abs() < 1e-9, "steady range 1.5 -> ATR 1.5");
        let (dif, dea, hist) = q::macd(&c, 12, 26, 9);
        assert!(dif[60] > 0.0, "linear uptrend -> positive MACD dif");
        assert!(dea[60].is_finite() && hist[60].is_finite());
        assert!(dif[..25].iter().all(|x| x.is_nan()));
    }

    #[test]
    fn vwap_equals_typical_price_when_constant() {
        let (h, l, c, v) = (vec![11.0f64; 40], vec![9.0f64; 40], vec![10.0f64; 40], vec![100.0f64; 40]);
        let w = q::vwap(&h, &l, &c, &v, 10);
        assert!((w[39] - 10.0).abs() < 1e-12);
        assert!(w[..9].iter().all(|x| x.is_nan()));
    }

    #[test]
    fn bs_rho_sign_matches_option_type() {
        let call = q::bs_rho("C", 100.0, 100.0, 0.5, 0.03, 0.2);
        let put = q::bs_rho("P", 100.0, 100.0, 0.5, 0.03, 0.2);
        assert!(call > 0.0, "call rho positive");
        assert!(put < 0.0, "put rho negative");
    }
}

#[cfg(test)]
mod wave_a_tests {
    use gtv_array::quant as q;

    fn series(step: f64, n: usize) -> Vec<f64> {
        (0..n).map(|i| 100.0 + i as f64 * step).collect()
    }

    #[test]
    fn ema_slope_and_accel_sign_on_trend() {
        let x = series(1.0, 80);
        let s = q::ema_slope(&x, 10);
        assert!(s[60] > 0.0, "uptrend -> positive ema slope");
        let a = q::ema_accel(&x, 10);
        assert!(a[70].abs() < 0.05, "linear trend -> ~0 accel, got {}", a[70]);
    }

    #[test]
    fn bb_width_and_mid_slope_flat_when_constant() {
        let c = vec![10.0f64; 60];
        assert!((q::boll_width(&c, 20)[40]).abs() < 1e-12);
        assert!((q::boll_mid_slope(&c, 20)[40]).abs() < 1e-12);
    }

    #[test]
    fn hv_and_vol_spike_detect_activity() {
        let flat = vec![100.0f64; 40];
        assert_eq!(q::hv(&flat, 10, 252)[30], 0.0);
        let mut v = vec![100.0f64; 40];
        v[25] = 500.0; // volume spike
        let sp = q::vol_spike(&v, 10);
        assert!(sp[25] > 2.0, "spike 500 vs sma~140 -> >2, got {}", sp[25]);
        assert!(sp[20] < 0.2);
    }

    #[test]
    fn obv_accumulates_obv_slope_and_gap() {
        let c = series(1.0, 40);
        let v = vec![10.0f64; 40];
        let b = q::obv(&c, &v);
        assert!(b[30] > b[10], "rising closes -> obv rising");
        assert!(q::obv_slope(&c, &v, 5)[30] > 0.0);
        // gap: open 5% above prior close
        let c2 = vec![100.0f64; 30];
        let mut o = vec![100.0f64; 30];
        o[10] = 105.0;
        let g = q::gap(&o, &c2);
        assert!((g[10] - 0.05).abs() < 1e-9);
    }

    #[test]
    fn candles_and_hh_ll_bounds() {
        let (o, h, l, c) = (vec![10.0; 30], vec![12.0; 30], vec![9.0; 30], vec![11.5; 30]);
        for f in [q::candle_green, q::candle_red, q::candle_lower_shadow, q::candle_upper_shadow] {
            let out = f(&o, &h, &l, &c);
            assert!(out.iter().all(|x| (0.0..=1.0).contains(x)), "candle in [0,1]");
        }
        let hi = series(1.0, 40);
        let hh = q::hh(&hi, 5);
        assert!(hh[39] > 0.5, "strictly rising -> new highs");
    }
}

#[cfg(test)]
mod wave_b_tests {
    use gtv_array::quant as q;

    #[test]
    fn xrank_ranks_and_ties() {
        let x = vec![10.0, 30.0, 20.0, 20.0, 5.0];
        let r = q::xrank(&x);
        // sorted: 5,10,20,20,30 -> pct ranks 0, .25, .625, .625, 1 (tie avg of 2&3)
        assert!((r[0] - 0.25).abs() < 1e-12);
        assert!((r[1] - 1.0).abs() < 1e-12);
        assert!((r[2] - 0.625).abs() < 1e-12);
        assert!((r[3] - 0.625).abs() < 1e-12);
        assert!((r[4] - 0.0).abs() < 1e-12);
    }

    #[test]
    fn beta_identical_series_is_one() {
        // stock == 2*index levels => returns equal => beta 1
        let x: Vec<f64> = (0..80).map(|i| 100.0 + i as f64 * 0.5).collect();
        let y: Vec<f64> = x.iter().map(|v| v * 2.0).collect();
        let b = q::rolling_beta(&x, &y, 20);
        assert!((b[70] - 1.0).abs() < 1e-6, "identical returns -> beta 1");
    }
}
