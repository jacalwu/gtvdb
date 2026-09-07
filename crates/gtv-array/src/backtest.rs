//! Single-asset backtest simulation kernel (function2.md Phase A §1.4, phase 1).
//!
//! Position state machine over a (time, price, signal) series:
//! * signal `+1/-1/0` → long / short / flat; a position is opened at the close
//!   of the signal bar (same convention as the forecast pipeline) and closed at
//!   the close of the exit bar.
//! * exits, checked bar-by-bar at the close: stop-loss, take-profit,
//!   opposite-signal flip, or end-of-series. Conservative order: stop first,
//!   then take-profit, then signal.
//! * after any exit, re-entry is blocked until a flat (`0`) bar appears
//!   (prevents stop-→-instant-re-entry churn).
//! * `cost_bps` is the round-trip cost in basis points, deducted once per
//!   trade (`net = gross - cost_bps/1e4`).
//!
//! Portfolio / rebalance is phase 2 (not here). Metrics are computed on a
//! compounding per-trade equity curve (same basis as the forecast paper-sim).

/// Backtest parameters. `stop_loss`/`take_profit` in fractional returns
/// (0.05 == 5%); a non-positive value disables that rule.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BtParams {
    pub cost_bps: f64,
    pub stop_loss: f64,
    pub take_profit: f64,
}

impl Default for BtParams {
    fn default() -> Self {
        Self { cost_bps: 0.0, stop_loss: 0.0, take_profit: 0.0 }
    }
}

/// Exit reasons.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ExitReason {
    Signal = 0,
    StopLoss = 1,
    TakeProfit = 2,
    End = 3,
}

impl ExitReason {
    pub fn label(self) -> &'static str {
        match self {
            ExitReason::Signal => "signal",
            ExitReason::StopLoss => "stop_loss",
            ExitReason::TakeProfit => "take_profit",
            ExitReason::End => "end",
        }
    }
}

/// One closed round-trip trade.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BtFill {
    pub entry_ts: i64,
    pub exit_ts: i64,
    pub side: i8, // +1 long / -1 short
    pub entry_px: f64,
    pub exit_px: f64,
    pub gross: f64,
    pub net: f64,
    pub cost: f64,
    pub reason: ExitReason,
}

/// Simulate `sig` (values clamped to -1/0/1) against `px` over matching `ts`.
/// Entry on the rising edge of a signal (`flat -> long/short`); exit on
/// stop-loss / take-profit / signal-to-flat / signal flip. After any exit the
/// position stays flat until the next flat-then-nonzero edge, so stop- or
/// flip-outs never instantly re-enter while the signal is still active.
pub fn simulate(ts: &[i64], px: &[f64], sig: &[i8], p: &BtParams) -> Vec<BtFill> {
    let mut fills = Vec::new();
    if ts.is_empty() || px.len() != ts.len() || sig.len() != ts.len() {
        return fills;
    }
    let tot_cost = (p.cost_bps / 1e4).max(0.0);
    let mut side: i8 = 0;
    let mut entry_i = 0usize;
    let mut entry_px = 0.0f64;

    for i in 0..ts.len() {
        if side == 0 {
            let s = sig[i].clamp(-1, 1);
            if s == 0 {
                continue;
            }
            if i > 0 && sig[i - 1] != 0 {
                continue; // wait for a rising edge (flat -> signal)
            }
            side = s;
            entry_i = i;
            entry_px = px[i];
            continue;
        }
        // in position: check exits at bar i close
        let side_ret = if side > 0 { px[i] / entry_px - 1.0 } else { entry_px / px[i] - 1.0 };
        let mut reason = ExitReason::Signal;
        let mut exit = false;
        if p.stop_loss > 0.0 && side_ret <= -p.stop_loss {
            reason = ExitReason::StopLoss;
            exit = true;
        } else if p.take_profit > 0.0 && side_ret >= p.take_profit {
            reason = ExitReason::TakeProfit;
            exit = true;
        } else if sig[i] == 0 || sig[i] == -side {
            reason = ExitReason::Signal;
            exit = true;
        }
        if exit {
            fills.push(BtFill {
                entry_ts: ts[entry_i],
                exit_ts: ts[i],
                side,
                entry_px,
                exit_px: px[i],
                gross: side_ret,
                net: side_ret - tot_cost,
                cost: tot_cost,
                reason,
            });
            side = 0;
        }
    }
    // close any open position at the last bar
    if side != 0 {
        let last = ts.len() - 1;
        let side_ret = if side > 0 { px[last] / entry_px - 1.0 } else { entry_px / px[last] - 1.0 };
        fills.push(BtFill {
            entry_ts: ts[entry_i],
            exit_ts: ts[last],
            side,
            entry_px,
            exit_px: px[last],
            gross: side_ret,
            net: side_ret - tot_cost,
            cost: tot_cost,
            reason: ExitReason::End,
        });
    }
    fills
}

/// Backtest report metrics on a compounding per-trade equity curve.
#[derive(Debug, Clone, Copy, Default)]
pub struct BtReport {
    pub n_trades: i64,
    pub win_rate: f64,
    pub total_ret: f64,
    pub ann_ret: f64,
    pub mean_net: f64,
    pub sharpe: f64,
    pub maxdd: f64,
    pub cost_total: f64,
}

pub fn report(fills: &[BtFill]) -> BtReport {
    if fills.is_empty() {
        return BtReport::default();
    }
    let nets: Vec<f64> = fills.iter().map(|f| f.net).collect();
    let wins = nets.iter().filter(|r| **r > 0.0).count();
    let mut eq = 1.0f64;
    let mut peak = 1.0f64;
    let mut mdd = 0.0f64;
    for r in &nets {
        eq *= 1.0 + r;
        if eq > peak {
            peak = eq;
        }
        let dd = (peak - eq) / peak;
        if dd > mdd {
            mdd = dd;
        }
    }
    let total = eq - 1.0;
    let n = nets.len() as f64;
    let mean = nets.iter().sum::<f64>() / n;
    let var = nets.iter().map(|r| (r - mean) * (r - mean)).sum::<f64>() / n;
    let sd = var.sqrt();
    let span_ns = fills.last().unwrap().exit_ts - fills.first().unwrap().entry_ts;
    let years = span_ns as f64 / (365.25 * 86_400_000_000_000.0);
    let ann = if years > 0.0 && total > -1.0 {
        (1.0 + total).powf(1.0 / years) - 1.0
    } else {
        f64::NAN
    };
    BtReport {
        n_trades: fills.len() as i64,
        win_rate: wins as f64 / n,
        total_ret: total,
        ann_ret: ann,
        mean_net: mean,
        sharpe: if sd > 0.0 { mean / sd } else { f64::NAN },
        maxdd: mdd,
        cost_total: fills.iter().map(|f| f.cost).sum(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn series(step: f64) -> Vec<f64> {
        (0..40).map(|i| 100.0 + i as f64 * step).collect()
    }
    fn ts_of(len: usize) -> Vec<i64> {
        (0..len as i64).map(|i| i * 86_400_000_000_000).collect()
    }
    fn flat(len: usize) -> Vec<i8> {
        vec![0; len]
    }

    #[test]
    fn uptrend_long_profits_until_signal_flip() {
        let px = series(1.0);
        let ts = ts_of(px.len());
        // long from bar 5, flip flat-ish at 20 then stay long; flip short at 30
        let mut sig = flat(px.len());
        for i in 5..30 {
            sig[i] = 1;
        }
        sig[30] = -1;
        let fills = simulate(&ts, &px, &sig, &BtParams::default());
        assert_eq!(fills.len(), 1);
        let f = fills[0];
        assert_eq!(f.side, 1);
        assert_eq!(f.reason, ExitReason::Signal);
        assert!(f.gross > 0.0, "long on uptrend profits");
        assert!((f.net - f.gross).abs() < 1e-12);
    }

    #[test]
    fn stop_loss_and_take_profit_fire() {
        // whipsaw down after entry: stop at -5%
        let px = (0..20).map(|i| if i == 0 { 100.0 } else { 100.0 - i as f64 * 6.0 }).collect::<Vec<f64>>();
        let ts = ts_of(px.len());
        let mut sig = flat(px.len());
        for i in 1..sig.len() {
            sig[i] = 1;
        }
        let fills = simulate(&ts, &px, &sig, &BtParams { stop_loss: 0.05, ..Default::default() });
        assert!(!fills.is_empty());
        assert_eq!(fills[0].reason, ExitReason::StopLoss);
        assert!(fills[0].net <= -0.04, "stopped near -5%: {}", fills[0].net);
        // uptrend with take-profit 5%
        let px2 = series(3.0);
        let ts2 = ts_of(px2.len());
        let mut sig2 = flat(px2.len());
        for i in 5..sig2.len() {
            sig2[i] = 1;
        }
        let fills2 = simulate(&ts2, &px2, &sig2, &BtParams { take_profit: 0.05, ..Default::default() });
        assert!(!fills2.is_empty());
        assert_eq!(fills2[0].reason, ExitReason::TakeProfit);
        assert!(fills2[0].gross >= 0.05 - 1e-9);
    }

    #[test]
    fn short_and_cost_reduce_net() {
        let px = series(-1.0); // falling
        let ts = ts_of(px.len());
        let mut sig = flat(px.len());
        for i in 3..25 {
            sig[i] = -1;
        }
        let fills = simulate(&ts, &px, &sig, &BtParams { cost_bps: 20.0, ..Default::default() });
        assert_eq!(fills.len(), 1);
        let f = fills[0];
        assert_eq!(f.side, -1);
        assert!(f.gross > 0.0);
        assert!((f.net - (f.gross - 0.002)).abs() < 1e-9, "20bps deducted");
        let r = report(&fills);
        assert_eq!(r.n_trades, 1);
        assert!(r.win_rate > 0.99);
    }

    #[test]
    fn report_compounds_and_tracks_maxdd() {
        let fills = vec![
            BtFill { entry_ts: 0, exit_ts: 1, side: 1, entry_px: 100.0, exit_px: 110.0, gross: 0.10, net: 0.10, cost: 0.0, reason: ExitReason::TakeProfit },
            BtFill { entry_ts: 2, exit_ts: 3, side: 1, entry_px: 100.0, exit_px: 50.0, gross: -0.5, net: -0.5, cost: 0.0, reason: ExitReason::StopLoss },
        ];
        let r = report(&fills);
        assert_eq!(r.n_trades, 2);
        assert!(r.win_rate > 0.49);
        assert!((r.total_ret - (1.1 * 0.5 - 1.0)).abs() < 1e-9);
        assert!(r.maxdd > 0.0);
    }
}

// ---------------------------------------------------------------------------
// Portfolio backtest (function2.md §1.4 phase 2) — equal-weight long-only
// rebalanced at each grid close. Short signals are ignored (HK retail usually
// cannot short easily); cash holds the rest. Turnover costs in bps.
// ---------------------------------------------------------------------------

/// One symbol's sorted (time, price, signal) series.
#[derive(Debug, Clone, Default)]
pub struct PfSeries {
    pub ts: Vec<i64>,
    pub close: Vec<f64>,
    pub sig: Vec<i8>,
}

/// One portfolio grid row.
#[derive(Debug, Clone, Copy)]
pub struct PfDay {
    pub ts: i64,
    pub nav: f64,
    pub ret: f64,
    pub n_active: usize,
    pub turnover: f64,
}

/// Equal-weight long-only portfolio over the union time grid.
///
/// Per grid time: (1) realise the previous weights' return using each symbol's
/// close at-or-before that time (gaps carry forward -> 0 return), (2) rebalance
/// at the close to equal weight over the symbols whose signal is currently
/// `> 0` (rising edge not required here — active names are those signalled),
/// charging turnover `cost_bps` on `sum |target - mtm weight|`.
pub fn portfolio_sim(series: &[PfSeries], cost_bps: f64) -> Vec<PfDay> {
    // union grid
    let mut grid: Vec<i64> = series.iter().flat_map(|s| s.ts.iter().copied()).collect();
    grid.sort_unstable();
    grid.dedup();
    if grid.is_empty() {
        return Vec::new();
    }
    let cost = (cost_bps / 1e4).max(0.0);
    let nsym = series.len();
    let mut ptr = vec![0usize; nsym];
    let mut has_prev = vec![false; nsym];
    let mut prev_close = vec![0.0f64; nsym];
    let mut sig_eff = vec![0i8; nsym];
    let mut w = vec![0.0f64; nsym];
    let mut nav = 1.0f64;
    let mut out = Vec::with_capacity(grid.len());
    let mut sym_r = vec![0.0f64; nsym];
    for &g in &grid {
        // 1) realise returns using effective (carried) closes
        let mut r_port = 0.0f64;
        for s in 0..nsym {
            sym_r[s] = 0.0;
            while ptr[s] < series[s].ts.len() && series[s].ts[ptr[s]] <= g {
                let c = series[s].close[ptr[s]];
                if has_prev[s] && prev_close[s] > 0.0 {
                    let r = c / prev_close[s] - 1.0;
                    sym_r[s] = r;
                    r_port += w[s] * r;
                }
                prev_close[s] = c;
                has_prev[s] = true;
                sig_eff[s] = series[s].sig[ptr[s]].clamp(-1, 1);
                ptr[s] += 1;
            }
        }
        let mut n_active = 0usize;
        for s in 0..nsym {
            if sig_eff[s] > 0 && has_prev[s] {
                n_active += 1;
            }
        }
        // mark-to-market weights after the day's returns
        let mut mtm = w.clone();
        if r_port.abs() > 1e-15 {
            for s in 0..nsym {
                mtm[s] = w[s] * (1.0 + sym_r[s]) / (1.0 + r_port);
            }
        }
        let mut turnover = 0.0f64;
        let target = if n_active > 0 { 1.0 / n_active as f64 } else { 0.0 };
        for s in 0..nsym {
            let t = if sig_eff[s] > 0 && has_prev[s] { target } else { 0.0 };
            turnover += (t - mtm[s]).abs();
            w[s] = t;
        }
        let fee = turnover * cost;
        nav *= 1.0 + r_port;
        nav -= fee;
        out.push(PfDay { ts: g, nav, ret: r_port, n_active, turnover });
    }
    out
}

/// Portfolio report on the per-grid-row equity curve.
#[derive(Debug, Clone, Copy, Default)]
pub struct PfReport {
    pub n_bars: i64,
    pub final_nav: f64,
    pub total_ret: f64,
    pub ann_ret: f64,
    pub vol_daily: f64,
    pub sharpe_daily: f64,
    pub maxdd: f64,
    pub turnover_total: f64,
    pub avg_active: f64,
}

pub fn pf_report(days: &[PfDay]) -> PfReport {
    if days.is_empty() {
        return PfReport::default();
    }
    let last = *days.last().unwrap();
    let total = last.nav - 1.0;
    let mut peak = 0.0f64;
    let mut mdd = 0.0f64;
    for d in days {
        if d.nav > peak {
            peak = d.nav;
        }
        let dd = (peak - d.nav) / peak;
        if dd > mdd {
            mdd = dd;
        }
    }
    let span_ns = (last.ts - days[0].ts) as f64;
    let years = span_ns / (365.25 * 86_400_000_000_000.0);
    let ann = if years > 0.0 && last.nav > 0.0 {
        last.nav.powf(1.0 / years) - 1.0
    } else {
        f64::NAN
    };
    let rets: Vec<f64> = days.iter().map(|d| d.ret).collect();
    let n = rets.len() as f64;
    let mean = rets.iter().sum::<f64>() / n;
    let var = rets.iter().map(|r| (r - mean) * (r - mean)).sum::<f64>() / n;
    let sd = var.sqrt();
    let avg_active = days.iter().map(|d| d.n_active as f64).sum::<f64>() / n;
    PfReport {
        n_bars: days.len() as i64,
        final_nav: last.nav,
        total_ret: total,
        ann_ret: ann,
        vol_daily: sd,
        sharpe_daily: if sd > 0.0 { mean / sd * 252f64.sqrt() } else { f64::NAN },
        maxdd: mdd,
        turnover_total: days.iter().map(|d| d.turnover).sum(),
        avg_active,
    }
}

#[cfg(test)]
mod pf_tests {
    use super::*;

    fn day(ts0: i64, n: usize) -> Vec<i64> {
        (0..n as i64).map(|i| ts0 + i * 86_400_000_000_000).collect()
    }

    #[test]
    fn two_active_names_share_the_book_and_compound() {
        // both rise +1%/day, both signalled from day 0
        let n = 20usize;
        let ts = day(1_700_000_000_000_000_000, n);
        let a = PfSeries {
            close: (0..n).map(|i| 100.0 * (1.01f64).powi(i as i32)).collect(),
            sig: vec![1; n],
            ts: ts.clone(),
        };
        let b = PfSeries { ..a.clone() };
        let days = portfolio_sim(&[a, b], 0.0);
        let nav = days.last().unwrap().nav;
        assert!((nav - 1.01f64.powi(19)).abs() < 1e-6, "first bar earns nothing, then 19 daily 1%: nav {nav}");
        assert_eq!(days[0].n_active, 2);
    }

    #[test]
    fn shorts_are_ignored_and_weights_switch() {
        let n = 15usize;
        let ts = day(1_700_000_000_000_000_000, n);
        let up: Vec<f64> = (0..n).map(|i| 100.0 * (1.02f64).powi(i as i32)).collect();
        let down: Vec<f64> = (0..n).map(|i| 100.0 * (0.99f64).powi(i as i32)).collect();
        // rising name signalled days 0..4 only; falling name short-signalled all along
        let mut sig_a = vec![0i8; n];
        for i in 0..5 {
            sig_a[i] = 1;
        }
        let b = PfSeries { close: down, sig: vec![-1; n], ts: ts.clone() };
        let a = PfSeries { close: up, sig: sig_a, ts };
        let days = portfolio_sim(&[a, b], 0.0);
        let early = days[4].nav;
        assert!((early - 1.02f64.powi(4)).abs() < 1e-6, "only A active days 1..=4: {early}");
        // signal turns flat at close of day 5 (day-5 return still earned), then cash
        let later = days[10].nav;
        assert!((later - 1.02f64.powi(5)).abs() < 1e-6, "exit at close day5 then flat: {later}");
    }

    #[test]
    fn turnover_cost_drags_nav() {
        let n = 12usize;
        let ts = day(1_700_000_000_000_000_000, n);
        // both flat-priced; the active name switches from A (days 0..5) to B
        let mut sig_a = vec![0i8; n];
        let mut sig_b = vec![0i8; n];
        for i in 0..6 {
            sig_a[i] = 1;
        }
        for i in 6..n {
            sig_b[i] = 1;
        }
        let series = |sig: Vec<i8>| PfSeries { close: vec![100.0; n], sig, ts: ts.clone() };
        let free = portfolio_sim(&[series(sig_a.clone()), series(sig_b.clone())], 0.0);
        let paid = portfolio_sim(&[series(sig_a), series(sig_b)], 50.0);
        assert!(paid.last().unwrap().nav < free.last().unwrap().nav, "turnover cost must drag nav");
    }
}
