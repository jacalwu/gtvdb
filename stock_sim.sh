#!/usr/bin/env bash
#
# stock_sim.sh — paper-trade simulation of the calibrated trend signal.
#
# Feeds the strictly-causal walk-forward history (fwd_walk, same engine as
# stock_calib/diag) into a "probability -> action" rule:
#   signal when strength(cal_p) >= THR  -> enter at that bar's close
#   exit H trading days later           -> realised H-bar return (actual_ret)
# net of a round-trip cost in bps. Reports per-threshold and per-cost metrics
# (n signals, win%, mean net ret, per-signal Sharpe, max drawdown on the
# sequential signal equity curve). This answers "does acting on the signal
# actually make money after costs?" (design.md §7 M3).
#
# Usage:
#   ./stock_sim.sh HK.02513                # SOURCE=futu (default), HORIZON=7
#   SOURCE=parquet FILE=./bars.parquet ./stock_sim.sh HK.02513
#   HORIZON=10 THRESH=0.75 METHOD=platt ./stock_sim.sh HK.00700
#
# Env: SYMBOL (positional), SOURCE=futu|yahoo|parquet, HORIZON=7, K=20,
#   CAL=30 (% history used to fit the calibrator inside fwd_walk),
#   METHOD=platt|iso|raw (which p column decides), THRESH="0.60,0.65,...0.85"
#   COSTS="0,10,20,50" (round-trip bps), SIZING=5 (0 = full-notional):
#   position = min(1, SIZING * (strength - 0.5)) — strength .60 -> 0.5x,
#   .70+ -> full. Signals whose *side* historically loses are shown but the
#   per-side means used by the action layer stay gross (sign unchanged).
#   OUT_DIR=./analytics_out, GTV_PROFILE=release.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; cd "$here"

SYMBOL="${SYMBOL:-${1:-}}"
[ -n "$SYMBOL" ] || { echo "usage: $0 <SYMBOL>  (e.g. HK.02513 / 0700.HK)" >&2; exit 1; }
SOURCE="${SOURCE:-futu}"; HORIZON="${HORIZON:-7}"; K="${K:-20}"; CAL="${CAL:-30}"
METHOD="${METHOD:-platt}"      # platt|iso|raw
THRESH="${THRESH:-0.60,0.65,0.70,0.75,0.80,0.85}"
COSTS="${COSTS:-0,10,20,50}"; SIZING="${SIZING:-5}"
START="${START:-$(date -d '6 years ago' +%F 2>/dev/null || date -v-6y +%F)}"
END="${END:-$(date +%F)}"; FILE="${FILE:-}"
OUT_DIR="${OUT_DIR:-./analytics_out}"; GTV_PROFILE="${GTV_PROFILE:-release}"
BIN="target/$GTV_PROFILE/gtv"
TBL="$(printf '%s' "$SYMBOL" | tr -c 'A-Za-z0-9' '_' | tr 'A-Z' 'a-z')_bars"
[ -x "$BIN" ] || { echo "build gtv ($GTV_PROFILE) first" >&2; exit 1; }
mkdir -p "$OUT_DIR"

case "$SOURCE" in
  futu)   CMD="md klines futu $TBL $SYMBOL --period 1d --start $START --end $END --adjust qfq" ;;
  yahoo)  CMD="md klines yahoo $TBL $SYMBOL --period 1d --start $START --end $END" ;;
  parquet) [ -n "$FILE" ] && [ -f "$FILE" ] || { echo "SOURCE=parquet needs FILE=<path>" >&2; exit 1; }
          CMD="load $TBL $FILE" ;;
  *) echo "unknown SOURCE=$SOURCE" >&2; exit 1 ;;
esac
CALF="$(awk "BEGIN{printf \"%.3f\", $CAL/100}")"
LOG="$(mktemp)"; trap 'rm -f "$LOG"' EXIT
printf '%b\n' "$CMD
fwd_walk('$TBL',$HORIZON,$K,$CALF)
quit" | "$BIN" > "$LOG" 2>&1 || { echo "gtv failed:"; tail -5 "$LOG" >&2; exit 1; }

THRESH_TSV="$OUT_DIR/${SYMBOL//./_}_sim_H${HORIZON}d.tsv"
python3 - "$LOG" "$SYMBOL" "$HORIZON" "$METHOD" "$THRESH" "$COSTS" "$THRESH_TSV" "$SIZING" <<'PY'
import sys, math
log = open(sys.argv[1], encoding="utf-8", errors="replace").read()
sym, H, method = sys.argv[2], int(sys.argv[3]), sys.argv[4]
thrs = [float(x) for x in sys.argv[5].split(",")]
costs = [float(x) for x in sys.argv[6].split(",")]
tsv_path = sys.argv[7]
SZ = float(sys.argv[8])

# parse the fwd_walk table by header names (engine fills cal cols causally)
hdr = body = None
for l in log.splitlines():
    if not l.startswith("|"): continue
    cells = [c.strip() for c in l.split("|")]
    if "actual_ret" in cells and "cal_p_up_platt" in cells:
        hdr, body = cells, []
    elif hdr is not None and len(cells) >= len(hdr):
        body.append(cells)
if not hdr:
    sys.exit("no fwd_walk table in output (history too short?)")
def col(name):
    i = hdr.index(name); return [float(c[i]) if c[i] else float("nan") for c in body]
t, up, act = col("t"), col("up"), col("actual_ret")
p_raw = col("p_up")
p_cal = col("cal_p_up_platt" if method == "platt" else "cal_p_up_iso" if method == "iso" else "p_up")

# decision probability per row (method 'raw' uses the full eval region)
p = p_cal if method != "raw" else p_raw
rows = [(tt, u, a, pp) for tt, u, a, pp in zip(t, up, act, p) if math.isfinite(a)]
rows = [(tt, u, a, max(0.001, min(0.999, pp))) for tt, u, a, pp in rows if math.isfinite(pp)]
rows.sort(key=lambda r: r[0])

def sim(thr, cost_bps, sizing):
    cost = cost_bps / 1e4
    net_trades, buyl_g, selll_g = [], [], []
    for _, _, a, pp in rows:
        st = max(pp, 1 - pp)
        if st < thr: continue                        # no signal -> no trade
        dir_r = a if pp >= 0.5 else -a               # long / short direction
        pos = 0.0 if sizing <= 0 else min(1.0, sizing * (st - 0.5))
        if sizing <= 0: pos = 1.0                    # legacy full-notional
        (buyl_g if pp >= 0.5 else selll_g).append(dir_r)
        net_trades.append(pos * (dir_r - cost))      # sized trade, fee on notional
    if not net_trades: return dict(n=0)
    eq, peak, mdd = 1.0, 1.0, 0.0
    for r in net_trades:
        eq *= 1 + r; peak = max(peak, eq); mdd = max(mdd, (peak - eq) / peak)
    import statistics
    mu = statistics.fmean(net_trades)
    sd = statistics.stdev(net_trades) if len(net_trades) > 1 else 0.0
    mb = statistics.fmean(buyl_g) if buyl_g else float("nan")   # gross, sign stable
    ms = statistics.fmean(selll_g) if selll_g else float("nan")
    return dict(n=len(net_trades), buy=len(buyl_g), sell=len(selll_g), buy_mean=mb, sell_mean=ms,
                win=sum(1 for r in net_trades if r > 0) / len(net_trades),
                mean_bp=mu * 1e4, sharpe=(mu / sd) if sd > 0 else float("nan"),
                mdd=mdd * 100, cost=cost_bps, thr=thr, sizing=sizing)

out = []
print(f"=== paper sim (sized): {sym} | H={H} td | method={method} | sizing={SZ:g} | eval decisions={len(rows)} ===")
print(f"{'THR':>5} | {'cost bps':>8} {'n_sig':>6} {'win%':>6} {'mean_net(bp)':>11} {'sharpe':>7} {'maxDD(sz)':>9} {'maxDD(full)':>11}")
for thr in thrs:
    for c in costs:
        r = sim(thr, c, SZ)
        rf = sim(thr, c, 0.0)
        out.append((sym, H, method, thr, c, SZ, r.get("n", 0), r.get("buy", 0), r.get("sell", 0),
                    round(r.get("buy_mean", 0) * 1e4, 1) if r.get("n") else 0.0,
                    round(r.get("sell_mean", 0) * 1e4, 1) if r.get("n") else 0.0,
                    round(r["win"], 3) if r.get("n") else 0.0,
                    round(r["mean_bp"], 1) if r.get("n") else 0.0,
                    round(r["sharpe"], 2) if r.get("n") else 0.0,
                    round(r["mdd"], 2) if r.get("n") else 0.0,
                    round(rf["mdd"], 2) if rf.get("n") else 0.0))
        if r.get("n"):
            print(f"{thr:>5.2f} | {c:>8.0f} {r['n']:>6} (L{r['buy']}/S{r['sell']}) {r['win']*100:>6.1f} {r['mean_bp']:>11.1f} {r['sharpe']:>7.2f} {r['mdd']:>9.1f} {rf['mdd']:>11.1f}")
with open(tsv_path, "w", encoding="utf-8") as f:
    f.write("symbol\thorizon\tmethod\tthr\tcost_bps\tsizing\tn_sig\tn_buy\tn_sell\tmean_buy_bp\tmean_sell_bp\twin_pct\tmean_net_bp\tsharpe\tmaxdd_sized_pct\tmaxdd_full_pct\n")
    for r in out:
        f.write("\t".join(str(x) for x in r) + "\n")
print(f"-> {tsv_path}")
PY
