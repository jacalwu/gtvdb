#!/usr/bin/env bash
#
# holdings_sim.sh — portfolio-level paper-sim summary with a BUY/HOLD/SELL
# action column, distilled from each holding's stock_sim H=7 output.
#
# Rule (documented, THR=0.60, cost 0):
#   BUY  : expected net return > 0 AND win% > 52
#   HOLD : positive-or-flat expectancy but win% <= 52 (e.g. right-skewed AI)
#          OR history too short to simulate (new listings — signal unverified)
#   SELL : negative expected net return -> acting on this signal loses
# (HK retail has no easy shorting: SELL here means "avoid / trim if holding".)
#
# Usage: ./holdings_sim.sh          (needs analytics_out/*_sim_H7d.tsv first:
#        run  ./holdings_sim.sh --run   to also run stock_sim.sh for all holdings)
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; cd "$here"
HORIZON="${HORIZON:-7}"; OUT_DIR="${OUT_DIR:-./analytics_out}"
if [ "${1:-}" = "--run" ]; then
  for s in $(sed -e '/^#/d' -e '/^[[:space:]]*$/d' HOLDING.txt | awk '{print $1}'); do
    ./stock_sim.sh "$s" >/dev/null
  done
fi
OUT_DIR="$OUT_DIR" HORIZON="$HORIZON" python3 - <<'PY'
import os, csv
H = int(os.environ["HORIZON"])
hold = []
for line in open("HOLDING.txt", encoding="utf-8"):
    line = line.strip()
    if not line or line.startswith("#"): continue
    num = "".join(ch for ch in line.split()[0] if ch.isdigit()).lstrip("0")
    hold.append("HK." + num.rjust(5, "0"))

def sim_row(sym):
    f = f"{os.environ['OUT_DIR']}/{sym.replace('.','_')}_sim_H{H}d.tsv"
    if not os.path.exists(f): return None
    for r in csv.DictReader(open(f), delimiter="\t"):
        if abs(float(r["thr"]) - 0.6) < 1e-9 and float(r["cost_bps"]) == 0 and int(r["n_sig"]) > 0:
            return r
    return None

def pcal(sym):
    f = f"{os.environ['OUT_DIR']}/holdings_forecast.tsv"
    if not os.path.exists(f):
        f = f"{os.environ['OUT_DIR']}/holdings_forecast_{H}d.tsv"
    if not os.path.exists(f): return None
    with open(f, encoding="utf-8") as fh:
        rows = [l.rstrip("\n").split("\t") for l in fh if l.strip()]
    hdr, body = rows[0], rows[1:]
    for b in body:
        if b[0] == sym and "p_cal" in hdr:
            try: return float(b[hdr.index("p_cal")])
            except (ValueError, IndexError): return None
    return None

def act(pc, r):
    """BUY/HOLD/SELL from today's calibrated signal + that side's paper-sim
    expectancy (THR .6, 0 cost): only act on a signal whose historical bets
    paid; short history / no signals -> HOLD (unverified)."""
    if r is None: return "HOLD"
    nb, mb = int(r["n_buy"]), float(r["mean_buy_bp"])
    ns, ms = int(r["n_sell"]), float(r["mean_sell_bp"])
    up = pc is None or pc >= 0.5
    s = max(pc if pc is not None else 0.5, 1 - (pc if pc is not None else 0.5))
    if up and s >= 0.60:
        return "BUY" if nb >= 30 and mb > 0 else "HOLD"     # up call, longs paid?
    if (not up) and s >= 0.55:
        return "SELL" if ns >= 30 and ms > 0 else "HOLD"    # down call, shorts paid?
    return "HOLD"

print(f"{'symbol':<10} {'action':>5} {'p_cal':>7} {'nBuy':>5} {'buyMean':>9} {'nSell':>5} {'sellMean':>9}")
out = []
for sym in sorted(hold, key=lambda s: int("".join(ch for ch in s if ch.isdigit()))):
    pc = pcal(sym)
    r = sim_row(sym)
    a = act(pc, r)
    if r is None:
        note = "short history / no signal >= .6"
        print(f"{sym:<10} {a:>5} {pc if pc is None else round(pc,3):>7}   -     -     -     -   ({note})")
        out.append([sym, a, "" if pc is None else round(pc, 3), "", "", "", "", note])
    else:
        nb, mb, ns, ms = int(r["n_buy"]), float(r["mean_buy_bp"]), int(r["n_sell"]), float(r["mean_sell_bp"])
        note = "BUY=up-signal & longs paid | SELL=down-signal & shorts paid | else HOLD"
        print(f"{sym:<10} {a:>5} {pc if pc is None else round(pc,3):>7} {nb:>5} {mb if mb==mb else float('nan'):>9.1f} {ns:>5} {ms if ms==ms else float('nan'):>9.1f}")
        out.append([sym, a, round(pc, 3) if pc is not None else "", nb, round(mb, 1), ns, round(ms, 1), ""])
dest = f"{os.environ['OUT_DIR']}/holdings_action_{H}d.tsv"
with open(dest, "w", encoding="utf-8") as f:
    f.write("symbol\taction\tp_cal\tn_buy\tmean_buy_bp\tn_sell\tmean_sell_bp\tnote\n")
    for o in out: f.write("\t".join(str(x) for x in o) + "\n")
print(f"\nrule: {note}")
print(f"-> {dest}")
PY
