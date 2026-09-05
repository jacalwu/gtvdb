#!/usr/bin/env bash
#
# stock_calib.sh — M2 walk-forward calibration report + threshold suggestion
#
# Strictly-causal backtest of the historical-analog rule (fwd_walk): bucket
# predicted confidence vs realised outcomes, then suggest the smallest signal
# threshold whose directional hit-rate clears TARGET_HIT on >= MIN_N trades.
#
# Usage:
#   ./stock_calib.sh HK.00700            # SOURCE=futu (default), HORIZON=10, K=20
#   SOURCE=yahoo ./stock_calib.sh AAPL
#   SOURCE=parquet FILE=./bars.parquet ./stock_calib.sh HK.00700
#   HORIZON=14 TARGET_HIT=0.6 MIN_N=40 ./stock_calib.sh HK.00700
#
# Env (defaults): SYMBOL (positional), SOURCE=futu|yahoo|parquet, PERIOD=1d,
#   ADJUST=qfq, HORIZON=10, K=20, WARMUP=<k*10,20>, TARGET_HIT=0.65, MIN_N=30,
#   START=<6y back>, END=<today>, FILE=(parquet), OUT_DIR=./analytics_out,
#   GTV_PROFILE=release
#
# Exit: 0 ok. Research/education only.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$here"

SYMBOL="${SYMBOL:-${1:-}}"
[ -n "$SYMBOL" ] || { echo "usage: $0 <SYMBOL>" >&2; exit 1; }

SOURCE="${SOURCE:-futu}"
PERIOD="${PERIOD:-1d}"
ADJUST="${ADJUST:-qfq}"
HORIZON="${HORIZON:-10}"
K="${K:-20}"
WARMUP="${WARMUP:-}"
TARGET_HIT="${TARGET_HIT:-0.65}"
MIN_N="${MIN_N:-30}"
START="${START:-$(date -d '6 years ago' +%F 2>/dev/null || date -v-6y +%F)}"
END="${END:-$(date +%F)}"
FILE="${FILE:-}"
OUT_DIR="${OUT_DIR:-./analytics_out}"
GTV_PROFILE="${GTV_PROFILE:-release}"
BIN="target/$GTV_PROFILE/gtv"

TBL="$(printf '%s' "$SYMBOL" | tr -c 'A-Za-z0-9' '_' | tr 'A-Z' 'a-z')_bars"

if [[ ! -x "$BIN" ]]; then
  echo "building gtv ($GTV_PROFILE)…"
  if [[ "$GTV_PROFILE" == "release" ]]; then
    cargo build --release -p gtv-cli --bin gtv
  else
    cargo build -p gtv-cli --bin gtv
  fi
fi

case "$SOURCE" in
  futu)   CMD="md klines futu $TBL $SYMBOL --period $PERIOD --start $START --end $END --adjust $ADJUST" ;;
  yahoo)  CMD="md klines yahoo $TBL $SYMBOL --period $PERIOD --start $START --end $END" ;;
  parquet)
    [ -n "$FILE" ] && [ -f "$FILE" ] || { echo "SOURCE=parquet needs FILE=<path>" >&2; exit 1; }
    CMD="load $TBL $FILE" ;;
  *) echo "unknown SOURCE=$SOURCE (futu|yahoo|parquet)" >&2; exit 1 ;;
esac

LOG="$(mktemp)"
trap 'rm -f "$LOG"' EXIT
mkdir -p "$OUT_DIR"

# fwd_walk(name, horizon, k [, warmup] [, feats]) — no warmup -> engine default.
WALK_ARGS="'$TBL',$HORIZON,$K"
[ -n "$WARMUP" ] && WALK_ARGS="'$TBL',$HORIZON,$K,$WARMUP"

printf "%s\nfwd_walk($WALK_ARGS)\nquit\n" "$CMD" \
  | "$BIN" > "$LOG" 2>&1 || { echo "gtv failed:"; tail -3 "$LOG" >&2; exit 1; }

python3 - "$LOG" "$SYMBOL" "$HORIZON" "$K" "$TARGET_HIT" "$MIN_N" "$OUT_DIR" <<'PY'
import sys, statistics
log = open(sys.argv[1], encoding="utf-8", errors="replace").read()
sym, H, K = sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
target_hit, min_n = float(sys.argv[5]), int(sys.argv[6])
out_dir = sys.argv[7]

# parse last ASCII table (lines with '|'; separators start with '+')
tables, cur = [], None
for l in log.splitlines():
    if not l.startswith("|"):
        continue
    cells = [c.strip() for c in l.split("|")]
    if "p_up" in cells:
        tables.append((cells, [])); cur = tables[-1]
    elif cur is not None and len(cells) >= len(cur[0]):
        cur[1].append(cells)
if not tables:
    sys.exit("no fwd_walk table in output")
hdr, body = tables[-1]
def col(name): return hdr.index(name)
rows = []
for cells in body[1:]:
    rows.append({
        "t": int(cells[col("t")]), "p_up": float(cells[col("p_up")]),
        "p_down": float(cells[col("p_down")]), "up": int(cells[col("up")]) == 1,
        "n": int(cells[col("n_analogs")]),
    })
if not rows:
    sys.exit("fwd_walk produced no evaluation rows")
n = len(rows)
base_up = sum(r["up"] for r in rows) / n
overall_hit = sum((r["p_up"] >= r["p_down"]) == r["up"] for r in rows) / n

# ---- calibration: bucket by predicted p_up (deciles) -------------------------
cal = []
for b in range(10):
    lo, hi = b / 10, (b + 1) / 10
    sub = [r for r in rows if lo <= r["p_up"] < hi]
    if sub:
        cal.append((lo, hi, len(sub), sum(r["p_up"] for r in sub) / len(sub),
                    sum(r["up"] for r in sub) / len(sub)))

# ---- rule table: s = max(p_up, p_down), dir = argmax ------------------------
def hit_dir(r):
    return (r["p_up"] >= r["p_down"]) == r["up"]

report = []
best = None
for T in [x / 100 for x in range(50, 96, 5)]:
    sub = [r for r in rows if max(r["p_up"], r["p_down"]) >= T]
    if not sub:
        continue
    h = sum(hit_dir(r) for r in sub) / len(sub)
    report.append((T, len(sub), h,
                   sum(1 for r in sub if r["p_up"] >= r["p_down"] and r["up"]),
                   sum(1 for r in sub if r["p_up"] >= r["p_down"]),
                   sum(1 for r in sub if r["p_up"] < r["p_down"] and not r["up"]),
                   sum(1 for r in sub if r["p_up"] < r["p_down"])))
    if len(sub) >= min_n and best is None and h >= target_hit:
        best = (T, len(sub), h)

suggestion = f"keep default threshold 0.70 (insufficient evidence; target {target_hit:.2f} hit on >= {min_n} signals)"
if best:
    suggestion = (f"use threshold >= {best[0]:.2f}: {best[2]*100:.1f}% directional hit "
                  f"on {best[1]} signals (target {target_hit:.2f})")

# ---- outputs ----------------------------------------------------------------
def pct(x): return f"{x*100:.1f}%"
print(f"=== walk-forward calibration: {sym} | H={H} td, K={K} | {n} evaluation rows ===")
print(f"base up-rate {pct(base_up)} | overall directional hit {pct(overall_hit)}\n")
print(f"{'p_up bucket':<14}{'n':>5}{'avg p_up':>10}{'actual up':>11}")
for lo, hi, cnt, avg, act in cal:
    tag = f"[{lo:.1f},{hi:.1f})" if hi < 1 else f"[{lo:.1f},1.0]"
    print(f"{tag:<14}{cnt:>5}{pct(avg):>10}{pct(act):>11}")
print(f"\n{'threshold':<10}{'signals':>8}{'dir hit':>9}{'long ok/t':>11}{'short ok/t':>11}")
for T, cnt, h, lo_ok, lo_n, so_ok, so_n in report:
    lo = f"{lo_ok}/{lo_n}" if lo_n else "-"
    so = f"{so_ok}/{so_n}" if so_n else "-"
    print(f"{T:<10.2f}{cnt:>8}{pct(h):>9}{lo:>11}{so:>11}")
print(f"\nsuggestion: {suggestion}")

import datetime
with open(f"{out_dir}/{sym.replace('.', '_')}_calib.tsv", "w") as f:
    f.write("# walk-forward calibration (fwd_walk) — strict past-only analogs\n")
    f.write(f"# symbol={sym} horizon={H} k={K} rows={n} "
            f"base_up_rate={base_up:.4f} overall_dir_hit={overall_hit:.4f}\n")
    f.write("threshold\tsignals\tdir_hit\tlong_hit\tlong_n\tshort_hit\tshort_n\n")
    for T, cnt, h, lo_ok, lo_n, so_ok, so_n in report:
        lh = lo_ok / lo_n if lo_n else float("nan")
        sh = so_ok / so_n if so_n else float("nan")
        f.write(f"{T:.2f}\t{cnt}\t{h:.4f}\t{lh:.4f}\t{lo_n}\t{sh:.4f}\t{so_n}\n")
print(f"\nreport: {out_dir}/{sym.replace('.', '_')}_calib.tsv")
PY
