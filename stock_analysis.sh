#!/usr/bin/env bash
#
# stock_analysis.sh — P(up/down) over the next H trading days (default ≈2 weeks)
#
# Pulls daily history through the unified market framework, scores the latest
# bar with the historical-analog model and — by default — scales that raw
# probability through a causal Platt/isotonic calibrator fit on the walk-forward
# history (past-only), so a threshold of 0.75 means what it says.
#
# Usage:
#   ./stock_analysis.sh HK.00700                  # SOURCE=futu, H=10, K=20, calibrated
#   SOURCE=yahoo ./stock_analysis.sh 0700.HK
#   SOURCE=parquet FILE=./bars.parquet ./stock_analysis.sh HK.00700
#   THRESHOLD=0.6 METHOD=raw ./stock_analysis.sh HK.00700    # switch scaling off
#   CALMODEL=./analytics_out/HK_00700_calib_model.tsv ./stock_analysis.sh HK.00700  # reuse
#
# Env: SYMBOL (positional), SOURCE=futu|yahoo|parquet, PERIOD=1d, ADJUST=qfq,
#   HORIZON=10, K=20, THRESHOLD=0.75, METHOD=platt|iso|raw (default platt),
#   CAL=30 (% of walk history used to fit), CALMODEL=<path> (reuse saved model),
#   START/END, FILE=(parquet), OUT_DIR=./analytics_out, GTV_PROFILE=release
#
# Exit: 0 ok/no alert · 1 data/param error · 3 alert triggered (cron: check 3).
# Research/education only — not investment advice.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$here"

SYMBOL="${SYMBOL:-${1:-}}"
[ -n "$SYMBOL" ] || { echo "usage: $0 <SYMBOL>   (e.g. HK.00700 / 0700.HK / AAPL)" >&2; exit 1; }

SOURCE="${SOURCE:-futu}"            # futu | yahoo | parquet
PERIOD="${PERIOD:-1d}"
ADJUST="${ADJUST:-qfq}"
HORIZON="${HORIZON:-10}"            # trading days ≈ 2 weeks
K="${K:-20}"
THRESHOLD="${THRESHOLD:-0.75}"   # alert when max(p_cal, 1-p_cal) >= THRESHOLD
METHOD="${METHOD:-platt}"        # platt | iso | raw
CAL="${CAL:-30}"                 # percent of walk history used to fit calibrator
CALMODEL="${CALMODEL:-}"         # optional saved model to reuse (skip refit)
START="${START:-$(date -d '6 years ago' +%F 2>/dev/null || date -v-6y +%F)}"
END="${END:-$(date +%F)}"
FILE="${FILE:-}"                    # required when SOURCE=parquet
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

mkdir -p "$OUT_DIR"
PARQUET_FILE="$OUT_DIR/${SYMBOL//./_}_bars.parquet"
TSV_FILE="$OUT_DIR/${SYMBOL//./_}_analysis.tsv"
MODEL_FILE="${CALMODEL:-$OUT_DIR/${SYMBOL//./_}_calib_model.tsv}"

SEC="$(date -u -d "$END" +%s 2>/dev/null || date -u -j -f '%F' "$END" +%s)"
ASOF_NS="$((SEC * 1000000000))"

case "$SOURCE" in
  futu)   CMD="md klines futu $TBL $SYMBOL --period $PERIOD --start $START --end $END --adjust $ADJUST" ;;
  yahoo)  CMD="md klines yahoo $TBL $SYMBOL --period $PERIOD --start $START --end $END" ;;
  parquet)
    [ -n "$FILE" ] && [ -f "$FILE" ] || { echo "SOURCE=parquet needs FILE=<path>" >&2; exit 1; }
    if [ "$(realpath "$FILE")" != "$(realpath "$PARQUET_FILE")" ]; then cp "$FILE" "$PARQUET_FILE"; fi
    CMD="load $TBL $PARQUET_FILE" ;;
  *) echo "unknown SOURCE=$SOURCE (futu|yahoo|parquet)" >&2; exit 1 ;;
esac

REUSE_MODEL=0
if [ "$METHOD" != "raw" ] && [ -n "$CALMODEL" ] && [ -f "$CALMODEL" ]; then
  REUSE_MODEL=1
fi

CALF="$(awk "BEGIN{printf \"%.3f\", $CAL/100}")"
LINES="$CMD"
if [ "$METHOD" != "raw" ] && [ "$REUSE_MODEL" = 0 ]; then
  LINES="$LINES
fwd_walk('$TBL',$HORIZON,$K,$CALF)"
fi
LINES="$LINES
fwd_regress('$TBL',$ASOF_NS,$HORIZON,$K)
save $TBL $PARQUET_FILE
quit"

LOG="$(mktemp)"
trap 'rm -f "$LOG"' EXIT
printf '%b\n' "$LINES" | "$BIN" > "$LOG" 2>&1 || { echo "gtv failed:"; tail -6 "$LOG" >&2; exit 1; }

python3 - "$LOG" "$SYMBOL" "$HORIZON" "$K" "$METHOD" "$THRESHOLD" "$REUSE_MODEL" "$MODEL_FILE" "$TSV_FILE" "$CAL" <<'PY'
import sys, math, os, datetime
log = open(sys.argv[1], encoding="utf-8", errors="replace").read()
sym, H, K = sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
method, threshold = sys.argv[5], float(sys.argv[6])
reuse = sys.argv[7] == "1"
model_file, tsv_file, cal_pct = sys.argv[8], sys.argv[9], float(sys.argv[10])

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
    sys.stderr.write("log tail:\n" + "\n".join(log.splitlines()[-14:]) + "\n")
    sys.exit("no prediction table in output")

def rows_of(t):
    hdr, body = t
    out = []
    for cells in body:
        row = {}
        for name in hdr:
            try: row[name] = float(cells[hdr.index(name)])
            except (ValueError, IndexError): row[name] = float("nan")
        out.append(row)
    return out

walk = reg = None
for t in tables:
    if "bar_ret" in t[0]:
        walk = rows_of(t)
    elif "pred_lo" in t[0]:
        rr = rows_of(t)
        reg = rr[0] if rr else None
if reg is None:
    sys.exit("no fwd_regress row for the decision bar (history too short?)")

p_raw = reg["p_up"]
date = datetime.datetime.fromtimestamp(reg["t"] / 1e9, datetime.timezone.utc).strftime("%Y-%m-%d")
close = reg["close"]

# ---------- calibration helpers ------------------------------------------------
def isotonic_fit(scores, labels):
    pairs = sorted(zip(scores, labels))
    stack = []  # (sum, count, x_end)
    for x, y in pairs:
        stack.append((y, 1.0, x))
        while len(stack) >= 2 and stack[-1][0]/stack[-1][1] < stack[-2][0]/stack[-2][1] - 1e-12:
            (s2, c2, _) = stack.pop(); (s1, c1, _) = stack.pop()
            stack.append((s1 + s2, c1 + c2, x))
    xs = [e[2] for e in stack]
    ys = [e[0]/e[1] for e in stack]
    return xs, ys

def iso_apply(q, xs, ys):
    if not xs: return q
    if q <= xs[0]: return ys[0]
    if q >= xs[-1]: return ys[-1]
    return ys[next(i for i, x in enumerate(xs) if x >= q)]

def platt_fit(scores, labels):
    npos = sum(1 for y in labels if y > 0); nneg = len(labels) - npos
    tar = [(npos + 1.0)/(npos + 2.0) if y > 0 else 1.0/(nneg + 2.0) for y in labels]
    a, b = 1.0, 0.0
    for _ in range(100):
        g0 = g1 = h00 = h01 = h11 = 0.0
        for f, t in zip(scores, tar):
            p = 1.0/(1.0 + math.exp(-(a*f + b)))
            e = p - t
            g0 += e*f; g1 += e
            w = p*(1.0 - p)
            h00 += w*f*f; h01 += w*f; h11 += w
        det = h00*h11 - h01*h01
        if abs(det) < 1e-12: break
        da = (g0*h11 - g1*h01)/det; db = (g1*h00 - g0*h01)/det
        a -= da; b -= db
        if da*da + db*db < 1e-10: break
    return a, b

# ---------- fit / reuse the calibrator ----------------------------------------
platt, iso_x, iso_y = (1.0, 0.0), [], []
if method != "raw":
    if reuse:
        for line in open(model_file, encoding="utf-8"):
            f = line.rstrip("\n").split("\t")
            if f and f[0] == "platt": platt = (float(f[1]), float(f[2]))
            elif f and f[0] == "iso": iso_x.append(float(f[1])); iso_y.append(float(f[2]))
    elif walk and len(walk) >= 60:
        n_tr = max(20, int(len(walk) * cal_pct / 100.0))
        train = walk[:n_tr]
        sc = [r["p_up"] for r in train if r["up"] == r["up"]]
        lb = [1.0 if r["up"] > 0 else 0.0 for r in train if r["up"] == r["up"]]
        if len(sc) >= 20:
            platt = platt_fit(sc, lb)
            iso_x, iso_y = isotonic_fit(sc, lb)
            with open(model_file, "w", encoding="utf-8") as f:
                f.write(f"platt\t{platt[0]:.8f}\t{platt[1]:.8f}\n")
                for x, y in zip(iso_x, iso_y):
                    f.write(f"iso\t{x:.8f}\t{y:.8f}\n")
        else:
            sys.exit(f"not enough labelled walk rows ({len(sc)}) to fit the calibrator")

# ---------- apply + decide ----------------------------------------------------
if method == "platt":
    p = 1.0 / (1.0 + math.exp(-(platt[0] * p_raw + platt[1])))
elif method == "iso":
    p = iso_apply(p_raw, iso_x, iso_y)
else:
    p = p_raw
p = min(0.999, max(0.001, p))
strength = max(p, 1.0 - p)
direction = "UP" if p >= 0.5 else "DOWN"
decision = f"{direction}_ALERT" if strength >= threshold else "NO_SIGNAL"
label = "raw" if method == "raw" else method

print(f"{sym} | decision bar {date} close={close:.2f} | method={label}")
print(f"  p_raw={p_raw:.3f} -> p_cal={p:.3f} | direction={direction} strength={strength:.3f} "
      f"(threshold {threshold:.2f})")
if method == "raw":
    print("  calibration: off (METHOD=raw)")
elif reuse:
    print(f"  calibrated model: reused from {model_file}")
else:
    print(f"  calibrated model: fitted on first {cal_pct:.0f}% of walk history, saved -> {model_file}")
print(f"  -> {tsv_file}")

# persist decision row (append; header on first write)
first = not os.path.exists(tsv_file)
os.makedirs(os.path.dirname(tsv_file) or ".", exist_ok=True)
with open(tsv_file, "a", encoding="utf-8") as f:
    if first:
        f.write("symbol\tdate\tclose\tp_raw\tp_cal\tdecision\tmethod\thorizon\tk\n")
    f.write(f"{sym}\t{date}\t{close:.4f}\t{p_raw:.4f}\t{p:.4f}\t{decision}\t{label}\t{H}\t{K}\n")

if decision == "NO_SIGNAL":
    print(f"no strong signal (strength {strength:.3f} < {threshold:.2f})")
    sys.exit(0)
print(f"ALERT: {sym} likely {direction} ({strength:.3f} >= {threshold:.2f}) over next {H} trading days")
sys.exit(3)

PY
