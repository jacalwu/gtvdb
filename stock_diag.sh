#!/usr/bin/env bash
#
# stock_diag.sh — model diagnostics: calibration, quantile coverage,
#                 tail error decomposition, regime performance
#
# Runs the strictly-causal walk-forward engine function `fwd_walk` (which
# reports, per evaluated bar, p_up/p_down, the realised H-bar return and the
# neighbour forward-return quantiles q05..q95 plus bar_ret/vol20) and turns it
# into the four diagnostic CSVs of doc/forcast.md:
#
#   calibration_points.csv   p_up bins vs empirical up-rate (+ECE)
#   quantile_coverage.csv    nominal (q_a,q_1-a) band vs realised coverage
#   tail_error_stats.csv     left/right/middle MAE & bias (pred=q50)
#   regime_performance.csv   per-regime hit / MAE / bias / band coverage
#
# Usage:
#   ./stock_diag.sh HK.00700
#   ./stock_diag.sh HK.00700 HORIZON=14
#   REGIME=kmeans ./stock_diag.sh HK.00700      # k-means regimes (default: vol terciles)
#   SOURCE=yahoo ./stock_diag.sh AAPL
#
# Env: SYMBOL (positional), SOURCE=futu|yahoo|parquet, PERIOD=1d, ADJUST=qfq,
#   HORIZON=10, K=20, WARMUP=<auto>, START=<6y back>, END=<today>, FILE=,
#   REGIME=vol|kmeans, OUT_DIR=./analytics_out, GTV_PROFILE=release
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
REGIME="${REGIME:-vol}"
START="${START:-$(date -d '6 years ago' +%F 2>/dev/null || date -v-6y +%F)}"
END="${END:-$(date +%F)}"
FILE="${FILE:-}"
OUT_DIR="${OUT_DIR:-./analytics_out}"
GTV_PROFILE="${GTV_PROFILE:-release}"
BIN="target/$GTV_PROFILE/gtv"

TBL="$(printf '%s' "$SYMBOL" | tr -c 'A-Za-z0-9' '_' | tr 'A-Z' 'a-z')_bars"
mkdir -p "$OUT_DIR"
PARQUET_FILE="$OUT_DIR/${SYMBOL//./_}_bars.parquet"

if [[ ! -x "$BIN" ]]; then
  echo "building gtv ($GTV_PROFILE)…"
  if [[ "$GTV_PROFILE" == "release" ]]; then
    cargo build --release -p gtv-cli --bin gtv
  else
    cargo build -p gtv-cli --bin gtv
  fi
fi

# ---- prepare bars (engine market cache + analytics_out parquet reuse) -------
MD_LINE=""
case "$SOURCE" in
  futu)   MD_LINE="md klines futu $TBL $SYMBOL --period $PERIOD --start $START --end $END --adjust $ADJUST" ;;
  yahoo)  MD_LINE="md klines yahoo $TBL $SYMBOL --period $PERIOD --start $START --end $END" ;;
  parquet)
    [ -n "$FILE" ] && [ -f "$FILE" ] || { echo "SOURCE=parquet needs FILE=<path>" >&2; exit 1; }
    if [ "$(realpath "$FILE")" != "$(realpath "$PARQUET_FILE")" ]; then cp "$FILE" "$PARQUET_FILE"; fi ;;
  *) echo "unknown SOURCE=$SOURCE (futu|yahoo|parquet)" >&2; exit 1 ;;
esac
PREP_LOG="$(mktemp)"; DIAG_LOG="$(mktemp)"
trap 'rm -f "$PREP_LOG" "$DIAG_LOG"' EXIT
if [ ! -f "$PARQUET_FILE" ] && [ -n "$MD_LINE" ]; then
  printf '%s\nsave %s %s\nquit\n' "$MD_LINE" "$TBL" "$PARQUET_FILE" \
    | "$BIN" > "$PREP_LOG" 2>&1 || { echo "gtv prepare failed:"; tail -3 "$PREP_LOG" >&2; exit 1; }
fi

WALK_ARGS="'$TBL',$HORIZON,$K"
[ -n "$WARMUP" ] && WALK_ARGS="'$TBL',$HORIZON,$K,$WARMUP"

{
  echo "load $TBL $PARQUET_FILE"
  echo "fwd_walk($WALK_ARGS)"
  echo "quit"
} | "$BIN" > "$DIAG_LOG" 2>&1 || { echo "gtv diag failed:"; tail -5 "$DIAG_LOG" >&2; exit 1; }

python3 - "$DIAG_LOG" "$SYMBOL" "$HORIZON" "$K" "$REGIME" "$OUT_DIR" <<'PY'
import sys, math, statistics
log = open(sys.argv[1], encoding="utf-8", errors="replace").read()
sym, H, K, regime, out_dir = sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), sys.argv[5], sys.argv[6]

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
    sys.stderr.write("log tail:\n" + "\n".join(log.splitlines()[-12:]) + "\n")
    sys.exit("no fwd_walk table in output")

hdr, body = tables[-1]
def col(name): return hdr.index(name)
R = []
for cells in body[1:]:
    R.append({name: float(cells[col(name)]) for name in
             ["t","close","p_up","p_down","up","n_analogs","actual_ret",
              "q05","q10","q25","q50","q75","q90","q95","bar_ret","vol20"]})
if not R:
    sys.exit("fwd_walk produced no evaluation rows")
n = len(R)
def pct(x): return f"{x*100:.1f}%"
def f4(x): return f"{x:.4f}"

print(f"=== diagnostics: {sym} | H={H} td, K={K} | {n} walk-forward rows ===")

# ---------------- 1) calibration (p_up bins 0.05) ----------------------------
cal = []
for b in range(20):
    lo, hi = b*0.05, (b+1)*0.05
    sub = [r for r in R if lo <= r["p_up"] < hi]
    if not sub:
        continue
    mean_p = sum(r["p_up"] for r in sub)/len(sub)
    emp = sum(r["up"] > 0 for r in sub)/len(sub)
    cal.append((lo, hi, len(sub), mean_p, emp))
ece = sum(c[2]/n*abs(c[3]-c[4]) for c in cal) if cal else float("nan")
with open(f"{out_dir}/calibration_points.csv","w") as f:
    f.write("bin_low,bin_high,count,mean_p_up,empirical_up_rate\n")
    for lo,hi,c,mp,eu in cal:
        f.write(f"{lo:.2f},{hi:.2f},{c},{mp:.6f},{eu:.6f}\n")
print(f"calibration: {len(cal)} populated bins | ECE={f4(ece)} | "
      f"base up-rate={pct(sum(r['up']>0 for r in R)/n)}")
if cal:
    worst = max(cal, key=lambda c: abs(c[3]-c[4]))
    print(f"  worst bin [{worst[0]:.2f},{worst[1]:.2f}): p_up={pct(worst[3])} vs actual={pct(worst[4])} (n={worst[2]})")

# ---------------- 2) quantile coverage ---------------------------------------
# Interpolate neighbour quantiles for any two-sided level (only 05/10/25/50/
# 75/90/95 are stored), so the coverage curve can be drawn finely.
PS = [0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95]

def q_at(r, a):
    if a <= PS[0]:
        return r["q05"]
    if a >= PS[-1]:
        return r["q95"]
    for lo, hi in zip(PS, PS[1:]):
        if lo <= a <= hi:
            x = (a - lo) / (hi - lo)
            return r[f"q{int(lo*100):02d}"] * (1 - x) + r[f"q{int(hi*100):02d}"] * x
    return float("nan")

qc = []
for a in [0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45]:
    hi = 1 - a
    sub = [r for r in R if math.isfinite(q_at(r, a)) and math.isfinite(q_at(r, hi))]
    if not sub:
        continue
    inside = sum(q_at(r, a) <= r["actual_ret"] <= q_at(r, hi) for r in sub)
    qc.append((hi - a, len(sub), inside / len(sub)))
with open(f"{out_dir}/quantile_coverage.csv", "w") as f:
    f.write("nominal_coverage,count,empirical_coverage\n")
    for nom, c, ec in qc:
        f.write(f"{nom:.2f},{c},{ec:.6f}\n")
print("band coverage (endpoint return): " + ", ".join(
    f"{nom:.0%}->{pct(ec)}" for nom, c, ec in qc))

# ---------------- 3) tail error decomposition --------------------------------
rets = sorted(r["actual_ret"] for r in R)
q10 = rets[max(0, int(0.10*(len(rets)-1)))]
q90 = rets[min(len(rets)-1, int(0.90*(len(rets)-1)))]
def seg_stats(rows):
    errs = [abs(r["actual_ret"]-r["q50"]) for r in rows]
    bias = [r["actual_ret"]-r["q50"] for r in rows]
    return (len(rows), sum(errs)/len(errs), sum(bias)/len(bias))
tail_rows = []
for name, pred in [("left_tail", lambda r: r["actual_ret"] <= q10),
                   ("middle", lambda r: q10 < r["actual_ret"] < q90),
                   ("right_tail", lambda r: r["actual_ret"] >= q90)]:
    sub = [r for r in R if pred(r)]
    if sub:
        c, mae, bias = seg_stats(sub)
        tail_rows.append((name, c, mae, bias, q10, q90))
with open(f"{out_dir}/tail_error_stats.csv","w") as f:
    f.write("segment,count,mae,bias,q10_threshold,q90_threshold\n")
    for name,c,mae,bias,*_ in tail_rows:
        f.write(f"{name},{c},{mae:.6f},{bias:.6f},{q10:.6f},{q90:.6f}\n")
print("tail errors (pred=q50): " + "; ".join(
    f"{name}: n={c} MAE={pct(mae)} bias={pct(bias)}" for name,c,mae,bias,*_ in tail_rows))

# ---------------- 4) regime performance --------------------------------------
def kmeans(rows, k=3, iters=40):
    """Deterministic k-means over [z(vol20), z(bar_ret)]."""
    xs = [[r["vol20"], r["bar_ret"]] for r in rows]
    d = len(xs[0])
    means = [sum(x[j] for x in xs)/len(xs) for j in range(d)]
    sds = [math.sqrt(sum((x[j]-means[j])**2 for x in xs)/len(xs)) or 1.0 for j in range(d)]
    zs = [[(x[j]-means[j])/sds[j] for j in range(d)] for x in xs]
    # init: spread along each dim quantiles
    qs = []
    for j in range(d):
        col = sorted(z[j] for z in zs)
        qs.append([col[int(p*(len(col)-1))] for p in (0.2, 0.5, 0.8)])
    cents = [[qs[0][c], qs[1][c]] for c in range(k)]
    lab = [0]*len(zs)
    for _ in range(iters):
        for i, z in enumerate(zs):
            lab[i] = min(range(k), key=lambda c: sum((z[j]-cents[c][j])**2 for j in range(d)))
        for c in range(k):
            mem = [i for i in range(len(zs)) if lab[i] == c]
            if mem:
                for j in range(d):
                    cents[c][j] = sum(zs[i][j] for i in mem)/len(mem)
    return lab

labels = None
if regime == "kmeans":
    labels = kmeans(R)
    desc = "k-means clusters over z(vol20), z(bar_ret)"
else:
    vols = sorted(r["vol20"] for r in R)
    lo_t = vols[int(1/3*(len(vols)-1))]; hi_t = vols[int(2/3*(len(vols)-1))]
    labels = [0 if r["vol20"] <= lo_t else (2 if r["vol20"] > hi_t else 1) for r in R]
    desc = "vol20 terciles: 0 low, 1 mid, 2 high"

rp = []
for g in sorted(set(labels)):
    sub = [r for r, lb in zip(R, labels) if lb == g]
    if len(sub) < 5:
        continue
    hit = sum((r["p_up"] >= r["p_down"]) == (r["up"] > 0) for r in sub)/len(sub)
    mae = sum(abs(r["actual_ret"]-r["q50"]) for r in sub)/len(sub)
    bias = sum(r["actual_ret"]-r["q50"] for r in sub)/len(sub)
    band = [r for r in sub if math.isfinite(r["q10"]) and math.isfinite(r["q90"])]
    cov = (sum(r["q10"] <= r["actual_ret"] <= r["q90"] for r in band)/len(band)) if band else float("nan")
    rp.append((g, len(sub), hit, mae, bias, cov,
               sum(r["vol20"] for r in sub)/len(sub), sum(r["bar_ret"] for r in sub)/len(sub),
               sum(r["p_up"] for r in sub)/len(sub)))
with open(f"{out_dir}/regime_performance.csv","w") as f:
    f.write("regime_id,n,directional_hit_rate,mae,bias,coverage_80,mean_vol20,mean_bar_ret,mean_p_up\n")
    for g,c,hit,mae,bias,cov,mv,mr,mp in rp:
        f.write(f"{g},{c},{hit:.6f},{mae:.6f},{bias:.6f},{cov:.6f},{mv:.6f},{mr:.6f},{mp:.6f}\n")
print(f"regimes ({regime}, {desc}):")
for g,c,hit,mae,bias,cov,mv,mr,mp in rp:
    print(f"  #{g}: n={c:<6} dir_hit={pct(hit):>7} MAE={pct(mae):>8} bias={pct(bias):>8} "
          f"cov80={'-' if math.isnan(cov) else pct(cov):<7} avg_vol={mv*100:.1f}% avg_ret={mr*100:+.2f}%")

print(f"\nCSVs -> {out_dir}/{{calibration_points,quantile_coverage,tail_error_stats,regime_performance}}.csv")
PY
