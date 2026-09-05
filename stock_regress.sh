#!/usr/bin/env bash
#
# stock_regress.sh — as-of-date regression test of the trend/range forecast
#
# Replays the exact production decision (historical-analog fwd_proba on the
# bars up to an as-of day) at chosen past dates, then compares the forecast —
# direction (p_up/p_down) and predicted price band (pred_lo..pred_hi) — with
# the realised next-H-bar path, to judge whether the algorithm is reasonable.
#
# Usage:
#   ./stock_regress.sh HK.00700 2026-08-03               # one as-of date
#   ./stock_regress.sh HK.00700 2026-08-03 2026-07-01 2026-06-03
#   ASOF_DATES=2026-08-03,2026-07-01 ./stock_regress.sh HK.00700
#   ./stock_regress.sh HK.00700                          # auto: ~12 monthly dates
#   SOURCE=yahoo ./stock_regress.sh AAPL 2026-07-01
#
# Env: SYMBOL (positional), SOURCE=futu|yahoo|parquet, PERIOD=1d, ADJUST=qfq,
#   HORIZON=10, K=20, START=<6y back>, END=<today>, FILE=(parquet),
#   OUT_DIR=./analytics_out, GTV_PROFILE=release
#
# Output: one line per date (forecast vs realised + band coverage + ret error)
# and an aggregate summary (directional hit, band coverage, MAE/median error).
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$here"

SYMBOL="${SYMBOL:-${1:-}}"
[ -n "$SYMBOL" ] || { echo "usage: $0 <SYMBOL> [ASOF_DATE ...]" >&2; exit 1; }

SOURCE="${SOURCE:-futu}"
PERIOD="${PERIOD:-1d}"
ADJUST="${ADJUST:-qfq}"
HORIZON="${HORIZON:-10}"
K="${K:-20}"
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

# as-of dates: explicit args > ASOF_DATES > auto monthly set ending ~25d before today
if [ "$#" -gt 1 ]; then
  ASOF_LIST=("${@:2}")
elif [ -n "${ASOF_DATES:-}" ]; then
  IFS=',' read -r -a ASOF_LIST <<< "$ASOF_DATES"
else
  ASOF_LIST=()
  for k in $(seq 1 12); do
    ASOF_LIST+=("$(date -d "$END - $((25 + k * 30)) days" +%F 2>/dev/null || date -v-${k}m -v-25d +%F)")
  done
fi

# ---- 1) prepare the daily-bars table (fetched once, cached as parquet) ------
MD_LINE=""
case "$SOURCE" in
  futu)   MD_LINE="md klines futu $TBL $SYMBOL --period $PERIOD --start $START --end $END --adjust $ADJUST" ;;
  yahoo)  MD_LINE="md klines yahoo $TBL $SYMBOL --period $PERIOD --start $START --end $END" ;;
  parquet)
    [ -n "$FILE" ] && [ -f "$FILE" ] || { echo "SOURCE=parquet needs FILE=<path>" >&2; exit 1; }
    if [ "$(realpath "$FILE")" != "$(realpath "$PARQUET_FILE")" ]; then
      cp "$FILE" "$PARQUET_FILE"
    fi ;;
  *) echo "unknown SOURCE=$SOURCE (futu|yahoo|parquet)" >&2; exit 1 ;;
esac

# as-of epoch ns (UTC midnight) per date
NS_LIST=()
for d in "${ASOF_LIST[@]}"; do
  sec="$(date -u -d "$d" +%s 2>/dev/null || date -u -j -f '%F' "$d" +%s)"
  NS_LIST+=("$((sec * 1000000000))")
done

PREP_LOG="$(mktemp)"; REG_LOG="$(mktemp)"
trap 'rm -f "$PREP_LOG" "$REG_LOG"' EXIT

# fetch+save in one session when cache missing
if [ ! -f "$PARQUET_FILE" ] && [ -n "$MD_LINE" ]; then
  printf '%s\nsave %s %s\nquit\n' "$MD_LINE" "$TBL" "$PARQUET_FILE" \
    | "$BIN" > "$PREP_LOG" 2>&1 || { echo "gtv prepare failed:"; tail -3 "$PREP_LOG" >&2; exit 1; }
fi

# ---- 2) one session: load + one fwd_regress call per as-of date ------------
{
  echo "load $TBL $PARQUET_FILE"
  for ns in "${NS_LIST[@]}"; do
    echo "fwd_regress('$TBL',$ns,$HORIZON,$K)"
  done
  echo "quit"
} | "$BIN" > "$REG_LOG" 2>&1 || { echo "gtv regress failed:"; tail -5 "$REG_LOG" >&2; exit 1; }

python3 - "$REG_LOG" "$SYMBOL" "$HORIZON" "$K" "$OUT_DIR" <<'PY'
import sys
log = open(sys.argv[1], encoding="utf-8", errors="replace").read()
sym, H, K, out_dir = sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), sys.argv[5]

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
    sys.exit("no fwd_regress table in output")

def num(hdr, row, name):
    return float(row[hdr.index(name)])

rows = []
for hdr, body in tables:
    if not body:
        continue
    cells = body[0]
    t = int(num(hdr, cells, "t"))
    import datetime
    date = datetime.datetime.fromtimestamp(t / 1e9, datetime.timezone.utc).strftime("%Y-%m-%d")
    up = int(num(hdr, cells, "up"))
    rows.append({
        "date": date,
        "close": num(hdr, cells, "close"),
        "p_up": num(hdr, cells, "p_up"), "p_down": num(hdr, cells, "p_down"),
        "pred_lo": num(hdr, cells, "pred_lo"), "pred_hi": num(hdr, cells, "pred_hi"),
        "pred_ret": num(hdr, cells, "pred_ret"),
        "act_lo": num(hdr, cells, "act_lo"), "act_hi": num(hdr, cells, "act_hi"),
        "act_ret": num(hdr, cells, "act_ret"),
        "up": up, "n_future": int(num(hdr, cells, "n_future")),
    })

def pct(x): return f"{x*100:+.2f}%"
def pct0(x): return f"{x*100:.2f}%"
print(f"=== as-of regression: {sym} | next {H} trading days, K={K} ===")
print(f"{'asof':<12}{'close':>9}{'p_up':>7}{'p_dn':>7}{'pred band':>17}{'realised':>17}{'pred ret':>10}{'act ret':>10}{'cov':>5}{'hit':>5}")

valid = []
tsv = [f"asof\tclose\tp_up\tp_down\tpred_lo\tpred_hi\tpred_ret\tact_lo\tact_hi\tact_ret\tdir_hit\tband_cov\tn_future"]
for r in sorted(rows, key=lambda x: x["date"]):
    dir_up = r["p_up"] >= r["p_down"]
    hit = (dir_up == (r["up"] == 1)) if r["up"] >= 0 else None
    cov = (r["act_lo"] >= r["pred_lo"]) and (r["act_hi"] <= r["pred_hi"]) if r["up"] >= 0 else None
    if r["up"] < 0:
        print(f"{r['date']:<12}{r['close']:>9.2f}   (not enough future bars after as-of; n_future={r['n_future']})")
        continue
    valid.append((r, hit, cov))
    band = f"[{pct0(r['pred_lo'])},{pct0(r['pred_hi'])}]"
    act = f"[{pct0(r['act_lo'])},{pct0(r['act_hi'])}]"
    print(f"{r['date']:<12}{r['close']:>9.2f}{pct0(r['p_up']):>7}{pct0(r['p_down']):>7}"
          f"{band:>17}{act:>17}{pct(r['pred_ret']):>10}{pct(r['act_ret']):>10}"
          f"{'Y' if cov else 'N':>5}{'Y' if hit else 'N':>5}")
    tsv.append("\t".join([
        r["date"], f"{r['close']:.4f}", f"{r['p_up']:.4f}", f"{r['p_down']:.4f}",
        f"{r['pred_lo']:.6f}", f"{r['pred_hi']:.6f}", f"{r['pred_ret']:.6f}",
        f"{r['act_lo']:.6f}", f"{r['act_hi']:.6f}", f"{r['act_ret']:.6f}",
        "1" if hit else "0", "1" if cov else "0", str(r["n_future"]),
    ]))

if valid:
    n = len(valid)
    hits = sum(1 for _, h, _ in valid if h)
    covs = sum(1 for _, _, c in valid if c)
    errs = [r["act_ret"] - r["pred_ret"] for r, _, _ in valid]
    errs_s = sorted(errs)
    med = errs_s[len(errs_s)//2]
    mae = sum(abs(e) for e in errs) / n
    bias = sum(errs) / n
    pred_up = sum(1 for r, _, _ in valid if r["p_up"] >= r["p_down"])
    print(f"\n=== aggregate ({n} as-of dates) ===")
    print(f"directional hit       : {hits}/{n} = {hits/n*100:.1f}%   (up-signals {pred_up}, base up {sum(r['up']==1 for r,_,_ in valid)/n*100:.0f}%)")
    print(f"band coverage         : {covs}/{n} = {covs/n*100:.1f}%   (realised low/high inside pred_lo..pred_hi)")
    print(f"ret error act-pred    : bias {bias*100:+.2f}pp | MAE {mae*100:.2f}pp | median {med*100:+.2f}pp")
    print(f"mean |pred band|      : {sum(r['pred_hi']-r['pred_lo'] for r,_,_ in valid)/n*100:.2f}pp "
          f"(vs mean |actual range| {sum(r['act_hi']-r['act_lo'] for r,_,_ in valid)/n*100:.2f}pp)")
else:
    print("\nno as-of date had enough future bars to evaluate")

import os
with open(f"{out_dir}/{sym.replace('.', '_')}_regress.tsv", "w") as f:
    f.write("\n".join(tsv) + "\n")
print(f"\nreport: {out_dir}/{sym.replace('.', '_')}_regress.tsv")
PY
