#!/usr/bin/env bash
#
# stock_analysis.sh — P(up/down) over the next H trading days (default ≈2 weeks)
#
# One-shot research signal: pull a symbol's daily history through the unified
# market provider framework (`md klines`), score the latest bar with the
# historical-analog model (`fwd_proba`) and alert when the signal is strong.
#
# Usage:
#   ./stock_analysis.sh HK.00700                 # SOURCE=futu (default), HORIZON=10, K=20
#   SOURCE=yahoo ./stock_analysis.sh 0700.HK     # Yahoo free feed (Yahoo code format)
#   SOURCE=parquet FILE=./bars.parquet ./stock_analysis.sh 0700.HK   # reuse a saved table
#   HORIZON=14 THRESHOLD=0.8 PERIOD=1d ./stock_analysis.sh HK.00700
#
# Env (defaults): SYMBOL (positional), SOURCE=futu|yahoo|parquet, PERIOD=1d,
#   ADJUST=qfq, HORIZON=10, K=20, THRESHOLD=0.75 (calibrated default; override any time),
#   START=<6y back>, END=<today>,
#   FILE=(parquet), OUT_DIR=./analytics_out, GTV_PROFILE=release
#
# Exit codes: 0 ok/no alert · 1 data/param error · 3 alert triggered (cron: check 3).
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
THRESHOLD="${THRESHOLD:-0.75}"   # alert when max(p_up,p_down) >= THRESHOLD (user-adjustable)
START="${START:-$(date -d '6 years ago' +%F 2>/dev/null || date -v-6y +%F)}"
END="${END:-$(date +%F)}"
FILE="${FILE:-}"                    # required when SOURCE=parquet
OUT_DIR="${OUT_DIR:-./analytics_out}"
GTV_PROFILE="${GTV_PROFILE:-release}"
BIN="target/$GTV_PROFILE/gtv"

# --- symbol -> table name (safe for SQL identifiers) -------------------------
TBL="$(printf '%s' "$SYMBOL" | tr -c 'A-Za-z0-9' '_' | tr 'A-Z' 'a-z')_bars"

if [[ ! -x "$BIN" ]]; then
  echo "building gtv ($GTV_PROFILE)…"
  if [[ "$GTV_PROFILE" == "release" ]]; then
    cargo build --release -p gtv-cli --bin gtv
  else
    cargo build -p gtv-cli --bin gtv
  fi
fi

LOG="$(mktemp)"
trap 'rm -f "$LOG"' EXIT

mkdir -p "$OUT_DIR"
PARQUET_FILE="$OUT_DIR/${SYMBOL//./_}_bars.parquet"
TSV_FILE="$OUT_DIR/${SYMBOL//./_}_analysis.tsv"

case "$SOURCE" in
  futu)
    PROVIDER=futu
    CMD="md klines $PROVIDER $TBL $SYMBOL --period $PERIOD --start $START --end $END --adjust $ADJUST"
    ;;
  yahoo)
    PROVIDER=yahoo
    CMD="md klines $PROVIDER $TBL $SYMBOL --period $PERIOD --start $START --end $END"
    ;;
  parquet)
    [ -n "$FILE" ] && [ -f "$FILE" ] || { echo "SOURCE=parquet needs FILE=<path>" >&2; exit 1; }
    CMD="load $TBL $FILE"
    ;;
  *)
    echo "unknown SOURCE=$SOURCE (futu|yahoo|parquet)" >&2; exit 1 ;;
esac

printf '%s\nfwd_proba('"'"'%s'"'"',%s,%s)\nsave %s %s\nquit\n' "$CMD" "$TBL" "$HORIZON" "$K" "$TBL" "$PARQUET_FILE" \
  | "$BIN" > "$LOG" 2>&1 || { echo "gtv failed:"; tail -3 "$LOG" >&2; exit 1; }

# --- parse the fwd_proba table (last ASCII table in the log) -----------------
TSV="$(python3 - "$LOG" <<'PY'
import sys
log = open(sys.argv[1], encoding="utf-8", errors="replace").read()
rows = [l for l in log.splitlines() if l.startswith("|")]
tables, cur = [], []
for r in rows:
    if any(h.strip() == "p_up" for h in r.split("|")):
        tables.append(([h.strip() for h in r.split("|")], []))
    elif tables:
        tables[-1][1].append([c.strip() for c in r.split("|")])
if not tables:
    sys.exit("no fwd_proba table in output")
hdr, body = tables[-1]
line = body[0]
def col(name):
    return line[hdr.index(name)]
import datetime
t = int(col("t"))
date = datetime.datetime.fromtimestamp(t / 1_000_000_000, datetime.timezone.utc).strftime("%Y-%m-%d")
print("|".join([date, col("close"), col("p_up"), col("p_down"),
                col("hit_rate"), col("n_analogs"), col("n_labeled")]))
PY
)"

IFS='|' read -r DATE CLOSE P_UP P_DOWN HIT_RATE N_ANALOGS N_LABELED <<< "$TSV"
P_UP=$(printf '%.4f' "$P_UP"); P_DOWN=$(printf '%.4f' "$P_DOWN")
P_MAX=$(awk "BEGIN{print ($P_UP>=$P_DOWN)?$P_UP:$P_DOWN}")
DIR="DOWN"
[ "$(awk "BEGIN{print ($P_UP>=$P_DOWN)?1:0}")" = "1" ] && DIR="UP"

if awk "BEGIN{exit !($P_MAX >= $THRESHOLD)}"; then
  DECISION="${DIR}_ALERT"
else
  DECISION="NO_SIGNAL"
fi

# Persist the bars table for later reuse: SOURCE=parquet FILE=$OUT_DIR/..._bars.parquet
if [ ! -f "$TSV_FILE" ]; then
  printf 'symbol\tdate\tclose\tp_up\tp_down\tdecision\thit_rate\tn_analogs\tn_labeled\thorizon\tk\n' > "$TSV_FILE"
fi
printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
  "$SYMBOL" "$DATE" "$CLOSE" "$P_UP" "$P_DOWN" "$DECISION" "$HIT_RATE" "$N_ANALOGS" "$N_LABELED" "$HORIZON" "$K" >> "$TSV_FILE"

printf '%s | next %s td | p_up=%.2f p_down=%.2f (K=%s, hit=%.2f, n=%s)\n' \
  "$SYMBOL" "$HORIZON" "$P_UP" "$P_DOWN" "$K" "$HIT_RATE" "$N_ANALOGS"
printf '  -> %s\n' "$TSV_FILE"
if [ "$DECISION" = "NO_SIGNAL" ]; then
  printf 'no strong signal (max %.2f < %.2f)\n' "$P_MAX" "$THRESHOLD"
  exit 0
fi
printf 'ALERT: %s likely %s (%.2f >= %.2f) over next %s trading days\n' \
  "$SYMBOL" "$DIR" "$P_MAX" "$THRESHOLD" "$HORIZON"
exit 3
