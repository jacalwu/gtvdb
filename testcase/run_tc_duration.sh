#!/usr/bin/env bash
#
# Run TC1–TC5 (plus real tick-data tests) through the gtv REPL against FIXED
# standard datasets. Nothing is regenerated at run time — the committed files
# below are loaded as-is, so every machine / code version tests identical bytes.
#
#   testcase/data/pt_test_tab.csv          1M rows  -> pt_test_tab (TC1/TC2/TC5)
#   testcase/data/edges_1m.csv             1M edges -> edges      (TC3 wash)
#   testcase/data/vectors_1m.csv           1M × 8-d -> vecs       (TC4 knn)
#   testcase/data/stocks_{MCO,NVDA,TSLA}_tick.parquet  2.5M rows  (scan / TC7)
#
# The `*_from <table>` REPL commands (added in Phase 2) bind the operators to a
# loaded table, so TC1/TC3/TC4 now run on the same fixed data as TC2/TC5.
#
# Each test case runs once as warmup (discarded) and then `ITERS` measured runs
# (default 5); the script reports the min and the mean of those runs. The
# `Δ(µs)` / `Δ%` columns compare this round's **average** with the previous
# round's average (loaded from $RESULT_FILE). Results are persisted to a TSV
# with the binary sha + dataset shas for cross-version attribution.
#
# Usage:
#   ./testcase/run_tc_duration.sh
#   ITERS=10 ./testcase/run_tc_duration.sh
#   GTV_PROFILE=debug ./testcase/run_tc_duration.sh
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/.." && pwd)"
cd "$repo"

PROFILE="${GTV_PROFILE:-release}"
BIN="${GTV_BIN:-$repo/target/$PROFILE/gtv}"
#BIN="${GTV_BIN:-taskset -c 3 $repo/target/$PROFILE/gtv}"
DATA_CSV="${DATA_CSV:-$here/data/pt_test_tab.csv}"
EDGES_CSV="${EDGES_CSV:-$here/data/edges_1m.csv}"
VECTORS_CSV="${VECTORS_CSV:-$here/data/vectors_1m.csv}"
MCO_PARQ="${MCO_PARQ:-$here/data/stocks_MCO_tick.parquet}"
NVDA_PARQ="${NVDA_PARQ:-$here/data/stocks_NVDA_tick.parquet}"
TSLA_PARQ="${TSLA_PARQ:-$here/data/stocks_TSLA_tick.parquet}"
PT_T="${PT_T:-500000}"                                  # point-in-time query time
WASH_T="${WASH_T:-1000000}"                             # wash query time (all edges active)
KNN_QUERY="${KNN_QUERY:-0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5}"
VEC_DIM="${VEC_DIM:-8}"
ITERS="${ITERS:-10}"
WARMUP="${WARMUP:-3}"
RESULT_FILE="${RESULT_FILE:-$here/tc_duration_last.tsv}"

case "$PROFILE" in
  release|debug) ;;
  *) echo "GTV_PROFILE must be 'release' or 'debug' (got '$PROFILE')" >&2; exit 2 ;;
esac

if [[ ! -x "$BIN" ]]; then
  echo "gtv banary not found at $BIN" >&2
  echo "build it once with: cargo build --$PROFILE -p gtv-cli --bin gtv" >&2
  exit 1
fi

for f in "$DATA_CSV" "$EDGES_CSV" "$VECTORS_CSV" "$MCO_PARQ" "$NVDA_PARQ" "$TSLA_PARQ"; do
  [[ -f "$f" ]] || { echo "dataset not found at $f" >&2; exit 1; }
done

# ---------------------------------------------------------------------------
# Reproducibility metadata: pin the binary and every dataset so a later round
# can be attributed to a specific code version + identical data.
# ---------------------------------------------------------------------------
BIN_SHA="$(sha256sum "$BIN" | cut -d' ' -f1 | cut -c1-12)"
GIT_SHA="$(git -C "$repo" rev-parse --short HEAD 2>/dev/null || echo unknown)"
DATA_SHA="$(sha256sum "$DATA_CSV" | cut -d' ' -f1 | cut -c1-12)"
EDGES_SHA="$(sha256sum "$EDGES_CSV" | cut -d' ' -f1 | cut -c1-12)"
VECTORS_SHA="$(sha256sum "$VECTORS_CSV" | cut -d' ' -f1 | cut -c1-12)"
MCO_SHA="$(sha256sum "$MCO_PARQ" | cut -d' ' -f1 | cut -c1-12)"
NVDA_SHA="$(sha256sum "$NVDA_PARQ" | cut -d' ' -f1 | cut -c1-12)"
TSLA_SHA="$(sha256sum "$TSLA_PARQ" | cut -d' ' -f1 | cut -c1-12)"
DATA_ROWS="$(wc -l < "$DATA_CSV")"   # includes the header line
NOW="$(date +%Y-%m-%dT%H:%M:%S)"

# ---------------------------------------------------------------------------
# TC definitions:  id | operator | scale | preamble(; separated, may be empty) | query
# ---------------------------------------------------------------------------
TCS=(
  "tc1|aj|1M|loadcsv pt_test_tab $DATA_CSV|aj_bench pt_test_tab pt_test_tab 500000"
  "tc2|ofi|1M|loadcsv pt_test_tab $DATA_CSV|SELECT t, ofi(bid, ask, bid_sz, ask_sz, 100) OVER (ORDER BY t) FROM pt_test_tab;"
  "tc3|wash|1M|loadcsv edges $EDGES_CSV; wash_from edges|wash($WASH_T);"
  "tc4|knn|1M|loadcsv vecs $VECTORS_CSV; knn_from bigvec vecs $VEC_DIM|SELECT id FROM knn('bigvec', '$KNN_QUERY', 3);"
  "tc5|pit|1M|loadcsv pt_test_tab $DATA_CSV|SELECT count(*) FROM pt_test_tab WHERE valid_from <= $PT_T AND $PT_T < valid_to;"
  "scan_mco|count|tick|load mco $MCO_PARQ|SELECT count(*) FROM mco;"
  "tc7_mco|ttrade|tick|load mco $MCO_PARQ|SELECT count(*) FROM tick_to_trade('mco', 100);"
  "scan_nvda|count|tick|load nvda $NVDA_PARQ|SELECT count(*) FROM nvda;"
  "tc7_nvda|ttrade|tick|load nvda $NVDA_PARQ|SELECT count(*) FROM tick_to_trade('nvda', 100);"
  "scan_tsla|count|tick|load tsla $TSLA_PARQ|SELECT count(*) FROM tsla;"
  "tc7_tsla|ttrade|tick|load tsla $TSLA_PARQ|SELECT count(*) FROM tick_to_trade('tsla', 100);"
)

# Run one query `iters` times (plus `warmup` discarded runs) in a fresh REPL
# session and print the measured durations in µs, one per line.
run_query() {
  sleep 0.1
  local preamble="$1" query="$2"
  local session=""
  local i
  # preamble runs once (before any measured query); it prints no `duration:` line.
  # NOTE: REPL built-in commands (loadcsv/load/aj_from/…) take raw args, so do NOT
  # append a trailing `;` here — it would become part of the last argument.
  # `tr ';' '\n'` splits multiple preamble commands onto separate lines.
  if [[ -n "$preamble" ]]; then
    while IFS= read -r cmd; do
      [[ -z "$cmd" ]] && continue
      session+="$cmd"$'\n'
    done < <(tr ';' '\n' <<< "$preamble")
  fi
  for ((i = 0; i < WARMUP; i++)); do session+="$query"$'\n'; done
  for ((i = 0; i < ITERS; i++)); do session+="$query"$'\n'; done

  printf 'SET DURATION = ON;\n%squit\n' "$session" \
    | taskset -c 3 "$BIN" 2>&1 \
    | sed -n 's/^duration: \([0-9][0-9.]*\) µs.*/\1/p' \
    | tail -n +$((WARMUP + 1))
}

# Load the previous round keyed by "tc|scale": value = avg_us.
declare -A last_avg=()
last_bin=""; last_data=""
if [[ -f "$RESULT_FILE" ]]; then
  while IFS=$'\t' read -r id op scale q min avg; do
    if [[ "$id" == "#"* ]]; then
      case "$id" in
        "# bin="*) last_bin="${id#*bin=}" ;;
        "# data"*) last_data="$id" ;;
      esac
      continue
    fi
    [[ -z "$id" ]] && continue
    if [[ -z "$avg" ]]; then
      last_avg["$id|demo"]="$q"
    else
      last_avg["$id|$scale"]="$avg"
    fi
  done < "$RESULT_FILE"
fi

{
  printf '# bin=%s git=%s date=%s\n' "$BIN_SHA" "$GIT_SHA" "$NOW"
  printf '# data pt_test_tab=%s edges=%s vectors=%s\n' "$DATA_SHA" "$EDGES_SHA" "$VECTORS_SHA"
  printf '# data mco=%s nvda=%s tsla=%s (2.5M tick rows each)\n' "$MCO_SHA" "$NVDA_SHA" "$TSLA_SHA"
  printf '# tc\toperator\tscale\tquery\tmin_us\tavg_us\n'
} > "$RESULT_FILE.new"

printf '%-4s %-8s %-6s %14s %14s %14s %14s %10s\n' \
  "TC" "op" "scale" "min(µs)" "avg(µs)" "last avg" "Δ(µs)" "Δ%"
printf '%-4s %-8s %-6s %14s %14s %14s %14s %10s\n' \
  "----" "------" "------" "--------" "--------" "--------" "--------" "--------"

for entry in "${TCS[@]}"; do
  IFS='|' read -r id op scale preamble query <<< "$entry"
  durs="$(run_query "$preamble" "$query")"
  [[ -n "$durs" ]] || { echo "no duration lines captured for $id" >&2; exit 1; }

  read -r min_us avg_us <<< "$(awk 'NR==1{m=$1} {if($1<m)m=$1; s+=$1} END{printf "%.3f %.3f", m, s/NR}' <<< "$durs")"

  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$id" "$op" "$scale" "$query" "$min_us" "$avg_us" >> "$RESULT_FILE.new"

  if [[ -n "${last_avg["$id|$scale"]:-}" ]]; then
    delta="$(awk -v n="$avg_us" -v o="${last_avg["$id|$scale"]}" 'BEGIN{printf "%.3f", n-o}')"
    pct="$(awk -v n="$avg_us" -v o="${last_avg["$id|$scale"]}" 'BEGIN{if(o>0) printf "%+.1f", (n-o)/o*100; else print "-"}')"
    printf '%-4s %-8s %-6s %14s %14s %14s %14s %9s%%\n' \
      "$id" "$op" "$scale" "$min_us" "$avg_us" "${last_avg["$id|$scale"]}" "$delta" "$pct"
  else
    printf '%-4s %-8s %-6s %14s %14s %14s %14s %10s\n' \
      "$id" "$op" "$scale" "$min_us" "$avg_us" "—" "—" "—"
  fi
done

mv "$RESULT_FILE.new" "$RESULT_FILE"
echo
echo "results saved to $RESULT_FILE (avg = mean of $ITERS measured runs, warmup=$WARMUP)"
echo "datasets: pt_test_tab=$DATA_SHA edges=$EDGES_SHA vectors=$VECTORS_SHA"
echo "tick data: mco=$MCO_SHA nvda=$NVDA_SHA tsla=$TSLA_SHA (2.5M rows each)"
echo "run  : bin=$BIN_SHA git=$GIT_SHA date=$NOW"
if [[ -n "$last_bin" ]]; then
  if [[ "$last_bin" == *"$BIN_SHA"* ]]; then
    echo "vs last round: SAME binary → Δ is run-to-run noise"
  else
    echo "vs last round: binary CHANGED ($last_bin → bin=$BIN_SHA) → Δ is a code-version comparison"
  fi
  if [[ -n "$last_data" && "$last_data" != *"pt_test_tab=$DATA_SHA"* ]]; then
    echo "WARNING: dataset changed vs last round ($last_data) — Δ is NOT same-data"
  fi
fi
