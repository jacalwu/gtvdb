#!/usr/bin/env bash
#
# TC duration runner (variant 1): timing-focused, driven by the gtv catalog
# persistence (GTV_HOME). Each measurement runs in a fresh REPL whose datasets
# are auto-restored from the catalog, so the measured `duration:` lines cover
# only the query kernel — replay/load time is excluded.
#
# Usage:
#   ./testcase/run_tc_duration1.sh            # reload=Y (rebuild datasets)
#   ./testcase/run_tc_duration1.sh N          # reuse persisted datasets
#   RELOAD=N ./testcase/run_tc_duration1.sh   # same as above
#   GTV_HOME=/elsewhere ./testcase/run_tc_duration1.sh N
#
# reload=Y  : wipe GTV_HOME, reload the six fixed datasets from
#             testcase/data (CSV/Parquet) and rebuild the catalog, then measure.
# reload=N  : skip the reload phase; datasets are restored from the catalog that
#             a previous reload=Y run created. Fails fast if the catalog is
#             missing/empty (run reload=Y once first).
#
# Non-persisted session bindings are rebuilt per measurement REPL from the
# restored tables: `wash_from edges` (tc3) and `knn_from bigvec vecs` (tc4).

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/.." && pwd)"
cd "$repo"

PROFILE="${GTV_PROFILE:-release}"
BIN="${GTV_BIN:-$repo/target/$PROFILE/gtv}"

CPU_CORE="${CPU_CORE:-3}"
TASKSET="taskset -c $CPU_CORE"

DATA_CSV="$here/data/pt_test_tab.csv"
EDGES_CSV="$here/data/edges_1m.csv"
VECTORS_CSV="$here/data/vectors_1m.csv"
MCO_PARQ="$here/data/stocks_MCO_tick.parquet"
NVDA_PARQ="$here/data/stocks_NVDA_tick.parquet"
TSLA_PARQ="$here/data/stocks_TSLA_tick.parquet"

PT_T="${PT_T:-500000}"
WASH_T="${WASH_T:-1000000}"
KNN_QUERY="${KNN_QUERY:-0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5}"
VEC_DIM="${VEC_DIM:-8}"

WARMUP="${WARMUP:-3}"
ITERS="${ITERS:-15}"
RESULT_FILE="${RESULT_FILE:-$here/tc_duration_last.tsv}"

# ---------------------------------------------------------------------------
# reload switch: reload=Y reloads csv/parquet & rebuilds the tables in the
# catalog; reload=N reuses the persisted catalog and runs the tests directly.
# ---------------------------------------------------------------------------
RELOAD="${RELOAD:-Y}"
if [[ $# -ge 1 ]]; then
  RELOAD="$1"
fi
case "${RELOAD^^}" in
  Y|YES|1) RELOAD=Y ;;
  N|NO|0) RELOAD=N ;;
  *) echo "reload must be Y or N (got '$RELOAD')" >&2; exit 2 ;;
esac

# Persistence home for the catalog (share across reload=Y/N runs).
export GTV_HOME="${GTV_HOME:-$here/gtv_home}"
MANIFEST="$GTV_HOME/catalog.tsv"

[[ -x "$BIN" ]] || { echo "gtv binary not found at $BIN"; exit 1; }
for f in "$DATA_CSV" "$EDGES_CSV" "$VECTORS_CSV" "$MCO_PARQ" "$NVDA_PARQ" "$TSLA_PARQ"; do
  [[ -f "$f" ]] || { echo "dataset not found at $f"; exit 1; }
done

# Dataset tables the catalog must provide after a reload=Y run.
catalog_has_datasets() {
  [[ -f "$MANIFEST" ]] || return 1
  local t
  for t in pt_test_tab edges vecs mco nvda tsla; do
    grep -qP "^$t\t" "$MANIFEST" || return 1
  done
}

if [[ "$RELOAD" == "Y" ]]; then
  echo "reload=Y: wiping $GTV_HOME and rebuilding datasets…"
  rm -rf "$GTV_HOME"
  mkdir -p "$GTV_HOME"
else
  if ! catalog_has_datasets; then
    echo "reload=N but the catalog at $MANIFEST is missing/empty." >&2
    echo "run once with reload=Y first:  ./$0 Y" >&2
    exit 1
  fi
  echo "reload=N: reusing persisted catalog at $GTV_HOME (skip dataset reload)"
fi

# -----------------------------
# Dataset reload phase (reload=Y only); each file is loaded into its own REPL
# so the load is recorded in the catalog. Wall-clock includes the replay of
# datasets already recorded in this run (informational timing only).
# -----------------------------
time_load() {
  local cmd="$1"
  local start=$(date +%s%N)
  printf "SET DURATION = OFF\n$cmd\nquit\n" | $TASKSET "$BIN" >/dev/null 2>&1
  local end=$(date +%s%N)
  local dur=$(( (end - start) / 1000 ))
  printf "[load] %-40s %10s µs\n" "$cmd" "$dur"
}

if [[ "$RELOAD" == "Y" ]]; then
  echo "=== Loading datasets ==="
  time_load "loadcsv pt_test_tab $DATA_CSV"
  time_load "loadcsv edges $EDGES_CSV"
  time_load "loadcsv vecs $VECTORS_CSV"
  time_load "load mco $MCO_PARQ"
  time_load "load nvda $NVDA_PARQ"
  time_load "load tsla $TSLA_PARQ"
  echo "=== Dataset loading complete (catalog: $MANIFEST) ==="
fi

# -----------------------------
# TC definitions: id|op|scale|preamble|query
# The preamble re-binds non-persisted session objects from catalog-restored
# tables in each measurement REPL; it emits no `duration:` lines.
# -----------------------------
TCS=(
  "tc1|aj|1M||aj_bench pt_test_tab pt_test_tab $PT_T"
  "tc2|ofi|1M||SELECT t, ofi(bid, ask, bid_sz, ask_sz, 100) OVER (ORDER BY t) FROM pt_test_tab;"
  "tc3|wash|1M|wash_from edges|wash($WASH_T);"
  "tc4|knn|1M|knn_from bigvec vecs $VEC_DIM|SELECT id FROM knn('bigvec', '$KNN_QUERY', 3);"
  "tc5|pit|1M||SELECT count(*) FROM pt_test_tab WHERE valid_from <= $PT_T AND $PT_T < valid_to;"
  "scan_mco|count|tick||SELECT count(*) FROM mco;"
  "tc7_mco|ttrade|tick||SELECT count(*) FROM tick_to_trade('mco', 100);"
  "scan_nvda|count|tick||SELECT count(*) FROM nvda;"
  "tc7_nvda|ttrade|tick||SELECT count(*) FROM tick_to_trade('nvda', 100);"
  "scan_tsla|count|tick||SELECT count(*) FROM tsla;"
  "tc7_tsla|ttrade|tick||SELECT count(*) FROM tick_to_trade('tsla', 100);"
)

# Run one query `WARMUP` discarded + `ITERS` measured times inside a fresh
# REPL. The catalog replay (GTV_HOME) restores the datasets before the query
# runs; the preamble rebuilds non-persisted bindings. Prints one duration µs
# per measured run (the warmup lines are dropped).
run_query() {
  local preamble="$1" query="$2"
  local session=""
  local i
  [[ -n "$preamble" ]] && session+="$preamble"$'\n'
  for ((i = 0; i < WARMUP; i++)); do session+="$query"$'\n'; done
  for ((i = 0; i < ITERS; i++)); do session+="$query"$'\n'; done

  printf 'SET DURATION = ON;\n%squit\n' "$session" \
    | $TASKSET "$BIN" 2>&1 \
    | sed -n 's/^duration: \([0-9][0-9.]*\) µs.*/\1/p' \
    | tail -n +$((WARMUP + 1))
}

# -----------------------------
# Output header
# -----------------------------
printf '%-4s %-8s %-6s %14s %14s %14s %14s %10s\n' \
  "TC" "op" "scale" "min(µs)" "avg(µs)" "last avg" "Δ(µs)" "Δ%"
printf '%-4s %-8s %-6s %14s %14s %14s %14s %10s\n' \
  "----" "------" "------" "--------" "--------" "--------" "--------" "--------"

# -----------------------------
# Load previous results
# -----------------------------
declare -A last_avg=()
if [[ -f "$RESULT_FILE" ]]; then
  while IFS=$'\t' read -r id op scale q min avg; do
    [[ "$id" == "#"* ]] && continue
    [[ -z "$id" ]] && continue
    last_avg["$id|$scale"]="$avg"
  done < "$RESULT_FILE"
fi

# -----------------------------
# Run all TC
# -----------------------------
{
  echo "# bin=$(sha256sum "$BIN" | cut -d' ' -f1 | cut -c1-12)"
  echo "# date=$(date +%Y-%m-%dT%H:%M:%S)"
  echo "# reload=$RELOAD gtv_home=$GTV_HOME"
  echo "# tc    operator    scale   query   min_us  avg_us"
} > "$RESULT_FILE.new"
trap 'rm -f "$RESULT_FILE.new"' EXIT

for entry in "${TCS[@]}"; do
  IFS='|' read -r id op scale preamble query <<< "$entry"

  durs="$(run_query "$preamble" "$query")"
  if [[ -z "$durs" ]]; then
    echo "WARNING: $id: no duration lines captured (table missing? run reload=Y)" >&2
    min_us="0.000"
    avg_us="0.000"
  else
    read -r min_us avg_us <<< "$(awk 'NR==1{m=$1} {if($1<m)m=$1; s+=$1} END{printf "%.3f %.3f", m, s/NR}' <<< "$durs")"
  fi

  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$id" "$op" "$scale" "$query" "$min_us" "$avg_us" >> "$RESULT_FILE.new"

  last="${last_avg["$id|$scale"]:-}"
  if [[ -n "$last" ]]; then
    delta=$(awk -v n="$avg_us" -v o="$last" 'BEGIN{printf "%.3f", n-o}')
    pct=$(awk -v n="$avg_us" -v o="$last" 'BEGIN{if(o>0) printf "%+.1f", (n-o)/o*100; else print "-"}')
    printf '%-4s %-8s %-6s %14s %14s %14s %14s %9s%%\n' \
      "$id" "$op" "$scale" "$min_us" "$avg_us" "$last" "$delta" "$pct"
  else
    printf '%-4s %-8s %-6s %14s %14s %14s %14s %10s\n' \
      "$id" "$op" "$scale" "$min_us" "$avg_us" "—" "—" "—"
  fi
done

mv "$RESULT_FILE.new" "$RESULT_FILE"
trap - EXIT
echo
echo "results saved to $RESULT_FILE (reload=$RELOAD, warmup=$WARMUP, iters=$ITERS, catalog=$GTV_HOME)"
