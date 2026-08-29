#!/usr/bin/env bash
#
# Runs the gtvdb REPL golden tests.
#
# Single-node (default):
#   GTV_BIN=/path/to/gtv ./run_tests.sh
#
# P5 distributed (once implemented) — set a client that speaks the same REPL
# line protocol (or SQL over gRPC) and produces the same stdout shape:
#   GTV_P5_CLIENT="/path/to/p5-client --endpoint localhost:50051" ./run_tests.sh
#
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GTV_BIN="${GTV_BIN:-$here/../target/debug/gtv}"
GTV_P5_CLIENT="${GTV_P5_CLIENT:-}"
EXPECTED="$here/expected"
SCRIPTS="$here/scripts"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# The save/load test writes to this fixed path (also baked into its golden file).
mkdir -p /tmp/gtvdb_testcase
trap 'rm -rf "$TMP" /tmp/gtvdb_testcase/prices.parquet' EXIT

run_one() {
  local script="$1"
  local out="$2"
  if [[ -n "$GTV_P5_CLIENT" ]]; then
    # P5 client hook: replay the script through the distributed endpoint.
    $GTV_P5_CLIENT < "$script" | tail -n +2 > "$out"
  else
    "$GTV_BIN" < "$script" 2>&1 | tail -n +2 > "$out"
  fi
}

fail=0
count=0
for script in "$SCRIPTS"/*.gtv; do
  name="$(basename "$script" .gtv)"
  golden="$EXPECTED/$name.txt"
  [[ -f "$golden" ]] || { echo "SKIP $name (no golden)"; continue; }
  count=$((count + 1))
  out="$TMP/$name.out"
  if run_one "$script" "$out" && diff -u "$golden" "$out"; then
    echo "PASS $name"
  else
    echo "FAIL $name"
    fail=1
  fi
done

echo "----"
echo "ran $count test(s)"
exit "$fail"
