#!/usr/bin/env bash
#
# CI test: hdb_flush automatic day rollover using the fake-date hook.
#
# GTV_TODAY_FILE lets a background hdb_flush task "cross midnight" without
# waiting for the real clock: the script starts the task on 2024.01.31, loads
# day-1 rows (checkpoint), flips the date file to 2024.02.01 (auto-rollover +
# hot-table clear), loads day-2 rows (checkpoint under the new date), then
# verifies both partitions exist and a hot+cold range view returns 4 rows.
#
# Usage:
#   ./testcase/test_rollover.sh                 # target/release/gtv
#   GTV_PROFILE=debug ./testcase/test_rollover.sh
#   GTV_BIN=/path/to/gtv ./testcase/test_rollover.sh

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/.." && pwd)"

PROFILE="${GTV_PROFILE:-release}"
BIN="${GTV_BIN:-$repo/target/$PROFILE/gtv}"
[[ -x "$BIN" ]] || {
  echo "gtv binary not found at $BIN" >&2
  echo "build it once with: cargo build --$PROFILE -p gtv-cli --bin gtv" >&2
  exit 2
}

TMP="$(mktemp -d)"
ROOT="$TMP/cold"
DKEY="$TMP/date_key"
FIFO="$TMP/pipe"
LOG="$TMP/repl.log"
trap 'rm -rf "$TMP"' EXIT

# day-1 / day-2 rows (epoch ns: 2024-01-31 / 2024-02-01)
printf 'symbol,price,t\nMCO,100.0,1706659200000000000\nNVDA,200.0,1706659260000000000\n' > "$TMP/d1.csv"
printf 'symbol,price,t\nMCO,101.0,1706745600000000000\nNVDA,202.0,1706745660000000000\n' > "$TMP/d2.csv"

fail() { echo "FAIL: $*" >&2; exit 1; }
ok()   { echo "ok: $*"; }

# ---------------------------------------------------------------------------
# Phase 1: background hdb_flush with a simulated midnight crossing.
# ---------------------------------------------------------------------------
echo '2024.01.31' > "$DKEY"
mkfifo "$FIFO"
{
  printf 'hdb_flush tk %s 1\n' "$ROOT"        # open day = 2024.01.31 (date file)
  printf 'loadcsv tk %s/d1.csv\n' "$TMP"      # day-1 rows arrive
  sleep 2                                     # let the checkpoint tick fire
  printf '2024.02.01' > "$DKEY"               # simulate midnight while REPL lives
  sleep 2                                     # let the auto-rollover tick fire
  printf 'loadcsv tk %s/d2.csv\n' "$TMP"      # day-2 rows arrive under the new open day
  sleep 2                                     # let the day-2 checkpoint fire
  printf 'quit\n'
} > "$FIFO" &
GTV_TODAY_FILE="$DKEY" "$BIN" < "$FIFO" > "$LOG" 2>&1 || fail "repl exited non-zero"
wait

[[ -f "$ROOT/2024.01.31/tk/MCO.parquet" ]] || fail "missing day-1 partition MCO"
[[ -f "$ROOT/2024.01.31/tk/NVDA.parquet" ]] || fail "missing day-1 partition NVDA"
ok "day-1 checkpoint partition exists (2024.01.31, 2 symbols)"

[[ -f "$ROOT/2024.02.01/tk/MCO.parquet" ]] || fail "missing day-2 partition MCO"
[[ -f "$ROOT/2024.02.01/tk/NVDA.parquet" ]] || fail "missing day-2 partition NVDA"
ok "day-2 checkpoint partition exists (2024.02.01, 2 symbols)"

grep -q 'auto-rollover' "$LOG" || fail "no auto-rollover event in hdb_flush log"
ok "hdb_flush logged an automatic rollover on the date change"

# ---------------------------------------------------------------------------
# Phase 2: a fresh session loads the full cold range (no hot table) -> 4 rows.
# ---------------------------------------------------------------------------
CNT="$(printf 'hc_load v tk 2024.01.31 2024.02.01 --root %s\nSELECT count(*) FROM v;\nquit\n' "$ROOT" \
  | GTV_TODAY_FILE="$DKEY" "$BIN" 2>&1 \
  | grep -E '^\| [0-9]+' | head -1 | tr -dc '0-9')"
[[ "$CNT" == "4" ]] || fail "expected 4 rows across both days, got '$CNT'"
ok "cold range view returns 4 rows (2 per day)"

# ---------------------------------------------------------------------------
# Phase 3: GTV_TODAY static override drives `rollover`'s default date.
# ---------------------------------------------------------------------------
( cd "$TMP" && printf 'loadcsv tk %s/d1.csv\nrollover tk\nquit\n' "$TMP" \
    | GTV_TODAY=2025.06.30 "$BIN" >/dev/null 2>&1 )
[[ -f "$TMP/hdb/2025.06.30/tk/MCO.parquet" ]] || fail "GTV_TODAY override did not pin rollover date"
ok "GTV_TODAY=2025.06.30 pins rollover's default date"

echo
echo "PASS: automatic day rollover (fake-date hook)"
