#!/usr/bin/env bash
#
# rebuild.sh — rebuild the gtv binaries after Rust/embedded-python changes.
#
# The REPL/server launchers (`cli.sh`, `start.sh`, `stock_analysis.sh`) reuse an
# existing binary and do NOT recompile, so after any `crates/` change you must
# rebuild — this script is the one-stop way. Remember: `gtv-engine` embeds
# `python/futu_bridge.py` via include_str!, so even a pure-python edit requires
# a rebuild to take effect.
#
# Usage:
#   ./rebuild.sh                 # release gtv (default)
#   ./rebuild.sh debug           # debug gtv
#   ./rebuild.sh --server        # release gtv + gtv-server (gRPC endpoint)
#   ./rebuild.sh check           # fast cargo check (no link) — CI-style
#
# Then launch with ./cli.sh / ./start.sh / ./stock_analysis.sh as usual.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$here"

PROFILE="${1:-release}"
EXTRA=""
case "$PROFILE" in
  release) PROFILE=release ;;
  debug)   PROFILE=debug ;;
  check)   exec cargo check -p gtv-cli -p gtv-server ;;
  --server) PROFILE=release; EXTRA="server" ;;
  -h|--help) sed -n '1,20p' "$0"; exit 0 ;;
  *) echo "usage: $0 [release|debug|check|--server]" >&2; exit 2 ;;
esac

CARGO_FLAGS=(--profile "$PROFILE")
if [ "$PROFILE" = "release" ]; then
  CARGO_FLAGS=(--release)
fi

echo "== rebuilding gtv ($PROFILE) =="
cargo build "${CARGO_FLAGS[@]}" -p gtv-cli --bin gtv
if [ -n "$EXTRA" ]; then
  cargo build "${CARGO_FLAGS[@]}" -p gtv-server --bin gtv-server
fi

BIN="target/$PROFILE/gtv"
echo
echo "ok — $BIN is up to date ($(date '+%H:%M:%S'))"
case "$PROFILE" in
  release)
    echo "  ./cli.sh                     # REPL"
    echo "  ./start.sh                   # gtv-server (gRPC)"
    echo "  ./stock_analysis.sh HK.00700 # trend signal (SOURCE=futu)"
    echo "  ./stock_calib.sh HK.00700    # threshold calibration"
    ;;
  debug)
    echo "  GTV_PROFILE=debug ./cli.sh"
    echo "  GTV_PROFILE=debug ./stock_analysis.sh HK.00700"
    ;;
esac
