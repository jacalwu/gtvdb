#!/usr/bin/env bash
#
# Build and start the gtv-server gRPC endpoint in the background.
#
# Usage:
#   ./start.sh                          # build (release) and start on 0.0.0.0:50051
#   GTV_ADDR=0.0.0.0:50052 ./start.sh   # bind a different address
#   GTV_PROFILE=debug ./start.sh        # use a debug build (faster to compile)
#
# Runtime artifacts (PID file + log) are written under target/ (gitignored).
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$here"

PROFILE="${GTV_PROFILE:-release}"
ADDR="${GTV_ADDR:-0.0.0.0:50051}"
BIN="target/$PROFILE/gtv-server"
PIDFILE="target/gtv-server.pid"
LOGFILE="target/gtv-server.log"

case "$PROFILE" in
  release|debug) ;;
  *) echo "GTV_PROFILE must be 'release' or 'debug' (got '$PROFILE')" >&2; exit 2 ;;
esac

if [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
  echo "gtv-server is already running (pid $(cat "$PIDFILE")). Use ./stop.sh first." >&2
  exit 1
fi
rm -f "$PIDFILE"

echo "building gtv-server ($PROFILE)…"
if [[ "$PROFILE" == "release" ]]; then
  cargo build --release -p gtv-server --bin gtv-server
else
  cargo build -p gtv-server --bin gtv-server
fi

echo "starting gtv-server on $ADDR (log: $LOGFILE)"
GTV_ADDR="$ADDR" nohup "$BIN" >"$LOGFILE" 2>&1 &
pid=$!
echo "$pid" > "$PIDFILE"

# Give it a beat to bind; if it died right away (e.g. port already in use),
# surface the log tail instead of pretending it started.
sleep 1
if kill -0 "$pid" 2>/dev/null; then
  echo "gtv-server started (pid $pid) on $ADDR"
else
  echo "gtv-server exited immediately. Last log lines:" >&2
  tail -n 20 "$LOGFILE" >&2 || true
  rm -f "$PIDFILE"
  exit 1
fi
