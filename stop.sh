#!/usr/bin/env bash
#
# Stop a gtv-server started by start.sh.
#
# Sends SIGTERM and waits briefly for a graceful shutdown; escalates to SIGKILL
# if the process hangs. Removes the PID file either way.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$here"

PIDFILE="target/gtv-server.pid"

if [[ ! -f "$PIDFILE" ]]; then
  echo "gtv-server is not running (no pidfile)." >&2
  exit 0
fi

pid="$(cat "$PIDFILE")"
if ! kill -0 "$pid" 2>/dev/null; then
  echo "gtv-server (pid $pid) is not running; removing stale pidfile."
  rm -f "$PIDFILE"
  exit 0
fi

echo "stopping gtv-server (pid $pid)…"
kill "$pid"

for _ in {1..50}; do
  kill -0 "$pid" 2>/dev/null || break
  sleep 0.1
done

if kill -0 "$pid" 2>/dev/null; then
  echo "gtv-server did not stop gracefully; sending SIGKILL." >&2
  kill -9 "$pid" || true
fi

rm -f "$PIDFILE"
echo "gtv-server stopped."
