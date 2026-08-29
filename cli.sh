#!/usr/bin/env bash
#
# Launch the gtv interactive shell (REPL).
#
# The REPL starts with the local demo dataset. To run SQL against a running
# gtv-server (see start.sh), use the `remote` command inside the shell:
#
#   remote <host:port> <sql>
#
# e.g.
#   remote 127.0.0.1:50051 SELECT * FROM prices
#
# Usage:
#   ./cli.sh
#   GTV_PROFILE=debug ./cli.sh
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$here"

PROFILE="${GTV_PROFILE:-release}"
BIN="target/$PROFILE/gtv"

case "$PROFILE" in
  release|debug) ;;
  *) echo "GTV_PROFILE must be 'release' or 'debug' (got '$PROFILE')" >&2; exit 2 ;;
esac

if [[ ! -x "$BIN" ]]; then
  echo "building gtv ($PROFILE)…"
  if [[ "$PROFILE" == "release" ]]; then
    cargo build --release -p gtv-cli --bin gtv
  else
    cargo build -p gtv-cli --bin gtv
  fi
fi

exec "$BIN"
