#!/usr/bin/env bash
#
# prod_p4 boundary check: the analysis engine must never depend on an
# enterprise-batch crate. Dependency direction is one-way:
#
#     enterprise crates  ->  analysis engine crates
#
# This script fails if any engine / kernel crate lists an enterprise crate in
# its Cargo.toml. Run it in CI next to `cargo test --workspace`.
#
# Usage: ./testcase/check_boundary.sh

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/.." && pwd)"

# crates that make up the analysis engine main path
engine_crates=(
  gtv-core gtv-array gtv-engine gtv-index gtv-pattern
  gtv-catalog gtv-storage gtv-delta gtv-udf gtv-proto gtv-server gtv-cli
  gtv-index-store gtv-ingest
)

# enterprise-batch crates (prod_p4)
enterprise_crates=(
  gtv-scenario gtv-refdata gtv-governance gtv-observe gtv-security gtv-ops
)

fail=0
for e in "${engine_crates[@]}"; do
  manifest="$repo/crates/$e/Cargo.toml"
  [[ -f "$manifest" ]] || continue
  for x in "${enterprise_crates[@]}"; do
    if grep -Eq "^[[:space:]]*${x}[[:space:]]*=" "$manifest"; then
      echo "BOUNDARY VIOLATION: engine crate '$e' depends on enterprise crate '$x'" >&2
      fail=1
    fi
  done
done

if [[ $fail -ne 0 ]]; then
  echo "FAIL: enterprise crates must not be pulled into the analysis engine path" >&2
  exit 1
fi

echo "PASS: analysis engine has no dependency on enterprise-batch crates"
