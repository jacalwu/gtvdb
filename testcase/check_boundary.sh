#!/usr/bin/env bash
#
# prod_p4 boundary check: the analysis engine must never depend on an
# enterprise-batch crate. Dependency direction is one-way:
#
#     enterprise crates  ->  analysis engine crates
#
# The composition roots (gtv-cli, gtv-server) are *exempt*: they own the
# enterprise registry and register its UDF / table functions on the DataFusion
# session (see doc/prod_p4_design.md §3). The kernel crates themselves must not
# link enterprise code.
#
# Run it in CI next to `cargo test --workspace`.
#
# Usage: ./testcase/check_boundary.sh

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/.." && pwd)"

# analysis-engine kernel crates: must not depend on any enterprise crate
kernel_crates=(
  gtv-core gtv-array gtv-engine gtv-index gtv-pattern
  gtv-catalog gtv-storage gtv-delta gtv-udf gtv-proto
  gtv-index-store gtv-ingest
)

# composition roots: exempt (they register the enterprise SQL surface)
composition_roots=(
  gtv-cli gtv-server
)

# enterprise-batch crates (prod_p4)
enterprise_crates=(
  gtv-scenario gtv-refdata gtv-governance gtv-observe gtv-security gtv-ops
  gtv-enterprise-sql gtv-largeexposure
)

fail=0
for e in "${kernel_crates[@]}"; do
  manifest="$repo/crates/$e/Cargo.toml"
  [[ -f "$manifest" ]] || continue
  for x in "${enterprise_crates[@]}"; do
    if grep -Eq "^[[:space:]]*${x}[[:space:]]*=" "$manifest"; then
      echo "BOUNDARY VIOLATION: kernel crate '$e' depends on enterprise crate '$x'" >&2
      fail=1
    fi
  done
done

if [[ $fail -ne 0 ]]; then
  echo "FAIL: enterprise crates must not be pulled into the analysis engine path" >&2
  exit 1
fi

echo "PASS: analysis engine kernel has no dependency on enterprise-batch crates"
echo "      (composition roots exempt: ${composition_roots[*]})"
