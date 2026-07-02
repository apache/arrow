#!/usr/bin/env bash
# Build and test the Arrow R package for WebAssembly locally,
# replicating the test-r-wasm crossbow job exactly.
#
# Usage: bash r/run-wasm-build-and-test.sh
# Run from the repo root.

set -euxo pipefail

cd "$(git rev-parse --show-toplevel)/r"

# Step 1: sync C++ source and build R source tarball (done on host, not in Docker)
make sync-cpp
R CMD build --no-build-vignettes .

# Step 2: build Wasm binary inside the R-universe container
docker run --rm \
  -v "${PWD}:/work" \
  -w /work \
  ghcr.io/r-universe-org/build-wasm:latest \
  bash -lc '
    set -euxo pipefail
    R -q -e "if (!requireNamespace(\"pak\", quietly = TRUE)) install.packages(\"pak\", repos = \"https://cloud.r-project.org\"); if (!requireNamespace(\"rwasm\", quietly = TRUE)) pak::pak(\"r-wasm/rwasm\"); rwasm::build(\".\")" \
      2>&1 | tee build-wasm.log
  '

# Step 3: run the test suite inside the R-universe container
ARROW_ROOT="$(git rev-parse --show-toplevel)"
docker run --rm \
  -v "${ARROW_ROOT}:/arrow" \
  -w /tmp \
  ghcr.io/r-universe-org/build-wasm:latest \
  bash /arrow/ci/scripts/r_wasm_test.sh /arrow/r
