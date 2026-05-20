#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Test the arrow R package built for WebAssembly.
#
# This script is intended to run inside the ghcr.io/r-universe-org/build-wasm
# Docker container after rwasm::build() has produced a .tgz binary. It:
#   1. Sets up a CRAN-like repo structure from the built .tgz
#   2. Installs the npm webr package (Node.js webR runtime)
#   3. Boots webR, installs arrow from the local repo, and verifies:
#      - The package can be installed and loaded
#      - Multithreading is disabled (arrow.use_threads == FALSE)
#      - The testthat test suite runs
#
# Tests that require threading are automatically skipped via
# skip_if_not(CanRunWithCapturedR()) since CanRunWithCapturedR() returns
# FALSE under Emscripten.
#
# Usage:
#   r_wasm_test.sh <path-to-arrow-r-dir>
#
# Example:
#   r_wasm_test.sh /work
#
# The arrow .tgz file(s) should already exist in <path-to-arrow-r-dir>.

set -euxo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
arrow_r_dir="${1:-.}"

# Set up a fake CRAN-like repo so we can install the package
tgz_file=$(ls "${arrow_r_dir}"/arrow_*.tgz 2>/dev/null | head -1)
if [ -z "${tgz_file}" ]; then
  echo "ERROR: No arrow_*.tgz found in ${arrow_r_dir}" >&2
  exit 1
fi
echo "Found Wasm binary: ${tgz_file}"

repo_dir=$(mktemp -d)
# TODO: Not sure if we need this
# Cover multiple R minor versions in case the npm webr package
# uses a different R version than the Docker image's build R.
for r_ver in 4.4 4.5 4.6; do
  contrib_dir="${repo_dir}/bin/emscripten/contrib/${r_ver}"
  mkdir -p "${contrib_dir}"
  cp "${tgz_file}" "${contrib_dir}/"
  # type=mac.binary matches .tgz file extension
  R -q -e "tools::write_PACKAGES('${contrib_dir}', type = 'mac.binary')"
done

echo "Repo structure:"
find "${repo_dir}" -type f

# Install webr in a temporary node project
work_dir=$(mktemp -d)
cd "${work_dir}"
npm init -y > /dev/null 2>&1
npm install --silent webr 2>/dev/null

# Run our test script
ARROW_WASM_REPO_DIR="${repo_dir}" NODE_PATH="${work_dir}/node_modules" node "${SCRIPT_DIR}/r_wasm_test.cjs"

# Cleanup temp dirs
rm -rf "${work_dir}" "${repo_dir}"
