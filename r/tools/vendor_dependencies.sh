#!/usr/bin/env bash
#
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

# libarrow requires some thirdparty dependencies, even for a minimal build.
# This script downloads the source tarballs that libarrow would otherwise
# download at build time, so that they can be shipped in the R package.
# nixlibs.R uses them when the dependencies are not found on the system, so
# that building without network access (e.g. on CRAN) still works.
#
# Usage: tools/vendor_dependencies.sh [DESTDIR]
# Must be run after tools/cpp has been synced (see `make sync-cpp`).

set -euo pipefail

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DESTDIR="${1:-${SOURCE_DIR}/vendored}"

# If you change this list, you also need to edit
# `vendored_dependencies` in nixlibs.R
VENDORED_DEPENDENCIES=(
  ARROW_URIPARSER_URL
  ARROW_XSIMD_URL
)

# Load `DEPENDENCIES` and the *_BUILD_SHA256_CHECKSUM variables.
# shellcheck source=/dev/null
source "${SOURCE_DIR}/cpp/thirdparty/versions.txt"

sha256() {
  if command -v sha256sum > /dev/null 2>&1; then
    sha256sum "$1" | cut -d " " -f 1
  else
    shasum -a 256 "$1" | cut -d " " -f 1
  fi
}

mkdir -p "${DESTDIR}"
keep=()
for vendored_url_var in "${VENDORED_DEPENDENCIES[@]}"; do
  url=""
  for dep_packed in "${DEPENDENCIES[@]}"; do
    # Unpack each entry of the form "$home_var $tar_out $dep_url"
    IFS=" " read -r dep_url_var dep_tar_name dep_url <<< "${dep_packed}"
    if [ "${dep_url_var}" = "${vendored_url_var}" ]; then
      tar_name="${dep_tar_name}"
      url="${dep_url}"
      break
    fi
  done
  if [ -z "${url}" ]; then
    echo "${vendored_url_var} not found in versions.txt" >&2
    exit 1
  fi
  # e.g. ARROW_XSIMD_URL -> ARROW_XSIMD_BUILD_SHA256_CHECKSUM
  checksum_var="${vendored_url_var%_URL}_BUILD_SHA256_CHECKSUM"
  checksum="${!checksum_var}"
  keep+=("${tar_name}")

  out="${DESTDIR}/${tar_name}"
  if [ -f "${out}" ] && [ "$(sha256 "${out}")" = "${checksum}" ]; then
    continue
  fi

  echo "Downloading ${url}"
  curl --fail --silent --show-error --location --retry 3 --output "${out}" "${url}"

  if [ "$(sha256 "${out}")" != "${checksum}" ]; then
    echo "Checksum mismatch for ${out}" >&2
    rm -f "${out}"
    exit 1
  fi
done

# Remove tarballs for other versions
for file in "${DESTDIR}"/*; do
  stale=true
  for name in "${keep[@]}"; do
    if [ "$(basename "${file}")" = "${name}" ]; then
      stale=false
    fi
  done
  if [ "${stale}" = "true" ]; then
    rm -f "${file}"
  fi
done
