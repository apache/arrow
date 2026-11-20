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

# Verify that every ODBC dylib in an expanded ODBC macOS installer PKG is
# universal (ARM64 and AMD64), then flatten it back to a PKG.
#
# Usage: odbc_macos_flatten_universal_pkg.sh EXPANDED_DIR OUTPUT_PKG
#
#   EXPANDED_DIR: Directory created by `pkgutil --expand-full`
#   OUTPUT_PKG: Path of the flat PKG to create

set -euo pipefail

if [ $# -ne 2 ]; then
  echo "Usage: $0 EXPANDED_DIR OUTPUT_PKG" >&2
  exit 1
fi

expanded_dir="${1}"
output_pkg="${2}"
work_dir="$(mktemp -d)"

symlink_path="$(find "${expanded_dir}" -name libarrow_flight_sql_odbc.dylib)"
lib_dir="$(dirname "${symlink_path}")"

# Verify every dylib (including the symlinks) is universal before re-packaging.
# Check architecture with `grep` on `lipo -archs` to be compatible on macOS 27.
for lib in "${lib_dir}"/libarrow_flight_sql_odbc*.dylib; do
  dylib_archs="$(lipo -archs "${lib}")"
  echo "${lib}: ${dylib_archs}"
  for arch in arm64 x86_64; do
    if ! grep -qw "${arch}" <<< "${dylib_archs}"; then
      echo "${lib} is missing '${arch}' (got: ${dylib_archs})"
      exit 1
    fi
  done
done

# --flatten-full regenerates each component's Bom and PackageInfo
# installKBytes, but leaves Distribution with outdated PKG's sizes. 
# Workaround is to copy regenerated sizes into Distribution, 
# then re-flatten without -full so the correct sizes stay in Distribution.
#
# Use sudo for every step that touches the expanded payload so the root-owned
# files in the PKG keep their ownership instead of being re-stamped with the
# runner user's uid/gid.
sudo pkgutil --flatten-full "${expanded_dir}" "${work_dir}/flattened.pkg"
sudo pkgutil --expand "${work_dir}/flattened.pkg" "${work_dir}/shallow"
for component in "${work_dir}"/shallow/*.pkg; do
  component_name="$(basename "${component}")"
  kbytes="$(sudo sed -n 's/.*installKBytes="\([0-9]*\)".*/\1/p' "${component}/PackageInfo")"
  sudo sed -i '' \
    "/>#${component_name}<\/pkg-ref>/ s/installKBytes=\"[0-9]*\"/installKBytes=\"${kbytes}\"/" \
    "${work_dir}/shallow/Distribution"
done
mkdir -p "$(dirname "${output_pkg}")"
sudo pkgutil --flatten "${work_dir}/shallow" "${output_pkg}"
sudo chown "$(id -u):$(id -g)" "${output_pkg}"
