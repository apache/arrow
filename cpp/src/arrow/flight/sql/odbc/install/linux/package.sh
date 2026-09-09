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

set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: package.sh <driver-library> <output-directory>" >&2
  exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../../../../../../../../" && pwd)"
driver="$(realpath "$1")"
output_dir="$(realpath -m "$2")"
version="25.0.1"
package_name="apache-arrow-flight-sql-odbc-${version}-linux-x86_64"
direct_library="libarrow_flight_sql_odbc-${version}-linux-x86_64.so"

if [[ ! -f ${driver} ]]; then
  echo "driver library does not exist: ${driver}" >&2
  exit 1
fi
if ! file "${driver}" | grep -q 'ELF 64-bit.*x86-64'; then
  echo "driver is not a Linux x86_64 ELF shared library" >&2
  exit 1
fi

mkdir -p "${output_dir}"
staging="$(mktemp -d)"
trap 'rm -rf "${staging}"' EXIT
package_root="${staging}/${package_name}"
mkdir -p "${package_root}/lib" "${package_root}/share/doc" \
  "${package_root}/smoke"

install -m 0755 "${driver}" "${output_dir}/${direct_library}"
strip --strip-unneeded "${output_dir}/${direct_library}"
install -m 0755 "${output_dir}/${direct_library}" \
  "${package_root}/lib/libarrow_flight_sql_odbc.so"
install -m 0755 "${script_dir}/install.sh" "${package_root}/install.sh"
install -m 0755 "${script_dir}/uninstall.sh" "${package_root}/uninstall.sh"
install -m 0644 "${script_dir}/odbcinst.ini.in" "${package_root}/odbcinst.ini.in"
install -m 0644 "${script_dir}/BUILDING.md" "${package_root}/share/doc/BUILDING.md"
install -m 0644 "${script_dir}/VALIDATION.md" "${package_root}/share/doc/VALIDATION.md"
install -m 0644 "${script_dir}/../../connection-options.md" \
  "${package_root}/share/doc/connection-options.md"
install -m 0644 "${script_dir}/../../README.md" "${package_root}/share/doc/README.md"
install -m 0644 "${repo_root}/LICENSE.txt" "${package_root}/share/doc/LICENSE.txt"
install -m 0644 "${repo_root}/NOTICE.txt" "${package_root}/share/doc/NOTICE.txt"
install -m 0644 "${script_dir}/smoke_test.cc" "${package_root}/smoke/smoke_test.cc"
install -m 0755 "${script_dir}/build_smoke_test.sh" \
  "${package_root}/smoke/build_smoke_test.sh"
"${script_dir}/build_smoke_test.sh" \
  "${package_root}/smoke/flight_sql_odbc_smoke_test"

tar --sort=name --mtime='UTC 2026-08-05' --owner=0 --group=0 --numeric-owner \
  -C "${staging}" -czf "${output_dir}/${package_name}.tar.gz" "${package_name}"

(
  cd "${output_dir}"
  sha256sum "${direct_library}" "${package_name}.tar.gz" >SHA256SUMS
)
