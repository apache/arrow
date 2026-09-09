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

if [[ ${EUID} -ne 0 ]]; then
  echo "uninstall.sh must run as root" >&2
  exit 1
fi
if ! command -v odbcinst >/dev/null 2>&1; then
  echo "odbcinst is required (install the odbcinst package)" >&2
  exit 1
fi

prefix="${1:-/opt/apache-arrow-flight-sql-odbc/25.0.1}"
if [[ ${prefix} != /* || ${prefix} == / ]]; then
  echo "installation prefix must be an absolute directory other than /" >&2
  exit 1
fi
driver_name="Apache Arrow Flight SQL ODBC Driver"
target_library="${prefix}/lib/libarrow_flight_sql_odbc.so"

while registration="$(odbcinst -q -d -n "${driver_name}" 2>/dev/null)"; do
  registered_driver="$(
    printf '%s\n' "${registration}" |
      awk -F= 'tolower($1) == "driver" {sub(/^[^=]*=/, ""); print; exit}'
  )"
  if [[ ${registered_driver} != "${target_library}" ]]; then
    echo "refusing to unregister driver at a different path: ${registered_driver}" >&2
    exit 1
  fi
  odbcinst -u -d -n "${driver_name}"
done

rm -f "${prefix}/lib/libarrow_flight_sql_odbc.so"
rmdir "${prefix}/lib" 2>/dev/null || true
rm -f "${prefix}/share/doc/BUILDING.md" \
  "${prefix}/share/doc/VALIDATION.md" \
  "${prefix}/share/doc/connection-options.md" \
  "${prefix}/share/doc/README.md" \
  "${prefix}/share/doc/LICENSE.txt" \
  "${prefix}/share/doc/NOTICE.txt"
rmdir "${prefix}/share/doc" 2>/dev/null || true
rmdir "${prefix}/share" 2>/dev/null || true
rmdir "${prefix}" 2>/dev/null || true

echo "Uninstalled Apache Arrow Flight SQL ODBC Driver from ${prefix}"
