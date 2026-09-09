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
  echo "install.sh must run as root" >&2
  exit 1
fi
if ! command -v odbcinst >/dev/null 2>&1; then
  echo "odbcinst is required (install the odbcinst package)" >&2
  exit 1
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
prefix="${1:-/opt/apache-arrow-flight-sql-odbc/25.0.1}"
if [[ ${prefix} != /* || ${prefix} == / ]]; then
  echo "installation prefix must be an absolute directory other than /" >&2
  exit 1
fi
library_name="libarrow_flight_sql_odbc.so"
source_library="${script_dir}/lib/${library_name}"
target_library="${prefix}/lib/${library_name}"
driver_name="Apache Arrow Flight SQL ODBC Driver"

if [[ ! -f ${source_library} ]]; then
  echo "driver library is missing from the package" >&2
  exit 1
fi

already_registered=false
if existing_registration="$(odbcinst -q -d -n "${driver_name}" 2>/dev/null)"; then
  existing_driver="$(
    printf '%s\n' "${existing_registration}" |
      awk -F= 'tolower($1) == "driver" {sub(/^[^=]*=/, ""); print; exit}'
  )"
  if [[ ${existing_driver} != "${target_library}" ]]; then
    echo "driver name is already registered at a different path: ${existing_driver}" >&2
    exit 1
  fi
  already_registered=true
fi

install -d -m 0755 "${prefix}/lib" "${prefix}/share/doc"
install -m 0755 "${source_library}" "${target_library}"
if [[ -d ${script_dir}/share/doc ]]; then
  cp -R "${script_dir}/share/doc/." "${prefix}/share/doc/"
fi

registration="$(mktemp)"
trap 'rm -f "${registration}"' EXIT
if [[ ${already_registered} == false ]]; then
  sed "s|@DRIVER_PATH@|${target_library}|g" \
    "${script_dir}/odbcinst.ini.in" >"${registration}"
  odbcinst -i -d -f "${registration}"
fi

echo "Installed Apache Arrow Flight SQL ODBC Driver at ${target_library}"
