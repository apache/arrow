#!/bin/bash
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

#!/usr/bin/env bash
set -euo pipefail

SYSTEM_ODBC_FILE="${1:-}"

if [[ -z "$SYSTEM_ODBC_FILE" ]]; then
  echo "ERROR: path to system ODBC DSN is not specified." >&2
  echo "Usage: install_odbc_ini.sh <abs_path_to_odbc_dsn_ini>" >&2
  exit 1
fi

DRIVER_NAME="Apache Arrow Flight SQL ODBC Driver"
DSN_NAME="Apache Arrow Flight SQL ODBC DSN"

if ! touch "$SYSTEM_ODBC_FILE"; then
  echo "ERROR: Cannot access or create $SYSTEM_ODBC_FILE" >&2
  exit 1
fi

if grep -q "^\[$DSN_NAME\]" "$SYSTEM_ODBC_FILE"; then
  echo "DSN [$DSN_NAME] already exists in $SYSTEM_ODBC_FILE"
else
  echo "Adding [$DSN_NAME] to $SYSTEM_ODBC_FILE..."
  cat >> "$SYSTEM_ODBC_FILE" <<EOF

[$DSN_NAME]
Description = An ODBC Driver DSN for Apache Arrow Flight SQL
Driver      = $DRIVER_NAME
Host        =
Port        =
UID         =
PWD         =
EOF
fi

# Check if [ODBC Data Sources] section exists
if grep -q '^\[ODBC Data Sources\]' "$SYSTEM_ODBC_FILE"; then
  # Section exists: check if DSN entry exists
  if ! grep -Eq "^${DSN_NAME}[[:space:]]*=" "$SYSTEM_ODBC_FILE"; then
    # Add DSN entry under [ODBC Data Sources] section
    tmp_file="$(mktemp "${SYSTEM_ODBC_FILE}.XXXX")"

    # Use awk to insert the line immediately after [ODBC Data Sources]
    awk -v dsn="$DSN_NAME" -v driver="$DRIVER_NAME" '
      $0 ~ /^\[ODBC Data Sources\]/ && !inserted {
        print
        print dsn "=" driver
        inserted=1
        next
      }
      { print }
    ' "$SYSTEM_ODBC_FILE" > "$tmp_file"

    mv "$tmp_file" "$SYSTEM_ODBC_FILE"
  fi
else
  # Section doesn't exist, append section and DSN entry at end
  {
    echo ""
    echo "[ODBC Data Sources]"
    echo "${DSN_NAME}=${DRIVER_NAME}"
  } >> "$SYSTEM_ODBC_FILE"
fi
