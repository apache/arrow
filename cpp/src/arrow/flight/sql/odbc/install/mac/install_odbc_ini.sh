#!/bin/sh
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

# GH-47876 TODO: create macOS ODBC Installer.
# Script for installing macOS ODBC driver, to be used for macOS installer.

source_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

odbc_install_script="${source_dir}/install_odbc.sh"

chmod +x "$odbc_install_script"
. "$odbc_install_script" /Library/Apache/ArrowFlightSQLODBC/lib/libarrow_flight_sql_odbc.dylib

USER_ODBC_FILE="$HOME/Library/ODBC/odbc.ini"
DRIVER_NAME="Apache Arrow Flight SQL ODBC Driver"
DSN_NAME="Apache Arrow Flight SQL ODBC DSN"

touch "$USER_ODBC_FILE"

if [ $EUID -ne 0 ]; then 
    echo "Please run this script with sudo"
    exit 1
fi

if grep -q "^\[$DSN_NAME\]" "$USER_ODBC_FILE"; then
  echo "DSN [$DSN_NAME] already exists in $USER_ODBC_FILE"
else
  echo "Adding [$DSN_NAME] to $USER_ODBC_FILE..."
  cat >> "$USER_ODBC_FILE" <<EOF

[$DSN_NAME]
Description = An ODBC Driver DSN for Apache Arrow Flight SQL
Driver      = Apache Arrow Flight SQL ODBC Driver
Host        =
Port        =
UID         =
PWD         =
EOF
fi

# Check if [ODBC Data Sources] section exists
if grep -q '^\[ODBC Data Sources\]' "$USER_ODBC_FILE"; then
  # Section exists: check if DSN entry exists
  if ! grep -q "^${DSN_NAME}=" "$USER_ODBC_FILE"; then
    # Add DSN entry under [ODBC Data Sources] section

    # Use awk to insert the line immediately after [ODBC Data Sources]
    awk -v dsn="$DSN_NAME" -v driver="$DRIVER_NAME" '
      $0 ~ /^\[ODBC Data Sources\]/ && !inserted {
        print
        print dsn "=" driver
        inserted=1
        next
      }
      { print }
    ' "$USER_ODBC_FILE" > "${USER_ODBC_FILE}.tmp" && mv "${USER_ODBC_FILE}.tmp" "$USER_ODBC_FILE"
  fi
else
  # Section doesn't exist, append section and DSN entry at end
  {
    echo ""
    echo "[ODBC Data Sources]"
    echo "${DSN_NAME}=${DRIVER_NAME}"
  } >> "$USER_ODBC_FILE"
fi

