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

# Used by cpp/src/arrow/flight/sql/odbc/install/mac/postinstall

set -euo pipefail

# Admin privilege is needed to add ODBC driver registration
if [ $EUID -ne 0 ]; then
  echo "Please run this script with sudo"
  exit 1
fi

ODBC_64BIT="$1"

if [[ -z "$ODBC_64BIT" ]]; then
  echo "error: 64-bit driver is not specified. Call format: install_odbc abs_path_to_64_bit_driver"
  exit 1
fi

if [ ! -f "$ODBC_64BIT" ]; then
  echo "64-bit driver can not be found: $ODBC_64BIT"
  echo "Call format: install_odbc abs_path_to_64_bit_driver"
  exit 1
fi

if ! command -v odbcinst >/dev/null 2>&1; then
  echo "error: odbcinst not found. Please install unixODBC first."
  exit 1
fi

# On macOS, iODBC reads from /Library/ODBC/odbcinst.ini. Point odbcinst there
# so the registration is visible to iODBC rather than written to the unixODBC
# Homebrew path that iODBC never reads.
if [ "$(uname)" = "Darwin" ]; then
  mkdir -p /Library/ODBC
  export ODBCINSTINI=/Library/ODBC/odbcinst.ini
fi

DRIVER_NAME="Apache Arrow Flight SQL ODBC Driver"

TEMPLATE_FILE=$(mktemp /tmp/arrow_odbc_XXXXXX.ini)
trap 'rm -f "$TEMPLATE_FILE"' EXIT

cat > "$TEMPLATE_FILE" <<EOF
[$DRIVER_NAME]
Description=An ODBC Driver for Apache Arrow Flight SQL
Driver=$ODBC_64BIT
EOF

echo "Registering [$DRIVER_NAME] via odbcinst..."
odbcinst -i -d -f "$TEMPLATE_FILE"
