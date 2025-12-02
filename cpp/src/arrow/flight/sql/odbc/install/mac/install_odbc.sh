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

# Used by macOS ODBC installer script and macOS ODBC testing

ODBC_64BIT="$1"

if [[ -z "$ODBC_64BIT" ]]; then
  echo "error: 64-bit driver is not specified. Call format: install_odbc abs_path_to_64_bit_driver"
  exit 1
fi

if [ ! -f $ODBC_64BIT ]; then
    echo "64-bit driver can not be found: $ODBC_64BIT"
	  echo "Call format: install_odbc abs_path_to_64_bit_driver"
    exit 1
fi

USER_ODBCINST_FILE="$HOME/Library/ODBC/odbcinst.ini"
DRIVER_NAME="Apache Arrow Flight SQL ODBC Driver"
DSN_NAME="Apache Arrow Flight SQL ODBC DSN"

mkdir -p $HOME/Library/ODBC

touch "$USER_ODBCINST_FILE"

# Admin privilege is needed to add ODBC driver registration
if [ $EUID -ne 0 ]; then 
    echo "Please run this script with sudo"
    exit 1
fi

if grep -q "^\[$DRIVER_NAME\]" "$USER_ODBCINST_FILE"; then
  echo "Driver [$DRIVER_NAME] already exists in odbcinst.ini"
else
  echo "Adding [$DRIVER_NAME] to odbcinst.ini..."
  echo "
[$DRIVER_NAME]
Description=An ODBC Driver for Apache Arrow Flight SQL
Driver=$ODBC_64BIT
" >> "$USER_ODBCINST_FILE"
fi

# Check if [ODBC Drivers] section exists
if grep -q '^\[ODBC Drivers\]' "$USER_ODBCINST_FILE"; then
  # Section exists: check if driver entry exists
  if ! grep -q "^${DRIVER_NAME}=" "$USER_ODBCINST_FILE"; then
    # Driver entry does not exist, add under [ODBC Drivers]
    sed -i '' "/^\[ODBC Drivers\]/a\\
${DRIVER_NAME}=Installed
" "$USER_ODBCINST_FILE"
  fi
else
  # Section doesn't exist, append both section and driver entry at end
  {
    echo ""
    echo "[ODBC Drivers]"
    echo "${DRIVER_NAME}=Installed"
  } >> "$USER_ODBCINST_FILE"
fi
