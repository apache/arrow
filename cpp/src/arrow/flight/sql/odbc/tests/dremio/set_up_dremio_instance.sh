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

# GH-48068 TODO: run remote ODBC tests on Linux

#!/bin/bash
set -e

HOST_URL="http://localhost:9047"
NEW_USER_URL="$HOST_URL/apiv2/bootstrap/firstuser"
LOGIN_URL="$HOST_URL/apiv2/login"
SQL_URL="$HOST_URL/api/v3/sql"

ADMIN_USER="admin"
ADMIN_PASSWORD="admin2025"

# Wait for Dremio to be available.
until curl -s "$NEW_USER_URL"; do
    echo 'Waiting for Dremio to start...'
    sleep 5
done

echo ""
echo 'Creating admin user...'

# Create new admin account.
curl -X PUT "$NEW_USER_URL" \
     -H "Content-Type: application/json" \
     -d "{ \"userName\": \"$ADMIN_USER\", \"password\": \"$ADMIN_PASSWORD\" }"

echo ""
echo "Created admin user."

# Use admin account to login and acquire a token.
TOKEN=$(curl -s -X POST "$LOGIN_URL" \
     -H "Content-Type: application/json" \
     -d "{ \"userName\": \"$ADMIN_USER\", \"password\": \"$ADMIN_PASSWORD\" }" \
     | grep -oP '(?<="token":")[^"]+')

SQL_QUERY="Create Table \$scratch.ODBCTest As SELECT CAST(2147483647 AS INTEGER) AS sinteger_max, CAST(9223372036854775807 AS BIGINT) AS sbigint_max, CAST(999999999 AS DECIMAL(38,0)) AS decimal_positive, CAST(3.40282347E38 AS FLOAT) AS float_max, CAST(1.7976931348623157E308 AS DOUBLE) AS double_max, CAST(true AS BOOLEAN) AS bit_true, CAST(DATE '9999-12-31' AS DATE) AS date_max, CAST(TIME '23:59:59' AS TIME) AS time_max, CAST(TIMESTAMP '9999-12-31 23:59:59' AS TIMESTAMP) AS timestamp_max;"
ESCAPED_QUERY=$(printf '%s' "$SQL_QUERY" | sed 's/"/\\"/g')

echo "Creating \$scratch.ODBCTest table."

# Create a new table by sending a SQL query.
curl -i -X POST "$SQL_URL" \
     -H "Authorization: _dremio$TOKEN" \
     -H "Content-Type: application/json" \
     -d "{\"sql\": \"$ESCAPED_QUERY\"}"

echo ""
echo "Finished setting up dremio docker instance."
