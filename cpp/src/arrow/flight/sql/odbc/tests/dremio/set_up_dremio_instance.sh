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

set -euo pipefail

HOST_URL="http://localhost:9047"
NEW_USER_URL="$HOST_URL/apiv2/bootstrap/firstuser"
LOGIN_URL="$HOST_URL/apiv2/login"
SQL_URL="$HOST_URL/api/v3/sql"

ADMIN_USER="admin"
ADMIN_PASSWORD="admin2025"

MAX_WAIT_ATTEMPTS=60
WAIT_INTERVAL_SECONDS=5

# Wait for Dremio to be available.
attempt=1
until status_code="$(
    curl --silent \
         --output /dev/null \
         --write-out '%{http_code}' \
         "$NEW_USER_URL"
)"; [[ "$status_code" == "200" || "$status_code" == "404" || "$status_code" == "405" ]]; do

    if [[ "$attempt" -ge "$MAX_WAIT_ATTEMPTS" ]]; then
        echo "Timed out waiting for Dremio to start." >&2
        exit 1
    fi

    echo "Waiting for Dremio to start... HTTP $status_code"
    attempt=$((attempt + 1))
    sleep "$WAIT_INTERVAL_SECONDS"
done

echo ""
echo "Creating admin user..."

# Create new admin account.
curl --fail --silent --show-error \
     -X PUT "$NEW_USER_URL" \
     -H "Content-Type: application/json" \
     -d "$(python3 - <<EOF
import json
print(json.dumps({
    "userName": "$ADMIN_USER",
    "password": "$ADMIN_PASSWORD"
}))
EOF
)"

echo ""
echo "Created admin user."

echo "Logging in as admin user..."

# Login and capture response body.
LOGIN_RESPONSE="$(curl --fail --silent --show-error \
     -X POST "$LOGIN_URL" \
     -H "Content-Type: application/json" \
     -d "$(python3 - <<EOF
import json
print(json.dumps({
    "userName": "$ADMIN_USER",
    "password": "$ADMIN_PASSWORD"
}))
EOF
)")"

# Extract token safely using Python JSON parsing.
TOKEN="$(python3 - <<EOF
import json
import sys

try:
    response = json.loads("""$LOGIN_RESPONSE""")
except json.JSONDecodeError as exc:
    print(f"Failed to parse login response JSON: {exc}", file=sys.stderr)
    sys.exit(1)

token = response.get("token")

if not token:
    print("Login response did not contain a token.", file=sys.stderr)
    sys.exit(1)

print(token)
EOF
)"

SQL_QUERY="
Create Table \$scratch.ODBCTest As
  SELECT CAST(2147483647 AS INTEGER) AS sinteger_max,
         CAST(9223372036854775807 AS BIGINT) AS sbigint_max,
         CAST(999999999 AS DECIMAL(38,0)) AS decimal_positive,
         CAST(3.40282347E38 AS FLOAT) AS float_max,
         CAST(1.7976931348623157E308 AS DOUBLE) AS double_max,
         CAST(true AS BOOLEAN) AS bit_true,
         CAST(DATE '9999-12-31' AS DATE) AS date_max,
         CAST(TIME '23:59:59' AS TIME) AS time_max,
         CAST(TIMESTAMP '9999-12-31 23:59:59' AS TIMESTAMP) AS timestamp_max;
"

echo "Creating \$scratch.ODBCTest table."

# Create a new table by sending a SQL query.
curl --fail --silent --show-error \
     -X POST "$SQL_URL" \
     -H "Authorization: _dremio$TOKEN" \
     -H "Content-Type: application/json" \
     -d "$(python3 - <<EOF
import json
print(json.dumps({"sql": """$SQL_QUERY"""}))
EOF
)"

echo ""
echo "Finished setting up dremio docker instance."
