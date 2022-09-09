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

import pytest

try:
    from pyarrow import flight_sql
    from pyarrow.flight_sql import connect
except (ImportError, RuntimeError):
    flight_sql = None
    connect = None

# Marks all of the tests in this module
# Ignore these with pytest ... -m 'not flight_sql'
pytestmark = pytest.mark.flight_sql


# Without a Flight SQL server implementation in Python, the best we
# can do is just basic smoke tests

def test_import():
    assert flight_sql.connect


def test_invalid_uri():
    match = ".*No client transport implementation for invalid.*"
    with pytest.raises(flight_sql.Error, match=match):
        connect("invalid://foo")


def test_valid_uri():
    # Connection still fails, but we should get something different
    match = ".*ADBC_STATUS_IO.*"
    with pytest.raises(flight_sql.OperationalError, match=match):
        with connect("grpc+tcp://localhost:1234") as conn:
            conn.adbc_get_info()


def test_db_kwargs():
    match = ".*type name invalid_type is not recognized.*"
    db_kwargs = {"arrow.flight.sql.quirks.ingest_type.invalid_type": "integer"}
    with pytest.raises(flight_sql.ProgrammingError, match=match):
        with connect("grpc+tcp://localhost:1234", db_kwargs=db_kwargs) as conn:
            conn.adbc_get_info()


def test_conn_kwargs():
    match = ".*Invalid timeout option value.*"
    conn_kwargs = {"arrow.flight.sql.rpc.timeout_seconds.fetch": "invalid"}
    with pytest.raises(flight_sql.ProgrammingError, match=match):
        with connect("grpc+tcp://localhost:1234",
                     conn_kwargs=conn_kwargs) as conn:
            conn.adbc_get_info()
