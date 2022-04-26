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

import os
import pathlib
import pyarrow as pa
from pyarrow.lib import tobytes
from pyarrow.lib import ArrowInvalid
import pyarrow.parquet as pq
import pytest

try:
    import pyarrow.engine as engine
except ImportError:
    engine = None

# Marks all of the tests in this module
# Ignore these with pytest ... -m 'not engine'
pytestmark = pytest.mark.engine


_substrait_query = """
    {
        "relations": [
        {"rel": {
            "read": {
            "base_schema": {
                "struct": {
                "types": [
                            {"binary": {}}
                        ]
                },
                "names": [
                        "foo"
                        ]
            },
            "local_files": {
                "items": [
                {
                    "uri_file": "file://FILENAME_PLACEHOLDER",
                    "format": "FILE_FORMAT_PARQUET"
                }
                ]
            }
            }
        }}
        ]
    }
    """


def resource_root():
    """Get the path to the test resources directory."""
    if not os.environ.get("PARQUET_TEST_DATA"):
        raise RuntimeError("Test resources not found; set "
                           "PARQUET_TEST_DATA to "
                           "<repo root>/cpp/submodules/parquet-testing/data")
    return pathlib.Path(os.environ["PARQUET_TEST_DATA"])


def test_run_query():
    filename = str(resource_root() / "binary.parquet")

    query = tobytes(_substrait_query.replace("FILENAME_PLACEHOLDER", filename))
    reader = engine.run_query(query)
    res_tb = reader.read_all()

    expected_tb = pq.read_table(filename)

    assert expected_tb.num_rows == res_tb.num_rows


def test_run_query_in_bytes():
    filename = str(resource_root() / "binary.parquet")

    query = tobytes(_substrait_query.replace("FILENAME_PLACEHOLDER", filename))

    buf = pa._engine._parse_json_plan(query)

    reader = engine.run_query(buf)
    res_tb = reader.read_all()

    expected_tb = pq.read_table(filename)

    assert expected_tb.num_rows == res_tb.num_rows


def test_invalid_plan():
    query = """
    {
        "relations": [
        ]
    }
    """
    exec_message = "ExecPlan has no node"
    with pytest.raises(ArrowInvalid, match=exec_message):
        engine.run_query(tobytes(query))
