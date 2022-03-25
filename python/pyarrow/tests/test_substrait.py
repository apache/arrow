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
import pyarrow.parquet as pq

try:
    from pyarrow import engine
    from pyarrow.engine import (
        run_query,
    )
except ImportError:
    engine = None


def test_import():
    # So we see the ImportError somewhere
    import pyarrow.engine  # noqa


def resource_root():
    """Get the path to the test resources directory."""
    if not os.environ.get("PARQUET_TEST_DATA"):
        raise RuntimeError("Test resources not found; set "
                           "PARQUET_TEST_DATA to "
                           "<repo root>/cpp/submodules/parquet-testing/data")
    return pathlib.Path(os.environ["PARQUET_TEST_DATA"])


def test_run_query():
    filename = str(resource_root() / "binary.parquet")

    query = """
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

    query = tobytes(query.replace("FILENAME_PLACEHOLDER", filename))

    schema = pa.schema({"foo": pa.binary()})

    reader = run_query(query, schema)

    res = reader.read_all()

    assert res.schema == schema
    assert res.num_rows > 0
    
    assert pq.read_table(filename).equals(res)
    