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
import sys
import pytest

import pyarrow as pa
from pyarrow.lib import tobytes
from pyarrow.lib import ArrowInvalid

try:
    import pyarrow.substrait as substrait
except ImportError:
    substrait = None

# Marks all of the tests in this module
# Ignore these with pytest ... -m 'not substrait'
pytestmark = [pytest.mark.dataset, pytest.mark.substrait]


def _write_dummy_data_to_disk(tmpdir, file_name, table):
    path = os.path.join(str(tmpdir), file_name)
    with pa.ipc.RecordBatchFileWriter(path, schema=table.schema) as writer:
        writer.write_table(table)
    return path


@pytest.mark.skipif(sys.platform == 'win32',
                    reason="ARROW-16392: file based URI is" +
                    " not fully supported for Windows")
def test_run_serialized_query(tmpdir):
    substrait_query = """
    {
        "relations": [
        {"rel": {
            "read": {
            "base_schema": {
                "struct": {
                "types": [
                            {"i64": {}}
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
                    "arrow": {}
                }
                ]
            }
            }
        }}
        ]
    }
    """

    file_name = "read_data.arrow"
    table = pa.table([[1, 2, 3, 4, 5]], names=['foo'])
    path = _write_dummy_data_to_disk(tmpdir, file_name, table)
    query = tobytes(substrait_query.replace("FILENAME_PLACEHOLDER", path))

    buf = pa._substrait._parse_json_plan(query)

    reader = substrait.run_query(buf)
    res_tb = reader.read_all()

    assert table.select(["foo"]) == res_tb.select(["foo"])


def test_invalid_plan():
    query = """
    {
        "relations": [
        ]
    }
    """
    buf = pa._substrait._parse_json_plan(tobytes(query))
    exec_message = "Empty substrait plan is passed."
    with pytest.raises(ArrowInvalid, match=exec_message):
        substrait.run_query(buf)


@pytest.mark.skipif(sys.platform == 'win32',
                    reason="ARROW-16392: file based URI is" +
                    " not fully supported for Windows")
def test_binary_conversion_with_json_options(tmpdir):
    substrait_query = """
    {
        "relations": [
        {"rel": {
            "read": {
            "base_schema": {
                "struct": {
                "types": [
                            {"i64": {}}
                        ]
                },
                "names": [
                        "bar"
                        ]
            },
            "local_files": {
                "items": [
                {
                    "uri_file": "file://FILENAME_PLACEHOLDER",
                    "arrow": {},
                    "metadata" : {
                      "created_by" : {},
                    }
                }
                ]
            }
            }
        }}
        ]
    }
    """

    file_name = "binary_json_data.arrow"
    table = pa.table([[1, 2, 3, 4, 5]], names=['bar'])
    path = _write_dummy_data_to_disk(tmpdir, file_name, table)
    query = tobytes(substrait_query.replace("FILENAME_PLACEHOLDER", path))
    buf = pa._substrait._parse_json_plan(tobytes(query))

    reader = substrait.run_query(buf)
    res_tb = reader.read_all()

    assert table.select(["bar"]) == res_tb.select(["bar"])
