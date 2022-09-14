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

_base_ext_uri = "https://github.com/substrait-io" \
    + "/substrait/blob/main/extensions"
_kAggregateURI = _base_ext_uri + "/functions_aggregate_generic.yaml"


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


# Substrait has not finalized what the URI should be for standard functions
# In the meantime, lets just check the suffix
def has_function(fns, ext_file, fn_name):
    suffix = f'{ext_file}#{fn_name}'
    for fn in fns:
        if fn.endswith(suffix):
            return True
    return False


def test_get_supported_functions():
    supported_functions = pa._substrait.get_supported_functions()
    # It probably doesn't make sense to exhaustively verfiy this list but
    # we can check a sample aggregate and a sample non-aggregate entry
    assert has_function(supported_functions,
                        'functions_arithmetic.yaml', 'add')
    assert has_function(supported_functions,
                        'functions_arithmetic.yaml', 'sum')


@pytest.mark.skipif(sys.platform == 'win32',
                    reason="ARROW-16392: file based URI is" +
                    " not fully supported for Windows")
def test_run_aggregate_query(tmpdir):
    substrait_query = """
    {
        "relations": [{
        "rel": {
            "aggregate": {
            "input": {
                "read": {
                "base_schema": {
                    "names": ["A", "B", "C"],
                    "struct": {
                    "types": [{
                        "i64": {}
                    }, {
                        "i64": {}
                    }, {
                        "i64": {}
                    }]
                    }
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
            },
            "groupings": [{
                "groupingExpressions": [{
                "selection": {
                    "directReference": {
                    "structField": {
                        "field": 0
                    }
                    }
                }
                }]
            }],
            "measures": [{
                "measure": {
                "functionReference": 0,
                "arguments": [{
                    "value": {
                    "selection": {
                        "directReference": {
                        "structField": {
                            "field": 1
                        }
                        }
                    }
                    }
                }],
                "sorts": [],
                "phase": "AGGREGATION_PHASE_INITIAL_TO_RESULT",
                "invocation": "AGGREGATION_INVOCATION_ALL",
                "outputType": {
                    "i64": {}
                }
                }
            }]
            }
        }
        }],
        "extensionUris": [{
        "extension_uri_anchor": 0,
        "uri": "AGGREGATE_URI_PLACEHOLDER"
        }],
        "extensions": [{
        "extension_function": {
            "extension_uri_reference": 0,
            "function_anchor": 0,
            "name": "count"
        }
        }]
    }
    """

    file_name = "read_agg_data.arrow"
    table = pa.Table.from_arrays([[1, 2, 3, 2, 3, 1, 1],
                                  [10, 20, 30, 40, 50, 60, 70],
                                  [3, 4, 5, 1, 2, 0, 20]],
                                 names=['A', 'B', 'C'])
    path = _write_dummy_data_to_disk(tmpdir, file_name, table)

    query_with_path = substrait_query.replace("FILENAME_PLACEHOLDER", path)
    query_with_ext_uri = query_with_path.replace(
        "AGGREGATE_URI_PLACEHOLDER", _kAggregateURI)
    query = tobytes(query_with_ext_uri)

    buf = pa._substrait._parse_json_plan(query)

    reader = substrait.run_query(buf)
    res_tb = reader.read_all()

    print(res_tb)
