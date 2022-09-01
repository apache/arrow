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

# cython: language_level = 3
from cython.operator cimport dereference as deref
from libcpp.vector cimport vector as std_vector

from pyarrow import Buffer
from pyarrow.lib import frombytes
from pyarrow.lib cimport *
from pyarrow.includes.libarrow cimport *
from pyarrow.includes.libarrow_substrait cimport *


def run_query(plan):
    """
    Execute a Substrait plan and read the results as a RecordBatchReader.

    Parameters
    ----------
    plan : Buffer
        The serialized Substrait plan to execute.
    """

    cdef:
        CResult[shared_ptr[CRecordBatchReader]] c_res_reader
        shared_ptr[CRecordBatchReader] c_reader
        RecordBatchReader reader
        c_string c_str_plan
        shared_ptr[CBuffer] c_buf_plan

    c_buf_plan = pyarrow_unwrap_buffer(plan)
    with nogil:
        c_res_reader = ExecuteSerializedPlan(deref(c_buf_plan))

    c_reader = GetResultValue(c_res_reader)

    reader = RecordBatchReader.__new__(RecordBatchReader)
    reader.reader = c_reader
    return reader


def _parse_json_plan(plan):
    """
    Parse a JSON plan into equivalent serialized Protobuf.

    Parameters
    ----------
    plan: bytes
        Substrait plan in JSON.

    Returns
    -------
    Buffer
        A buffer containing the serialized Protobuf plan.
    """

    cdef:
        CResult[shared_ptr[CBuffer]] c_res_buffer
        c_string c_str_plan
        shared_ptr[CBuffer] c_buf_plan

    c_str_plan = plan
    c_res_buffer = SerializeJsonPlan(c_str_plan)
    with nogil:
        c_buf_plan = GetResultValue(c_res_buffer)
    return pyarrow_wrap_buffer(c_buf_plan)


def get_supported_functions():
    """
    Get a list of Substrait functions that the underlying
    engine currently supports.

    Returns
    -------
    list[str]
        A list of function ids encoded as '{uri}#{name}'
    """

    cdef:
        ExtensionIdRegistry* c_id_registry
        std_vector[c_string] c_ids

    c_id_registry = default_extension_id_registry()
    c_ids = c_id_registry.GetSupportedSubstraitFunctions()

    functions_list = []
    for c_id in c_ids:
        functions_list.append(frombytes(c_id))
    return functions_list


cdef shared_ptr[CTable] _process_named_table(dict named_args, const std_vector[c_string]& names):
    cdef c_string c_name
    py_names = []
    for i in range(names.size()):
        c_name = names[i]
        py_names.append(frombytes(c_name))
    return pyarrow_unwrap_table(named_args["provider"](py_names))


def run_query_with_provider(plan, table_provider):
    cdef:
        CResult[shared_ptr[CRecordBatchReader]] c_res_reader
        shared_ptr[CRecordBatchReader] c_reader
        RecordBatchReader reader
        c_string c_str_plan
        shared_ptr[CBuffer] c_buf_plan
        function[named_table_provider] c_table_provider

    if table_provider is not None:
        named_table_args = {
            "provider": table_provider
        }
        c_table_provider = BindFunction[named_table_provider](
            &_process_named_table, named_table_args)

    c_buf_plan = pyarrow_unwrap_buffer(plan)
    with nogil:
        c_res_reader = ExecuteSerializedPlan(
            deref(c_buf_plan), c_table_provider)

    c_reader = GetResultValue(c_res_reader)

    reader = RecordBatchReader.__new__(RecordBatchReader)
    reader.reader = c_reader
    return reader
