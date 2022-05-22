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

from pyarrow import Buffer
from pyarrow.lib cimport *
from pyarrow.includes.libarrow cimport *
from pyarrow.includes.libarrow_substrait cimport *


from pyarrow._exec_plan cimport is_supported_execplan_output_type, execplan


def make_extension_id_registry():
    cdef:
        shared_ptr[CExtensionIdRegistry] c_registry
        ExtensionIdRegistry registry

    with nogil:
        c_registry = MakeExtensionIdRegistry()

    registry = ExtensionIdRegistry.__new__(ExtensionIdRegistry)
    registry.registry = &deref(c_registry)
    return registry

def register_function(registry, id_uri, id_name, arrow_function_name):
    cdef:
        c_string c_id_uri, c_id_name, c_arrow_function_name
        shared_ptr[CExtensionIdRegistry] c_registry
        CStatus c_status

    c_registry = pyarrow_unwrap_extension_id_registry(registry)
    c_id_uri = id_uri
    c_id_name = id_name
    c_arrow_function_name = arrow_function_name

    with nogil:
        c_status = RegisterFunction(
            deref(c_registry), c_id_uri, c_id_name, c_arrow_function_name
        )

    return c_status.ok()

def run_query_as(plan, output_type=RecordBatchReader):
    if output_type == RecordBatchReader:
        return run_query(plan)
    return _run_query(plan, output_type)

def _run_query(plan, output_type):
    cdef:
        CResult[vector[CDeclaration]] c_res_decls
        vector[CDeclaration] c_decls
        shared_ptr[CBuffer] c_buf_plan

    if not is_supported_execplan_output_type(output_type):
        raise TypeError(f"Unsupported output type {output_type}")

    c_buf_plan = pyarrow_unwrap_buffer(plan)
    with nogil:
        c_res_decls = DeserializePlans(deref(c_buf_plan))
    c_decls = GetResultValue(c_res_decls)
    return execplan([], output_type, c_decls)

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
