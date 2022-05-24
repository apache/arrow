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
from cython.operator cimport dereference as deref, preincrement as inc

from pyarrow import Buffer
from pyarrow.lib import frombytes, tobytes
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

    return pyarrow_wrap_extension_id_registry(c_registry)

def get_udf_declarations(plan, registry):
    cdef:
        shared_ptr[CBuffer] c_buf_plan
        shared_ptr[CExtensionIdRegistry] c_registry
        vector[CUdfDeclaration] c_decls
        vector[CUdfDeclaration].iterator c_decls_iter
        vector[pair[shared_ptr[CDataType], c_bool]].iterator c_in_types_iter

    c_buf_plan = pyarrow_unwrap_buffer(plan)
    c_registry = pyarrow_unwrap_extension_id_registry(registry)
    with nogil:
        c_res_decls = DeserializePlanUdfs(deref(c_buf_plan), &deref(c_registry))
    c_decls = GetResultValue(c_res_decls)

    decls = []
    c_decls_iter = c_decls.begin()
    while c_decls_iter != c_decls.end():
        input_types = []
        c_in_types_iter = deref(c_decls_iter).input_types.begin()
        while c_in_types_iter != deref(c_decls_iter).input_types.end():
            input_types.append((pyarrow_wrap_data_type(deref(c_in_types_iter).first),
                                deref(c_in_types_iter).second))
            inc(c_in_types_iter)
        decls.append({
            "name": frombytes(deref(c_decls_iter).name),
            "code": frombytes(deref(c_decls_iter).code),
            "summary": frombytes(deref(c_decls_iter).summary),
            "description": frombytes(deref(c_decls_iter).description),
            "input_types": input_types,
            "output_type": (pyarrow_wrap_data_type(deref(c_decls_iter).output_type.first),
                            deref(c_decls_iter).output_type.second),
        })
        inc(c_decls_iter)
    return decls

def register_function(registry, id_uri, id_name, arrow_function_name):
    cdef:
        c_string c_id_uri, c_id_name, c_arrow_function_name
        shared_ptr[CExtensionIdRegistry] c_registry
        CStatus c_status

    c_registry = pyarrow_unwrap_extension_id_registry(registry)
    c_id_uri = id_uri or default_extension_types_uri()
    c_id_name = tobytes(id_name)
    c_arrow_function_name = tobytes(arrow_function_name)

    with nogil:
        c_status = RegisterFunction(
            deref(c_registry), c_id_uri, c_id_name, c_arrow_function_name
        )

    check_status(c_status)

def run_query_as(plan, registry, output_type=RecordBatchReader):
    if output_type == RecordBatchReader:
        return run_query(plan, registry)
    return _run_query(plan, registry, output_type)

def _run_query(plan, registry, output_type):
    cdef:
        shared_ptr[CBuffer] c_buf_plan
        shared_ptr[CExtensionIdRegistry] c_registry
        CResult[vector[CDeclaration]] c_res_decls
        vector[CDeclaration] c_decls

    if not is_supported_execplan_output_type(output_type):
        raise TypeError(f"Unsupported output type {output_type}")

    c_buf_plan = pyarrow_unwrap_buffer(plan)
    c_registry = pyarrow_unwrap_extension_id_registry(registry)
    with nogil:
        c_res_decls = DeserializePlans(deref(c_buf_plan), &deref(c_registry))
    c_decls = GetResultValue(c_res_decls)
    return execplan([], output_type, c_decls)

def run_query(plan, registry):
    """
    Execute a Substrait plan and read the results as a RecordBatchReader.

    Parameters
    ----------
    plan : Buffer
        The serialized Substrait plan to execute.
    """

    cdef:
        shared_ptr[CBuffer] c_buf_plan
        shared_ptr[CExtensionIdRegistry] c_registry
        CResult[shared_ptr[CRecordBatchReader]] c_res_reader
        shared_ptr[CRecordBatchReader] c_reader
        RecordBatchReader reader

    c_buf_plan = pyarrow_unwrap_buffer(plan)
    c_registry = pyarrow_unwrap_extension_id_registry(registry)
    with nogil:
        c_res_reader = ExecuteSerializedPlan(deref(c_buf_plan), &deref(c_registry))

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
