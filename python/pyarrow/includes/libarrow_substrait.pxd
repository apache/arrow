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

# distutils: language = c++

from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport *


cdef extern from "arrow/engine/substrait/extension_set.h" namespace "arrow::engine" nogil:
    cppclass CExtensionIdRegistry "arrow::engine::ExtensionIdRegistry"

cdef extern from "arrow/engine/substrait/serde.h" namespace "arrow::engine" nogil:
    cppclass CUdfDeclaration "arrow::engine::UdfDeclaration":
        c_string name
        c_string code
        c_string summary
        c_string description
        vector[pair[shared_ptr[CDataType], c_bool]] input_types
        pair[shared_ptr[CDataType], c_bool] output_type
        c_bool is_tabular

    CResult[vector[CUdfDeclaration]] DeserializePlanUdfs(const CBuffer& substrait_buffer, const CExtensionIdRegistry* registry)

cdef extern from "arrow/engine/substrait/util.h" namespace "arrow::engine::substrait" nogil:
    shared_ptr[CExtensionIdRegistry] MakeExtensionIdRegistry()
    CStatus RegisterFunction(CExtensionIdRegistry& registry, const c_string& id_uri, const c_string& id_name, const c_string& arrow_function_name)

    CResult[shared_ptr[CRecordBatchReader]] ExecuteSerializedPlan(const CBuffer& substrait_buffer, const CExtensionIdRegistry* extid_registry, CFunctionRegistry* func_registry)
    CResult[shared_ptr[CBuffer]] SerializeJsonPlan(const c_string& substrait_json)
    CResult[vector[CDeclaration]] DeserializePlans(const CBuffer& substrait_buffer, const CExtensionIdRegistry* registry)

    const c_string& default_extension_types_uri()
