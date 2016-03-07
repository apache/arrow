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

from arrow.includes.common cimport *

cdef extern from "arrow/api.h" namespace "arrow" nogil:

    enum LogicalType" arrow::LogicalType::type":
        LogicalType_NA" arrow::LogicalType::NA"

        LogicalType_BOOL" arrow::LogicalType::BOOL"

        LogicalType_UINT8" arrow::LogicalType::UINT8"
        LogicalType_INT8" arrow::LogicalType::INT8"
        LogicalType_UINT16" arrow::LogicalType::UINT16"
        LogicalType_INT16" arrow::LogicalType::INT16"
        LogicalType_UINT32" arrow::LogicalType::UINT32"
        LogicalType_INT32" arrow::LogicalType::INT32"
        LogicalType_UINT64" arrow::LogicalType::UINT64"
        LogicalType_INT64" arrow::LogicalType::INT64"

        LogicalType_FLOAT" arrow::LogicalType::FLOAT"
        LogicalType_DOUBLE" arrow::LogicalType::DOUBLE"

        LogicalType_STRING" arrow::LogicalType::STRING"

        LogicalType_LIST" arrow::LogicalType::LIST"
        LogicalType_STRUCT" arrow::LogicalType::STRUCT"

    cdef cppclass CDataType" arrow::DataType":
        LogicalType type
        c_bool nullable

        c_bool Equals(const CDataType* other)

        c_string ToString()

    cdef cppclass MemoryPool" arrow::MemoryPool":
        int64_t bytes_allocated()

    cdef cppclass CListType" arrow::ListType"(CDataType):
        CListType(const shared_ptr[CDataType]& value_type,
                  c_bool nullable)

    cdef cppclass CStringType" arrow::StringType"(CDataType):
        pass

    cdef cppclass CField" arrow::Field":
        c_string name
        shared_ptr[CDataType] type

        CField(const c_string& name, const shared_ptr[CDataType]& type)

    cdef cppclass CStructType" arrow::StructType"(CDataType):
        CStructType(const vector[shared_ptr[CField]]& fields,
                    c_bool nullable)

    cdef cppclass CSchema" arrow::Schema":
        CSchema(const shared_ptr[CField]& fields)

    cdef cppclass CArray" arrow::Array":
        const shared_ptr[CDataType]& type()

        int32_t length()
        int32_t null_count()
        LogicalType logical_type()

        c_bool IsNull(int i)

    cdef cppclass CUInt8Array" arrow::UInt8Array"(CArray):
        pass

    cdef cppclass CInt8Array" arrow::Int8Array"(CArray):
        pass

    cdef cppclass CListArray" arrow::ListArray"(CArray):
        pass

    cdef cppclass CStringArray" arrow::StringArray"(CListArray):
        pass
