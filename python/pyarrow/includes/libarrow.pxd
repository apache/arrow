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
        uint8_t Value(int i)

    cdef cppclass CInt8Array" arrow::Int8Array"(CArray):
        int8_t Value(int i)

    cdef cppclass CUInt16Array" arrow::UInt16Array"(CArray):
        uint16_t Value(int i)

    cdef cppclass CInt16Array" arrow::Int16Array"(CArray):
        int16_t Value(int i)

    cdef cppclass CUInt32Array" arrow::UInt32Array"(CArray):
        uint32_t Value(int i)

    cdef cppclass CInt32Array" arrow::Int32Array"(CArray):
        int32_t Value(int i)

    cdef cppclass CUInt64Array" arrow::UInt64Array"(CArray):
        uint64_t Value(int i)

    cdef cppclass CInt64Array" arrow::Int64Array"(CArray):
        int64_t Value(int i)

    cdef cppclass CFloatArray" arrow::FloatArray"(CArray):
        float Value(int i)

    cdef cppclass CDoubleArray" arrow::DoubleArray"(CArray):
        double Value(int i)

    cdef cppclass CListArray" arrow::ListArray"(CArray):
        const int32_t* offsets()
        int32_t offset(int i)
        int32_t value_length(int i)
        const shared_ptr[CArray]& values()
        const shared_ptr[CDataType]& value_type()

    cdef cppclass CStringArray" arrow::StringArray"(CListArray):
        c_string GetString(int i)
