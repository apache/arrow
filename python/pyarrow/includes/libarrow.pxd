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

    enum Type" arrow::Type::type":
        Type_NA" arrow::Type::NA"

        Type_BOOL" arrow::Type::BOOL"

        Type_UINT8" arrow::Type::UINT8"
        Type_INT8" arrow::Type::INT8"
        Type_UINT16" arrow::Type::UINT16"
        Type_INT16" arrow::Type::INT16"
        Type_UINT32" arrow::Type::UINT32"
        Type_INT32" arrow::Type::INT32"
        Type_UINT64" arrow::Type::UINT64"
        Type_INT64" arrow::Type::INT64"

        Type_FLOAT" arrow::Type::FLOAT"
        Type_DOUBLE" arrow::Type::DOUBLE"

        Type_STRING" arrow::Type::STRING"

        Type_LIST" arrow::Type::LIST"
        Type_STRUCT" arrow::Type::STRUCT"

    cdef cppclass CDataType" arrow::DataType":
        Type type

        c_bool Equals(const CDataType* other)

        c_string ToString()

    cdef cppclass MemoryPool" arrow::MemoryPool":
        int64_t bytes_allocated()

    cdef cppclass CListType" arrow::ListType"(CDataType):
        CListType(const shared_ptr[CDataType]& value_type)

    cdef cppclass CStringType" arrow::StringType"(CDataType):
        pass

    cdef cppclass CField" arrow::Field":
        c_string name
        shared_ptr[CDataType] type

        c_bool nullable

        CField(const c_string& name, const shared_ptr[CDataType]& type,
               c_bool nullable)

    cdef cppclass CStructType" arrow::StructType"(CDataType):
        CStructType(const vector[shared_ptr[CField]]& fields)

    cdef cppclass CSchema" arrow::Schema":
        CSchema(const vector[shared_ptr[CField]]& fields)
        const shared_ptr[CField]& field(int i)
        int num_fields()
        c_string ToString()

    cdef cppclass CArray" arrow::Array":
        const shared_ptr[CDataType]& type()

        int32_t length()
        int32_t null_count()
        Type type_enum()

        c_bool IsNull(int i)

    cdef cppclass CBooleanArray" arrow::BooleanArray"(CArray):
        c_bool Value(int i)

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


cdef extern from "arrow/api.h" namespace "arrow" nogil:
    # We can later add more of the common status factory methods as needed
    cdef CStatus CStatus_OK "Status::OK"()

    cdef cppclass CStatus "arrow::Status":
        CStatus()

        c_string ToString()

        c_bool ok()
        c_bool IsOutOfMemory()
        c_bool IsKeyError()
        c_bool IsNotImplemented()
        c_bool IsInvalid()

    cdef cppclass Buffer:
        uint8_t* data()
        int64_t size()


cdef extern from "arrow/ipc/metadata.h" namespace "arrow::ipc" nogil:
    cdef cppclass SchemaMessage:
        int num_fields()
        CStatus GetField(int i, shared_ptr[CField]* out)
        CStatus GetSchema(shared_ptr[CSchema]* out)

    cdef cppclass FieldMetadata:
        pass

    cdef cppclass BufferMetadata:
        pass

    cdef cppclass RecordBatchMessage:
        pass

    cdef cppclass DictionaryBatchMessage:
        pass

    enum MessageType" arrow::ipc::Message::Type":
        MessageType_SCHEMA" arrow::ipc::Message::SCHEMA"
        MessageType_RECORD_BATCH" arrow::ipc::Message::RECORD_BATCH"
        MessageType_DICTIONARY_BATCH" arrow::ipc::Message::DICTIONARY_BATCH"

    cdef cppclass Message:
        CStatus Open(const shared_ptr[Buffer]& buf,
                     shared_ptr[Message]* out)
        int64_t body_length()
        MessageType type()

        shared_ptr[SchemaMessage] GetSchema()
        shared_ptr[RecordBatchMessage] GetRecordBatch()
        shared_ptr[DictionaryBatchMessage] GetDictionaryBatch()
