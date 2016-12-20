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

        Type_TIMESTAMP" arrow::Type::TIMESTAMP"
        Type_DATE" arrow::Type::DATE"
        Type_BINARY" arrow::Type::BINARY"
        Type_STRING" arrow::Type::STRING"

        Type_LIST" arrow::Type::LIST"
        Type_STRUCT" arrow::Type::STRUCT"

    enum TimeUnit" arrow::TimeUnit":
        TimeUnit_SECOND" arrow::TimeUnit::SECOND"
        TimeUnit_MILLI" arrow::TimeUnit::MILLI"
        TimeUnit_MICRO" arrow::TimeUnit::MICRO"
        TimeUnit_NANO" arrow::TimeUnit::NANO"

    cdef cppclass CDataType" arrow::DataType":
        Type type

        c_bool Equals(const CDataType* other)

        c_string ToString()

    cdef cppclass MemoryPool" arrow::MemoryPool":
        int64_t bytes_allocated()

    cdef cppclass CBuffer" arrow::Buffer":
        uint8_t* data()
        int64_t size()

    cdef cppclass ResizableBuffer(CBuffer):
        CStatus Resize(int64_t nbytes)
        CStatus Reserve(int64_t nbytes)

    cdef cppclass PoolBuffer(ResizableBuffer):
        PoolBuffer()
        PoolBuffer(MemoryPool*)

    cdef MemoryPool* default_memory_pool()

    cdef cppclass CListType" arrow::ListType"(CDataType):
        CListType(const shared_ptr[CDataType]& value_type)

    cdef cppclass CStringType" arrow::StringType"(CDataType):
        pass

    cdef cppclass CTimestampType" arrow::TimestampType"(CDataType):
        TimeUnit unit

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

        c_bool Equals(const shared_ptr[CSchema]& other)

        shared_ptr[CField] field(int i)
        int num_fields()
        c_string ToString()

    cdef cppclass CArray" arrow::Array":
        shared_ptr[CDataType] type()

        int32_t length()
        int32_t null_count()
        Type type_enum()

        c_bool Equals(const shared_ptr[CArray]& arr)
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

    cdef cppclass CDateArray" arrow::DateArray"(CArray):
        int64_t Value(int i)

    cdef cppclass CTimestampArray" arrow::TimestampArray"(CArray):
        int64_t Value(int i)

    cdef cppclass CFloatArray" arrow::FloatArray"(CArray):
        float Value(int i)

    cdef cppclass CDoubleArray" arrow::DoubleArray"(CArray):
        double Value(int i)

    cdef cppclass CListArray" arrow::ListArray"(CArray):
        const int32_t* offsets()
        int32_t offset(int i)
        int32_t value_length(int i)
        shared_ptr[CArray] values()
        shared_ptr[CDataType] value_type()

    cdef cppclass CBinaryArray" arrow::BinaryArray"(CListArray):
        const uint8_t* GetValue(int i, int32_t* length)

    cdef cppclass CStringArray" arrow::StringArray"(CBinaryArray):
        c_string GetString(int i)

    cdef cppclass CChunkedArray" arrow::ChunkedArray":
        int64_t length()
        int64_t null_count()
        int num_chunks()
        shared_ptr[CArray] chunk(int i)

    cdef cppclass CColumn" arrow::Column":
        CColumn(const shared_ptr[CField]& field,
                const shared_ptr[CArray]& data)

        CColumn(const shared_ptr[CField]& field,
                const vector[shared_ptr[CArray]]& chunks)

        int64_t length()
        int64_t null_count()
        const c_string& name()
        shared_ptr[CDataType] type()
        shared_ptr[CChunkedArray] data()

    cdef cppclass CRecordBatch" arrow::RecordBatch":
        CRecordBatch(const shared_ptr[CSchema]& schema, int32_t num_rows,
                     const vector[shared_ptr[CArray]]& columns)

        c_bool Equals(const CRecordBatch& other)

        shared_ptr[CSchema] schema()
        shared_ptr[CArray] column(int i)
        const c_string& column_name(int i)

        const vector[shared_ptr[CArray]]& columns()

        int num_columns()
        int32_t num_rows()

    cdef cppclass CTable" arrow::Table":
        CTable(const c_string& name, const shared_ptr[CSchema]& schema,
               const vector[shared_ptr[CColumn]]& columns)

        int num_columns()
        int num_rows()

        const c_string& name()

        shared_ptr[CSchema] schema()
        shared_ptr[CColumn] column(int i)


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
        CStatus Open(const shared_ptr[CBuffer]& buf,
                     shared_ptr[Message]* out)
        int64_t body_length()
        MessageType type()

        shared_ptr[SchemaMessage] GetSchema()
        shared_ptr[RecordBatchMessage] GetRecordBatch()
        shared_ptr[DictionaryBatchMessage] GetDictionaryBatch()
