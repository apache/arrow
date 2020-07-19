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

cdef extern from "arrow/util/key_value_metadata.h" namespace "arrow" nogil:
    cdef cppclass CKeyValueMetadata" arrow::KeyValueMetadata":
        CKeyValueMetadata()
        CKeyValueMetadata(const unordered_map[c_string, c_string]&)
        CKeyValueMetadata(const vector[c_string]& keys,
                          const vector[c_string]& values)

        void reserve(int64_t n)
        int64_t size() const
        c_string key(int64_t i) const
        c_string value(int64_t i) const
        int FindKey(const c_string& key) const

        shared_ptr[CKeyValueMetadata] Copy() const
        c_bool Equals(const CKeyValueMetadata& other)
        void Append(const c_string& key, const c_string& value)
        void ToUnorderedMap(unordered_map[c_string, c_string]*) const
        c_string ToString() const

        CResult[c_string] Get(const c_string& key) const
        CStatus Delete(const c_string& key)
        CStatus Set(const c_string& key, const c_string& value)
        c_bool Contains(const c_string& key) const


cdef extern from "arrow/util/decimal.h" namespace "arrow" nogil:
    cdef cppclass CDecimal128" arrow::Decimal128":
        c_string ToString(int32_t scale) const


cdef extern from "arrow/api.h" namespace "arrow" nogil:

    cdef cppclass CBuildInfo "arrow::BuildInfo":
        int version
        int version_major
        int version_minor
        int version_patch
        c_string version_string
        c_string so_version
        c_string full_so_version
        c_string compiler_id
        c_string compiler_version
        c_string compiler_flags
        c_string git_id
        c_string git_description
        c_string package_kind

    const CBuildInfo& GetBuildInfo()

    enum Type" arrow::Type::type":
        _Type_NA" arrow::Type::NA"

        _Type_BOOL" arrow::Type::BOOL"

        _Type_UINT8" arrow::Type::UINT8"
        _Type_INT8" arrow::Type::INT8"
        _Type_UINT16" arrow::Type::UINT16"
        _Type_INT16" arrow::Type::INT16"
        _Type_UINT32" arrow::Type::UINT32"
        _Type_INT32" arrow::Type::INT32"
        _Type_UINT64" arrow::Type::UINT64"
        _Type_INT64" arrow::Type::INT64"

        _Type_HALF_FLOAT" arrow::Type::HALF_FLOAT"
        _Type_FLOAT" arrow::Type::FLOAT"
        _Type_DOUBLE" arrow::Type::DOUBLE"

        _Type_DECIMAL" arrow::Type::DECIMAL"

        _Type_DATE32" arrow::Type::DATE32"
        _Type_DATE64" arrow::Type::DATE64"
        _Type_TIMESTAMP" arrow::Type::TIMESTAMP"
        _Type_TIME32" arrow::Type::TIME32"
        _Type_TIME64" arrow::Type::TIME64"
        _Type_DURATION" arrow::Type::DURATION"

        _Type_BINARY" arrow::Type::BINARY"
        _Type_STRING" arrow::Type::STRING"
        _Type_LARGE_BINARY" arrow::Type::LARGE_BINARY"
        _Type_LARGE_STRING" arrow::Type::LARGE_STRING"
        _Type_FIXED_SIZE_BINARY" arrow::Type::FIXED_SIZE_BINARY"

        _Type_LIST" arrow::Type::LIST"
        _Type_LARGE_LIST" arrow::Type::LARGE_LIST"
        _Type_FIXED_SIZE_LIST" arrow::Type::FIXED_SIZE_LIST"
        _Type_STRUCT" arrow::Type::STRUCT"
        _Type_SPARSE_UNION" arrow::Type::SPARSE_UNION"
        _Type_DENSE_UNION" arrow::Type::DENSE_UNION"
        _Type_DICTIONARY" arrow::Type::DICTIONARY"
        _Type_MAP" arrow::Type::MAP"

        _Type_EXTENSION" arrow::Type::EXTENSION"

    enum UnionMode" arrow::UnionMode::type":
        _UnionMode_SPARSE" arrow::UnionMode::SPARSE"
        _UnionMode_DENSE" arrow::UnionMode::DENSE"

    enum TimeUnit" arrow::TimeUnit::type":
        TimeUnit_SECOND" arrow::TimeUnit::SECOND"
        TimeUnit_MILLI" arrow::TimeUnit::MILLI"
        TimeUnit_MICRO" arrow::TimeUnit::MICRO"
        TimeUnit_NANO" arrow::TimeUnit::NANO"

    cdef cppclass CBufferSpec" arrow::DataTypeLayout::BufferSpec":
        pass

    cdef cppclass CDataTypeLayout" arrow::DataTypeLayout":
        vector[CBufferSpec] buffers
        c_bool has_dictionary

    cdef cppclass CDataType" arrow::DataType":
        Type id()

        c_bool Equals(const CDataType& other)

        shared_ptr[CField] field(int i)
        const vector[shared_ptr[CField]] fields()
        int num_fields()
        CDataTypeLayout layout()
        c_string ToString()

    c_bool is_primitive(Type type)

    cdef cppclass CArrayData" arrow::ArrayData":
        shared_ptr[CDataType] type
        int64_t length
        int64_t null_count
        int64_t offset
        vector[shared_ptr[CBuffer]] buffers
        vector[shared_ptr[CArrayData]] child_data
        shared_ptr[CArrayData] dictionary

        @staticmethod
        shared_ptr[CArrayData] Make(const shared_ptr[CDataType]& type,
                                    int64_t length,
                                    vector[shared_ptr[CBuffer]]& buffers,
                                    int64_t null_count,
                                    int64_t offset)

        @staticmethod
        shared_ptr[CArrayData] MakeWithChildren" Make"(
            const shared_ptr[CDataType]& type,
            int64_t length,
            vector[shared_ptr[CBuffer]]& buffers,
            vector[shared_ptr[CArrayData]]& child_data,
            int64_t null_count,
            int64_t offset)

        @staticmethod
        shared_ptr[CArrayData] MakeWithChildrenAndDictionary" Make"(
            const shared_ptr[CDataType]& type,
            int64_t length,
            vector[shared_ptr[CBuffer]]& buffers,
            vector[shared_ptr[CArrayData]]& child_data,
            shared_ptr[CArrayData]& dictionary,
            int64_t null_count,
            int64_t offset)

    cdef cppclass CArray" arrow::Array":
        shared_ptr[CDataType] type()

        int64_t length()
        int64_t null_count()
        int64_t offset()
        Type type_id()

        int num_fields()

        CResult[shared_ptr[CScalar]] GetScalar(int64_t i) const

        c_string Diff(const CArray& other)
        c_bool Equals(const CArray& arr)
        c_bool IsNull(int i)

        shared_ptr[CArrayData] data()

        shared_ptr[CArray] Slice(int64_t offset)
        shared_ptr[CArray] Slice(int64_t offset, int64_t length)

        CStatus Validate() const
        CStatus ValidateFull() const
        CResult[shared_ptr[CArray]] View(const shared_ptr[CDataType]& type)

    shared_ptr[CArray] MakeArray(const shared_ptr[CArrayData]& data)
    CResult[shared_ptr[CArray]] MakeArrayOfNull(
        const shared_ptr[CDataType]& type, int64_t length, CMemoryPool* pool)

    CResult[shared_ptr[CArray]] MakeArrayFromScalar(
        const CScalar& scalar, int64_t length, CMemoryPool* pool)

    CStatus DebugPrint(const CArray& arr, int indent)

    cdef cppclass CFixedWidthType" arrow::FixedWidthType"(CDataType):
        int bit_width()

    cdef cppclass CNullArray" arrow::NullArray"(CArray):
        CNullArray(int64_t length)

    cdef cppclass CDictionaryArray" arrow::DictionaryArray"(CArray):
        CDictionaryArray(const shared_ptr[CDataType]& type,
                         const shared_ptr[CArray]& indices,
                         const shared_ptr[CArray]& dictionary)

        @staticmethod
        CResult[shared_ptr[CArray]] FromArrays(
            const shared_ptr[CDataType]& type,
            const shared_ptr[CArray]& indices,
            const shared_ptr[CArray]& dictionary)

        shared_ptr[CArray] indices()
        shared_ptr[CArray] dictionary()

    cdef cppclass CDate32Type" arrow::Date32Type"(CFixedWidthType):
        pass

    cdef cppclass CDate64Type" arrow::Date64Type"(CFixedWidthType):
        pass

    cdef cppclass CTimestampType" arrow::TimestampType"(CFixedWidthType):
        CTimestampType(TimeUnit unit)
        TimeUnit unit()
        const c_string& timezone()

    cdef cppclass CTime32Type" arrow::Time32Type"(CFixedWidthType):
        TimeUnit unit()

    cdef cppclass CTime64Type" arrow::Time64Type"(CFixedWidthType):
        TimeUnit unit()

    shared_ptr[CDataType] ctime32" arrow::time32"(TimeUnit unit)
    shared_ptr[CDataType] ctime64" arrow::time64"(TimeUnit unit)

    cdef cppclass CDurationType" arrow::DurationType"(CFixedWidthType):
        TimeUnit unit()

    shared_ptr[CDataType] cduration" arrow::duration"(TimeUnit unit)

    cdef cppclass CDictionaryType" arrow::DictionaryType"(CFixedWidthType):
        CDictionaryType(const shared_ptr[CDataType]& index_type,
                        const shared_ptr[CDataType]& value_type,
                        c_bool ordered)

        shared_ptr[CDataType] index_type()
        shared_ptr[CDataType] value_type()
        c_bool ordered()

    shared_ptr[CDataType] ctimestamp" arrow::timestamp"(TimeUnit unit)
    shared_ptr[CDataType] ctimestamp" arrow::timestamp"(
        TimeUnit unit, const c_string& timezone)

    cdef cppclass CMemoryPool" arrow::MemoryPool":
        int64_t bytes_allocated()
        int64_t max_memory()

    cdef cppclass CLoggingMemoryPool" arrow::LoggingMemoryPool"(CMemoryPool):
        CLoggingMemoryPool(CMemoryPool*)

    cdef cppclass CProxyMemoryPool" arrow::ProxyMemoryPool"(CMemoryPool):
        CProxyMemoryPool(CMemoryPool*)

    cdef cppclass CBuffer" arrow::Buffer":
        CBuffer(const uint8_t* data, int64_t size)
        const uint8_t* data()
        uint8_t* mutable_data()
        uintptr_t address()
        uintptr_t mutable_address()
        int64_t size()
        shared_ptr[CBuffer] parent()
        c_bool is_cpu() const
        c_bool is_mutable() const
        c_string ToHexString()
        c_bool Equals(const CBuffer& other)

    shared_ptr[CBuffer] SliceBuffer(const shared_ptr[CBuffer]& buffer,
                                    int64_t offset, int64_t length)
    shared_ptr[CBuffer] SliceBuffer(const shared_ptr[CBuffer]& buffer,
                                    int64_t offset)

    cdef cppclass CMutableBuffer" arrow::MutableBuffer"(CBuffer):
        CMutableBuffer(const uint8_t* data, int64_t size)

    cdef cppclass CResizableBuffer" arrow::ResizableBuffer"(CMutableBuffer):
        CStatus Resize(const int64_t new_size, c_bool shrink_to_fit)
        CStatus Reserve(const int64_t new_size)

    CResult[unique_ptr[CBuffer]] AllocateBuffer(const int64_t size,
                                                CMemoryPool* pool)

    CResult[unique_ptr[CResizableBuffer]] AllocateResizableBuffer(
        const int64_t size, CMemoryPool* pool)

    cdef CMemoryPool* c_default_memory_pool" arrow::default_memory_pool"()

    CStatus c_jemalloc_set_decay_ms" arrow::jemalloc_set_decay_ms"(int ms)

    cdef cppclass CListType" arrow::ListType"(CDataType):
        CListType(const shared_ptr[CDataType]& value_type)
        CListType(const shared_ptr[CField]& field)
        shared_ptr[CDataType] value_type()
        shared_ptr[CField] value_field()

    cdef cppclass CLargeListType" arrow::LargeListType"(CDataType):
        CLargeListType(const shared_ptr[CDataType]& value_type)
        CLargeListType(const shared_ptr[CField]& field)
        shared_ptr[CDataType] value_type()
        shared_ptr[CField] value_field()

    cdef cppclass CMapType" arrow::MapType"(CDataType):
        CMapType(const shared_ptr[CDataType]& key_type,
                 const shared_ptr[CDataType]& item_type, c_bool keys_sorted)
        shared_ptr[CDataType] key_type()
        shared_ptr[CDataType] item_type()
        c_bool keys_sorted()

    cdef cppclass CFixedSizeListType" arrow::FixedSizeListType"(CDataType):
        CFixedSizeListType(const shared_ptr[CDataType]& value_type,
                           int32_t list_size)
        CFixedSizeListType(const shared_ptr[CField]& field, int32_t list_size)
        shared_ptr[CDataType] value_type()
        shared_ptr[CField] value_field()
        int32_t list_size()

    cdef cppclass CStringType" arrow::StringType"(CDataType):
        pass

    cdef cppclass CFixedSizeBinaryType \
            " arrow::FixedSizeBinaryType"(CFixedWidthType):
        CFixedSizeBinaryType(int byte_width)
        int byte_width()
        int bit_width()

    cdef cppclass CDecimal128Type \
            " arrow::Decimal128Type"(CFixedSizeBinaryType):
        CDecimal128Type(int precision, int scale)
        int precision()
        int scale()

    cdef cppclass CField" arrow::Field":
        cppclass CMergeOptions "arrow::Field::MergeOptions":
            c_bool promote_nullability

            @staticmethod
            CMergeOptions Defaults()

        const c_string& name()
        shared_ptr[CDataType] type()
        c_bool nullable()

        c_string ToString()
        c_bool Equals(const CField& other, c_bool check_metadata)

        shared_ptr[const CKeyValueMetadata] metadata()

        CField(const c_string& name, const shared_ptr[CDataType]& type,
               c_bool nullable)

        CField(const c_string& name, const shared_ptr[CDataType]& type,
               c_bool nullable, const shared_ptr[CKeyValueMetadata]& metadata)

        # Removed const in Cython so don't have to cast to get code to generate
        shared_ptr[CField] AddMetadata(
            const shared_ptr[CKeyValueMetadata]& metadata)
        shared_ptr[CField] WithMetadata(
            const shared_ptr[CKeyValueMetadata]& metadata)
        shared_ptr[CField] RemoveMetadata()
        shared_ptr[CField] WithType(const shared_ptr[CDataType]& type)
        shared_ptr[CField] WithName(const c_string& name)
        shared_ptr[CField] WithNullable(c_bool nullable)
        vector[shared_ptr[CField]] Flatten()

    cdef cppclass CFieldRef" arrow::FieldRef":
        CFieldRef()
        CFieldRef(c_string name)
        CFieldRef(int index)

    cdef cppclass CStructType" arrow::StructType"(CDataType):
        CStructType(const vector[shared_ptr[CField]]& fields)

        shared_ptr[CField] GetFieldByName(const c_string& name)
        vector[shared_ptr[CField]] GetAllFieldsByName(const c_string& name)
        int GetFieldIndex(const c_string& name)
        vector[int] GetAllFieldIndices(const c_string& name)

    cdef cppclass CUnionType" arrow::UnionType"(CDataType):
        UnionMode mode()
        const vector[int8_t]& type_codes()
        const vector[int]& child_ids()

    cdef shared_ptr[CDataType] CMakeSparseUnionType" arrow::sparse_union"(
        vector[shared_ptr[CField]] fields,
        vector[int8_t] type_codes)

    cdef shared_ptr[CDataType] CMakeDenseUnionType" arrow::dense_union"(
        vector[shared_ptr[CField]] fields,
        vector[int8_t] type_codes)

    cdef cppclass CSchema" arrow::Schema":
        CSchema(const vector[shared_ptr[CField]]& fields)
        CSchema(const vector[shared_ptr[CField]]& fields,
                const shared_ptr[const CKeyValueMetadata]& metadata)

        # Does not actually exist, but gets Cython to not complain
        CSchema(const vector[shared_ptr[CField]]& fields,
                const shared_ptr[CKeyValueMetadata]& metadata)

        c_bool Equals(const CSchema& other, c_bool check_metadata)

        shared_ptr[CField] field(int i)
        shared_ptr[const CKeyValueMetadata] metadata()
        shared_ptr[CField] GetFieldByName(const c_string& name)
        vector[shared_ptr[CField]] GetAllFieldsByName(const c_string& name)
        int GetFieldIndex(const c_string& name)
        vector[int] GetAllFieldIndices(const c_string& name)
        int num_fields()
        c_string ToString()

        CResult[shared_ptr[CSchema]] AddField(int i,
                                              const shared_ptr[CField]& field)
        CResult[shared_ptr[CSchema]] RemoveField(int i)
        CResult[shared_ptr[CSchema]] SetField(int i,
                                              const shared_ptr[CField]& field)

        # Removed const in Cython so don't have to cast to get code to generate
        shared_ptr[CSchema] AddMetadata(
            const shared_ptr[CKeyValueMetadata]& metadata)
        shared_ptr[CSchema] WithMetadata(
            const shared_ptr[CKeyValueMetadata]& metadata)
        shared_ptr[CSchema] RemoveMetadata()

    CResult[shared_ptr[CSchema]] UnifySchemas(
        const vector[shared_ptr[CSchema]]& schemas)

    cdef cppclass PrettyPrintOptions:
        PrettyPrintOptions()
        PrettyPrintOptions(int indent_arg)
        PrettyPrintOptions(int indent_arg, int window_arg)
        int indent
        int indent_size
        int window
        c_string null_rep
        c_bool skip_new_lines
        c_bool truncate_metadata
        c_bool show_field_metadata
        c_bool show_schema_metadata

        @staticmethod
        PrettyPrintOptions Defaults()

    CStatus PrettyPrint(const CArray& schema,
                        const PrettyPrintOptions& options,
                        c_string* result)
    CStatus PrettyPrint(const CChunkedArray& schema,
                        const PrettyPrintOptions& options,
                        c_string* result)
    CStatus PrettyPrint(const CSchema& schema,
                        const PrettyPrintOptions& options,
                        c_string* result)

    cdef cppclass CBooleanArray" arrow::BooleanArray"(CArray):
        c_bool Value(int i)
        int64_t false_count()
        int64_t true_count()

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

    cdef cppclass CDate32Array" arrow::Date32Array"(CArray):
        int32_t Value(int i)

    cdef cppclass CDate64Array" arrow::Date64Array"(CArray):
        int64_t Value(int i)

    cdef cppclass CTime32Array" arrow::Time32Array"(CArray):
        int32_t Value(int i)

    cdef cppclass CTime64Array" arrow::Time64Array"(CArray):
        int64_t Value(int i)

    cdef cppclass CTimestampArray" arrow::TimestampArray"(CArray):
        int64_t Value(int i)

    cdef cppclass CDurationArray" arrow::DurationArray"(CArray):
        int64_t Value(int i)

    cdef cppclass CHalfFloatArray" arrow::HalfFloatArray"(CArray):
        uint16_t Value(int i)

    cdef cppclass CFloatArray" arrow::FloatArray"(CArray):
        float Value(int i)

    cdef cppclass CDoubleArray" arrow::DoubleArray"(CArray):
        double Value(int i)

    cdef cppclass CFixedSizeBinaryArray" arrow::FixedSizeBinaryArray"(CArray):
        const uint8_t* GetValue(int i)

    cdef cppclass CDecimal128Array" arrow::Decimal128Array"(
        CFixedSizeBinaryArray
    ):
        c_string FormatValue(int i)

    cdef cppclass CListArray" arrow::ListArray"(CArray):
        @staticmethod
        CResult[shared_ptr[CArray]] FromArrays(
            const CArray& offsets, const CArray& values, CMemoryPool* pool)

        const int32_t* raw_value_offsets()
        int32_t value_offset(int i)
        int32_t value_length(int i)
        shared_ptr[CArray] values()
        shared_ptr[CArray] offsets()
        shared_ptr[CDataType] value_type()

    cdef cppclass CLargeListArray" arrow::LargeListArray"(CArray):
        @staticmethod
        CResult[shared_ptr[CArray]] FromArrays(
            const CArray& offsets, const CArray& values, CMemoryPool* pool)

        int64_t value_offset(int i)
        int64_t value_length(int i)
        shared_ptr[CArray] values()
        shared_ptr[CArray] offsets()
        shared_ptr[CDataType] value_type()

    cdef cppclass CFixedSizeListArray" arrow::FixedSizeListArray"(CArray):
        @staticmethod
        CResult[shared_ptr[CArray]] FromArrays(
            const shared_ptr[CArray]& values, int32_t list_size)

        int64_t value_offset(int i)
        int64_t value_length(int i)
        shared_ptr[CArray] values()
        shared_ptr[CDataType] value_type()

    cdef cppclass CMapArray" arrow::MapArray"(CArray):
        @staticmethod
        CResult[shared_ptr[CArray]] FromArrays(
            const shared_ptr[CArray]& offsets,
            const shared_ptr[CArray]& keys,
            const shared_ptr[CArray]& items,
            CMemoryPool* pool)

        shared_ptr[CArray] keys()
        shared_ptr[CArray] items()
        CMapType* map_type()
        int64_t value_offset(int i)
        int64_t value_length(int i)
        shared_ptr[CArray] values()
        shared_ptr[CDataType] value_type()

    cdef cppclass CUnionArray" arrow::UnionArray"(CArray):
        shared_ptr[CBuffer] type_codes()
        int8_t* raw_type_codes()
        int child_id(int64_t index)
        shared_ptr[CArray] field(int pos)
        const CArray* UnsafeField(int pos)
        UnionMode mode()

    cdef cppclass CSparseUnionArray" arrow::SparseUnionArray"(CUnionArray):
        @staticmethod
        CResult[shared_ptr[CArray]] Make(
            const CArray& type_codes,
            const vector[shared_ptr[CArray]]& children,
            const vector[c_string]& field_names,
            const vector[int8_t]& type_codes)

    cdef cppclass CDenseUnionArray" arrow::DenseUnionArray"(CUnionArray):
        @staticmethod
        CResult[shared_ptr[CArray]] Make(
            const CArray& type_codes,
            const CArray& value_offsets,
            const vector[shared_ptr[CArray]]& children,
            const vector[c_string]& field_names,
            const vector[int8_t]& type_codes)

        int32_t value_offset(int i)
        shared_ptr[CBuffer] value_offsets()

    cdef cppclass CBinaryArray" arrow::BinaryArray"(CArray):
        const uint8_t* GetValue(int i, int32_t* length)
        shared_ptr[CBuffer] value_data()
        int32_t value_offset(int64_t i)
        int32_t value_length(int64_t i)
        int32_t total_values_length()

    cdef cppclass CLargeBinaryArray" arrow::LargeBinaryArray"(CArray):
        const uint8_t* GetValue(int i, int64_t* length)
        shared_ptr[CBuffer] value_data()
        int64_t value_offset(int64_t i)
        int64_t value_length(int64_t i)
        int64_t total_values_length()

    cdef cppclass CStringArray" arrow::StringArray"(CBinaryArray):
        CStringArray(int64_t length, shared_ptr[CBuffer] value_offsets,
                     shared_ptr[CBuffer] data,
                     shared_ptr[CBuffer] null_bitmap,
                     int64_t null_count,
                     int64_t offset)

        c_string GetString(int i)

    cdef cppclass CLargeStringArray" arrow::LargeStringArray" \
            (CLargeBinaryArray):
        CLargeStringArray(int64_t length, shared_ptr[CBuffer] value_offsets,
                          shared_ptr[CBuffer] data,
                          shared_ptr[CBuffer] null_bitmap,
                          int64_t null_count,
                          int64_t offset)

        c_string GetString(int i)

    cdef cppclass CStructArray" arrow::StructArray"(CArray):
        CStructArray(shared_ptr[CDataType] type, int64_t length,
                     vector[shared_ptr[CArray]] children,
                     shared_ptr[CBuffer] null_bitmap=nullptr,
                     int64_t null_count=-1,
                     int64_t offset=0)

        # XXX Cython crashes if default argument values are declared here
        # https://github.com/cython/cython/issues/2167
        @staticmethod
        CResult[shared_ptr[CArray]] MakeFromFieldNames "Make"(
            vector[shared_ptr[CArray]] children,
            vector[c_string] field_names,
            shared_ptr[CBuffer] null_bitmap,
            int64_t null_count,
            int64_t offset)

        @staticmethod
        CResult[shared_ptr[CArray]] MakeFromFields "Make"(
            vector[shared_ptr[CArray]] children,
            vector[shared_ptr[CField]] fields,
            shared_ptr[CBuffer] null_bitmap,
            int64_t null_count,
            int64_t offset)

        shared_ptr[CArray] field(int pos)
        shared_ptr[CArray] GetFieldByName(const c_string& name) const

        CResult[vector[shared_ptr[CArray]]] Flatten(CMemoryPool* pool)

    cdef cppclass CChunkedArray" arrow::ChunkedArray":
        CChunkedArray(const vector[shared_ptr[CArray]]& arrays)
        CChunkedArray(const vector[shared_ptr[CArray]]& arrays,
                      const shared_ptr[CDataType]& type)
        int64_t length()
        int64_t null_count()
        int num_chunks()
        c_bool Equals(const CChunkedArray& other)

        shared_ptr[CArray] chunk(int i)
        shared_ptr[CDataType] type()
        shared_ptr[CChunkedArray] Slice(int64_t offset, int64_t length) const
        shared_ptr[CChunkedArray] Slice(int64_t offset) const

        CResult[vector[shared_ptr[CChunkedArray]]] Flatten(CMemoryPool* pool)

        CStatus Validate() const
        CStatus ValidateFull() const

    cdef cppclass CRecordBatch" arrow::RecordBatch":
        @staticmethod
        shared_ptr[CRecordBatch] Make(
            const shared_ptr[CSchema]& schema, int64_t num_rows,
            const vector[shared_ptr[CArray]]& columns)

        @staticmethod
        CResult[shared_ptr[CRecordBatch]] FromStructArray(
            const shared_ptr[CArray]& array)

        c_bool Equals(const CRecordBatch& other, c_bool check_metadata)

        shared_ptr[CSchema] schema()
        shared_ptr[CArray] column(int i)
        const c_string& column_name(int i)

        const vector[shared_ptr[CArray]]& columns()

        int num_columns()
        int64_t num_rows()

        CStatus Validate() const
        CStatus ValidateFull() const

        shared_ptr[CRecordBatch] ReplaceSchemaMetadata(
            const shared_ptr[CKeyValueMetadata]& metadata)

        shared_ptr[CRecordBatch] Slice(int64_t offset)
        shared_ptr[CRecordBatch] Slice(int64_t offset, int64_t length)

    cdef cppclass CTable" arrow::Table":
        CTable(const shared_ptr[CSchema]& schema,
               const vector[shared_ptr[CChunkedArray]]& columns)

        @staticmethod
        shared_ptr[CTable] Make(
            const shared_ptr[CSchema]& schema,
            const vector[shared_ptr[CChunkedArray]]& columns)

        @staticmethod
        shared_ptr[CTable] MakeFromArrays" Make"(
            const shared_ptr[CSchema]& schema,
            const vector[shared_ptr[CArray]]& arrays)

        @staticmethod
        CResult[shared_ptr[CTable]] FromRecordBatches(
            const shared_ptr[CSchema]& schema,
            const vector[shared_ptr[CRecordBatch]]& batches)

        int num_columns()
        int64_t num_rows()

        c_bool Equals(const CTable& other, c_bool check_metadata)

        shared_ptr[CSchema] schema()
        shared_ptr[CChunkedArray] column(int i)
        shared_ptr[CField] field(int i)

        CResult[shared_ptr[CTable]] AddColumn(
            int i, shared_ptr[CField] field, shared_ptr[CChunkedArray] column)
        CResult[shared_ptr[CTable]] RemoveColumn(int i)
        CResult[shared_ptr[CTable]] SetColumn(
            int i, shared_ptr[CField] field, shared_ptr[CChunkedArray] column)

        vector[c_string] ColumnNames()
        CResult[shared_ptr[CTable]] RenameColumns(const vector[c_string]&)
        CResult[shared_ptr[CTable]] SelectColumns(const vector[int]&)

        CResult[shared_ptr[CTable]] Flatten(CMemoryPool* pool)

        CResult[shared_ptr[CTable]] CombineChunks(CMemoryPool* pool)

        CStatus Validate() const
        CStatus ValidateFull() const

        shared_ptr[CTable] ReplaceSchemaMetadata(
            const shared_ptr[CKeyValueMetadata]& metadata)

        shared_ptr[CTable] Slice(int64_t offset)
        shared_ptr[CTable] Slice(int64_t offset, int64_t length)

    cdef cppclass CRecordBatchReader" arrow::RecordBatchReader":
        shared_ptr[CSchema] schema()
        CStatus ReadNext(shared_ptr[CRecordBatch]* batch)
        CStatus ReadAll(shared_ptr[CTable]* out)

    cdef cppclass TableBatchReader(CRecordBatchReader):
        TableBatchReader(const CTable& table)
        void set_chunksize(int64_t chunksize)

    cdef cppclass CTensor" arrow::Tensor":
        shared_ptr[CDataType] type()
        shared_ptr[CBuffer] data()

        const vector[int64_t]& shape()
        const vector[int64_t]& strides()
        int64_t size()

        int ndim()
        const vector[c_string]& dim_names()
        const c_string& dim_name(int i)

        c_bool is_mutable()
        c_bool is_contiguous()
        Type type_id()
        c_bool Equals(const CTensor& other)

    cdef cppclass CSparseIndex" arrow::SparseIndex":
        pass

    cdef cppclass CSparseCOOIndex" arrow::SparseCOOIndex":
        c_bool is_canonical()

    cdef cppclass CSparseCOOTensor" arrow::SparseCOOTensor":
        shared_ptr[CDataType] type()
        shared_ptr[CBuffer] data()
        CResult[shared_ptr[CTensor]] ToTensor()

        shared_ptr[CSparseIndex] sparse_index()

        const vector[int64_t]& shape()
        int64_t size()
        int64_t non_zero_length()

        int ndim()
        const vector[c_string]& dim_names()
        const c_string& dim_name(int i)

        c_bool is_mutable()
        Type type_id()
        c_bool Equals(const CSparseCOOTensor& other)

    cdef cppclass CSparseCSRMatrix" arrow::SparseCSRMatrix":
        shared_ptr[CDataType] type()
        shared_ptr[CBuffer] data()
        CResult[shared_ptr[CTensor]] ToTensor()

        const vector[int64_t]& shape()
        int64_t size()
        int64_t non_zero_length()

        int ndim()
        const vector[c_string]& dim_names()
        const c_string& dim_name(int i)

        c_bool is_mutable()
        Type type_id()
        c_bool Equals(const CSparseCSRMatrix& other)

    cdef cppclass CSparseCSCMatrix" arrow::SparseCSCMatrix":
        shared_ptr[CDataType] type()
        shared_ptr[CBuffer] data()
        CResult[shared_ptr[CTensor]] ToTensor()

        const vector[int64_t]& shape()
        int64_t size()
        int64_t non_zero_length()

        int ndim()
        const vector[c_string]& dim_names()
        const c_string& dim_name(int i)

        c_bool is_mutable()
        Type type_id()
        c_bool Equals(const CSparseCSCMatrix& other)

    cdef cppclass CSparseCSFTensor" arrow::SparseCSFTensor":
        shared_ptr[CDataType] type()
        shared_ptr[CBuffer] data()
        CResult[shared_ptr[CTensor]] ToTensor()

        const vector[int64_t]& shape()
        int64_t size()
        int64_t non_zero_length()

        int ndim()
        const vector[c_string]& dim_names()
        const c_string& dim_name(int i)

        c_bool is_mutable()
        Type type_id()
        c_bool Equals(const CSparseCSFTensor& other)

    cdef cppclass CScalar" arrow::Scalar":
        shared_ptr[CDataType] type
        c_bool is_valid
        c_string ToString() const
        c_bool Equals(const CScalar& other) const
        CResult[shared_ptr[CScalar]] CastTo(shared_ptr[CDataType] to) const

    cdef cppclass CScalarHash" arrow::Scalar::Hash":
        size_t operator()(const shared_ptr[CScalar]& scalar) const

    cdef cppclass CNullScalar" arrow::NullScalar"(CScalar):
        CNullScalar()

    cdef cppclass CBooleanScalar" arrow::BooleanScalar"(CScalar):
        c_bool value

    cdef cppclass CInt8Scalar" arrow::Int8Scalar"(CScalar):
        int8_t value

    cdef cppclass CUInt8Scalar" arrow::UInt8Scalar"(CScalar):
        uint8_t value

    cdef cppclass CInt16Scalar" arrow::Int16Scalar"(CScalar):
        int16_t value

    cdef cppclass CUInt16Scalar" arrow::UInt16Scalar"(CScalar):
        uint16_t value

    cdef cppclass CInt32Scalar" arrow::Int32Scalar"(CScalar):
        int32_t value

    cdef cppclass CUInt32Scalar" arrow::UInt32Scalar"(CScalar):
        uint32_t value

    cdef cppclass CInt64Scalar" arrow::Int64Scalar"(CScalar):
        int64_t value

    cdef cppclass CUInt64Scalar" arrow::UInt64Scalar"(CScalar):
        uint64_t value

    cdef cppclass CHalfFloatScalar" arrow::HalfFloatScalar"(CScalar):
        npy_half value

    cdef cppclass CFloatScalar" arrow::FloatScalar"(CScalar):
        float value

    cdef cppclass CDoubleScalar" arrow::DoubleScalar"(CScalar):
        double value

    cdef cppclass CDecimal128Scalar" arrow::Decimal128Scalar"(CScalar):
        CDecimal128 value

    cdef cppclass CDate32Scalar" arrow::Date32Scalar"(CScalar):
        int32_t value

    cdef cppclass CDate64Scalar" arrow::Date64Scalar"(CScalar):
        int64_t value

    cdef cppclass CTime32Scalar" arrow::Time32Scalar"(CScalar):
        int32_t value

    cdef cppclass CTime64Scalar" arrow::Time64Scalar"(CScalar):
        int64_t value

    cdef cppclass CTimestampScalar" arrow::TimestampScalar"(CScalar):
        int64_t value

    cdef cppclass CDurationScalar" arrow::DurationScalar"(CScalar):
        int64_t value

    cdef cppclass CBaseBinaryScalar" arrow::BaseBinaryScalar"(CScalar):
        shared_ptr[CBuffer] value

    cdef cppclass CBaseListScalar" arrow::BaseListScalar"(CScalar):
        shared_ptr[CArray] value

    cdef cppclass CListScalar" arrow::ListScalar"(CBaseListScalar):
        pass

    cdef cppclass CMapScalar" arrow::MapScalar"(CListScalar):
        pass

    cdef cppclass CStructScalar" arrow::StructScalar"(CScalar):
        vector[shared_ptr[CScalar]] value
        CResult[shared_ptr[CScalar]] field(CFieldRef ref) const

    cdef cppclass CDictionaryScalar" arrow::DictionaryScalar"(CScalar):
        cppclass CDictionaryValue "arrow::DictionaryScalar::ValueType":
            shared_ptr[CScalar] index
            shared_ptr[CArray] dictionary

        CDictionaryValue value
        CResult[shared_ptr[CScalar]] GetEncodedValue()

    cdef cppclass CUnionScalar" arrow::UnionScalar"(CScalar):
        shared_ptr[CScalar] value

    shared_ptr[CScalar] MakeScalar[Value](Value value)

    cdef cppclass CConcatenateTablesOptions" arrow::ConcatenateTablesOptions":
        c_bool unify_schemas
        CField.CMergeOptions field_merge_options

        @staticmethod
        CConcatenateTablesOptions Defaults()

    CResult[shared_ptr[CTable]] ConcatenateTables(
        const vector[shared_ptr[CTable]]& tables,
        CConcatenateTablesOptions options,
        CMemoryPool* memory_pool)

cdef extern from "arrow/builder.h" namespace "arrow" nogil:

    cdef cppclass CArrayBuilder" arrow::ArrayBuilder":
        CArrayBuilder(shared_ptr[CDataType], CMemoryPool* pool)

        int64_t length()
        int64_t null_count()
        CStatus AppendNull()
        CStatus Finish(shared_ptr[CArray]* out)
        CStatus Reserve(int64_t additional_capacity)

    cdef cppclass CBooleanBuilder" arrow::BooleanBuilder"(CArrayBuilder):
        CBooleanBuilder(CMemoryPool* pool)
        CStatus Append(const bint val)
        CStatus Append(const uint8_t val)

    cdef cppclass CInt8Builder" arrow::Int8Builder"(CArrayBuilder):
        CInt8Builder(CMemoryPool* pool)
        CStatus Append(const int8_t value)

    cdef cppclass CInt16Builder" arrow::Int16Builder"(CArrayBuilder):
        CInt16Builder(CMemoryPool* pool)
        CStatus Append(const int16_t value)

    cdef cppclass CInt32Builder" arrow::Int32Builder"(CArrayBuilder):
        CInt32Builder(CMemoryPool* pool)
        CStatus Append(const int32_t value)

    cdef cppclass CInt64Builder" arrow::Int64Builder"(CArrayBuilder):
        CInt64Builder(CMemoryPool* pool)
        CStatus Append(const int64_t value)

    cdef cppclass CUInt8Builder" arrow::UInt8Builder"(CArrayBuilder):
        CUInt8Builder(CMemoryPool* pool)
        CStatus Append(const uint8_t value)

    cdef cppclass CUInt16Builder" arrow::UInt16Builder"(CArrayBuilder):
        CUInt16Builder(CMemoryPool* pool)
        CStatus Append(const uint16_t value)

    cdef cppclass CUInt32Builder" arrow::UInt32Builder"(CArrayBuilder):
        CUInt32Builder(CMemoryPool* pool)
        CStatus Append(const uint32_t value)

    cdef cppclass CUInt64Builder" arrow::UInt64Builder"(CArrayBuilder):
        CUInt64Builder(CMemoryPool* pool)
        CStatus Append(const uint64_t value)

    cdef cppclass CHalfFloatBuilder" arrow::HalfFloatBuilder"(CArrayBuilder):
        CHalfFloatBuilder(CMemoryPool* pool)

    cdef cppclass CFloatBuilder" arrow::FloatBuilder"(CArrayBuilder):
        CFloatBuilder(CMemoryPool* pool)
        CStatus Append(const float value)

    cdef cppclass CDoubleBuilder" arrow::DoubleBuilder"(CArrayBuilder):
        CDoubleBuilder(CMemoryPool* pool)
        CStatus Append(const double value)

    cdef cppclass CBinaryBuilder" arrow::BinaryBuilder"(CArrayBuilder):
        CArrayBuilder(shared_ptr[CDataType], CMemoryPool* pool)
        CStatus Append(const char* value, int32_t length)

    cdef cppclass CStringBuilder" arrow::StringBuilder"(CBinaryBuilder):
        CStringBuilder(CMemoryPool* pool)

        CStatus Append(const c_string& value)

    cdef cppclass CTimestampBuilder "arrow::TimestampBuilder"(CArrayBuilder):
        CTimestampBuilder(const shared_ptr[CDataType] typ, CMemoryPool* pool)
        CStatus Append(const int64_t value)

    cdef cppclass CDate32Builder "arrow::Date32Builder"(CArrayBuilder):
        CDate32Builder(CMemoryPool* pool)
        CStatus Append(const int32_t value)

    cdef cppclass CDate64Builder "arrow::Date64Builder"(CArrayBuilder):
        CDate64Builder(CMemoryPool* pool)
        CStatus Append(const int64_t value)


# Use typedef to emulate syntax for std::function<void(..)>
ctypedef void CallbackTransform(
    object, const shared_ptr[CBuffer]& src, shared_ptr[CBuffer]* dest)


cdef extern from "arrow/io/api.h" namespace "arrow::io" nogil:
    enum FileMode" arrow::io::FileMode::type":
        FileMode_READ" arrow::io::FileMode::READ"
        FileMode_WRITE" arrow::io::FileMode::WRITE"
        FileMode_READWRITE" arrow::io::FileMode::READWRITE"

    enum ObjectType" arrow::io::ObjectType::type":
        ObjectType_FILE" arrow::io::ObjectType::FILE"
        ObjectType_DIRECTORY" arrow::io::ObjectType::DIRECTORY"

    cdef cppclass FileStatistics:
        int64_t size
        ObjectType kind

    cdef cppclass FileInterface:
        CStatus Close()
        CResult[int64_t] Tell()
        FileMode mode()
        c_bool closed()

    cdef cppclass Readable:
        # put overload under a different name to avoid cython bug with multiple
        # layers of inheritance
        CResult[shared_ptr[CBuffer]] ReadBuffer" Read"(int64_t nbytes)
        CResult[int64_t] Read(int64_t nbytes, uint8_t* out)

    cdef cppclass Seekable:
        CStatus Seek(int64_t position)

    cdef cppclass Writable:
        CStatus WriteBuffer" Write"(shared_ptr[CBuffer] data)
        CStatus Write(const uint8_t* data, int64_t nbytes)
        CStatus Flush()

    cdef cppclass COutputStream" arrow::io::OutputStream"(FileInterface,
                                                          Writable):
        pass

    cdef cppclass CInputStream" arrow::io::InputStream"(FileInterface,
                                                        Readable):
        pass

    cdef cppclass CRandomAccessFile" arrow::io::RandomAccessFile"(CInputStream,
                                                                  Seekable):
        CResult[int64_t] GetSize()

        CResult[int64_t] ReadAt(int64_t position, int64_t nbytes,
                                uint8_t* buffer)
        CResult[shared_ptr[CBuffer]] ReadAt(int64_t position, int64_t nbytes)
        c_bool supports_zero_copy()

    cdef cppclass WritableFile(COutputStream, Seekable):
        CStatus WriteAt(int64_t position, const uint8_t* data,
                        int64_t nbytes)

    cdef cppclass ReadWriteFileInterface(CRandomAccessFile,
                                         WritableFile):
        pass

    cdef cppclass CIOFileSystem" arrow::io::FileSystem":
        CStatus Stat(const c_string& path, FileStatistics* stat)

    cdef cppclass FileOutputStream(COutputStream):
        @staticmethod
        CResult[shared_ptr[COutputStream]] Open(const c_string& path)

        int file_descriptor()

    cdef cppclass ReadableFile(CRandomAccessFile):
        @staticmethod
        CResult[shared_ptr[ReadableFile]] Open(const c_string& path)

        @staticmethod
        CResult[shared_ptr[ReadableFile]] Open(const c_string& path,
                                               CMemoryPool* memory_pool)

        int file_descriptor()

    cdef cppclass CMemoryMappedFile \
            " arrow::io::MemoryMappedFile"(ReadWriteFileInterface):

        @staticmethod
        CResult[shared_ptr[CMemoryMappedFile]] Create(const c_string& path,
                                                      int64_t size)

        @staticmethod
        CResult[shared_ptr[CMemoryMappedFile]] Open(const c_string& path,
                                                    FileMode mode)

        CStatus Resize(int64_t size)

        int file_descriptor()

    cdef cppclass CCompressedInputStream \
            " arrow::io::CompressedInputStream"(CInputStream):
        @staticmethod
        CResult[shared_ptr[CCompressedInputStream]] Make(
            CCodec* codec, shared_ptr[CInputStream] raw)

    cdef cppclass CCompressedOutputStream \
            " arrow::io::CompressedOutputStream"(COutputStream):
        @staticmethod
        CResult[shared_ptr[CCompressedOutputStream]] Make(
            CCodec* codec, shared_ptr[COutputStream] raw)

    cdef cppclass CBufferedInputStream \
            " arrow::io::BufferedInputStream"(CInputStream):

        @staticmethod
        CResult[shared_ptr[CBufferedInputStream]] Create(
            int64_t buffer_size, CMemoryPool* pool,
            shared_ptr[CInputStream] raw)

        CResult[shared_ptr[CInputStream]] Detach()

    cdef cppclass CBufferedOutputStream \
            " arrow::io::BufferedOutputStream"(COutputStream):

        @staticmethod
        CResult[shared_ptr[CBufferedOutputStream]] Create(
            int64_t buffer_size, CMemoryPool* pool,
            shared_ptr[COutputStream] raw)

        CResult[shared_ptr[COutputStream]] Detach()

    cdef cppclass CTransformInputStreamVTable \
            "arrow::py::TransformInputStreamVTable":
        CTransformInputStreamVTable()
        function[CallbackTransform] transform

    shared_ptr[CInputStream] MakeTransformInputStream \
        "arrow::py::MakeTransformInputStream"(
        shared_ptr[CInputStream] wrapped, CTransformInputStreamVTable vtable,
        object method_arg)

    # ----------------------------------------------------------------------
    # HDFS

    CStatus HaveLibHdfs()
    CStatus HaveLibHdfs3()

    enum HdfsDriver" arrow::io::HdfsDriver":
        HdfsDriver_LIBHDFS" arrow::io::HdfsDriver::LIBHDFS"
        HdfsDriver_LIBHDFS3" arrow::io::HdfsDriver::LIBHDFS3"

    cdef cppclass HdfsConnectionConfig:
        c_string host
        int port
        c_string user
        c_string kerb_ticket
        unordered_map[c_string, c_string] extra_conf
        HdfsDriver driver

    cdef cppclass HdfsPathInfo:
        ObjectType kind
        c_string name
        c_string owner
        c_string group
        int32_t last_modified_time
        int32_t last_access_time
        int64_t size
        int16_t replication
        int64_t block_size
        int16_t permissions

    cdef cppclass HdfsReadableFile(CRandomAccessFile):
        pass

    cdef cppclass HdfsOutputStream(COutputStream):
        pass

    cdef cppclass CIOHadoopFileSystem \
            "arrow::io::HadoopFileSystem"(CIOFileSystem):
        @staticmethod
        CStatus Connect(const HdfsConnectionConfig* config,
                        shared_ptr[CIOHadoopFileSystem]* client)

        CStatus MakeDirectory(const c_string& path)

        CStatus Delete(const c_string& path, c_bool recursive)

        CStatus Disconnect()

        c_bool Exists(const c_string& path)

        CStatus Chmod(const c_string& path, int mode)
        CStatus Chown(const c_string& path, const char* owner,
                      const char* group)

        CStatus GetCapacity(int64_t* nbytes)
        CStatus GetUsed(int64_t* nbytes)

        CStatus ListDirectory(const c_string& path,
                              vector[HdfsPathInfo]* listing)

        CStatus GetPathInfo(const c_string& path, HdfsPathInfo* info)

        CStatus Rename(const c_string& src, const c_string& dst)

        CStatus OpenReadable(const c_string& path,
                             shared_ptr[HdfsReadableFile]* handle)

        CStatus OpenWritable(const c_string& path, c_bool append,
                             int32_t buffer_size, int16_t replication,
                             int64_t default_block_size,
                             shared_ptr[HdfsOutputStream]* handle)

    cdef cppclass CBufferReader \
            " arrow::io::BufferReader"(CRandomAccessFile):
        CBufferReader(const shared_ptr[CBuffer]& buffer)
        CBufferReader(const uint8_t* data, int64_t nbytes)

    cdef cppclass CBufferOutputStream \
            " arrow::io::BufferOutputStream"(COutputStream):
        CBufferOutputStream(const shared_ptr[CResizableBuffer]& buffer)

    cdef cppclass CMockOutputStream \
            " arrow::io::MockOutputStream"(COutputStream):
        CMockOutputStream()
        int64_t GetExtentBytesWritten()

    cdef cppclass CFixedSizeBufferWriter \
            " arrow::io::FixedSizeBufferWriter"(WritableFile):
        CFixedSizeBufferWriter(const shared_ptr[CBuffer]& buffer)

        void set_memcopy_threads(int num_threads)
        void set_memcopy_blocksize(int64_t blocksize)
        void set_memcopy_threshold(int64_t threshold)


cdef extern from "arrow/ipc/api.h" namespace "arrow::ipc" nogil:
    enum MessageType" arrow::ipc::MessageType":
        MessageType_SCHEMA" arrow::ipc::MessageType::SCHEMA"
        MessageType_RECORD_BATCH" arrow::ipc::MessageType::RECORD_BATCH"
        MessageType_DICTIONARY_BATCH\
            " arrow::ipc::MessageType::DICTIONARY_BATCH"

    # TODO: use "cpdef enum class" to automatically get a Python wrapper?
    # See
    # https://github.com/cython/cython/commit/2c7c22f51405299a4e247f78edf52957d30cf71d#diff-61c1365c0f761a8137754bb3a73bfbf7
    ctypedef enum CMetadataVersion" arrow::ipc::MetadataVersion":
        CMetadataVersion_V1" arrow::ipc::MetadataVersion::V1"
        CMetadataVersion_V2" arrow::ipc::MetadataVersion::V2"
        CMetadataVersion_V3" arrow::ipc::MetadataVersion::V3"
        CMetadataVersion_V4" arrow::ipc::MetadataVersion::V4"
        CMetadataVersion_V5" arrow::ipc::MetadataVersion::V5"

    cdef cppclass CIpcWriteOptions" arrow::ipc::IpcWriteOptions":
        c_bool allow_64bit
        int max_recursion_depth
        int32_t alignment
        c_bool write_legacy_ipc_format
        CMemoryPool* memory_pool
        CMetadataVersion metadata_version
        CCompressionType compression
        c_bool use_threads

        @staticmethod
        CIpcWriteOptions Defaults()

    cdef cppclass CIpcReadOptions" arrow::ipc::IpcReadOptions":
        int max_recursion_depth
        CMemoryPool* memory_pool
        shared_ptr[unordered_set[int]] included_fields

        @staticmethod
        CIpcReadOptions Defaults()

    cdef cppclass CDictionaryMemo" arrow::ipc::DictionaryMemo":
        pass

    cdef cppclass CIpcPayload" arrow::ipc::IpcPayload":
        MessageType type
        shared_ptr[CBuffer] metadata
        vector[shared_ptr[CBuffer]] body_buffers
        int64_t body_length

    cdef cppclass CMessage" arrow::ipc::Message":
        CResult[unique_ptr[CMessage]] Open(shared_ptr[CBuffer] metadata,
                                           shared_ptr[CBuffer] body)

        shared_ptr[CBuffer] body()

        c_bool Equals(const CMessage& other)

        shared_ptr[CBuffer] metadata()
        CMetadataVersion metadata_version()
        MessageType type()

        CStatus SerializeTo(COutputStream* stream,
                            const CIpcWriteOptions& options,
                            int64_t* output_length)

    c_string FormatMessageType(MessageType type)

    cdef cppclass CMessageReader" arrow::ipc::MessageReader":
        @staticmethod
        unique_ptr[CMessageReader] Open(const shared_ptr[CInputStream]& stream)

        CResult[unique_ptr[CMessage]] ReadNextMessage()

    cdef cppclass CRecordBatchWriter" arrow::ipc::RecordBatchWriter":
        CStatus Close()
        CStatus WriteRecordBatch(const CRecordBatch& batch)
        CStatus WriteTable(const CTable& table, int64_t max_chunksize)

    cdef cppclass CRecordBatchStreamReader \
            " arrow::ipc::RecordBatchStreamReader"(CRecordBatchReader):
        @staticmethod
        CResult[shared_ptr[CRecordBatchReader]] Open(
            const CInputStream* stream, const CIpcReadOptions& options)

        @staticmethod
        CResult[shared_ptr[CRecordBatchReader]] Open2" Open"(
            unique_ptr[CMessageReader] message_reader,
            const CIpcReadOptions& options)

    CResult[shared_ptr[CRecordBatchWriter]] NewStreamWriter(
        COutputStream* sink, const shared_ptr[CSchema]& schema,
        CIpcWriteOptions& options)

    CResult[shared_ptr[CRecordBatchWriter]] NewFileWriter(
        COutputStream* sink, const shared_ptr[CSchema]& schema,
        CIpcWriteOptions& options)

    cdef cppclass CRecordBatchFileReader \
            " arrow::ipc::RecordBatchFileReader":
        @staticmethod
        CResult[shared_ptr[CRecordBatchFileReader]] Open(
            CRandomAccessFile* file,
            const CIpcReadOptions& options)

        @staticmethod
        CResult[shared_ptr[CRecordBatchFileReader]] Open2" Open"(
            CRandomAccessFile* file, int64_t footer_offset,
            const CIpcReadOptions& options)

        shared_ptr[CSchema] schema()

        int num_record_batches()

        CResult[shared_ptr[CRecordBatch]] ReadRecordBatch(int i)

    CResult[unique_ptr[CMessage]] ReadMessage(CInputStream* stream,
                                              CMemoryPool* pool)

    CStatus GetRecordBatchSize(const CRecordBatch& batch, int64_t* size)
    CStatus GetTensorSize(const CTensor& tensor, int64_t* size)

    CStatus WriteTensor(const CTensor& tensor, COutputStream* dst,
                        int32_t* metadata_length,
                        int64_t* body_length)

    CResult[shared_ptr[CTensor]] ReadTensor(CInputStream* stream)

    CResult[shared_ptr[CRecordBatch]] ReadRecordBatch(
        const CMessage& message, const shared_ptr[CSchema]& schema,
        CDictionaryMemo* dictionary_memo,
        const CIpcReadOptions& options)

    CResult[shared_ptr[CBuffer]] SerializeSchema(
        const CSchema& schema, CDictionaryMemo* dictionary_memo,
        CMemoryPool* pool)

    CResult[shared_ptr[CBuffer]] SerializeRecordBatch(
        const CRecordBatch& schema, const CIpcWriteOptions& options)

    CResult[shared_ptr[CSchema]] ReadSchema(CInputStream* stream,
                                            CDictionaryMemo* dictionary_memo)

    CResult[shared_ptr[CRecordBatch]] ReadRecordBatch(
        const shared_ptr[CSchema]& schema,
        CDictionaryMemo* dictionary_memo,
        const CIpcReadOptions& options,
        CInputStream* stream)

    CStatus AlignStream(CInputStream* stream, int64_t alignment)
    CStatus AlignStream(COutputStream* stream, int64_t alignment)

    cdef CStatus GetRecordBatchPayload\
        " arrow::ipc::GetRecordBatchPayload"(
            const CRecordBatch& batch,
            const CIpcWriteOptions& options,
            CIpcPayload* out)

    int kFeatherV1Version" arrow::ipc::feather::kFeatherV1Version"
    int kFeatherV2Version" arrow::ipc::feather::kFeatherV2Version"

    cdef cppclass CFeatherProperties" arrow::ipc::feather::WriteProperties":
        int version
        int chunksize
        CCompressionType compression
        int compression_level

    CStatus WriteFeather" arrow::ipc::feather::WriteTable"\
        (const CTable& table, COutputStream* out,
         CFeatherProperties properties)

    cdef cppclass CFeatherReader" arrow::ipc::feather::Reader":
        @staticmethod
        CResult[shared_ptr[CFeatherReader]] Open(
            const shared_ptr[CRandomAccessFile]& file)
        int version()
        shared_ptr[CSchema] schema()

        CStatus Read(shared_ptr[CTable]* out)
        CStatus Read(const vector[int] indices, shared_ptr[CTable]* out)
        CStatus Read(const vector[c_string] names, shared_ptr[CTable]* out)


cdef extern from 'arrow/util/value_parsing.h' namespace 'arrow' nogil:
    cdef cppclass CTimestampParser" arrow::TimestampParser":
        const char* kind() const
        const char* format() const

        @staticmethod
        shared_ptr[CTimestampParser] MakeStrptime(c_string format)

        @staticmethod
        shared_ptr[CTimestampParser] MakeISO8601()


cdef extern from "arrow/csv/api.h" namespace "arrow::csv" nogil:

    cdef cppclass CCSVParseOptions" arrow::csv::ParseOptions":
        unsigned char delimiter
        c_bool quoting
        unsigned char quote_char
        c_bool double_quote
        c_bool escaping
        unsigned char escape_char
        c_bool newlines_in_values
        c_bool ignore_empty_lines

        @staticmethod
        CCSVParseOptions Defaults()

    cdef cppclass CCSVConvertOptions" arrow::csv::ConvertOptions":
        c_bool check_utf8
        unordered_map[c_string, shared_ptr[CDataType]] column_types
        vector[c_string] null_values
        vector[c_string] true_values
        vector[c_string] false_values
        c_bool strings_can_be_null
        vector[shared_ptr[CTimestampParser]] timestamp_parsers

        c_bool auto_dict_encode
        int32_t auto_dict_max_cardinality

        vector[c_string] include_columns
        c_bool include_missing_columns

        @staticmethod
        CCSVConvertOptions Defaults()

    cdef cppclass CCSVReadOptions" arrow::csv::ReadOptions":
        c_bool use_threads
        int32_t block_size
        int32_t skip_rows
        vector[c_string] column_names
        c_bool autogenerate_column_names

        @staticmethod
        CCSVReadOptions Defaults()

    cdef cppclass CCSVReader" arrow::csv::TableReader":
        @staticmethod
        CResult[shared_ptr[CCSVReader]] Make(
            CMemoryPool*, shared_ptr[CInputStream],
            CCSVReadOptions, CCSVParseOptions, CCSVConvertOptions)

        CResult[shared_ptr[CTable]] Read()

    cdef cppclass CCSVStreamingReader" arrow::csv::StreamingReader"(
            CRecordBatchReader):
        @staticmethod
        CResult[shared_ptr[CCSVStreamingReader]] Make(
            CMemoryPool*, shared_ptr[CInputStream],
            CCSVReadOptions, CCSVParseOptions, CCSVConvertOptions)


cdef extern from "arrow/json/options.h" nogil:

    ctypedef enum CUnexpectedFieldBehavior \
            "arrow::json::UnexpectedFieldBehavior":
        CUnexpectedFieldBehavior_Ignore \
            "arrow::json::UnexpectedFieldBehavior::Ignore"
        CUnexpectedFieldBehavior_Error \
            "arrow::json::UnexpectedFieldBehavior::Error"
        CUnexpectedFieldBehavior_InferType \
            "arrow::json::UnexpectedFieldBehavior::InferType"

    cdef cppclass CJSONReadOptions" arrow::json::ReadOptions":
        c_bool use_threads
        int32_t block_size

        @staticmethod
        CJSONReadOptions Defaults()

    cdef cppclass CJSONParseOptions" arrow::json::ParseOptions":
        shared_ptr[CSchema] explicit_schema
        c_bool newlines_in_values
        CUnexpectedFieldBehavior unexpected_field_behavior

        @staticmethod
        CJSONParseOptions Defaults()


cdef extern from "arrow/json/reader.h" namespace "arrow::json" nogil:

    cdef cppclass CJSONReader" arrow::json::TableReader":
        @staticmethod
        CResult[shared_ptr[CJSONReader]] Make(
            CMemoryPool*, shared_ptr[CInputStream],
            CJSONReadOptions, CJSONParseOptions)

        CResult[shared_ptr[CTable]] Read()


cdef extern from "arrow/compute/api.h" namespace "arrow::compute" nogil:

    cdef cppclass CExecContext" arrow::compute::ExecContext":
        CExecContext()
        CExecContext(CMemoryPool* pool)

    cdef cppclass CKernelSignature" arrow::compute::KernelSignature":
        c_string ToString() const

    cdef cppclass CKernel" arrow::compute::Kernel":
        shared_ptr[CKernelSignature] signature

    cdef cppclass CArrayKernel" arrow::compute::ArrayKernel"(CKernel):
        pass

    cdef cppclass CScalarKernel" arrow::compute::ScalarKernel"(CArrayKernel):
        pass

    cdef cppclass CVectorKernel" arrow::compute::VectorKernel"(CArrayKernel):
        pass

    cdef cppclass CScalarAggregateKernel \
            " arrow::compute::ScalarAggregateKernel"(CKernel):
        pass

    cdef cppclass CArity" arrow::compute::Arity":
        int num_args
        c_bool is_varargs

    enum FunctionKind" arrow::compute::Function::Kind":
        FunctionKind_SCALAR" arrow::compute::Function::SCALAR"
        FunctionKind_VECTOR" arrow::compute::Function::VECTOR"
        FunctionKind_SCALAR_AGGREGATE \
            " arrow::compute::Function::SCALAR_AGGREGATE"
        FunctionKind_META \
            " arrow::compute::Function::META"

    cdef cppclass CFunctionOptions" arrow::compute::FunctionOptions":
        pass

    cdef cppclass CFunction" arrow::compute::Function":
        const c_string& name() const
        FunctionKind kind() const
        const CArity& arity() const
        int num_kernels() const
        CResult[CDatum] Execute(const vector[CDatum]& args,
                                const CFunctionOptions* options,
                                CExecContext* ctx)

    cdef cppclass CScalarFunction" arrow::compute::ScalarFunction"(CFunction):
        vector[const CScalarKernel*] kernels() const

    cdef cppclass CVectorFunction" arrow::compute::VectorFunction"(CFunction):
        vector[const CVectorKernel*] kernels() const

    cdef cppclass CScalarAggregateFunction\
            " arrow::compute::ScalarAggregateFunction"\
            (CFunction):
        vector[const CScalarAggregateKernel*] kernels() const

    cdef cppclass CMetaFunction" arrow::compute::MetaFunction"(CFunction):
        pass

    cdef cppclass CFunctionRegistry" arrow::compute::FunctionRegistry":
        CResult[shared_ptr[CFunction]] GetFunction(
            const c_string& name) const
        vector[c_string] GetFunctionNames() const
        int num_functions() const

    CFunctionRegistry* GetFunctionRegistry()

    cdef cppclass CMatchSubstringOptions \
            "arrow::compute::MatchSubstringOptions"(CFunctionOptions):
        CMatchSubstringOptions(c_string pattern)
        c_string pattern

    cdef cppclass CCastOptions" arrow::compute::CastOptions"(CFunctionOptions):
        CCastOptions()
        CCastOptions(c_bool safe)

        @staticmethod
        CCastOptions Safe()

        @staticmethod
        CCastOptions Unsafe()
        shared_ptr[CDataType] to_type
        c_bool allow_int_overflow
        c_bool allow_time_truncate
        c_bool allow_time_overflow
        c_bool allow_float_truncate
        c_bool allow_invalid_utf8

    enum CFilterNullSelectionBehavior \
            "arrow::compute::FilterOptions::NullSelectionBehavior":
        CFilterNullSelectionBehavior_DROP \
            "arrow::compute::FilterOptions::DROP"
        CFilterNullSelectionBehavior_EMIT_NULL \
            "arrow::compute::FilterOptions::EMIT_NULL"

    cdef cppclass CFilterOptions \
            " arrow::compute::FilterOptions"(CFunctionOptions):
        CFilterOptions()
        CFilterOptions(CFilterNullSelectionBehavior null_selection)
        CFilterNullSelectionBehavior null_selection_behavior

    cdef cppclass CTakeOptions \
            " arrow::compute::TakeOptions"(CFunctionOptions):
        c_bool boundscheck

    enum DatumType" arrow::Datum::type":
        DatumType_NONE" arrow::Datum::NONE"
        DatumType_SCALAR" arrow::Datum::SCALAR"
        DatumType_ARRAY" arrow::Datum::ARRAY"
        DatumType_CHUNKED_ARRAY" arrow::Datum::CHUNKED_ARRAY"
        DatumType_RECORD_BATCH" arrow::Datum::RECORD_BATCH"
        DatumType_TABLE" arrow::Datum::TABLE"
        DatumType_COLLECTION" arrow::Datum::COLLECTION"

    cdef cppclass CDatum" arrow::Datum":
        CDatum()
        CDatum(const shared_ptr[CArray]& value)
        CDatum(const shared_ptr[CChunkedArray]& value)
        CDatum(const shared_ptr[CScalar]& value)
        CDatum(const shared_ptr[CRecordBatch]& value)
        CDatum(const shared_ptr[CTable]& value)

        DatumType kind()

        shared_ptr[CArrayData] array()
        shared_ptr[CChunkedArray] chunked_array()
        shared_ptr[CRecordBatch] record_batch()
        shared_ptr[CTable] table()
        shared_ptr[CScalar] scalar()


cdef extern from "arrow/python/api.h" namespace "arrow::py":
    # Requires GIL
    CStatus InferArrowType(object obj, object mask,
                           c_bool pandas_null_sentinels,
                           shared_ptr[CDataType]* out_type)


cdef extern from "arrow/python/api.h" namespace "arrow::py" nogil:
    shared_ptr[CDataType] GetPrimitiveType(Type type)

    object PyHalf_FromHalf(npy_half value)

    cdef cppclass PyConversionOptions:
        PyConversionOptions()

        shared_ptr[CDataType] type
        int64_t size
        CMemoryPool* pool
        c_bool from_pandas

    # TODO Some functions below are not actually "nogil"

    CStatus ConvertPySequence(object obj, object mask,
                              const PyConversionOptions& options,
                              shared_ptr[CChunkedArray]* out)

    CStatus NumPyDtypeToArrow(object dtype, shared_ptr[CDataType]* type)

    CStatus NdarrayToArrow(CMemoryPool* pool, object ao, object mo,
                           c_bool from_pandas,
                           const shared_ptr[CDataType]& type,
                           shared_ptr[CChunkedArray]* out)

    CStatus NdarrayToArrow(CMemoryPool* pool, object ao, object mo,
                           c_bool from_pandas,
                           const shared_ptr[CDataType]& type,
                           const CCastOptions& cast_options,
                           shared_ptr[CChunkedArray]* out)

    CStatus NdarrayToTensor(CMemoryPool* pool, object ao,
                            const vector[c_string]& dim_names,
                            shared_ptr[CTensor]* out)

    CStatus TensorToNdarray(const shared_ptr[CTensor]& tensor, object base,
                            PyObject** out)

    CStatus SparseCOOTensorToNdarray(
        const shared_ptr[CSparseCOOTensor]& sparse_tensor, object base,
        PyObject** out_data, PyObject** out_coords)

    CStatus SparseCSRMatrixToNdarray(
        const shared_ptr[CSparseCSRMatrix]& sparse_tensor, object base,
        PyObject** out_data, PyObject** out_indptr, PyObject** out_indices)

    CStatus SparseCSCMatrixToNdarray(
        const shared_ptr[CSparseCSCMatrix]& sparse_tensor, object base,
        PyObject** out_data, PyObject** out_indptr, PyObject** out_indices)

    CStatus SparseCSFTensorToNdarray(
        const shared_ptr[CSparseCSFTensor]& sparse_tensor, object base,
        PyObject** out_data, PyObject** out_indptr, PyObject** out_indices)

    CStatus NdarraysToSparseCOOTensor(CMemoryPool* pool, object data_ao,
                                      object coords_ao,
                                      const vector[int64_t]& shape,
                                      const vector[c_string]& dim_names,
                                      shared_ptr[CSparseCOOTensor]* out)

    CStatus NdarraysToSparseCSRMatrix(CMemoryPool* pool, object data_ao,
                                      object indptr_ao, object indices_ao,
                                      const vector[int64_t]& shape,
                                      const vector[c_string]& dim_names,
                                      shared_ptr[CSparseCSRMatrix]* out)

    CStatus NdarraysToSparseCSCMatrix(CMemoryPool* pool, object data_ao,
                                      object indptr_ao, object indices_ao,
                                      const vector[int64_t]& shape,
                                      const vector[c_string]& dim_names,
                                      shared_ptr[CSparseCSCMatrix]* out)

    CStatus NdarraysToSparseCSFTensor(CMemoryPool* pool, object data_ao,
                                      object indptr_ao, object indices_ao,
                                      const vector[int64_t]& shape,
                                      const vector[int64_t]& axis_order,
                                      const vector[c_string]& dim_names,
                                      shared_ptr[CSparseCSFTensor]* out)

    CStatus TensorToSparseCOOTensor(shared_ptr[CTensor],
                                    shared_ptr[CSparseCOOTensor]* out)

    CStatus TensorToSparseCSRMatrix(shared_ptr[CTensor],
                                    shared_ptr[CSparseCSRMatrix]* out)

    CStatus TensorToSparseCSCMatrix(shared_ptr[CTensor],
                                    shared_ptr[CSparseCSCMatrix]* out)

    CStatus TensorToSparseCSFTensor(shared_ptr[CTensor],
                                    shared_ptr[CSparseCSFTensor]* out)

    CStatus ConvertArrayToPandas(const PandasOptions& options,
                                 shared_ptr[CArray] arr,
                                 object py_ref, PyObject** out)

    CStatus ConvertChunkedArrayToPandas(const PandasOptions& options,
                                        shared_ptr[CChunkedArray] arr,
                                        object py_ref, PyObject** out)

    CStatus ConvertTableToPandas(const PandasOptions& options,
                                 shared_ptr[CTable] table,
                                 PyObject** out)

    void c_set_default_memory_pool \
        " arrow::py::set_default_memory_pool"(CMemoryPool* pool)\

    CMemoryPool* c_get_memory_pool \
        " arrow::py::get_memory_pool"()

    cdef cppclass PyBuffer(CBuffer):
        @staticmethod
        CResult[shared_ptr[CBuffer]] FromPyObject(object obj)

    cdef cppclass PyForeignBuffer(CBuffer):
        @staticmethod
        CStatus Make(const uint8_t* data, int64_t size, object base,
                     shared_ptr[CBuffer]* out)

    cdef cppclass PyReadableFile(CRandomAccessFile):
        PyReadableFile(object fo)

    cdef cppclass PyOutputStream(COutputStream):
        PyOutputStream(object fo)

    cdef cppclass PandasOptions:
        CMemoryPool* pool
        c_bool strings_to_categorical
        c_bool zero_copy_only
        c_bool integer_object_nulls
        c_bool date_as_object
        c_bool timestamp_as_object
        c_bool use_threads
        c_bool coerce_temporal_nanoseconds
        c_bool deduplicate_objects
        c_bool safe_cast
        c_bool split_blocks
        c_bool self_destruct
        unordered_set[c_string] categorical_columns
        unordered_set[c_string] extension_columns

    cdef cppclass CSerializedPyObject" arrow::py::SerializedPyObject":
        shared_ptr[CRecordBatch] batch
        vector[shared_ptr[CTensor]] tensors

        CStatus WriteTo(COutputStream* dst)
        CStatus GetComponents(CMemoryPool* pool, PyObject** dst)

    CStatus SerializeObject(object context, object sequence,
                            CSerializedPyObject* out)

    CStatus DeserializeObject(object context,
                              const CSerializedPyObject& obj,
                              PyObject* base, PyObject** out)

    CStatus ReadSerializedObject(CRandomAccessFile* src,
                                 CSerializedPyObject* out)

    cdef cppclass SparseTensorCounts:
        SparseTensorCounts()
        int coo
        int csr
        int csc
        int csf
        int ndim_csf
        int num_total_tensors() const
        int num_total_buffers() const

    CStatus GetSerializedFromComponents(
        int num_tensors,
        const SparseTensorCounts& num_sparse_tensors,
        int num_ndarrays,
        int num_buffers,
        object buffers,
        CSerializedPyObject* out)


cdef extern from "arrow/python/api.h" namespace "arrow::py::internal" nogil:
    cdef cppclass CTimePoint "arrow::py::internal::TimePoint":
        pass

    CTimePoint PyDateTime_to_TimePoint(PyDateTime_DateTime* pydatetime)
    int64_t TimePoint_to_ns(CTimePoint val)
    CTimePoint TimePoint_from_s(double val)
    CTimePoint TimePoint_from_ns(int64_t val)


cdef extern from 'arrow/python/init.h':
    int arrow_init_numpy() except -1


cdef extern from 'arrow/python/pyarrow.h' namespace 'arrow::py':
    int import_pyarrow() except -1


cdef extern from 'arrow/python/common.h' namespace "arrow::py":
    c_bool IsPyError(const CStatus& status)
    void RestorePyError(const CStatus& status)


cdef extern from 'arrow/python/inference.h' namespace 'arrow::py':
    c_bool IsPyBool(object o)
    c_bool IsPyInt(object o)
    c_bool IsPyFloat(object o)


cdef extern from 'arrow/extension_type.h' namespace 'arrow':
    cdef cppclass CExtensionTypeRegistry" arrow::ExtensionTypeRegistry":
        @staticmethod
        shared_ptr[CExtensionTypeRegistry] GetGlobalRegistry()

    cdef cppclass CExtensionType" arrow::ExtensionType"(CDataType):
        c_string extension_name()
        shared_ptr[CDataType] storage_type()

    cdef cppclass CExtensionArray" arrow::ExtensionArray"(CArray):
        CExtensionArray(shared_ptr[CDataType], shared_ptr[CArray] storage)

        shared_ptr[CArray] storage()


cdef extern from 'arrow/python/extension_type.h' namespace 'arrow::py':
    cdef cppclass CPyExtensionType \
            " arrow::py::PyExtensionType"(CExtensionType):
        @staticmethod
        CStatus FromClass(const shared_ptr[CDataType] storage_type,
                          const c_string extension_name, object typ,
                          shared_ptr[CExtensionType]* out)

        @staticmethod
        CStatus FromInstance(shared_ptr[CDataType] storage_type,
                             object inst, shared_ptr[CExtensionType]* out)

        object GetInstance()
        CStatus SetInstance(object)

    c_string PyExtensionName()
    CStatus RegisterPyExtensionType(shared_ptr[CDataType])
    CStatus UnregisterPyExtensionType(c_string type_name)


cdef extern from 'arrow/python/benchmark.h' namespace 'arrow::py::benchmark':
    void Benchmark_PandasObjectIsNull(object lst) except *


cdef extern from 'arrow/util/compression.h' namespace 'arrow' nogil:
    enum CCompressionType" arrow::Compression::type":
        CCompressionType_UNCOMPRESSED" arrow::Compression::UNCOMPRESSED"
        CCompressionType_SNAPPY" arrow::Compression::SNAPPY"
        CCompressionType_GZIP" arrow::Compression::GZIP"
        CCompressionType_BROTLI" arrow::Compression::BROTLI"
        CCompressionType_ZSTD" arrow::Compression::ZSTD"
        CCompressionType_LZ4" arrow::Compression::LZ4"
        CCompressionType_LZ4_FRAME" arrow::Compression::LZ4_FRAME"
        CCompressionType_BZ2" arrow::Compression::BZ2"

    cdef cppclass CCodec" arrow::util::Codec":
        @staticmethod
        CResult[unique_ptr[CCodec]] Create(CCompressionType codec)

        @staticmethod
        c_bool IsAvailable(CCompressionType codec)

        CResult[int64_t] Decompress(int64_t input_len, const uint8_t* input,
                                    int64_t output_len,
                                    uint8_t* output_buffer)
        CResult[int64_t] Compress(int64_t input_len, const uint8_t* input,
                                  int64_t output_buffer_len,
                                  uint8_t* output_buffer)
        const char* name() const
        int64_t MaxCompressedLen(int64_t input_len, const uint8_t* input)


cdef extern from 'arrow/util/io_util.h' namespace 'arrow::internal' nogil:
    int ErrnoFromStatus(CStatus status)
    int WinErrorFromStatus(CStatus status)

cdef extern from 'arrow/util/iterator.h' namespace 'arrow' nogil:
    cdef cppclass CIterator" arrow::Iterator"[T]:
        CResult[T] Next()
        CStatus Visit[Visitor](Visitor&& visitor)
        cppclass RangeIterator:
            CResult[T] operator*()
            RangeIterator& operator++()
            bint operator!=(RangeIterator) const
        RangeIterator begin()
        RangeIterator end()

cdef extern from 'arrow/util/thread_pool.h' namespace 'arrow' nogil:
    int GetCpuThreadPoolCapacity()
    CStatus SetCpuThreadPoolCapacity(int threads)

cdef extern from 'arrow/array/concatenate.h' namespace 'arrow' nogil:
    CResult[shared_ptr[CArray]] Concatenate(
        const vector[shared_ptr[CArray]]& arrays,
        CMemoryPool* pool)

cdef extern from 'arrow/c/abi.h':
    cdef struct ArrowSchema:
        pass

    cdef struct ArrowArray:
        pass

cdef extern from 'arrow/c/bridge.h' namespace 'arrow' nogil:
    CStatus ExportType(CDataType&, ArrowSchema* out)
    CResult[shared_ptr[CDataType]] ImportType(ArrowSchema*)

    CStatus ExportSchema(CSchema&, ArrowSchema* out)
    CResult[shared_ptr[CSchema]] ImportSchema(ArrowSchema*)

    CStatus ExportArray(CArray&, ArrowArray* out)
    CStatus ExportArray(CArray&, ArrowArray* out, ArrowSchema* out_schema)
    CResult[shared_ptr[CArray]] ImportArray(ArrowArray*,
                                            shared_ptr[CDataType])
    CResult[shared_ptr[CArray]] ImportArray(ArrowArray*, ArrowSchema*)

    CStatus ExportRecordBatch(CRecordBatch&, ArrowArray* out)
    CStatus ExportRecordBatch(CRecordBatch&, ArrowArray* out,
                              ArrowSchema* out_schema)
    CResult[shared_ptr[CRecordBatch]] ImportRecordBatch(ArrowArray*,
                                                        shared_ptr[CSchema])
    CResult[shared_ptr[CRecordBatch]] ImportRecordBatch(ArrowArray*,
                                                        ArrowSchema*)
