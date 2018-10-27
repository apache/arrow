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

        c_bool Equals(const CKeyValueMetadata& other)

        void Append(const c_string& key, const c_string& value)
        void ToUnorderedMap(unordered_map[c_string, c_string]*) const


cdef extern from "arrow/api.h" namespace "arrow" nogil:

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
        _Type_BINARY" arrow::Type::BINARY"
        _Type_STRING" arrow::Type::STRING"
        _Type_FIXED_SIZE_BINARY" arrow::Type::FIXED_SIZE_BINARY"

        _Type_LIST" arrow::Type::LIST"
        _Type_STRUCT" arrow::Type::STRUCT"
        _Type_UNION" arrow::Type::UNION"
        _Type_DICTIONARY" arrow::Type::DICTIONARY"
        _Type_MAP" arrow::Type::MAP"

    enum UnionMode" arrow::UnionMode::type":
        _UnionMode_SPARSE" arrow::UnionMode::SPARSE"
        _UnionMode_DENSE" arrow::UnionMode::DENSE"

    enum TimeUnit" arrow::TimeUnit::type":
        TimeUnit_SECOND" arrow::TimeUnit::SECOND"
        TimeUnit_MILLI" arrow::TimeUnit::MILLI"
        TimeUnit_MICRO" arrow::TimeUnit::MICRO"
        TimeUnit_NANO" arrow::TimeUnit::NANO"

    cdef cppclass CDataType" arrow::DataType":
        Type id()

        c_bool Equals(const CDataType& other)

        shared_ptr[CField] child(int i)

        const vector[shared_ptr[CField]] children()

        int num_children()

        c_string ToString()

    c_bool is_primitive(Type type)

    cdef cppclass CArrayData" arrow::ArrayData":
        shared_ptr[CDataType] type
        int64_t length
        int64_t null_count
        int64_t offset
        vector[shared_ptr[CBuffer]] buffers
        vector[shared_ptr[CArrayData]] child_data

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

    cdef cppclass CArray" arrow::Array":
        shared_ptr[CDataType] type()

        int64_t length()
        int64_t null_count()
        int64_t offset()
        Type type_id()

        int num_fields()

        c_bool Equals(const CArray& arr)
        c_bool IsNull(int i)

        shared_ptr[CArrayData] data()

        shared_ptr[CArray] Slice(int64_t offset)
        shared_ptr[CArray] Slice(int64_t offset, int64_t length)

    shared_ptr[CArray] MakeArray(const shared_ptr[CArrayData]& data)

    CStatus DebugPrint(const CArray& arr, int indent)

    cdef cppclass CFixedWidthType" arrow::FixedWidthType"(CDataType):
        int bit_width()

    cdef cppclass CDictionaryArray" arrow::DictionaryArray"(CArray):
        CDictionaryArray(const shared_ptr[CDataType]& type,
                         const shared_ptr[CArray]& indices)

        @staticmethod
        CStatus FromArrays(const shared_ptr[CDataType]& type,
                           const shared_ptr[CArray]& indices,
                           shared_ptr[CArray]* out)

        shared_ptr[CArray] indices()
        shared_ptr[CArray] dictionary()

    cdef cppclass CDate32Type" arrow::Date32Type"(CFixedWidthType):
        pass

    cdef cppclass CDate64Type" arrow::Date64Type"(CFixedWidthType):
        pass

    cdef cppclass CTimestampType" arrow::TimestampType"(CFixedWidthType):
        TimeUnit unit()
        const c_string& timezone()

    cdef cppclass CTime32Type" arrow::Time32Type"(CFixedWidthType):
        TimeUnit unit()

    cdef cppclass CTime64Type" arrow::Time64Type"(CFixedWidthType):
        TimeUnit unit()

    shared_ptr[CDataType] ctime32" arrow::time32"(TimeUnit unit)
    shared_ptr[CDataType] ctime64" arrow::time64"(TimeUnit unit)

    cdef cppclass CDictionaryType" arrow::DictionaryType"(CFixedWidthType):
        CDictionaryType(const shared_ptr[CDataType]& index_type,
                        const shared_ptr[CArray]& dictionary,
                        c_bool ordered)

        shared_ptr[CDataType] index_type()
        shared_ptr[CArray] dictionary()
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
        int64_t size()
        shared_ptr[CBuffer] parent()
        c_bool is_mutable() const
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

    CStatus AllocateBuffer(CMemoryPool* pool, const int64_t size,
                           shared_ptr[CBuffer]* out)

    CStatus AllocateResizableBuffer(CMemoryPool* pool, const int64_t size,
                                    shared_ptr[CResizableBuffer]* out)

    cdef CMemoryPool* c_default_memory_pool" arrow::default_memory_pool"()

    cdef cppclass CListType" arrow::ListType"(CDataType):
        CListType(const shared_ptr[CDataType]& value_type)
        CListType(const shared_ptr[CField]& field)
        shared_ptr[CDataType] value_type()
        shared_ptr[CField] value_field()

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
        const c_string& name()
        shared_ptr[CDataType] type()
        c_bool nullable()

        c_string ToString()
        c_bool Equals(const CField& other)

        shared_ptr[const CKeyValueMetadata] metadata()

        CField(const c_string& name, const shared_ptr[CDataType]& type,
               c_bool nullable)

        CField(const c_string& name, const shared_ptr[CDataType]& type,
               c_bool nullable, const shared_ptr[CKeyValueMetadata]& metadata)

        # Removed const in Cython so don't have to cast to get code to generate
        shared_ptr[CField] AddMetadata(
            const shared_ptr[CKeyValueMetadata]& metadata)
        shared_ptr[CField] RemoveMetadata()
        vector[shared_ptr[CField]] Flatten()

    cdef cppclass CStructType" arrow::StructType"(CDataType):
        CStructType(const vector[shared_ptr[CField]]& fields)

        shared_ptr[CField] GetChildByName(const c_string& name)
        int GetChildIndex(const c_string& name)

    cdef cppclass CUnionType" arrow::UnionType"(CDataType):
        CUnionType(const vector[shared_ptr[CField]]& fields,
                   const vector[uint8_t]& type_codes, UnionMode mode)
        UnionMode mode()

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
        int64_t GetFieldIndex(const c_string& name)
        int num_fields()
        c_string ToString()

        CStatus AddField(int i, const shared_ptr[CField]& field,
                         shared_ptr[CSchema]* out)
        CStatus RemoveField(int i, shared_ptr[CSchema]* out)
        CStatus SetField(int i, const shared_ptr[CField]& field,
                         shared_ptr[CSchema]* out)

        # Removed const in Cython so don't have to cast to get code to generate
        shared_ptr[CSchema] AddMetadata(
            const shared_ptr[CKeyValueMetadata]& metadata)
        shared_ptr[CSchema] RemoveMetadata()

    cdef cppclass PrettyPrintOptions:
        PrettyPrintOptions(int indent_arg)
        PrettyPrintOptions(int indent_arg, int window_arg)
        int indent
        int window

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
        CStatus FromArrays(const CArray& offsets, const CArray& values,
                           CMemoryPool* pool, shared_ptr[CArray]* out)

        const int32_t* raw_value_offsets()
        int32_t value_offset(int i)
        int32_t value_length(int i)
        shared_ptr[CArray] values()
        shared_ptr[CDataType] value_type()

    cdef cppclass CUnionArray" arrow::UnionArray"(CArray):
        @staticmethod
        CStatus MakeSparse(const CArray& type_ids,
                           const vector[shared_ptr[CArray]]& children,
                           shared_ptr[CArray]* out)

        @staticmethod
        CStatus MakeDense(const CArray& type_ids, const CArray& value_offsets,
                          const vector[shared_ptr[CArray]]& children,
                          shared_ptr[CArray]* out)
        uint8_t* raw_type_ids()
        int32_t value_offset(int i)
        shared_ptr[CArray] child(int pos)
        const CArray* UnsafeChild(int pos)
        UnionMode mode()

    cdef cppclass CBinaryArray" arrow::BinaryArray"(CListArray):
        const uint8_t* GetValue(int i, int32_t* length)

    cdef cppclass CStringArray" arrow::StringArray"(CBinaryArray):
        CStringArray(int64_t length, shared_ptr[CBuffer] value_offsets,
                     shared_ptr[CBuffer] data,
                     shared_ptr[CBuffer] null_bitmap,
                     int64_t null_count,
                     int64_t offset)

        c_string GetString(int i)

    cdef cppclass CStructArray" arrow::StructArray"(CArray):
        CStructArray(shared_ptr[CDataType] type, int64_t length,
                     vector[shared_ptr[CArray]] children,
                     shared_ptr[CBuffer] null_bitmap=nullptr,
                     int64_t null_count=0,
                     int64_t offset=0)

        shared_ptr[CArray] field(int pos)
        shared_ptr[CArray] GetFieldByName(const c_string& name) const

        CStatus Flatten(CMemoryPool* pool, vector[shared_ptr[CArray]]* out)

    CStatus ValidateArray(const CArray& array)

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

    cdef cppclass CColumn" arrow::Column":
        CColumn(const shared_ptr[CField]& field,
                const shared_ptr[CArray]& data)

        CColumn(const shared_ptr[CField]& field,
                const vector[shared_ptr[CArray]]& chunks)

        CColumn(const shared_ptr[CField]& field,
                const shared_ptr[CChunkedArray]& data)

        c_bool Equals(const CColumn& other)

        CStatus Flatten(CMemoryPool* pool, vector[shared_ptr[CColumn]]* out)

        shared_ptr[CField] field()

        int64_t length()
        int64_t null_count()
        const c_string& name()
        shared_ptr[CDataType] type()
        shared_ptr[CChunkedArray] data()

    cdef cppclass CRecordBatch" arrow::RecordBatch":
        @staticmethod
        shared_ptr[CRecordBatch] Make(
            const shared_ptr[CSchema]& schema, int64_t num_rows,
            const vector[shared_ptr[CArray]]& columns)

        c_bool Equals(const CRecordBatch& other)

        shared_ptr[CSchema] schema()
        shared_ptr[CArray] column(int i)
        const c_string& column_name(int i)

        const vector[shared_ptr[CArray]]& columns()

        int num_columns()
        int64_t num_rows()

        shared_ptr[CRecordBatch] ReplaceSchemaMetadata(
            const shared_ptr[CKeyValueMetadata]& metadata)

        shared_ptr[CRecordBatch] Slice(int64_t offset)
        shared_ptr[CRecordBatch] Slice(int64_t offset, int64_t length)

    cdef cppclass CTable" arrow::Table":
        CTable(const shared_ptr[CSchema]& schema,
               const vector[shared_ptr[CColumn]]& columns)

        @staticmethod
        shared_ptr[CTable] Make(
            const shared_ptr[CSchema]& schema,
            const vector[shared_ptr[CColumn]]& columns)

        @staticmethod
        CStatus FromRecordBatches(
            const shared_ptr[CSchema]& schema,
            const vector[shared_ptr[CRecordBatch]]& batches,
            shared_ptr[CTable]* table)

        int num_columns()
        int64_t num_rows()

        c_bool Equals(const CTable& other)

        shared_ptr[CSchema] schema()
        shared_ptr[CColumn] column(int i)

        CStatus AddColumn(int i, const shared_ptr[CColumn]& column,
                          shared_ptr[CTable]* out)
        CStatus RemoveColumn(int i, shared_ptr[CTable]* out)
        CStatus SetColumn(int i, const shared_ptr[CColumn]& column,
                          shared_ptr[CTable]* out)

        CStatus Flatten(CMemoryPool* pool, shared_ptr[CTable]* out)

        CStatus Validate()

        shared_ptr[CTable] ReplaceSchemaMetadata(
            const shared_ptr[CKeyValueMetadata]& metadata)

    cdef cppclass RecordBatchReader:
        CStatus ReadNext(shared_ptr[CRecordBatch]* out)

    cdef cppclass TableBatchReader(RecordBatchReader):
        TableBatchReader(const CTable& table)
        void set_chunksize(int64_t chunksize)

    cdef cppclass CTensor" arrow::Tensor":
        shared_ptr[CDataType] type()
        shared_ptr[CBuffer] data()

        const vector[int64_t]& shape()
        const vector[int64_t]& strides()
        int64_t size()

        int ndim()
        const c_string& dim_name(int i)

        c_bool is_mutable()
        c_bool is_contiguous()
        Type type_id()
        c_bool Equals(const CTensor& other)

    CStatus ConcatenateTables(const vector[shared_ptr[CTable]]& tables,
                              shared_ptr[CTable]* result)

    cdef extern from "arrow/builder.h" namespace "arrow" nogil:

        cdef cppclass CArrayBuilder" arrow::ArrayBuilder":
            CArrayBuilder(shared_ptr[CDataType], CMemoryPool* pool)

        cdef cppclass CBinaryBuilder" arrow::BinaryBuilder"(CArrayBuilder):
            CArrayBuilder(shared_ptr[CDataType], CMemoryPool* pool)

        cdef cppclass CStringBuilder" arrow::StringBuilder"(CBinaryBuilder):
            CStringBuilder(CMemoryPool* pool)

            int64_t length()
            int64_t null_count()

            CStatus Append(const c_string& value)
            CStatus AppendNull()
            CStatus Finish(shared_ptr[CArray]* out)


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
        CStatus Tell(int64_t* position)
        FileMode mode()

    cdef cppclass Readable:
        # put overload under a different name to avoid cython bug with multiple
        # layers of inheritance
        CStatus ReadB" Read"(int64_t nbytes, shared_ptr[CBuffer]* out)
        CStatus Read(int64_t nbytes, int64_t* bytes_read, uint8_t* out)

    cdef cppclass Seekable:
        CStatus Seek(int64_t position)

    cdef cppclass Writable:
        CStatus Write(const uint8_t* data, int64_t nbytes)

    cdef cppclass OutputStream(FileInterface, Writable):
        pass

    cdef cppclass InputStream(FileInterface, Readable):
        pass

    cdef cppclass RandomAccessFile(InputStream, Seekable):
        CStatus GetSize(int64_t* size)

        CStatus ReadAt(int64_t position, int64_t nbytes,
                       int64_t* bytes_read, uint8_t* buffer)
        CStatus ReadAt(int64_t position, int64_t nbytes,
                       shared_ptr[CBuffer]* out)
        c_bool supports_zero_copy()

    cdef cppclass WritableFile(OutputStream, Seekable):
        CStatus WriteAt(int64_t position, const uint8_t* data,
                        int64_t nbytes)

    cdef cppclass ReadWriteFileInterface(RandomAccessFile,
                                         WritableFile):
        pass

    cdef cppclass FileSystem:
        CStatus Stat(const c_string& path, FileStatistics* stat)

    cdef cppclass FileOutputStream(OutputStream):
        @staticmethod
        CStatus Open(const c_string& path, shared_ptr[OutputStream]* file)

        int file_descriptor()

    cdef cppclass ReadableFile(RandomAccessFile):
        @staticmethod
        CStatus Open(const c_string& path, shared_ptr[ReadableFile]* file)

        @staticmethod
        CStatus Open(const c_string& path, CMemoryPool* memory_pool,
                     shared_ptr[ReadableFile]* file)

        int file_descriptor()

    cdef cppclass CMemoryMappedFile \
            " arrow::io::MemoryMappedFile"(ReadWriteFileInterface):

        @staticmethod
        CStatus Create(const c_string& path, int64_t size,
                       shared_ptr[CMemoryMappedFile]* file)

        @staticmethod
        CStatus Open(const c_string& path, FileMode mode,
                     shared_ptr[CMemoryMappedFile]* file)

        CStatus Resize(int64_t size)

        int file_descriptor()

    cdef cppclass CompressedInputStream(InputStream):
        @staticmethod
        CStatus Make(CMemoryPool* pool, CCodec* codec,
                     shared_ptr[InputStream] raw,
                     shared_ptr[CompressedInputStream]* out)

        @staticmethod
        CStatus Make(CCodec* codec, shared_ptr[InputStream] raw,
                     shared_ptr[CompressedInputStream]* out)

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

    cdef cppclass HdfsReadableFile(RandomAccessFile):
        pass

    cdef cppclass HdfsOutputStream(OutputStream):
        pass

    cdef cppclass CHadoopFileSystem" arrow::io::HadoopFileSystem"(FileSystem):
        @staticmethod
        CStatus Connect(const HdfsConnectionConfig* config,
                        shared_ptr[CHadoopFileSystem]* client)

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
            " arrow::io::BufferReader"(RandomAccessFile):
        CBufferReader(const shared_ptr[CBuffer]& buffer)
        CBufferReader(const uint8_t* data, int64_t nbytes)

    cdef cppclass CBufferOutputStream \
            " arrow::io::BufferOutputStream"(OutputStream):
        CBufferOutputStream(const shared_ptr[CResizableBuffer]& buffer)

    cdef cppclass CMockOutputStream \
            " arrow::io::MockOutputStream"(OutputStream):
        CMockOutputStream()
        int64_t GetExtentBytesWritten()

    cdef cppclass CFixedSizeBufferWriter \
            " arrow::io::FixedSizeBufferWriter"(WritableFile):
        CFixedSizeBufferWriter(const shared_ptr[CBuffer]& buffer)

        void set_memcopy_threads(int num_threads)
        void set_memcopy_blocksize(int64_t blocksize)
        void set_memcopy_threshold(int64_t threshold)


cdef extern from "arrow/ipc/api.h" namespace "arrow::ipc" nogil:
    enum MessageType" arrow::ipc::Message::Type":
        MessageType_SCHEMA" arrow::ipc::Message::SCHEMA"
        MessageType_RECORD_BATCH" arrow::ipc::Message::RECORD_BATCH"
        MessageType_DICTIONARY_BATCH" arrow::ipc::Message::DICTIONARY_BATCH"

    enum MetadataVersion" arrow::ipc::MetadataVersion":
        MessageType_V1" arrow::ipc::MetadataVersion::V1"
        MessageType_V2" arrow::ipc::MetadataVersion::V2"
        MessageType_V3" arrow::ipc::MetadataVersion::V3"
        MessageType_V4" arrow::ipc::MetadataVersion::V4"

    cdef cppclass CMessage" arrow::ipc::Message":
        CStatus Open(const shared_ptr[CBuffer]& metadata,
                     const shared_ptr[CBuffer]& body,
                     unique_ptr[CMessage]* out)

        shared_ptr[CBuffer] body()

        c_bool Equals(const CMessage& other)

        shared_ptr[CBuffer] metadata()
        MetadataVersion metadata_version()
        MessageType type()

        CStatus SerializeTo(OutputStream* stream, int32_t alignment,
                            int64_t* output_length)

    c_string FormatMessageType(MessageType type)

    cdef cppclass CMessageReader" arrow::ipc::MessageReader":
        @staticmethod
        unique_ptr[CMessageReader] Open(const shared_ptr[InputStream]& stream)

        CStatus ReadNextMessage(unique_ptr[CMessage]* out)

    cdef cppclass CRecordBatchWriter" arrow::ipc::RecordBatchWriter":
        CStatus Close()
        CStatus WriteRecordBatch(const CRecordBatch& batch,
                                 c_bool allow_64bit)
        CStatus WriteTable(const CTable& table, int64_t max_chunksize)

    cdef cppclass CRecordBatchReader" arrow::ipc::RecordBatchReader":
        shared_ptr[CSchema] schema()
        CStatus ReadNext(shared_ptr[CRecordBatch]* batch)

    cdef cppclass CRecordBatchStreamReader \
            " arrow::ipc::RecordBatchStreamReader"(CRecordBatchReader):
        @staticmethod
        CStatus Open(const InputStream* stream,
                     shared_ptr[CRecordBatchReader]* out)

        @staticmethod
        CStatus Open2" Open"(unique_ptr[CMessageReader] message_reader,
                             shared_ptr[CRecordBatchStreamReader]* out)

    cdef cppclass CRecordBatchStreamWriter \
            " arrow::ipc::RecordBatchStreamWriter"(CRecordBatchWriter):
        @staticmethod
        CStatus Open(OutputStream* sink, const shared_ptr[CSchema]& schema,
                     shared_ptr[CRecordBatchWriter]* out)

    cdef cppclass CRecordBatchFileWriter \
            " arrow::ipc::RecordBatchFileWriter"(CRecordBatchWriter):
        @staticmethod
        CStatus Open(OutputStream* sink, const shared_ptr[CSchema]& schema,
                     shared_ptr[CRecordBatchWriter]* out)

    cdef cppclass CRecordBatchFileReader \
            " arrow::ipc::RecordBatchFileReader":
        @staticmethod
        CStatus Open(RandomAccessFile* file,
                     shared_ptr[CRecordBatchFileReader]* out)

        @staticmethod
        CStatus Open2" Open"(RandomAccessFile* file,
                             int64_t footer_offset,
                             shared_ptr[CRecordBatchFileReader]* out)

        shared_ptr[CSchema] schema()

        int num_record_batches()

        CStatus ReadRecordBatch(int i, shared_ptr[CRecordBatch]* batch)

    CStatus ReadMessage(InputStream* stream, unique_ptr[CMessage]* message)

    CStatus GetRecordBatchSize(const CRecordBatch& batch, int64_t* size)
    CStatus GetTensorSize(const CTensor& tensor, int64_t* size)

    CStatus WriteTensor(const CTensor& tensor, OutputStream* dst,
                        int32_t* metadata_length,
                        int64_t* body_length)

    CStatus ReadTensor(InputStream* stream, shared_ptr[CTensor]* out)

    CStatus ReadRecordBatch(const CMessage& message,
                            const shared_ptr[CSchema]& schema,
                            shared_ptr[CRecordBatch]* out)

    CStatus SerializeSchema(const CSchema& schema, CMemoryPool* pool,
                            shared_ptr[CBuffer]* out)

    CStatus SerializeRecordBatch(const CRecordBatch& schema,
                                 CMemoryPool* pool,
                                 shared_ptr[CBuffer]* out)

    CStatus ReadSchema(InputStream* stream, shared_ptr[CSchema]* out)

    CStatus ReadRecordBatch(const shared_ptr[CSchema]& schema,
                            InputStream* stream,
                            shared_ptr[CRecordBatch]* out)

    CStatus AlignStream(InputStream* stream, int64_t alignment)
    CStatus AlignStream(OutputStream* stream, int64_t alignment)

    cdef cppclass CFeatherWriter" arrow::ipc::feather::TableWriter":
        @staticmethod
        CStatus Open(const shared_ptr[OutputStream]& stream,
                     unique_ptr[CFeatherWriter]* out)

        void SetDescription(const c_string& desc)
        void SetNumRows(int64_t num_rows)

        CStatus Append(const c_string& name, const CArray& values)
        CStatus Finalize()

    cdef cppclass CFeatherReader" arrow::ipc::feather::TableReader":
        @staticmethod
        CStatus Open(const shared_ptr[RandomAccessFile]& file,
                     unique_ptr[CFeatherReader]* out)

        c_string GetDescription()
        c_bool HasDescription()

        int64_t num_rows()
        int64_t num_columns()

        shared_ptr[CSchema] schema()

        CStatus GetColumn(int i, shared_ptr[CColumn]* out)
        c_string GetColumnName(int i)

        CStatus Read(shared_ptr[CTable]* out)
        CStatus Read(const vector[int] indices, shared_ptr[CTable]* out)
        CStatus Read(const vector[c_string] names, shared_ptr[CTable]* out)


cdef extern from "arrow/csv/api.h" namespace "arrow::csv" nogil:

    cdef cppclass CCSVParseOptions" arrow::csv::ParseOptions":
        unsigned char delimiter
        c_bool quoting
        unsigned char quote_char
        c_bool double_quote
        c_bool escaping
        unsigned char escape_char
        int32_t header_rows
        c_bool newlines_in_values

        @staticmethod
        CCSVParseOptions Defaults()

    cdef cppclass CCSVConvertOptions" arrow::csv::ConvertOptions":
        @staticmethod
        CCSVConvertOptions Defaults()

    cdef cppclass CCSVReadOptions" arrow::csv::ReadOptions":
        c_bool use_threads
        int32_t block_size

        @staticmethod
        CCSVReadOptions Defaults()

    cdef cppclass CCSVReader" arrow::csv::TableReader":
        @staticmethod
        CStatus Make(CMemoryPool*, shared_ptr[InputStream],
                     CCSVReadOptions, CCSVParseOptions, CCSVConvertOptions,
                     shared_ptr[CCSVReader]* out)

        CStatus Read(shared_ptr[CTable]* out)


cdef extern from "arrow/compute/api.h" namespace "arrow::compute" nogil:

    cdef cppclass CFunctionContext" arrow::compute::FunctionContext":
        CFunctionContext()
        CFunctionContext(CMemoryPool* pool)

    cdef cppclass CCastOptions" arrow::compute::CastOptions":
        CCastOptions()
        CCastOptions(c_bool safe)
        CCastOptions Safe()
        CCastOptions Unsafe()
        c_bool allow_int_overflow
        c_bool allow_time_truncate
        c_bool allow_float_truncate

    enum DatumType" arrow::compute::Datum::type":
        DatumType_NONE" arrow::compute::Datum::NONE"
        DatumType_SCALAR" arrow::compute::Datum::SCALAR"
        DatumType_ARRAY" arrow::compute::Datum::ARRAY"
        DatumType_CHUNKED_ARRAY" arrow::compute::Datum::CHUNKED_ARRAY"
        DatumType_RECORD_BATCH" arrow::compute::Datum::RECORD_BATCH"
        DatumType_TABLE" arrow::compute::Datum::TABLE"
        DatumType_COLLECTION" arrow::compute::Datum::COLLECTION"

    cdef cppclass CDatum" arrow::compute::Datum":
        CDatum()
        CDatum(const shared_ptr[CArray]& value)
        CDatum(const shared_ptr[CChunkedArray]& value)
        CDatum(const shared_ptr[CRecordBatch]& value)
        CDatum(const shared_ptr[CTable]& value)

        DatumType kind()

        shared_ptr[CArrayData] array()
        shared_ptr[CChunkedArray] chunked_array()

    CStatus Cast(CFunctionContext* context, const CArray& array,
                 const shared_ptr[CDataType]& to_type,
                 const CCastOptions& options,
                 shared_ptr[CArray]* out)

    CStatus Cast(CFunctionContext* context, const CDatum& value,
                 const shared_ptr[CDataType]& to_type,
                 const CCastOptions& options, CDatum* out)

    CStatus Unique(CFunctionContext* context, const CDatum& value,
                   shared_ptr[CArray]* out)

    CStatus DictionaryEncode(CFunctionContext* context, const CDatum& value,
                             CDatum* out)


cdef extern from "arrow/python/api.h" namespace "arrow::py" nogil:
    shared_ptr[CDataType] GetPrimitiveType(Type type)
    shared_ptr[CDataType] GetTimestampType(TimeUnit unit)

    object PyHalf_FromHalf(npy_half value)

    cdef cppclass PyConversionOptions:
        PyConversionOptions()

        shared_ptr[CDataType] type
        int64_t size
        CMemoryPool* pool
        c_bool from_pandas

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
                            shared_ptr[CTensor]* out)

    CStatus TensorToNdarray(const shared_ptr[CTensor]& tensor, object base,
                            PyObject** out)

    CStatus ConvertArrayToPandas(PandasOptions options,
                                 const shared_ptr[CArray]& arr,
                                 object py_ref, PyObject** out)

    CStatus ConvertChunkedArrayToPandas(PandasOptions options,
                                        const shared_ptr[CChunkedArray]& arr,
                                        object py_ref, PyObject** out)

    CStatus ConvertColumnToPandas(PandasOptions options,
                                  const shared_ptr[CColumn]& arr,
                                  object py_ref, PyObject** out)

    CStatus ConvertTableToPandas(
        PandasOptions options,
        const unordered_set[c_string]& categorical_columns,
        const shared_ptr[CTable]& table,
        CMemoryPool* pool,
        PyObject** out)

    void c_set_default_memory_pool \
        " arrow::py::set_default_memory_pool"(CMemoryPool* pool)\

    CMemoryPool* c_get_memory_pool \
        " arrow::py::get_memory_pool"()

    cdef cppclass PyBuffer(CBuffer):
        @staticmethod
        CStatus FromPyObject(object obj, shared_ptr[CBuffer]* out)

    cdef cppclass PyForeignBuffer(CBuffer):
        @staticmethod
        CStatus Make(const uint8_t* data, int64_t size, object base,
                     shared_ptr[CBuffer]* out)

    cdef cppclass PyReadableFile(RandomAccessFile):
        PyReadableFile(object fo)

    cdef cppclass PyOutputStream(OutputStream):
        PyOutputStream(object fo)

    cdef struct PandasOptions:
        c_bool strings_to_categorical
        c_bool zero_copy_only
        c_bool integer_object_nulls
        c_bool date_as_object
        c_bool use_threads

cdef extern from "arrow/python/api.h" namespace 'arrow::py' nogil:

    cdef cppclass CSerializedPyObject" arrow::py::SerializedPyObject":
        shared_ptr[CRecordBatch] batch
        vector[shared_ptr[CTensor]] tensors

        CStatus WriteTo(OutputStream* dst)
        CStatus GetComponents(CMemoryPool* pool, PyObject** dst)

    CStatus SerializeObject(object context, object sequence,
                            CSerializedPyObject* out)

    CStatus DeserializeObject(object context,
                              const CSerializedPyObject& obj,
                              PyObject* base, PyObject** out)

    CStatus ReadSerializedObject(RandomAccessFile* src,
                                 CSerializedPyObject* out)

    CStatus GetSerializedFromComponents(int num_tensors, int num_buffers,
                                        object buffers,
                                        CSerializedPyObject* out)


cdef extern from 'arrow/python/init.h':
    int arrow_init_numpy() except -1


cdef extern from 'arrow/python/config.h' namespace 'arrow::py':
    void set_numpy_nan(object o)


cdef extern from 'arrow/python/inference.h' namespace 'arrow::py':
    c_bool IsPyBool(object o)
    c_bool IsPyInt(object o)
    c_bool IsPyFloat(object o)


cdef extern from 'arrow/python/benchmark.h' namespace 'arrow::py::benchmark':
    void Benchmark_PandasObjectIsNull(object lst) except *


cdef extern from 'arrow/util/compression.h' namespace 'arrow' nogil:
    enum CompressionType" arrow::Compression::type":
        CompressionType_UNCOMPRESSED" arrow::Compression::UNCOMPRESSED"
        CompressionType_SNAPPY" arrow::Compression::SNAPPY"
        CompressionType_GZIP" arrow::Compression::GZIP"
        CompressionType_BROTLI" arrow::Compression::BROTLI"
        CompressionType_ZSTD" arrow::Compression::ZSTD"
        CompressionType_LZ4" arrow::Compression::LZ4"
        CompressionType_BZ2" arrow::Compression::BZ2"

    cdef cppclass CCodec" arrow::util::Codec":
        @staticmethod
        CStatus Create(CompressionType codec, unique_ptr[CCodec]* out)

        CStatus Decompress(int64_t input_len, const uint8_t* input,
                           int64_t output_len, uint8_t* output_buffer)

        CStatus Compress(int64_t input_len, const uint8_t* input,
                         int64_t output_buffer_len, uint8_t* output_buffer,
                         int64_t* output_length)

        int64_t MaxCompressedLen(int64_t input_len, const uint8_t* input)


cdef extern from 'arrow/util/thread-pool.h' namespace 'arrow' nogil:
    int GetCpuThreadPoolCapacity()
    CStatus SetCpuThreadPoolCapacity(int threads)
