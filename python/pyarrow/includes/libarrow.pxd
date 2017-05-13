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
        _Type_DICTIONARY" arrow::Type::DICTIONARY"

    enum TimeUnit" arrow::TimeUnit::type":
        TimeUnit_SECOND" arrow::TimeUnit::SECOND"
        TimeUnit_MILLI" arrow::TimeUnit::MILLI"
        TimeUnit_MICRO" arrow::TimeUnit::MICRO"
        TimeUnit_NANO" arrow::TimeUnit::NANO"

    cdef cppclass CDataType" arrow::DataType":
        Type id()

        c_bool Equals(const CDataType& other)

        c_string ToString()

    cdef cppclass CArray" arrow::Array":
        shared_ptr[CDataType] type()

        int64_t length()
        int64_t null_count()
        Type type_id()

        c_bool Equals(const CArray& arr)
        c_bool IsNull(int i)

        shared_ptr[CArray] Slice(int64_t offset)
        shared_ptr[CArray] Slice(int64_t offset, int64_t length)

    cdef cppclass CFixedWidthType" arrow::FixedWidthType"(CDataType):
        int bit_width()

    cdef cppclass CDictionaryArray" arrow::DictionaryArray"(CArray):
        CDictionaryArray(const shared_ptr[CDataType]& type,
                         const shared_ptr[CArray]& indices)

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
                        const shared_ptr[CArray]& dictionary)

        shared_ptr[CDataType] index_type()
        shared_ptr[CArray] dictionary()

    shared_ptr[CDataType] ctimestamp" arrow::timestamp"(TimeUnit unit)
    shared_ptr[CDataType] ctimestamp" arrow::timestamp"(
        TimeUnit unit, const c_string& timezone)

    cdef cppclass CMemoryPool" arrow::MemoryPool":
        int64_t bytes_allocated()

    cdef cppclass CLoggingMemoryPool" arrow::LoggingMemoryPool"(CMemoryPool):
        CLoggingMemoryPool(CMemoryPool*)

    cdef cppclass CBuffer" arrow::Buffer":
        uint8_t* data()
        int64_t size()
        shared_ptr[CBuffer] parent()

    cdef cppclass ResizableBuffer(CBuffer):
        CStatus Resize(int64_t nbytes)
        CStatus Reserve(int64_t nbytes)

    cdef cppclass PoolBuffer(ResizableBuffer):
        PoolBuffer()
        PoolBuffer(CMemoryPool*)

    cdef CMemoryPool* c_default_memory_pool" arrow::default_memory_pool"()

    cdef cppclass CListType" arrow::ListType"(CDataType):
        CListType(const shared_ptr[CDataType]& value_type)

    cdef cppclass CStringType" arrow::StringType"(CDataType):
        pass

    cdef cppclass CFixedSizeBinaryType" arrow::FixedSizeBinaryType"(CFixedWidthType):
        CFixedSizeBinaryType(int byte_width)
        int byte_width()
        int bit_width()

    cdef cppclass CDecimalType" arrow::DecimalType"(CFixedSizeBinaryType):
        int precision()
        int scale()
        CDecimalType(int precision, int scale)

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
        CStatus AddMetadata(const shared_ptr[CKeyValueMetadata]& metadata,
                            shared_ptr[CField]* out)
        shared_ptr[CField] RemoveMetadata()


    cdef cppclass CStructType" arrow::StructType"(CDataType):
        CStructType(const vector[shared_ptr[CField]]& fields)

    cdef cppclass CSchema" arrow::Schema":
        CSchema(const vector[shared_ptr[CField]]& fields)
        CSchema(const vector[shared_ptr[CField]]& fields,
                const shared_ptr[const CKeyValueMetadata]& metadata)

        # Does not actually exist, but gets Cython to not complain
        CSchema(const vector[shared_ptr[CField]]& fields,
                const shared_ptr[CKeyValueMetadata]& metadata)

        c_bool Equals(const CSchema& other)

        shared_ptr[CField] field(int i)
        shared_ptr[const CKeyValueMetadata] metadata()
        shared_ptr[CField] GetFieldByName(c_string& name)
        int num_fields()
        c_string ToString()

        # Removed const in Cython so don't have to cast to get code to generate
        CStatus AddMetadata(const shared_ptr[CKeyValueMetadata]& metadata,
                            shared_ptr[CSchema]* out)
        shared_ptr[CSchema] RemoveMetadata()

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

    cdef cppclass CTimestampArray" arrow::TimestampArray"(CArray):
        int64_t Value(int i)

    cdef cppclass CFloatArray" arrow::FloatArray"(CArray):
        float Value(int i)

    cdef cppclass CDoubleArray" arrow::DoubleArray"(CArray):
        double Value(int i)

    cdef cppclass CFixedSizeBinaryArray" arrow::FixedSizeBinaryArray"(CArray):
        const uint8_t* GetValue(int i)

    cdef cppclass CDecimalArray" arrow::DecimalArray"(CFixedSizeBinaryArray):
        c_string FormatValue(int i)

    cdef cppclass CListArray" arrow::ListArray"(CArray):
        const int32_t* raw_value_offsets()
        int32_t value_offset(int i)
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

        c_bool Equals(const CColumn& other)

        int64_t length()
        int64_t null_count()
        const c_string& name()
        shared_ptr[CDataType] type()
        shared_ptr[CChunkedArray] data()

    cdef cppclass CRecordBatch" arrow::RecordBatch":
        CRecordBatch(const shared_ptr[CSchema]& schema, int64_t num_rows,
                     const vector[shared_ptr[CArray]]& columns)

        c_bool Equals(const CRecordBatch& other)

        shared_ptr[CSchema] schema()
        shared_ptr[CArray] column(int i)
        const c_string& column_name(int i)

        const vector[shared_ptr[CArray]]& columns()

        int num_columns()
        int64_t num_rows()

        shared_ptr[CRecordBatch] Slice(int64_t offset)
        shared_ptr[CRecordBatch] Slice(int64_t offset, int64_t length)

    cdef cppclass CTable" arrow::Table":
        CTable(const shared_ptr[CSchema]& schema,
               const vector[shared_ptr[CColumn]]& columns)

        @staticmethod
        CStatus FromRecordBatches(
            const vector[shared_ptr[CRecordBatch]]& batches,
            shared_ptr[CTable]* table)

        int num_columns()
        int num_rows()

        c_bool Equals(const CTable& other)

        shared_ptr[CSchema] schema()
        shared_ptr[CColumn] column(int i)

        CStatus AddColumn(int i, const shared_ptr[CColumn]& column,
                          shared_ptr[CTable]* out)
        CStatus RemoveColumn(int i, shared_ptr[CTable]* out)

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


cdef extern from "arrow/io/interfaces.h" namespace "arrow::io" nogil:
    enum FileMode" arrow::io::FileMode::type":
        FileMode_READ" arrow::io::FileMode::READ"
        FileMode_WRITE" arrow::io::FileMode::WRITE"
        FileMode_READWRITE" arrow::io::FileMode::READWRITE"

    enum ObjectType" arrow::io::ObjectType::type":
        ObjectType_FILE" arrow::io::ObjectType::FILE"
        ObjectType_DIRECTORY" arrow::io::ObjectType::DIRECTORY"

    cdef cppclass FileInterface:
        CStatus Close()
        CStatus Tell(int64_t* position)
        FileMode mode()

    cdef cppclass Readable:
        CStatus ReadB" Read"(int64_t nbytes, shared_ptr[CBuffer]* out)
        CStatus Read(int64_t nbytes, int64_t* bytes_read, uint8_t* out)

    cdef cppclass Seekable:
        CStatus Seek(int64_t position)

    cdef cppclass Writeable:
        CStatus Write(const uint8_t* data, int64_t nbytes)

    cdef cppclass OutputStream(FileInterface, Writeable):
        pass

    cdef cppclass InputStream(FileInterface, Readable):
        pass

    cdef cppclass RandomAccessFile(InputStream, Seekable):
        CStatus GetSize(int64_t* size)

        CStatus ReadAt(int64_t position, int64_t nbytes,
                       int64_t* bytes_read, uint8_t* buffer)
        CStatus ReadAt(int64_t position, int64_t nbytes,
                       int64_t* bytes_read, shared_ptr[CBuffer]* out)

    cdef cppclass WriteableFile(OutputStream, Seekable):
        CStatus WriteAt(int64_t position, const uint8_t* data,
                        int64_t nbytes)

    cdef cppclass ReadWriteFileInterface(RandomAccessFile,
                                         WriteableFile):
        pass


cdef extern from "arrow/io/file.h" namespace "arrow::io" nogil:


    cdef cppclass FileOutputStream(OutputStream):
        @staticmethod
        CStatus Open(const c_string& path, shared_ptr[FileOutputStream]* file)

        int file_descriptor()

    cdef cppclass ReadableFile(RandomAccessFile):
        @staticmethod
        CStatus Open(const c_string& path, shared_ptr[ReadableFile]* file)

        @staticmethod
        CStatus Open(const c_string& path, CMemoryPool* memory_pool,
                     shared_ptr[ReadableFile]* file)

        int file_descriptor()

    cdef cppclass CMemoryMappedFile" arrow::io::MemoryMappedFile"\
        (ReadWriteFileInterface):

        @staticmethod
        CStatus Create(const c_string& path, int64_t size,
                     shared_ptr[CMemoryMappedFile]* file)

        @staticmethod
        CStatus Open(const c_string& path, FileMode mode,
                     shared_ptr[CMemoryMappedFile]* file)

        int file_descriptor()


cdef extern from "arrow/io/hdfs.h" namespace "arrow::io" nogil:
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
        HdfsDriver driver

    cdef cppclass HdfsPathInfo:
        ObjectType kind;
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

    cdef cppclass CHdfsClient" arrow::io::HdfsClient":
        @staticmethod
        CStatus Connect(const HdfsConnectionConfig* config,
                        shared_ptr[CHdfsClient]* client)

        CStatus MakeDirectory(const c_string& path)

        CStatus Delete(const c_string& path, c_bool recursive)

        CStatus Disconnect()

        c_bool Exists(const c_string& path)

        CStatus GetCapacity(int64_t* nbytes)
        CStatus GetUsed(int64_t* nbytes)

        CStatus ListDirectory(const c_string& path,
                              vector[HdfsPathInfo]* listing)

        CStatus GetPathInfo(const c_string& path, HdfsPathInfo* info)

        CStatus Rename(const c_string& src, const c_string& dst)

        CStatus OpenReadable(const c_string& path,
                             shared_ptr[HdfsReadableFile]* handle)

        CStatus OpenWriteable(const c_string& path, c_bool append,
                              int32_t buffer_size, int16_t replication,
                              int64_t default_block_size,
                              shared_ptr[HdfsOutputStream]* handle)


cdef extern from "arrow/io/memory.h" namespace "arrow::io" nogil:
    cdef cppclass CBufferReader" arrow::io::BufferReader"\
        (RandomAccessFile):
        CBufferReader(const shared_ptr[CBuffer]& buffer)
        CBufferReader(const uint8_t* data, int64_t nbytes)

    cdef cppclass BufferOutputStream(OutputStream):
        BufferOutputStream(const shared_ptr[ResizableBuffer]& buffer)


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


cdef extern from "arrow/ipc/api.h" namespace "arrow::ipc" nogil:

    cdef cppclass CStreamWriter " arrow::ipc::StreamWriter":
        @staticmethod
        CStatus Open(OutputStream* sink, const shared_ptr[CSchema]& schema,
                     shared_ptr[CStreamWriter]* out)

        CStatus Close()
        CStatus WriteRecordBatch(const CRecordBatch& batch)

    cdef cppclass CStreamReader " arrow::ipc::StreamReader":

        @staticmethod
        CStatus Open(const shared_ptr[InputStream]& stream,
                     shared_ptr[CStreamReader]* out)

        shared_ptr[CSchema] schema()

        CStatus GetNextRecordBatch(shared_ptr[CRecordBatch]* batch)

    cdef cppclass CFileWriter " arrow::ipc::FileWriter"(CStreamWriter):
        @staticmethod
        CStatus Open(OutputStream* sink, const shared_ptr[CSchema]& schema,
                     shared_ptr[CFileWriter]* out)

    cdef cppclass CFileReader " arrow::ipc::FileReader":

        @staticmethod
        CStatus Open(const shared_ptr[RandomAccessFile]& file,
                     shared_ptr[CFileReader]* out)

        @staticmethod
        CStatus Open2" Open"(const shared_ptr[RandomAccessFile]& file,
                     int64_t footer_offset, shared_ptr[CFileReader]* out)

        shared_ptr[CSchema] schema()

        int num_record_batches()

        CStatus GetRecordBatch(int i, shared_ptr[CRecordBatch]* batch)

    CStatus GetRecordBatchSize(const CRecordBatch& batch, int64_t* size)
    CStatus GetTensorSize(const CTensor& tensor, int64_t* size)

    CStatus WriteTensor(const CTensor& tensor, OutputStream* dst,
                        int32_t* metadata_length,
                        int64_t* body_length)

    CStatus ReadTensor(int64_t offset, RandomAccessFile* file,
                       shared_ptr[CTensor]* out)


cdef extern from "arrow/ipc/feather.h" namespace "arrow::ipc::feather" nogil:

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


cdef extern from "arrow/python/api.h" namespace "arrow::py" nogil:
    shared_ptr[CDataType] GetPrimitiveType(Type type)
    shared_ptr[CDataType] GetTimestampType(TimeUnit unit)
    CStatus ConvertPySequence(object obj, CMemoryPool* pool,
                              shared_ptr[CArray]* out)
    CStatus ConvertPySequence(object obj, CMemoryPool* pool,
                              shared_ptr[CArray]* out,
                              const shared_ptr[CDataType]& type)

    CStatus NumPyDtypeToArrow(object dtype, shared_ptr[CDataType]* type)

    CStatus PandasToArrow(CMemoryPool* pool, object ao, object mo,
                          const shared_ptr[CDataType]& type,
                          shared_ptr[CArray]* out)

    CStatus PandasObjectsToArrow(CMemoryPool* pool, object ao, object mo,
                                 const shared_ptr[CDataType]& type,
                                 shared_ptr[CArray]* out)

    CStatus NdarrayToTensor(CMemoryPool* pool, object ao,
                            shared_ptr[CTensor]* out);

    CStatus TensorToNdarray(const CTensor& tensor, object base,
                            PyObject** out)

    CStatus ConvertArrayToPandas(const shared_ptr[CArray]& arr,
                                 object py_ref, PyObject** out)

    CStatus ConvertColumnToPandas(const shared_ptr[CColumn]& arr,
                                  object py_ref, PyObject** out)

    CStatus ConvertTableToPandas(const shared_ptr[CTable]& table,
                                 int nthreads, PyObject** out)

    void c_set_default_memory_pool \
        " arrow::py::set_default_memory_pool"(CMemoryPool* pool)\

    CMemoryPool* c_get_memory_pool \
        " arrow::py::get_memory_pool"()

    cdef cppclass PyBuffer(CBuffer):
        PyBuffer(object o)

    cdef cppclass PyReadableFile(RandomAccessFile):
        PyReadableFile(object fo)

    cdef cppclass PyOutputStream(OutputStream):
        PyOutputStream(object fo)

    cdef cppclass PyBytesReader(CBufferReader):
        PyBytesReader(object fo)

cdef extern from 'arrow/python/init.h':
    int arrow_init_numpy() except -1

cdef extern from 'arrow/python/config.h' namespace 'arrow::py':
    void set_numpy_nan(object o)
