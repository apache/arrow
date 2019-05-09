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
# cython: language_level = 3

from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport (CChunkedArray, CSchema, CStatus,
                                        CTable, CMemoryPool, CBuffer,
                                        CKeyValueMetadata,
                                        RandomAccessFile, OutputStream,
                                        TimeUnit)


cdef extern from "parquet/api/schema.h" namespace "parquet::schema" nogil:
    cdef cppclass Node:
        pass

    cdef cppclass GroupNode(Node):
        pass

    cdef cppclass PrimitiveNode(Node):
        pass

    cdef cppclass ColumnPath:
        c_string ToDotString()
        vector[c_string] ToDotVector()


cdef extern from "parquet/api/schema.h" namespace "parquet" nogil:
    enum ParquetType" parquet::Type::type":
        ParquetType_BOOLEAN" parquet::Type::BOOLEAN"
        ParquetType_INT32" parquet::Type::INT32"
        ParquetType_INT64" parquet::Type::INT64"
        ParquetType_INT96" parquet::Type::INT96"
        ParquetType_FLOAT" parquet::Type::FLOAT"
        ParquetType_DOUBLE" parquet::Type::DOUBLE"
        ParquetType_BYTE_ARRAY" parquet::Type::BYTE_ARRAY"
        ParquetType_FIXED_LEN_BYTE_ARRAY" parquet::Type::FIXED_LEN_BYTE_ARRAY"

    enum ParquetLogicalType" parquet::LogicalType::type":
        ParquetLogicalType_NONE" parquet::LogicalType::NONE"
        ParquetLogicalType_UTF8" parquet::LogicalType::UTF8"
        ParquetLogicalType_MAP" parquet::LogicalType::MAP"
        ParquetLogicalType_MAP_KEY_VALUE" parquet::LogicalType::MAP_KEY_VALUE"
        ParquetLogicalType_LIST" parquet::LogicalType::LIST"
        ParquetLogicalType_ENUM" parquet::LogicalType::ENUM"
        ParquetLogicalType_DECIMAL" parquet::LogicalType::DECIMAL"
        ParquetLogicalType_DATE" parquet::LogicalType::DATE"
        ParquetLogicalType_TIME_MILLIS" parquet::LogicalType::TIME_MILLIS"
        ParquetLogicalType_TIME_MICROS" parquet::LogicalType::TIME_MICROS"
        ParquetLogicalType_TIMESTAMP_MILLIS \
            " parquet::LogicalType::TIMESTAMP_MILLIS"
        ParquetLogicalType_TIMESTAMP_MICROS \
            " parquet::LogicalType::TIMESTAMP_MICROS"
        ParquetLogicalType_UINT_8" parquet::LogicalType::UINT_8"
        ParquetLogicalType_UINT_16" parquet::LogicalType::UINT_16"
        ParquetLogicalType_UINT_32" parquet::LogicalType::UINT_32"
        ParquetLogicalType_UINT_64" parquet::LogicalType::UINT_64"
        ParquetLogicalType_INT_8" parquet::LogicalType::INT_8"
        ParquetLogicalType_INT_16" parquet::LogicalType::INT_16"
        ParquetLogicalType_INT_32" parquet::LogicalType::INT_32"
        ParquetLogicalType_INT_64" parquet::LogicalType::INT_64"
        ParquetLogicalType_JSON" parquet::LogicalType::JSON"
        ParquetLogicalType_BSON" parquet::LogicalType::BSON"
        ParquetLogicalType_INTERVAL" parquet::LogicalType::INTERVAL"

    enum ParquetRepetition" parquet::Repetition::type":
        ParquetRepetition_REQUIRED" parquet::REPETITION::REQUIRED"
        ParquetRepetition_OPTIONAL" parquet::REPETITION::OPTIONAL"
        ParquetRepetition_REPEATED" parquet::REPETITION::REPEATED"

    enum ParquetEncoding" parquet::Encoding::type":
        ParquetEncoding_PLAIN" parquet::Encoding::PLAIN"
        ParquetEncoding_PLAIN_DICTIONARY" parquet::Encoding::PLAIN_DICTIONARY"
        ParquetEncoding_RLE" parquet::Encoding::RLE"
        ParquetEncoding_BIT_PACKED" parquet::Encoding::BIT_PACKED"
        ParquetEncoding_DELTA_BINARY_PACKED \
            " parquet::Encoding::DELTA_BINARY_PACKED"
        ParquetEncoding_DELTA_LENGTH_BYTE_ARRAY \
            " parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY"
        ParquetEncoding_DELTA_BYTE_ARRAY" parquet::Encoding::DELTA_BYTE_ARRAY"
        ParquetEncoding_RLE_DICTIONARY" parquet::Encoding::RLE_DICTIONARY"

    enum ParquetCompression" parquet::Compression::type":
        ParquetCompression_UNCOMPRESSED" parquet::Compression::UNCOMPRESSED"
        ParquetCompression_SNAPPY" parquet::Compression::SNAPPY"
        ParquetCompression_GZIP" parquet::Compression::GZIP"
        ParquetCompression_LZO" parquet::Compression::LZO"
        ParquetCompression_BROTLI" parquet::Compression::BROTLI"
        ParquetCompression_LZ4" parquet::Compression::LZ4"
        ParquetCompression_ZSTD" parquet::Compression::ZSTD"

    enum ParquetVersion" parquet::ParquetVersion::type":
        ParquetVersion_V1" parquet::ParquetVersion::PARQUET_1_0"
        ParquetVersion_V2" parquet::ParquetVersion::PARQUET_2_0"

    enum ParquetSortOrder" parquet::SortOrder::type":
        ParquetSortOrder_SIGNED" parquet::SortOrder::SIGNED"
        ParquetSortOrder_UNSIGNED" parquet::SortOrder::UNSIGNED"
        ParquetSortOrder_UNKNOWN" parquet::SortOrder::UNKNOWN"

    cdef cppclass ColumnDescriptor:
        c_bool Equals(const ColumnDescriptor& other)

        shared_ptr[ColumnPath] path()
        int16_t max_definition_level()
        int16_t max_repetition_level()

        ParquetType physical_type()
        ParquetLogicalType logical_type()
        const c_string& name()
        int type_length()
        int type_precision()
        int type_scale()

    cdef cppclass SchemaDescriptor:
        const ColumnDescriptor* Column(int i)
        shared_ptr[Node] schema()
        GroupNode* group()
        c_bool Equals(const SchemaDescriptor& other)
        int num_columns()

    cdef c_string FormatStatValue(ParquetType parquet_type, c_string val)


cdef extern from "parquet/api/reader.h" namespace "parquet" nogil:
    cdef cppclass ColumnReader:
        pass

    cdef cppclass BoolReader(ColumnReader):
        pass

    cdef cppclass Int32Reader(ColumnReader):
        pass

    cdef cppclass Int64Reader(ColumnReader):
        pass

    cdef cppclass Int96Reader(ColumnReader):
        pass

    cdef cppclass FloatReader(ColumnReader):
        pass

    cdef cppclass DoubleReader(ColumnReader):
        pass

    cdef cppclass ByteArrayReader(ColumnReader):
        pass

    cdef cppclass RowGroupReader:
        pass

    cdef cppclass CEncodedStatistics" parquet::EncodedStatistics":
        const c_string& max() const
        const c_string& min() const
        int64_t null_count
        int64_t distinct_count
        bint has_min
        bint has_max
        bint has_null_count
        bint has_distinct_count

    cdef cppclass CRowGroupStatistics" parquet::RowGroupStatistics":
        int64_t null_count() const
        int64_t distinct_count() const
        int64_t num_values() const
        bint HasMinMax()
        void Reset()
        c_string EncodeMin()
        c_string EncodeMax()
        CEncodedStatistics Encode()
        void SetComparator()
        ParquetType physical_type() const

    cdef cppclass CColumnChunkMetaData" parquet::ColumnChunkMetaData":
        int64_t file_offset() const
        const c_string& file_path() const

        ParquetType type() const
        int64_t num_values() const
        shared_ptr[ColumnPath] path_in_schema() const
        bint is_stats_set() const
        shared_ptr[CRowGroupStatistics] statistics() const
        ParquetCompression compression() const
        const vector[ParquetEncoding]& encodings() const

        int64_t has_dictionary_page() const
        int64_t dictionary_page_offset() const
        int64_t data_page_offset() const
        int64_t index_page_offset() const
        int64_t total_compressed_size() const
        int64_t total_uncompressed_size() const

    cdef cppclass CRowGroupMetaData" parquet::RowGroupMetaData":
        int num_columns()
        int64_t num_rows()
        int64_t total_byte_size()
        unique_ptr[CColumnChunkMetaData] ColumnChunk(int i) const

    cdef cppclass CFileMetaData" parquet::FileMetaData":
        uint32_t size()
        int num_columns()
        int64_t num_rows()
        int num_row_groups()
        ParquetVersion version()
        const c_string created_by()
        int num_schema_elements()

        unique_ptr[CRowGroupMetaData] RowGroup(int i)
        const SchemaDescriptor* schema()
        shared_ptr[const CKeyValueMetadata] key_value_metadata() const
        void WriteTo(ParquetOutputStream* dst) const

    cdef shared_ptr[CFileMetaData] CFileMetaData_Make \
        " parquet::FileMetaData::Make"(const void* serialized_metadata,
                                       uint32_t* metadata_len)

    cdef cppclass ReaderProperties:
        pass

    ReaderProperties default_reader_properties()

    cdef cppclass ParquetFileReader:
        @staticmethod
        unique_ptr[ParquetFileReader] Open(
            const shared_ptr[RandomAccessFile]& file,
            const ReaderProperties& props,
            const shared_ptr[CFileMetaData]& metadata)

        @staticmethod
        unique_ptr[ParquetFileReader] OpenFile(const c_string& path)
        shared_ptr[CFileMetaData] metadata()


cdef extern from "parquet/api/writer.h" namespace "parquet" nogil:
    cdef cppclass ParquetOutputStream" parquet::OutputStream":
        pass

    cdef cppclass ParquetInMemoryOutputStream \
            " parquet::InMemoryOutputStream"(ParquetOutputStream):
        shared_ptr[CBuffer] GetBuffer()

    cdef cppclass WriterProperties:
        cppclass Builder:
            Builder* version(ParquetVersion version)
            Builder* compression(ParquetCompression codec)
            Builder* compression(const c_string& path,
                                 ParquetCompression codec)
            Builder* disable_dictionary()
            Builder* enable_dictionary()
            Builder* enable_dictionary(const c_string& path)
            shared_ptr[WriterProperties] build()


cdef extern from "parquet/arrow/reader.h" namespace "parquet::arrow" nogil:
    CStatus OpenFile(const shared_ptr[RandomAccessFile]& file,
                     CMemoryPool* allocator,
                     const ReaderProperties& properties,
                     const shared_ptr[CFileMetaData]& metadata,
                     unique_ptr[FileReader]* reader)

    cdef cppclass FileReader:
        FileReader(CMemoryPool* pool, unique_ptr[ParquetFileReader] reader)
        CStatus ReadColumn(int i, shared_ptr[CChunkedArray]* out)
        CStatus ReadSchemaField(int i, shared_ptr[CChunkedArray]* out)

        int num_row_groups()
        CStatus ReadRowGroup(int i, shared_ptr[CTable]* out)
        CStatus ReadRowGroup(int i, const vector[int]& column_indices,
                             shared_ptr[CTable]* out)

        CStatus ReadTable(shared_ptr[CTable]* out)
        CStatus ReadTable(const vector[int]& column_indices,
                          shared_ptr[CTable]* out)

        CStatus ScanContents(vector[int] columns, int32_t column_batch_size,
                             int64_t* num_rows)

        const ParquetFileReader* parquet_reader()

        void set_use_threads(c_bool use_threads)


cdef extern from "parquet/arrow/schema.h" namespace "parquet::arrow" nogil:
    CStatus FromParquetSchema(
        const SchemaDescriptor* parquet_schema,
        const shared_ptr[const CKeyValueMetadata]& key_value_metadata,
        shared_ptr[CSchema]* out)

    CStatus ToParquetSchema(
        const CSchema* arrow_schema,
        const shared_ptr[const CKeyValueMetadata]& key_value_metadata,
        shared_ptr[SchemaDescriptor]* out)


cdef extern from "parquet/arrow/writer.h" namespace "parquet::arrow" nogil:
    cdef cppclass FileWriter:

        @staticmethod
        CStatus Open(const CSchema& schema, CMemoryPool* pool,
                     const shared_ptr[OutputStream]& sink,
                     const shared_ptr[WriterProperties]& properties,
                     const shared_ptr[ArrowWriterProperties]& arrow_properties,
                     unique_ptr[FileWriter]* writer)

        CStatus WriteTable(const CTable& table, int64_t chunk_size)
        CStatus NewRowGroup(int64_t chunk_size)
        CStatus Close()

        const shared_ptr[CFileMetaData] metadata() const

    cdef cppclass ArrowWriterProperties:
        cppclass Builder:
            Builder()
            Builder* disable_deprecated_int96_timestamps()
            Builder* enable_deprecated_int96_timestamps()
            Builder* coerce_timestamps(TimeUnit unit)
            Builder* allow_truncated_timestamps()
            Builder* disallow_truncated_timestamps()
            shared_ptr[ArrowWriterProperties] build()
        c_bool support_deprecated_int96_timestamps()
