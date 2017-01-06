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
from pyarrow.includes.libarrow cimport CArray, CSchema, CStatus, CTable, MemoryPool
from pyarrow.includes.libarrow_io cimport ReadableFileInterface, OutputStream


cdef extern from "parquet/api/schema.h" namespace "parquet::schema" nogil:
  cdef cppclass Node:
    pass

  cdef cppclass GroupNode(Node):
    pass

  cdef cppclass PrimitiveNode(Node):
    pass

  cdef cppclass ColumnPath:
    c_string ToDotString()

cdef extern from "parquet/api/schema.h" namespace "parquet" nogil:
  enum ParquetVersion" parquet::ParquetVersion::type":
      PARQUET_1_0" parquet::ParquetVersion::PARQUET_1_0"
      PARQUET_2_0" parquet::ParquetVersion::PARQUET_2_0"

  enum Compression" parquet::Compression::type":
      UNCOMPRESSED" parquet::Compression::UNCOMPRESSED"
      SNAPPY" parquet::Compression::SNAPPY"
      GZIP" parquet::Compression::GZIP"
      LZO" parquet::Compression::LZO"
      BROTLI" parquet::Compression::BROTLI"

  cdef cppclass ColumnDescriptor:
    shared_ptr[ColumnPath] path()

  cdef cppclass SchemaDescriptor:
    const ColumnDescriptor* Column(int i)
    shared_ptr[Node] schema()
    GroupNode* group()


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

    cdef cppclass FileMetaData:
        uint32_t size()
        int num_columns()
        int64_t num_rows()
        int num_row_groups()
        int32_t version()
        const c_string created_by()
        int num_schema_elements()
        const SchemaDescriptor* schema()

    cdef cppclass ParquetFileReader:
        # TODO: Some default arguments are missing
        @staticmethod
        unique_ptr[ParquetFileReader] OpenFile(const c_string& path)
        shared_ptr[FileMetaData] metadata();


cdef extern from "parquet/api/writer.h" namespace "parquet" nogil:
    cdef cppclass ParquetOutputStream" parquet::OutputStream":
        pass

    cdef cppclass LocalFileOutputStream(ParquetOutputStream):
        LocalFileOutputStream(const c_string& path)
        void Close()

    cdef cppclass WriterProperties:
        cppclass Builder:
            Builder* version(ParquetVersion version)
            Builder* compression(Compression codec)
            Builder* compression(const c_string& path, Compression codec)
            Builder* disable_dictionary()
            Builder* enable_dictionary()
            Builder* enable_dictionary(const c_string& path)
            shared_ptr[WriterProperties] build()


cdef extern from "parquet/arrow/reader.h" namespace "parquet::arrow" nogil:
    CStatus OpenFile(const shared_ptr[ReadableFileInterface]& file,
                     MemoryPool* allocator,
                     unique_ptr[FileReader]* reader)

    cdef cppclass FileReader:
        FileReader(MemoryPool* pool, unique_ptr[ParquetFileReader] reader)
        CStatus ReadFlatColumn(int i, shared_ptr[CArray]* out);
        CStatus ReadFlatTable(shared_ptr[CTable]* out);
        const ParquetFileReader* parquet_reader();


cdef extern from "parquet/arrow/schema.h" namespace "parquet::arrow" nogil:
    CStatus FromParquetSchema(const SchemaDescriptor* parquet_schema,
                              shared_ptr[CSchema]* out)
    CStatus ToParquetSchema(const CSchema* arrow_schema,
                            shared_ptr[SchemaDescriptor]* out)


cdef extern from "parquet/arrow/writer.h" namespace "parquet::arrow" nogil:
    cdef CStatus WriteFlatTable(
        const CTable* table, MemoryPool* pool,
        const shared_ptr[OutputStream]& sink,
        int64_t chunk_size,
        const shared_ptr[WriterProperties]& properties)
