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

# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True

from cython.operator cimport dereference as deref
from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport *
from pyarrow.lib cimport (Array, Schema,
                          check_status,
                          MemoryPool, maybe_unbox_memory_pool,
                          Table,
                          pyarrow_wrap_schema,
                          pyarrow_wrap_table,
                          NativeFile, get_reader, get_writer)

from pyarrow.compat import tobytes, frombytes
from pyarrow.formatting import indent
from pyarrow.lib import ArrowException, NativeFile, _stringify_path

import six
import warnings


cdef class RowGroupStatistics:
    cdef:
        shared_ptr[CRowGroupStatistics] statistics

    def __cinit__(self):
        pass

    cdef init(self, const shared_ptr[CRowGroupStatistics]& statistics):
        self.statistics = statistics

    def __repr__(self):
        return """{0}
  has_min_max: {1}
  min: {2}
  max: {3}
  null_count: {4}
  distinct_count: {5}
  num_values: {6}
  physical_type: {7}""".format(object.__repr__(self),
                               self.has_min_max,
                               self.min,
                               self.max,
                               self.null_count,
                               self.distinct_count,
                               self.num_values,
                               self.physical_type)

    cdef inline _cast_statistic(self, object value):
        # Input value is bytes
        cdef ParquetType physical_type = self.statistics.get().physical_type()
        if physical_type == ParquetType_BOOLEAN:
            return bool(int(value))
        elif physical_type == ParquetType_INT32:
            return int(value)
        elif physical_type == ParquetType_INT64:
            return int(value)
        elif physical_type == ParquetType_INT96:
            # Leave as PyBytes
            return value
        elif physical_type == ParquetType_FLOAT:
            return float(value)
        elif physical_type == ParquetType_DOUBLE:
            return float(value)
        elif physical_type == ParquetType_BYTE_ARRAY:
            # Leave as PyBytes
            return value
        elif physical_type == ParquetType_FIXED_LEN_BYTE_ARRAY:
            # Leave as PyBytes
            return value
        else:
            raise ValueError('Unknown physical ParquetType')

    @property
    def has_min_max(self):
        return self.statistics.get().HasMinMax()

    @property
    def min(self):
        raw_physical_type = self.statistics.get().physical_type()
        encode_min = self.statistics.get().EncodeMin()

        min_value = FormatStatValue(raw_physical_type, encode_min)
        return self._cast_statistic(min_value)

    @property
    def max(self):
        raw_physical_type = self.statistics.get().physical_type()
        encode_max = self.statistics.get().EncodeMax()

        max_value = FormatStatValue(raw_physical_type, encode_max)
        return self._cast_statistic(max_value)

    @property
    def null_count(self):
        return self.statistics.get().null_count()

    @property
    def distinct_count(self):
        return self.statistics.get().distinct_count()

    @property
    def num_values(self):
        return self.statistics.get().num_values()

    @property
    def physical_type(self):
        raw_physical_type = self.statistics.get().physical_type()
        return physical_type_name_from_enum(raw_physical_type)


cdef class ColumnChunkMetaData:
    cdef:
        unique_ptr[CColumnChunkMetaData] up_metadata
        CColumnChunkMetaData* metadata

    def __cinit__(self):
        pass

    cdef init(self, const CRowGroupMetaData& row_group_metadata, int i):
        self.up_metadata = row_group_metadata.ColumnChunk(i)
        self.metadata = self.up_metadata.get()

    def __repr__(self):
        statistics = indent(repr(self.statistics), 4 * ' ')
        return """{0}
  file_offset: {1}
  file_path: {2}
  physical_type: {3}
  num_values: {4}
  path_in_schema: {5}
  is_stats_set: {6}
  statistics:
{7}
  compression: {8}
  encodings: {9}
  has_dictionary_page: {10}
  dictionary_page_offset: {11}
  data_page_offset: {12}
  total_compressed_size: {13}
  total_uncompressed_size: {14}""".format(object.__repr__(self),
                                          self.file_offset,
                                          self.file_path,
                                          self.physical_type,
                                          self.num_values,
                                          self.path_in_schema,
                                          self.is_stats_set,
                                          statistics,
                                          self.compression,
                                          self.encodings,
                                          self.has_dictionary_page,
                                          self.dictionary_page_offset,
                                          self.data_page_offset,
                                          self.total_compressed_size,
                                          self.total_uncompressed_size)

    @property
    def file_offset(self):
        return self.metadata.file_offset()

    @property
    def file_path(self):
        return frombytes(self.metadata.file_path())

    @property
    def physical_type(self):
        return physical_type_name_from_enum(self.metadata.type())

    @property
    def num_values(self):
        return self.metadata.num_values()

    @property
    def path_in_schema(self):
        path = self.metadata.path_in_schema().get().ToDotString()
        return frombytes(path)

    @property
    def is_stats_set(self):
        return self.metadata.is_stats_set()

    @property
    def statistics(self):
        if not self.metadata.is_stats_set():
            return None
        statistics = RowGroupStatistics()
        statistics.init(self.metadata.statistics())
        return statistics

    @property
    def compression(self):
        return compression_name_from_enum(self.metadata.compression())

    @property
    def encodings(self):
        return tuple(map(encoding_name_from_enum, self.metadata.encodings()))

    @property
    def has_dictionary_page(self):
        return bool(self.metadata.has_dictionary_page())

    @property
    def dictionary_page_offset(self):
        if self.has_dictionary_page:
            return self.metadata.dictionary_page_offset()
        else:
            return None

    @property
    def data_page_offset(self):
        return self.metadata.data_page_offset()

    @property
    def has_index_page(self):
        raise NotImplementedError('not supported in parquet-cpp')

    @property
    def index_page_offset(self):
        raise NotImplementedError("parquet-cpp doesn't return valid values")

    @property
    def total_compressed_size(self):
        return self.metadata.total_compressed_size()

    @property
    def total_uncompressed_size(self):
        return self.metadata.total_uncompressed_size()


cdef class RowGroupMetaData:
    cdef:
        unique_ptr[CRowGroupMetaData] up_metadata
        CRowGroupMetaData* metadata
        FileMetaData parent

    def __cinit__(self, FileMetaData parent, int i):
        if i < 0 or i >= parent.num_row_groups:
            raise IndexError('{0} out of bounds'.format(i))
        self.up_metadata = parent._metadata.RowGroup(i)
        self.metadata = self.up_metadata.get()
        self.parent = parent

    def column(self, int i):
        chunk = ColumnChunkMetaData()
        chunk.init(deref(self.metadata), i)
        return chunk

    def __repr__(self):
        return """{0}
  num_columns: {1}
  num_rows: {2}
  total_byte_size: {3}""".format(object.__repr__(self),
                                 self.num_columns,
                                 self.num_rows,
                                 self.total_byte_size)

    @property
    def num_columns(self):
        return self.metadata.num_columns()

    @property
    def num_rows(self):
        return self.metadata.num_rows()

    @property
    def total_byte_size(self):
        return self.metadata.total_byte_size()


cdef class FileMetaData:
    cdef:
        shared_ptr[CFileMetaData] sp_metadata
        CFileMetaData* _metadata
        ParquetSchema _schema

    def __cinit__(self):
        pass

    cdef init(self, const shared_ptr[CFileMetaData]& metadata):
        self.sp_metadata = metadata
        self._metadata = metadata.get()

    def __repr__(self):
        return """{0}
  created_by: {1}
  num_columns: {2}
  num_rows: {3}
  num_row_groups: {4}
  format_version: {5}
  serialized_size: {6}""".format(object.__repr__(self),
                                 self.created_by, self.num_columns,
                                 self.num_rows, self.num_row_groups,
                                 self.format_version,
                                 self.serialized_size)

    @property
    def schema(self):
        if self._schema is None:
            self._schema = ParquetSchema(self)
        return self._schema

    @property
    def serialized_size(self):
        return self._metadata.size()

    @property
    def num_columns(self):
        return self._metadata.num_columns()

    @property
    def num_rows(self):
        return self._metadata.num_rows()

    @property
    def num_row_groups(self):
        return self._metadata.num_row_groups()

    @property
    def format_version(self):
        cdef ParquetVersion version = self._metadata.version()
        if version == ParquetVersion_V1:
            return '1.0'
        if version == ParquetVersion_V2:
            return '2.0'
        else:
            warnings.warn('Unrecognized file version, assuming 1.0: {}'
                          .format(version))
            return '1.0'

    @property
    def created_by(self):
        return frombytes(self._metadata.created_by())

    @property
    def metadata(self):
        cdef:
            unordered_map[c_string, c_string] metadata
            const CKeyValueMetadata* underlying_metadata
        underlying_metadata = self._metadata.key_value_metadata().get()
        if underlying_metadata != NULL:
            underlying_metadata.ToUnorderedMap(&metadata)
            return metadata
        else:
            return None

    def row_group(self, int i):
        return RowGroupMetaData(self, i)


cdef class ParquetSchema:
    cdef:
        FileMetaData parent  # the FileMetaData owning the SchemaDescriptor
        const SchemaDescriptor* schema

    def __cinit__(self, FileMetaData container):
        self.parent = container
        self.schema = container._metadata.schema()

    def __repr__(self):
        cdef const ColumnDescriptor* descr
        elements = []
        for i in range(self.schema.num_columns()):
            col = self.column(i)
            logical_type = col.logical_type
            formatted = '{0}: {1}'.format(col.path, col.physical_type)
            if logical_type != 'NONE':
                formatted += ' {0}'.format(logical_type)
            elements.append(formatted)

        return """{0}
{1}
 """.format(object.__repr__(self), '\n'.join(elements))

    def __len__(self):
        return self.schema.num_columns()

    def __getitem__(self, i):
        return self.column(i)

    @property
    def names(self):
        return [self[i].name for i in range(len(self))]

    def to_arrow_schema(self):
        """
        Convert Parquet schema to effective Arrow schema

        Returns
        -------
        schema : pyarrow.Schema
        """
        cdef shared_ptr[CSchema] sp_arrow_schema

        with nogil:
            check_status(FromParquetSchema(
                self.schema, self.parent._metadata.key_value_metadata(),
                &sp_arrow_schema))

        return pyarrow_wrap_schema(sp_arrow_schema)

    def __eq__(self, other):
        try:
            return self.equals(other)
        except TypeError:
            return NotImplemented

    def equals(self, ParquetSchema other):
        """
        Returns True if the Parquet schemas are equal
        """
        return self.schema.Equals(deref(other.schema))

    def column(self, i):
        if i < 0 or i >= len(self):
            raise IndexError('{0} out of bounds'.format(i))

        return ColumnSchema(self, i)


cdef class ColumnSchema:
    cdef:
        ParquetSchema parent
        const ColumnDescriptor* descr

    def __cinit__(self, ParquetSchema schema, int i):
        self.parent = schema
        self.descr = schema.schema.Column(i)

    def __eq__(self, other):
        try:
            return self.equals(other)
        except TypeError:
            return NotImplemented

    def equals(self, ColumnSchema other):
        """
        Returns True if the column schemas are equal
        """
        return self.descr.Equals(deref(other.descr))

    def __repr__(self):
        physical_type = self.physical_type
        logical_type = self.logical_type
        if logical_type == 'DECIMAL':
            logical_type = 'DECIMAL({0}, {1})'.format(self.precision,
                                                      self.scale)
        elif physical_type == 'FIXED_LEN_BYTE_ARRAY':
            logical_type = ('FIXED_LEN_BYTE_ARRAY(length={0})'
                            .format(self.length))

        return """<ParquetColumnSchema>
  name: {0}
  path: {1}
  max_definition_level: {2}
  max_repetition_level: {3}
  physical_type: {4}
  logical_type: {5}""".format(self.name, self.path, self.max_definition_level,
                              self.max_repetition_level, physical_type,
                              logical_type)

    @property
    def name(self):
        return frombytes(self.descr.name())

    @property
    def path(self):
        return frombytes(self.descr.path().get().ToDotString())

    @property
    def max_definition_level(self):
        return self.descr.max_definition_level()

    @property
    def max_repetition_level(self):
        return self.descr.max_repetition_level()

    @property
    def physical_type(self):
        return physical_type_name_from_enum(self.descr.physical_type())

    @property
    def logical_type(self):
        return logical_type_name_from_enum(self.descr.logical_type())

    # FIXED_LEN_BYTE_ARRAY attribute
    @property
    def length(self):
        return self.descr.type_length()

    # Decimal attributes
    @property
    def precision(self):
        return self.descr.type_precision()

    @property
    def scale(self):
        return self.descr.type_scale()


cdef physical_type_name_from_enum(ParquetType type_):
    return {
        ParquetType_BOOLEAN: 'BOOLEAN',
        ParquetType_INT32: 'INT32',
        ParquetType_INT64: 'INT64',
        ParquetType_INT96: 'INT96',
        ParquetType_FLOAT: 'FLOAT',
        ParquetType_DOUBLE: 'DOUBLE',
        ParquetType_BYTE_ARRAY: 'BYTE_ARRAY',
        ParquetType_FIXED_LEN_BYTE_ARRAY: 'FIXED_LEN_BYTE_ARRAY',
    }.get(type_, 'UNKNOWN')


cdef logical_type_name_from_enum(ParquetLogicalType type_):
    return {
        ParquetLogicalType_NONE: 'NONE',
        ParquetLogicalType_UTF8: 'UTF8',
        ParquetLogicalType_MAP: 'MAP',
        ParquetLogicalType_MAP_KEY_VALUE: 'MAP_KEY_VALUE',
        ParquetLogicalType_LIST: 'LIST',
        ParquetLogicalType_ENUM: 'ENUM',
        ParquetLogicalType_DECIMAL: 'DECIMAL',
        ParquetLogicalType_DATE: 'DATE',
        ParquetLogicalType_TIME_MILLIS: 'TIME_MILLIS',
        ParquetLogicalType_TIME_MICROS: 'TIME_MICROS',
        ParquetLogicalType_TIMESTAMP_MILLIS: 'TIMESTAMP_MILLIS',
        ParquetLogicalType_TIMESTAMP_MICROS: 'TIMESTAMP_MICROS',
        ParquetLogicalType_UINT_8: 'UINT_8',
        ParquetLogicalType_UINT_16: 'UINT_16',
        ParquetLogicalType_UINT_32: 'UINT_32',
        ParquetLogicalType_UINT_64: 'UINT_64',
        ParquetLogicalType_INT_8: 'INT_8',
        ParquetLogicalType_INT_16: 'INT_16',
        ParquetLogicalType_INT_32: 'INT_32',
        ParquetLogicalType_INT_64: 'UINT_64',
        ParquetLogicalType_JSON: 'JSON',
        ParquetLogicalType_BSON: 'BSON',
        ParquetLogicalType_INTERVAL: 'INTERVAL',
    }.get(type_, 'UNKNOWN')


cdef encoding_name_from_enum(ParquetEncoding encoding_):
    return {
        ParquetEncoding_PLAIN: 'PLAIN',
        ParquetEncoding_PLAIN_DICTIONARY: 'PLAIN_DICTIONARY',
        ParquetEncoding_RLE: 'RLE',
        ParquetEncoding_BIT_PACKED: 'BIT_PACKED',
        ParquetEncoding_DELTA_BINARY_PACKED: 'DELTA_BINARY_PACKED',
        ParquetEncoding_DELTA_LENGTH_BYTE_ARRAY: 'DELTA_LENGTH_BYTE_ARRAY',
        ParquetEncoding_DELTA_BYTE_ARRAY: 'DELTA_BYTE_ARRAY',
        ParquetEncoding_RLE_DICTIONARY: 'RLE_DICTIONARY',
    }.get(encoding_, 'UNKNOWN')


cdef compression_name_from_enum(ParquetCompression compression_):
    return {
        ParquetCompression_UNCOMPRESSED: 'UNCOMPRESSED',
        ParquetCompression_SNAPPY: 'SNAPPY',
        ParquetCompression_GZIP: 'GZIP',
        ParquetCompression_LZO: 'LZO',
        ParquetCompression_BROTLI: 'BROTLI',
        ParquetCompression_LZ4: 'LZ4',
        ParquetCompression_ZSTD: 'ZSTD',
    }.get(compression_, 'UNKNOWN')


cdef int check_compression_name(name) except -1:
    if name.upper() not in {'NONE', 'SNAPPY', 'GZIP', 'LZO', 'BROTLI', 'LZ4',
                            'ZSTD'}:
        raise ArrowException("Unsupported compression: " + name)
    return 0


cdef ParquetCompression compression_from_name(str name):
    name = name.upper()
    if name == 'SNAPPY':
        return ParquetCompression_SNAPPY
    elif name == 'GZIP':
        return ParquetCompression_GZIP
    elif name == 'LZO':
        return ParquetCompression_LZO
    elif name == 'BROTLI':
        return ParquetCompression_BROTLI
    elif name == 'LZ4':
        return ParquetCompression_LZ4
    elif name == 'ZSTD':
        return ParquetCompression_ZSTD
    else:
        return ParquetCompression_UNCOMPRESSED


cdef class ParquetReader:
    cdef:
        object source
        CMemoryPool* allocator
        unique_ptr[FileReader] reader
        FileMetaData _metadata

    cdef public:
        _column_idx_map

    def __cinit__(self, MemoryPool memory_pool=None):
        self.allocator = maybe_unbox_memory_pool(memory_pool)
        self._metadata = None

    def open(self, object source, c_bool use_memory_map=True,
             FileMetaData metadata=None):
        cdef:
            shared_ptr[RandomAccessFile] rd_handle
            shared_ptr[CFileMetaData] c_metadata
            ReaderProperties properties = default_reader_properties()
            c_string path

        if metadata is not None:
            c_metadata = metadata.sp_metadata

        self.source = source

        get_reader(source, use_memory_map, &rd_handle)
        with nogil:
            check_status(OpenFile(rd_handle, self.allocator, properties,
                                  c_metadata, &self.reader))

    @property
    def column_paths(self):
        cdef:
            FileMetaData container = self.metadata
            const CFileMetaData* metadata = container._metadata
            vector[c_string] path
            int i = 0

        paths = []
        for i in range(0, metadata.num_columns()):
            path = (metadata.schema().Column(i)
                    .path().get().ToDotVector())
            paths.append([frombytes(x) for x in path])

        return paths

    @property
    def metadata(self):
        cdef:
            shared_ptr[CFileMetaData] metadata
            FileMetaData result
        if self._metadata is not None:
            return self._metadata

        with nogil:
            metadata = self.reader.get().parquet_reader().metadata()

        self._metadata = result = FileMetaData()
        result.init(metadata)
        return result

    @property
    def num_row_groups(self):
        return self.reader.get().num_row_groups()

    def set_use_threads(self, bint use_threads):
        self.reader.get().set_use_threads(use_threads)

    def read_row_group(self, int i, column_indices=None,
                       bint use_threads=True):
        cdef:
            shared_ptr[CTable] ctable
            vector[int] c_column_indices

        if use_threads:
            self.set_use_threads(use_threads)

        if column_indices is not None:
            for index in column_indices:
                c_column_indices.push_back(index)

            with nogil:
                check_status(self.reader.get()
                             .ReadRowGroup(i, c_column_indices, &ctable))
        else:
            # Read all columns
            with nogil:
                check_status(self.reader.get()
                             .ReadRowGroup(i, &ctable))
        return pyarrow_wrap_table(ctable)

    def read_all(self, column_indices=None, bint use_threads=True):
        cdef:
            shared_ptr[CTable] ctable
            vector[int] c_column_indices

        if use_threads:
            self.set_use_threads(use_threads)

        if column_indices is not None:
            for index in column_indices:
                c_column_indices.push_back(index)

            with nogil:
                check_status(self.reader.get()
                             .ReadTable(c_column_indices, &ctable))
        else:
            # Read all columns
            with nogil:
                check_status(self.reader.get()
                             .ReadTable(&ctable))
        return pyarrow_wrap_table(ctable)

    def scan_contents(self, column_indices=None, batch_size=65536):
        cdef:
            vector[int] c_column_indices
            int32_t c_batch_size
            int64_t c_num_rows

        if column_indices is not None:
            for index in column_indices:
                c_column_indices.push_back(index)

        c_batch_size = batch_size

        with nogil:
            check_status(self.reader.get()
                         .ScanContents(c_column_indices, c_batch_size,
                                       &c_num_rows))

        return c_num_rows

    def column_name_idx(self, column_name):
        """
        Find the matching index of a column in the schema.

        Parameter
        ---------
        column_name: str
            Name of the column, separation of nesting levels is done via ".".

        Returns
        -------
        column_idx: int
            Integer index of the position of the column
        """
        cdef:
            FileMetaData container = self.metadata
            const CFileMetaData* metadata = container._metadata
            int i = 0

        if self._column_idx_map is None:
            self._column_idx_map = {}
            for i in range(0, metadata.num_columns()):
                col_bytes = tobytes(metadata.schema().Column(i)
                                    .path().get().ToDotString())
                self._column_idx_map[col_bytes] = i

        return self._column_idx_map[tobytes(column_name)]

    def read_column(self, int column_index):
        cdef:
            Array array = Array()
            shared_ptr[CArray] carray

        with nogil:
            check_status(self.reader.get()
                         .ReadColumn(column_index, &carray))

        array.init(carray)
        return array

    def read_schema_field(self, int field_index):
        cdef:
            Array array = Array()
            shared_ptr[CArray] carray

        with nogil:
            check_status(self.reader.get()
                         .ReadSchemaField(field_index, &carray))

        array.init(carray)
        return array


cdef class ParquetWriter:
    cdef:
        unique_ptr[FileWriter] writer
        shared_ptr[OutputStream] sink
        bint own_sink

    cdef readonly:
        object use_dictionary
        object use_deprecated_int96_timestamps
        object coerce_timestamps
        object compression
        object version
        int row_group_size

    def __cinit__(self, where, Schema schema, use_dictionary=None,
                  compression=None, version=None,
                  MemoryPool memory_pool=None,
                  use_deprecated_int96_timestamps=False,
                  coerce_timestamps=None):
        cdef:
            shared_ptr[WriterProperties] properties
            c_string c_where
            CMemoryPool* pool

        try:
            where = _stringify_path(where)
        except TypeError:
            get_writer(where, &self.sink)
            self.own_sink = False
        else:
            c_where = tobytes(where)
            with nogil:
                check_status(FileOutputStream.Open(c_where,
                                                   &self.sink))
            self.own_sink = True

        self.use_dictionary = use_dictionary
        self.compression = compression
        self.version = version
        self.use_deprecated_int96_timestamps = use_deprecated_int96_timestamps
        self.coerce_timestamps = coerce_timestamps

        cdef WriterProperties.Builder properties_builder
        self._set_version(&properties_builder)
        self._set_compression_props(&properties_builder)
        self._set_dictionary_props(&properties_builder)
        properties = properties_builder.build()

        cdef ArrowWriterProperties.Builder arrow_properties_builder
        self._set_int96_support(&arrow_properties_builder)
        self._set_coerce_timestamps(&arrow_properties_builder)
        arrow_properties = arrow_properties_builder.build()

        pool = maybe_unbox_memory_pool(memory_pool)
        with nogil:
            check_status(
                FileWriter.Open(deref(schema.schema), pool,
                                self.sink, properties, arrow_properties,
                                &self.writer))

    cdef void _set_int96_support(self, ArrowWriterProperties.Builder* props):
        if self.use_deprecated_int96_timestamps:
            props.enable_deprecated_int96_timestamps()
        else:
            props.disable_deprecated_int96_timestamps()

    cdef int _set_coerce_timestamps(
            self, ArrowWriterProperties.Builder* props) except -1:
        if self.coerce_timestamps == 'ms':
            props.coerce_timestamps(TimeUnit_MILLI)
        elif self.coerce_timestamps == 'us':
            props.coerce_timestamps(TimeUnit_MICRO)
        elif self.coerce_timestamps is not None:
            raise ValueError('Invalid value for coerce_timestamps: {0}'
                             .format(self.coerce_timestamps))

    cdef void _set_version(self, WriterProperties.Builder* props):
        if self.version is not None:
            if self.version == "1.0":
                props.version(ParquetVersion_V1)
            elif self.version == "2.0":
                props.version(ParquetVersion_V2)
            else:
                raise ArrowException("Unsupported Parquet format version")

    cdef void _set_compression_props(self, WriterProperties.Builder* props):
        if isinstance(self.compression, basestring):
            check_compression_name(self.compression)
            props.compression(compression_from_name(self.compression))
        elif self.compression is not None:
            for column, codec in self.compression.iteritems():
                check_compression_name(codec)
                props.compression(column, compression_from_name(codec))

    cdef void _set_dictionary_props(self, WriterProperties.Builder* props):
        if isinstance(self.use_dictionary, bool):
            if self.use_dictionary:
                props.enable_dictionary()
            else:
                props.disable_dictionary()
        elif self.use_dictionary is not None:
            # Deactivate dictionary encoding by default
            props.disable_dictionary()
            for column in self.use_dictionary:
                props.enable_dictionary(column)

    def close(self):
        with nogil:
            check_status(self.writer.get().Close())
            if self.own_sink:
                check_status(self.sink.get().Close())

    def write_table(self, Table table, row_group_size=None):
        cdef CTable* ctable = table.table

        if row_group_size is None or row_group_size == -1:
            if ctable.num_rows() > 0:
                row_group_size = ctable.num_rows()
            else:
                row_group_size = 1
        elif row_group_size == 0:
            raise ValueError('Row group size cannot be 0')

        cdef int64_t c_row_group_size = row_group_size

        with nogil:
            check_status(self.writer.get()
                         .WriteTable(deref(ctable), c_row_group_size))
