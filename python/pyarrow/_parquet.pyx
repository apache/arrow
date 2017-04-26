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
from pyarrow.lib import ArrowException, NativeFile

import six


cdef class RowGroupMetaData:
    cdef:
        unique_ptr[CRowGroupMetaData] up_metadata
        CRowGroupMetaData* metadata
        FileMetaData parent

    def __cinit__(self):
        pass

    cdef void init_from_file(self, FileMetaData parent, int i):
        if i < 0 or i >= parent.num_row_groups:
            raise IndexError('{0} out of bounds'.format(i))
        self.up_metadata = parent._metadata.RowGroup(i)
        self.metadata = self.up_metadata.get()
        self.parent = parent

    def __repr__(self):
        return """{0}
  num_columns: {1}
  num_rows: {2}
  total_byte_size: {3}""".format(object.__repr__(self),
                                 self.num_columns,
                                 self.num_rows,
                                 self.total_byte_size)

    property num_columns:

        def __get__(self):
            return self.metadata.num_columns()

    property num_rows:

        def __get__(self):
            return self.metadata.num_rows()

    property total_byte_size:

        def __get__(self):
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
        if self._schema is not None:
            return self._schema

        cdef ParquetSchema schema = ParquetSchema()
        schema.init_from_filemeta(self)
        self._schema = schema
        return schema

    property serialized_size:

        def __get__(self):
            return self._metadata.size()

    property num_columns:

        def __get__(self):
            return self._metadata.num_columns()

    property num_rows:

        def __get__(self):
            return self._metadata.num_rows()

    property num_row_groups:

        def __get__(self):
            return self._metadata.num_row_groups()

    property format_version:

        def __get__(self):
            cdef ParquetVersion version = self._metadata.version()
            if version == ParquetVersion_V1:
                return '1.0'
            if version == ParquetVersion_V2:
                return '2.0'
            else:
                print('Unrecognized file version, assuming 1.0: {0}'
                      .format(version))
                return '1.0'

    property created_by:

        def __get__(self):
            return frombytes(self._metadata.created_by())

    def row_group(self, int i):
        """

        """
        cdef RowGroupMetaData result = RowGroupMetaData()
        result.init_from_file(self, i)
        return result

    property metadata:

        def __get__(self):
            cdef:
                unordered_map[c_string, c_string] metadata
                const CKeyValueMetadata* underlying_metadata
            underlying_metadata = self._metadata.key_value_metadata().get()
            if underlying_metadata != NULL:
                underlying_metadata.ToUnorderedMap(&metadata)
                return metadata
            else:
                return None


cdef class ParquetSchema:
    cdef:
        FileMetaData parent  # the FileMetaData owning the SchemaDescriptor
        const SchemaDescriptor* schema

    def __cinit__(self):
        self.schema = NULL

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

    cdef init_from_filemeta(self, FileMetaData container):
        self.parent = container
        self.schema = container._metadata.schema()

    def __len__(self):
        return self.schema.num_columns()

    def __getitem__(self, i):
        return self.column(i)

    property names:

        def __get__(self):
            return [self[i].name for i in range(len(self))]

    def to_arrow_schema(self):
        """
        Convert Parquet schema to effective Arrow schema

        Returns
        -------
        schema : pyarrow.Schema
        """
        cdef:
            shared_ptr[CSchema] sp_arrow_schema

        with nogil:
            check_status(FromParquetSchema(
                self.schema, self.parent._metadata.key_value_metadata(),
                &sp_arrow_schema))

        return pyarrow_wrap_schema(sp_arrow_schema)

    def equals(self, ParquetSchema other):
        """
        Returns True if the Parquet schemas are equal
        """
        return self.schema.Equals(deref(other.schema))

    def column(self, i):
        if i < 0 or i >= len(self):
            raise IndexError('{0} out of bounds'.format(i))

        cdef ColumnSchema col = ColumnSchema()
        col.init_from_schema(self, i)
        return col


cdef class ColumnSchema:
    cdef:
        ParquetSchema parent
        const ColumnDescriptor* descr

    def __cinit__(self):
        self.descr = NULL

    cdef init_from_schema(self, ParquetSchema schema, int i):
        self.parent = schema
        self.descr = schema.schema.Column(i)

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

    property name:

        def __get__(self):
            return frombytes(self.descr.name())

    property path:

        def __get__(self):
            return frombytes(self.descr.path().get().ToDotString())

    property max_definition_level:

        def __get__(self):
            return self.descr.max_definition_level()

    property max_repetition_level:

        def __get__(self):
            return self.descr.max_repetition_level()

    property physical_type:

        def __get__(self):
            return physical_type_name_from_enum(self.descr.physical_type())

    property logical_type:

        def __get__(self):
            return logical_type_name_from_enum(self.descr.logical_type())

    # FIXED_LEN_BYTE_ARRAY attribute
    property length:

        def __get__(self):
            return self.descr.type_length()

    # Decimal attributes
    property precision:

        def __get__(self):
            return self.descr.type_precision()

    property scale:

        def __get__(self):
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


cdef class ParquetReader:
    cdef:
        object source
        CMemoryPool* allocator
        unique_ptr[FileReader] reader
        column_idx_map
        FileMetaData _metadata

    def __cinit__(self, MemoryPool memory_pool=None):
        self.allocator = maybe_unbox_memory_pool(memory_pool)
        self._metadata = None

    def open(self, object source, FileMetaData metadata=None):
        cdef:
            shared_ptr[RandomAccessFile] rd_handle
            shared_ptr[CFileMetaData] c_metadata
            ReaderProperties properties = default_reader_properties()
            c_string path

        if metadata is not None:
            c_metadata = metadata.sp_metadata

        self.source = source

        get_reader(source, &rd_handle)
        with nogil:
            check_status(OpenFile(rd_handle, self.allocator, properties,
                                  c_metadata, &self.reader))

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

    property num_row_groups:

        def __get__(self):
            return self.reader.get().num_row_groups()

    def set_num_threads(self, int nthreads):
        self.reader.get().set_num_threads(nthreads)

    def read_row_group(self, int i, column_indices=None):
        cdef:
            shared_ptr[CTable] ctable
            vector[int] c_column_indices

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

    def read_all(self, column_indices=None):
        cdef:
            shared_ptr[CTable] ctable
            vector[int] c_column_indices

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

        if self.column_idx_map is None:
            self.column_idx_map = {}
            for i in range(0, metadata.num_columns()):
                col_bytes = tobytes(metadata.schema().Column(i)
                                    .path().get().ToDotString())
                self.column_idx_map[col_bytes] = i

        return self.column_idx_map[tobytes(column_name)]

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
                         .ReadSchemaField(field_index, &carray));

        array.init(carray)
        return array

cdef int check_compression_name(name) except -1:
    if name.upper() not in ['NONE', 'SNAPPY', 'GZIP', 'LZO', 'BROTLI']:
        raise ArrowException("Unsupported compression: " + name)
    return 0


cdef ParquetCompression compression_from_name(str name):
    name = name.upper()
    if name == "SNAPPY":
        return ParquetCompression_SNAPPY
    elif name == "GZIP":
        return ParquetCompression_GZIP
    elif name == "LZO":
        return ParquetCompression_LZO
    elif name == "BROTLI":
        return ParquetCompression_BROTLI
    else:
        return ParquetCompression_UNCOMPRESSED


cdef class ParquetWriter:
    cdef:
        unique_ptr[FileWriter] writer

    cdef readonly:
        object use_dictionary
        object compression
        object version
        int row_group_size

    def __cinit__(self, where, Schema schema, use_dictionary=None,
                  compression=None, version=None,
                  MemoryPool memory_pool=None):
        cdef:
            shared_ptr[FileOutputStream] filestream
            shared_ptr[OutputStream] sink
            shared_ptr[WriterProperties] properties

        if isinstance(where, six.string_types):
            check_status(FileOutputStream.Open(tobytes(where), &filestream))
            sink = <shared_ptr[OutputStream]> filestream
        else:
            get_writer(where, &sink)

        self.use_dictionary = use_dictionary
        self.compression = compression
        self.version = version

        cdef WriterProperties.Builder properties_builder
        self._set_version(&properties_builder)
        self._set_compression_props(&properties_builder)
        self._set_dictionary_props(&properties_builder)
        properties = properties_builder.build()

        check_status(
            FileWriter.Open(deref(schema.schema),
                            maybe_unbox_memory_pool(memory_pool),
                            sink, properties, &self.writer))

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

    def write_table(self, Table table, row_group_size=None):
        cdef CTable* ctable = table.table

        if row_group_size is None or row_group_size == -1:
            row_group_size = ctable.num_rows()
        elif row_group_size == 0:
            raise ValueError('Row group size cannot be 0')

        cdef int c_row_group_size = row_group_size

        with nogil:
            check_status(self.writer.get()
                         .WriteTable(deref(ctable), c_row_group_size))
