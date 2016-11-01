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

from pyarrow.includes.libarrow cimport *
from pyarrow.includes.parquet cimport *
from pyarrow.includes.libarrow_io cimport ReadableFileInterface
cimport pyarrow.includes.pyarrow as pyarrow

from pyarrow.compat import tobytes
from pyarrow.error import ArrowException
from pyarrow.error cimport check_status
from pyarrow.io import NativeFile
from pyarrow.table cimport Table

from pyarrow.io cimport NativeFile

import six

__all__ = [
    'read_table',
    'write_table'
]

cdef class ParquetReader:
    cdef:
        ParquetAllocator allocator
        unique_ptr[FileReader] reader

    def __cinit__(self):
        self.allocator.set_pool(default_memory_pool())

    cdef open_local_file(self, file_path):
        cdef c_string path = tobytes(file_path)

        # Must be in one expression to avoid calling std::move which is not
        # possible in Cython (due to missing rvalue support)

        # TODO(wesm): ParquetFileReader::OpenFIle can throw?
        self.reader = unique_ptr[FileReader](
            new FileReader(default_memory_pool(),
                           ParquetFileReader.OpenFile(path)))

    cdef open_native_file(self, NativeFile file):
        cdef shared_ptr[ReadableFileInterface] cpp_handle
        file.read_handle(&cpp_handle)

        check_status(OpenFile(cpp_handle, &self.allocator, &self.reader))

    def read_all(self):
        cdef:
            Table table = Table()
            shared_ptr[CTable] ctable

        with nogil:
            check_status(self.reader.get()
                         .ReadFlatTable(&ctable))

        table.init(ctable)
        return table


def read_table(source, columns=None):
    """
    Read a Table from Parquet format

    Returns
    -------
    pyarrow.table.Table
        Content of the file as a table (of columns)
    """
    cdef ParquetReader reader = ParquetReader()

    if isinstance(source, six.string_types):
        reader.open_local_file(source)
    elif isinstance(source, NativeFile):
        reader.open_native_file(source)

    return reader.read_all()


def write_table(table, filename, chunk_size=None, version=None,
                use_dictionary=True, compression=None):
    """
    Write a Table to Parquet format

    Parameters
    ----------
    table : pyarrow.Table
    filename : string
    chunk_size : int
        The maximum number of rows in each Parquet RowGroup. As a default,
        we will write a single RowGroup per file.
    version : {"1.0", "2.0"}, default "1.0"
        The Parquet format version, defaults to 1.0
    use_dictionary : bool or list
        Specify if we should use dictionary encoding in general or only for
        some columns.
    compression : str or dict
        Specify the compression codec, either on a general basis or per-column.
    """
    cdef Table table_ = table
    cdef CTable* ctable_ = table_.table
    cdef shared_ptr[ParquetOutputStream] sink
    cdef WriterProperties.Builder properties_builder
    cdef int64_t chunk_size_ = 0
    if chunk_size is None:
        chunk_size_ = ctable_.num_rows()
    else:
        chunk_size_ = chunk_size

    if version is not None:
        if version == "1.0":
            properties_builder.version(PARQUET_1_0)
        elif version == "2.0":
            properties_builder.version(PARQUET_2_0)
        else:
            raise ArrowException("Unsupported Parquet format version")

    if isinstance(use_dictionary, bool):
        if use_dictionary:
            properties_builder.enable_dictionary()
        else:
            properties_builder.disable_dictionary()
    else:
        # Deactivate dictionary encoding by default
        properties_builder.disable_dictionary()
        for column in use_dictionary:
            properties_builder.enable_dictionary(column)

    if isinstance(compression, basestring):
        if compression == "NONE":
            properties_builder.compression(UNCOMPRESSED)
        elif compression == "SNAPPY":
            properties_builder.compression(SNAPPY)
        elif compression == "GZIP":
            properties_builder.compression(GZIP)
        elif compression == "LZO":
            properties_builder.compression(LZO)
        elif compression == "BROTLI":
            properties_builder.compression(BROTLI)
        else:
            raise ArrowException("Unsupport compression codec")
    elif compression is not None:
        # Deactivate dictionary encoding by default
        properties_builder.disable_dictionary()
        for column, codec in compression.iteritems():
            if codec == "NONE":
                properties_builder.compression(column, UNCOMPRESSED)
            elif codec == "SNAPPY":
                properties_builder.compression(column, SNAPPY)
            elif codec == "GZIP":
                properties_builder.compression(column, GZIP)
            elif codec == "LZO":
                properties_builder.compression(column, LZO)
            elif codec == "BROTLI":
                properties_builder.compression(column, BROTLI)
            else:
                raise ArrowException("Unsupport compression codec")

    sink.reset(new LocalFileOutputStream(tobytes(filename)))
    with nogil:
        check_status(WriteFlatTable(ctable_, default_memory_pool(), sink,
                                    chunk_size_, properties_builder.build()))
