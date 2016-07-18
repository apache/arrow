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
from pyarrow.includes.libarrow_io cimport RandomAccessFile, WriteableFile
cimport pyarrow.includes.pyarrow as pyarrow

from pyarrow.compat import tobytes
from pyarrow.error import ArrowException
from pyarrow.error cimport check_cstatus
from pyarrow.io import NativeFileInterface
from pyarrow.table cimport Table

from pyarrow.io cimport NativeFileInterface

import six


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

    cdef open_native_file(self, NativeFileInterface file):
        cdef shared_ptr[RandomAccessFile] cpp_handle
        file.read_handle(&cpp_handle)

        check_cstatus(OpenFile(cpp_handle, &self.allocator, &self.reader))

    def read_all(self):
        cdef:
            Table table = Table()
            shared_ptr[CTable] ctable

        with nogil:
            check_cstatus(self.reader.get()
                          .ReadFlatTable(&ctable))

        table.init(ctable)
        return table


def read_table(source, columns=None):
    """
    Read a Table from Parquet format
    Returns
    -------
    table: pyarrow.Table
    """
    cdef ParquetReader reader = ParquetReader()

    if isinstance(source, six.string_types):
        reader.open_local_file(source)
    elif isinstance(source, NativeFileInterface):
        reader.open_native_file(source)

    return reader.read_all()


def write_table(table, filename, chunk_size=None, version=None):
    """
    Write a Table to Parquet format

    Parameters
    ----------
    table : pyarrow.Table
    filename : string
    chunk_size : int
        The maximum number of rows in each Parquet RowGroup
    version : {"1.0", "2.0"}, default "1.0"
        The Parquet format version, defaults to 1.0
    """
    cdef Table table_ = table
    cdef CTable* ctable_ = table_.table
    cdef shared_ptr[OutputStream] sink
    cdef WriterProperties.Builder properties_builder
    cdef int64_t chunk_size_ = 0
    if chunk_size is None:
        chunk_size_ = min(ctable_.num_rows(), int(2**16))
    else:
        chunk_size_ = chunk_size

    if version is not None:
        if version == "1.0":
            properties_builder.version(PARQUET_1_0)
        elif version == "2.0":
            properties_builder.version(PARQUET_2_0)
        else:
            raise ArrowException("Unsupported Parquet format version")

    sink.reset(new LocalFileOutputStream(tobytes(filename)))
    with nogil:
        check_cstatus(WriteFlatTable(ctable_, default_memory_pool(), sink,
            chunk_size_, properties_builder.build()))
