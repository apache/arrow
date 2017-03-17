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
from pyarrow.includes.libarrow cimport CArray, CColumn, CSchema, CStatus
from pyarrow.includes.libarrow_io cimport RandomAccessFile, OutputStream

from libcpp.string cimport string
from libcpp cimport bool as c_bool

cimport cpython

from pyarrow.compat import frombytes, tobytes, encode_file_path

from pyarrow.array cimport Array
from pyarrow.error cimport check_status
from pyarrow.table cimport Column

cdef extern from "arrow/ipc/feather.h" namespace "arrow::ipc::feather" nogil:

    cdef cppclass TableWriter:
        @staticmethod
        CStatus Open(const shared_ptr[OutputStream]& stream,
                     unique_ptr[TableWriter]* out)

        @staticmethod
        CStatus OpenFile(const string& abspath, unique_ptr[TableWriter]* out)

        void SetDescription(const string& desc)
        void SetNumRows(int64_t num_rows)

        CStatus Append(const string& name, const CArray& values)
        CStatus Finalize()

    cdef cppclass TableReader:
        TableReader(const shared_ptr[RandomAccessFile]& source)

        @staticmethod
        CStatus OpenFile(const string& abspath, unique_ptr[TableReader]* out)

        string GetDescription()
        c_bool HasDescription()

        int64_t num_rows()
        int64_t num_columns()

        shared_ptr[CSchema] schema()

        CStatus GetColumn(int i, shared_ptr[CColumn]* out)
        c_string GetColumnName(int i)


class FeatherError(Exception):
    pass


cdef class FeatherWriter:
    cdef:
        unique_ptr[TableWriter] writer

    cdef public:
        int64_t num_rows

    def __cinit__(self):
        self.num_rows = -1

    def open(self, object dest):
        cdef:
            string c_name = encode_file_path(dest)

        check_status(TableWriter.OpenFile(c_name, &self.writer))

    def close(self):
        if self.num_rows < 0:
            self.num_rows = 0
        self.writer.get().SetNumRows(self.num_rows)
        check_status(self.writer.get().Finalize())

    def write_array(self, object name, object col, object mask=None):
        cdef Array arr

        if self.num_rows >= 0:
            if len(col) != self.num_rows:
                raise ValueError('prior column had a different number of rows')
        else:
            self.num_rows = len(col)

        if isinstance(col, Array):
            arr = col
        else:
            arr = Array.from_pandas(col, mask=mask)

        cdef c_string c_name = tobytes(name)

        with nogil:
            check_status(
                self.writer.get().Append(c_name, deref(arr.sp_array)))


cdef class FeatherReader:
    cdef:
        unique_ptr[TableReader] reader

    def __cinit__(self):
        pass

    def open(self, source):
        cdef:
            string c_name = encode_file_path(source)

        check_status(TableReader.OpenFile(c_name, &self.reader))

    property num_rows:

        def __get__(self):
            return self.reader.get().num_rows()

    property num_columns:

        def __get__(self):
            return self.reader.get().num_columns()

    def get_column_name(self, int i):
        cdef c_string name = self.reader.get().GetColumnName(i)
        return frombytes(name)

    def get_column(self, int i):
        if i < 0 or i >= self.num_columns:
            raise IndexError(i)

        cdef shared_ptr[CColumn] sp_column
        with nogil:
            check_status(self.reader.get()
                         .GetColumn(i, &sp_column))

        cdef Column col = Column()
        col.init(sp_column)
        return col
