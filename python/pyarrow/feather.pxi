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

# ---------------------------------------------------------------------
# Implement legacy Feather file format


class FeatherError(Exception):
    pass


cdef class FeatherWriter:
    cdef:
        unique_ptr[CFeatherWriter] writer

    cdef public:
        int64_t num_rows

    def __cinit__(self):
        self.num_rows = -1

    def open(self, object dest):
        cdef shared_ptr[OutputStream] sink
        get_writer(dest, &sink)

        with nogil:
            check_status(CFeatherWriter.Open(sink, &self.writer))

    def close(self):
        if self.num_rows < 0:
            self.num_rows = 0
        self.writer.get().SetNumRows(self.num_rows)
        with nogil:
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
        unique_ptr[CFeatherReader] reader

    def __cinit__(self):
        pass

    def open(self, source):
        cdef shared_ptr[RandomAccessFile] reader
        get_reader(source, &reader)

        with nogil:
            check_status(CFeatherReader.Open(reader, &self.reader))

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

        return pyarrow_wrap_column(sp_column)
