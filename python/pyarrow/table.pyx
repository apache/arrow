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
cimport pyarrow.includes.pyarrow as pyarrow

import pyarrow.config

from pyarrow.array cimport Array, box_arrow_array
from pyarrow.error cimport check_status
from pyarrow.schema cimport box_data_type, box_schema

from pyarrow.compat import frombytes, tobytes


cdef class ChunkedArray:
    '''
    Do not call this class's constructor directly.
    '''

    def __cinit__(self):
        self.chunked_array = NULL

    cdef init(self, const shared_ptr[CChunkedArray]& chunked_array):
        self.sp_chunked_array = chunked_array
        self.chunked_array = chunked_array.get()

    cdef _check_nullptr(self):
        if self.chunked_array == NULL:
            raise ReferenceError("ChunkedArray object references a NULL pointer."
                    "Not initialized.")

    def length(self):
        self._check_nullptr()
        return self.chunked_array.length()

    def __len__(self):
        return self.length()

    property null_count:

        def __get__(self):
            self._check_nullptr()
            return self.chunked_array.null_count()

    property num_chunks:

        def __get__(self):
            self._check_nullptr()
            return self.chunked_array.num_chunks()

    def chunk(self, i):
        self._check_nullptr()
        return box_arrow_array(self.chunked_array.chunk(i))


    def iterchunks(self):
        for i in range(self.num_chunks):
            yield self.chunk(i)


cdef class Column:
    '''
    Do not call this class's constructor directly.
    '''

    def __cinit__(self):
        self.column = NULL

    cdef init(self, const shared_ptr[CColumn]& column):
        self.sp_column = column
        self.column = column.get()

    def to_pandas(self):
        """
        Convert the arrow::Column to a pandas Series
        """
        cdef:
            PyObject* arr

        import pandas as pd

        check_status(pyarrow.ArrowToPandas(self.sp_column, self, &arr))
        return pd.Series(<object>arr, name=self.name)

    cdef _check_nullptr(self):
        if self.column == NULL:
            raise ReferenceError("Column object references a NULL pointer."
                    "Not initialized.")

    def __len__(self):
        self._check_nullptr()
        return self.column.length()

    def length(self):
        self._check_nullptr()
        return self.column.length()

    property shape:

        def __get__(self):
            self._check_nullptr()
            return (self.length(),)

    property null_count:

        def __get__(self):
            self._check_nullptr()
            return self.column.null_count()

    property name:

        def __get__(self):
            return frombytes(self.column.name())

    property type:

        def __get__(self):
            return box_data_type(self.column.type())

    property data:

        def __get__(self):
            cdef ChunkedArray chunked_array = ChunkedArray()
            chunked_array.init(self.column.data())
            return chunked_array


cdef class Table:
    '''
    Do not call this class's constructor directly.
    '''

    def __cinit__(self):
        self.table = NULL

    cdef init(self, const shared_ptr[CTable]& table):
        self.sp_table = table
        self.table = table.get()

    cdef _check_nullptr(self):
        if self.table == NULL:
            raise ReferenceError("Table object references a NULL pointer."
                    "Not initialized.")

    @staticmethod
    def from_pandas(df, name=None):
        return from_pandas_dataframe(df, name=name)

    @staticmethod
    def from_arrays(names, arrays, name=None):
        cdef:
            Array arr
            Table result
            c_string c_name
            vector[shared_ptr[CField]] fields
            vector[shared_ptr[CColumn]] columns
            shared_ptr[CSchema] schema
            shared_ptr[CTable] table

        cdef int K = len(arrays)

        fields.resize(K)
        columns.resize(K)
        for i in range(K):
            arr = arrays[i]
            c_name = tobytes(names[i])

            fields[i].reset(new CField(c_name, arr.type.sp_type, True))
            columns[i].reset(new CColumn(fields[i], arr.sp_array))

        if name is None:
            c_name = ''
        else:
            c_name = tobytes(name)

        schema.reset(new CSchema(fields))
        table.reset(new CTable(c_name, schema, columns))

        result = Table()
        result.init(table)

        return result

    def to_pandas(self):
        """
        Convert the arrow::Table to a pandas DataFrame
        """
        cdef:
            PyObject* arr
            shared_ptr[CColumn] col
            Column column

        import pandas as pd

        names = []
        data = []
        for i in range(self.table.num_columns()):
            col = self.table.column(i)
            column = self.column(i)
            check_status(pyarrow.ArrowToPandas(col, column, &arr))
            names.append(frombytes(col.get().name()))
            data.append(<object> arr)

        return pd.DataFrame(dict(zip(names, data)), columns=names)

    property name:

        def __get__(self):
            self._check_nullptr()
            return frombytes(self.table.name())

    property schema:

        def __get__(self):
            raise box_schema(self.table.schema())

    def column(self, index):
        self._check_nullptr()
        cdef Column column = Column()
        column.init(self.table.column(index))
        return column

    def __getitem__(self, i):
        return self.column(i)

    def itercolumns(self):
        for i in range(self.num_columns):
            yield self.column(i)

    property num_columns:

        def __get__(self):
            self._check_nullptr()
            return self.table.num_columns()

    property num_rows:

        def __get__(self):
            self._check_nullptr()
            return self.table.num_rows()

    def __len__(self):
        return self.num_rows

    property shape:

        def __get__(self):
            return (self.num_rows, self.num_columns)



def from_pandas_dataframe(object df, name=None, timestamps_to_ms=False):
    """
    Convert pandas.DataFrame to an Arrow Table

    Parameters
    ----------
    df: pandas.DataFrame

    name: str

    timestamps_to_ms: bool
        Convert datetime columns to ms resolution. This is needed for
        compability with other functionality like Parquet I/O which
        only supports milliseconds.
    """
    from pyarrow.array import from_pandas_series

    cdef:
        list names = []
        list arrays = []

    for name in df.columns:
        col = df[name]
        arr = from_pandas_series(col, timestamps_to_ms=timestamps_to_ms)

        names.append(name)
        arrays.append(arr)

    return Table.from_arrays(names, arrays, name=name)
