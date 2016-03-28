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

from pyarrow.compat import frombytes, tobytes
from pyarrow.error cimport check_status

cimport pyarrow.scalar as scalar
from pyarrow.scalar import NA

from pyarrow.schema cimport Schema
import pyarrow.schema as schema

def total_allocated_bytes():
    cdef MemoryPool* pool = pyarrow.GetMemoryPool()
    return pool.bytes_allocated()


cdef class Array:

    cdef init(self, const shared_ptr[CArray]& sp_array):
        self.sp_array = sp_array
        self.ap = sp_array.get()
        self.type = DataType()
        self.type.init(self.sp_array.get().type())

    @staticmethod
    def from_pandas(obj, mask=None):
        return from_pandas_series(obj, mask)

    property null_count:

        def __get__(self):
            return self.sp_array.get().null_count()

    def __iter__(self):
        for i in range(len(self)):
            yield self.getitem(i)
        raise StopIteration

    def __repr__(self):
        from pyarrow.formatting import array_format
        type_format = object.__repr__(self)
        values = array_format(self, window=10)
        return '{0}\n{1}'.format(type_format, values)

    def __len__(self):
        if self.sp_array.get():
            return self.sp_array.get().length()
        else:
            return 0

    def isnull(self):
        raise NotImplemented

    def __getitem__(self, key):
        cdef:
            Py_ssize_t n = len(self)

        if PySlice_Check(key):
            start = key.start or 0
            while start < 0:
                start += n

            stop = key.stop if key.stop is not None else n
            while stop < 0:
                stop += n

            step = key.step or 1
            if step != 1:
                raise NotImplementedError
            else:
                return self.slice(start, stop)

        while key < 0:
            key += len(self)

        return self.getitem(key)

    cdef getitem(self, int i):
        return scalar.box_arrow_scalar(self.type, self.sp_array, i)

    def slice(self, start, end):
        pass


cdef class NullArray(Array):
    pass


cdef class BooleanArray(Array):
    pass


cdef class NumericArray(Array):
    pass


cdef class Int8Array(NumericArray):
    pass


cdef class UInt8Array(NumericArray):
    pass


cdef class Int16Array(NumericArray):
    pass


cdef class UInt16Array(NumericArray):
    pass


cdef class Int32Array(NumericArray):
    pass


cdef class UInt32Array(NumericArray):
    pass


cdef class Int64Array(NumericArray):
    pass


cdef class UInt64Array(NumericArray):
    pass


cdef class FloatArray(NumericArray):
    pass


cdef class DoubleArray(NumericArray):
    pass


cdef class ListArray(Array):
    pass


cdef class StringArray(Array):
    pass


cdef dict _array_classes = {
    Type_NA: NullArray,
    Type_BOOL: BooleanArray,
    Type_UINT8: UInt8Array,
    Type_UINT16: UInt16Array,
    Type_UINT32: UInt32Array,
    Type_UINT64: UInt64Array,
    Type_INT8: Int8Array,
    Type_INT16: Int16Array,
    Type_INT32: Int32Array,
    Type_INT64: Int64Array,
    Type_FLOAT: FloatArray,
    Type_DOUBLE: DoubleArray,
    Type_LIST: ListArray,
    Type_STRING: StringArray,
}

cdef object box_arrow_array(const shared_ptr[CArray]& sp_array):
    if sp_array.get() == NULL:
        raise ValueError('Array was NULL')

    cdef CDataType* data_type = sp_array.get().type().get()

    if data_type == NULL:
        raise ValueError('Array data type was NULL')

    cdef Array arr = _array_classes[data_type.type]()
    arr.init(sp_array)
    return arr


def from_pylist(object list_obj, DataType type=None):
    """
    Convert Python list to Arrow array
    """
    cdef:
        shared_ptr[CArray] sp_array

    if type is None:
        check_status(pyarrow.ConvertPySequence(list_obj, &sp_array))
    else:
        raise NotImplementedError

    return box_arrow_array(sp_array)


def from_pandas_series(object series, object mask=None):
    cdef:
        shared_ptr[CArray] out

    series_values = series_as_ndarray(series)

    if mask is None:
        check_status(pyarrow.PandasToArrow(pyarrow.GetMemoryPool(),
                                           series_values, &out))
    else:
        mask = series_as_ndarray(mask)
        check_status(pyarrow.PandasMaskedToArrow(
            pyarrow.GetMemoryPool(), series_values, mask, &out))

    return box_arrow_array(out)


def from_pandas_dataframe(object df, name=None):
    cdef:
        list names = []
        list arrays = []

    for name in df.columns:
        col = df[name]
        arr = from_pandas_series(col)

        names.append(name)
        arrays.append(arr)

    return Table.from_arrays(names, arrays, name=name)


cdef object series_as_ndarray(object obj):
    import pandas as pd

    if isinstance(obj, pd.Series):
        result = obj.values
    else:
        result = obj

    return result

#----------------------------------------------------------------------
# Table-like data structures

cdef class RowBatch:
    """

    """
    cdef readonly:
        Schema schema
        int num_rows
        list arrays

    def __cinit__(self, Schema schema, int num_rows, list arrays):
        self.schema = schema
        self.num_rows = num_rows
        self.arrays = arrays

        if len(self.schema) != len(arrays):
            raise ValueError('Mismatch number of data arrays and '
                             'schema fields')

    def __len__(self):
        return self.num_rows

    property num_columns:

        def __get__(self):
            return len(self.arrays)

    def __getitem__(self, i):
        return self.arrays[i]


cdef class Table:
    '''
    Do not call this class's constructor directly.
    '''
    cdef:
        shared_ptr[CTable] sp_table
        CTable* table

    def __cinit__(self):
        pass

    cdef init(self, const shared_ptr[CTable]& table):
        self.sp_table = table
        self.table = table.get()

    @staticmethod
    def from_pandas(df, name=None):
        pass

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

        import pandas as pd

        names = []
        data = []
        for i in range(self.table.num_columns()):
            col = self.table.column(i)
            check_status(pyarrow.ArrowToPandas(col, &arr))
            names.append(frombytes(col.get().name()))
            data.append(<object> arr)

        return pd.DataFrame(dict(zip(names, data)), columns=names)
