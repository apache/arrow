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

import numpy as np

from pyarrow.includes.libarrow cimport *
from pyarrow.includes.common cimport PyObject_to_object
cimport pyarrow.includes.pyarrow as pyarrow

import pyarrow.config

from pyarrow.compat import frombytes, tobytes
from pyarrow.error cimport check_status
from pyarrow.memory cimport MemoryPool, maybe_unbox_memory_pool

cimport pyarrow.scalar as scalar
from pyarrow.scalar import NA

from pyarrow.schema cimport (DataType, Field, Schema, DictionaryType,
                             box_data_type)
import pyarrow.schema as schema

cimport cpython


cdef _pandas():
    import pandas as pd
    return pd


cdef maybe_coerce_datetime64(values, dtype, DataType type,
                             timestamps_to_ms=False):

    from pyarrow.compat import DatetimeTZDtype

    if values.dtype.type != np.datetime64:
        return values, type

    coerce_ms = timestamps_to_ms and values.dtype != 'datetime64[ms]'

    if coerce_ms:
        values = values.astype('datetime64[ms]')

    if isinstance(dtype, DatetimeTZDtype):
        tz = dtype.tz
        unit = 'ms' if coerce_ms else dtype.unit
        type = schema.timestamp(unit, tz)
    else:
        # Trust the NumPy dtype
        type = schema.type_from_numpy_dtype(values.dtype)

    return values, type


cdef class Array:

    cdef init(self, const shared_ptr[CArray]& sp_array):
        self.sp_array = sp_array
        self.ap = sp_array.get()
        self.type = box_data_type(self.sp_array.get().type())

    @staticmethod
    def from_pandas(obj, mask=None, DataType type=None,
                    timestamps_to_ms=False,
                    MemoryPool memory_pool=None):
        """
        Convert pandas.Series to an Arrow Array.

        Parameters
        ----------
        series : pandas.Series or numpy.ndarray

        mask : pandas.Series or numpy.ndarray, optional
            boolean mask if the object is valid or null

        type : pyarrow.DataType
            Explicit type to attempt to coerce to

        timestamps_to_ms : bool, optional
            Convert datetime columns to ms resolution. This is needed for
            compatibility with other functionality like Parquet I/O which
            only supports milliseconds.

        memory_pool: MemoryPool, optional
            Specific memory pool to use to allocate the resulting Arrow array.

        Notes
        -----
        Localized timestamps will currently be returned as UTC (pandas's native
        representation).  Timezone-naive data will be implicitly interpreted as
        UTC.

        Examples
        --------

        >>> import pandas as pd
        >>> import pyarrow as pa
        >>> pa.Array.from_pandas(pd.Series([1, 2]))
        <pyarrow.array.Int64Array object at 0x7f674e4c0e10>
        [
          1,
          2
        ]

        >>> import numpy as np
        >>> pa.Array.from_pandas(pd.Series([1, 2]), np.array([0, 1],
        ... dtype=bool))
        <pyarrow.array.Int64Array object at 0x7f9019e11208>
        [
          1,
          NA
        ]

        Returns
        -------
        pyarrow.array.Array
        """
        cdef:
            shared_ptr[CArray] out
            shared_ptr[CDataType] c_type
            CMemoryPool* pool

        pd = _pandas()

        if mask is not None:
            mask = get_series_values(mask)

        values = get_series_values(obj)
        pool = maybe_unbox_memory_pool(memory_pool)

        if isinstance(values, pd.Categorical):
            return DictionaryArray.from_arrays(
                values.codes, values.categories.values,
                mask=mask, memory_pool=memory_pool)
        elif values.dtype == object:
            # Object dtype undergoes a different conversion path as more type
            # inference may be needed
            if type is not None:
                c_type = type.sp_type
            with nogil:
                check_status(pyarrow.PandasObjectsToArrow(
                    pool, values, mask, c_type, &out))
        else:
            values, type = maybe_coerce_datetime64(
                values, obj.dtype, type, timestamps_to_ms=timestamps_to_ms)

            if type is None:
                check_status(pyarrow.PandasDtypeToArrow(values.dtype, &c_type))
            else:
                c_type = type.sp_type

            with nogil:
                check_status(pyarrow.PandasToArrow(
                    pool, values, mask, c_type, &out))

        return box_array(out)

    @staticmethod
    def from_list(object list_obj, DataType type=None,
                  MemoryPool memory_pool=None):
        """
        Convert Python list to Arrow array

        Parameters
        ----------
        list_obj : array_like

        Returns
        -------
        pyarrow.array.Array
        """
        cdef:
           shared_ptr[CArray] sp_array
           CMemoryPool* pool

        pool = maybe_unbox_memory_pool(memory_pool)
        if type is None:
            check_status(pyarrow.ConvertPySequence(list_obj, pool, &sp_array))
        else:
            raise NotImplementedError()

        return box_array(sp_array)

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

    def equals(Array self, Array other):
        return self.ap.Equals(other.sp_array)

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
                raise IndexError('only slices with step 1 supported')
            else:
                return self.slice(start, stop - start)

        while key < 0:
            key += len(self)

        return self.getitem(key)

    cdef getitem(self, int64_t i):
        return scalar.box_scalar(self.type, self.sp_array, i)

    def slice(self, offset=0, length=None):
        """
        Compute zero-copy slice of this array

        Parameters
        ----------
        offset : int, default 0
            Offset from start of array to slice
        length : int, default None
            Length of slice (default is until end of Array starting from
            offset)

        Returns
        -------
        sliced : RecordBatch
        """
        cdef:
            shared_ptr[CArray] result

        if offset < 0:
            raise IndexError('Offset must be non-negative')

        if length is None:
            result = self.ap.Slice(offset)
        else:
            result = self.ap.Slice(offset, length)

        return box_array(result)

    def to_pandas(self):
        """
        Convert to an array object suitable for use in pandas

        See also
        --------
        Column.to_pandas
        Table.to_pandas
        RecordBatch.to_pandas
        """
        cdef:
            PyObject* out

        with nogil:
            check_status(
                pyarrow.ConvertArrayToPandas(self.sp_array, <PyObject*> self,
                                             &out))
        return wrap_array_output(out)

    def to_pylist(self):
        """
        Convert to an list of native Python objects.
        """
        return [x.as_py() for x in self]


cdef wrap_array_output(PyObject* output):
    cdef object obj = PyObject_to_object(output)

    if isinstance(obj, dict):
        return _pandas().Categorical(obj['indices'],
                                     categories=obj['dictionary'],
                                     fastpath=True)
    else:
        return obj


cdef class NullArray(Array):
    pass


cdef class BooleanArray(Array):
    pass


cdef class NumericArray(Array):
    pass


cdef class IntegerArray(NumericArray):
    pass


cdef class FloatingPointArray(NumericArray):
    pass


cdef class Int8Array(IntegerArray):
    pass


cdef class UInt8Array(IntegerArray):
    pass


cdef class Int16Array(IntegerArray):
    pass


cdef class UInt16Array(IntegerArray):
    pass


cdef class Int32Array(IntegerArray):
    pass


cdef class UInt32Array(IntegerArray):
    pass


cdef class Int64Array(IntegerArray):
    pass


cdef class UInt64Array(IntegerArray):
    pass


cdef class DateArray(NumericArray):
    pass


cdef class TimestampArray(NumericArray):
    pass


cdef class FloatArray(FloatingPointArray):
    pass


cdef class DoubleArray(FloatingPointArray):
    pass


cdef class ListArray(Array):
    pass


cdef class StringArray(Array):
    pass


cdef class BinaryArray(Array):
    pass


cdef class DictionaryArray(Array):

    @staticmethod
    def from_arrays(indices, dictionary, mask=None,
                    MemoryPool memory_pool=None):
        """
        Construct Arrow DictionaryArray from array of indices (must be
        non-negative integers) and corresponding array of dictionary values

        Parameters
        ----------
        indices : ndarray or pandas.Series, integer type
        dictionary : ndarray or pandas.Series
        mask : ndarray or pandas.Series, boolean type
            True values indicate that indices are actually null

        Returns
        -------
        dict_array : DictionaryArray
        """
        cdef:
            Array arrow_indices, arrow_dictionary
            DictionaryArray result
            shared_ptr[CDataType] c_type
            shared_ptr[CArray] c_result

        if mask is None:
            mask = indices == -1
        else:
            mask = mask | (indices == -1)

        arrow_indices = Array.from_pandas(indices, mask=mask,
                                          memory_pool=memory_pool)
        arrow_dictionary = Array.from_pandas(dictionary,
                                             memory_pool=memory_pool)

        if not isinstance(arrow_indices, IntegerArray):
            raise ValueError('Indices must be integer type')

        c_type.reset(new CDictionaryType(arrow_indices.type.sp_type,
                                         arrow_dictionary.sp_array))
        c_result.reset(new CDictionaryArray(c_type, arrow_indices.sp_array))

        result = DictionaryArray()
        result.init(c_result)
        return result


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
    Type_DATE: DateArray,
    Type_FLOAT: FloatArray,
    Type_DOUBLE: DoubleArray,
    Type_LIST: ListArray,
    Type_BINARY: BinaryArray,
    Type_STRING: StringArray,
    Type_TIMESTAMP: TimestampArray,
    Type_DICTIONARY: DictionaryArray
}

cdef object box_array(const shared_ptr[CArray]& sp_array):
    if sp_array.get() == NULL:
        raise ValueError('Array was NULL')

    cdef CDataType* data_type = sp_array.get().type().get()

    if data_type == NULL:
        raise ValueError('Array data type was NULL')

    cdef Array arr = _array_classes[data_type.type]()
    arr.init(sp_array)
    return arr


cdef object get_series_values(object obj):
    import pandas as pd

    if isinstance(obj, pd.Series):
        result = obj.values
    elif isinstance(obj, np.ndarray):
        result = obj
    else:
        result = pd.Series(obj).values

    return result


from_pylist = Array.from_list
from_pandas_series = Array.from_pandas
