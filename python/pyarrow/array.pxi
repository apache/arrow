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


cdef _sequence_to_array(object sequence, object size, DataType type,
                        CMemoryPool* pool):
    cdef shared_ptr[CArray] out
    cdef int64_t c_size
    if type is None:
        if size is None:
            with nogil:
                check_status(ConvertPySequence(sequence, pool, &out))
        else:
            c_size = size
            with nogil:
                check_status(
                    ConvertPySequence(sequence, c_size, pool, &out)
                )
    else:
        if size is None:
            with nogil:
                check_status(
                    ConvertPySequence(
                        sequence, type.sp_type, pool, &out,
                    )
                )
        else:
            c_size = size
            with nogil:
                check_status(
                    ConvertPySequence(
                        sequence, c_size, type.sp_type, pool, &out,
                    )
                )

    return pyarrow_wrap_array(out)


cdef _is_array_like(obj):
    try:
        import pandas
        return isinstance(obj, (np.ndarray, pd.Series, pd.Index, Categorical))
    except ImportError:
        return isinstance(obj, np.ndarray)


cdef _ndarray_to_array(object values, object mask, DataType type,
                       c_bool use_pandas_null_sentinels,
                       CMemoryPool* pool):
    cdef shared_ptr[CChunkedArray] chunked_out
    cdef shared_ptr[CDataType] c_type

    dtype = values.dtype

    if type is None and dtype != object:
        with nogil:
            check_status(NumPyDtypeToArrow(dtype, &c_type))

    if type is not None:
        c_type = type.sp_type

    with nogil:
        check_status(NdarrayToArrow(pool, values, mask,
                                    use_pandas_null_sentinels,
                                    c_type, &chunked_out))

    if chunked_out.get().num_chunks() > 1:
        return pyarrow_wrap_chunked_array(chunked_out)
    else:
        return pyarrow_wrap_array(chunked_out.get().chunk(0))


cdef inline DataType _ensure_type(object type):
    if type is None:
        return None
    elif not isinstance(type, DataType):
        return type_for_alias(type)
    else:
        return type


def array(object obj, type=None, mask=None,
          MemoryPool memory_pool=None, size=None,
          from_pandas=False):
    """
    Create pyarrow.Array instance from a Python object

    Parameters
    ----------
    obj : sequence, iterable, ndarray or Series
        If both type and size are specified may be a single use iterable. If
        not strongly-typed, Arrow type will be inferred for resulting array
    mask : array (boolean), optional
        Indicate which values are null (True) or not null (False).
    type : pyarrow.DataType
        Explicit type to attempt to coerce to, otherwise will be inferred from
        the data
    memory_pool : pyarrow.MemoryPool, optional
        If not passed, will allocate memory from the currently-set default
        memory pool

    size : int64, optional
        Size of the elements. If the imput is larger than size bail at this
        length. For iterators, if size is larger than the input iterator this
        will be treated as a "max size", but will involve an initial allocation
        of size followed by a resize to the actual size (so if you know the
        exact size specifying it correctly will give you better performance).
    from_pandas : boolean, default False
        Use pandas's semantics for inferring nulls from values in ndarray-like
        data. If passed, the mask tasks precendence, but if a value is unmasked
        (not-null), but still null according to pandas semantics, then it is
        null

    Notes
    -----
    Localized timestamps will currently be returned as UTC (pandas's native
    representation).  Timezone-naive data will be implicitly interpreted as
    UTC.

    Examples
    --------
    >>> import pandas as pd
    >>> import pyarrow as pa
    >>> pa.array(pd.Series([1, 2]))
    <pyarrow.array.Int64Array object at 0x7f674e4c0e10>
    [
      1,
      2
    ]

    >>> import numpy as np
    >>> pa.array(pd.Series([1, 2]), np.array([0, 1],
    ... dtype=bool))
    <pyarrow.array.Int64Array object at 0x7f9019e11208>
    [
      1,
      NA
    ]

    Returns
    -------
    array : pyarrow.Array or pyarrow.ChunkedArray (if object data
    overflowed binary storage)
    """
    type = _ensure_type(type)
    cdef CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)

    if _is_array_like(obj):
        if mask is not None:
            mask = get_series_values(mask)

        values = get_series_values(obj)

        if isinstance(values, Categorical):
            return DictionaryArray.from_arrays(
                values.codes, values.categories.values,
                mask=mask, ordered=values.ordered,
                from_pandas=from_pandas,
                memory_pool=memory_pool)
        else:
            values, type = pdcompat.get_datetimetz_type(values, obj.dtype,
                                                        type)
            return _ndarray_to_array(values, mask, type, from_pandas, pool)
    else:
        if mask is not None:
            raise ValueError("Masks only supported with ndarray-like inputs")
        return _sequence_to_array(obj, size, type, pool)


def asarray(values, type=None):
    """
    Convert to pyarrow.Array, inferring type if not provided. Attempt to cast
    if indicated type is different

    Parameters
    ----------
    values : array-like (sequence, numpy.ndarray, pyarrow.Array)
    type : string or DataType

    Returns
    -------
    arr : Array
    """
    if isinstance(values, Array):
        if type is not None and not values.type.equals(type):
            values = values.cast(type)

        return values
    else:
        return array(values, type=type)


def _normalize_slice(object arrow_obj, slice key):
    cdef Py_ssize_t n = len(arrow_obj)

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
        return arrow_obj.slice(start, stop - start)


cdef class _FunctionContext:
    cdef:
        unique_ptr[CFunctionContext] ctx

    def __cinit__(self):
        self.ctx.reset(new CFunctionContext(c_default_memory_pool()))

cdef _FunctionContext _global_ctx = _FunctionContext()

cdef CFunctionContext* _context() nogil:
    return _global_ctx.ctx.get()


cdef wrap_datum(const CDatum& datum):
    if datum.kind() == DatumType_ARRAY:
        return pyarrow_wrap_array(MakeArray(datum.array()))
    elif datum.kind() == DatumType_CHUNKED_ARRAY:
        return pyarrow_wrap_chunked_array(datum.chunked_array())
    else:
        raise ValueError("Unable to wrap Datum in a Python object")


cdef class Array:

    cdef void init(self, const shared_ptr[CArray]& sp_array):
        self.sp_array = sp_array
        self.ap = sp_array.get()
        self.type = pyarrow_wrap_data_type(self.sp_array.get().type())

    def _debug_print(self):
        with nogil:
            check_status(DebugPrint(deref(self.ap), 0))

    def cast(self, object target_type, safe=True):
        """
        Cast array values to another data type

        Parameters
        ----------
        target_type : DataType
            Type to cast to
        safe : boolean, default True
            Check for overflows or other unsafe conversions

        Returns
        -------
        casted : Array
        """
        cdef:
            CCastOptions options
            shared_ptr[CArray] result
            DataType type

        type = _ensure_type(target_type)

        options.allow_int_overflow = not safe
        options.allow_time_truncate = not safe

        with nogil:
            check_status(Cast(_context(), self.ap[0], type.sp_type,
                              options, &result))

        return pyarrow_wrap_array(result)

    def unique(self):
        """
        Compute distinct elements in array
        """
        cdef shared_ptr[CArray] result

        with nogil:
            check_status(Unique(_context(), CDatum(self.sp_array), &result))

        return pyarrow_wrap_array(result)

    def dictionary_encode(self):
        """
        Compute dictionary-encoded representation of array
        """
        cdef CDatum out

        with nogil:
            check_status(DictionaryEncode(_context(), CDatum(self.sp_array),
                                          &out))

        return wrap_datum(out)

    @staticmethod
    def from_pandas(obj, mask=None, type=None, MemoryPool memory_pool=None):
        """
        Convert pandas.Series to an Arrow Array, using pandas's semantics about
        what values indicate nulls. See pyarrow.array for more general
        conversion from arrays or sequences to Arrow arrays

        Parameters
        ----------
        sequence : ndarray, Inded Series
        mask : array (boolean), optional
            Indicate which values are null (True) or not null (False)
        type : pyarrow.DataType
            Explicit type to attempt to coerce to, otherwise will be inferred
            from the data
        memory_pool : pyarrow.MemoryPool, optional
            If not passed, will allocate memory from the currently-set default
            memory pool

        Notes
        -----
        Localized timestamps will currently be returned as UTC (pandas's native
        representation).  Timezone-naive data will be implicitly interpreted as
        UTC.

        Returns
        -------
        array : pyarrow.Array or pyarrow.ChunkedArray (if object data
        overflows binary buffer)
        """
        return array(obj, mask=mask, type=type, memory_pool=memory_pool,
                     from_pandas=True)

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
        return self.ap.Equals(deref(other.ap))

    def __len__(self):
        if self.sp_array.get():
            return self.sp_array.get().length()
        else:
            return 0

    def isnull(self):
        raise NotImplemented

    def __getitem__(self, key):
        cdef Py_ssize_t n = len(self)

        if PySlice_Check(key):
            return _normalize_slice(self, key)

        while key < 0:
            key += len(self)

        return self.getitem(key)

    cdef getitem(self, int64_t i):
        return box_scalar(self.type, self.sp_array, i)

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

        return pyarrow_wrap_array(result)

    def to_pandas(self, c_bool strings_to_categorical=False,
                  zero_copy_only=False):
        """
        Convert to an array object suitable for use in pandas

        Parameters
        ----------
        strings_to_categorical : boolean, default False
            Encode string (UTF8) and binary types to pandas.Categorical
        zero_copy_only : boolean, default False
            Raise an ArrowException if this function call would require copying
            the underlying data

        See also
        --------
        Column.to_pandas
        Table.to_pandas
        RecordBatch.to_pandas
        """
        cdef:
            PyObject* out
            PandasOptions options

        options = PandasOptions(
            strings_to_categorical=strings_to_categorical,
            zero_copy_only=zero_copy_only)
        with nogil:
            check_status(ConvertArrayToPandas(options, self.sp_array,
                                              self, &out))
        return wrap_array_output(out)

    def to_pylist(self):
        """
        Convert to an list of native Python objects.
        """
        return [x.as_py() for x in self]

    def validate(self):
        """
        Perform any validation checks implemented by
        arrow::ValidateArray. Raises exception with error message if array does
        not validate

        Raises
        ------
        ArrowInvalid
        """
        with nogil:
            check_status(ValidateArray(deref(self.ap)))


cdef class Tensor:

    cdef void init(self, const shared_ptr[CTensor]& sp_tensor):
        self.sp_tensor = sp_tensor
        self.tp = sp_tensor.get()
        self.type = pyarrow_wrap_data_type(self.tp.type())

    def __repr__(self):
        return """<pyarrow.Tensor>
type: {0}
shape: {1}
strides: {2}""".format(self.type, self.shape, self.strides)

    @staticmethod
    def from_numpy(obj):
        cdef shared_ptr[CTensor] ctensor
        with nogil:
            check_status(NdarrayToTensor(c_default_memory_pool(), obj,
                                         &ctensor))
        return pyarrow_wrap_tensor(ctensor)

    def to_numpy(self):
        """
        Convert arrow::Tensor to numpy.ndarray with zero copy
        """
        cdef:
            PyObject* out

        with nogil:
            check_status(TensorToNdarray(deref(self.tp), self, &out))
        return PyObject_to_object(out)

    def equals(self, Tensor other):
        """
        Return true if the tensors contains exactly equal data
        """
        return self.tp.Equals(deref(other.tp))

    property is_mutable:

        def __get__(self):
            return self.tp.is_mutable()

    property is_contiguous:

        def __get__(self):
            return self.tp.is_contiguous()

    property ndim:

        def __get__(self):
            return self.tp.ndim()

    property size:

        def __get__(self):
            return self.tp.size()

    property shape:

        def __get__(self):
            cdef size_t i
            py_shape = []
            for i in range(self.tp.shape().size()):
                py_shape.append(self.tp.shape()[i])
            return py_shape

    property strides:

        def __get__(self):
            cdef size_t i
            py_strides = []
            for i in range(self.tp.strides().size()):
                py_strides.append(self.tp.strides()[i])
            return py_strides


cdef wrap_array_output(PyObject* output):
    cdef object obj = PyObject_to_object(output)

    if isinstance(obj, dict):
        return Categorical(obj['indices'],
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


cdef class Date32Array(NumericArray):
    pass


cdef class Date64Array(NumericArray):
    pass


cdef class TimestampArray(NumericArray):
    pass


cdef class Time32Array(NumericArray):
    pass


cdef class Time64Array(NumericArray):
    pass


cdef class FloatArray(FloatingPointArray):
    pass


cdef class DoubleArray(FloatingPointArray):
    pass


cdef class FixedSizeBinaryArray(Array):
    pass


cdef class Decimal128Array(FixedSizeBinaryArray):
    pass


cdef class ListArray(Array):

    @staticmethod
    def from_arrays(offsets, values, MemoryPool pool=None):
        """
        Construct ListArray from arrays of int32 offsets and values

        Parameters
        ----------
        offset : Array (int32 type)
        values : Array (any type)

        Returns
        -------
        list_array : ListArray
        """
        cdef:
            Array _offsets, _values
            shared_ptr[CArray] out
        cdef CMemoryPool* cpool = maybe_unbox_memory_pool(pool)

        _offsets = asarray(offsets, type='int32')
        _values = asarray(values)

        with nogil:
            check_status(CListArray.FromArrays(_offsets.ap[0], _values.ap[0],
                                               cpool, &out))
        return pyarrow_wrap_array(out)


cdef class UnionArray(Array):

    @staticmethod
    def from_dense(Array types, Array value_offsets, list children):
        """
        Construct dense UnionArray from arrays of int8 types, int32 offsets and
        children arrays

        Parameters
        ----------
        types : Array (int8 type)
        value_offsets : Array (int32 type)
        children : list

        Returns
        -------
        union_array : UnionArray
        """
        cdef shared_ptr[CArray] out
        cdef vector[shared_ptr[CArray]] c
        cdef Array child
        for child in children:
            c.push_back(child.sp_array)
        with nogil:
            check_status(CUnionArray.MakeDense(
                deref(types.ap), deref(value_offsets.ap), c, &out))
        return pyarrow_wrap_array(out)

    @staticmethod
    def from_sparse(Array types, list children):
        """
        Construct sparse UnionArray from arrays of int8 types and children
        arrays

        Parameters
        ----------
        types : Array (int8 type)
        children : list

        Returns
        -------
        union_array : UnionArray
        """
        cdef shared_ptr[CArray] out
        cdef vector[shared_ptr[CArray]] c
        cdef Array child
        for child in children:
            c.push_back(child.sp_array)
        with nogil:
            check_status(CUnionArray.MakeSparse(deref(types.ap), c, &out))
        return pyarrow_wrap_array(out)

cdef class StringArray(Array):
    pass


cdef class BinaryArray(Array):
    pass


cdef class DictionaryArray(Array):

    cdef getitem(self, int64_t i):
        cdef Array dictionary = self.dictionary
        index = self.indices[i]
        if index is NA:
            return index
        else:
            return box_scalar(dictionary.type, dictionary.sp_array,
                              index.as_py())

    def dictionary_encode(self):
        return self

    property dictionary:

        def __get__(self):
            cdef CDictionaryArray* darr = <CDictionaryArray*>(self.ap)

            if self._dictionary is None:
                self._dictionary = pyarrow_wrap_array(darr.dictionary())

            return self._dictionary

    property indices:

        def __get__(self):
            cdef CDictionaryArray* darr = <CDictionaryArray*>(self.ap)

            if self._indices is None:
                self._indices = pyarrow_wrap_array(darr.indices())

            return self._indices

    @staticmethod
    def from_arrays(indices, dictionary, mask=None, ordered=False,
                    from_pandas=False, MemoryPool memory_pool=None):
        """
        Construct Arrow DictionaryArray from array of indices (must be
        non-negative integers) and corresponding array of dictionary values

        Parameters
        ----------
        indices : ndarray or pandas.Series, integer type
        dictionary : ndarray or pandas.Series
        mask : ndarray or pandas.Series, boolean type
            True values indicate that indices are actually null
        from_pandas : boolean, default False
            If True, the indices should be treated as though they originated in
            a pandas.Categorical (null encoded as -1)
        ordered : boolean, default False
            Set to True if the category values are ordered
        memory_pool : MemoryPool, default None
            For memory allocations, if required, otherwise uses default pool

        Returns
        -------
        dict_array : DictionaryArray
        """
        cdef:
            Array _indices, _dictionary
            DictionaryArray result
            shared_ptr[CDataType] c_type
            shared_ptr[CArray] c_result

        if isinstance(indices, Array):
            if mask is not None:
                raise NotImplementedError(
                    "mask not implemented with Arrow array inputs yet")
            _indices = indices
        else:
            if from_pandas:
                if mask is None:
                    mask = indices == -1
                else:
                    mask = mask | (indices == -1)
            _indices = array(indices, mask=mask, memory_pool=memory_pool)

        if isinstance(dictionary, Array):
            _dictionary = dictionary
        else:
            _dictionary = array(dictionary, memory_pool=memory_pool)

        if not isinstance(_indices, IntegerArray):
            raise ValueError('Indices must be integer type')

        cdef c_bool c_ordered = ordered

        c_type.reset(new CDictionaryType(_indices.type.sp_type,
                                         _dictionary.sp_array, c_ordered))
        c_result.reset(new CDictionaryArray(c_type, _indices.sp_array))

        result = DictionaryArray()
        result.init(c_result)
        return result


cdef class StructArray(Array):
    @staticmethod
    def from_arrays(field_names, arrays):
        cdef:
            Array array
            shared_ptr[CArray] c_array
            vector[shared_ptr[CArray]] c_arrays
            shared_ptr[CArray] c_result
            ssize_t num_arrays
            ssize_t length
            ssize_t i

        num_arrays = len(arrays)
        if num_arrays == 0:
            raise ValueError("arrays list is empty")

        length = len(arrays[0])

        c_arrays.resize(num_arrays)
        for i in range(num_arrays):
            array = arrays[i]
            if len(array) != length:
                raise ValueError("All arrays must have the same length")
            c_arrays[i] = array.sp_array

        cdef DataType struct_type = struct([
            field(name, array.type)
            for name, array in
            zip(field_names, arrays)
        ])

        c_result.reset(new CStructArray(struct_type.sp_type, length, c_arrays))
        result = StructArray()
        result.init(c_result)
        return result


cdef dict _array_classes = {
    _Type_NA: NullArray,
    _Type_BOOL: BooleanArray,
    _Type_UINT8: UInt8Array,
    _Type_UINT16: UInt16Array,
    _Type_UINT32: UInt32Array,
    _Type_UINT64: UInt64Array,
    _Type_INT8: Int8Array,
    _Type_INT16: Int16Array,
    _Type_INT32: Int32Array,
    _Type_INT64: Int64Array,
    _Type_DATE32: Date32Array,
    _Type_DATE64: Date64Array,
    _Type_TIMESTAMP: TimestampArray,
    _Type_TIME32: Time32Array,
    _Type_TIME64: Time64Array,
    _Type_FLOAT: FloatArray,
    _Type_DOUBLE: DoubleArray,
    _Type_LIST: ListArray,
    _Type_UNION: UnionArray,
    _Type_BINARY: BinaryArray,
    _Type_STRING: StringArray,
    _Type_DICTIONARY: DictionaryArray,
    _Type_FIXED_SIZE_BINARY: FixedSizeBinaryArray,
    _Type_DECIMAL: Decimal128Array,
    _Type_STRUCT: StructArray,
}


cdef object get_series_values(object obj):
    if isinstance(obj, PandasSeries):
        result = obj.values
    elif isinstance(obj, np.ndarray):
        result = obj
    else:
        result = PandasSeries(obj).values

    return result
