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


cdef _sequence_to_array(object sequence, object mask, object size,
                        DataType type, CMemoryPool* pool, c_bool from_pandas):
    cdef int64_t c_size
    cdef PyConversionOptions options

    if type is not None:
        options.type = type.sp_type

    if size is not None:
        options.size = size

    options.pool = pool
    options.from_pandas = from_pandas

    cdef shared_ptr[CChunkedArray] out

    with nogil:
        check_status(ConvertPySequence(sequence, mask, options, &out))

    if out.get().num_chunks() == 1:
        return pyarrow_wrap_array(out.get().chunk(0))
    else:
        return pyarrow_wrap_chunked_array(out)


cdef inline _is_array_like(obj):
    if isinstance(obj, np.ndarray):
        return True
    return pandas_api._have_pandas_internal() and pandas_api.is_array_like(obj)


def _ndarray_to_arrow_type(object values, DataType type):
    return pyarrow_wrap_data_type(_ndarray_to_type(values, type))


cdef shared_ptr[CDataType] _ndarray_to_type(object values,
                                            DataType type) except *:
    cdef shared_ptr[CDataType] c_type

    dtype = values.dtype

    if type is None and dtype != object:
        with nogil:
            check_status(NumPyDtypeToArrow(dtype, &c_type))

    if type is not None:
        c_type = type.sp_type

    return c_type


cdef _ndarray_to_array(object values, object mask, DataType type,
                       c_bool from_pandas, c_bool safe, CMemoryPool* pool):
    cdef:
        shared_ptr[CChunkedArray] chunked_out
        shared_ptr[CDataType] c_type = _ndarray_to_type(values, type)
        CCastOptions cast_options = CCastOptions(safe)

    with nogil:
        check_status(NdarrayToArrow(pool, values, mask, from_pandas,
                                    c_type, cast_options, &chunked_out))

    if chunked_out.get().num_chunks() > 1:
        return pyarrow_wrap_chunked_array(chunked_out)
    else:
        return pyarrow_wrap_array(chunked_out.get().chunk(0))


def array(object obj, type=None, mask=None, size=None, from_pandas=None,
          bint safe=True, MemoryPool memory_pool=None):
    """
    Create pyarrow.Array instance from a Python object

    Parameters
    ----------
    obj : sequence, iterable, ndarray or Series
        If both type and size are specified may be a single use iterable. If
        not strongly-typed, Arrow type will be inferred for resulting array
    type : pyarrow.DataType
        Explicit type to attempt to coerce to, otherwise will be inferred from
        the data
    mask : array (boolean), optional
        Indicate which values are null (True) or not null (False).
    size : int64, optional
        Size of the elements. If the imput is larger than size bail at this
        length. For iterators, if size is larger than the input iterator this
        will be treated as a "max size", but will involve an initial allocation
        of size followed by a resize to the actual size (so if you know the
        exact size specifying it correctly will give you better performance).
    from_pandas : boolean, default None
        Use pandas's semantics for inferring nulls from values in
        ndarray-like data. If passed, the mask tasks precendence, but
        if a value is unmasked (not-null), but still null according to
        pandas semantics, then it is null. Defaults to False if not
        passed explicitly by user, or True if a pandas object is
        passed in
    safe : boolean, default True
        Check for overflows or other unsafe conversions
    memory_pool : pyarrow.MemoryPool, optional
        If not passed, will allocate memory from the currently-set default
        memory pool

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
      null
    ]

    Returns
    -------
    array : pyarrow.Array or pyarrow.ChunkedArray (if object data
    overflowed binary storage)
    """
    cdef:
        CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        bint is_pandas_object = False
        bint c_from_pandas

    type = ensure_type(type, allow_none=True)

    if from_pandas is None:
        c_from_pandas = False
    else:
        c_from_pandas = from_pandas

    if _is_array_like(obj):
        if mask is not None:
            # out argument unused
            mask = get_series_values(mask, &is_pandas_object)

        values = get_series_values(obj, &is_pandas_object)
        if is_pandas_object and from_pandas is None:
            c_from_pandas = True

        if pandas_api.is_categorical(values):
            return DictionaryArray.from_arrays(
                values.codes, values.categories.values,
                mask=mask, ordered=values.ordered,
                from_pandas=True, safe=safe,
                memory_pool=memory_pool)
        else:
            if pandas_api.have_pandas:
                values, type = pandas_api.compat.get_datetimetz_type(
                    values, obj.dtype, type)
            return _ndarray_to_array(values, mask, type, c_from_pandas, safe,
                                     pool)
    else:
        # ConvertPySequence does strict conversion if type is explicitly passed
        return _sequence_to_array(obj, mask, size, type, pool, c_from_pandas)


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
    cdef:
        Py_ssize_t start, stop, step
        Py_ssize_t n = len(arrow_obj)

    start = key.start or 0
    if start < 0:
        start += n
        if start < 0:
            start = 0
    elif start >= n:
        start = n

    stop = key.stop if key.stop is not None else n
    if stop < 0:
        stop += n
        if stop < 0:
            stop = 0
    elif stop >= n:
        stop = n

    step = key.step or 1
    if step != 1:
        raise IndexError('only slices with step 1 supported')
    else:
        return arrow_obj.slice(start, stop - start)


cdef Py_ssize_t _normalize_index(Py_ssize_t index,
                                 Py_ssize_t length) except -1:
    if index < 0:
        index += length
        if index < 0:
            raise IndexError("index out of bounds")
    elif index >= length:
        raise IndexError("index out of bounds")
    return index


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
    elif datum.kind() == DatumType_SCALAR:
        return pyarrow_wrap_scalar(datum.scalar())
    else:
        raise ValueError("Unable to wrap Datum in a Python object")


cdef _append_array_buffers(const CArrayData* ad, list res):
    """
    Recursively append Buffer wrappers from *ad* and its children.
    """
    cdef size_t i, n
    assert ad != NULL
    n = ad.buffers.size()
    for i in range(n):
        buf = ad.buffers[i]
        res.append(pyarrow_wrap_buffer(buf)
                   if buf.get() != NULL else None)
    n = ad.child_data.size()
    for i in range(n):
        _append_array_buffers(ad.child_data[i].get(), res)


cdef _reduce_array_data(const CArrayData* ad):
    """
    Recursively dissect ArrayData to (pickable) tuples.
    """
    cdef size_t i, n
    assert ad != NULL

    n = ad.buffers.size()
    buffers = []
    for i in range(n):
        buf = ad.buffers[i]
        buffers.append(pyarrow_wrap_buffer(buf)
                       if buf.get() != NULL else None)

    children = []
    n = ad.child_data.size()
    for i in range(n):
        children.append(_reduce_array_data(ad.child_data[i].get()))

    return pyarrow_wrap_data_type(ad.type), ad.length, ad.null_count, \
        ad.offset, buffers, children


cdef shared_ptr[CArrayData] _reconstruct_array_data(data):
    """
    Reconstruct CArrayData objects from the tuple structure generated
    by _reduce_array_data.
    """
    cdef:
        int64_t length, null_count, offset, i
        DataType dtype
        Buffer buf
        vector[shared_ptr[CBuffer]] c_buffers
        vector[shared_ptr[CArrayData]] c_children

    dtype, length, null_count, offset, buffers, children = data

    for i in range(len(buffers)):
        buf = buffers[i]
        if buf is None:
            c_buffers.push_back(shared_ptr[CBuffer]())
        else:
            c_buffers.push_back(buf.buffer)

    for i in range(len(children)):
        c_children.push_back(_reconstruct_array_data(children[i]))

    return CArrayData.MakeWithChildren(
        dtype.sp_type,
        length,
        c_buffers,
        c_children,
        null_count,
        offset)


def _restore_array(data):
    """
    Reconstruct an Array from pickled ArrayData.
    """
    cdef shared_ptr[CArrayData] ad = _reconstruct_array_data(data)
    return pyarrow_wrap_array(MakeArray(ad))


cdef class _PandasConvertible:

    def to_pandas(self, categories=None, bint strings_to_categorical=False,
                  bint zero_copy_only=False, bint integer_object_nulls=False,
                  bint date_as_object=True, bint use_threads=True,
                  bint deduplicate_objects=True, bint ignore_metadata=False):
        """
        Convert to a pandas-compatible NumPy array or DataFrame, as appropriate

        Parameters
        ----------
        strings_to_categorical : boolean, default False
            Encode string (UTF8) and binary types to pandas.Categorical
        categories: list, default empty
            List of fields that should be returned as pandas.Categorical. Only
            applies to table-like data structures
        zero_copy_only : boolean, default False
            Raise an ArrowException if this function call would require copying
            the underlying data
        integer_object_nulls : boolean, default False
            Cast integers with nulls to objects
        date_as_object : boolean, default False
            Cast dates to objects
        use_threads: boolean, default True
            Whether to parallelize the conversion using multiple threads
        deduplicate_objects : boolean, default False
            Do not create multiple copies Python objects when created, to save
            on memory use. Conversion will be slower
        ignore_metadata : boolean, default False
            If True, do not use the 'pandas' metadata to reconstruct the
            DataFrame index, if present

        Returns
        -------
        NumPy array or DataFrame depending on type of object
        """
        cdef:
            PyObject* out
            PandasOptions options

        options = PandasOptions(
            strings_to_categorical=strings_to_categorical,
            zero_copy_only=zero_copy_only,
            integer_object_nulls=integer_object_nulls,
            date_as_object=date_as_object,
            use_threads=use_threads,
            deduplicate_objects=deduplicate_objects)

        return self._to_pandas(options, categories=categories,
                               ignore_metadata=ignore_metadata)


cdef class Array(_PandasConvertible):
    """
    The base class for all Arrow arrays.
    """

    def __init__(self):
        raise TypeError("Do not call {}'s constructor directly, use one of "
                        "the `pyarrow.Array.from_*` functions instead."
                        .format(self.__class__.__name__))

    cdef void init(self, const shared_ptr[CArray]& sp_array) except *:
        self.sp_array = sp_array
        self.ap = sp_array.get()
        self.type = pyarrow_wrap_data_type(self.sp_array.get().type())

    def __eq__(self, other):
        raise NotImplementedError('Comparisons with pyarrow.Array are not '
                                  'implemented')

    def _debug_print(self):
        with nogil:
            check_status(DebugPrint(deref(self.ap), 0))

    def cast(self, object target_type, bint safe=True):
        """
        Cast array values to another data type.

        Example
        -------

        >>> from datetime import datetime
        >>> import pyarrow as pa
        >>> arr = pa.array([datetime(2010, 1, 1), datetime(2015, 1, 1)])
        >>> arr.type
        TimestampType(timestamp[us])

        You can use ``pyarrow.DataType`` objects to specify the target type:

        >>> arr.cast(pa.timestamp('ms'))
        <pyarrow.lib.TimestampArray object at 0x10420eb88>
        [
          1262304000000,
          1420070400000
        ]
        >>> arr.cast(pa.timestamp('ms')).type
        TimestampType(timestamp[ms])

        Alternatively, it is also supported to use the string aliases for these
        types:

        >>> arr.cast('timestamp[ms]')
        <pyarrow.lib.TimestampArray object at 0x10420eb88>
        [
          1262304000000,
          1420070400000
        ]
        >>> arr.cast('timestamp[ms]').type
        TimestampType(timestamp[ms])

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
            CCastOptions options = CCastOptions(safe)
            DataType type = ensure_type(target_type)
            shared_ptr[CArray] result

        with nogil:
            check_status(Cast(_context(), self.ap[0], type.sp_type,
                              options, &result))

        return pyarrow_wrap_array(result)

    def sum(self):
        """
        Sum the values in a numerical array.
        """
        cdef CDatum out

        with nogil:
            check_status(Sum(_context(), CDatum(self.sp_array), &out))

        return wrap_datum(out)

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
    def from_pandas(obj, mask=None, type=None, bint safe=True,
                    MemoryPool memory_pool=None):
        """
        Convert pandas.Series to an Arrow Array, using pandas's semantics about
        what values indicate nulls. See pyarrow.array for more general
        conversion from arrays or sequences to Arrow arrays.

        Parameters
        ----------
        sequence : ndarray, Inded Series
        mask : array (boolean), optional
            Indicate which values are null (True) or not null (False)
        type : pyarrow.DataType
            Explicit type to attempt to coerce to, otherwise will be inferred
            from the data
        safe : boolean, default True
            Check for overflows or other unsafe conversions
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
        return array(obj, mask=mask, type=type, safe=safe, from_pandas=True,
                     memory_pool=memory_pool)

    def __reduce__(self):
        return _restore_array, \
            (_reduce_array_data(self.sp_array.get().data().get()),)

    @staticmethod
    def from_buffers(DataType type, length, buffers, null_count=-1, offset=0,
                     children=None):
        """
        Construct an Array from a sequence of buffers. The concrete type
        returned depends on the datatype.

        Parameters
        ----------
        type : DataType
            The value type of the array
        length : int
            The number of values in the array
        buffers: List[Buffer]
            The buffers backing this array
        null_count : int, default -1
        offset : int, default 0
            The array's logical offset (in values, not in bytes) from the
            start of each buffer
        children : List[Array], default None
            Nested type children with length matching type.num_children

        Returns
        -------
        array : Array
        """
        cdef:
            Buffer buf
            Array child
            vector[shared_ptr[CBuffer]] c_buffers
            vector[shared_ptr[CArrayData]] c_child_data
            shared_ptr[CArrayData] array_data

        children = children or []

        if type.num_children != len(children):
            raise ValueError("Type's expected number of children "
                             "({0}) did not match the passed number "
                             "({1}).".format(type.num_children, len(children)))

        if type.num_buffers != len(buffers):
            raise ValueError("Type's expected number of buffers "
                             "({0}) did not match the passed number "
                             "({1}).".format(type.num_buffers, len(buffers)))

        for buf in buffers:
            # None will produce a null buffer pointer
            c_buffers.push_back(pyarrow_unwrap_buffer(buf))

        for child in children:
            c_child_data.push_back(child.ap.data())

        array_data = CArrayData.MakeWithChildren(type.sp_type, length,
                                                 c_buffers, c_child_data,
                                                 null_count, offset)
        cdef Array result = pyarrow_wrap_array(MakeArray(array_data))
        result.validate()
        return result

    @property
    def null_count(self):
        return self.sp_array.get().null_count()

    def __iter__(self):
        for i in range(len(self)):
            yield self.getitem(i)

    def __repr__(self):
        type_format = object.__repr__(self)
        return '{0}\n{1}'.format(type_format, str(self))

    def format(self, int indent=0, int window=10):
        cdef:
            c_string result

        with nogil:
            check_status(
                PrettyPrint(
                    deref(self.ap),
                    PrettyPrintOptions(indent, window),
                    &result
                )
            )

        return frombytes(result)

    def __str__(self):
        return self.format()

    def equals(Array self, Array other):
        return self.ap.Equals(deref(other.ap))

    def __len__(self):
        return self.length()

    cdef int64_t length(self):
        if self.sp_array.get():
            return self.sp_array.get().length()
        else:
            return 0

    def isnull(self):
        raise NotImplemented

    def __getitem__(self, index):
        """
        Return the value at the given index.

        Returns
        -------
        value : Scalar
        """
        if PySlice_Check(index):
            return _normalize_slice(self, index)

        return self.getitem(_normalize_index(index, self.length()))

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

    def take(self, Array indices):
        """
        Take elements from an array.

        The resulting array will be of the same type as the input array, with
        elements taken from the input array at the given indices. If an index
        is null then the taken element will be null.

        Parameters
        ----------
        indices : Array
            The indices of the values to extract. Array needs to be of
            integer type.

        Returns
        -------
        Array

        Examples
        --------

        >>> import pyarrow as pa
        >>> arr = pa.array(["a", "b", "c", None, "e", "f"])
        >>> indices = pa.array([0, None, 4, 3])
        >>> arr.take(indices)
        <pyarrow.lib.StringArray object at 0x7ffa4fc7d368>
        [
          "a",
          null,
          "e",
          null
        ]
        """
        cdef:
            cdef CTakeOptions options
            cdef CDatum out

        with nogil:
            check_status(Take(_context(), CDatum(self.sp_array),
                              CDatum(indices.sp_array), options, &out))

        return wrap_datum(out)

    def _to_pandas(self, options, **kwargs):
        cdef:
            PyObject* out
            PandasOptions c_options = options

        with nogil:
            check_status(ConvertArrayToPandas(c_options, self.sp_array,
                                              self, &out))
        return wrap_array_output(out)

    def __array__(self, dtype=None):
        if dtype is None:
            return self.to_pandas()
        return self.to_pandas().astype(dtype)

    def to_numpy(self):
        """
        Experimental: return a NumPy view of this array. Only primitive
        arrays with the same memory layout as NumPy (i.e. integers,
        floating point), without any nulls, are supported.

        Returns
        -------
        array : numpy.ndarray
        """
        if self.null_count:
            raise NotImplementedError('NumPy array view is only supported '
                                      'for arrays without nulls.')
        if not is_primitive(self.type.id) or self.type.id == _Type_BOOL:
            raise NotImplementedError('NumPy array view is only supported '
                                      'for primitive types.')
        buflist = self.buffers()
        assert len(buflist) == 2
        return np.frombuffer(buflist[-1], dtype=self.type.to_pandas_dtype())[
            self.offset:self.offset + len(self)]

    def to_pylist(self):
        """
        Convert to a list of native Python objects.

        Returns
        -------
        lst : list
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

    @property
    def offset(self):
        """
        A relative position into another array's data, to enable zero-copy
        slicing. This value defaults to zero but must be applied on all
        operations with the physical storage buffers.
        """
        return self.sp_array.get().offset()

    def buffers(self):
        """
        Return a list of Buffer objects pointing to this array's physical
        storage.

        To correctly interpret these buffers, you need to also apply the offset
        multiplied with the size of the stored data type.
        """
        res = []
        _append_array_buffers(self.sp_array.get().data().get(), res)
        return res


cdef class Tensor:
    """
    A n-dimensional array a.k.a Tensor.
    """

    def __init__(self):
        raise TypeError("Do not call Tensor's constructor directly, use one "
                        "of the `pyarrow.Tensor.from_*` functions instead.")

    cdef void init(self, const shared_ptr[CTensor]& sp_tensor):
        self.sp_tensor = sp_tensor
        self.tp = sp_tensor.get()
        self.type = pyarrow_wrap_data_type(self.tp.type())

    def __repr__(self):
        return """<pyarrow.Tensor>
type: {0.type}
shape: {0.shape}
strides: {0.strides}""".format(self)

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
        cdef PyObject* out

        with nogil:
            check_status(TensorToNdarray(self.sp_tensor, self, &out))
        return PyObject_to_object(out)

    def equals(self, Tensor other):
        """
        Return true if the tensors contains exactly equal data
        """
        return self.tp.Equals(deref(other.tp))

    def __eq__(self, other):
        if isinstance(other, Tensor):
            return self.equals(other)
        else:
            return NotImplemented

    @property
    def is_mutable(self):
        return self.tp.is_mutable()

    @property
    def is_contiguous(self):
        return self.tp.is_contiguous()

    @property
    def ndim(self):
        return self.tp.ndim()

    @property
    def size(self):
        return self.tp.size()

    @property
    def shape(self):
        # Cython knows how to convert a vector[T] to a Python list
        return tuple(self.tp.shape())

    @property
    def strides(self):
        return tuple(self.tp.strides())

    def __getbuffer__(self, cp.Py_buffer* buffer, int flags):
        buffer.buf = <char *> self.tp.data().get().data()
        pep3118_format = self.type.pep3118_format
        if pep3118_format is None:
            raise NotImplementedError("type %s not supported for buffer "
                                      "protocol" % (self.type,))
        buffer.format = pep3118_format
        buffer.itemsize = self.type.bit_width // 8
        buffer.internal = NULL
        buffer.len = self.tp.size() * buffer.itemsize
        buffer.ndim = self.tp.ndim()
        buffer.obj = self
        if self.tp.is_mutable():
            buffer.readonly = 0
        else:
            buffer.readonly = 1
        # NOTE: This assumes Py_ssize_t == int64_t, and that the shape
        # and strides arrays lifetime is tied to the tensor's
        buffer.shape = <Py_ssize_t *> &self.tp.shape()[0]
        buffer.strides = <Py_ssize_t *> &self.tp.strides()[0]
        buffer.suboffsets = NULL


cdef wrap_array_output(PyObject* output):
    cdef object obj = PyObject_to_object(output)

    if isinstance(obj, dict):
        return pandas_api.categorical_type(obj['indices'],
                                           categories=obj['dictionary'],
                                           ordered=obj['ordered'],
                                           fastpath=True)
    else:
        return obj


cdef class NullArray(Array):
    """
    Concrete class for Arrow arrays of null data type.
    """


cdef class BooleanArray(Array):
    """
    Concrete class for Arrow arrays of boolean data type.
    """


cdef class NumericArray(Array):
    """
    A base class for Arrow numeric arrays.
    """


cdef class IntegerArray(NumericArray):
    """
    A base class for Arrow integer arrays.
    """


cdef class FloatingPointArray(NumericArray):
    """
    A base class for Arrow floating-point arrays.
    """


cdef class Int8Array(IntegerArray):
    """
    Concrete class for Arrow arrays of int8 data type.
    """


cdef class UInt8Array(IntegerArray):
    """
    Concrete class for Arrow arrays of uint8 data type.
    """


cdef class Int16Array(IntegerArray):
    """
    Concrete class for Arrow arrays of int16 data type.
    """


cdef class UInt16Array(IntegerArray):
    """
    Concrete class for Arrow arrays of uint16 data type.
    """


cdef class Int32Array(IntegerArray):
    """
    Concrete class for Arrow arrays of int32 data type.
    """


cdef class UInt32Array(IntegerArray):
    """
    Concrete class for Arrow arrays of uint32 data type.
    """


cdef class Int64Array(IntegerArray):
    """
    Concrete class for Arrow arrays of int64 data type.
    """


cdef class UInt64Array(IntegerArray):
    """
    Concrete class for Arrow arrays of uint64 data type.
    """


cdef class Date32Array(NumericArray):
    """
    Concrete class for Arrow arrays of date32 data type.
    """


cdef class Date64Array(NumericArray):
    """
    Concrete class for Arrow arrays of date64 data type.
    """


cdef class TimestampArray(NumericArray):
    """
    Concrete class for Arrow arrays of timestamp data type.
    """


cdef class Time32Array(NumericArray):
    """
    Concrete class for Arrow arrays of time32 data type.
    """


cdef class Time64Array(NumericArray):
    """
    Concrete class for Arrow arrays of time64 data type.
    """


cdef class HalfFloatArray(FloatingPointArray):
    """
    Concrete class for Arrow arrays of float16 data type.
    """


cdef class FloatArray(FloatingPointArray):
    """
    Concrete class for Arrow arrays of float32 data type.
    """


cdef class DoubleArray(FloatingPointArray):
    """
    Concrete class for Arrow arrays of float64 data type.
    """


cdef class FixedSizeBinaryArray(Array):
    """
    Concrete class for Arrow arrays of a fixed-size binary data type.
    """


cdef class Decimal128Array(FixedSizeBinaryArray):
    """
    Concrete class for Arrow arrays of decimal128 data type.
    """


cdef class ListArray(Array):
    """
    Concrete class for Arrow arrays of a list data type.
    """

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

    def flatten(self):
        """
        Unnest this ListArray by one level

        Returns
        -------
        result : Array
        """
        cdef CListArray* arr = <CListArray*> self.ap
        return pyarrow_wrap_array(arr.values())


cdef class UnionArray(Array):
    """
    Concrete class for Arrow arrays of a Union data type.
    """

    @staticmethod
    def from_dense(Array types, Array value_offsets, list children,
                   list field_names=None, list type_codes=None):
        """
        Construct dense UnionArray from arrays of int8 types, int32 offsets and
        children arrays

        Parameters
        ----------
        types : Array (int8 type)
        value_offsets : Array (int32 type)
        children : list
        field_names : list
        type_codes : list

        Returns
        -------
        union_array : UnionArray
        """
        cdef shared_ptr[CArray] out
        cdef vector[shared_ptr[CArray]] c
        cdef Array child
        cdef vector[c_string] c_field_names
        cdef vector[uint8_t] c_type_codes
        for child in children:
            c.push_back(child.sp_array)
        if field_names is not None:
            for x in field_names:
                c_field_names.push_back(tobytes(x))
        if type_codes is not None:
            for x in type_codes:
                c_type_codes.push_back(x)
        with nogil:
            check_status(CUnionArray.MakeDense(
                deref(types.ap), deref(value_offsets.ap), c, c_field_names,
                c_type_codes, &out))
        return pyarrow_wrap_array(out)

    @staticmethod
    def from_sparse(Array types, list children, list field_names=None,
                    list type_codes=None):
        """
        Construct sparse UnionArray from arrays of int8 types and children
        arrays

        Parameters
        ----------
        types : Array (int8 type)
        children : list
        field_names : list
        type_codes : list

        Returns
        -------
        union_array : UnionArray
        """
        cdef shared_ptr[CArray] out
        cdef vector[shared_ptr[CArray]] c
        cdef Array child
        cdef vector[c_string] c_field_names
        cdef vector[uint8_t] c_type_codes
        for child in children:
            c.push_back(child.sp_array)
        if field_names is not None:
            for x in field_names:
                c_field_names.push_back(tobytes(x))
        if type_codes is not None:
            for x in type_codes:
                c_type_codes.push_back(x)
        with nogil:
            check_status(CUnionArray.MakeSparse(deref(types.ap), c,
                                                c_field_names,
                                                c_type_codes,
                                                &out))
        return pyarrow_wrap_array(out)


cdef class StringArray(Array):
    """
    Concrete class for Arrow arrays of string (or utf8) data type.
    """

    @staticmethod
    def from_buffers(int length, Buffer value_offsets, Buffer data,
                     Buffer null_bitmap=None, int null_count=-1,
                     int offset=0):
        """
        Construct a StringArray from value_offsets and data buffers.
        If there are nulls in the data, also a null_bitmap and the matching
        null_count must be passed.

        Parameters
        ----------
        length : int
        value_offsets : Buffer
        data : Buffer
        null_bitmap : Buffer, optional
        null_count : int, default 0
        offset : int, default 0

        Returns
        -------
        string_array : StringArray
        """
        return Array.from_buffers(utf8(), length,
                                  [null_bitmap, value_offsets, data],
                                  null_count, offset)


cdef class BinaryArray(Array):
    """
    Concrete class for Arrow arrays of variable-sized binary data type.
    """


cdef class DictionaryArray(Array):
    """
    Concrete class for dictionary-encoded Arrow arrays.
    """

    def dictionary_encode(self):
        return self

    @property
    def dictionary(self):
        cdef CDictionaryArray* darr = <CDictionaryArray*>(self.ap)

        if self._dictionary is None:
            self._dictionary = pyarrow_wrap_array(darr.dictionary())

        return self._dictionary

    @property
    def indices(self):
        cdef CDictionaryArray* darr = <CDictionaryArray*>(self.ap)

        if self._indices is None:
            self._indices = pyarrow_wrap_array(darr.indices())

        return self._indices

    @staticmethod
    def from_arrays(indices, dictionary, mask=None, bint ordered=False,
                    bint from_pandas=False, bint safe=True,
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
        from_pandas : boolean, default False
            If True, the indices should be treated as though they originated in
            a pandas.Categorical (null encoded as -1)
        ordered : boolean, default False
            Set to True if the category values are ordered
        safe : boolean, default True
            If True, check that the dictionary indices are in range
        memory_pool : MemoryPool, default None
            For memory allocations, if required, otherwise uses default pool

        Returns
        -------
        dict_array : DictionaryArray
        """
        cdef:
            Array _indices, _dictionary
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
                                         _dictionary.sp_array.get().type(),
                                         c_ordered))

        if safe:
            with nogil:
                check_status(
                    CDictionaryArray.FromArrays(c_type, _indices.sp_array,
                                                _dictionary.sp_array,
                                                &c_result))
        else:
            c_result.reset(new CDictionaryArray(c_type, _indices.sp_array,
                                                _dictionary.sp_array))

        return pyarrow_wrap_array(c_result)


cdef class StructArray(Array):
    """
    Concrete class for Arrow arrays of a struct data type.
    """

    def field(self, index):
        """
        Retrieves the child array belonging to field

        Parameters
        ----------
        index : Union[int, str]
            Index / position or name of the field

        Returns
        -------
        result : Array
        """
        cdef:
            CStructArray* arr = <CStructArray*> self.ap
            shared_ptr[CArray] child

        if isinstance(index, six.string_types):
            child = arr.GetFieldByName(tobytes(index))
            if child == nullptr:
                raise KeyError(index)
        elif isinstance(index, six.integer_types):
            child = arr.field(
                <int>_normalize_index(index, self.ap.num_fields()))
        else:
            raise TypeError('Expected integer or string index')

        return pyarrow_wrap_array(child)

    def flatten(self, MemoryPool memory_pool=None):
        """
        Flatten this StructArray, returning one individual array for each
        field in the struct.

        Parameters
        ----------
        memory_pool : MemoryPool, default None
            For memory allocations, if required, otherwise use default pool

        Returns
        -------
        result : List[Array]
        """
        cdef:
            vector[shared_ptr[CArray]] arrays
            CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
            CStructArray* sarr = <CStructArray*> self.ap

        with nogil:
            check_status(sarr.Flatten(pool, &arrays))

        return [pyarrow_wrap_array(arr) for arr in arrays]

    @staticmethod
    def from_arrays(arrays, names=None):
        """
        Construct StructArray from collection of arrays representing each field
        in the struct

        Parameters
        ----------
        arrays : sequence of Array
        names : List[str]
            Field names

        Returns
        -------
        result : StructArray
        """
        cdef:
            Array array
            shared_ptr[CArray] c_array
            vector[shared_ptr[CArray]] c_arrays
            shared_ptr[CArray] c_result
            ssize_t num_arrays
            ssize_t length
            ssize_t i
            DataType struct_type

        if names is None:
            raise ValueError('Names are currently required')

        arrays = [asarray(x) for x in arrays]

        num_arrays = len(arrays)
        if num_arrays != len(names):
            raise ValueError("The number of names should be equal to the "
                             "number of arrays")

        if num_arrays > 0:
            length = len(arrays[0])
            c_arrays.resize(num_arrays)
            for i in range(num_arrays):
                array = arrays[i]
                if len(array) != length:
                    raise ValueError("All arrays must have the same length")
                c_arrays[i] = array.sp_array
        else:
            length = 0

        struct_type = struct([
            field(name, array.type) for name, array in zip(names, arrays)
        ])

        c_result.reset(new CStructArray(struct_type.sp_type, length, c_arrays))

        return pyarrow_wrap_array(c_result)


cdef class ExtensionArray(Array):
    """
    Concrete class for Arrow extension arrays.
    """

    @property
    def storage(self):
        cdef:
            CExtensionArray* ext_array = <CExtensionArray*>(self.ap)

        return pyarrow_wrap_array(ext_array.storage())

    @staticmethod
    def from_storage(BaseExtensionType typ, Array storage):
        """
        Construct ExtensionArray from type and storage array.

        Parameters
        ----------
        typ: DataType
            The extension type for the result array.
        storage: Array
            The underlying storage for the result array.

        Returns
        -------
        ext_array : ExtensionArray
        """
        cdef:
            shared_ptr[CExtensionArray] ext_array

        if storage.type != typ.storage_type:
            raise TypeError("Incompatible storage type {0} "
                            "for extension type {1}".format(storage.type, typ))

        ext_array = make_shared[CExtensionArray](typ.sp_type, storage.sp_array)
        return pyarrow_wrap_array(<shared_ptr[CArray]> ext_array)


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
    _Type_HALF_FLOAT: HalfFloatArray,
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
    _Type_EXTENSION: ExtensionArray,
}


cdef object get_series_values(object obj, bint* is_series):
    if pandas_api.is_series(obj):
        result = obj.values
        is_series[0] = True
    elif isinstance(obj, np.ndarray):
        result = obj
        is_series[0] = False
    else:
        result = pandas_api.make_series(obj).values
        is_series[0] = False

    return result


def concat_arrays(arrays, MemoryPool memory_pool=None):
    """
    Returns a concatenation of the given arrays. The contents of those arrays
    are copied into the returned array. Raises exception if all of the arrays
    are not of the same type.

    Parameters
    ----------
    arrays : iterable of pyarrow.Array objects
    memory_pool : MemoryPool, default None
        For memory allocations. If None, the default pool is used.
    """
    cdef:
        vector[shared_ptr[CArray]] c_arrays
        shared_ptr[CArray] c_result
        Array array
        CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)

    for array in arrays:
        c_arrays.push_back(array.sp_array)

    with nogil:
        check_status(Concatenate(c_arrays, pool, &c_result))

    return pyarrow_wrap_array(c_result)
