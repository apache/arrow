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

import warnings


cdef class ChunkedArray(_PandasConvertible):
    """
    An array-like composed from a (possibly empty) collection of pyarrow.Arrays

    Warnings
    --------
    Do not call this class's constructor directly.
    """

    def __cinit__(self):
        self.chunked_array = NULL

    def __init__(self):
        raise TypeError("Do not call ChunkedArray's constructor directly, use "
                        "`chunked_array` function instead.")

    cdef void init(self, const shared_ptr[CChunkedArray]& chunked_array):
        self.sp_chunked_array = chunked_array
        self.chunked_array = chunked_array.get()

    def __reduce__(self):
        return chunked_array, (self.chunks, self.type)

    @property
    def data(self):
        import warnings
        warnings.warn("Calling .data on ChunkedArray is provided for "
                      "compatibility after Column was removed, simply drop "
                      "this attribute", FutureWarning)
        return self

    @property
    def type(self):
        return pyarrow_wrap_data_type(self.sp_chunked_array.get().type())

    def length(self):
        return self.chunked_array.length()

    def __len__(self):
        return self.length()

    def __repr__(self):
        type_format = object.__repr__(self)
        return '{0}\n{1}'.format(type_format, str(self))

    def to_string(self, *, int indent=0, int window=10,
                  c_bool skip_new_lines=False):
        """
        Render a "pretty-printed" string representation of the ChunkedArray

        Parameters
        ----------
        indent : int
            How much to indent right the content of the array,
            by default ``0``.
        window : int
            How many items to preview at the begin and end
            of the array when the arrays is bigger than the window.
            The other elements will be ellipsed.
        skip_new_lines : bool
            If the array should be rendered as a single line of text
            or if each element should be on its own line.
        """
        cdef:
            c_string result
            PrettyPrintOptions options

        with nogil:
            options = PrettyPrintOptions(indent, window)
            options.skip_new_lines = skip_new_lines
            check_status(
                PrettyPrint(
                    deref(self.chunked_array),
                    options,
                    &result
                )
            )

        return frombytes(result, safe=True)

    def format(self, **kwargs):
        import warnings
        warnings.warn('ChunkedArray.format is deprecated, '
                      'use ChunkedArray.to_string')
        return self.to_string(**kwargs)

    def __str__(self):
        return self.to_string()

    def validate(self, *, full=False):
        """
        Perform validation checks.  An exception is raised if validation fails.

        By default only cheap validation checks are run.  Pass `full=True`
        for thorough validation checks (potentially O(n)).

        Parameters
        ----------
        full: bool, default False
            If True, run expensive checks, otherwise cheap checks only.

        Raises
        ------
        ArrowInvalid
        """
        if full:
            with nogil:
                check_status(self.sp_chunked_array.get().ValidateFull())
        else:
            with nogil:
                check_status(self.sp_chunked_array.get().Validate())

    @property
    def null_count(self):
        """
        Number of null entries

        Returns
        -------
        int
        """
        return self.chunked_array.null_count()

    @property
    def nbytes(self):
        """
        Total number of bytes consumed by the elements of the chunked array.

        In other words, the sum of bytes from all buffer ranges referenced.

        Unlike `get_total_buffer_size` this method will account for array
        offsets.

        If buffers are shared between arrays then the shared
        portion will only be counted multiple times.

        The dictionary of dictionary arrays will always be counted in their 
        entirety even if the array only references a portion of the dictionary.
        """
        cdef:
            CResult[int64_t] c_res_buffer

        c_res_buffer = ReferencedBufferSize(deref(self.chunked_array))
        size = GetResultValue(c_res_buffer)
        return size

    def get_total_buffer_size(self):
        """
        The sum of bytes in each buffer referenced by the chunked array.

        An array may only reference a portion of a buffer.
        This method will overestimate in this case and return the
        byte size of the entire buffer.

        If a buffer is referenced multiple times then it will
        only be counted once.
        """
        cdef:
            int64_t total_buffer_size

        total_buffer_size = TotalBufferSize(deref(self.chunked_array))
        return total_buffer_size

    def __sizeof__(self):
        return super(ChunkedArray, self).__sizeof__() + self.nbytes

    def __iter__(self):
        for chunk in self.iterchunks():
            for item in chunk:
                yield item

    def __getitem__(self, key):
        """
        Slice or return value at given index

        Parameters
        ----------
        key : integer or slice
            Slices with step not equal to 1 (or None) will produce a copy
            rather than a zero-copy view

        Returns
        -------
        value : Scalar (index) or ChunkedArray (slice)
        """
        if isinstance(key, slice):
            return _normalize_slice(self, key)

        return self.getitem(_normalize_index(key, self.chunked_array.length()))

    cdef getitem(self, int64_t index):
        cdef int j

        for j in range(self.num_chunks):
            if index < self.chunked_array.chunk(j).get().length():
                return self.chunk(j)[index]
            else:
                index -= self.chunked_array.chunk(j).get().length()

    def is_null(self, *, nan_is_null=False):
        """
        Return boolean array indicating the null values.

        Parameters
        ----------
        nan_is_null : bool (optional, default False)
            Whether floating-point NaN values should also be considered null.

        Returns
        -------
        array : boolean Array or ChunkedArray
        """
        options = _pc().NullOptions(nan_is_null=nan_is_null)
        return _pc().call_function('is_null', [self], options)

    def is_valid(self):
        """
        Return boolean array indicating the non-null values.
        """
        return _pc().is_valid(self)

    def __eq__(self, other):
        try:
            return self.equals(other)
        except TypeError:
            return NotImplemented

    def fill_null(self, fill_value):
        """
        See pyarrow.compute.fill_null docstring for usage.
        """
        return _pc().fill_null(self, fill_value)

    def equals(self, ChunkedArray other):
        """
        Return whether the contents of two chunked arrays are equal.

        Parameters
        ----------
        other : pyarrow.ChunkedArray
            Chunked array to compare against.

        Returns
        -------
        are_equal : bool
        """
        if other is None:
            return False

        cdef:
            CChunkedArray* this_arr = self.chunked_array
            CChunkedArray* other_arr = other.chunked_array
            c_bool result

        with nogil:
            result = this_arr.Equals(deref(other_arr))

        return result

    def _to_pandas(self, options, **kwargs):
        return _array_like_to_pandas(self, options)

    def to_numpy(self):
        """
        Return a NumPy copy of this array (experimental).

        Returns
        -------
        array : numpy.ndarray
        """
        cdef:
            PyObject* out
            PandasOptions c_options
            object values

        if self.type.id == _Type_EXTENSION:
            storage_array = chunked_array(
                [chunk.storage for chunk in self.iterchunks()],
                type=self.type.storage_type
            )
            return storage_array.to_numpy()

        with nogil:
            check_status(
                ConvertChunkedArrayToPandas(
                    c_options,
                    self.sp_chunked_array,
                    self,
                    &out
                )
            )

        # wrap_array_output uses pandas to convert to Categorical, here
        # always convert to numpy array
        values = PyObject_to_object(out)

        if isinstance(values, dict):
            values = np.take(values['dictionary'], values['indices'])

        return values

    def __array__(self, dtype=None):
        values = self.to_numpy()
        if dtype is None:
            return values
        return values.astype(dtype)

    def cast(self, object target_type, safe=True):
        """
        Cast array values to another data type

        See pyarrow.compute.cast for usage
        """
        return _pc().cast(self, target_type, safe=safe)

    def dictionary_encode(self, null_encoding='mask'):
        """
        Compute dictionary-encoded representation of array

        Returns
        -------
        pyarrow.ChunkedArray
            Same chunking as the input, all chunks share a common dictionary.
        """
        options = _pc().DictionaryEncodeOptions(null_encoding)
        return _pc().call_function('dictionary_encode', [self], options)

    def flatten(self, MemoryPool memory_pool=None):
        """
        Flatten this ChunkedArray.  If it has a struct type, the column is
        flattened into one array per struct field.

        Parameters
        ----------
        memory_pool : MemoryPool, default None
            For memory allocations, if required, otherwise use default pool

        Returns
        -------
        result : list of ChunkedArray
        """
        cdef:
            vector[shared_ptr[CChunkedArray]] flattened
            CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)

        with nogil:
            flattened = GetResultValue(self.chunked_array.Flatten(pool))

        return [pyarrow_wrap_chunked_array(col) for col in flattened]

    def combine_chunks(self, MemoryPool memory_pool=None):
        """
        Flatten this ChunkedArray into a single non-chunked array.

        Parameters
        ----------
        memory_pool : MemoryPool, default None
            For memory allocations, if required, otherwise use default pool

        Returns
        -------
        result : Array
        """
        return concat_arrays(self.chunks)

    def unique(self):
        """
        Compute distinct elements in array

        Returns
        -------
        pyarrow.Array
        """
        return _pc().call_function('unique', [self])

    def value_counts(self):
        """
        Compute counts of unique elements in array.

        Returns
        -------
        An array of  <input type "Values", int64_t "Counts"> structs
        """
        return _pc().call_function('value_counts', [self])

    def slice(self, offset=0, length=None):
        """
        Compute zero-copy slice of this ChunkedArray

        Parameters
        ----------
        offset : int, default 0
            Offset from start of array to slice
        length : int, default None
            Length of slice (default is until end of batch starting from
            offset)

        Returns
        -------
        sliced : ChunkedArray
        """
        cdef shared_ptr[CChunkedArray] result

        if offset < 0:
            raise IndexError('Offset must be non-negative')

        offset = min(len(self), offset)
        if length is None:
            result = self.chunked_array.Slice(offset)
        else:
            result = self.chunked_array.Slice(offset, length)

        return pyarrow_wrap_chunked_array(result)

    def filter(self, mask, object null_selection_behavior="drop"):
        """
        Select values from a chunked array. See pyarrow.compute.filter for full
        usage.
        """
        return _pc().filter(self, mask, null_selection_behavior)

    def index(self, value, start=None, end=None, *, memory_pool=None):
        """
        Find the first index of a value.

        See pyarrow.compute.index for full usage.
        """
        return _pc().index(self, value, start, end, memory_pool=memory_pool)

    def take(self, object indices):
        """
        Select values from a chunked array. See pyarrow.compute.take for full
        usage.
        """
        return _pc().take(self, indices)

    def drop_null(self):
        """
        Remove missing values from a chunked array.
        See pyarrow.compute.drop_null for full description.
        """
        return _pc().drop_null(self)

    def unify_dictionaries(self, MemoryPool memory_pool=None):
        """
        Unify dictionaries across all chunks.

        This method returns an equivalent chunked array, but where all
        chunks share the same dictionary values.  Dictionary indices are
        transposed accordingly.

        If there are no dictionaries in the chunked array, it is returned
        unchanged.

        Parameters
        ----------
        memory_pool : MemoryPool, default None
            For memory allocations, if required, otherwise use default pool

        Returns
        -------
        result : ChunkedArray
        """
        cdef:
            CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
            shared_ptr[CChunkedArray] c_result

        with nogil:
            c_result = GetResultValue(CDictionaryUnifier.UnifyChunkedArray(
                self.sp_chunked_array, pool))

        return pyarrow_wrap_chunked_array(c_result)

    @property
    def num_chunks(self):
        """
        Number of underlying chunks

        Returns
        -------
        int
        """
        return self.chunked_array.num_chunks()

    def chunk(self, i):
        """
        Select a chunk by its index

        Parameters
        ----------
        i : int

        Returns
        -------
        pyarrow.Array
        """
        if i >= self.num_chunks or i < 0:
            raise IndexError('Chunk index out of range.')

        return pyarrow_wrap_array(self.chunked_array.chunk(i))

    @property
    def chunks(self):
        return list(self.iterchunks())

    def iterchunks(self):
        for i in range(self.num_chunks):
            yield self.chunk(i)

    def to_pylist(self):
        """
        Convert to a list of native Python objects.
        """
        result = []
        for i in range(self.num_chunks):
            result += self.chunk(i).to_pylist()
        return result


def chunked_array(arrays, type=None):
    """
    Construct chunked array from list of array-like objects

    Parameters
    ----------
    arrays : Array, list of Array, or array-like
        Must all be the same data type. Can be empty only if type also passed.
    type : DataType or string coercible to DataType

    Returns
    -------
    ChunkedArray
    """
    cdef:
        Array arr
        vector[shared_ptr[CArray]] c_arrays
        shared_ptr[CChunkedArray] sp_chunked_array

    type = ensure_type(type, allow_none=True)

    if isinstance(arrays, Array):
        arrays = [arrays]

    for x in arrays:
        arr = x if isinstance(x, Array) else array(x, type=type)

        if type is None:
            # it allows more flexible chunked array construction from to coerce
            # subsequent arrays to the firstly inferred array type
            # it also spares the inference overhead after the first chunk
            type = arr.type
        else:
            if arr.type != type:
                raise TypeError(
                    "All array chunks must have type {}".format(type)
                )

        c_arrays.push_back(arr.sp_array)

    if c_arrays.size() == 0 and type is None:
        raise ValueError("When passing an empty collection of arrays "
                         "you must also pass the data type")

    sp_chunked_array.reset(
        new CChunkedArray(c_arrays, pyarrow_unwrap_data_type(type))
    )
    with nogil:
        check_status(sp_chunked_array.get().Validate())

    return pyarrow_wrap_chunked_array(sp_chunked_array)


cdef _schema_from_arrays(arrays, names, metadata, shared_ptr[CSchema]* schema):
    cdef:
        Py_ssize_t K = len(arrays)
        c_string c_name
        shared_ptr[CDataType] c_type
        shared_ptr[const CKeyValueMetadata] c_meta
        vector[shared_ptr[CField]] c_fields

    if metadata is not None:
        c_meta = KeyValueMetadata(metadata).unwrap()

    if K == 0:
        if names is None or len(names) == 0:
            schema.reset(new CSchema(c_fields, c_meta))
            return arrays
        else:
            raise ValueError('Length of names ({}) does not match '
                             'length of arrays ({})'.format(len(names), K))

    c_fields.resize(K)

    if names is None:
        raise ValueError('Must pass names or schema when constructing '
                         'Table or RecordBatch.')

    if len(names) != K:
        raise ValueError('Length of names ({}) does not match '
                         'length of arrays ({})'.format(len(names), K))

    converted_arrays = []
    for i in range(K):
        val = arrays[i]
        if not isinstance(val, (Array, ChunkedArray)):
            val = array(val)

        c_type = (<DataType> val.type).sp_type

        if names[i] is None:
            c_name = b'None'
        else:
            c_name = tobytes(names[i])
        c_fields[i].reset(new CField(c_name, c_type, True))
        converted_arrays.append(val)

    schema.reset(new CSchema(c_fields, c_meta))
    return converted_arrays


cdef _sanitize_arrays(arrays, names, schema, metadata,
                      shared_ptr[CSchema]* c_schema):
    cdef Schema cy_schema
    if schema is None:
        converted_arrays = _schema_from_arrays(arrays, names, metadata,
                                               c_schema)
    else:
        if names is not None:
            raise ValueError('Cannot pass both schema and names')
        if metadata is not None:
            raise ValueError('Cannot pass both schema and metadata')
        cy_schema = schema

        if len(schema) != len(arrays):
            raise ValueError('Schema and number of arrays unequal')

        c_schema[0] = cy_schema.sp_schema
        converted_arrays = []
        for i, item in enumerate(arrays):
            item = asarray(item, type=schema[i].type)
            converted_arrays.append(item)
    return converted_arrays


cdef class RecordBatch(_PandasConvertible):
    """
    Batch of rows of columns of equal length

    Warnings
    --------
    Do not call this class's constructor directly, use one of the
    ``RecordBatch.from_*`` functions instead.
    """

    def __cinit__(self):
        self.batch = NULL
        self._schema = None

    def __init__(self):
        raise TypeError("Do not call RecordBatch's constructor directly, use "
                        "one of the `RecordBatch.from_*` functions instead.")

    cdef void init(self, const shared_ptr[CRecordBatch]& batch):
        self.sp_batch = batch
        self.batch = batch.get()

    @staticmethod
    def from_pydict(mapping, schema=None, metadata=None):
        """
        Construct a RecordBatch from Arrow arrays or columns.

        Parameters
        ----------
        mapping : dict or Mapping
            A mapping of strings to Arrays or Python lists.
        schema : Schema, default None
            If not passed, will be inferred from the Mapping values.
        metadata : dict or Mapping, default None
            Optional metadata for the schema (if inferred).

        Returns
        -------
        RecordBatch

        Examples
        --------
        >>> import pyarrow as pa
        >>> pydict = {'int': [1, 2], 'str': ['a', 'b']}
        >>> pa.RecordBatch.from_pydict(pydict)
        pyarrow.RecordBatch
        int: int64
        str: string
        """

        return _from_pydict(cls=RecordBatch,
                            mapping=mapping,
                            schema=schema,
                            metadata=metadata)

    @staticmethod
    def from_pylist(mapping, schema=None, metadata=None):
        """
        Construct a RecordBatch from list of rows / dictionaries.

        Parameters
        ----------
        mapping : list of dicts of rows
            A mapping of strings to row values.
        schema : Schema, default None
            If not passed, will be inferred from the first row of the
            mapping values.
        metadata : dict or Mapping, default None
            Optional metadata for the schema (if inferred).

        Returns
        -------
        RecordBatch

        Examples
        --------
        >>> import pyarrow as pa
        >>> pylist = [{'int': 1, 'str': 'a'}, {'int': 2, 'str': 'b'}]
        >>> pa.RecordBatch.from_pylist(pylist)
        pyarrow.RecordBatch
        int: int64
        str: string
        >>> pa.RecordBatch.from_pylist(pylist)[0]
        <pyarrow.lib.Int64Array object at 0x1256b08e0>
        [
          1,
          2
        ]
        """

        return _from_pylist(cls=RecordBatch,
                            mapping=mapping,
                            schema=schema,
                            metadata=metadata)

    def __reduce__(self):
        return _reconstruct_record_batch, (self.columns, self.schema)

    def __len__(self):
        return self.batch.num_rows()

    def __eq__(self, other):
        try:
            return self.equals(other)
        except TypeError:
            return NotImplemented

    def to_string(self, show_metadata=False):
        # Use less verbose schema output.
        schema_as_string = self.schema.to_string(
            show_field_metadata=show_metadata,
            show_schema_metadata=show_metadata
        )
        return 'pyarrow.{}\n{}'.format(type(self).__name__, schema_as_string)

    def __repr__(self):
        return self.to_string()

    def validate(self, *, full=False):
        """
        Perform validation checks.  An exception is raised if validation fails.

        By default only cheap validation checks are run.  Pass `full=True`
        for thorough validation checks (potentially O(n)).

        Parameters
        ----------
        full: bool, default False
            If True, run expensive checks, otherwise cheap checks only.

        Raises
        ------
        ArrowInvalid
        """
        if full:
            with nogil:
                check_status(self.batch.ValidateFull())
        else:
            with nogil:
                check_status(self.batch.Validate())

    def replace_schema_metadata(self, metadata=None):
        """
        Create shallow copy of record batch by replacing schema
        key-value metadata with the indicated new metadata (which may be None,
        which deletes any existing metadata

        Parameters
        ----------
        metadata : dict, default None

        Returns
        -------
        shallow_copy : RecordBatch
        """
        cdef:
            shared_ptr[const CKeyValueMetadata] c_meta
            shared_ptr[CRecordBatch] c_batch

        metadata = ensure_metadata(metadata, allow_none=True)
        c_meta = pyarrow_unwrap_metadata(metadata)
        with nogil:
            c_batch = self.batch.ReplaceSchemaMetadata(c_meta)

        return pyarrow_wrap_batch(c_batch)

    @property
    def num_columns(self):
        """
        Number of columns

        Returns
        -------
        int
        """
        return self.batch.num_columns()

    @property
    def num_rows(self):
        """
        Number of rows

        Due to the definition of a RecordBatch, all columns have the same
        number of rows.

        Returns
        -------
        int
        """
        return len(self)

    @property
    def schema(self):
        """
        Schema of the RecordBatch and its columns

        Returns
        -------
        pyarrow.Schema
        """
        if self._schema is None:
            self._schema = pyarrow_wrap_schema(self.batch.schema())

        return self._schema

    def field(self, i):
        """
        Select a schema field by its column name or numeric index

        Parameters
        ----------
        i : int or string
            The index or name of the field to retrieve

        Returns
        -------
        pyarrow.Field
        """
        return self.schema.field(i)

    @property
    def columns(self):
        """
        List of all columns in numerical order

        Returns
        -------
        list of pyarrow.Array
        """
        return [self.column(i) for i in range(self.num_columns)]

    def _ensure_integer_index(self, i):
        """
        Ensure integer index (convert string column name to integer if needed).
        """
        if isinstance(i, (bytes, str)):
            field_indices = self.schema.get_all_field_indices(i)

            if len(field_indices) == 0:
                raise KeyError(
                    "Field \"{}\" does not exist in record batch schema"
                    .format(i))
            elif len(field_indices) > 1:
                raise KeyError(
                    "Field \"{}\" exists {} times in record batch schema"
                    .format(i, len(field_indices)))
            else:
                return field_indices[0]
        elif isinstance(i, int):
            return i
        else:
            raise TypeError("Index must either be string or integer")

    def column(self, i):
        """
        Select single column from record batch

        Parameters
        ----------
        i : int or string
            The index or name of the column to retrieve.

        Returns
        -------
        column : pyarrow.Array
        """
        return self._column(self._ensure_integer_index(i))

    def _column(self, int i):
        """
        Select single column from record batch by its numeric index.

        Parameters
        ----------
        i : int
            The index of the column to retrieve.

        Returns
        -------
        column : pyarrow.Array
        """
        cdef int index = <int> _normalize_index(i, self.num_columns)
        cdef Array result = pyarrow_wrap_array(self.batch.column(index))
        result._name = self.schema[index].name
        return result

    @property
    def nbytes(self):
        """
        Total number of bytes consumed by the elements of the record batch.

        In other words, the sum of bytes from all buffer ranges referenced.

        Unlike `get_total_buffer_size` this method will account for array
        offsets.

        If buffers are shared between arrays then the shared
        portion will only be counted multiple times.

        The dictionary of dictionary arrays will always be counted in their
        entirety even if the array only references a portion of the dictionary.
        """
        cdef:
            CResult[int64_t] c_res_buffer

        c_res_buffer = ReferencedBufferSize(deref(self.batch))
        size = GetResultValue(c_res_buffer)
        return size

    def get_total_buffer_size(self):
        """
        The sum of bytes in each buffer referenced by the record batch

        An array may only reference a portion of a buffer.
        This method will overestimate in this case and return the
        byte size of the entire buffer.

        If a buffer is referenced multiple times then it will
        only be counted once.
        """
        cdef:
            int64_t total_buffer_size

        total_buffer_size = TotalBufferSize(deref(self.batch))
        return total_buffer_size

    def __sizeof__(self):
        return super(RecordBatch, self).__sizeof__() + self.nbytes

    def __getitem__(self, key):
        """
        Slice or return column at given index or column name

        Parameters
        ----------
        key : integer, str, or slice
            Slices with step not equal to 1 (or None) will produce a copy
            rather than a zero-copy view

        Returns
        -------
        value : Array (index/column) or RecordBatch (slice)
        """
        if isinstance(key, slice):
            return _normalize_slice(self, key)
        else:
            return self.column(key)

    def serialize(self, memory_pool=None):
        """
        Write RecordBatch to Buffer as encapsulated IPC message.

        Parameters
        ----------
        memory_pool : MemoryPool, default None
            Uses default memory pool if not specified

        Returns
        -------
        serialized : Buffer
        """
        cdef shared_ptr[CBuffer] buffer
        cdef CIpcWriteOptions options = CIpcWriteOptions.Defaults()
        options.memory_pool = maybe_unbox_memory_pool(memory_pool)

        with nogil:
            buffer = GetResultValue(
                SerializeRecordBatch(deref(self.batch), options))
        return pyarrow_wrap_buffer(buffer)

    def slice(self, offset=0, length=None):
        """
        Compute zero-copy slice of this RecordBatch

        Parameters
        ----------
        offset : int, default 0
            Offset from start of record batch to slice
        length : int, default None
            Length of slice (default is until end of batch starting from
            offset)

        Returns
        -------
        sliced : RecordBatch
        """
        cdef shared_ptr[CRecordBatch] result

        if offset < 0:
            raise IndexError('Offset must be non-negative')

        offset = min(len(self), offset)
        if length is None:
            result = self.batch.Slice(offset)
        else:
            result = self.batch.Slice(offset, length)

        return pyarrow_wrap_batch(result)

    def filter(self, mask, object null_selection_behavior="drop"):
        """
        Select record from a record batch. See pyarrow.compute.filter for full
        usage.
        """
        return _pc().filter(self, mask, null_selection_behavior)

    def equals(self, object other, bint check_metadata=False):
        """
        Check if contents of two record batches are equal.

        Parameters
        ----------
        other : pyarrow.RecordBatch
            RecordBatch to compare against.
        check_metadata : bool, default False
            Whether schema metadata equality should be checked as well.

        Returns
        -------
        are_equal : bool
        """
        cdef:
            CRecordBatch* this_batch = self.batch
            shared_ptr[CRecordBatch] other_batch = pyarrow_unwrap_batch(other)
            c_bool result

        if not other_batch:
            return False

        with nogil:
            result = this_batch.Equals(deref(other_batch), check_metadata)

        return result

    def take(self, object indices):
        """
        Select records from a RecordBatch. See pyarrow.compute.take for full
        usage.
        """
        return _pc().take(self, indices)

    def drop_null(self):
        """
        Remove missing values from a RecordBatch.
        See pyarrow.compute.drop_null for full usage.
        """
        return _pc().drop_null(self)

    def to_pydict(self):
        """
        Convert the RecordBatch to a dict or OrderedDict.

        Returns
        -------
        dict
        """
        entries = []
        for i in range(self.batch.num_columns()):
            name = bytes(self.batch.column_name(i)).decode('utf8')
            column = self[i].to_pylist()
            entries.append((name, column))
        return ordered_dict(entries)

    def to_pylist(self):
        """
        Convert the RecordBatch to a list of rows / dictionaries.

        Returns
        -------
        list
        """

        pydict = self.to_pydict()
        names = self.schema.names
        pylist = [{column: pydict[column][row] for column in names}
                  for row in range(self.num_rows)]
        return pylist

    def _to_pandas(self, options, **kwargs):
        return Table.from_batches([self])._to_pandas(options, **kwargs)

    @classmethod
    def from_pandas(cls, df, Schema schema=None, preserve_index=None,
                    nthreads=None, columns=None):
        """
        Convert pandas.DataFrame to an Arrow RecordBatch

        Parameters
        ----------
        df : pandas.DataFrame
        schema : pyarrow.Schema, optional
            The expected schema of the RecordBatch. This can be used to
            indicate the type of columns if we cannot infer it automatically.
            If passed, the output will have exactly this schema. Columns
            specified in the schema that are not found in the DataFrame columns
            or its index will raise an error. Additional columns or index
            levels in the DataFrame which are not specified in the schema will
            be ignored.
        preserve_index : bool, optional
            Whether to store the index as an additional column in the resulting
            ``RecordBatch``. The default of None will store the index as a
            column, except for RangeIndex which is stored as metadata only. Use
            ``preserve_index=True`` to force it to be stored as a column.
        nthreads : int, default None
            If greater than 1, convert columns to Arrow in parallel using
            indicated number of threads. By default, this follows
            :func:`pyarrow.cpu_count` (may use up to system CPU count threads).
        columns : list, optional
           List of column to be converted. If None, use all columns.

        Returns
        -------
        pyarrow.RecordBatch
        """
        from pyarrow.pandas_compat import dataframe_to_arrays
        arrays, schema = dataframe_to_arrays(
            df, schema, preserve_index, nthreads=nthreads, columns=columns
        )
        return cls.from_arrays(arrays, schema=schema)

    @staticmethod
    def from_arrays(list arrays, names=None, schema=None, metadata=None):
        """
        Construct a RecordBatch from multiple pyarrow.Arrays

        Parameters
        ----------
        arrays : list of pyarrow.Array
            One for each field in RecordBatch
        names : list of str, optional
            Names for the batch fields. If not passed, schema must be passed
        schema : Schema, default None
            Schema for the created batch. If not passed, names must be passed
        metadata : dict or Mapping, default None
            Optional metadata for the schema (if inferred).

        Returns
        -------
        pyarrow.RecordBatch
        """
        cdef:
            Array arr
            shared_ptr[CSchema] c_schema
            vector[shared_ptr[CArray]] c_arrays
            int64_t num_rows

        if len(arrays) > 0:
            num_rows = len(arrays[0])
        else:
            num_rows = 0

        if isinstance(names, Schema):
            import warnings
            warnings.warn("Schema passed to names= option, please "
                          "pass schema= explicitly. "
                          "Will raise exception in future", FutureWarning)
            schema = names
            names = None

        converted_arrays = _sanitize_arrays(arrays, names, schema, metadata,
                                            &c_schema)

        c_arrays.reserve(len(arrays))
        for arr in converted_arrays:
            if len(arr) != num_rows:
                raise ValueError('Arrays were not all the same length: '
                                 '{0} vs {1}'.format(len(arr), num_rows))
            c_arrays.push_back(arr.sp_array)

        result = pyarrow_wrap_batch(CRecordBatch.Make(c_schema, num_rows,
                                                      c_arrays))
        result.validate()
        return result

    @staticmethod
    def from_struct_array(StructArray struct_array):
        """
        Construct a RecordBatch from a StructArray.

        Each field in the StructArray will become a column in the resulting
        ``RecordBatch``.

        Parameters
        ----------
        struct_array : StructArray
            Array to construct the record batch from.

        Returns
        -------
        pyarrow.RecordBatch
        """
        cdef:
            shared_ptr[CRecordBatch] c_record_batch
        with nogil:
            c_record_batch = GetResultValue(
                CRecordBatch.FromStructArray(struct_array.sp_array))
        return pyarrow_wrap_batch(c_record_batch)

    def _export_to_c(self, out_ptr, out_schema_ptr=0):
        """
        Export to a C ArrowArray struct, given its pointer.

        If a C ArrowSchema struct pointer is also given, the record batch
        schema is exported to it at the same time.

        Parameters
        ----------
        out_ptr: int
            The raw pointer to a C ArrowArray struct.
        out_schema_ptr: int (optional)
            The raw pointer to a C ArrowSchema struct.

        Be careful: if you don't pass the ArrowArray struct to a consumer,
        array memory will leak.  This is a low-level function intended for
        expert users.
        """
        cdef:
            void* c_ptr = _as_c_pointer(out_ptr)
            void* c_schema_ptr = _as_c_pointer(out_schema_ptr,
                                               allow_null=True)
        with nogil:
            check_status(ExportRecordBatch(deref(self.sp_batch),
                                           <ArrowArray*> c_ptr,
                                           <ArrowSchema*> c_schema_ptr))

    @staticmethod
    def _import_from_c(in_ptr, schema):
        """
        Import RecordBatch from a C ArrowArray struct, given its pointer
        and the imported schema.

        Parameters
        ----------
        in_ptr: int
            The raw pointer to a C ArrowArray struct.
        type: Schema or int
            Either a Schema object, or the raw pointer to a C ArrowSchema
            struct.

        This is a low-level function intended for expert users.
        """
        cdef:
            void* c_ptr = _as_c_pointer(in_ptr)
            void* c_schema_ptr
            shared_ptr[CRecordBatch] c_batch

        c_schema = pyarrow_unwrap_schema(schema)
        if c_schema == nullptr:
            # Not a Schema object, perhaps a raw ArrowSchema pointer
            c_schema_ptr = _as_c_pointer(schema, allow_null=True)
            with nogil:
                c_batch = GetResultValue(ImportRecordBatch(
                    <ArrowArray*> c_ptr, <ArrowSchema*> c_schema_ptr))
        else:
            with nogil:
                c_batch = GetResultValue(ImportRecordBatch(
                    <ArrowArray*> c_ptr, c_schema))
        return pyarrow_wrap_batch(c_batch)


def _reconstruct_record_batch(columns, schema):
    """
    Internal: reconstruct RecordBatch from pickled components.
    """
    return RecordBatch.from_arrays(columns, schema=schema)


def table_to_blocks(options, Table table, categories, extension_columns):
    cdef:
        PyObject* result_obj
        shared_ptr[CTable] c_table
        CMemoryPool* pool
        PandasOptions c_options = _convert_pandas_options(options)

    if categories is not None:
        c_options.categorical_columns = {tobytes(cat) for cat in categories}
    if extension_columns is not None:
        c_options.extension_columns = {tobytes(col)
                                       for col in extension_columns}

    # ARROW-3789(wesm); Convert date/timestamp types to datetime64[ns]
    c_options.coerce_temporal_nanoseconds = True

    if c_options.self_destruct:
        # Move the shared_ptr, table is now unsafe to use further
        c_table = move(table.sp_table)
        table.table = NULL
    else:
        c_table = table.sp_table

    with nogil:
        check_status(
            libarrow.ConvertTableToPandas(c_options, move(c_table),
                                          &result_obj)
        )

    return PyObject_to_object(result_obj)


cdef class Table(_PandasConvertible):
    """
    A collection of top-level named, equal length Arrow arrays.

    Warnings
    --------
    Do not call this class's constructor directly, use one of the ``from_*``
    methods instead.
    """

    def __cinit__(self):
        self.table = NULL

    def __init__(self):
        raise TypeError("Do not call Table's constructor directly, use one of "
                        "the `Table.from_*` functions instead.")

    def to_string(self, *, show_metadata=False, preview_cols=0):
        """
        Return human-readable string representation of Table.

        Parameters
        ----------
        show_metadata : bool, default False
            Display Field-level and Schema-level KeyValueMetadata.
        preview_cols : int, default 0
            Display values of the columns for the first N columns.

        Returns
        -------
        str
        """
        # Use less verbose schema output.
        schema_as_string = self.schema.to_string(
            show_field_metadata=show_metadata,
            show_schema_metadata=show_metadata
        )
        title = 'pyarrow.{}\n{}'.format(type(self).__name__, schema_as_string)
        pieces = [title]
        if preview_cols:
            pieces.append('----')
            for i in range(min(self.num_columns, preview_cols)):
                pieces.append('{}: {}'.format(
                    self.field(i).name,
                    self.column(i).to_string(indent=0, skip_new_lines=True)
                ))
            if preview_cols < self.num_columns:
                pieces.append('...')
        return '\n'.join(pieces)

    def __repr__(self):
        if self.table == NULL:
            raise ValueError("Table's internal pointer is NULL, do not use "
                             "any methods or attributes on this object")
        return self.to_string(preview_cols=10)

    cdef void init(self, const shared_ptr[CTable]& table):
        self.sp_table = table
        self.table = table.get()

    def validate(self, *, full=False):
        """
        Perform validation checks.  An exception is raised if validation fails.

        By default only cheap validation checks are run.  Pass `full=True`
        for thorough validation checks (potentially O(n)).

        Parameters
        ----------
        full: bool, default False
            If True, run expensive checks, otherwise cheap checks only.

        Raises
        ------
        ArrowInvalid
        """
        if full:
            with nogil:
                check_status(self.table.ValidateFull())
        else:
            with nogil:
                check_status(self.table.Validate())

    def __reduce__(self):
        # Reduce the columns as ChunkedArrays to avoid serializing schema
        # data twice
        columns = [col for col in self.columns]
        return _reconstruct_table, (columns, self.schema)

    def __getitem__(self, key):
        """
        Slice or return column at given index or column name.

        Parameters
        ----------
        key : integer, str, or slice
            Slices with step not equal to 1 (or None) will produce a copy
            rather than a zero-copy view.

        Returns
        -------
        ChunkedArray (index/column) or Table (slice)
        """
        if isinstance(key, slice):
            return _normalize_slice(self, key)
        else:
            return self.column(key)

    def slice(self, offset=0, length=None):
        """
        Compute zero-copy slice of this Table.

        Parameters
        ----------
        offset : int, default 0
            Offset from start of table to slice.
        length : int, default None
            Length of slice (default is until end of table starting from
            offset).

        Returns
        -------
        Table
        """
        cdef shared_ptr[CTable] result

        if offset < 0:
            raise IndexError('Offset must be non-negative')

        offset = min(len(self), offset)
        if length is None:
            result = self.table.Slice(offset)
        else:
            result = self.table.Slice(offset, length)

        return pyarrow_wrap_table(result)

    def filter(self, mask, object null_selection_behavior="drop"):
        """
        Select records from a Table. See :func:`pyarrow.compute.filter` for
        full usage.
        """
        return _pc().filter(self, mask, null_selection_behavior)

    def take(self, object indices):
        """
        Select records from a Table. See :func:`pyarrow.compute.take` for full
        usage.
        """
        return _pc().take(self, indices)

    def drop_null(self):
        """
        Remove missing values from a Table.
        See :func:`pyarrow.compute.drop_null` for full usage.
        """
        return _pc().drop_null(self)

    def select(self, object columns):
        """
        Select columns of the Table.

        Returns a new Table with the specified columns, and metadata
        preserved.

        Parameters
        ----------
        columns : list-like
            The column names or integer indices to select.

        Returns
        -------
        Table
        """
        cdef:
            shared_ptr[CTable] c_table
            vector[int] c_indices

        for idx in columns:
            idx = self._ensure_integer_index(idx)
            idx = _normalize_index(idx, self.num_columns)
            c_indices.push_back(<int> idx)

        with nogil:
            c_table = GetResultValue(self.table.SelectColumns(move(c_indices)))

        return pyarrow_wrap_table(c_table)

    def replace_schema_metadata(self, metadata=None):
        """
        Create shallow copy of table by replacing schema
        key-value metadata with the indicated new metadata (which may be None),
        which deletes any existing metadata.

        Parameters
        ----------
        metadata : dict, default None

        Returns
        -------
        Table
        """
        cdef:
            shared_ptr[const CKeyValueMetadata] c_meta
            shared_ptr[CTable] c_table

        metadata = ensure_metadata(metadata, allow_none=True)
        c_meta = pyarrow_unwrap_metadata(metadata)
        with nogil:
            c_table = self.table.ReplaceSchemaMetadata(c_meta)

        return pyarrow_wrap_table(c_table)

    def flatten(self, MemoryPool memory_pool=None):
        """
        Flatten this Table.

        Each column with a struct type is flattened
        into one column per struct field.  Other columns are left unchanged.

        Parameters
        ----------
        memory_pool : MemoryPool, default None
            For memory allocations, if required, otherwise use default pool

        Returns
        -------
        Table
        """
        cdef:
            shared_ptr[CTable] flattened
            CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)

        with nogil:
            flattened = GetResultValue(self.table.Flatten(pool))

        return pyarrow_wrap_table(flattened)

    def combine_chunks(self, MemoryPool memory_pool=None):
        """
        Make a new table by combining the chunks this table has.

        All the underlying chunks in the ChunkedArray of each column are
        concatenated into zero or one chunk.

        Parameters
        ----------
        memory_pool : MemoryPool, default None
            For memory allocations, if required, otherwise use default pool.

        Returns
        -------
        Table
        """
        cdef:
            shared_ptr[CTable] combined
            CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)

        with nogil:
            combined = GetResultValue(self.table.CombineChunks(pool))

        return pyarrow_wrap_table(combined)

    def unify_dictionaries(self, MemoryPool memory_pool=None):
        """
        Unify dictionaries across all chunks.

        This method returns an equivalent table, but where all chunks of
        each column share the same dictionary values.  Dictionary indices
        are transposed accordingly.

        Columns without dictionaries are returned unchanged.

        Parameters
        ----------
        memory_pool : MemoryPool, default None
            For memory allocations, if required, otherwise use default pool

        Returns
        -------
        Table
        """
        cdef:
            CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
            shared_ptr[CTable] c_result

        with nogil:
            c_result = GetResultValue(CDictionaryUnifier.UnifyTable(
                deref(self.table), pool))

        return pyarrow_wrap_table(c_result)

    def __eq__(self, other):
        try:
            return self.equals(other)
        except TypeError:
            return NotImplemented

    def equals(self, Table other, bint check_metadata=False):
        """
        Check if contents of two tables are equal.

        Parameters
        ----------
        other : pyarrow.Table
            Table to compare against.
        check_metadata : bool, default False
            Whether schema metadata equality should be checked as well.

        Returns
        -------
        bool
        """
        if other is None:
            return False

        cdef:
            CTable* this_table = self.table
            CTable* other_table = other.table
            c_bool result

        with nogil:
            result = this_table.Equals(deref(other_table), check_metadata)

        return result

    def cast(self, Schema target_schema, bint safe=True):
        """
        Cast table values to another schema.

        Parameters
        ----------
        target_schema : Schema
            Schema to cast to, the names and order of fields must match.
        safe : bool, default True
            Check for overflows or other unsafe conversions.

        Returns
        -------
        Table
        """
        cdef:
            ChunkedArray column, casted
            Field field
            list newcols = []

        if self.schema.names != target_schema.names:
            raise ValueError("Target schema's field names are not matching "
                             "the table's field names: {!r}, {!r}"
                             .format(self.schema.names, target_schema.names))

        for column, field in zip(self.itercolumns(), target_schema):
            casted = column.cast(field.type, safe=safe)
            newcols.append(casted)

        return Table.from_arrays(newcols, schema=target_schema)

    @classmethod
    def from_pandas(cls, df, Schema schema=None, preserve_index=None,
                    nthreads=None, columns=None, bint safe=True):
        """
        Convert pandas.DataFrame to an Arrow Table.

        The column types in the resulting Arrow Table are inferred from the
        dtypes of the pandas.Series in the DataFrame. In the case of non-object
        Series, the NumPy dtype is translated to its Arrow equivalent. In the
        case of `object`, we need to guess the datatype by looking at the
        Python objects in this Series.

        Be aware that Series of the `object` dtype don't carry enough
        information to always lead to a meaningful Arrow type. In the case that
        we cannot infer a type, e.g. because the DataFrame is of length 0 or
        the Series only contains None/nan objects, the type is set to
        null. This behavior can be avoided by constructing an explicit schema
        and passing it to this function.

        Parameters
        ----------
        df : pandas.DataFrame
        schema : pyarrow.Schema, optional
            The expected schema of the Arrow Table. This can be used to
            indicate the type of columns if we cannot infer it automatically.
            If passed, the output will have exactly this schema. Columns
            specified in the schema that are not found in the DataFrame columns
            or its index will raise an error. Additional columns or index
            levels in the DataFrame which are not specified in the schema will
            be ignored.
        preserve_index : bool, optional
            Whether to store the index as an additional column in the resulting
            ``Table``. The default of None will store the index as a column,
            except for RangeIndex which is stored as metadata only. Use
            ``preserve_index=True`` to force it to be stored as a column.
        nthreads : int, default None
            If greater than 1, convert columns to Arrow in parallel using
            indicated number of threads. By default, this follows
            :func:`pyarrow.cpu_count` (may use up to system CPU count threads).
        columns : list, optional
           List of column to be converted. If None, use all columns.
        safe : bool, default True
           Check for overflows or other unsafe conversions.

        Returns
        -------
        Table

        Examples
        --------

        >>> import pandas as pd
        >>> import pyarrow as pa
        >>> df = pd.DataFrame({
            ...     'int': [1, 2],
            ...     'str': ['a', 'b']
            ... })
        >>> pa.Table.from_pandas(df)
        <pyarrow.lib.Table object at 0x7f05d1fb1b40>
        """
        from pyarrow.pandas_compat import dataframe_to_arrays
        arrays, schema = dataframe_to_arrays(
            df,
            schema=schema,
            preserve_index=preserve_index,
            nthreads=nthreads,
            columns=columns,
            safe=safe
        )
        return cls.from_arrays(arrays, schema=schema)

    @staticmethod
    def from_arrays(arrays, names=None, schema=None, metadata=None):
        """
        Construct a Table from Arrow arrays.

        Parameters
        ----------
        arrays : list of pyarrow.Array or pyarrow.ChunkedArray
            Equal-length arrays that should form the table.
        names : list of str, optional
            Names for the table columns. If not passed, schema must be passed.
        schema : Schema, default None
            Schema for the created table. If not passed, names must be passed.
        metadata : dict or Mapping, default None
            Optional metadata for the schema (if inferred).

        Returns
        -------
        Table
        """
        cdef:
            vector[shared_ptr[CChunkedArray]] columns
            shared_ptr[CSchema] c_schema
            int i, K = <int> len(arrays)

        converted_arrays = _sanitize_arrays(arrays, names, schema, metadata,
                                            &c_schema)

        columns.reserve(K)
        for item in converted_arrays:
            if isinstance(item, Array):
                columns.push_back(
                    make_shared[CChunkedArray](
                        (<Array> item).sp_array
                    )
                )
            elif isinstance(item, ChunkedArray):
                columns.push_back((<ChunkedArray> item).sp_chunked_array)
            else:
                raise TypeError(type(item))

        result = pyarrow_wrap_table(CTable.Make(c_schema, columns))
        result.validate()
        return result

    @staticmethod
    def from_pydict(mapping, schema=None, metadata=None):
        """
        Construct a Table from Arrow arrays or columns.

        Parameters
        ----------
        mapping : dict or Mapping
            A mapping of strings to Arrays or Python lists.
        schema : Schema, default None
            If not passed, will be inferred from the Mapping values.
        metadata : dict or Mapping, default None
            Optional metadata for the schema (if inferred).

        Returns
        -------
        Table

        Examples
        --------
        >>> import pyarrow as pa
        >>> pydict = {'int': [1, 2], 'str': ['a', 'b']}
        >>> pa.Table.from_pydict(pydict)
        pyarrow.Table
        int: int64
        str: string
        ----
        int: [[1,2]]
        str: [["a","b"]]
        """

        return _from_pydict(cls=Table,
                            mapping=mapping,
                            schema=schema,
                            metadata=metadata)

    @staticmethod
    def from_pylist(mapping, schema=None, metadata=None):
        """
        Construct a Table from list of rows / dictionaries.

        Parameters
        ----------
        mapping : list of dicts of rows
            A mapping of strings to row values.
        schema : Schema, default None
            If not passed, will be inferred from the first row of the
            mapping values.
        metadata : dict or Mapping, default None
            Optional metadata for the schema (if inferred).

        Returns
        -------
        Table

        Examples
        --------
        >>> import pyarrow as pa
        >>> pylist = [{'int': 1, 'str': 'a'}, {'int': 2, 'str': 'b'}]
        >>> pa.Table.from_pylist(pylist)
        pyarrow.Table
        int: int64
        str: string
        ----
        int: [[1,2]]
        str: [["a","b"]]
        """

        return _from_pylist(cls=Table,
                            mapping=mapping,
                            schema=schema,
                            metadata=metadata)

    @staticmethod
    def from_batches(batches, Schema schema=None):
        """
        Construct a Table from a sequence or iterator of Arrow RecordBatches.

        Parameters
        ----------
        batches : sequence or iterator of RecordBatch
            Sequence of RecordBatch to be converted, all schemas must be equal.
        schema : Schema, default None
            If not passed, will be inferred from the first RecordBatch.

        Returns
        -------
        Table
        """
        cdef:
            vector[shared_ptr[CRecordBatch]] c_batches
            shared_ptr[CTable] c_table
            shared_ptr[CSchema] c_schema
            RecordBatch batch

        for batch in batches:
            c_batches.push_back(batch.sp_batch)

        if schema is None:
            if c_batches.size() == 0:
                raise ValueError('Must pass schema, or at least '
                                 'one RecordBatch')
            c_schema = c_batches[0].get().schema()
        else:
            c_schema = schema.sp_schema

        with nogil:
            c_table = GetResultValue(
                CTable.FromRecordBatches(c_schema, move(c_batches)))

        return pyarrow_wrap_table(c_table)

    def to_batches(self, max_chunksize=None, **kwargs):
        """
        Convert Table to list of (contiguous) RecordBatch objects.

        Parameters
        ----------
        max_chunksize : int, default None
            Maximum size for RecordBatch chunks. Individual chunks may be
            smaller depending on the chunk layout of individual columns.

        Returns
        -------
        list[RecordBatch]
        """
        cdef:
            unique_ptr[TableBatchReader] reader
            int64_t c_max_chunksize
            list result = []
            shared_ptr[CRecordBatch] batch

        reader.reset(new TableBatchReader(deref(self.table)))

        if 'chunksize' in kwargs:
            max_chunksize = kwargs['chunksize']
            msg = ('The parameter chunksize is deprecated for '
                   'pyarrow.Table.to_batches as of 0.15, please use '
                   'the parameter max_chunksize instead')
            warnings.warn(msg, FutureWarning)

        if max_chunksize is not None:
            c_max_chunksize = max_chunksize
            reader.get().set_chunksize(c_max_chunksize)

        while True:
            with nogil:
                check_status(reader.get().ReadNext(&batch))

            if batch.get() == NULL:
                break

            result.append(pyarrow_wrap_batch(batch))

        return result

    def _to_pandas(self, options, categories=None, ignore_metadata=False,
                   types_mapper=None):
        from pyarrow.pandas_compat import table_to_blockmanager
        mgr = table_to_blockmanager(
            options, self, categories,
            ignore_metadata=ignore_metadata,
            types_mapper=types_mapper)
        return pandas_api.data_frame(mgr)

    def to_pydict(self):
        """
        Convert the Table to a dict or OrderedDict.

        Returns
        -------
        dict

        Examples
        --------
        >>> import pyarrow as pa
        >>> table = pa.table([
        ...     pa.array([1, 2]),
        ...     pa.array(["a", "b"])
        ... ], names=["int", "str"])
        >>> table.to_pydict()
        {'int': [1, 2], 'str': ['a', 'b']}
        """
        cdef:
            size_t i
            size_t num_columns = self.table.num_columns()
            list entries = []
            ChunkedArray column

        for i in range(num_columns):
            column = self.column(i)
            entries.append((self.field(i).name, column.to_pylist()))

        return ordered_dict(entries)

    def to_pylist(self):
        """
        Convert the Table to a list of rows / dictionaries.

        Returns
        -------
        list

        Examples
        --------
        >>> import pyarrow as pa
        >>> table = pa.table([
        ...     pa.array([1, 2]),
        ...     pa.array(["a", "b"])
        ... ], names=["int", "str"])
        >>> table.to_pylist()
        [{'int': 1, 'str': 'a'}, {'int': 2, 'str': 'b'}]
        """
        pydict = self.to_pydict()
        names = self.schema.names
        pylist = [{column: pydict[column][row] for column in names}
                  for row in range(self.num_rows)]
        return pylist

    @property
    def schema(self):
        """
        Schema of the table and its columns.

        Returns
        -------
        Schema
        """
        return pyarrow_wrap_schema(self.table.schema())

    def field(self, i):
        """
        Select a schema field by its column name or numeric index.

        Parameters
        ----------
        i : int or string
            The index or name of the field to retrieve.

        Returns
        -------
        Field
        """
        return self.schema.field(i)

    def _ensure_integer_index(self, i):
        """
        Ensure integer index (convert string column name to integer if needed).
        """
        if isinstance(i, (bytes, str)):
            field_indices = self.schema.get_all_field_indices(i)

            if len(field_indices) == 0:
                raise KeyError("Field \"{}\" does not exist in table schema"
                               .format(i))
            elif len(field_indices) > 1:
                raise KeyError("Field \"{}\" exists {} times in table schema"
                               .format(i, len(field_indices)))
            else:
                return field_indices[0]
        elif isinstance(i, int):
            return i
        else:
            raise TypeError("Index must either be string or integer")

    def column(self, i):
        """
        Select a column by its column name, or numeric index.

        Parameters
        ----------
        i : int or string
            The index or name of the column to retrieve.

        Returns
        -------
        ChunkedArray
        """
        return self._column(self._ensure_integer_index(i))

    def _column(self, int i):
        """
        Select a column by its numeric index.

        Parameters
        ----------
        i : int
            The index of the column to retrieve.

        Returns
        -------
        ChunkedArray
        """
        cdef int index = <int> _normalize_index(i, self.num_columns)
        cdef ChunkedArray result = pyarrow_wrap_chunked_array(
            self.table.column(index))
        result._name = self.schema[index].name
        return result

    def itercolumns(self):
        """
        Iterator over all columns in their numerical order.

        Yields
        ------
        ChunkedArray
        """
        for i in range(self.num_columns):
            yield self._column(i)

    @property
    def columns(self):
        """
        List of all columns in numerical order.

        Returns
        -------
        list of ChunkedArray
        """
        return [self._column(i) for i in range(self.num_columns)]

    @property
    def num_columns(self):
        """
        Number of columns in this table.

        Returns
        -------
        int
        """
        return self.table.num_columns()

    @property
    def num_rows(self):
        """
        Number of rows in this table.

        Due to the definition of a table, all columns have the same number of
        rows.

        Returns
        -------
        int
        """
        return self.table.num_rows()

    def __len__(self):
        return self.num_rows

    @property
    def shape(self):
        """
        Dimensions of the table: (#rows, #columns).

        Returns
        -------
        (int, int)
            Number of rows and number of columns.
        """
        return (self.num_rows, self.num_columns)

    @property
    def nbytes(self):
        """
        Total number of bytes consumed by the elements of the table.

        In other words, the sum of bytes from all buffer ranges referenced.

        Unlike `get_total_buffer_size` this method will account for array
        offsets.

        If buffers are shared between arrays then the shared
        portion will only be counted multiple times.

        The dictionary of dictionary arrays will always be counted in their
        entirety even if the array only references a portion of the dictionary.
        """
        cdef:
            CResult[int64_t] c_res_buffer

        c_res_buffer = ReferencedBufferSize(deref(self.table))
        size = GetResultValue(c_res_buffer)
        return size

    def get_total_buffer_size(self):
        """
        The sum of bytes in each buffer referenced by the table.

        An array may only reference a portion of a buffer.
        This method will overestimate in this case and return the
        byte size of the entire buffer.

        If a buffer is referenced multiple times then it will
        only be counted once.
        """
        cdef:
            int64_t total_buffer_size

        total_buffer_size = TotalBufferSize(deref(self.table))
        return total_buffer_size

    def __sizeof__(self):
        return super(Table, self).__sizeof__() + self.nbytes

    def add_column(self, int i, field_, column):
        """
        Add column to Table at position.

        A new table is returned with the column added, the original table
        object is left unchanged.

        Parameters
        ----------
        i : int
            Index to place the column at.
        field_ : str or Field
            If a string is passed then the type is deduced from the column
            data.
        column : Array, list of Array, or values coercible to arrays
            Column data.

        Returns
        -------
        Table
            New table with the passed column added.
        """
        cdef:
            shared_ptr[CTable] c_table
            Field c_field
            ChunkedArray c_arr

        if isinstance(column, ChunkedArray):
            c_arr = column
        else:
            c_arr = chunked_array(column)

        if isinstance(field_, Field):
            c_field = field_
        else:
            c_field = field(field_, c_arr.type)

        with nogil:
            c_table = GetResultValue(self.table.AddColumn(
                i, c_field.sp_field, c_arr.sp_chunked_array))

        return pyarrow_wrap_table(c_table)

    def append_column(self, field_, column):
        """
        Append column at end of columns.

        Parameters
        ----------
        field_ : str or Field
            If a string is passed then the type is deduced from the column
            data.
        column : Array, list of Array, or values coercible to arrays
            Column data.

        Returns
        -------
        Table
            New table with the passed column added.
        """
        return self.add_column(self.num_columns, field_, column)

    def remove_column(self, int i):
        """
        Create new Table with the indicated column removed.

        Parameters
        ----------
        i : int
            Index of column to remove.

        Returns
        -------
        Table
            New table without the column.
        """
        cdef shared_ptr[CTable] c_table

        with nogil:
            c_table = GetResultValue(self.table.RemoveColumn(i))

        return pyarrow_wrap_table(c_table)

    def set_column(self, int i, field_, column):
        """
        Replace column in Table at position.

        Parameters
        ----------
        i : int
            Index to place the column at.
        field_ : str or Field
            If a string is passed then the type is deduced from the column
            data.
        column : Array, list of Array, or values coercible to arrays
            Column data.

        Returns
        -------
        Table
            New table with the passed column set.
        """
        cdef:
            shared_ptr[CTable] c_table
            Field c_field
            ChunkedArray c_arr

        if isinstance(column, ChunkedArray):
            c_arr = column
        else:
            c_arr = chunked_array(column)

        if isinstance(field_, Field):
            c_field = field_
        else:
            c_field = field(field_, c_arr.type)

        with nogil:
            c_table = GetResultValue(self.table.SetColumn(
                i, c_field.sp_field, c_arr.sp_chunked_array))

        return pyarrow_wrap_table(c_table)

    @property
    def column_names(self):
        """
        Names of the table's columns.

        Returns
        -------
        list of str
        """
        names = self.table.ColumnNames()
        return [frombytes(name) for name in names]

    def rename_columns(self, names):
        """
        Create new table with columns renamed to provided names.

        Parameters
        ----------
        names : list of str
            List of new column names.

        Returns
        -------
        Table
        """
        cdef:
            shared_ptr[CTable] c_table
            vector[c_string] c_names

        for name in names:
            c_names.push_back(tobytes(name))

        with nogil:
            c_table = GetResultValue(self.table.RenameColumns(move(c_names)))

        return pyarrow_wrap_table(c_table)

    def drop(self, columns):
        """
        Drop one or more columns and return a new table.

        Parameters
        ----------
        columns : list of str
            List of field names referencing existing columns.

        Raises
        ------
        KeyError
            If any of the passed columns name are not existing.

        Returns
        -------
        Table
            New table without the columns.
        """
        indices = []
        for col in columns:
            idx = self.schema.get_field_index(col)
            if idx == -1:
                raise KeyError("Column {!r} not found".format(col))
            indices.append(idx)

        indices.sort()
        indices.reverse()

        table = self
        for idx in indices:
            table = table.remove_column(idx)

        return table

    def group_by(self, keys):
        """Declare a grouping over the columns of the table.

        Resulting grouping can then be used to perform aggregations
        with a subsequent ``aggregate()`` method.

        Parameters
        ----------
        keys : str or list[str]
            Name of the columns that should be used as the grouping key.

        Returns
        -------
        TableGroupBy

        See Also
        --------
        TableGroupBy.aggregate
        """
        return TableGroupBy(self, keys)

    def sort_by(self, sorting):
        """
        Sort the table by one or multiple columns.

        Parameters
        ----------
        sorting : str or list[tuple(name, order)]
            Name of the column to use to sort (ascending), or
            a list of multiple sorting conditions where
            each entry is a tuple with column name
            and sorting order ("ascending" or "descending")

        Returns
        -------
        Table
            A new table sorted according to the sort keys.
        """
        if isinstance(sorting, str):
            sorting = [(sorting, "ascending")]

        indices = _pc().sort_indices(
            self,
            sort_keys=sorting
        )
        return self.take(indices)


def _reconstruct_table(arrays, schema):
    """
    Internal: reconstruct pa.Table from pickled components.
    """
    return Table.from_arrays(arrays, schema=schema)


def record_batch(data, names=None, schema=None, metadata=None):
    """
    Create a pyarrow.RecordBatch from another Python data structure or sequence
    of arrays.

    Parameters
    ----------
    data : pandas.DataFrame, list
        A DataFrame or list of arrays or chunked arrays.
    names : list, default None
        Column names if list of arrays passed as data. Mutually exclusive with
        'schema' argument.
    schema : Schema, default None
        The expected schema of the RecordBatch. If not passed, will be inferred
        from the data. Mutually exclusive with 'names' argument.
    metadata : dict or Mapping, default None
        Optional metadata for the schema (if schema not passed).

    Returns
    -------
    RecordBatch

    See Also
    --------
    RecordBatch.from_arrays, RecordBatch.from_pandas, table
    """
    # accept schema as first argument for backwards compatibility / usability
    if isinstance(names, Schema) and schema is None:
        schema = names
        names = None

    if isinstance(data, (list, tuple)):
        return RecordBatch.from_arrays(data, names=names, schema=schema,
                                       metadata=metadata)
    elif _pandas_api.is_data_frame(data):
        return RecordBatch.from_pandas(data, schema=schema)
    else:
        raise TypeError("Expected pandas DataFrame or list of arrays")


def table(data, names=None, schema=None, metadata=None, nthreads=None):
    """
    Create a pyarrow.Table from a Python data structure or sequence of arrays.

    Parameters
    ----------
    data : pandas.DataFrame, dict, list
        A DataFrame, mapping of strings to Arrays or Python lists, or list of
        arrays or chunked arrays.
    names : list, default None
        Column names if list of arrays passed as data. Mutually exclusive with
        'schema' argument.
    schema : Schema, default None
        The expected schema of the Arrow Table. If not passed, will be inferred
        from the data. Mutually exclusive with 'names' argument.
        If passed, the output will have exactly this schema (raising an error
        when columns are not found in the data and ignoring additional data not
        specified in the schema, when data is a dict or DataFrame).
    metadata : dict or Mapping, default None
        Optional metadata for the schema (if schema not passed).
    nthreads : int, default None
        For pandas.DataFrame inputs: if greater than 1, convert columns to
        Arrow in parallel using indicated number of threads. By default,
        this follows :func:`pyarrow.cpu_count` (may use up to system CPU count
        threads).

    Returns
    -------
    Table

    See Also
    --------
    Table.from_arrays, Table.from_pandas, Table.from_pydict
    """
    # accept schema as first argument for backwards compatibility / usability
    if isinstance(names, Schema) and schema is None:
        schema = names
        names = None

    if isinstance(data, (list, tuple)):
        return Table.from_arrays(data, names=names, schema=schema,
                                 metadata=metadata)
    elif isinstance(data, dict):
        if names is not None:
            raise ValueError(
                "The 'names' argument is not valid when passing a dictionary")
        return Table.from_pydict(data, schema=schema, metadata=metadata)
    elif _pandas_api.is_data_frame(data):
        if names is not None or metadata is not None:
            raise ValueError(
                "The 'names' and 'metadata' arguments are not valid when "
                "passing a pandas DataFrame")
        return Table.from_pandas(data, schema=schema, nthreads=nthreads)
    else:
        raise TypeError(
            "Expected pandas DataFrame, python dictionary or list of arrays")


def concat_tables(tables, c_bool promote=False, MemoryPool memory_pool=None):
    """
    Concatenate pyarrow.Table objects.

    If promote==False, a zero-copy concatenation will be performed. The schemas
    of all the Tables must be the same (except the metadata), otherwise an
    exception will be raised. The result Table will share the metadata with the
    first table.

    If promote==True, any null type arrays will be casted to the type of other
    arrays in the column of the same name. If a table is missing a particular
    field, null values of the appropriate type will be generated to take the
    place of the missing field. The new schema will share the metadata with the
    first table. Each field in the new schema will share the metadata with the
    first table which has the field defined. Note that type promotions may
    involve additional allocations on the given ``memory_pool``.

    Parameters
    ----------
    tables : iterable of pyarrow.Table objects
        Pyarrow tables to concatenate into a single Table.
    promote : bool, default False
        If True, concatenate tables with null-filling and null type promotion.
    memory_pool : MemoryPool, default None
        For memory allocations, if required, otherwise use default pool.
    """
    cdef:
        vector[shared_ptr[CTable]] c_tables
        shared_ptr[CTable] c_result_table
        CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)
        Table table
        CConcatenateTablesOptions options = (
            CConcatenateTablesOptions.Defaults())

    for table in tables:
        c_tables.push_back(table.sp_table)

    with nogil:
        options.unify_schemas = promote
        c_result_table = GetResultValue(
            ConcatenateTables(c_tables, options, pool))

    return pyarrow_wrap_table(c_result_table)


def _from_pydict(cls, mapping, schema, metadata):
    """
    Construct a Table/RecordBatch from Arrow arrays or columns.

    Parameters
    ----------
    cls : Class Table/RecordBatch
    mapping : dict or Mapping
        A mapping of strings to Arrays or Python lists.
    schema : Schema, default None
        If not passed, will be inferred from the Mapping values.
    metadata : dict or Mapping, default None
        Optional metadata for the schema (if inferred).

    Returns
    -------
    Table/RecordBatch
    """

    arrays = []
    if schema is None:
        names = []
        for k, v in mapping.items():
            names.append(k)
            arrays.append(asarray(v))
        return cls.from_arrays(arrays, names, metadata=metadata)
    elif isinstance(schema, Schema):
        for field in schema:
            try:
                v = mapping[field.name]
            except KeyError:
                try:
                    v = mapping[tobytes(field.name)]
                except KeyError:
                    present = mapping.keys()
                    missing = [n for n in schema.names if n not in present]
                    raise KeyError(
                        "The passed mapping doesn't contain the "
                        "following field(s) of the schema: {}".
                        format(', '.join(missing))
                    )
            arrays.append(asarray(v, type=field.type))
        # Will raise if metadata is not None
        return cls.from_arrays(arrays, schema=schema, metadata=metadata)
    else:
        raise TypeError('Schema must be an instance of pyarrow.Schema')


def _from_pylist(cls, mapping, schema, metadata):
    """
    Construct a Table/RecordBatch from list of rows / dictionaries.

    Parameters
    ----------
    cls : Class Table/RecordBatch
    mapping : list of dicts of rows
        A mapping of strings to row values.
    schema : Schema, default None
        If not passed, will be inferred from the first row of the
        mapping values.
    metadata : dict or Mapping, default None
        Optional metadata for the schema (if inferred).

    Returns
    -------
    Table/RecordBatch
    """

    arrays = []
    if schema is None:
        names = []
        if mapping:
            names = list(mapping[0].keys())
        for n in names:
            v = [row[n] if n in row else None for row in mapping]
            arrays.append(v)
        return cls.from_arrays(arrays, names, metadata=metadata)
    else:
        if isinstance(schema, Schema):
            for n in schema.names:
                v = [row[n] if n in row else None for row in mapping]
                arrays.append(v)
            # Will raise if metadata is not None
            return cls.from_arrays(arrays, schema=schema, metadata=metadata)
        else:
            raise TypeError('Schema must be an instance of pyarrow.Schema')


class TableGroupBy:
    """
    A grouping of columns in a table on which to perform aggregations.

    Parameters
    ----------
    table : pyarrow.Table
        Input table to execute the aggregation on.
    keys : str or list[str]
        Name of the grouped columns.
    """

    def __init__(self, table, keys):
        if isinstance(keys, str):
            keys = [keys]

        self._table = table
        self.keys = keys

    def aggregate(self, aggregations):
        """
        Perform an aggregation over the grouped columns of the table.

        Parameters
        ----------
        aggregations : list[tuple(str, str)] or \
list[tuple(str, str, FunctionOptions)]
            List of tuples made of aggregation column names followed
            by function names and optionally aggregation function options.

        Returns
        -------
        Table
            Results of the aggregation functions.

        Examples
        --------
        >>> t = pa.table([
        ...       pa.array(["a", "a", "b", "b", "c"]),
        ...       pa.array([1, 2, 3, 4, 5]),
        ... ], names=["keys", "values"])
        >>> t.group_by("keys").aggregate([("values", "sum")])
        pyarrow.Table
        values_sum: int64
        keys: string
        ----
        values_sum: [[3,7,5]]
        keys: [["a","b","c"]]
        """
        columns = [a[0] for a in aggregations]
        aggrfuncs = [
            (a[1], a[2]) if len(a) > 2 else (a[1], None)
            for a in aggregations
        ]

        group_by_aggrs = []
        for aggr in aggrfuncs:
            if not aggr[0].startswith("hash_"):
                aggr = ("hash_" + aggr[0], aggr[1])
            group_by_aggrs.append(aggr)

        # Build unique names for aggregation result columns
        # so that it's obvious what they refer to.
        column_names = [
            aggr_name.replace("hash", col_name)
            for col_name, (aggr_name, _) in zip(columns, group_by_aggrs)
        ] + self.keys

        result = _pc()._group_by(
            [self._table[c] for c in columns],
            [self._table[k] for k in self.keys],
            group_by_aggrs
        )

        t = Table.from_batches([RecordBatch.from_struct_array(result)])
        return t.rename_columns(column_names)
