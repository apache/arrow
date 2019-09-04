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

cdef class ChunkedArray(_PandasConvertible):
    """
    Array backed via one or more memory chunks.

    Warning
    -------
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

    def format(self, int indent=0, int window=10):
        cdef:
            c_string result

        with nogil:
            check_status(
                PrettyPrint(
                    deref(self.chunked_array),
                    PrettyPrintOptions(indent, window),
                    &result
                )
            )

        return frombytes(result)

    def __str__(self):
        return self.format()

    def validate(self):
        """
        Validate chunked array consistency.
        """
        with nogil:
            check_status(self.sp_chunked_array.get().Validate())

    @property
    def null_count(self):
        """
        Number of null entires

        Returns
        -------
        int
        """
        return self.chunked_array.null_count()

    def __iter__(self):
        for chunk in self.iterchunks():
            for item in chunk:
                yield item

    def __getitem__(self, key):
        if isinstance(key, slice):
            return _normalize_slice(self, key)
        elif isinstance(key, six.integer_types):
            return self.getitem(key)
        else:
            raise TypeError("key must either be a slice or integer")

    cdef getitem(self, int64_t i):
        cdef int j

        index = _normalize_index(i, self.chunked_array.length())
        for j in range(self.num_chunks):
            if index < self.chunked_array.chunk(j).get().length():
                return self.chunk(j)[index]
            else:
                index -= self.chunked_array.chunk(j).get().length()

    def __eq__(self, other):
        try:
            return self.equals(other)
        except TypeError:
            return NotImplemented

    def equals(self, ChunkedArray other):
        """
        Return whether the contents of two chunked arrays are equal

        Parameters
        ----------
        other : pyarrow.ChunkedArray

        Returns
        -------
        are_equal : boolean
        """
        cdef:
            CChunkedArray* this_arr = self.chunked_array
            CChunkedArray* other_arr = other.chunked_array
            c_bool result

        if other is None:
            return False

        with nogil:
            result = this_arr.Equals(deref(other_arr))

        return result

    def _to_pandas(self, options, **kwargs):
        cdef:
            PyObject* out
            PandasOptions c_options = options

        with nogil:
            check_status(libarrow.ConvertChunkedArrayToPandas(
                c_options,
                self.sp_chunked_array,
                self, &out))

        return wrap_array_output(out)

    def __array__(self, dtype=None):
        if dtype is None:
            return self.to_pandas()
        return self.to_pandas().astype(dtype)

    def cast(self, object target_type, bint safe=True):
        """
        Cast values to another data type

        Parameters
        ----------
        target_type : DataType
            Type to cast to
        safe : boolean, default True
            Check for overflows or other unsafe conversions

        Returns
        -------
        casted : ChunkedArray
        """
        cdef:
            CCastOptions options = CCastOptions(safe)
            DataType type = ensure_type(target_type)
            shared_ptr[CArray] result
            CDatum out

        with nogil:
            check_status(Cast(_context(), CDatum(self.sp_chunked_array),
                              type.sp_type, options, &out))

        return pyarrow_wrap_chunked_array(out.chunked_array())

    def dictionary_encode(self):
        """
        Compute dictionary-encoded representation of array

        Returns
        -------
        pyarrow.ChunkedArray
            Same chunking as the input, all chunks share a common dictionary.
        """
        cdef CDatum out

        with nogil:
            check_status(
                DictionaryEncode(_context(), CDatum(self.sp_chunked_array),
                                 &out))

        return wrap_datum(out)

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
        result : List[ChunkedArray]
        """
        cdef:
            vector[shared_ptr[CChunkedArray]] flattened
            CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)

        with nogil:
            check_status(self.chunked_array.Flatten(pool, &flattened))

        return [pyarrow_wrap_chunked_array(col) for col in flattened]

    def unique(self):
        """
        Compute distinct elements in array

        Returns
        -------
        pyarrow.Array
        """
        cdef shared_ptr[CArray] result

        with nogil:
            check_status(
                Unique(_context(), CDatum(self.sp_chunked_array), &result))

        return pyarrow_wrap_array(result)

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

        if length is None:
            result = self.chunked_array.Slice(offset)
        else:
            result = self.chunked_array.Slice(offset, length)

        return pyarrow_wrap_chunked_array(result)

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
    arrays : Array, list of Array, or values coercible to arrays
        Must all be the same data type. Can be empty only if type also
        passed
    type : DataType or string coercible to DataType

    Returns
    -------
    ChunkedArray
    """
    cdef:
        Array arr
        vector[shared_ptr[CArray]] c_arrays
        shared_ptr[CChunkedArray] sp_chunked_array
        shared_ptr[CDataType] sp_data_type

    if isinstance(arrays, Array):
        arrays = [arrays]

    for x in arrays:
        if isinstance(x, Array):
            arr = x
            if type is not None:
                assert x.type == type
        else:
            arr = array(x, type=type)

        c_arrays.push_back(arr.sp_array)

    if type:
        sp_data_type = pyarrow_unwrap_data_type(type)
        sp_chunked_array.reset(new CChunkedArray(c_arrays, sp_data_type))
    else:
        if c_arrays.size() == 0:
            raise ValueError("Cannot construct a chunked array with neither "
                             "arrays nor type")
        sp_chunked_array.reset(new CChunkedArray(c_arrays))

    with nogil:
        check_status(sp_chunked_array.get().Validate())

    return pyarrow_wrap_chunked_array(sp_chunked_array)


cdef _schema_from_arrays(arrays, names, metadata, shared_ptr[CSchema]* schema):
    cdef:
        Py_ssize_t K = len(arrays)
        c_string c_name
        shared_ptr[CDataType] c_type
        shared_ptr[CKeyValueMetadata] c_meta
        vector[shared_ptr[CField]] c_fields

    if metadata is not None:
        if not isinstance(metadata, dict):
            raise TypeError('Metadata must be an instance of dict')
        c_meta = pyarrow_unwrap_metadata(metadata)

    if K == 0:
        schema.reset(new CSchema(c_fields, c_meta))
        return arrays

    c_fields.resize(K)

    if names is None:
        raise ValueError('Must pass names or schema to Table.from_arrays')

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
            c_name = tobytes(u'None')
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
            if not isinstance(item, (Array, ChunkedArray)):
                item = array(item, type=schema[i].type)
            converted_arrays.append(item)
    return converted_arrays


cdef class RecordBatch(_PandasConvertible):
    """
    Batch of rows of columns of equal length

    Warning
    -------
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

    def __reduce__(self):
        return _reconstruct_record_batch, (self.columns, self.schema)

    def __len__(self):
        return self.batch.num_rows()

    def validate(self):
        """
        Validate RecordBatch consistency.
        """
        with nogil:
            check_status(self.batch.Validate())

    def replace_schema_metadata(self, metadata=None):
        """
        EXPERIMENTAL: Create shallow copy of record batch by replacing schema
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
            shared_ptr[CKeyValueMetadata] c_meta
            shared_ptr[CRecordBatch] c_batch

        if metadata is not None:
            if not isinstance(metadata, dict):
                raise TypeError('Metadata must be an instance of dict')
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

    @property
    def columns(self):
        """
        List of all columns in numerical order

        Returns
        -------
        list of pa.ChunkedArray
        """
        return [self.column(i) for i in range(self.num_columns)]

    def column(self, i):
        """
        Select single column from record batch

        Returns
        -------
        column : pyarrow.Array
        """
        cdef int index = <int> _normalize_index(i, self.num_columns)
        return pyarrow_wrap_array(self.batch.column(index))

    def __getitem__(self, key):
        if isinstance(key, slice):
            return _normalize_slice(self, key)
        else:
            return self.column(key)

    def serialize(self, memory_pool=None):
        """
        Write RecordBatch to Buffer as encapsulated IPC message

        Parameters
        ----------
        memory_pool : MemoryPool, default None
            Uses default memory pool if not specified

        Returns
        -------
        serialized : Buffer
        """
        cdef:
            shared_ptr[CBuffer] buffer
            CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)

        with nogil:
            check_status(SerializeRecordBatch(deref(self.batch),
                                              pool, &buffer))
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

        if length is None:
            result = self.batch.Slice(offset)
        else:
            result = self.batch.Slice(offset, length)

        return pyarrow_wrap_batch(result)

    def equals(self, RecordBatch other):
        cdef:
            CRecordBatch* this_batch = self.batch
            CRecordBatch* other_batch = other.batch
            c_bool result

        with nogil:
            result = this_batch.Equals(deref(other_batch))

        return result

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

    def _to_pandas(self, options, **kwargs):
        return Table.from_batches([self])._to_pandas(options, **kwargs)

    @classmethod
    def from_pandas(cls, df, Schema schema=None, preserve_index=None,
                    nthreads=None, columns=None):
        """
        Convert pandas.DataFrame to an Arrow RecordBatch

        Parameters
        ----------
        df: pandas.DataFrame
        schema: pyarrow.Schema, optional
            The expected schema of the RecordBatch. This can be used to
            indicate the type of columns if we cannot infer it automatically.
        preserve_index : bool, optional
            Whether to store the index as an additional column in the resulting
            ``RecordBatch``. The default of None will store the index as a
            column, except for RangeIndex which is stored as metadata only. Use
            ``preserve_index=True`` to force it to be stored as a column.
        nthreads : int, default None (may use up to system CPU count threads)
            If greater than 1, convert columns to Arrow in parallel using
            indicated number of threads
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
        arrays: list of pyarrow.Array
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


def _reconstruct_record_batch(columns, schema):
    """
    Internal: reconstruct RecordBatch from pickled components.
    """
    return RecordBatch.from_arrays(columns, schema=schema)


def table_to_blocks(PandasOptions options, Table table,
                    MemoryPool memory_pool, categories):
    cdef:
        PyObject* result_obj
        shared_ptr[CTable] c_table = table.sp_table
        CMemoryPool* pool
        unordered_set[c_string] categorical_columns

    if categories is not None:
        categorical_columns = {tobytes(cat) for cat in categories}

    pool = maybe_unbox_memory_pool(memory_pool)
    with nogil:
        check_status(
            libarrow.ConvertTableToPandas(
                options, categorical_columns, c_table, pool, &result_obj)
        )

    return PyObject_to_object(result_obj)


cdef class Table(_PandasConvertible):
    """
    A collection of top-level named, equal length Arrow arrays.

    Warning
    -------
    Do not call this class's constructor directly, use one of the ``from_*``
    methods instead.
    """

    def __cinit__(self):
        self.table = NULL

    def __init__(self):
        raise TypeError("Do not call Table's constructor directly, use one of "
                        "the `Table.from_*` functions instead.")

    def __repr__(self):
        return 'pyarrow.{}\n{}'.format(type(self).__name__, str(self.schema))

    cdef void init(self, const shared_ptr[CTable]& table):
        self.sp_table = table
        self.table = table.get()

    def validate(self):
        """
        Validate table consistency.
        """
        with nogil:
            check_status(self.table.Validate())

    def __reduce__(self):
        # Reduce the columns as ChunkedArrays to avoid serializing schema
        # data twice
        columns = [col for col in self.columns]
        return _reconstruct_table, (columns, self.schema)

    def __getitem__(self, key):
        if isinstance(key, slice):
            return _normalize_slice(self, key)
        else:
            return self.column(key)

    def slice(self, offset=0, length=None):
        """
        Compute zero-copy slice of this Table

        Parameters
        ----------
        offset : int, default 0
            Offset from start of table to slice
        length : int, default None
            Length of slice (default is until end of table starting from
            offset)

        Returns
        -------
        sliced : Table
        """
        cdef shared_ptr[CTable] result

        if offset < 0:
            raise IndexError('Offset must be non-negative')

        if length is None:
            result = self.table.Slice(offset)
        else:
            result = self.table.Slice(offset, length)

        return pyarrow_wrap_table(result)

    def replace_schema_metadata(self, metadata=None):
        """
        EXPERIMENTAL: Create shallow copy of table by replacing schema
        key-value metadata with the indicated new metadata (which may be None,
        which deletes any existing metadata

        Parameters
        ----------
        metadata : dict, default None

        Returns
        -------
        shallow_copy : Table
        """
        cdef:
            shared_ptr[CKeyValueMetadata] c_meta
            shared_ptr[CTable] c_table

        if metadata is not None:
            if not isinstance(metadata, dict):
                raise TypeError('Metadata must be an instance of dict')
            c_meta = pyarrow_unwrap_metadata(metadata)

        with nogil:
            c_table = self.table.ReplaceSchemaMetadata(c_meta)

        return pyarrow_wrap_table(c_table)

    def flatten(self, MemoryPool memory_pool=None):
        """
        Flatten this Table.  Each column with a struct type is flattened
        into one column per struct field.  Other columns are left unchanged.

        Parameters
        ----------
        memory_pool : MemoryPool, default None
            For memory allocations, if required, otherwise use default pool

        Returns
        -------
        result : Table
        """
        cdef:
            shared_ptr[CTable] flattened
            CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)

        with nogil:
            check_status(self.table.Flatten(pool, &flattened))

        return pyarrow_wrap_table(flattened)

    def combine_chunks(self, MemoryPool memory_pool=None):
        """
        Make a new table by combining the chunks this table has.

        All the underlying chunks in the ChunkedArray of each column are
        concatenated into zero or one chunk.

        Parameters
        ----------
        memory_pool : MemoryPool, default None
            For memory allocations, if required, otherwise use default pool

        Returns
        -------
        result : Table
        """
        cdef:
            shared_ptr[CTable] combined
            CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)

        with nogil:
            check_status(self.table.CombineChunks(pool, &combined))

        return pyarrow_wrap_table(combined)

    def __eq__(self, other):
        try:
            return self.equals(other)
        except TypeError:
            return NotImplemented

    def equals(self, Table other):
        """
        Check if contents of two tables are equal

        Parameters
        ----------
        other : pyarrow.Table

        Returns
        -------
        are_equal : boolean
        """
        cdef:
            CTable* this_table = self.table
            CTable* other_table = other.table
            c_bool result

        if other is None:
            return False

        with nogil:
            result = this_table.Equals(deref(other_table))

        return result

    def cast(self, Schema target_schema, bint safe=True):
        """
        Cast table values to another schema

        Parameters
        ----------
        target_schema : Schema
            Schema to cast to, the names and order of fields must match
        safe : boolean, default True
            Check for overflows or other unsafe conversions

        Returns
        -------
        casted : Table
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
        preserve_index : bool, optional
            Whether to store the index as an additional column in the resulting
            ``Table``. The default of None will store the index as a column,
            except for RangeIndex which is stored as metadata only. Use
            ``preserve_index=True`` to force it to be stored as a column.
        nthreads : int, default None (may use up to system CPU count threads)
            If greater than 1, convert columns to Arrow in parallel using
            indicated number of threads
        columns : list, optional
           List of column to be converted. If None, use all columns.
        safe : boolean, default True
           Check for overflows or other unsafe conversions

        Returns
        -------
        pyarrow.Table

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
        Construct a Table from Arrow arrays

        Parameters
        ----------
        arrays : list of pyarrow.Array or pyarrow.ChunkedArray
            Equal-length arrays that should form the table.
        names : list of str, optional
            Names for the table columns. If not passed, schema must be passed
        schema : Schema, default None
            Schema for the created table. If not passed, names must be passed
        metadata : dict or Mapping, default None
            Optional metadata for the schema (if inferred).

        Returns
        -------
        pyarrow.Table

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
        Construct a Table from Arrow arrays or columns

        Parameters
        ----------
        mapping : dict or Mapping
            A mapping of strings to Arrays or Python lists.
        schema : Schema, default None
            If not passed, will be inferred from the Mapping values
        metadata : dict or Mapping, default None
            Optional metadata for the schema (if inferred).

        Returns
        -------
        pyarrow.Table

        """
        arrays = []
        if schema is not None:
            for field in schema:
                try:
                    v = mapping[field.name]
                except KeyError as e:
                    try:
                        v = mapping[tobytes(field.name)]
                    except KeyError as e2:
                        raise e
                arrays.append(array(v, type=field.type))
            # Will raise if metadata is not None
            return Table.from_arrays(arrays, schema=schema, metadata=metadata)
        else:
            names = []
            for k, v in mapping.items():
                names.append(k)
                if not isinstance(v, (Array, ChunkedArray)):
                    v = array(v)
                arrays.append(v)
            return Table.from_arrays(arrays, names, metadata=metadata)

    @staticmethod
    def from_batches(batches, Schema schema=None):
        """
        Construct a Table from a sequence or iterator of Arrow RecordBatches

        Parameters
        ----------
        batches : sequence or iterator of RecordBatch
            Sequence of RecordBatch to be converted, all schemas must be equal
        schema : Schema, default None
            If not passed, will be inferred from the first RecordBatch

        Returns
        -------
        table : Table
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
            check_status(CTable.FromRecordBatches(c_schema, c_batches,
                                                  &c_table))

        return pyarrow_wrap_table(c_table)

    def to_batches(self, chunksize=None):
        """
        Convert Table to list of (contiguous) RecordBatch objects, with optimal
        maximum chunk size

        Parameters
        ----------
        chunksize : int, default None
            Maximum size for RecordBatch chunks. Individual chunks may be
            smaller depending on the chunk layout of individual columns

        Returns
        -------
        batches : list of RecordBatch
        """
        cdef:
            unique_ptr[TableBatchReader] reader
            int64_t c_chunksize
            list result = []
            shared_ptr[CRecordBatch] batch

        reader.reset(new TableBatchReader(deref(self.table)))

        if chunksize is not None:
            c_chunksize = chunksize
            reader.get().set_chunksize(c_chunksize)

        while True:
            with nogil:
                check_status(reader.get().ReadNext(&batch))

            if batch.get() == NULL:
                break

            result.append(pyarrow_wrap_batch(batch))

        return result

    def _to_pandas(self, options, categories=None, ignore_metadata=False):
        from pyarrow.pandas_compat import table_to_blockmanager
        mgr = table_to_blockmanager(
            options, self, categories,
            ignore_metadata=ignore_metadata)
        return pandas_api.data_frame(mgr)

    def to_pydict(self):
        """
        Convert the Table to a dict or OrderedDict.

        Returns
        -------
        dict
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

    @property
    def schema(self):
        """
        Schema of the table and its columns

        Returns
        -------
        pyarrow.Schema
        """
        return pyarrow_wrap_schema(self.table.schema())

    def field(self, i):
        """
        Select a schema field by its colunm name or numeric index.

        Parameters
        ----------
        i : int or string

        Returns
        -------
        pyarrow.Field
        """
        return self.schema.field(i)

    def column(self, i):
        """
        Select a column by its column name, or numeric index.

        Parameters
        ----------
        i : int or string

        Returns
        -------
        pyarrow.ChunkedArray
        """
        if isinstance(i, six.string_types):
            field_index = self.schema.get_field_index(i)
            if field_index < 0:
                raise KeyError("Column {} does not exist in table".format(i))
            else:
                return self._column(field_index)
        elif isinstance(i, six.integer_types):
            return self._column(i)
        else:
            raise TypeError("Index must either be string or integer")

    def _column(self, int i):
        """
        Select a column by its numeric index.

        Parameters
        ----------
        i : int

        Returns
        -------
        pyarrow.ChunkedArray
        """
        cdef int index = <int> _normalize_index(i, self.num_columns)
        return pyarrow_wrap_chunked_array(self.table.column(index))

    def itercolumns(self):
        """
        Iterator over all columns in their numerical order
        """
        for i in range(self.num_columns):
            yield self._column(i)

    @property
    def columns(self):
        """
        List of all columns in numerical order

        Returns
        -------
        list of pa.ChunkedArray
        """
        return [self._column(i) for i in range(self.num_columns)]

    @property
    def num_columns(self):
        """
        Number of columns in this table

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
        Dimensions of the table: (#rows, #columns)

        Returns
        -------
        (int, int)
        """
        return (self.num_rows, self.num_columns)

    def add_column(self, int i, field_, column):
        """
        Add column to Table at position. Returns new table
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
            check_status(self.table.AddColumn(i, c_field.sp_field,
                                              c_arr.sp_chunked_array,
                                              &c_table))

        return pyarrow_wrap_table(c_table)

    def append_column(self, field_, column):
        """
        Append column at end of columns. Returns new table
        """
        return self.add_column(self.num_columns, field_, column)

    def remove_column(self, int i):
        """
        Create new Table with the indicated column removed
        """
        cdef shared_ptr[CTable] c_table

        with nogil:
            check_status(self.table.RemoveColumn(i, &c_table))

        return pyarrow_wrap_table(c_table)

    def set_column(self, int i, field_, column):
        """
        Replace column in Table at position. Returns new table
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
            check_status(self.table.SetColumn(i, c_field.sp_field,
                                              c_arr.sp_chunked_array,
                                              &c_table))

        return pyarrow_wrap_table(c_table)

    @property
    def column_names(self):
        """
        Names of the table's columns
        """
        names = self.table.ColumnNames()
        return [frombytes(name) for name in names]

    def rename_columns(self, names):
        """
        Create new table with columns renamed to provided names
        """
        cdef:
            shared_ptr[CTable] c_table
            vector[c_string] c_names

        for name in names:
            c_names.push_back(tobytes(name))

        with nogil:
            check_status(self.table.RenameColumns(c_names, &c_table))

        return pyarrow_wrap_table(c_table)

    def drop(self, columns):
        """
        Drop one or more columns and return a new table.

        columns: list of str

        Returns pa.Table
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


def _reconstruct_table(arrays, schema):
    """
    Internal: reconstruct pa.Table from pickled components.
    """
    return Table.from_arrays(arrays, schema=schema)


def record_batch(data, names=None, schema=None, metadata=None):
    """
    Create a pyarrow.RecordBatch from another Python data structure or sequence
    of arrays

    Parameters
    ----------
    data : pandas.DataFrame, dict, list
        A DataFrame, mapping of strings to Arrays or Python lists, or list of
        arrays or chunked arrays
    names : list, default None
        Column names if list of arrays passed as data. Mutually exclusive with
        'schema' argument
    schema : Schema, default None
        The expected schema of the RecordBatch. If not passed, will be inferred
        from the data. Mutually exclusive with 'names' argument
    metadata : dict or Mapping, default None
        Optional metadata for the schema (if schema not passed).

    Returns
    -------
    RecordBatch

    See Also
    --------
    RecordBatch.from_arrays, RecordBatch.from_pandas, Table.from_pydict
    """
    if isinstance(data, (list, tuple)):
        return RecordBatch.from_arrays(data, names=names, schema=schema,
                                       metadata=metadata)
    elif isinstance(data, _pandas_api.pd.DataFrame):
        return RecordBatch.from_pandas(data, schema=schema)
    else:
        return TypeError("Expected pandas DataFrame or python dictionary")


def table(data, names=None, schema=None, metadata=None):
    """
    Create a pyarrow.Table from another Python data structure or sequence of
    arrays

    Parameters
    ----------
    data : pandas.DataFrame, dict, list
        A DataFrame, mapping of strings to Arrays or Python lists, or list of
        arrays or chunked arrays
    names : list, default None
        Column names if list of arrays passed as data. Mutually exclusive with
        'schema' argument
    schema : Schema, default None
        The expected schema of the Arrow Table. If not passed, will be inferred
        from the data. Mutually exclusive with 'names' argument
    metadata : dict or Mapping, default None
        Optional metadata for the schema (if schema not passed).

    Returns
    -------
    Table

    See Also
    --------
    Table.from_arrays, Table.from_pandas, Table.from_pydict
    """
    if isinstance(data, (list, tuple)):
        return Table.from_arrays(data, names=names, schema=schema,
                                 metadata=metadata)
    elif isinstance(data, dict):
        return Table.from_pydict(data, schema=schema, metadata=metadata)
    elif isinstance(data, _pandas_api.pd.DataFrame):
        return Table.from_pandas(data, schema=schema)
    else:
        return TypeError("Expected pandas DataFrame or python dictionary")


def concat_tables(tables):
    """
    Perform zero-copy concatenation of pyarrow.Table objects. Raises exception
    if all of the Table schemas are not the same

    Parameters
    ----------
    tables : iterable of pyarrow.Table objects
    output_name : string, default None
      A name for the output table, if any
    """
    cdef:
        vector[shared_ptr[CTable]] c_tables
        shared_ptr[CTable] c_result
        Table table

    for table in tables:
        c_tables.push_back(table.sp_table)

    with nogil:
        check_status(ConcatenateTables(c_tables, &c_result))

    return pyarrow_wrap_table(c_result)
