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
    arrays : list of Array or values coercible to arrays
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


def column(object field_or_name, arr):
    """
    Create Column object from field/string and array-like data

    Parameters
    ----------
    field_or_name : string or Field
    arr : Array, list of Arrays, or ChunkedArray

    Returns
    -------
    column : Column
    """
    cdef:
        Field boxed_field
        Array _arr
        ChunkedArray _carr
        shared_ptr[CColumn] sp_column

    if isinstance(arr, list):
        arr = chunked_array(arr)
    elif not isinstance(arr, (Array, ChunkedArray)):
        arr = array(arr)

    if isinstance(field_or_name, Field):
        boxed_field = field_or_name
        if arr.type != boxed_field.type:
            raise ValueError('Passed field type does not match array')
    else:
        boxed_field = field(field_or_name, arr.type)

    if isinstance(arr, Array):
        _arr = arr
        sp_column.reset(new CColumn(boxed_field.sp_field, _arr.sp_array))
    elif isinstance(arr, ChunkedArray):
        _carr = arr
        sp_column.reset(new CColumn(boxed_field.sp_field,
                                    _carr.sp_chunked_array))
    else:
        raise ValueError("Unsupported type for column(...): {}"
                         .format(type(arr)))

    return pyarrow_wrap_column(sp_column)


cdef class Column(_PandasConvertible):
    """
    Named vector of elements of equal type.

    Warning
    -------
    Do not call this class's constructor directly.
    """

    def __cinit__(self):
        self.column = NULL

    def __init__(self):
        raise TypeError("Do not call Column's constructor directly, use one "
                        "of the `Column.from_*` functions instead.")

    cdef void init(self, const shared_ptr[CColumn]& column):
        self.sp_column = column
        self.column = column.get()

    def __reduce__(self):
        return column, (self.field, self.data)

    def __repr__(self):
        from pyarrow.compat import StringIO
        result = StringIO()
        result.write('<Column name={0!r} type={1!r}>'
                     .format(self.name, self.type))
        result.write('\n{}'.format(str(self.data)))

        return result.getvalue()

    def __getitem__(self, key):
        return self.data[key]

    @staticmethod
    def from_array(*args):
        return column(*args)

    def cast(self, object target_type, bint safe=True):
        """
        Cast column values to another data type

        Parameters
        ----------
        target_type : DataType
            Type to cast to
        safe : boolean, default True
            Check for overflows or other unsafe conversions

        Returns
        -------
        casted : Column
        """
        cdef:
            CCastOptions options = CCastOptions(safe)
            DataType type = ensure_type(target_type)
            shared_ptr[CArray] result
            CDatum out

        with nogil:
            check_status(Cast(_context(), CDatum(self.column.data()),
                              type.sp_type, options, &out))

        casted_data = pyarrow_wrap_chunked_array(out.chunked_array())
        return column(self.name, casted_data)

    def dictionary_encode(self):
        """
        Compute dictionary-encoded representation of array

        Returns
        -------
        pyarrow.Column
            Same chunking as the input, all chunks share a common dictionary.
        """
        ca = self.data.dictionary_encode()
        return column(self.name, ca)

    def unique(self):
        """
        Compute distinct elements in array

        Returns
        -------
        pyarrow.Array
        """
        return self.data.unique()

    def flatten(self, MemoryPool memory_pool=None):
        """
        Flatten this Column.  If it has a struct type, the column is
        flattened into one column per struct field.

        Parameters
        ----------
        memory_pool : MemoryPool, default None
            For memory allocations, if required, otherwise use default pool

        Returns
        -------
        result : List[Column]
        """
        cdef:
            vector[shared_ptr[CColumn]] flattened
            CMemoryPool* pool = maybe_unbox_memory_pool(memory_pool)

        with nogil:
            check_status(self.column.Flatten(pool, &flattened))

        return [pyarrow_wrap_column(col) for col in flattened]

    def _to_pandas(self, options, **kwargs):
        values = self.data._to_pandas(options)
        result = pandas_api.make_series(values, name=self.name)

        if isinstance(self.type, TimestampType):
            tz = self.type.tz
            if tz is not None:
                tz = string_to_tzinfo(tz)
                result = (result.dt.tz_localize('utc')
                          .dt.tz_convert(tz))

        return result

    def __array__(self, dtype=None):
        return self.data.__array__(dtype=dtype)

    def __eq__(self, other):
        try:
            return self.equals(other)
        except TypeError:
            return NotImplemented

    def equals(self, Column other):
        """
        Check if contents of two columns are equal

        Parameters
        ----------
        other : pyarrow.Column

        Returns
        -------
        are_equal : boolean
        """
        cdef:
            CColumn* this_col = self.column
            CColumn* other_col = other.column
            c_bool result

        if other is None:
            return False

        with nogil:
            result = this_col.Equals(deref(other_col))

        return result

    def to_pylist(self):
        """
        Convert to a list of native Python objects.
        """
        return self.data.to_pylist()

    def __len__(self):
        return self.length()

    def length(self):
        return self.column.length()

    @property
    def field(self):
        return pyarrow_wrap_field(self.column.field())

    @property
    def shape(self):
        """
        Dimensions of this columns

        Returns
        -------
        (int,)
        """
        return (self.length(),)

    @property
    def null_count(self):
        """
        Number of null entires

        Returns
        -------
        int
        """
        return self.column.null_count()

    @property
    def name(self):
        """
        Label of the column

        Returns
        -------
        str
        """
        return bytes(self.column.name()).decode('utf8')

    @property
    def type(self):
        """
        Type information for this column

        Returns
        -------
        pyarrow.DataType
        """
        return pyarrow_wrap_data_type(self.column.type())

    @property
    def data(self):
        """
        The underlying data

        Returns
        -------
        pyarrow.ChunkedArray
        """
        return pyarrow_wrap_chunked_array(self.column.data())


cdef _schema_from_arrays(arrays, names, metadata, shared_ptr[CSchema]* schema):
    cdef:
        Py_ssize_t K = len(arrays)
        c_string c_name
        CColumn* c_column
        shared_ptr[CDataType] c_type
        shared_ptr[CKeyValueMetadata] c_meta
        vector[shared_ptr[CField]] c_fields

    if metadata is not None:
        if not isinstance(metadata, dict):
            raise TypeError('Metadata must be an instance of dict')
        c_meta = pyarrow_unwrap_metadata(metadata)

    if K == 0:
        schema.reset(new CSchema(c_fields, c_meta))
        return

    c_fields.resize(K)

    if isinstance(arrays[0], Column):
        for i in range(K):
            c_column = (<Column>arrays[i]).column
            c_fields[i] = c_column.field()
    else:
        if names is None:
            raise ValueError('Must pass names when constructing '
                             'from Array objects')
        if len(names) != K:
            raise ValueError('Length of names ({}) does not match '
                             'length of arrays ({})'.format(len(names), K))
        for i in range(K):
            val = arrays[i]
            if isinstance(val, (Array, ChunkedArray)):
                c_type = (<DataType> val.type).sp_type
            else:
                raise TypeError(type(val))

            if names[i] is None:
                c_name = tobytes(u'None')
            else:
                c_name = tobytes(names[i])
            c_fields[i].reset(new CField(c_name, c_type, True))

    schema.reset(new CSchema(c_fields, c_meta))


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
        list of pa.Column
        """
        return [self.column(i) for i in range(self.num_columns)]

    def column(self, i):
        """
        Select single column from record batch

        Returns
        -------
        column : pyarrow.Array
        """
        if not -self.num_columns <= i < self.num_columns:
            raise IndexError(
                'Record batch column index {:d} is out of range'.format(i)
            )
        return pyarrow_wrap_array(self.batch.column(i))

    def __getitem__(self, key):
        if isinstance(key, slice):
            return _normalize_slice(self, key)
        else:
            return self.column(_normalize_index(key, self.num_columns))

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
            Offset from start of array to slice
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
    def from_pandas(cls, df, Schema schema=None, bint preserve_index=True,
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
            ``RecordBatch``.
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
        return cls.from_arrays(arrays, schema)

    @staticmethod
    def from_arrays(list arrays, names, metadata=None):
        """
        Construct a RecordBatch from multiple pyarrow.Arrays

        Parameters
        ----------
        arrays: list of pyarrow.Array
            column-wise data vectors
        names: pyarrow.Schema or list of str
            schema or list of labels for the columns

        Returns
        -------
        pyarrow.RecordBatch
        """
        cdef:
            Array arr
            c_string c_name
            shared_ptr[CSchema] c_schema
            vector[shared_ptr[CArray]] c_arrays
            int64_t num_rows
            int64_t i
            int64_t number_of_arrays = len(arrays)

        if len(arrays) > 0:
            num_rows = len(arrays[0])
        else:
            num_rows = 0
        if isinstance(names, Schema):
            c_schema = (<Schema> names).sp_schema
        else:
            _schema_from_arrays(arrays, names, metadata, &c_schema)

        c_arrays.reserve(len(arrays))
        for arr in arrays:
            if len(arr) != num_rows:
                raise ValueError('Arrays were not all the same length: '
                                 '{0} vs {1}'.format(len(arr), num_rows))
            c_arrays.push_back(arr.sp_array)

        return pyarrow_wrap_batch(
            CRecordBatch.Make(c_schema, num_rows, c_arrays))


def _reconstruct_record_batch(columns, schema):
    """
    Internal: reconstruct RecordBatch from pickled components.
    """
    return RecordBatch.from_arrays(columns, schema)


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

    def _validate(self):
        """
        Validate table consistency.
        """
        with nogil:
            check_status(self.table.Validate())

    def __reduce__(self):
        # Reduce the columns as ChunkedArrays to avoid serializing schema
        # data twice
        columns = [col.data for col in self.columns]
        return _reconstruct_table, (columns, self.schema)

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
            Column column, casted
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
    def from_pandas(cls, df, Schema schema=None, bint preserve_index=True,
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
            ``Table``.
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
        Construct a Table from Arrow arrays or columns

        Parameters
        ----------
        arrays : list of pyarrow.Array or pyarrow.Column
            Equal-length arrays that should form the table.
        names : list of str, optional
            Names for the table columns. If Columns passed, will be
            inferred. If Arrays passed, this argument is required
        schema : Schema, default None
            If not passed, will be inferred from the arrays
        metadata : dict or Mapping, default None
            Optional metadata for the schema (if inferred).

        Returns
        -------
        pyarrow.Table

        """
        cdef:
            vector[shared_ptr[CColumn]] columns
            Schema cy_schema
            shared_ptr[CSchema] c_schema
            int i, K = <int> len(arrays)

        if schema is None:
            _schema_from_arrays(arrays, names, metadata, &c_schema)
        elif schema is not None:
            if names is not None:
                raise ValueError('Cannot pass both schema and names')
            if metadata is not None:
                raise ValueError('Cannot pass both schema and metadata')
            cy_schema = schema

            if len(schema) != len(arrays):
                raise ValueError('Schema and number of arrays unequal')

            c_schema = cy_schema.sp_schema

        columns.reserve(K)

        for i in range(K):
            if isinstance(arrays[i], Array):
                columns.push_back(
                    make_shared[CColumn](
                        c_schema.get().field(i),
                        (<Array> arrays[i]).sp_array
                    )
                )
            elif isinstance(arrays[i], ChunkedArray):
                columns.push_back(
                    make_shared[CColumn](
                        c_schema.get().field(i),
                        (<ChunkedArray> arrays[i]).sp_chunked_array
                    )
                )
            elif isinstance(arrays[i], Column):
                # Make sure schema field and column are consistent
                columns.push_back(
                    make_shared[CColumn](
                        c_schema.get().field(i),
                        (<Column> arrays[i]).sp_column.get().data()
                    )
                )
            else:
                raise TypeError(type(arrays[i]))

        return pyarrow_wrap_table(CTable.Make(c_schema, columns))

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
        names = []
        arrays = []
        for k, v in mapping.items():
            names.append(k)
            if not isinstance(v, (Array, ChunkedArray)):
                v = array(v)
            arrays.append(v)
        if schema is None:
            return Table.from_arrays(arrays, names, metadata=metadata)
        else:
            # Will raise if metadata is not None
            return Table.from_arrays(arrays, schema=schema, metadata=metadata)

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
            Column column

        for i in range(num_columns):
            column = self.column(i)
            entries.append((column.name, column.to_pylist()))

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

    def column(self, i):
        """
        Select a column by its column name, or numeric index.

        Parameters
        ----------
        i : int or string

        Returns
        -------
        pyarrow.Column
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
        pyarrow.Column
        """
        cdef:
            int num_columns = self.num_columns
            int index

        if not -num_columns <= i < num_columns:
            raise IndexError(
                'Table column index {:d} is out of range'.format(i)
            )

        index = i if i >= 0 else num_columns + i
        assert index >= 0

        return pyarrow_wrap_column(self.table.column(index))

    def __getitem__(self, key):
        cdef int index = <int> _normalize_index(key, self.num_columns)
        return self.column(index)

    def itercolumns(self):
        """
        Iterator over all columns in their numerical order
        """
        for i in range(self.num_columns):
            yield self.column(i)

    @property
    def columns(self):
        """
        List of all columns in numerical order

        Returns
        -------
        list of pa.Column
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

    def add_column(self, int i, Column column):
        """
        Add column to Table at position. Returns new table
        """
        cdef shared_ptr[CTable] c_table

        with nogil:
            check_status(self.table.AddColumn(i, column.sp_column, &c_table))

        return pyarrow_wrap_table(c_table)

    def append_column(self, Column column):
        """
        Append column at end of columns. Returns new table
        """
        return self.add_column(self.num_columns, column)

    def remove_column(self, int i):
        """
        Create new Table with the indicated column removed
        """
        cdef shared_ptr[CTable] c_table

        with nogil:
            check_status(self.table.RemoveColumn(i, &c_table))

        return pyarrow_wrap_table(c_table)

    def set_column(self, int i, Column column):
        """
        Replace column in Table at position. Returns new table
        """
        cdef shared_ptr[CTable] c_table

        with nogil:
            check_status(self.table.SetColumn(i, column.sp_column, &c_table))

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
