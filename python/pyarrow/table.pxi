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

import json

from collections import OrderedDict

try:
    import pandas as pd
except ImportError:
    # The pure-Python based API works without a pandas installation
    pass
else:
    import pyarrow.pandas_compat as pdcompat


cdef class ChunkedArray:
    """
    Array backed via one or more memory chunks.

    Warning
    -------
    Do not call this class's constructor directly.
    """

    def __cinit__(self):
        self.chunked_array = NULL

    cdef void init(self, const shared_ptr[CChunkedArray]& chunked_array):
        self.sp_chunked_array = chunked_array
        self.chunked_array = chunked_array.get()

    cdef int _check_nullptr(self) except -1:
        if self.chunked_array == NULL:
            raise ReferenceError(
                "{} object references a NULL pointer. Not initialized.".format(
                    type(self).__name__
                )
            )
        return 0

    def length(self):
        self._check_nullptr()
        return self.chunked_array.length()

    def __len__(self):
        return self.length()

    @property
    def null_count(self):
        """
        Number of null entires

        Returns
        -------
        int
        """
        self._check_nullptr()
        return self.chunked_array.null_count()

    @property
    def num_chunks(self):
        """
        Number of underlying chunks

        Returns
        -------
        int
        """
        self._check_nullptr()
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
        self._check_nullptr()
        return pyarrow_wrap_array(self.chunked_array.chunk(i))

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


cdef class Column:
    """
    Named vector of elements of equal type.

    Warning
    -------
    Do not call this class's constructor directly.
    """

    def __cinit__(self):
        self.column = NULL

    cdef void init(self, const shared_ptr[CColumn]& column):
        self.sp_column = column
        self.column = column.get()

    @staticmethod
    def from_array(object field_or_name, Array arr):
        cdef Field boxed_field

        if isinstance(field_or_name, Field):
            boxed_field = field_or_name
        else:
            boxed_field = field(field_or_name, arr.type)

        cdef shared_ptr[CColumn] sp_column
        sp_column.reset(new CColumn(boxed_field.sp_field, arr.sp_array))
        return pyarrow_wrap_column(sp_column)

    def to_pandas(self):
        """
        Convert the arrow::Column to a pandas.Series

        Returns
        -------
        pandas.Series
        """
        cdef:
            PyObject* out

        check_status(libarrow.ConvertColumnToPandas(self.sp_column,
                                                    self, &out))

        return pd.Series(wrap_array_output(out), name=self.name)

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
            CColumn* my_col = self.column
            CColumn* other_col = other.column
            c_bool result

        self._check_nullptr()
        other._check_nullptr()

        with nogil:
            result = my_col.Equals(deref(other_col))

        return result

    def to_pylist(self):
        """
        Convert to a list of native Python objects.
        """
        return self.data.to_pylist()

    cdef int _check_nullptr(self) except -1:
        if self.column == NULL:
            raise ReferenceError(
                "{} object references a NULL pointer. Not initialized.".format(
                    type(self).__name__
                )
            )
        return 0

    def __len__(self):
        return self.length()

    def length(self):
        self._check_nullptr()
        return self.column.length()

    @property
    def shape(self):
        """
        Dimensions of this columns

        Returns
        -------
        (int,)
        """
        self._check_nullptr()
        return (self.length(),)

    @property
    def null_count(self):
        """
        Number of null entires

        Returns
        -------
        int
        """
        self._check_nullptr()
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
        cdef ChunkedArray chunked_array = ChunkedArray()
        chunked_array.init(self.column.data())
        return chunked_array


cdef shared_ptr[const CKeyValueMetadata] unbox_metadata(dict metadata):
    if metadata is None:
        return <shared_ptr[const CKeyValueMetadata]> nullptr
    cdef:
        unordered_map[c_string, c_string] unordered_metadata = metadata
    return (<shared_ptr[const CKeyValueMetadata]>
            make_shared[CKeyValueMetadata](unordered_metadata))


cdef int _schema_from_arrays(
        arrays, names, dict metadata, shared_ptr[CSchema]* schema) except -1:
    cdef:
        Array arr
        Column col
        c_string c_name
        vector[shared_ptr[CField]] fields
        shared_ptr[CDataType] type_
        int K = len(arrays)

    fields.resize(K)

    if len(arrays) == 0:
        raise ValueError('Must pass at least one array')

    if isinstance(arrays[0], Array):
        if names is None:
            raise ValueError('Must pass names when constructing '
                             'from Array objects')
        for i in range(K):
            arr = arrays[i]
            type_ = arr.type.sp_type
            c_name = tobytes(names[i])
            fields[i].reset(new CField(c_name, type_, True))
    elif isinstance(arrays[0], Column):
        for i in range(K):
            col = arrays[i]
            type_ = col.sp_column.get().type()
            c_name = tobytes(col.name)
            fields[i].reset(new CField(c_name, type_, True))
    else:
        raise TypeError(type(arrays[0]))

    schema.reset(new CSchema(fields, unbox_metadata(metadata)))
    return 0


cdef tuple _dataframe_to_arrays(
    df,
    bint timestamps_to_ms,
    Schema schema,
    bint preserve_index
):
    cdef:
        list names = []
        list arrays = []
        list index_levels = []
        DataType type = None
        dict metadata

    if preserve_index:
        index_levels.extend(getattr(df.index, 'levels', [df.index]))

    for name in df.columns:
        col = df[name]
        if schema is not None:
            type = schema.field_by_name(name).type

        arr = arrays.append(
            Array.from_pandas(
                col, type=type, timestamps_to_ms=timestamps_to_ms
            )
        )
        names.append(name)

    for i, level in enumerate(index_levels):
        arrays.append(
            Array.from_pandas(level, timestamps_to_ms=timestamps_to_ms)
        )
        names.append(pdcompat.index_level_name(level, i))

    metadata = pdcompat.construct_metadata(df, index_levels, preserve_index)
    return names, arrays, metadata


cdef class RecordBatch:
    """
    Batch of rows of columns of equal length

    Warning
    -------
    Do not call this class's constructor directly, use one of the ``from_*``
    methods instead.
    """

    def __cinit__(self):
        self.batch = NULL
        self._schema = None

    cdef void init(self, const shared_ptr[CRecordBatch]& batch):
        self.sp_batch = batch
        self.batch = batch.get()

    cdef int _check_nullptr(self) except -1:
        if self.batch == NULL:
            raise ReferenceError(
                "{} object references a NULL pointer. Not initialized.".format(
                    type(self).__name__
                )
            )
        return 0

    def __len__(self):
        self._check_nullptr()
        return self.batch.num_rows()

    @property
    def num_columns(self):
        """
        Number of columns

        Returns
        -------
        int
        """
        self._check_nullptr()
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
        cdef Schema schema
        self._check_nullptr()
        if self._schema is None:
            schema = Schema()
            schema.init_schema(self.batch.schema())
            self._schema = schema

        return self._schema

    def __getitem__(self, i):
        return pyarrow_wrap_array(self.batch.column(i))

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
            CRecordBatch* my_batch = self.batch
            CRecordBatch* other_batch = other.batch
            c_bool result

        self._check_nullptr()
        other._check_nullptr()

        with nogil:
            result = my_batch.Equals(deref(other_batch))

        return result

    def to_pydict(self):
        """
        Converted the arrow::RecordBatch to an OrderedDict

        Returns
        -------
        OrderedDict
        """
        entries = []
        for i in range(self.batch.num_columns()):
            name = bytes(self.batch.column_name(i)).decode('utf8')
            column = self[i].to_pylist()
            entries.append((name, column))
        return OrderedDict(entries)


    def to_pandas(self, nthreads=None):
        """
        Convert the arrow::RecordBatch to a pandas DataFrame

        Returns
        -------
        pandas.DataFrame
        """
        return Table.from_batches([self]).to_pandas(nthreads=nthreads)

    @classmethod
    def from_pandas(cls, df, Schema schema=None, bint preserve_index=True):
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

        Returns
        -------
        pyarrow.RecordBatch
        """
        names, arrays, metadata = _dataframe_to_arrays(
            df, False, schema, preserve_index
        )
        return cls.from_arrays(arrays, names, metadata)

    @staticmethod
    def from_arrays(list arrays, list names, dict metadata=None):
        """
        Construct a RecordBatch from multiple pyarrow.Arrays

        Parameters
        ----------
        arrays: list of pyarrow.Array
            column-wise data vectors
        names: list of str
            Labels for the columns

        Returns
        -------
        pyarrow.RecordBatch
        """
        cdef:
            Array arr
            c_string c_name
            shared_ptr[CSchema] schema
            shared_ptr[CRecordBatch] batch
            vector[shared_ptr[CArray]] c_arrays
            int64_t num_rows
            int64_t i
            int64_t number_of_arrays = len(arrays)

        if not number_of_arrays:
            raise ValueError('Record batch cannot contain no arrays (for now)')

        num_rows = len(arrays[0])
        _schema_from_arrays(arrays, names, metadata, &schema)

        c_arrays.reserve(len(arrays))
        for arr in arrays:
            c_arrays.push_back(arr.sp_array)

        batch.reset(new CRecordBatch(schema, num_rows, c_arrays))
        return pyarrow_wrap_batch(batch)


def table_to_blocks(Table table, int nthreads):
    cdef:
        PyObject* result_obj
        shared_ptr[CTable] c_table = table.sp_table

    with nogil:
        check_status(
            libarrow.ConvertTableToPandas(
                c_table, nthreads, &result_obj
            )
        )

    return PyObject_to_object(result_obj)



cdef class Table:
    """
    A collection of top-level named, equal length Arrow arrays.

    Warning
    -------
    Do not call this class's constructor directly, use one of the ``from_*``
    methods instead.
    """

    def __cinit__(self):
        self.table = NULL

    def __repr__(self):
        return 'pyarrow.{}\n{}'.format(type(self).__name__, str(self.schema))

    cdef void init(self, const shared_ptr[CTable]& table):
        self.sp_table = table
        self.table = table.get()

    cdef int _check_nullptr(self) except -1:
        if self.table == nullptr:
            raise ReferenceError(
                "Table object references a NULL pointer. Not initialized."
            )
        return 0

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
            CTable* my_table = self.table
            CTable* other_table = other.table
            c_bool result

        self._check_nullptr()
        other._check_nullptr()

        with nogil:
            result = my_table.Equals(deref(other_table))

        return result

    @classmethod
    def from_pandas(
        cls,
        df,
        bint timestamps_to_ms=False,
        Schema schema=None,
        bint preserve_index=True
    ):
        """
        Convert pandas.DataFrame to an Arrow Table

        Parameters
        ----------
        df : pandas.DataFrame
        timestamps_to_ms : bool
            Convert datetime columns to ms resolution. This is needed for
            compability with other functionality like Parquet I/O which
            only supports milliseconds.
        schema : pyarrow.Schema, optional
            The expected schema of the Arrow Table. This can be used to
            indicate the type of columns if we cannot infer it automatically.
        preserve_index : bool, optional
            Whether to store the index as an additional column in the resulting
            ``Table``.

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
        names, arrays, metadata = _dataframe_to_arrays(
            df,
            timestamps_to_ms=timestamps_to_ms,
            schema=schema,
            preserve_index=preserve_index
        )
        return cls.from_arrays(arrays, names=names, metadata=metadata)

    @staticmethod
    def from_arrays(arrays, names=None, dict metadata=None):
        """
        Construct a Table from Arrow arrays or columns

        Parameters
        ----------
        arrays: list of pyarrow.Array or pyarrow.Column
            Equal-length arrays that should form the table.
        names: list of str, optional
            Names for the table columns. If Columns passed, will be
            inferred. If Arrays passed, this argument is required

        Returns
        -------
        pyarrow.Table

        """
        cdef:
            vector[shared_ptr[CColumn]] columns
            shared_ptr[CSchema] schema
            shared_ptr[CTable] table
            size_t K = len(arrays)

        _schema_from_arrays(arrays, names, metadata, &schema)

        columns.reserve(K)

        for i in range(K):
            if isinstance(arrays[i], Array):
                columns.push_back(
                    make_shared[CColumn](
                        schema.get().field(i),
                        (<Array> arrays[i]).sp_array
                    )
                )
            elif isinstance(arrays[i], Column):
                columns.push_back((<Column> arrays[i]).sp_column)
            else:
                raise ValueError(type(arrays[i]))

        table.reset(new CTable(schema, columns))
        return pyarrow_wrap_table(table)

    @staticmethod
    def from_batches(batches):
        """
        Construct a Table from a list of Arrow RecordBatches

        Parameters
        ----------

        batches: list of RecordBatch
            RecordBatch list to be converted, schemas must be equal
        """
        cdef:
            vector[shared_ptr[CRecordBatch]] c_batches
            shared_ptr[CTable] c_table
            RecordBatch batch

        for batch in batches:
            c_batches.push_back(batch.sp_batch)

        with nogil:
            check_status(CTable.FromRecordBatches(c_batches, &c_table))

        return pyarrow_wrap_table(c_table)

    def to_pandas(self, nthreads=None):
        """
        Convert the arrow::Table to a pandas DataFrame

        Parameters
        ----------
        nthreads : int, default max(1, multiprocessing.cpu_count() / 2)
            For the default, we divide the CPU count by 2 because most modern
            computers have hyperthreading turned on, so doubling the CPU count
            beyond the number of physical cores does not help

        Returns
        -------
        pandas.DataFrame
        """
        self._check_nullptr()
        if nthreads is None:
            nthreads = cpu_count()

        mgr = pdcompat.table_to_blockmanager(self, nthreads)
        return pd.DataFrame(mgr)

    def to_pydict(self):
        """
        Converted the arrow::Table to an OrderedDict

        Returns
        -------
        OrderedDict
        """
        cdef:
            size_t i
            size_t num_columns = self.table.num_columns()
            list entries = []
            Column column

        self._check_nullptr()
        for i in range(num_columns):
            column = self.column(i)
            entries.append((column.name, column.to_pylist()))

        return OrderedDict(entries)

    @property
    def schema(self):
        """
        Schema of the table and its columns

        Returns
        -------
        pyarrow.Schema
        """
        self._check_nullptr()
        return pyarrow_wrap_schema(self.table.schema())

    def column(self, int64_t i):
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
            Column column = Column()
            int64_t num_columns = self.num_columns
            int64_t index

        self._check_nullptr()
        if not -num_columns <= i < num_columns:
            raise IndexError(
                'Table column index {:d} is out of range'.format(i)
            )

        index = i if i >= 0 else num_columns + i
        assert index >= 0

        column.init(self.table.column(index))
        return column

    def __getitem__(self, int64_t i):
        return self.column(i)

    def itercolumns(self):
        """
        Iterator over all columns in their numerical order
        """
        for i in range(self.num_columns):
            yield self.column(i)

    @property
    def num_columns(self):
        """
        Number of columns in this table

        Returns
        -------
        int
        """
        self._check_nullptr()
        return self.table.num_columns()

    @property
    def num_rows(self):
        """
        Number of rows in this table.

        Due to the definition of a table, all columns have the same number of rows.

        Returns
        -------
        int
        """
        self._check_nullptr()
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
        self._check_nullptr()

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
        self._check_nullptr()

        with nogil:
            check_status(self.table.RemoveColumn(i, &c_table))

        return pyarrow_wrap_table(c_table)


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
