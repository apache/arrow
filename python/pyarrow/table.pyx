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

from cython.operator cimport dereference as deref

from pyarrow.includes.libarrow cimport *
from pyarrow.includes.common cimport *
cimport pyarrow.includes.pyarrow as pyarrow

import pyarrow.config

from pyarrow.array cimport Array, box_array, wrap_array_output
from pyarrow.error import ArrowException
from pyarrow.error cimport check_status
from pyarrow.schema cimport box_data_type, box_schema, DataType

from pyarrow.compat import frombytes, tobytes

cimport cpython

from collections import OrderedDict


cdef _pandas():
    import pandas as pd
    return pd


cdef class ChunkedArray:
    """
    Array backed via one or more memory chunks.

    Warning
    -------
    Do not call this class's constructor directly.
    """

    def __cinit__(self):
        self.chunked_array = NULL

    cdef init(self, const shared_ptr[CChunkedArray]& chunked_array):
        self.sp_chunked_array = chunked_array
        self.chunked_array = chunked_array.get()

    cdef _check_nullptr(self):
        if self.chunked_array == NULL:
            raise ReferenceError("ChunkedArray object references a NULL "
                                 "pointer. Not initialized.")

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
        pyarrow.array.Array
        """
        self._check_nullptr()
        return box_array(self.chunked_array.chunk(i))

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

    cdef init(self, const shared_ptr[CColumn]& column):
        self.sp_column = column
        self.column = column.get()

    def to_pandas(self):
        """
        Convert the arrow::Column to a pandas.Series

        Returns
        -------
        pandas.Series
        """
        cdef:
            PyObject* out

        check_status(pyarrow.ConvertColumnToPandas(self.sp_column,
                                                   <PyObject*> self, &out))

        return _pandas().Series(wrap_array_output(out), name=self.name)

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
        pyarrow.schema.DataType
        """
        return box_data_type(self.column.type())

    @property
    def data(self):
        """
        The underlying data

        Returns
        -------
        pyarrow.table.ChunkedArray
        """
        cdef ChunkedArray chunked_array = ChunkedArray()
        chunked_array.init(self.column.data())
        return chunked_array


cdef _schema_from_arrays(arrays, names, shared_ptr[CSchema]* schema):
    cdef:
        Array arr
        Column col
        c_string c_name
        vector[shared_ptr[CField]] fields
        cdef shared_ptr[CDataType] type_

    cdef int K = len(arrays)

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

    schema.reset(new CSchema(fields))



cdef _dataframe_to_arrays(df, timestamps_to_ms, Schema schema):
    cdef:
        list names = []
        list arrays = []
        DataType type = None

    for name in df.columns:
        col = df[name]
        if schema is not None:
            type = schema.field_by_name(name).type

        arr = Array.from_numpy(col, type=type,
                                timestamps_to_ms=timestamps_to_ms)
        names.append(name)
        arrays.append(arr)

    return names, arrays


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

    cdef init(self, const shared_ptr[CRecordBatch]& batch):
        self.sp_batch = batch
        self.batch = batch.get()

    cdef _check_nullptr(self):
        if self.batch == NULL:
            raise ReferenceError("Object not initialized")

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
        pyarrow.schema.Schema
        """
        cdef Schema schema
        self._check_nullptr()
        if self._schema is None:
            schema = Schema()
            schema.init_schema(self.batch.schema())
            self._schema = schema

        return self._schema

    def __getitem__(self, i):
        return box_array(self.batch.column(i))

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

        return batch_from_cbatch(result)

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
    def from_pandas(cls, df, schema=None):
        """
        Convert pandas.DataFrame to an Arrow RecordBatch

        Parameters
        ----------
        df: pandas.DataFrame
        schema: pyarrow.Schema (optional)
            The expected schema of the RecordBatch. This can be used to
            indicate the type of columns if we cannot infer it automatically.

        Returns
        -------
        pyarrow.table.RecordBatch
        """
        names, arrays = _dataframe_to_arrays(df, False, schema)
        return cls.from_arrays(arrays, names)

    @staticmethod
    def from_arrays(arrays, names):
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
        pyarrow.table.RecordBatch
        """
        cdef:
            Array arr
            c_string c_name
            shared_ptr[CSchema] schema
            shared_ptr[CRecordBatch] batch
            vector[shared_ptr[CArray]] c_arrays
            int64_t num_rows

        if len(arrays) == 0:
            raise ValueError('Record batch cannot contain no arrays (for now)')

        num_rows = len(arrays[0])
        _schema_from_arrays(arrays, names, &schema)

        for i in range(len(arrays)):
            arr = arrays[i]
            c_arrays.push_back(arr.sp_array)

        batch.reset(new CRecordBatch(schema, num_rows, c_arrays))
        return batch_from_cbatch(batch)


cdef table_to_blockmanager(const shared_ptr[CTable]& table, int nthreads):
    cdef:
        PyObject* result_obj
        CColumn* col
        int i

    import pandas.core.internals as _int
    from pandas import RangeIndex, Categorical
    from pyarrow.compat import DatetimeTZDtype

    with nogil:
        check_status(pyarrow.ConvertTableToPandas(table, nthreads,
                                                  &result_obj))

    result = PyObject_to_object(result_obj)

    blocks = []
    for item in result:
        block_arr = item['block']
        placement = item['placement']
        if 'dictionary' in item:
            cat = Categorical(block_arr,
                              categories=item['dictionary'],
                              ordered=False, fastpath=True)
            block = _int.make_block(cat, placement=placement,
                                    klass=_int.CategoricalBlock,
                                    fastpath=True)
        elif 'timezone' in item:
            dtype = DatetimeTZDtype('ns', tz=item['timezone'])
            block = _int.make_block(block_arr, placement=placement,
                                    klass=_int.DatetimeTZBlock,
                                    dtype=dtype, fastpath=True)
        else:
            block = _int.make_block(block_arr, placement=placement)
        blocks.append(block)

    names = []
    for i in range(table.get().num_columns()):
        col = table.get().column(i).get()
        names.append(frombytes(col.name()))

    axes = [names, RangeIndex(table.get().num_rows())]
    return _int.BlockManager(blocks, axes)


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
        return 'pyarrow.Table\n{0}'.format(str(self.schema))

    cdef init(self, const shared_ptr[CTable]& table):
        self.sp_table = table
        self.table = table.get()

    cdef _check_nullptr(self):
        if self.table == NULL:
            raise ReferenceError("Table object references a NULL pointer."
                    "Not initialized.")

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
    def from_pandas(cls, df, timestamps_to_ms=False, schema=None):
        """
        Convert pandas.DataFrame to an Arrow Table

        Parameters
        ----------
        df: pandas.DataFrame

        timestamps_to_ms: bool
            Convert datetime columns to ms resolution. This is needed for
            compability with other functionality like Parquet I/O which
            only supports milliseconds.

        schema: pyarrow.Schema (optional)
            The expected schema of the Arrow Table. This can be used to
            indicate the type of columns if we cannot infer it automatically.

        Returns
        -------
        pyarrow.table.Table

        Examples
        --------

        >>> import pandas as pd
        >>> import pyarrow as pa
        >>> df = pd.DataFrame({
            ...     'int': [1, 2],
            ...     'str': ['a', 'b']
            ... })
        >>> pa.Table.from_pandas(df)
        <pyarrow.table.Table object at 0x7f05d1fb1b40>
        """
        names, arrays = _dataframe_to_arrays(df,
                                             timestamps_to_ms=timestamps_to_ms,
                                             schema=schema)
        return cls.from_arrays(arrays, names=names)

    @staticmethod
    def from_arrays(arrays, names=None):
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
        pyarrow.table.Table

        """
        cdef:
            vector[shared_ptr[CField]] fields
            vector[shared_ptr[CColumn]] columns
            shared_ptr[CSchema] schema
            shared_ptr[CTable] table

        _schema_from_arrays(arrays, names, &schema)

        cdef int K = len(arrays)
        columns.resize(K)

        for i in range(K):
            if isinstance(arrays[i], Array):
                columns[i].reset(new CColumn(schema.get().field(i),
                                             (<Array> arrays[i]).sp_array))
            elif isinstance(arrays[i], Column):
                columns[i] = (<Column> arrays[i]).sp_column
            else:
                raise ValueError(type(arrays[i]))

        table.reset(new CTable(schema, columns))
        return table_from_ctable(table)

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

        return table_from_ctable(c_table)

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
        if nthreads is None:
            nthreads = pyarrow.config.cpu_count()

        mgr = table_to_blockmanager(self.sp_table, nthreads)
        return _pandas().DataFrame(mgr)

    def to_pydict(self):
        """
        Converted the arrow::Table to an OrderedDict

        Returns
        -------
        OrderedDict
        """
        entries = []
        for i in range(self.table.num_columns()):
            name = self.column(i).name
            column = self.column(i).to_pylist()
            entries.append((name, column))
        return OrderedDict(entries)

    @property
    def schema(self):
        """
        Schema of the table and its columns

        Returns
        -------
        pyarrow.schema.Schema
        """
        return box_schema(self.table.schema())

    def column(self, index):
        """
        Select a column by its numeric index.

        Parameters
        ----------
        index: int

        Returns
        -------
        pyarrow.table.Column
        """
        self._check_nullptr()
        cdef Column column = Column()
        column.init(self.table.column(index))
        return column

    def __getitem__(self, i):
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

    def remove_column(self, int i):
        """
        Create new Table with the indicated column removed
        """
        cdef shared_ptr[CTable] c_table

        with nogil:
            check_status(self.table.RemoveColumn(i, &c_table))

        return table_from_ctable(c_table)


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

    return table_from_ctable(c_result)


cdef api object table_from_ctable(const shared_ptr[CTable]& ctable):
    cdef Table table = Table()
    table.init(ctable)
    return table


cdef api object batch_from_cbatch(const shared_ptr[CRecordBatch]& cbatch):
    cdef RecordBatch batch = RecordBatch()
    batch.init(cbatch)
    return batch
