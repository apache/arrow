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

# cython: language_level = 3

"""Dataset is currently unstable. APIs subject to change without notice."""

from cpython.object cimport Py_LT, Py_EQ, Py_GT, Py_LE, Py_NE, Py_GE
from cython.operator cimport dereference as deref

import pyarrow as pa
from pyarrow.lib cimport *
from pyarrow.includes.libarrow_dataset cimport *
from pyarrow.compat import frombytes, tobytes
from pyarrow._fs cimport FileSystem, FileInfo, FileSelector


def _forbid_instantiation(klass, subclasses_instead=True):
    msg = '{} is an abstract class thus cannot be initialized.'.format(
        klass.__name__
    )
    if subclasses_instead:
        subclasses = [cls.__name__ for cls in klass.__subclasses__]
        msg += ' Use one of the subclasses instead: {}'.format(
            ', '.join(subclasses)
        )
    raise TypeError(msg)


cdef class Dataset:
    """
    Collection of data fragments and potentially child datasets.

    Arrow Datasets allow you to query against data that has been split across
    multiple files. This sharding of data may indicate partitioning, which
    can accelerate queries that only touch some partitions (files).
    """

    cdef:
        shared_ptr[CDataset] wrapped
        CDataset* dataset

    def __init__(self):
        _forbid_instantiation(self.__class__)

    cdef void init(self, const shared_ptr[CDataset]& sp):
        self.wrapped = sp
        self.dataset = sp.get()

    @staticmethod
    cdef wrap(const shared_ptr[CDataset]& sp):
        cdef Dataset self

        typ = frombytes(sp.get().type_name())
        if typ == 'union':
            self = UnionDataset.__new__(UnionDataset)
        elif typ == 'filesystem':
            self = FileSystemDataset.__new__(FileSystemDataset)
        else:
            raise TypeError(typ)

        self.init(sp)
        return self

    cdef shared_ptr[CDataset] unwrap(self) nogil:
        return self.wrapped

    @property
    def partition_expression(self):
        """
        An Expression which evaluates to true for all data viewed by this
        Dataset.
        """
        cdef shared_ptr[CExpression] expr
        expr = self.dataset.partition_expression()
        if expr.get() == nullptr:
            return None
        else:
            return Expression.wrap(expr)

    def replace_schema(self, Schema schema not None):
        """
        Return a copy of this Dataset with a different schema.

        The copy will view the same Fragments. If the new schema is not
        compatible with the original dataset's schema then an error will
        be raised.
        """
        cdef shared_ptr[CDataset] copy = GetResultValue(
            self.dataset.ReplaceSchema(pyarrow_unwrap_schema(schema)))
        return Dataset.wrap(move(copy))

    def get_fragments(self, columns=None, filter=None):
        """Returns an iterator over the fragments in this dataset.

        Parameters
        ----------
        columns : list of str, default None
            List of columns to project.
        filter : Expression, default None
            Scan will return only the rows matching the filter.

        Returns
        -------
        fragments : iterator of Fragment
        """
        return Scanner(self, columns=columns, filter=filter).get_fragments()

    def scan(self, columns=None, filter=None, MemoryPool memory_pool=None):
        """Builds a scan operation against the dataset.

        It poduces a stream of ScanTasks which is meant to be a unit of work to
        be dispatched. The tasks are not executed automatically, the user is
        responsible to execute and dispatch the individual tasks, so custom
        local task scheduling can be implemented.

        Parameters
        ----------
        columns : list of str, default None
            List of columns to project. Order and duplicates will be preserved.
            The columns will be passed down to Datasets and corresponding data
            fragments to avoid loading, copying, and deserializing columns
            that will not be required further down the compute chain.
            By default all of the available columns are projected. Raises
            an exception if any of the referenced column names does not exist
            in the dataset's Schema.
        filter : Expression, default None
            Scan will return only the rows matching the filter.
            If possible the predicate will be pushed down to exploit the
            partition information or internal metadata found in fragments,
            e.g. Parquet statistics. Otherwise filters the loaded
            RecordBatches before yielding them.
        memory_pool : MemoryPool, default None
            For memory allocations, if required. If not specified, uses the
            default pool.

        Returns
        -------
        scan_tasks : iterator of ScanTask
        """
        scanner = Scanner(self, columns=columns, filter=filter,
                          memory_pool=memory_pool)
        return scanner.scan()

    def to_batches(self, columns=None, filter=None,
                   MemoryPool memory_pool=None):
        """Read the dataset as materialized record batches.

        Builds a scan operation against the dataset and sequentially executes
        the ScanTasks as the returned generator gets consumed.

        Parameters
        ----------
        columns : list of str, default None
            List of columns to project. Order and duplicates will be preserved.
            The columns will be passed down to Datasets and corresponding data
            fragments to avoid loading, copying, and deserializing columns
            that will not be required further down the compute chain.
            By default all of the available columns are projected. Raises
            an exception if any of the referenced column names does not exist
            in the dataset's Schema.
        filter : Expression, default None
            Scan will return only the rows matching the filter.
            If possible the predicate will be pushed down to exploit the
            partition information or internal metadata found in the fragments,
            e.g. Parquet statistics. Otherwise filters the loaded
            RecordBatches before yielding them.
        memory_pool : MemoryPool, default None
            For memory allocations, if required. If not specified, uses the
            default pool.

        Returns
        -------
        record_batches : iterator of RecordBatch
        """
        scanner = Scanner(self, columns=columns, filter=filter,
                          memory_pool=memory_pool)
        for task in scanner.scan():
            for batch in task.execute():
                yield batch

    def to_table(self, columns=None, filter=None, use_threads=True,
                 MemoryPool memory_pool=None):
        """Read the dataset to an arrow table.

        Note that this method reads all the selected data from the dataset
        into memory.

        Parameters
        ----------
        columns : list of str, default None
            List of columns to project. Order and duplicates will be preserved.
            The columns will be passed down to Datasets and corresponding data
            fragments to avoid loading, copying, and deserializing columns
            that will not be required further down the compute chain.
            By default all of the available columns are projected. Raises
            an exception if any of the referenced column names does not exist
            in the dataset's Schema.
        filter : Expression, default None
            Scan will return only the rows matching the filter.
            If possible the predicate will be pushed down to exploit the
            partition information or internal metadata found in the fragments,
            e.g. Parquet statistics. Otherwise filters the loaded
            RecordBatches before yielding them.
        use_threads : boolean, default True
            If enabled, then maximum parallelism will be used determined by
            the number of available CPU cores.
        memory_pool : MemoryPool, default None
            For memory allocations, if required. If not specified, uses the
            default pool.

        Returns
        -------
        table : Table instance
        """
        scanner = Scanner(self, columns=columns, filter=filter,
                          use_threads=use_threads, memory_pool=memory_pool)
        return scanner.to_table()

    @property
    def schema(self):
        """The common schema of the full Dataset"""
        return pyarrow_wrap_schema(self.dataset.schema())


cdef class UnionDataset(Dataset):
    """A Dataset wrapping child datasets.

    Children's schemas must agree with the provided schema.

    Parameters
    ----------
    schema : Schema
        A known schema to conform to.
    children : list of Dataset
        One or more input children
    """

    cdef:
        CUnionDataset* union_dataset

    def __init__(self, Schema schema not None, children):
        cdef:
            Dataset child
            CDatasetVector c_children
            shared_ptr[CUnionDataset] union_dataset

        for child in children:
            c_children.push_back(child.wrapped)

        union_dataset = GetResultValue(CUnionDataset.Make(
            pyarrow_unwrap_schema(schema), move(c_children)))
        self.init(<shared_ptr[CDataset]> union_dataset)

    cdef void init(self, const shared_ptr[CDataset]& sp):
        Dataset.init(self, sp)
        self.union_dataset = <CUnionDataset*> sp.get()


cdef class FileSystemDataset(Dataset):
    """A Dataset created from a set of files on a particular filesystem.

    Parameters
    ----------
    paths_or_selector : Union[FileSelector, List[FileInfo]]
        List of files/directories to consume.
    schema : Schema
        The top-level schema of the DataDataset.
    format : FileFormat
        File format to create fragments from, currently only
        ParquetFileFormat and IpcFileFormat are supported.
    filesystem : FileSystem
        The filesystem which files are from.
    partitions : List[Expression], optional
        Attach additional partition information for the file paths.
    root_partition : Expression, optional
        The top-level partition of the DataDataset.
    """

    cdef:
        CFileSystemDataset* filesystem_dataset

    def __init__(self, paths_or_selector, schema=None, format=None,
                 filesystem=None, partitions=None, root_partition=None):
        cdef:
            FileInfo info
            Expression expr
            vector[CFileInfo] c_file_infos
            vector[shared_ptr[CExpression]] c_partitions
            CResult[shared_ptr[CDataset]] result

        # validate required arguments
        for arg, class_, name in [
            (schema, Schema, 'schema'),
            (format, FileFormat, 'format'),
            (filesystem, FileSystem, 'filesystem')
        ]:
            if not isinstance(arg, class_):
                raise TypeError(
                    "Argument '{0}' has incorrect type (expected {1}, "
                    "got {2})".format(name, class_.__name__, type(arg))
                )

        for info in filesystem.get_file_info(paths_or_selector):
            c_file_infos.push_back(info.unwrap())

        if partitions is None:
            partitions = [
                ScalarExpression(True) for _ in range(c_file_infos.size())]
        for expr in partitions:
            c_partitions.push_back(expr.unwrap())

        if c_file_infos.size() != c_partitions.size():
            raise ValueError(
                'The number of files resulting from paths_or_selector '
                'must be equal to the number of partitions.'
            )

        if root_partition is None:
            root_partition = ScalarExpression(True)
        elif not isinstance(root_partition, Expression):
            raise TypeError(
                "Argument 'root_partition' has incorrect type (expected "
                "Expression, got {0})".format(type(root_partition))
            )

        result = CFileSystemDataset.Make(
            pyarrow_unwrap_schema(schema),
            (<Expression> root_partition).unwrap(),
            (<FileFormat> format).unwrap(),
            (<FileSystem> filesystem).unwrap(),
            c_file_infos,
            c_partitions
        )
        self.init(GetResultValue(result))

    cdef void init(self, const shared_ptr[CDataset]& sp):
        Dataset.init(self, sp)
        self.filesystem_dataset = <CFileSystemDataset*> sp.get()

    @property
    def files(self):
        """List of the files"""
        cdef vector[c_string] files = self.filesystem_dataset.files()
        return [frombytes(f) for f in files]

    @property
    def format(self):
        """The FileFormat of this source."""
        return FileFormat.wrap(self.filesystem_dataset.format())

cdef shared_ptr[CExpression] _insert_implicit_casts(Expression filter,
                                                    Schema schema) except *:
    assert schema is not None

    if filter is None:
        return ScalarExpression(True).unwrap()

    return GetResultValue(
        CInsertImplicitCasts(
            deref(filter.unwrap().get()),
            deref(pyarrow_unwrap_schema(schema).get())
        )
    )


cdef shared_ptr[CScanOptions] _make_scan_options(
        Schema schema, Expression partition_expression, object columns=None,
        Expression filter=None) except *:
    cdef:
        shared_ptr[CScanOptions] options
        CExpression* c_partition_expression

    assert schema is not None
    assert partition_expression is not None

    filter = Expression.wrap(_insert_implicit_casts(filter, schema))
    filter = filter.assume(partition_expression)

    empty_dataset = UnionDataset(schema, children=[])
    scanner = Scanner(empty_dataset, columns=columns, filter=filter)
    options = scanner.unwrap().get().options()

    c_partition_expression = partition_expression.unwrap().get()
    check_status(CSetPartitionKeysInProjector(deref(c_partition_expression),
                                              &options.get().projector))

    return options


cdef class FileFormat:

    cdef:
        shared_ptr[CFileFormat] wrapped
        CFileFormat* format

    def __init__(self):
        _forbid_instantiation(self.__class__)

    cdef void init(self, const shared_ptr[CFileFormat]& sp):
        self.wrapped = sp
        self.format = sp.get()

    @staticmethod
    cdef wrap(const shared_ptr[CFileFormat]& sp):
        cdef FileFormat self

        typ = frombytes(sp.get().type_name())
        if typ == 'parquet':
            self = ParquetFileFormat.__new__(ParquetFileFormat)
        elif typ == 'ipc':
            self = IpcFileFormat.__new__(IpcFileFormat)
        else:
            raise TypeError(typ)

        self.init(sp)
        return self

    cdef inline shared_ptr[CFileFormat] unwrap(self):
        return self.wrapped

    def inspect(self, str path not None, FileSystem filesystem not None):
        """Infer the schema of a file."""
        cdef:
            shared_ptr[CSchema] c_schema

        c_schema = GetResultValue(self.format.Inspect(CFileSource(
            tobytes(path), filesystem.unwrap().get())))
        return pyarrow_wrap_schema(move(c_schema))

    def make_fragment(self, str path not None, FileSystem filesystem not None,
                      Schema schema=None, columns=None, filter=None,
                      Expression partition_expression=ScalarExpression(True)):
        """
        Make a FileFragment of this FileFormat. The filter may not reference
        fields absent from the provided schema. If no schema is provided then
        one will be inferred.
        """
        cdef:
            shared_ptr[CScanOptions] c_options
            shared_ptr[CFileFragment] c_fragment

        if schema is None:
            schema = self.inspect(path, filesystem)

        c_options = _make_scan_options(schema, partition_expression,
                                       columns, filter)

        c_fragment = GetResultValue(
            self.format.MakeFragment(CFileSource(tobytes(path),
                                                 filesystem.unwrap().get()),
                                     move(c_options),
                                     partition_expression.unwrap()))
        return Fragment.wrap(<shared_ptr[CFragment]> move(c_fragment))

    def __eq__(self, other):
        try:
            return self.equals(other)
        except TypeError:
            return False


cdef class Fragment:
    """Fragment of data from a Dataset."""

    cdef:
        shared_ptr[CFragment] wrapped
        CFragment* fragment

    cdef void init(self, const shared_ptr[CFragment]& sp):
        self.wrapped = sp
        self.fragment = sp.get()

    @staticmethod
    cdef wrap(const shared_ptr[CFragment]& sp):
        # there's no discriminant in Fragment, so we can't downcast
        # to FileFragment for the path property
        cdef Fragment self = Fragment()

        typ = frombytes(sp.get().type_name())
        if typ == 'ipc':
            # IpcFileFormat does not have a corresponding subclass
            # of FileFragment
            self = FileFragment.__new__(FileFragment)
        elif typ == 'parquet':
            self = ParquetFileFragment.__new__(ParquetFileFragment)
        else:
            self = Fragment()

        self.init(sp)
        return self

    cdef inline shared_ptr[CFragment] unwrap(self):
        return self.wrapped

    @property
    def schema(self):
        """Return the schema of batches scanned from this Fragment."""
        return pyarrow_wrap_schema(self.fragment.schema())

    @property
    def partition_expression(self):
        """An Expression which evaluates to true for all data viewed by this
        Fragment.
        """
        return Expression.wrap(self.fragment.partition_expression())

    def to_table(self, use_threads=True, MemoryPool memory_pool=None):
        """Convert this Fragment into a Table.

        Use this convenience utility with care. This will serially materialize
        the Scan result in memory before creating the Table.

        Returns
        -------
        table : Table
        """
        scanner = Scanner._from_fragment(self, use_threads, memory_pool)
        return scanner.to_table()

    def scan(self, MemoryPool memory_pool=None):
        """Returns a stream of ScanTasks

        The caller is responsible to dispatch/schedule said tasks. Tasks should
        be safe to run in a concurrent fashion and outlive the iterator.

        Returns
        -------
        scan_tasks : iterator of ScanTask
        """
        return Scanner._from_fragment(self, memory_pool).scan()


cdef class FileFragment(Fragment):
    """A Fragment representing a data file."""

    cdef:
        CFileFragment* file_fragment

    cdef void init(self, const shared_ptr[CFragment]& sp):
        Fragment.init(self, sp)
        self.file_fragment = <CFileFragment*> sp.get()

    @property
    def path(self):
        """
        The path of the data file viewed by this fragment, if it views a
        file. If instead it views a buffer, this will be "<Buffer>".
        """
        return frombytes(self.file_fragment.source().path())

    @property
    def filesystem(self):
        """
        The FileSystem containing the data file viewed by this fragment, if
        it views a file. If instead it views a buffer, this will be None.
        """
        cdef:
            shared_ptr[CFileSystem] fs
        fs = self.file_fragment.source().filesystem().shared_from_this()
        return FileSystem.wrap(fs)

    @property
    def buffer(self):
        """
        The buffer viewed by this fragment, if it views a buffer. If
        instead it views a file, this will be None.
        """
        cdef:
            shared_ptr[CBuffer] c_buffer
        c_buffer = self.file_fragment.source().buffer()

        if c_buffer.get() == nullptr:
            return None

        return pyarrow_wrap_buffer(c_buffer)

    @property
    def format(self):
        """
        The format of the data file viewed by this fragment.
        """
        return FileFormat.wrap(self.file_fragment.format())


cdef class ParquetFileFragment(FileFragment):
    """A Fragment representing a parquet file."""

    cdef:
        CParquetFileFragment* parquet_file_fragment

    cdef void init(self, const shared_ptr[CFragment]& sp):
        FileFragment.init(self, sp)
        self.parquet_file_fragment = <CParquetFileFragment*> sp.get()

    @property
    def row_groups(self):
        row_groups = set(self.parquet_file_fragment.row_groups())
        if len(row_groups) != 0:
            return row_groups
        return None

    def get_row_group_fragments(self, Expression extra_filter=None):
        """
        Yield a Fragment wrapping each row group in this ParquetFileFragment.
        Row groups will be excluded whose metadata contradicts the either the
        filter provided on construction of this Fragment or the extra_filter
        argument.
        """
        cdef:
            CParquetFileFormat* c_format
            CFragmentIterator c_iterator
            shared_ptr[CExpression] c_extra_filter
            shared_ptr[CFragment] c_fragment

        c_extra_filter = _insert_implicit_casts(extra_filter, self.schema)
        c_format = <CParquetFileFormat*> self.file_fragment.format().get()
        c_iterator = move(GetResultValue(c_format.GetRowGroupFragments(deref(
            self.parquet_file_fragment), move(c_extra_filter))))

        while True:
            c_fragment = GetResultValue(c_iterator.Next())
            if c_fragment.get() == nullptr:
                raise StopIteration()
            else:
                yield Fragment.wrap(c_fragment)


cdef class ParquetReadOptions:
    """
    Parquet format specific options for reading.

    Parameters
    ----------
    use_buffered_stream : bool, default False
        Read files through buffered input streams rather than loading entire
        row groups at once. This may be enabled to reduce memory overhead.
        Disabled by default.
    buffer_size : int, default 8192
        Size of buffered stream, if enabled. Default is 8KB.
    dictionary_columns : list of string, default None
        Names of columns which should be dictionary encoded as
        they are read.
    """

    cdef public:
        bint use_buffered_stream
        uint32_t buffer_size
        set dictionary_columns

    def __init__(self, bint use_buffered_stream=False,
                 buffer_size=8192,
                 dictionary_columns=None):
        self.use_buffered_stream = use_buffered_stream
        if buffer_size <= 0:
            raise ValueError("Buffer size must be larger than zero")
        self.buffer_size = buffer_size
        self.dictionary_columns = set(dictionary_columns or set())

    def equals(self, ParquetReadOptions other):
        return (
            self.use_buffered_stream == other.use_buffered_stream and
            self.buffer_size == other.buffer_size and
            self.dictionary_columns == other.dictionary_columns
        )

    def __eq__(self, other):
        try:
            return self.equals(other)
        except TypeError:
            return False


cdef class ParquetFileFormat(FileFormat):

    cdef:
        CParquetFileFormat* parquet_format

    def __init__(self, read_options=None):
        cdef:
            shared_ptr[CParquetFileFormat] wrapped
            CParquetFileFormatReaderOptions* options

        if read_options is None:
            read_options = ParquetReadOptions()
        elif isinstance(read_options, dict):
            read_options = ParquetReadOptions(**read_options)
        elif not isinstance(read_options, ParquetReadOptions):
            raise TypeError('`read_options` must be either a dictionary or an '
                            'instance of ParquetReadOptions')

        wrapped = make_shared[CParquetFileFormat]()
        options = &(wrapped.get().reader_options)
        options.use_buffered_stream = read_options.use_buffered_stream
        options.buffer_size = read_options.buffer_size
        if read_options.dictionary_columns is not None:
            for column in read_options.dictionary_columns:
                options.dict_columns.insert(tobytes(column))

        self.init(<shared_ptr[CFileFormat]> wrapped)

    cdef void init(self, const shared_ptr[CFileFormat]& sp):
        FileFormat.init(self, sp)
        self.parquet_format = <CParquetFileFormat*> sp.get()

    @property
    def read_options(self):
        cdef CParquetFileFormatReaderOptions* options
        options = &self.parquet_format.reader_options
        return ParquetReadOptions(
            use_buffered_stream=options.use_buffered_stream,
            buffer_size=options.buffer_size,
            dictionary_columns={frombytes(col) for col in options.dict_columns}
        )

    def equals(self, ParquetFileFormat other):
        return self.read_options.equals(other.read_options)

    def __reduce__(self):
        return ParquetFileFormat, (self.read_options,)

    def make_fragment(self, str path not None, FileSystem filesystem not None,
                      Schema schema=None, columns=None, filter=None,
                      Expression partition_expression=ScalarExpression(True),
                      row_groups=None):
        cdef:
            shared_ptr[CScanOptions] c_options
            shared_ptr[CFileFragment] c_fragment
            vector[int] c_row_groups

        if row_groups is None:
            return super().make_fragment(path, filesystem, schema, columns,
                                         filter, partition_expression)
        for row_group in set(row_groups):
            c_row_groups.push_back(<int> row_group)

        if schema is None:
            schema = self.inspect(path, filesystem)

        c_options = _make_scan_options(schema, partition_expression,
                                       columns, filter)

        c_fragment = GetResultValue(
            self.parquet_format.MakeFragment(CFileSource(tobytes(path),
                                                         filesystem.unwrap()
                                                         .get()),
                                             move(c_options),
                                             partition_expression.unwrap(),
                                             move(c_row_groups)))
        return Fragment.wrap(<shared_ptr[CFragment]> move(c_fragment))


cdef class IpcFileFormat(FileFormat):

    def __init__(self):
        self.init(shared_ptr[CFileFormat](new CIpcFileFormat()))

    def equals(self, IpcFileFormat other):
        return True

    def __reduce__(self):
        return IpcFileFormat, tuple()


cdef class Partitioning:

    cdef:
        shared_ptr[CPartitioning] wrapped
        CPartitioning* partitioning

    def __init__(self):
        _forbid_instantiation(self.__class__)

    cdef init(self, const shared_ptr[CPartitioning]& sp):
        self.wrapped = sp
        self.partitioning = sp.get()

    @staticmethod
    cdef wrap(const shared_ptr[CPartitioning]& sp):
        cdef Partitioning self

        typ = frombytes(sp.get().type_name())
        if typ == 'schema':
            self = DirectoryPartitioning.__new__(DirectoryPartitioning)
        elif typ == 'hive':
            self = HivePartitioning.__new__(HivePartitioning)
        else:
            raise TypeError(typ)

        self.init(sp)
        return self

    cdef inline shared_ptr[CPartitioning] unwrap(self):
        return self.wrapped

    def parse(self, path):
        cdef CResult[shared_ptr[CExpression]] result
        result = self.partitioning.Parse(tobytes(path))
        return Expression.wrap(GetResultValue(result))

    @property
    def schema(self):
        """The arrow Schema attached to the partitioning."""
        return pyarrow_wrap_schema(self.partitioning.schema())


cdef class PartitioningFactory:

    cdef:
        shared_ptr[CPartitioningFactory] wrapped
        CPartitioningFactory* factory

    def __init__(self):
        _forbid_instantiation(self.__class__)

    cdef init(self, const shared_ptr[CPartitioningFactory]& sp):
        self.wrapped = sp
        self.factory = sp.get()

    @staticmethod
    cdef wrap(const shared_ptr[CPartitioningFactory]& sp):
        cdef PartitioningFactory self = PartitioningFactory.__new__(
            PartitioningFactory
        )
        self.init(sp)
        return self

    cdef inline shared_ptr[CPartitioningFactory] unwrap(self):
        return self.wrapped


cdef class DirectoryPartitioning(Partitioning):
    """
    A Partitioning based on a specified Schema.

    The DirectoryPartitioning expects one segment in the file path for each
    field in the schema (all fields are required to be present).
    For example given schema<year:int16, month:int8> the path "/2009/11" would
    be parsed to ("year"_ == 2009 and "month"_ == 11).

    Parameters
    ----------
    schema : Schema
        The schema that describes the partitions present in the file path.

    Returns
    -------
    DirectoryPartitioning

    Examples
    --------
    >>> from pyarrow.dataset import DirectoryPartitioning
    >>> partition = DirectoryPartitioning(
    ...     pa.schema([("year", pa.int16()), ("month", pa.int8())]))
    >>> print(partitioning.parse("/2009/11"))
    ((year == 2009:int16) and (month == 11:int8))
    """

    cdef:
        CDirectoryPartitioning* directory_partitioning

    def __init__(self, Schema schema not None):
        cdef shared_ptr[CDirectoryPartitioning] partitioning
        partitioning = make_shared[CDirectoryPartitioning](
            pyarrow_unwrap_schema(schema)
        )
        self.init(<shared_ptr[CPartitioning]> partitioning)

    cdef init(self, const shared_ptr[CPartitioning]& sp):
        Partitioning.init(self, sp)
        self.directory_partitioning = <CDirectoryPartitioning*> sp.get()

    @staticmethod
    def discover(field_names):
        """
        Discover a DirectoryPartitioning.

        Parameters
        ----------
        field_names : list of str
            The names to associate with the values from the subdirectory names.

        Returns
        -------
        DirectoryPartitioningFactory
            To be used in the FileSystemFactoryOptions.
        """
        cdef:
            PartitioningFactory factory
            vector[c_string] c_field_names
        c_field_names = [tobytes(s) for s in field_names]
        factory = PartitioningFactory.wrap(
            CDirectoryPartitioning.MakeFactory(c_field_names))
        return factory


cdef class HivePartitioning(Partitioning):
    """
    A Partitioning for "/$key=$value/" nested directories as found in
    Apache Hive.

    Multi-level, directory based partitioning scheme originating from
    Apache Hive with all data files stored in the leaf directories. Data is
    partitioned by static values of a particular column in the schema.
    Partition keys are represented in the form $key=$value in directory names.
    Field order is ignored, as are missing or unrecognized field names.

    For example, given schema<year:int16, month:int8, day:int8>, a possible
    path would be "/year=2009/month=11/day=15".

    Parameters
    ----------
    schema : Schema
        The schema that describes the partitions present in the file path.

    Returns
    -------
    HivePartitioning

    Examples
    --------
    >>> from pyarrow.dataset import HivePartitioning
    >>> partitioning = HivePartitioning(
    ...     pa.schema([("year", pa.int16()), ("month", pa.int8())]))
    >>> print(partitioning.parse("/year=2009/month=11"))
    ((year == 2009:int16) and (month == 11:int8))

    """

    cdef:
        CHivePartitioning* hive_partitioning

    def __init__(self, Schema schema not None):
        cdef shared_ptr[CHivePartitioning] partitioning
        partitioning = make_shared[CHivePartitioning](
            pyarrow_unwrap_schema(schema)
        )
        self.init(<shared_ptr[CPartitioning]> partitioning)

    cdef init(self, const shared_ptr[CPartitioning]& sp):
        Partitioning.init(self, sp)
        self.hive_partitioning = <CHivePartitioning*> sp.get()

    @staticmethod
    def discover():
        """
        Discover a HivePartitioning.

        Returns
        -------
        PartitioningFactory
            To be used in the FileSystemFactoryOptions.
        """
        cdef:
            PartitioningFactory factory
        factory = PartitioningFactory.wrap(
            CHivePartitioning.MakeFactory())
        return factory


cdef class DatasetFactory:
    """
    DatasetFactory is used to create a Dataset, inspect the Schema
    of the fragments contained in it, and declare a partitioning.
    """

    cdef:
        shared_ptr[CDatasetFactory] wrapped
        CDatasetFactory* factory

    def __init__(self, list children):
        _forbid_instantiation(self.__class__)

    cdef init(self, const shared_ptr[CDatasetFactory]& sp):
        self.wrapped = sp
        self.factory = sp.get()

    @staticmethod
    cdef wrap(const shared_ptr[CDatasetFactory]& sp):
        cdef DatasetFactory self = \
            DatasetFactory.__new__(DatasetFactory)
        self.init(sp)
        return self

    cdef inline shared_ptr[CDatasetFactory] unwrap(self) nogil:
        return self.wrapped

    @property
    def root_partition(self):
        cdef shared_ptr[CExpression] expr = self.factory.root_partition()
        if expr.get() == nullptr:
            return None
        else:
            return Expression.wrap(expr)

    @root_partition.setter
    def root_partition(self, Expression expr):
        check_status(self.factory.SetRootPartition(expr.unwrap()))

    def inspect_schemas(self):
        cdef CResult[vector[shared_ptr[CSchema]]] result
        cdef CInspectOptions options
        with nogil:
            result = self.factory.InspectSchemas(options)

        schemas = []
        for s in GetResultValue(result):
            schemas.append(pyarrow_wrap_schema(s))
        return schemas

    def inspect(self):
        """
        Inspect all data fragments and return a common Schema.

        Returns
        -------
        Schema
        """
        cdef:
            CInspectOptions options
            CResult[shared_ptr[CSchema]] result
        with nogil:
            result = self.factory.Inspect(options)
        return pyarrow_wrap_schema(GetResultValue(result))

    def finish(self, Schema schema=None):
        """
        Create a Dataset using the inspected schema or an explicit schema
        (if given).

        Parameters
        ----------
        schema: Schema, default None
            The schema to conform the source to.  If None, the inspected
            schema is used.

        Returns
        -------
        Dataset
        """
        cdef:
            shared_ptr[CSchema] sp_schema
            CResult[shared_ptr[CDataset]] result

        if schema is not None:
            sp_schema = pyarrow_unwrap_schema(schema)
            with nogil:
                result = self.factory.FinishWithSchema(sp_schema)
        else:
            with nogil:
                result = self.factory.Finish()

        return Dataset.wrap(GetResultValue(result))


cdef class FileSystemFactoryOptions:
    """
    Influences the discovery of filesystem paths.

    Parameters
    ----------
    partition_base_dir : str, optional
        For the purposes of applying the partitioning, paths will be
        stripped of the partition_base_dir. Files not matching the
        partition_base_dir prefix will be skipped for partitioning discovery.
        The ignored files will still be part of the Dataset, but will not
        have partition information.
    exclude_invalid_files : bool, optional (default True)
        If True, invalid files will be excluded (file format specific check).
        This will incur IO for each files in a serial and single threaded
        fashion. Disabling this feature will skip the IO, but unsupported
        files may be present in the Dataset (resulting in an error at scan
        time).
    selector_ignore_prefixes : list, optional
        When discovering from a Selector (and not from an explicit file list),
        ignore files and directories matching any of these prefixes.
        By default this is ['.', '_'].
    """

    cdef:
        CFileSystemFactoryOptions options

    __slots__ = ()  # avoid mistakingly creating attributes

    def __init__(self, partition_base_dir=None, partitioning=None,
                 exclude_invalid_files=None,
                 list selector_ignore_prefixes=None):
        if isinstance(partitioning, PartitioningFactory):
            self.partitioning_factory = partitioning
        elif isinstance(partitioning, Partitioning):
            self.partitioning = partitioning

        if partition_base_dir is not None:
            self.partition_base_dir = partition_base_dir
        if exclude_invalid_files is not None:
            self.exclude_invalid_files = exclude_invalid_files
        if selector_ignore_prefixes is not None:
            self.selector_ignore_prefixes = selector_ignore_prefixes

    cdef inline CFileSystemFactoryOptions unwrap(self):
        return self.options

    @property
    def partitioning(self):
        """Partitioning to apply to discovered files.

        NOTE: setting this property will overwrite partitioning_factory.
        """
        c_partitioning = self.options.partitioning.partitioning()
        if c_partitioning.get() == nullptr:
            return None
        return Partitioning.wrap(c_partitioning)

    @partitioning.setter
    def partitioning(self, Partitioning value):
        self.options.partitioning = (<Partitioning> value).unwrap()

    @property
    def partitioning_factory(self):
        """PartitioningFactory to apply to discovered files and
        discover a Partitioning.

        NOTE: setting this property will overwrite partitioning.
        """
        c_factory = self.options.partitioning.factory()
        if c_factory.get() == nullptr:
            return None
        return PartitioningFactory.wrap(c_factory)

    @partitioning_factory.setter
    def partitioning_factory(self, PartitioningFactory value):
        self.options.partitioning = (<PartitioningFactory> value).unwrap()

    @property
    def partition_base_dir(self):
        """
        Base directory to strip paths before applying the partitioning.
        """
        return frombytes(self.options.partition_base_dir)

    @partition_base_dir.setter
    def partition_base_dir(self, value):
        self.options.partition_base_dir = tobytes(value)

    @property
    def exclude_invalid_files(self):
        """Whether to exclude invalid files."""
        return self.options.exclude_invalid_files

    @exclude_invalid_files.setter
    def exclude_invalid_files(self, bint value):
        self.options.exclude_invalid_files = value

    @property
    def selector_ignore_prefixes(self):
        """
        List of prefixes. Files matching one of those prefixes will be
        ignored by the discovery process.
        """
        return [frombytes(p) for p in self.options.selector_ignore_prefixes]

    @selector_ignore_prefixes.setter
    def selector_ignore_prefixes(self, values):
        self.options.selector_ignore_prefixes = [tobytes(v) for v in values]


cdef class FileSystemDatasetFactory(DatasetFactory):
    """
    Create a DatasetFactory from a list of paths with schema inspection.

    Parameters
    ----------
    filesystem : pyarrow.fs.FileSystem
        Filesystem to discover.
    paths_or_selector: pyarrow.fs.Selector or list of path-likes
        Either a Selector object or a list of path-like objects.
    format : FileFormat
        Currently only ParquetFileFormat and IpcFileFormat are supported.
    options : FileSystemFactoryOptions, optional
        Various flags influencing the discovery of filesystem paths.
    """

    cdef:
        CFileSystemDatasetFactory* filesystem_factory

    def __init__(self, FileSystem filesystem not None, paths_or_selector,
                 FileFormat format not None,
                 FileSystemFactoryOptions options=None):
        cdef:
            vector[c_string] paths
            CFileSelector selector
            CResult[shared_ptr[CDatasetFactory]] result
            shared_ptr[CFileSystem] c_filesystem
            shared_ptr[CFileFormat] c_format
            CFileSystemFactoryOptions c_options

        c_filesystem = filesystem.unwrap()
        c_format = format.unwrap()

        options = options or FileSystemFactoryOptions()
        c_options = options.unwrap()

        if isinstance(paths_or_selector, FileSelector):
            with nogil:
                selector = (<FileSelector>paths_or_selector).selector
                result = CFileSystemDatasetFactory.MakeFromSelector(
                    c_filesystem,
                    selector,
                    c_format,
                    c_options
                )
        elif isinstance(paths_or_selector, (list, tuple)):
            paths = [tobytes(s) for s in paths_or_selector]
            with nogil:
                result = CFileSystemDatasetFactory.MakeFromPaths(
                    c_filesystem,
                    paths,
                    c_format,
                    c_options
                )
        else:
            raise TypeError('Must pass either paths or a FileSelector, but '
                            'passed {}'.format(type(paths_or_selector)))

        self.init(GetResultValue(result))

    cdef init(self, shared_ptr[CDatasetFactory]& sp):
        DatasetFactory.init(self, sp)
        self.filesystem_factory = <CFileSystemDatasetFactory*> sp.get()


cdef class UnionDatasetFactory(DatasetFactory):
    """
    Provides a way to inspect/discover a Dataset's expected schema before
    materialization.

    Parameters
    ----------
    factories : list of DatasetFactory
    """

    cdef:
        CUnionDatasetFactory* union_factory

    def __init__(self, list factories):
        cdef:
            DatasetFactory factory
            vector[shared_ptr[CDatasetFactory]] c_factories
        for factory in factories:
            c_factories.push_back(factory.unwrap())
        self.init(GetResultValue(CUnionDatasetFactory.Make(c_factories)))

    cdef init(self, const shared_ptr[CDatasetFactory]& sp):
        DatasetFactory.init(self, sp)
        self.union_factory = <CUnionDatasetFactory*> sp.get()


cdef class ScanTask:
    """Read record batches from a range of a single data fragment.

    A ScanTask is meant to be a unit of work to be dispatched.
    """

    cdef:
        shared_ptr[CScanTask] wrapped
        CScanTask* task

    def __init__(self):
        _forbid_instantiation(self.__class__, subclasses_instead=False)

    cdef init(self, shared_ptr[CScanTask]& sp):
        self.wrapped = sp
        self.task = self.wrapped.get()

    @staticmethod
    cdef wrap(shared_ptr[CScanTask]& sp):
        cdef ScanTask self = ScanTask.__new__(ScanTask)
        self.init(sp)
        return self

    cdef inline shared_ptr[CScanTask] unwrap(self) nogil:
        return self.wrapped

    def execute(self):
        """Iterate through sequence of materialized record batches.

        Execution semantics are encapsulated in the particular ScanTask
        implementation.

        Returns
        -------
        record_batches : iterator of RecordBatch
        """
        cdef:
            CRecordBatchIterator iterator
            shared_ptr[CRecordBatch] record_batch

        with nogil:
            iterator = move(GetResultValue(move(self.task.Execute())))

            while True:
                record_batch = GetResultValue(iterator.Next())
                if record_batch.get() == nullptr:
                    raise StopIteration()
                else:
                    with gil:
                        yield pyarrow_wrap_batch(record_batch)


cdef class Scanner:
    """A materialized scan operation with context and options bound.

    A scanner is the class that glues the scan tasks, data fragments and data
    sources together.

    Parameters
    ----------
    dataset : Dataset
        Dataset to scan.
    columns : list of str, default None
        List of columns to project. Order and duplicates will be preserved.
        The columns will be passed down to Datasets and corresponding data
        fragments to avoid loading, copying, and deserializing columns
        that will not be required further down the compute chain.
        By default all of the available columns are projected. Raises
        an exception if any of the referenced column names does not exist
        in the dataset's Schema.
    filter : Expression, default None
        Scan will return only the rows matching the filter.
        If possible the predicate will be pushed down to exploit the
        partition information or internal metadata found in the data
        source, e.g. Parquet statistics. Otherwise filters the loaded
        RecordBatches before yielding them.
    use_threads : bool, default True
        If enabled, then maximum parallelism will be used determined by
        the number of available CPU cores.
    batch_size : int, default 32K
        The maximum row count for scanned record batches. If scanned
        record batches are overflowing memory then this method can be
        called to reduce their size.
    memory_pool : MemoryPool, default None
        For memory allocations, if required. If not specified, uses the
        default pool.
    """

    cdef:
        shared_ptr[CScanner] wrapped
        CScanner* scanner

    def __init__(self, Dataset dataset, list columns=None,
                 Expression filter=None, bint use_threads=True,
                 int batch_size=32*2**10, MemoryPool memory_pool=None):
        cdef:
            shared_ptr[CScanContext] context
            shared_ptr[CScannerBuilder] builder
            shared_ptr[CExpression] filter_expression
            vector[c_string] columns_to_project

        # create scan context
        context = make_shared[CScanContext]()
        context.get().pool = maybe_unbox_memory_pool(memory_pool)
        if use_threads is not None:
            context.get().use_threads = use_threads

        # create scanner builder
        builder = GetResultValue(
            dataset.unwrap().get().NewScanWithContext(context)
        )

        # set the builder's properties
        if columns is not None:
            check_status(builder.get().Project([tobytes(c) for c in columns]))

        check_status(builder.get().Filter(_insert_implicit_casts(
            filter, pyarrow_wrap_schema(builder.get().schema())
        )))

        check_status(builder.get().BatchSize(batch_size))

        # instantiate the scanner object
        scanner = GetResultValue(builder.get().Finish())
        self.init(scanner)

    cdef void init(self, const shared_ptr[CScanner]& sp):
        self.wrapped = sp
        self.scanner = sp.get()

    @staticmethod
    cdef wrap(const shared_ptr[CScanner]& sp):
        cdef Scanner self = Scanner.__new__(Scanner)
        self.init(sp)
        return self

    @staticmethod
    def _from_fragment(Fragment fragment not None, bint use_threads=True,
                       MemoryPool memory_pool=None):
        cdef:
            shared_ptr[CScanContext] context

        context = make_shared[CScanContext]()
        context.get().pool = maybe_unbox_memory_pool(memory_pool)
        context.get().use_threads = use_threads

        return Scanner.wrap(make_shared[CScanner](fragment.unwrap(), context))

    cdef inline shared_ptr[CScanner] unwrap(self):
        return self.wrapped

    def scan(self):
        """Returns a stream of ScanTasks

        The caller is responsible to dispatch/schedule said tasks. Tasks should
        be safe to run in a concurrent fashion and outlive the iterator.

        Returns
        -------
        scan_tasks : iterator of ScanTask
        """
        cdef:
            CScanTaskIterator iterator
            shared_ptr[CScanTask] task

        iterator = move(GetResultValue(move(self.scanner.Scan())))

        while True:
            task = GetResultValue(iterator.Next())
            if task.get() == nullptr:
                raise StopIteration()
            else:
                yield ScanTask.wrap(task)

    def to_table(self):
        """Convert a Scanner into a Table.

        Use this convenience utility with care. This will serially materialize
        the Scan result in memory before creating the Table.

        Returns
        -------
        table : Table
        """
        cdef CResult[shared_ptr[CTable]] result

        with nogil:
            result = self.scanner.ToTable()

        return pyarrow_wrap_table(GetResultValue(result))

    def get_fragments(self):
        """Returns an iterator over the fragments in this scan.
        """
        cdef:
            CFragmentIterator iterator
            shared_ptr[CFragment] fragment

        iterator = self.scanner.GetFragments()

        while True:
            fragment = GetResultValue(iterator.Next())
            if fragment.get() == nullptr:
                raise StopIteration()
            else:
                yield Fragment.wrap(fragment)


def _binop(fn, left, right):
    # cython doesn't support reverse operands like __radd__ just passes the
    # arguments in the same order as the binary operator called
    if isinstance(left, Expression) and isinstance(right, Expression):
        pass
    elif isinstance(left, Expression):
        right = ScalarExpression(right)
    elif isinstance(right, Expression):
        left = ScalarExpression(left)
    else:
        raise TypeError('Neither left nor right arguments are Expressions')

    return fn(left, right)


cdef class Expression:

    cdef:
        shared_ptr[CExpression] wrapped
        CExpression* expr

    def __init__(self):
        _forbid_instantiation(self.__class__)

    cdef void init(self, const shared_ptr[CExpression]& sp):
        self.wrapped = sp
        self.expr = sp.get()

    @staticmethod
    cdef wrap(const shared_ptr[CExpression]& sp):
        cdef Expression self

        typ = sp.get().type()
        if typ == CExpressionType_FIELD:
            self = FieldExpression.__new__(FieldExpression)
        elif typ == CExpressionType_SCALAR:
            self = ScalarExpression.__new__(ScalarExpression)
        elif typ == CExpressionType_NOT:
            self = NotExpression.__new__(NotExpression)
        elif typ == CExpressionType_CAST:
            self = CastExpression.__new__(CastExpression)
        elif typ == CExpressionType_AND:
            self = AndExpression.__new__(AndExpression)
        elif typ == CExpressionType_OR:
            self = OrExpression.__new__(OrExpression)
        elif typ == CExpressionType_COMPARISON:
            self = ComparisonExpression.__new__(ComparisonExpression)
        elif typ == CExpressionType_IS_VALID:
            self = IsValidExpression.__new__(IsValidExpression)
        elif typ == CExpressionType_IN:
            self = InExpression.__new__(InExpression)
        else:
            raise TypeError(typ)

        self.init(sp)
        return self

    cdef inline shared_ptr[CExpression] unwrap(self):
        return self.wrapped

    def equals(self, Expression other):
        return self.expr.Equals(other.unwrap())

    def __str__(self):
        return frombytes(self.expr.ToString())

    def __repr__(self):
        return "<pyarrow.dataset.{0} {1}>".format(
            self.__class__.__name__, str(self)
        )

    def validate(self, Schema schema not None):
        """Validate this expression for execution against a schema.

        This will check that all reference fields are present (fields not in
        the schema will be replaced with null) and all subexpressions are
        executable. Returns the type to which this expression will evaluate.

        Parameters
        ----------
        schema : Schema
            Schema to execute the expression on.

        Returns
        -------
        type : DataType
        """
        cdef:
            shared_ptr[CSchema] sp_schema
            CResult[shared_ptr[CDataType]] result
        sp_schema = pyarrow_unwrap_schema(schema)
        result = self.expr.Validate(deref(sp_schema))
        return pyarrow_wrap_data_type(GetResultValue(result))

    def assume(self, Expression given):
        """Simplify to an equivalent Expression given assumed constraints."""
        return Expression.wrap(self.expr.Assume(given.unwrap()))

    def __invert__(self):
        return NotExpression(self)

    def __richcmp__(self, other, int op):
        operator_mapping = {
            Py_EQ: CompareOperator.Equal,
            Py_NE: CompareOperator.NotEqual,
            Py_GT: CompareOperator.Greater,
            Py_GE: CompareOperator.GreaterEqual,
            Py_LT: CompareOperator.Less,
            Py_LE: CompareOperator.LessEqual
        }

        if not isinstance(other, Expression):
            other = ScalarExpression(other)

        return ComparisonExpression(operator_mapping[op], self, other)

    def __and__(self, other):
        return _binop(AndExpression, self, other)

    def __or__(self, other):
        return _binop(OrExpression, self, other)

    def is_valid(self):
        """Checks whether the expression is not-null (valid)"""
        return IsValidExpression(self)

    def cast(self, type, bint safe=True):
        """Explicitly change the expression's data type"""
        return CastExpression(self, to=ensure_type(type), safe=safe)

    def isin(self, values):
        """Checks whether the expression is contained in values"""
        return InExpression(self, pa.array(values))


cdef class UnaryExpression(Expression):

    cdef CUnaryExpression* unary

    cdef void init(self, const shared_ptr[CExpression]& sp):
        Expression.init(self, sp)
        self.unary = <CUnaryExpression*> sp.get()

    @property
    def operand(self):
        return Expression.wrap(self.unary.operand())


cdef class BinaryExpression(Expression):

    cdef CBinaryExpression* binary

    cdef void init(self, const shared_ptr[CExpression]& sp):
        Expression.init(self, sp)
        self.binary = <CBinaryExpression*> sp.get()

    @property
    def left_operand(self):
        return Expression.wrap(self.binary.left_operand())

    @property
    def right_operand(self):
        return Expression.wrap(self.binary.right_operand())


cdef class ScalarExpression(Expression):

    cdef CScalarExpression* scalar

    def __init__(self, value):
        cdef:
            shared_ptr[CScalar] scalar
            shared_ptr[CScalarExpression] expr

        if value is None:
            scalar.reset(new CNullScalar())
        elif isinstance(value, bool):
            scalar = MakeScalar(<c_bool>value)
        elif isinstance(value, float):
            scalar = MakeScalar(<double>value)
        elif isinstance(value, int):
            scalar = MakeScalar(<int64_t>value)
        elif isinstance(value, (bytes, str)):
            scalar = MakeStringScalar(tobytes(value))
        else:
            raise TypeError('Not yet supported scalar value: {}'.format(value))

        expr.reset(new CScalarExpression(scalar))
        self.init(<shared_ptr[CExpression]> expr)

    cdef void init(self, const shared_ptr[CExpression]& sp):
        Expression.init(self, sp)
        self.scalar = <CScalarExpression*> sp.get()

    @property
    def value(self):
        cdef ScalarValue scalar = pyarrow_wrap_scalar(self.scalar.value())
        return scalar.as_py()

    def __reduce__(self):
        return ScalarExpression, (self.value,)


cdef class FieldExpression(Expression):

    cdef CFieldExpression* scalar

    def __init__(self, name):
        cdef:
            c_string field_name = tobytes(name)
            shared_ptr[CExpression] expr
        expr.reset(new CFieldExpression(field_name))
        self.init(expr)

    cdef void init(self, const shared_ptr[CExpression]& sp):
        Expression.init(self, sp)
        self.scalar = <CFieldExpression*> sp.get()

    @property
    def name(self):
        return frombytes(self.scalar.name())

    def __reduce__(self):
        return FieldExpression, (self.name,)


cpdef enum CompareOperator:
    Equal = <int8_t> CCompareOperator_EQUAL
    NotEqual = <int8_t> CCompareOperator_NOT_EQUAL
    Greater = <int8_t> CCompareOperator_GREATER
    GreaterEqual = <int8_t> CCompareOperator_GREATER_EQUAL
    Less = <int8_t> CCompareOperator_LESS
    LessEqual = <int8_t> CCompareOperator_LESS_EQUAL


cdef class ComparisonExpression(BinaryExpression):

    cdef CComparisonExpression* comparison

    def __init__(self, CompareOperator op, Expression left not None,
                 Expression right not None):
        cdef shared_ptr[CComparisonExpression] expr
        expr.reset(
            new CComparisonExpression(
                <CCompareOperator>op,
                left.unwrap(),
                right.unwrap()
            )
        )
        self.init(<shared_ptr[CExpression]> expr)

    cdef void init(self, const shared_ptr[CExpression]& sp):
        BinaryExpression.init(self, sp)
        self.comparison = <CComparisonExpression*> sp.get()

    @property
    def op(self):
        return <CompareOperator> self.comparison.op()

    def __reduce__(self):
        return ComparisonExpression, (
            self.op, self.left_operand, self.right_operand
        )


cdef class IsValidExpression(UnaryExpression):

    def __init__(self, Expression operand not None):
        cdef shared_ptr[CIsValidExpression] expr
        expr = make_shared[CIsValidExpression](operand.unwrap())
        self.init(<shared_ptr[CExpression]> expr)

    def __reduce__(self):
        return IsValidExpression, (self.operand,)


cdef class CastExpression(UnaryExpression):

    cdef CCastExpression *cast

    def __init__(self, Expression operand not None, DataType to not None,
                 bint safe=True):
        cdef:
            CastOptions options
            shared_ptr[CExpression] expr
        options = CastOptions.safe() if safe else CastOptions.unsafe()
        expr.reset(
            new CCastExpression(
                operand.unwrap(),
                pyarrow_unwrap_data_type(to),
                options.unwrap()
            )
        )
        self.init(expr)

    cdef void init(self, const shared_ptr[CExpression]& sp):
        UnaryExpression.init(self, sp)
        self.cast = <CCastExpression*> sp.get()

    @property
    def to(self):
        """
        Target DataType or Expression of the cast operation.

        Returns
        -------
        DataType or Expression
        """
        cdef shared_ptr[CDataType] typ = self.cast.to_type()

        if typ.get() != nullptr:
            return pyarrow_wrap_data_type(typ)
        else:
            raise TypeError(
                'Cannot determine the target type of the cast expression'
            )

    @property
    def safe(self):
        """
        Whether to check for overflows or other unsafe conversions.

        Returns
        -------
        bool
        """
        cdef CastOptions options = CastOptions.wrap(self.cast.options())
        return options.is_safe()

    def __reduce__(self):
        return CastExpression, (self.operand, self.to, self.safe)


cdef class InExpression(UnaryExpression):

    cdef:
        CInExpression* inexpr

    def __init__(self, Expression operand not None, Array set_ not None):
        cdef shared_ptr[CExpression] expr
        expr.reset(
            new CInExpression(operand.unwrap(), pyarrow_unwrap_array(set_))
        )
        self.init(expr)

    cdef void init(self, const shared_ptr[CExpression]& sp):
        UnaryExpression.init(self, sp)
        self.inexpr = <CInExpression*> sp.get()

    @property
    def set_(self):
        return pyarrow_wrap_array(self.inexpr.set())

    def __reduce__(self):
        return InExpression, (self.operand, self.set_)


cdef class NotExpression(UnaryExpression):

    def __init__(self, Expression operand not None):
        cdef shared_ptr[CNotExpression] expr
        expr = CMakeNotExpression(operand.unwrap())
        self.init(<shared_ptr[CExpression]> expr)

    def __reduce__(self):
        return NotExpression, (self.operand,)


cdef class AndExpression(BinaryExpression):

    def __init__(self, Expression left not None, Expression right not None):
        cdef shared_ptr[CAndExpression] expr
        expr.reset(new CAndExpression(left.unwrap(), right.unwrap()))
        self.init(<shared_ptr[CExpression]> expr)

    def __reduce__(self):
        return AndExpression, (self.left_operand, self.right_operand)


cdef class OrExpression(BinaryExpression):

    def __init__(self, Expression left not None, Expression right not None):
        cdef shared_ptr[COrExpression] expr
        expr.reset(new COrExpression(left.unwrap(), right.unwrap()))
        self.init(<shared_ptr[CExpression]> expr)

    def __reduce__(self):
        return OrExpression, (self.left_operand, self.right_operand)
