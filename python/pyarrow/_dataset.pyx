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

from __future__ import absolute_import

import six
from cpython.object cimport Py_LT, Py_EQ, Py_GT, Py_LE, Py_NE, Py_GE
from cython.operator cimport dereference as deref

import pyarrow as pa
from pyarrow.lib cimport *
from pyarrow.includes.libarrow_dataset cimport *
from pyarrow.compat import frombytes, tobytes
from pyarrow._fs cimport FileSystem, FileStats, FileSelector


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
    cdef wrap(shared_ptr[CFileFormat]& sp):
        cdef FileFormat self

        typ = frombytes(sp.get().type_name())
        if typ == 'parquet':
            self = ParquetFileFormat.__new__(ParquetFileFormat)
        else:
            raise TypeError(typ)

        self.init(sp)
        return self

    cdef inline shared_ptr[CFileFormat] unwrap(self):
        return self.wrapped


cdef class ParquetFileFormat(FileFormat):

    def __init__(self):
        self.init(shared_ptr[CFileFormat](new CParquetFileFormat()))


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
        if typ == 'default':
            self = DefaultPartitioning.__new__(DefaultPartitioning)
        elif typ == 'schema':
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
        return Expr.wrap(GetResultValue(result))

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


cdef class DefaultPartitioning(Partitioning):

    cdef:
        CDefaultPartitioning* default_partitioning

    def __init__(self):
        cdef shared_ptr[CDefaultPartitioning] partitioning
        partitioning = make_shared[CDefaultPartitioning]()
        self.init(<shared_ptr[CPartitioning]> partitioning)

    cdef init(self, const shared_ptr[CPartitioning]& sp):
        Partitioning.init(self, sp)
        self.default_partitioning = <CDefaultPartitioning*> sp.get()


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


cdef class FileSystemFactoryOptions:
    """
    Options for FileSystemFactoryOptions.

    Parameters
    ----------
    partition_base_dir : str, optional
        For the purposes of applying the partitioning, paths will be
        stripped of the partition_base_dir. Files not matching the
        partition_base_dir prefix will be skipped for partitioning discovery.
        The ignored files will still be part of the Source, but will not
        have partition information.
    exclude_invalid_files : bool, optional (default True)
        If True, invalid files will be excluded (file format specific check).
        This will incur IO for each files in a serial and single threaded
        fashion. Disabling this feature will skip the IO, but unsupported
        files may be present in the Source (resulting in an error at scan
        time).
    ignore_prefixes : list, optional
        Files matching one of those prefixes will be ignored by the
        discovery process. This is matched to the basename of a path.
        By default this is ['.', '_'].
    """

    cdef:
        CFileSystemFactoryOptions options

    __slots__ = ()  # avoid mistakingly creating attributes

    def __init__(self, partition_base_dir=None, exclude_invalid_files=None,
                 list ignore_prefixes=None):
        if partition_base_dir is not None:
            self.partition_base_dir = partition_base_dir
        if exclude_invalid_files is not None:
            self.exclude_invalid_files = exclude_invalid_files
        if ignore_prefixes is not None:
            self.ignore_prefixes = ignore_prefixes

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
    def ignore_prefixes(self):
        """
        List of prefixes. Files matching one of those prefixes will be
        ignored by the discovery process.
        """
        return [frombytes(p) for p in self.options.ignore_prefixes]

    @ignore_prefixes.setter
    def ignore_prefixes(self, values):
        self.options.ignore_prefixes = [tobytes(v) for v in values]


cdef class SourceFactory:
    """
    SourceFactory is used to create a Source, inspect the Schema
    of the fragments contained in it, and declare a partitioning.
    """

    cdef:
        shared_ptr[CSourceFactory] wrapped
        CSourceFactory* factory

    def __init__(self):
        _forbid_instantiation(self.__class__)

    cdef init(self, shared_ptr[CSourceFactory]& sp):
        self.wrapped = sp
        self.factory = sp.get()

    @staticmethod
    cdef wrap(shared_ptr[CSourceFactory]& sp):
        cdef SourceFactory self = \
            SourceFactory.__new__(SourceFactory)
        self.init(sp)
        return self

    cdef inline shared_ptr[CSourceFactory] unwrap(self) nogil:
        return self.wrapped

    @property
    def root_partition(self):
        cdef shared_ptr[CExpression] expr = self.factory.root_partition()
        if expr.get() == nullptr:
            return None
        else:
            return Expr.wrap(expr)

    @root_partition.setter
    def root_partition(self, Expr expr):
        check_status(self.factory.SetRootPartition(expr.unwrap()))

    def inspect_schemas(self):
        cdef CResult[vector[shared_ptr[CSchema]]] result
        with nogil:
            result = self.factory.InspectSchemas()

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
        cdef CResult[shared_ptr[CSchema]] result
        with nogil:
            result = self.factory.Inspect()
        return pyarrow_wrap_schema(GetResultValue(result))

    def finish(self, Schema schema=None):
        """
        Create a Source using the inspected schema or an explicit schema
        (if given).

        Parameters
        ----------
        schema: Schema, default None
            The schema to conform the source to.  If None, the inspected
            schema is used.

        Returns
        -------
        Source
        """
        cdef:
            shared_ptr[CSchema] sp_schema
            CResult[shared_ptr[CSource]] result
        if schema is not None:
            sp_schema = pyarrow_unwrap_schema(schema)
            with nogil:
                result = self.factory.Finish(sp_schema)
        else:
            with nogil:
                result = self.factory.Finish()
        return Source.wrap(GetResultValue(result))


cdef class FileSystemSourceFactory(SourceFactory):
    """
    Create a SourceFactory from a list of paths with schema inspection.

    Parameters
    ----------
    filesystem : pyarrow.fs.FileSystem
    paths_or_selector: pyarrow.fs.Selector or list of path-likes
        Either a Selector object or a list of path-like objects.
    format : FileFormat
    options : FileSystemFactoryOptions, optional
    """

    cdef:
        CFileSystemSourceFactory* filesystem_factory

    def __init__(self, FileSystem filesystem not None, paths_or_selector,
                 FileFormat format not None,
                 FileSystemFactoryOptions options=None):
        cdef:
            vector[c_string] paths
            CFileSelector selector
            CResult[shared_ptr[CSourceFactory]] result
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
                result = CFileSystemSourceFactory.MakeFromSelector(
                    c_filesystem,
                    selector,
                    c_format,
                    c_options
                )
        elif isinstance(paths_or_selector, (list, tuple)):
            paths = [tobytes(s) for s in paths_or_selector]
            with nogil:
                result = CFileSystemSourceFactory.MakeFromPaths(
                    c_filesystem,
                    paths,
                    c_format,
                    c_options
                )
        else:
            raise TypeError('Must pass either paths or a FileSelector')

        self.init(GetResultValue(result))

    cdef init(self, shared_ptr[CSourceFactory]& sp):
        SourceFactory.init(self, sp)
        self.filesystem_factory = <CFileSystemSourceFactory*> sp.get()


cdef class Source:
    """Basic component of a Dataset which yields zero or more fragments.  """

    cdef:
        shared_ptr[CSource] wrapped
        CSource* source

    def __init__(self):
        _forbid_instantiation(self.__class__)

    cdef void init(self, const shared_ptr[CSource]& sp):
        self.wrapped = sp
        self.source = sp.get()

    @staticmethod
    cdef wrap(shared_ptr[CSource]& sp):
        cdef Source self

        typ = frombytes(sp.get().type_name())
        if typ == 'tree':
            self = TreeSource.__new__(TreeSource)
        elif typ == 'filesystem':
            self = FileSystemSource.__new__(FileSystemSource)
        else:
            raise TypeError(typ)

        self.init(sp)
        return self

    cdef shared_ptr[CSource] unwrap(self) nogil:
        return self.wrapped

    @property
    def schema(self):
        """
        Schema of all RecordBatches contained in this DataSource.
        """
        return pyarrow_wrap_schema(self.source.schema())

    @property
    def partition_expression(self):
        """
        An Expr which evaluates to true for all data viewed by this
        Source.
        """
        cdef shared_ptr[CExpression] expr
        expr = self.source.partition_expression()
        if expr.get() == nullptr:
            return None
        else:
            return Expr.wrap(expr)


cdef class TreeSource(Source):
    """A Source created from other source objects"""

    cdef:
        CTreeSource* tree_source

    def __init__(self, schema, sources):
        cdef:
            Source child
            CSourceVector children
            shared_ptr[CTreeSource] tree_source

        for child in sources:
            children.push_back(child.wrapped)

        tree_source = make_shared[CTreeSource](
            pyarrow_unwrap_schema(schema), children)
        self.init(<shared_ptr[CSource]> tree_source)

    cdef void init(self, const shared_ptr[CSource]& sp):
        Source.init(self, sp)
        self.tree_source = <CTreeSource*> sp.get()


cdef class FileSystemSource(Source):
    """A Source created from a set of files on a particular filesystem"""

    cdef:
        CFileSystemSource* filesystem_source

    def __init__(self, Schema schema not None,
                 Expr source_partition,
                 FileFormat file_format not None,
                 FileSystem filesystem not None,
                 paths_or_selector, partitions):
        """Create a FileSystemSource

        Parameters
        ----------
        schema : Schema
            Schema for resulting Source
        source_partition : Expr
        file_format : FileFormat
        filesystem : FileSystem
            FileSystem which will be explored to discover data files.
        paths_or_selector : Union[FileSelector, List[FileStats]]
            The file stats object can be queried by the
            filesystem.get_target_stats method.
        partitions : List[Expr]
        """
        cdef:
            shared_ptr[CExpression] c_source_partition
            FileStats stats
            Expr expr
            vector[CFileStats] c_file_stats
            vector[shared_ptr[CExpression]] c_partitions
            CResult[shared_ptr[CSource]] result

        for stats in filesystem.get_target_stats(paths_or_selector):
            c_file_stats.push_back(stats.unwrap())

        for expr in partitions:
            c_partitions.push_back(expr.unwrap())

        if c_file_stats.size() != c_partitions.size():
            raise ValueError(
                'The number of files resulting from paths_or_selector must be '
                'equal to the number of partitions.'
            )

        if source_partition is None:
            source_partition = ScalarExpr(True)
        c_source_partition = source_partition.unwrap()

        result = CFileSystemSource.Make(
            pyarrow_unwrap_schema(schema),
            c_source_partition,
            file_format.unwrap(),
            filesystem.unwrap(),
            c_file_stats,
            c_partitions
        )
        self.init(GetResultValue(result))

    cdef void init(self, const shared_ptr[CSource]& sp):
        Source.init(self, sp)
        self.filesystem_source = <CFileSystemSource*> sp.get()


cdef class DatasetFactory:
    """
    Provides a way to inspect/discover a Dataset's expected schema before
    materializing the Dataset and underlying Sources.

    Parameters
    ----------
    factories : list of SourceFactory
    """

    cdef:
        shared_ptr[CDatasetFactory] wrapped
        CDatasetFactory* factory

    def __init__(self, list factories):
        cdef:
            SourceFactory factory
            vector[shared_ptr[CSourceFactory]] c_factories
        for factory in factories:
            c_factories.push_back(factory.unwrap())
        self.init(GetResultValue(CDatasetFactory.Make(c_factories)))

    cdef void init(self, const shared_ptr[CDatasetFactory]& sp):
        self.wrapped = sp
        self.factory = sp.get()

    @staticmethod
    cdef wrap(shared_ptr[CDatasetFactory]& sp):
        cdef DatasetFactory self = DatasetFactory.__new__(DatasetFactory)
        self.init(sp)
        return self

    cdef inline shared_ptr[CDatasetFactory] unwrap(self) nogil:
        return self.wrapped

    @property
    def sources(self):
        cdef:
            shared_ptr[CSourceFactory] source
            vector[shared_ptr[CSourceFactory]] sources
        sources = self.factory.factories()
        return [SourceFactory.wrap(source) for source in sources]

    def inspect_schemas(self):
        cdef vector[shared_ptr[CSchema]] schemas
        schemas = GetResultValue(self.factory.InspectSchemas())
        return [pyarrow_wrap_schema(schema) for schema in schemas]

    def inspect(self):
        return pyarrow_wrap_schema(GetResultValue(self.factory.Inspect()))

    def finish(self, Schema schema=None):
        cdef CResult[shared_ptr[CDataset]] result

        if schema is None:
            result = self.factory.Finish()
        else:
            result = self.factory.FinishWithSchema(
                pyarrow_unwrap_schema(schema)
            )

        return Dataset.wrap(GetResultValue(result))


cdef class Dataset:
    """
    Collection of data fragments coming from possibly multiple sources.

    Arrow Datasets allow you to query against data that has been split across
    multiple files. This sharding of data may indicate partitioning, which
    can accelerate queries that only touch some partitions (files).
    """

    cdef:
        shared_ptr[CDataset] wrapped
        CDataset* dataset

    def __init__(self, sources, Schema schema not None):
        """Create a dataset

        A schema must be passed because most of the sources' schema is
        unknown before executing possibly expensive scanning operation, but
        projecting, filtering, predicate pushdown requires a well defined
        schema to work on.

        Parameters
        ----------
        sources : list of Source
            One or more input sources
        schema : Schema
            A known schema to conform to.
        """
        cdef:
            Source source
            CSourceVector c_sources
            CResult[shared_ptr[CDataset]] result

        for source in sources:
            c_sources.push_back(source.unwrap())

        result = CDataset.Make(c_sources, pyarrow_unwrap_schema(schema))
        self.init(GetResultValue(result))

    cdef void init(self, const shared_ptr[CDataset]& sp):
        self.wrapped = sp
        self.dataset = sp.get()

    @staticmethod
    cdef wrap(shared_ptr[CDataset]& sp):
        cdef Dataset self = Dataset.__new__(Dataset)
        self.init(sp)
        return self

    cdef inline shared_ptr[CDataset] unwrap(self) nogil:
        return self.wrapped

    def new_scan(self, MemoryPool memory_pool=None):
        """
        Begin to build a new Scan operation against this Dataset.

        Parameters
        ----------
        memory_pool : MemoryPool, default None
            For memory allocations, if required. If not specified, uses the
            default pool.

        Returns
        -------
        ScannerBuilder
        """
        cdef:
            shared_ptr[CScanContext] context = make_shared[CScanContext]()
            CResult[shared_ptr[CScannerBuilder]] result
        context.get().pool = maybe_unbox_memory_pool(memory_pool)
        result = self.dataset.NewScanWithContext(context)
        return ScannerBuilder.wrap(GetResultValue(result))

    @property
    def sources(self):
        """List of the sources"""
        cdef vector[shared_ptr[CSource]] sources = self.dataset.sources()
        return [Source.wrap(source) for source in sources]

    @property
    def schema(self):
        """The common schema of the full Dataset"""
        return pyarrow_wrap_schema(self.dataset.schema())


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
        cdef InMemoryScanTask self = InMemoryScanTask.__new__(InMemoryScanTask)
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

        iterator = move(GetResultValue(move(self.task.Execute())))

        while True:
            record_batch = GetResultValue(iterator.Next())
            if record_batch.get() == nullptr:
                raise StopIteration()
            else:
                yield pyarrow_wrap_batch(record_batch)


cdef class InMemoryScanTask(ScanTask):
    """A trivial ScanTask that yields the RecordBatch of an array."""

    cdef:
        CInMemoryScanTask* in_memory_task

    cdef init(self, shared_ptr[CScanTask]& sp):
        ScanTask.init(self, sp)
        self.in_memory_task = <CInMemoryScanTask*> sp.get()


cdef class ScannerBuilder:
    """Factory class to construct a Scanner.

    It is used to pass information, notably a potential filter expression and a
    subset of columns to materialize.

    Parameters
    ----------
    dataset : Dataset
        The dataset to scan.
    memory_pool : MemoryPool, default None
        For memory allocations, if required. If not specified, uses the
        default pool.
    """

    cdef:
        shared_ptr[CScannerBuilder] wrapped
        CScannerBuilder* builder

    def __init__(self, Dataset dataset not None, MemoryPool memory_pool=None):
        cdef:
            shared_ptr[CScannerBuilder] builder
            shared_ptr[CScanContext] context = make_shared[CScanContext]()
        context.get().pool = maybe_unbox_memory_pool(memory_pool)
        builder = make_shared[CScannerBuilder](dataset.unwrap(), context)
        self.init(builder)

    cdef void init(self, shared_ptr[CScannerBuilder]& sp):
        self.wrapped = sp
        self.builder = sp.get()

    @staticmethod
    cdef wrap(shared_ptr[CScannerBuilder]& sp):
        cdef ScannerBuilder self = ScannerBuilder.__new__(ScannerBuilder)
        self.init(sp)
        return self

    cdef inline shared_ptr[CScannerBuilder] unwrap(self) nogil:
        return self.wrapped

    def project(self, columns):
        """Set the subset of columns to materialize.

        This subset will be passed down to Sources and corresponding
        data fragments. The goal is to avoid loading, copying, and
        deserializing columns that will not be required further down the
        compute chain.

        It alters the object in place and returns the object itself enabling
        method chaining. Raises exception if any of the referenced column names
        does not exists in the dataset's Schema.

        Parameters
        ----------
        columns : list of str
            List of columns to project. Order and duplicates will be preserved.

        Returns
        -------
        self : ScannerBuilder
        """
        cdef vector[c_string] cols = [tobytes(c) for c in columns]
        check_status(self.builder.Project(cols))

        return self

    def finish(self):
        """Return the constructed now-immutable Scanner object

        Returns
        -------
        Scanner
        """
        return Scanner.wrap(GetResultValue(self.builder.Finish()))

    def filter(self, Expr filter_expr not None):
        """Set the filter Expr to return only rows matching the filter.

        The predicate will be passed down to Sources and corresponding
        data fragments to exploit predicate pushdown if possible using
        partition information or internal metadata, e.g. Parquet statistics.
        Otherwise filters the loaded RecordBatches before yielding them.

        It alters the object in place and returns the object itself enabling
        method chaining. Raises exception if any of the referenced column names
        does not exists in the dataset's Schema.

        Parameters
        ----------
        filter_expr : Expr
            Boolean Expr to filter rows with.

        Returns
        -------
        self : ScannerBuilder
        """
        cdef:
            shared_ptr[CExpression] c_casting_filter_expression

        c_casting_filter_expression = GetResultValue(
            CInsertImplicitCasts(
                deref(filter_expression.unwrap().get()),
                deref(self.builder.schema().get())
            )
        )
        check_status(self.builder.Filter(c_casting_filter_expression))
        return self

    def use_threads(self, bint value):
        """Set whether the Scanner should make use of the thread pool.

        It alters the object in place and returns with the object itself
        enabling method chaining.

        Parameters
        ----------
        value : boolean

        Returns
        -------
        self : ScannerBuilder
        """
        check_status(self.builder.UseThreads(value))
        return self

    @property
    def schema(self):
        return pyarrow_wrap_schema(self.builder.schema())


cdef class Scanner:
    """A materialized scan operation with context and options bound.

    Create this using the ScannerBuilder factory class.

    A scanner is the class that glues the scan tasks, data fragments and data
    sources together.
    """

    cdef:
        shared_ptr[CScanner] wrapped
        CScanner* scanner

    def __init__(self):
        raise TypeError('Scanner cannot be initialized directly, use '
                        'ScannerBuilder instead')

    cdef void init(self, shared_ptr[CScanner]& sp):
        self.wrapped = sp
        self.scanner = sp.get()

    @staticmethod
    cdef wrap(shared_ptr[CScanner]& sp):
        cdef Scanner self = Scanner.__new__(Scanner)
        self.init(sp)
        return self

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


def _binop(fn, left, right):
    # cython doesn't support reverse operands like __radd__ just passes the
    # arguments in the same order as the binary operator called

    if isinstance(left, Expr) and isinstance(right, Expr):
        pass
    elif isinstance(left, Expr):
        try:
            right = ScalarExpr(right)
        except TypeError:
            return NotImplemented

    elif isinstance(right, Expr):
        try:
            left = ScalarExpr(left)
        except TypeError:
            return NotImplemented
    else:
        raise TypeError('Neither left nor right arguments are Expressions')

    return fn(left, right)


cdef class Expr:

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
        cdef Expr self

        typ = sp.get().type()
        if typ == CExpressionType_FIELD:
            self = FieldExpr.__new__(FieldExpr)
        elif typ == CExpressionType_SCALAR:
            self = ScalarExpr.__new__(ScalarExpr)
        elif typ == CExpressionType_NOT:
            self = NotExpr.__new__(NotExpr)
        elif typ == CExpressionType_CAST:
            self = CastExpr.__new__(CastExpr)
        elif typ == CExpressionType_AND:
            self = AndExpr.__new__(AndExpr)
        elif typ == CExpressionType_OR:
            self = OrExpr.__new__(OrExpr)
        elif typ == CExpressionType_COMPARISON:
            self = ComparisonExpr.__new__(ComparisonExpr)
        elif typ == CExpressionType_IS_VALID:
            self = IsValidExpr.__new__(IsValidExpr)
        elif typ == CExpressionType_IN:
            self = InExpr.__new__(InExpr)
        else:
            raise TypeError(typ)

        self.init(sp)
        return self

    cdef inline shared_ptr[CExpression] unwrap(self):
        return self.wrapped

    def equals(self, Expr other):
        return self.expr.Equals(other.unwrap())

    def __str__(self):
        return frombytes(self.expr.ToString())

    def validate(self, Schema schema not None):
        cdef:
            shared_ptr[CSchema] sp_schema
            CResult[shared_ptr[CDataType]] result
        sp_schema = pyarrow_unwrap_schema(schema)
        result = self.expr.Validate(deref(sp_schema))
        return pyarrow_wrap_data_type(GetResultValue(result))

    def assume(self, Expr given):
        return Expr.wrap(self.expr.Assume(given.unwrap()))

    def __invert__(self):
        return NotExpr(self)

    def __richcmp__(self, other, int op):
        cdef Compare

        operator_mapping = {
            Py_EQ: CompareOperator.Equal,
            Py_NE: CompareOperator.NotEqual,
            Py_GT: CompareOperator.Greater,
            Py_GE: CompareOperator.GreaterEqual,
            Py_LT: CompareOperator.Less,
            Py_LE: CompareOperator.LessEqual
        }

        if not isinstance(other, Expr):
            try:
                other = ScalarExpr(other)
            except TypeError:
                return NotImplemented

        return ComparisonExpr(operator_mapping[op], self, other)

    def __and__(self, other):
        return _binop(AndExpr, self, other)

    def __or__(self, other):
        return _binop(OrExpr, self, other)

    def is_valid(self):
        return IsValidExpr(self)

    def cast(self, type, bint safe=True):
        return CastExpr(self, to=ensure_type(type), safe=safe)

    def isin(self, values):
        return InExpr(self, pa.array(values))


cdef class UnaryExpr(Expr):

    cdef CUnaryExpression* unary

    cdef void init(self, const shared_ptr[CExpression]& sp):
        Expr.init(self, sp)
        self.unary = <CUnaryExpression*> sp.get()


cdef class BinaryExpr(Expr):

    cdef CBinaryExpression* binary

    cdef void init(self, const shared_ptr[CExpression]& sp):
        Expr.init(self, sp)
        self.binary = <CBinaryExpression*> sp.get()

    @property
    def left_operand(self):
        return Expr.wrap(self.binary.left_operand())

    @property
    def right_operand(self):
        return Expr.wrap(self.binary.right_operand())


cdef class ScalarExpr(Expr):

    cdef CScalarExpression* scalar

    def __init__(self, value):
        cdef:
            shared_ptr[CScalar] scalar
            shared_ptr[CScalarExpression] expr

        if isinstance(value, bool):
            scalar = MakeScalar(<c_bool>value)
        elif isinstance(value, float):
            scalar = MakeScalar(<double>value)
        elif isinstance(value, int):
            scalar = MakeScalar(<int64_t>value)
        elif isinstance(value, six.string_types):
            scalar = MakeStringScalar(tobytes(value))
        else:
            raise TypeError('Not yet supported scalar value: {}'.format(value))

        expr.reset(new CScalarExpression(scalar))
        self.init(<shared_ptr[CExpression]> expr)

    cdef void init(self, const shared_ptr[CExpression]& sp):
        Expr.init(self, sp)
        self.scalar = <CScalarExpression*> sp.get()


cdef class FieldExpr(Expr):

    cdef CFieldExpression* scalar

    def __init__(self, name):
        cdef:
            c_string field_name = tobytes(name)
            shared_ptr[CExpression] expr
        expr.reset(new CFieldExpression(field_name))
        self.init(expr)

    cdef void init(self, const shared_ptr[CExpression]& sp):
        Expr.init(self, sp)
        self.scalar = <CFieldExpression*> sp.get()

    def name(self):
        return frombytes(self.scalar.name())


cpdef enum CompareOperator:
    Equal = <int8_t> CCompareOperator_EQUAL
    NotEqual = <int8_t> CCompareOperator_NOT_EQUAL
    Greater = <int8_t> CCompareOperator_GREATER
    GreaterEqual = <int8_t> CCompareOperator_GREATER_EQUAL
    Less = <int8_t> CCompareOperator_LESS
    LessEqual = <int8_t> CCompareOperator_LESS_EQUAL


cdef class ComparisonExpr(BinaryExpr):

    cdef CComparisonExpression* comparison

    def __init__(self, CompareOperator op, Expr left not None,
                 Expr right not None):
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
        BinaryExpr.init(self, sp)
        self.comparison = <CComparisonExpression*> sp.get()

    def op(self):
        return <CompareOperator> self.comparison.op()


cdef class IsValidExpr(UnaryExpr):

    def __init__(self, Expr operand not None):
        cdef shared_ptr[CIsValidExpression] expr
        expr = make_shared[CIsValidExpression](operand.unwrap())
        self.init(<shared_ptr[CExpression]> expr)


cdef class CastExpr(UnaryExpr):

    def __init__(self, Expr operand not None, DataType to not None,
                 bint safe=True):
        # TODO(kszucs): safe is consistently used across pyarrow, but on long
        #               term we should expose the CastOptions object
        cdef:
            CastOptions options
            shared_ptr[CExpression] Expr
        options = CastOptions.safe() if safe else CastOptions.unsafe()
        Expr.reset(new CCastExpression(
            operand.unwrap(),
            pyarrow_unwrap_data_type(to),
            options.unwrap()
        ))
        self.init(Expr)


cdef class InExpr(UnaryExpr):

    def __init__(self, Expr operand not None, Array haystack not None):
        cdef shared_ptr[CExpression] Expr
        Expr.reset(
            new CInExpression(operand.unwrap(), pyarrow_unwrap_array(haystack))
        )
        self.init(Expr)


cdef class NotExpr(UnaryExpr):

    def __init__(self, Expr operand not None):
        cdef shared_ptr[CNotExpression] Expr
        expr = CMakeNotExpression(operand.unwrap())
        self.init(<shared_ptr[CExpression]> expr)


cdef class AndExpr(BinaryExpr):

    def __init__(self, Expr left not None, Expr right not None,
                 *additional_operands):
        cdef:
            Expr operand
            vector[shared_ptr[CExpression]] exprs
        exprs.push_back(left.unwrap())
        exprs.push_back(right.unwrap())
        for operand in additional_operands:
            exprs.push_back(operand.unwrap())
        self.init(CMakeAndExpression(exprs))


cdef class OrExpr(BinaryExpr):

    def __init__(self, Expr left not None, Expr right not None,
                 *additional_operands):
        cdef:
            Expr operand
            vector[shared_ptr[CExpression]] exprs
        exprs.push_back(left.unwrap())
        exprs.push_back(right.unwrap())
        for operand in additional_operands:
            exprs.push_back(operand.unwrap())
        self.init(CMakeOrExpression(exprs))
