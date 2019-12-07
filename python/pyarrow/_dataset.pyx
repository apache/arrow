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

from __future__ import absolute_import

import collections
import six

from cython.operator cimport dereference as deref

from pyarrow.lib cimport *
from pyarrow.includes.libarrow_dataset cimport *
from pyarrow.compat import frombytes, tobytes
from pyarrow._fs cimport FileSystem, FileStats, Selector


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

        # TODO(kszucs): either choose type() or name() but consistently
        typ = frombytes(sp.get().name())
        if typ == 'parquet':
            self = ParquetFileFormat.__new__(ParquetFileFormat)
        else:
            raise TypeError(typ)

        self.init(sp)
        return self

    cdef inline shared_ptr[CFileFormat] unwrap(self):
        return self.wrapped

    def name(self):
        return frombytes(self.format.name())


cdef class ParquetFileFormat(FileFormat):

    def __init__(self):
        self.init(shared_ptr[CFileFormat](new CParquetFileFormat()))


cdef class PartitionScheme:

    cdef:
        shared_ptr[CPartitionScheme] wrapped
        CPartitionScheme* scheme

    def __init__(self):
        _forbid_instantiation(self.__class__)

    cdef init(self, const shared_ptr[CPartitionScheme]& sp):
        self.wrapped = sp
        self.scheme = sp.get()

    @staticmethod
    cdef wrap(const shared_ptr[CPartitionScheme]& sp):
        cdef PartitionScheme self

        # TODO(kszucs): either choose type() or name() but consistently
        typ = frombytes(sp.get().name())
        if typ == 'schema_partition_scheme':
            self = SchemaPartitionScheme.__new__(SchemaPartitionScheme)
        elif typ == 'hive_partition_scheme':
            self = HivePartitionScheme.__new__(HivePartitionScheme)
        else:
            raise TypeError(typ)

        self.init(sp)
        return self

    cdef inline shared_ptr[CPartitionScheme] unwrap(self):
        return self.wrapped

    def parse(self, path):
        cdef CResult[shared_ptr[CExpression]] result
        result = self.scheme.Parse(tobytes(path))
        return Expression.wrap(GetResultValue(result))


cdef class SchemaPartitionScheme(PartitionScheme):

    cdef:
        CSchemaPartitionScheme* schema_scheme  # hmmm...

    def __init__(self, Schema schema not None):
        cdef shared_ptr[CSchemaPartitionScheme] scheme
        scheme = make_shared[CSchemaPartitionScheme](
            pyarrow_unwrap_schema(schema)
        )
        self.init(<shared_ptr[CPartitionScheme]> scheme)

    cdef init(self, const shared_ptr[CPartitionScheme]& sp):
        PartitionScheme.init(self, sp)
        self.schema_scheme = <CSchemaPartitionScheme*> sp.get()

    @property
    def schema(self):
        return pyarrow_wrap_schema(self.schema_scheme.schema())


cdef class HivePartitionScheme(PartitionScheme):

    cdef:
        CHivePartitionScheme* hive_scheme

    def __init__(self, Schema schema not None):
        cdef shared_ptr[CHivePartitionScheme] scheme
        scheme = make_shared[CHivePartitionScheme](
            pyarrow_unwrap_schema(schema)
        )
        self.init(<shared_ptr[CPartitionScheme]> scheme)

    cdef init(self, const shared_ptr[CPartitionScheme]& sp):
        PartitionScheme.init(self, sp)
        self.hive_scheme = <CHivePartitionScheme*> sp.get()

    @property
    def schema(self):
        return pyarrow_wrap_schema(self.hive_scheme.schema())


cdef class FileSystemDiscoveryOptions:

    cdef:
        CFileSystemDiscoveryOptions options

    __slots__ = ()  # avoid mistakingly creating attributes

    def __init__(self, partition_base_dir=None, exclude_invalid_files=None,
                 list ignore_prefixes=None):
        if partition_base_dir is not None:
            self.partition_base_dir = partition_base_dir
        if exclude_invalid_files is not None:
            self.exclude_invalid_files = exclude_invalid_files
        if ignore_prefixes is not None:
            self.ignore_prefixes = ignore_prefixes

    cdef inline CFileSystemDiscoveryOptions unwrap(self):
        return self.options

    @property
    def partition_base_dir(self):
        return frombytes(self.options.partition_base_dir)

    @partition_base_dir.setter
    def partition_base_dir(self, value):
        self.options.partition_base_dir = tobytes(value)

    @property
    def exclude_invalid_files(self):
        return self.options.exclude_invalid_files

    @exclude_invalid_files.setter
    def exclude_invalid_files(self, bint value):
        self.options.exclude_invalid_files = value

    @property
    def ignore_prefixes(self):
        return [frombytes(p) for p in self.options.ignore_prefixes]

    @ignore_prefixes.setter
    def ignore_prefixes(self, values):
        self.options.ignore_prefixes = [tobytes(v) for v in values]


cdef class DataSourceDiscovery:

    cdef:
        shared_ptr[CDataSourceDiscovery] wrapped
        CDataSourceDiscovery* discovery

    def __init__(self):
        _forbid_instantiation(self.__class__)

    cdef init(self, shared_ptr[CDataSourceDiscovery]& sp):
        self.wrapped = sp
        self.discovery = sp.get()

    @staticmethod
    cdef wrap(shared_ptr[CDataSourceDiscovery]& sp):
        cdef DataSourceDiscovery self = \
            DataSourceDiscovery.__new__(DataSourceDiscovery)
        self.init(sp)
        return self

    cdef inline shared_ptr[CDataSourceDiscovery] unwrap(self):
        return self.wrapped

    @property
    def schema(self):
        cdef shared_ptr[CSchema] schema = self.discovery.schema()
        if schema.get() == nullptr:
            return None
        else:
            return pyarrow_wrap_schema(schema)

    @property
    def partition_scheme(self):
        cdef shared_ptr[CPartitionScheme] scheme
        scheme = self.discovery.partition_scheme()
        if scheme.get() == nullptr:
            return None
        else:
            return PartitionScheme.wrap(scheme)

    @partition_scheme.setter
    def partition_scheme(self, PartitionScheme scheme not None):
        check_status(self.discovery.SetPartitionScheme(scheme.unwrap()))

    @property
    def root_partition(self):
        cdef shared_ptr[CExpression] expr = self.discovery.root_partition()
        if expr.get() == nullptr:
            return None
        else:
            return Expression.wrap(expr)

    def inspect(self):
        cdef CResult[shared_ptr[CSchema]] result
        with nogil:
            result = self.discovery.Inspect()
        return pyarrow_wrap_schema(GetResultValue(result))

    def finish(self):
        cdef CResult[shared_ptr[CDataSource]] result
        with nogil:
            result = self.discovery.Finish()
        return DataSource.wrap(GetResultValue(result))


cdef class FileSystemDataSourceDiscovery(DataSourceDiscovery):

    cdef:
        CFileSystemDataSourceDiscovery* filesystem_discovery

    def __init__(self, FileSystem filesystem not None, paths_or_selector,
                 FileFormat format not None,
                 FileSystemDiscoveryOptions options=None):
        cdef:
            FileStats file_stats
            vector[CFileStats] stats
            CResult[shared_ptr[CDataSourceDiscovery]] result

        options = options or FileSystemDiscoveryOptions()
        for file_stats in filesystem.get_target_stats(paths_or_selector):
            stats.push_back(file_stats.unwrap())

        result = CFileSystemDataSourceDiscovery.MakeFromFileStats(
            filesystem.unwrap(),
            stats,
            format.unwrap(),
            options.unwrap()
        )
        self.init(GetResultValue(result))

    cdef init(self, shared_ptr[CDataSourceDiscovery]& sp):
        DataSourceDiscovery.init(self, sp)
        self.filesystem_discovery = <CFileSystemDataSourceDiscovery*> sp.get()


cdef class DataSource:
    """Basic component of a Dataset which yields zero or more data fragments.

    A DataSource acts as a discovery mechanism of data fragments and
    partitions, e.g. files deeply nested in a directory.
    """

    cdef:
        shared_ptr[CDataSource] wrapped
        CDataSource* source

    def __init__(self):
        _forbid_instantiation(self.__class__)

    cdef void init(self, const shared_ptr[CDataSource]& sp):
        self.wrapped = sp
        self.source = sp.get()

    @staticmethod
    cdef wrap(shared_ptr[CDataSource]& sp):
        cdef DataSource self

        typ = frombytes(sp.get().type())
        if typ == 'tree_data_source':
            self = TreeDataSource.__new__(TreeDataSource)
        elif typ == 'simple_data_source':
            self = SimpleDataSource.__new__(SimpleDataSource)
        elif typ == 'filesystem_data_source':
            self = FileSystemDataSource.__new__(FileSystemDataSource)
        else:
            raise TypeError(typ)

        self.init(sp)
        return self

    cdef shared_ptr[CDataSource] unwrap(self):
        return self.wrapped

    @property
    def partition_expression(self):
        cdef shared_ptr[CExpression] expression
        expression = self.source.partition_expression()
        if expression.get() == nullptr:
            return None
        else:
            return Expression.wrap(expression)


# TODO(kszucs) Rename it to InMemoryDataSource
cdef class SimpleDataSource(DataSource):
    """An in memory DataSource created from in memory RecordBatches"""

    cdef:
        CSimpleDataSource* simple_source

    def __init__(self, record_batches):
        cdef:
            RecordBatch batch
            vector[shared_ptr[CRecordBatch]] batches
            shared_ptr[CSimpleDataSource] simple_source

        for batch in record_batches:
            batches.push_back(pyarrow_unwrap_batch(batch))

        simple_source = make_shared[CSimpleDataSource](batches)
        self.init(<shared_ptr[CDataSource]> simple_source)

    cdef void init(self, const shared_ptr[CDataSource]& sp):
        DataSource.init(self, sp)
        self.simple_source = <CSimpleDataSource*> sp.get()


cdef class TreeDataSource(DataSource):
    """A DataSource created from other data source objects"""

    cdef:
        CTreeDataSource* tree_source

    def __init__(self, data_sources):
        cdef:
            DataSource child
            CDataSourceVector children
            shared_ptr[CTreeDataSource] tree_source

        for child in data_sources:
            children.push_back(child.wrapped)

        tree_source = make_shared[CTreeDataSource](children)
        self.init(<shared_ptr[CDataSource]> tree_source)

    cdef void init(self, const shared_ptr[CDataSource]& sp):
        DataSource.init(self, sp)
        self.tree_source = <CTreeDataSource*> sp.get()


cdef class FileSystemDataSource(DataSource):
    """"""

    cdef:
        CFileSystemDataSource* filesystem_source

    def __init__(self, FileSystem filesystem not None, paths_or_selector,
                 Expression source_partition, dict path_partitions,
                 FileFormat file_format not None):
        """Create a FileSystemDataSource

        Parameters
        ----------
        filesystem : FileSystem
            Filesystem to discover.
        paths_or_selector : Union[Selector, List[FileStats]]
            The file stats object can be queried by the
            filesystem.get_target_stats method.
        source_partition : Expression
        path_partitions : Dict[str, Expression]
        file_format : FileFormat
        """
        cdef:
            FileStats stats
            Expression expression
            vector[CFileStats] c_file_stats
            shared_ptr[CExpression] c_source_partition
            unordered_map[c_string, shared_ptr[CExpression]] c_path_partitions
            CResult[shared_ptr[CDataSource]] result

        for stats in filesystem.get_target_stats(paths_or_selector):
            c_file_stats.push_back(stats.unwrap())

        for path, expression in path_partitions.items():
            c_path_partitions[tobytes(path)] = expression.unwrap()

        if source_partition is not None:
            c_source_partition = source_partition.unwrap()

        result = CFileSystemDataSource.Make(
            filesystem.unwrap(),
            c_file_stats,
            c_source_partition,
            c_path_partitions,
            file_format.unwrap()
        )
        self.init(GetResultValue(result))

    cdef void init(self, const shared_ptr[CDataSource]& sp):
        DataSource.init(self, sp)
        self.filesystem_source = <CFileSystemDataSource*> sp.get()


cdef class Dataset:
    """Collection of data fragments coming from possibly multiple sources."""

    cdef:
        shared_ptr[CDataset] wrapped
        CDataset* dataset

    def __init__(self, data_sources, Schema schema not None):
        """Create a dataset

        A schema must be passed because most of the data sources' schema is
        unknown before executing possibly expensive scanning operation, but
        projecting, filtering, predicate pushduwn requires a well defined
        schema to work on.

        Parameters
        ----------
        data_sources : list of DataSource
            One or more input data sources
        schema : Schema
            A known schema to conform to.
        """
        cdef:
            DataSource source
            CDataSourceVector sources
            CResult[CDatasetPtr] result
            shared_ptr[CSchema] sp_schema

        for source in data_sources:
            sources.push_back(source.wrapped)

        result = CDataset.Make(sources, pyarrow_unwrap_schema(schema))

        self.init(GetResultValue(result))

    cdef void init(self, const shared_ptr[CDataset]& sp):
        self.wrapped = sp
        self.dataset = sp.get()

    cdef inline shared_ptr[CDataset] unwrap(self):
        return self.wrapped

    def new_scan(self, ScanContext context=None):
        """Begin to build a new Scan operation against this Dataset."""
        cdef CResult[shared_ptr[CScannerBuilder]] result
        if context is None:
            result = self.dataset.NewScan()
        else:
            result = self.dataset.NewScanWithContext(context.unwrap())
        return ScannerBuilder.wrap(GetResultValue(result))

    @property
    def sources(self):
        cdef vector[shared_ptr[CDataSource]] sources = self.dataset.sources()
        return [DataSource.wrap(source) for source in sources]

    @property
    def schema(self):
        return pyarrow_wrap_schema(self.dataset.schema())


cdef class ScanContext:

    cdef:
        shared_ptr[CScanContext] wrapped
        CScanContext *context

    def __init__(self, MemoryPool memory_pool=None):
        cdef shared_ptr[CScanContext] context = make_shared[CScanContext]()
        self.init(context)
        if memory_pool is not None:
            self.memory_pool = memory_pool

    cdef init(self, shared_ptr[CScanContext]& sp):
        self.wrapped = sp
        self.context = sp.get()

    @staticmethod
    cdef ScanContext wrap(shared_ptr[CScanContext]& sp):
        cdef ScanContext self = ScanContext.__new__(ScanContext)
        self.init(sp)
        return self

    cdef inline shared_ptr[CScanContext] unwrap(self):
        return self.wrapped

    @property
    def memory_pool(self):
        # TODO(kszucs): there's no function to box/wrap memory pool
        cdef:
            MemoryPool pool = MemoryPool.__new__(MemoryPool)
        # this might be unsafe because of the raw pointers
        pool.init(self.context.pool)
        return pool

    @memory_pool.setter
    def memory_pool(self, MemoryPool pool):
        self.context.pool = maybe_unbox_memory_pool(pool)


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
        cdef SimpleScanTask self = SimpleScanTask.__new__(SimpleScanTask)
        self.init(sp)
        return self

    cdef inline shared_ptr[CScanTask] unwrap(self):
        return self.wrapped

    def scan(self):
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

        iterator = move(GetResultValue(move(self.task.Scan())))

        while True:
            iterator.Next(&record_batch)
            if record_batch.get() == nullptr:
                raise StopIteration()
            else:
                yield pyarrow_wrap_batch(record_batch)


cdef class SimpleScanTask(ScanTask):
    """A trivial ScanTask that yields the RecordBatch of an array."""

    cdef:
        CSimpleScanTask* simple_task

    cdef init(self, shared_ptr[CScanTask]& sp):
        ScanTask.init(self, sp)
        self.simple_task = <CSimpleScanTask*> sp.get()


cdef class ScannerBuilder:
    """Factory class to construct a Scanner.

    It is used to pass information, notably a potential filter expression and a
    subset of columns to materialize.
    """

    cdef:
        shared_ptr[CScannerBuilder] wrapped
        CScannerBuilder* builder

    def __init__(self, Dataset dataset not None, ScanContext context not None):
        cdef shared_ptr[CScannerBuilder] builder
        builder = make_shared[CScannerBuilder](
            dataset.unwrap(), context.unwrap()
        )
        self.init(builder)

    cdef void init(self, shared_ptr[CScannerBuilder]& sp):
        self.wrapped = sp
        self.builder = sp.get()

    @staticmethod
    cdef wrap(shared_ptr[CScannerBuilder]& sp):
        cdef ScannerBuilder self = ScannerBuilder.__new__(ScannerBuilder)
        self.init(sp)
        return self

    cdef inline shared_ptr[CScannerBuilder] unwrap(self):
        return self.wrapped

    def project(self, columns):
        """Set the subset of columns to materialize.

        This subset will be passed down to DataSources and corresponding
        data fragments. The goal is to avoid loading, copying, and
        deserializing columns that will not be required further down the
        compute chain.

        It alters the object in place and returns with the object itself
        enabling method chaining. Raises exception if any of the referenced
        column names does not exists in the dataset's Schema.

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
        self : ScannerBuilder
        """
        return Scanner.wrap(GetResultValue(self.builder.Finish()))

    def filter(self, Expression filter_expression not None):
        """Set the filter expression to return only rows matching the filter.

        The predicate will be passed down to DataSources and corresponding
        data fragments to exploit predicate pushdown if possible using
        partition information or internal metadata, e.g. Parquet statistics.
        Otherwise filters the loaded RecordBatches before yielding them.

        It alters the object in place and returns with the object itself
        enabling method chaining. Raises exception if any of the referenced
        column names does not exists in the dataset's Schema.

        Parameters
        ----------
        filter_expression : Expression
            Boolean expression to filter rows with.

        Returns
        -------
        self : ScannerBuilder
        """
        check_status(self.builder.Filter(filter_expression.unwrap()))
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
            iterator.Next(&task)
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


cdef class Expression:

    cdef:
        shared_ptr[CExpression] wrapped
        CExpression* expression

    def __init__(self):
        _forbid_instantiation(self.__class__)

    cdef void init(self, const shared_ptr[CExpression]& sp):
        self.wrapped = sp
        self.expression = sp.get()

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
        # TODO(kszucs): implement it
        # elif typ == CExpressionType_CUSTOM:
        #     self = CustomExpression.__new__(CustomExpression)
        else:
            raise TypeError(typ)

        self.init(sp)
        return self

    cdef inline shared_ptr[CExpression] unwrap(self):
        return self.wrapped

    def equals(self, Expression other):
        return self.expression.Equals(other.unwrap())

    def __str__(self):
        return frombytes(self.expression.ToString())

    def validate(self, Schema schema not None):
        cdef:
            shared_ptr[CSchema] sp_schema
            CResult[shared_ptr[CDataType]] result
        sp_schema = pyarrow_unwrap_schema(schema)
        result = self.expression.Validate(deref(sp_schema))
        return pyarrow_wrap_data_type(GetResultValue(result))

    def assume(self, Expression given):
        return Expression.wrap(self.expression.Assume(given.unwrap()))


cdef class UnaryExpression(Expression):

    cdef CUnaryExpression* unary

    cdef void init(self, const shared_ptr[CExpression]& sp):
        Expression.init(self, sp)
        self.unary = <CUnaryExpression*> sp.get()


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
            shared_ptr[CScalarExpression] expression

        if isinstance(value, bool):
            scalar = MakeScalar(<c_bool>value)
        elif isinstance(value, float):
            scalar = MakeScalar(<double>value)
        elif isinstance(value, int):
            scalar = MakeScalar(<int64_t>value)
        else:
            raise TypeError('Not yet supported scalar value: {}'.format(value))

        expression.reset(new CScalarExpression(scalar))
        self.init(<shared_ptr[CExpression]> expression)

    cdef void init(self, const shared_ptr[CExpression]& sp):
        Expression.init(self, sp)
        self.scalar = <CScalarExpression*> sp.get()

    # TODO(kszucs): implement once we have proper Scalar bindings
    # @property
    # def value(self):
    #     return pyarrow_wrap_scalar(self.scalar.value())


cdef class FieldExpression(Expression):

    cdef CFieldExpression* scalar

    def __init__(self, name):
        cdef:
            c_string field_name = tobytes(name)
            shared_ptr[CExpression] expression
        expression.reset(new CFieldExpression(field_name))
        self.init(expression)

    cdef void init(self, const shared_ptr[CExpression]& sp):
        Expression.init(self, sp)
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


cdef class ComparisonExpression(BinaryExpression):

    cdef CComparisonExpression* comparison

    def __init__(self, CompareOperator op,
                 Expression left_operand not None,
                 Expression right_operand not None):
        cdef shared_ptr[CComparisonExpression] expression
        expression.reset(
            new CComparisonExpression(
                <CCompareOperator>op,
                left_operand.unwrap(),
                right_operand.unwrap()
            )
        )
        self.init(<shared_ptr[CExpression]> expression)

    cdef void init(self, const shared_ptr[CExpression]& sp):
        BinaryExpression.init(self, sp)
        self.comparison = <CComparisonExpression*> sp.get()

    def op(self):
        return <CompareOperator> self.comparison.op()


cdef class IsValidExpression(UnaryExpression):

    def __init__(self, Expression operand not None):
        cdef shared_ptr[CIsValidExpression] expression
        expression = make_shared[CIsValidExpression](operand.unwrap())
        self.init(<shared_ptr[CExpression]> expression)


cdef class CastOptions:

    cdef:
        CCastOptions options

    __slots__ = ()  # avoid mistakingly creating attributes

    def __init__(self, allow_int_overflow=None,
                 allow_time_truncate=None, allow_time_overflow=None,
                 allow_float_truncate=None, allow_invalid_utf8=None):
        if allow_int_overflow is not None:
            self.allow_int_overflow = allow_int_overflow
        if allow_time_truncate is not None:
            self.allow_time_truncate = allow_time_truncate
        if allow_time_overflow is not None:
            self.allow_time_overflow = allow_time_overflow
        if allow_float_truncate is not None:
            self.allow_float_truncate = allow_float_truncate
        if allow_invalid_utf8 is not None:
            self.allow_invalid_utf8 = allow_invalid_utf8

    @staticmethod
    cdef wrap(CCastOptions options):
        cdef CastOptions self = CastOptions.__new__(CastOptions)
        self.options = options
        return self

    @staticmethod
    def safe():
        return CastOptions.wrap(CCastOptions.Safe())

    @staticmethod
    def unsafe():
        return CastOptions.wrap(CCastOptions.Unsafe())

    cdef inline CCastOptions unwrap(self):
        return self.options

    @property
    def allow_int_overflow(self):
        return self.options.allow_int_overflow

    @allow_int_overflow.setter
    def allow_int_overflow(self, bint flag):
        self.options.allow_int_overflow = flag

    @property
    def allow_time_truncate(self):
        return self.options.allow_time_truncate

    @allow_time_truncate.setter
    def allow_time_truncate(self, bint flag):
        self.options.allow_time_truncate = flag

    @property
    def allow_time_overflow(self):
        return self.options.allow_time_overflow

    @allow_time_overflow.setter
    def allow_time_overflow(self, bint flag):
        self.options.allow_time_overflow = flag

    @property
    def allow_float_truncate(self):
        return self.options.allow_float_truncate

    @allow_float_truncate.setter
    def allow_float_truncate(self, bint flag):
        self.options.allow_float_truncate = flag

    @property
    def allow_invalid_utf8(self):
        return self.options.allow_invalid_utf8

    @allow_invalid_utf8.setter
    def allow_invalid_utf8(self, bint flag):
        self.options.allow_invalid_utf8 = flag


cdef class CastExpression(UnaryExpression):

    def __init__(self, Expression operand not None, DataType to not None,
                 CastOptions options=None):
        cdef shared_ptr[CExpression] expression
        options = options or CastOptions.safe()
        expression.reset(new CCastExpression(
            operand.unwrap(),
            pyarrow_unwrap_data_type(to),
            options.unwrap()
        ))
        self.init(expression)


cdef class InExpression(UnaryExpression):

    def __init__(self, Expression operand not None, Array haystack not None):
        cdef shared_ptr[CExpression] expression
        expression.reset(
            new CInExpression(operand.unwrap(), pyarrow_unwrap_array(haystack))
        )
        self.init(expression)


cdef class NotExpression(UnaryExpression):

    def __init__(self, Expression operand not None):
        cdef shared_ptr[CNotExpression] expression
        expression = MakeNotExpression(operand.unwrap())
        self.init(<shared_ptr[CExpression]> expression)


cdef class AndExpression(BinaryExpression):

    def __init__(self, Expression left_operand not None,
                 Expression right_operand not None,
                 *additional_operands):
        cdef:
            Expression operand
            vector[shared_ptr[CExpression]] exprs
        exprs.push_back(left_operand.unwrap())
        exprs.push_back(right_operand.unwrap())
        for operand in additional_operands:
            exprs.push_back(operand.unwrap())
        self.init(MakeAndExpression(exprs))


cdef class OrExpression(BinaryExpression):

    def __init__(self, Expression left_operand not None,
                 Expression right_operand not None,
                 *additional_operands):
        cdef:
            Expression operand
            vector[shared_ptr[CExpression]] exprs
        exprs.push_back(left_operand.unwrap())
        exprs.push_back(right_operand.unwrap())
        for operand in additional_operands:
            exprs.push_back(operand.unwrap())
        self.init(MakeOrExpression(exprs))
