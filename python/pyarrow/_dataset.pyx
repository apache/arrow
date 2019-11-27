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

import pyarrow as pa
from pyarrow.compat import frombytes, tobytes
from pyarrow.lib cimport *
# from pyarrow.lib import ArrowException
# from pyarrow.lib import as_buffer
# from pyarrow.lib cimport RecordBatch, MemoryPool, Schema, check_status
from pyarrow._fs cimport FileSystem, Selector
from pyarrow.includes.libarrow_dataset cimport *


__all__ = [
    'ScanOptions',
    'ScanContext',
    'ScanTask',
    'SimpleScanTask',
    'DataFragment',
    'SimpleDataFragment',
    'DataSource',
    'SimpleDataSource',
    'Dataset'
]


cdef class WriteOptions:
    pass


cdef class FileScanOptions(ScanOptions):
    pass


cdef class ParquetScanOptions(FileScanOptions):
    pass


cdef class FileWriteOptions(WriteOptions):
    pass


cdef class ParquetWriterOptions(FileWriteOptions):
    pass


cdef class FileFormat:

    cdef:
        shared_ptr[CFileFormat] wrapped
        CFileFormat* format

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
        else:
            raise TypeError(typ)

        self.init(sp)
        return self

    cdef inline shared_ptr[CPartitionScheme] unwrap(self):
        return self.wrapped


cdef class SchemaPartitionScheme(PartitionScheme):

    cdef:
        CSchemaPartitionScheme* schema_scheme  # hmmm...

    def __init__(self, Schema schema):
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


cdef class FileSystemDiscoveryOptions:

    cdef:
        CFileSystemDiscoveryOptions options

     # Avoid mistakingly creating attributes
    __slots__ = ()

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
    def partition_scheme(self):
        return PartitionScheme.wrap(self.discovery.partition_scheme())

    @partition_scheme.setter
    def partition_scheme(self, PartitionScheme scheme not None):
        check_status(self.discovery.SetPartitionScheme(scheme.unwrap()))

    def inspect(self):
        cdef CResult[shared_ptr[CSchema]] result
        with nogil:
            result = self.discovery.Inspect()
        return pyarrow_wrap_schema(GetResultValue(result))

    def schema(self):
        return pyarrow_wrap_schema(self.discovery.schema())

    def finish(self):
        cdef CResult[shared_ptr[CDataSource]] result
        with nogil:
            result = self.discovery.Finish()
        return DataSource.wrap(GetResultValue(result))


cdef class FileSystemDataSourceDiscovery(DataSourceDiscovery):

    cdef:
        CFileSystemDataSourceDiscovery* filesystem_discovery

    def __init__(self, FileSystem filesystem not None,
                 Selector selector not None, FileFormat format not None,
                 FileSystemDiscoveryOptions options=None):
        # TODO(kszucs): support instantiating from explicit paths
        cdef CResult[shared_ptr[CDataSourceDiscovery]] result

        options = options or FileSystemDiscoveryOptions()
        result = CFileSystemDataSourceDiscovery.Make(
            filesystem.unwrap(),
            selector.unwrap(),
            format.unwrap(),
            options.unwrap()
        )
        self.init(GetResultValue(result))

    cdef init(self, shared_ptr[CDataSourceDiscovery]& sp):
        DataSourceDiscovery.init(self, sp)
        self.filesystem_discovery = <CFileSystemDataSourceDiscovery*> sp.get()


cdef class FileSource:

    cdef:
        shared_ptr[CFileSource] wrapped
        CFileSource* source

    cdef readonly:
        FileSystem fs

    def __init__(self, path, FileSystem filesystem not None, compression=None):
        cdef shared_ptr[CFileSource] source

        # need to hold a reference for the filesystem
        self.fs = filesystem

        source.reset(new CFileSource(
            tobytes(path),
            self.fs.unwrap().get(),
            _get_compression_type(compression)
        ))

        self.init(source)

    cdef init(self, shared_ptr[CFileSource]& sp):
        self.wrapped = sp
        self.source = sp.get()

    @staticmethod
    cdef wrap(shared_ptr[CFileSource]& sp):
        cdef FileSource self = FileSource.__new__(FileSource)
        self.init(sp)
        return self

    cdef inline shared_ptr[CFileSource] unwrap(self):
        return self.wrapped

    def equals(self, FileSource other):
        return self.wrapped.get() == other.wrapped.get()

    def __eq__(self, other):
        try:
            return self.equals(other)
        except TypeError:
            return NotImplementedError

    @property
    def compression(self):
        return self.wrapped.get().compression()

    @property
    def path(self):
        return frombytes(self.wrapped.get().path())

    # def open(self):
    #     CStatus Open(shared_ptr[CRandomAccessFile]* out)


cdef class DataFragment:
    cdef:
        shared_ptr[CDataFragment] wrapped
        CDataFragment* fragment

    cdef void init(self, const shared_ptr[CDataFragment]& sp):
        self.wrapped = sp
        self.fragment = sp.get()

    @staticmethod
    cdef wrap(shared_ptr[CDataFragment]& sp):
        cdef DataFragment self = DataFragment.__new__(DataFragment)
        self.init(sp)
        return self

    cdef inline shared_ptr[CDataFragment] unwrap(self):
        return self.wrapped

    def scan(self, ScanContext context=None):
        cdef:
            CResult[CScanTaskIterator] iterator_result
            CScanTaskIterator iterator
            CScanTaskPtr task

        context = context or ScanContext()
        iterator_result = self.fragment.Scan(context.unwrap())
        iterator = move(GetResultValue(move(iterator_result)))

        while True:
            iterator.Next(&task)
            if task.get() == nullptr:
                raise StopIteration()
            else:
                yield ScanTask.wrap(task)

    @property
    def splittable(self):
        return self.fragment.splittable()

    @property
    def scan_options(self):
        return ScanOptions.wrap(self.fragment.scan_options())


cdef class SimpleDataFragment(DataFragment):

    cdef:
        CSimpleDataFragment* simple_fragment

    def __init__(self, record_batches):
        cdef:
            RecordBatch batch
            vector[shared_ptr[CRecordBatch]] batches
            shared_ptr[CSimpleDataFragment] simple_fragment

        for batch in record_batches:
            batches.push_back(batch.sp_batch)

        simple_fragment = make_shared[CSimpleDataFragment](batches)
        self.init(<shared_ptr[CDataFragment]> simple_fragment)

    cdef void init(self, const shared_ptr[CDataFragment]& sp):
        DataFragment.init(self, sp)
        self.simple_fragment = <CSimpleDataFragment*> sp.get()


cdef class FileDataFragment(DataFragment):
    pass


cdef class ParquetDataFragment(FileDataFragment):
    pass


cdef class DataSource:

    cdef:
        shared_ptr[CDataSource] wrapped
        CDataSource* source

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

    # def __iter__(self):
    #     for fragment in self.fragments():
    #         yield fragment

    def fragments(self, ScanOptions options=None):
        cdef:
            CDataFragmentIterator iterator
            CDataFragmentPtr fragment

        options = options or ScanOptions()
        iterator = self.source.GetFragments(options.unwrap())

        while True:
            iterator.Next(&fragment)
            if fragment.get() == nullptr:
                raise StopIteration()
            else:
                yield DataFragment.wrap(fragment)


cdef class SimpleDataSource(DataSource):

    cdef:
        CSimpleDataSource* simple_source

    def __init__(self, data_fragments):
        cdef:
            DataFragment fragment
            CDataFragmentVector fragments
            shared_ptr[CSimpleDataSource] simple_source

        for fragment in data_fragments:
            fragments.push_back(fragment.unwrap())

        simple_source = make_shared[CSimpleDataSource](fragments)
        self.init(<shared_ptr[CDataSource]> simple_source)

    cdef void init(self, const shared_ptr[CDataSource]& sp):
        DataSource.init(self, sp)
        self.simple_source = <CSimpleDataSource*> sp.get()


cdef class TreeDataSource(DataSource):

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

    cdef:
        CFileSystemDataSource* filesystem_source

    def __init__(self, FileSystem filesystem not None, file_stats,
                 source_partition, path_partitions, file_format):
        cdef:
            DataSource child
            CDataSourceVector children
            shared_ptr[CFileSystemDataSource] filesystem_source

        # TODO(kszucs)
        # filesystem_source = CFileSystemDataSource.Make(
        #     fs::FileSystem* filesystem, std::vector<fs::FileStats> stats,
        #     std::shared_ptr<Expression> source_partition,
        #     PathPartitions partitions, std::shared_ptr<FileFormat> format,
        #     std::shared_ptr<DataSource>* out
        # )
        self.init(<shared_ptr[CDataSource]> filesystem_source)

    cdef void init(self, const shared_ptr[CDataSource]& sp):
        DataSource.init(self, sp)
        self.filesystem_source = <CFileSystemDataSource*> sp.get()


cdef class Dataset:

    cdef:
        shared_ptr[CDataset] wrapped
        CDataset* dataset

    def __init__(self, data_sources, Schema schema=None):
        cdef:
            DataSource source
            CDataSourceVector sources
            CResult[CDatasetPtr] result

        for source in data_sources:
            sources.push_back(source.wrapped)

        result = CDataset.Make(sources, schema.sp_schema)
        # if schema is None:
        #     dataset = make_shared[CDataset](sources)
        # else:
        #     dataset = make_shared[CDataset](sources, schema.sp_schema)
        self.init(GetResultValue(result))

    cdef void init(self, const shared_ptr[CDataset]& sp):
        self.wrapped = sp
        self.dataset = sp.get()

    cdef inline shared_ptr[CDataset] unwrap(self):
        return self.wrapped

    # TODO(kszucs): pass ScanContext
    def new_scan(self):
        cdef shared_ptr[CScannerBuilder] builder
        builder = GetResultValue(self.dataset.NewScan())
        # return ScannerBuilder(self, context)
        return ScannerBuilder.wrap(builder)

    @property
    def sources(self):
        cdef vector[shared_ptr[CDataSource]] sources = self.dataset.sources()
        return [DataSource.wrap(source) for source in sources]

    @property
    def schema(self):
        return pyarrow_wrap_schema(self.dataset.schema())


cdef class ScanOptions:

    cdef:
        shared_ptr[CScanOptions] wrapped
        CScanOptions* options

    def __init__(self, Schema schema=None):
        cdef shared_ptr[CScanOptions] options = CScanOptions.Defaults()
        if schema is not None:
            options.get().schema = pyarrow_unwrap_schema(schema)
        self.init(options)

    cdef init(self, const shared_ptr[CScanOptions]& sp):
        self.wrapped = sp
        self.options = sp.get()

    @staticmethod
    cdef wrap(const shared_ptr[CScanOptions]& sp):
        cdef ScanOptions self = ScanOptions.__new__(ScanOptions)
        self.init(sp)
        return self

    cdef inline shared_ptr[CScanOptions] unwrap(self):
        return self.wrapped


cdef class ScanContext:

    cdef:
        shared_ptr[CScanContext] wrapped
        CScanContext *context

    def __init__(self, MemoryPool memory_pool=None):
        cdef shared_ptr[CScanContext] context = make_shared[CScanContext]()

        if memory_pool is not None:
            context.get().pool = memory_pool.pool

        self.init(context)

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


cdef class ScanTask:

    cdef:
        shared_ptr[CScanTask] wrapped
        CScanTask* task

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

    cdef:
        CSimpleScanTask* simple_task

    cdef init(self, shared_ptr[CScanTask]& sp):
        ScanTask.init(self, sp)
        self.simple_task = <CSimpleScanTask*> sp.get()


cdef class ScannerBuilder:

    cdef:
        shared_ptr[CScannerBuilder] wrapped
        CScannerBuilder* builder

    def __init__(self, Dataset dataset, ScanContext context):
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
        cdef vector[c_string] cols = [tobytes(c) for c in columns]
        check_status(self.builder.Project(cols))
        return self

    def finish(self):
        return Scanner.wrap(GetResultValue(self.builder.Finish()))

    def filter(self, Expression expression):
        check_status(self.builder.Filter(expression.unwrap()))
        return self


cdef class Scanner:

    cdef:
        shared_ptr[CScanner] wrapped
        CScanner* scanner

    def __init__(self, data_sources, ScanOptions options, ScanContext context):
        cdef:
            DataSource source
            CDataSourceVector sources
            shared_ptr[CScanner] scanner

        for source in data_sources:
            sources.push_back(source.unwrap())

        scanner = make_shared[CScanner](
            sources, options.unwrap(), context.unwrap()
        )
        self.init(scanner)

    cdef void init(self, shared_ptr[CScanner]& sp):
        self.wrapped = sp
        self.scanner = sp.get()

    @staticmethod
    cdef wrap(shared_ptr[CScanner]& sp):
        cdef Scanner self = Scanner.__new__(Scanner)
        self.init(sp)
        return self

    def scan(self):
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
        cdef CResult[shared_ptr[CTable]] result

        with nogil:
            result = self.scanner.ToTable()

        return pyarrow_wrap_table(GetResultValue(result))


cdef class Expression:

    cdef:
        shared_ptr[CExpression] wrapped
        CExpression* expression

    def __init__(self):
        raise TypeError('Do not ititialize')

    cdef void init(self, const shared_ptr[CExpression]& sp):
        self.wrapped = sp
        self.expression = sp.get()

    @staticmethod
    cdef wrap(shared_ptr[CExpression]& sp):
        cdef Expression self

        typ = sp.get().type()
        if typ == CExpressionType_FIELD:
            self = FieldExpression.__new__(FieldExpression)
        elif typ == CExpressionType_SCALAR:
            self = ScalarExpression.__new__(ScalarExpression)
        elif typ == CExpressionType_NOT:
            self = NotExpression.__new__(NotExpression)
        # elif typ == CExpressionType_CAST:
        #     self = CastExpression.__new__(CastExpression)
        elif typ == CExpressionType_AND:
            self = AndExpression.__new__(AndExpression)
        elif typ == CExpressionType_OR:
            self = OrExpression.__new__(OrExpression)
        elif typ == CExpressionType_COMPARISON:
            self = ComparisonExpression.__new__(ComparisonExpression)
        # elif typ == CExpressionType_IS_VALID:
        #     self = IsValidExpression.__new__(IsValidExpression)
        # elif typ == CExpressionType_IN:
        #     self = InExpression.__new__(InExpression)
        # elif typ == CExpressionType_CUSTOM:
        #     self = CustomExpression.__new__(CustomExpression)
        else:
            raise TypeError(typ)

        self.init(sp)
        return self

    cdef inline shared_ptr[CExpression] unwrap(self):
        return self.wrapped


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

    # def value(self):
    #     return pyarrow_wrap_scalar(self.scalar.value())


cdef class FieldExpression(Expression):

    cdef CFieldExpression* scalar

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

    # def op(self):
    #     return ...


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
