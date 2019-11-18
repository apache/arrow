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

from pyarrow.compat import frombytes, tobytes
from pyarrow.lib cimport *
# from pyarrow.lib import ArrowException
# from pyarrow.lib import as_buffer
# from pyarrow.lib cimport RecordBatch, MemoryPool, Schema, check_status
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


cdef class CsvScanOptions(FileScanOptions):
    pass


cdef class FeatherScanOptions(FileScanOptions):
    pass


cdef class ParquetScanOptions(FileScanOptions):
    pass


cdef class JsonScanOptions(FileScanOptions):
    pass


cdef class FileWriteOptions(WriteOptions):
    pass


cdef class CsvWriteOptions(FileWriteOptions):
    pass


cdef class FeatherWriterOptions(FileWriteOptions):
    pass


cdef class JsonWriterOptions(FileWriteOptions):
    pass


cdef class ParquetWriterOptions(FileWriteOptions):
    pass


cdef class FileFormat:
    pass


cdef class CsvFileFormat(FileFormat):
    pass


cdef class FeatherFileFormat(FileFormat):
    pass


cdef class JsonFileFormat(FileFormat):
    pass


cdef class ParquetFileFormat(FileFormat):
    pass


cdef class FileSource:

    cdef:
        shared_ptr[CFileSource] wrapped


cdef class DataFragment:
    cdef:
        shared_ptr[CDataFragment] wrapped
        CDataFragment* fragment

    cdef void init(self, const shared_ptr[CDataFragment]& sp):
        self.wrapped = sp
        self.fragment = sp.get()

    @staticmethod
    cdef DataFragment wrap(shared_ptr[CDataFragment]& sp):
        cdef DataFragment self = DataFragment.__new__(DataFragment)
        self.init(sp)
        return self

    def scan(self, ScanContext context=None):
        cdef:
            CScanTaskIterator task_iterator
            unique_ptr[CScanTask] task

        context = context or ScanContext()
        check_status(self.fragment.Scan(context.wrapped, &task_iterator))

        while True:
            task_iterator.Next(&task)
            if task == nullptr:
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
            self = DataSource.__new__(DataSource)

        self.init(sp)
        return self

    cdef shared_ptr[CDataSource] unwrap(self):
        return self.wrapped

    @property
    def type(self):
        return frombytes(self.source.type())

    # def __iter__(self):
    #     for fragment in self.fragments():
    #         yield fragment

    def fragments(self, ScanOptions options=None):
        cdef:
            CDataFragmentIterator fragment_iterator
            shared_ptr[CDataFragment] fragment

        options = options or ScanOptions()

        # NOTE(kszucs): shouldn't we return Status here?
        fragment_iterator = self.source.GetFragments(options.wrapped)

        while True:
            fragment_iterator.Next(&fragment)
            if fragment == nullptr:
                raise StopIteration()
            else:
                yield DataFragment.wrap(fragment)

    get_fragments = fragments


cdef class SimpleDataSource(DataSource):

    cdef:
        CSimpleDataSource* simple_source

    def __init__(self, data_fragments):
        cdef:
            DataFragment fragment
            vector[shared_ptr[CDataFragment]] fragments
            shared_ptr[CSimpleDataSource] simple_source

        for fragment in data_fragments:
            fragments.push_back(fragment.wrapped)

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
            vector[shared_ptr[CDataSource]] children
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

    def __init__(self, filesystem, file_stats, source_partition,
                 path_partitions, file_format):
        cdef:
            DataSource child
            vector[shared_ptr[CDataSource]] children
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
            vector[shared_ptr[CDataSource]] sources
            shared_ptr[CDataset] dataset

        for source in data_sources:
            sources.push_back(source.wrapped)

        check_status(CDataset.Make(sources, schema.sp_schema, &dataset))
        # if schema is None:
        #     dataset = make_shared[CDataset](sources)
        # else:
        #     dataset = make_shared[CDataset](sources, schema.sp_schema)
        self.init(dataset)

    cdef void init(self, const shared_ptr[CDataset]& sp):
        self.wrapped = sp
        self.dataset = sp.get()

    def new_scan(self):
        cdef:
            unique_ptr[CScannerBuilder] builder
            unique_ptr[CScanner] scanner

        # TODO(kszucs): add other options like filter, schema etc.
        check_status(self.dataset.NewScan(&builder))
        check_status(builder.get().Finish(&scanner))

        return Scanner.wrap(scanner)

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

    def __init__(self):
        cdef shared_ptr[CScanOptions] options
        self.init(options)

    cdef init(self, const shared_ptr[CScanOptions]& sp):
        self.wrapped = sp
        self.options = sp.get()

    @staticmethod
    cdef ScanOptions wrap(const shared_ptr[CScanOptions]& sp):
        cdef ScanOptions self = ScanOptions.__new__(ScanOptions)
        self.init(sp)
        return self

    cdef inline shared_ptr[CScanOptions] unwrap(self):
        return self.wrapped


cdef class ScanContext:

    cdef:
        shared_ptr[CScanContext] wrapped
        CScanContext* context

    def __init__(self, MemoryPool memory_pool=None):
        cdef shared_ptr[CScanContext] context

        if memory_pool is not None:
            context.get().pool = memory_pool.pool

        self.init(context)

    cdef init(self, const shared_ptr[CScanContext]& sp):
        self.wrapped = sp
        self.context = sp.get()

    @staticmethod
    cdef ScanContext wrap(const shared_ptr[CScanContext]& sp):
        cdef ScanContext self = ScanContext.__new__(ScanContext)
        self.init(sp)
        return self

    cdef inline shared_ptr[CScanContext] unwrap(self):
        return self.wrapped


cdef class ScanTask:

    cdef:
        unique_ptr[CScanTask] wrapped
        CScanTask* task

    cdef init(self, unique_ptr[CScanTask]& up):
        self.wrapped = move(up)
        self.task = self.wrapped.get()

    @staticmethod
    cdef wrap(unique_ptr[CScanTask]& up):
        cdef SimpleScanTask self = SimpleScanTask.__new__(SimpleScanTask)
        self.init(up)
        return self

    def scan(self):
        cdef:
            CRecordBatchIterator it = self.task.Scan()
            shared_ptr[CRecordBatch] record_batch

        while True:
            it.Next(&record_batch)
            if record_batch == nullptr:
                raise StopIteration()
            else:
                yield pyarrow_wrap_batch(record_batch)


cdef class SimpleScanTask(ScanTask):

    cdef:
        CSimpleScanTask* simple_task

    cdef init(self, unique_ptr[CScanTask]& up):
        ScanTask.init(self, up)
        self.simple_task = <CSimpleScanTask*> up.get()


cdef class Scanner:

    cdef:
        unique_ptr[CScanner] wrapped
        CScanner* scanner

    cdef void init(self, unique_ptr[CScanner]& up):
        self.wrapped = move(up)
        self.scanner = self.wrapped.get()

    @staticmethod
    cdef wrap(unique_ptr[CScanner]& up):
        cdef Scanner self = SimpleScanner.__new__(SimpleScanner)
        self.init(up)
        return self

    def scan(self):
        cdef:
            CScanTaskIterator task_iterator = self.scanner.Scan()
            unique_ptr[CScanTask] task

        while True:
            task_iterator.Next(&task)
            if task == nullptr:
                raise StopIteration()
            else:
                yield ScanTask.wrap(task)

    def to_table(self):
        cdef shared_ptr[CTable] table

        with nogil:
            check_status(self.scanner.ToTable(&table))

        return pyarrow_wrap_table(table)


cdef class SimpleScanner(Scanner):

    cdef:
        CSimpleScanner* simple_scanner

    def __init__(self, data_sources, ScanOptions options, ScanContext context):
        cdef:
            DataSource source
            CDataSourceVector sources
            CSimpleScanner* simple_scanner
            unique_ptr[CScanner] scanner

        for source in data_sources:
            sources.push_back(source.unwrap())

        simple_scanner = new CSimpleScanner(
            sources, options.unwrap(), context.unwrap()
        )
        scanner.reset(simple_scanner)
        self.init(scanner)


# cdef class ScannerBuilder:
#     pass
