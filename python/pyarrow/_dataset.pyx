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
from pyarrow.lib cimport RecordBatch, MemoryPool, Schema, check_status
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


cdef class ScanTask:

    cdef:
        unique_ptr[CScanTask] wrapped
        CScanTask* task

    cdef init(self, unique_ptr[CScanTask]& sp):
        self.wrapped = move(sp)
        self.task = self.wrapped.get()

    @staticmethod
    cdef ScanTask wrap(unique_ptr[CScanTask]& sp):
        cdef ScanTask self = ScanTask.__new__(ScanTask)
        self.init(sp)
        return self


cdef class SimpleScanTask(ScanTask):

    cdef:
        CSimpleScanTask* simple_task

    cdef init(self, unique_ptr[CScanTask]& sp):
        ScanTask.init(self, sp)
        self.simple_task = <CSimpleScanTask*> sp.get()


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

    def tasks(self, ScanContext context=None):
        cdef:
            unique_ptr[CScanTaskIterator] task_iterator
            unique_ptr[CScanTask] task

        context = context or ScanContext()
        check_status(self.fragment.Scan(context.wrapped, &task_iterator))

        while True:
            task_iterator.get().Next(&task)
            if task == nullptr:
                raise StopIteration()
            else:
                yield ScanTask.wrap(task)

    scan = tasks

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


cdef class DataSource:

    cdef:
        shared_ptr[CDataSource] wrapped
        CDataSource* source

    cdef void init(self, const shared_ptr[CDataSource]& sp):
        self.wrapped = sp
        self.source = sp.get()

    @property
    def type(self):
        return frombytes(self.source.type())

    def fragments(self, ScanOptions options=None):
        cdef:
            unique_ptr[CDataFragmentIterator] fragment_iterator
            shared_ptr[CDataFragment] fragment

        options = options or ScanOptions()

        # NOTE(kszucs): shouldn't we return Status here?
        fragment_iterator = self.source.GetFragments(options.wrapped)

        while True:
            fragment_iterator.get().Next(&fragment)
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


# cdef class Dataset:
#
#     cdef:
#         shared_ptr[CDataset] wrapped
#         CDataset* dataset
#
#     def __init__(self, data_sources, Schema schema=None):
#         cdef:
#             DataSource source
#             vector[shared_ptr[CDataSource]] sources
#             shared_ptr[CDataset] dataset
#
#         for source in data_sources:
#             sources.push_back(source.wrapped)
#
#         if schema is None:
#             dataset = make_shared[CDataset](sources)
#         else:
#             dataset = make_shared[CDataset](sources, schema.sp_schema)
#
#         self.init(dataset)
#
#     cdef void init(self, const shared_ptr[CDataset]& sp):
#         self.wrapped = sp
#         self.dataset = sp.get()
#
#     def new_scan(self):
#         pass
#
#     @property
#     def sources(self):
#         pass
#
#     @property
#     def schema(self):
#         pass
#
#     def infer_schema(self):
#         pass
#
#     def replace_schema(self, Schema new_schema):
#         pass
#
#     with_schema = replace_schema
