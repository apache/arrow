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

from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow_dataset cimport *
from pyarrow.lib cimport *


cdef class Expression(_Weakrefable):

    cdef:
        CExpression expr


cdef class FragmentScanOptions(_Weakrefable):

    cdef:
        shared_ptr[CFragmentScanOptions] wrapped

    cdef void init(self, const shared_ptr[CFragmentScanOptions]& sp)

    @staticmethod
    cdef wrap(const shared_ptr[CFragmentScanOptions]& sp)


cdef class FileFormat(_Weakrefable):

    cdef:
        shared_ptr[CFileFormat] wrapped
        CFileFormat* format

    cdef void init(self, const shared_ptr[CFileFormat]& sp)

    @staticmethod
    cdef wrap(const shared_ptr[CFileFormat]& sp)

    cdef inline shared_ptr[CFileFormat] unwrap(self)

    cdef _set_default_fragment_scan_options(self, FragmentScanOptions options)


cdef class Partitioning(_Weakrefable):

    cdef:
        shared_ptr[CPartitioning] wrapped
        CPartitioning* partitioning

    @staticmethod
    cdef wrap(const shared_ptr[CPartitioning]& sp):


cdef class PartitioningFactory(_Weakrefable):

    cdef:
        shared_ptr[CPartitioningFactory] wrapped
        CPartitioningFactory* factory

    @staticmethod
    cdef wrap(const shared_ptr[CPartitioningFactory]& sp):


cdef class WrittenFile(_Weakrefable):

    cdef public str path
    cdef public object metadata

    def __init__(self, path, metadata):
        self.path = path
        self.metadata = metadata
