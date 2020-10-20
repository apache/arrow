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

import os

import pyarrow as pa
from pyarrow.lib cimport *
from pyarrow.lib import frombytes, tobytes
from pyarrow.includes.libarrow_dataset cimport *
from pyarrow._fs cimport FileSystem, FileInfo, FileSelector
from pyarrow._csv cimport ParseOptions
from pyarrow.util import _is_path_like, _stringify_path

from pyarrow._parquet cimport (
    _create_writer_properties, _create_arrow_writer_properties,
    FileMetaData, RowGroupMetaData, ColumnChunkMetaData
)

cdef class Dataset(_Weakrefable):
    """
    Collection of data fragments and potentially child datasets.
    Arrow Datasets allow you to query against data that has been split across
    multiple files. This sharding of data may indicate partitioning, which
    can accelerate queries that only touch some partitions (files).
    """

    cdef:
        shared_ptr[CDataset] wrapped
        CDataset* dataset

    cdef void init(self, const shared_ptr[CDataset]& sp)

    @staticmethod
    cdef wrap(const shared_ptr[CDataset]& sp)

    cdef shared_ptr[CDataset] unwrap(self) nogil


cdef class FileFormat(_Weakrefable):

    cdef:
        shared_ptr[CFileFormat] wrapped
        CFileFormat* format

    cdef void init(self, const shared_ptr[CFileFormat]& sp)

    @staticmethod
    cdef wrap(const shared_ptr[CFileFormat]& sp)

    cdef inline shared_ptr[CFileFormat] unwrap(self) nogil
