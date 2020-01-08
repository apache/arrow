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

import six

from pyarrow.compat import frombytes, tobytes
from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport PyDateTime_from_TimePoint
from pyarrow.includes.libarrow_fs cimport *
from pyarrow.lib import _detect_compression
from pyarrow.lib cimport *


cpdef enum FileType:
    NonExistent = <int8_t> CFileType_NonExistent
    Unknown = <int8_t> CFileType_Unknown
    File = <int8_t> CFileType_File
    Directory = <int8_t> CFileType_Directory


cdef class FileStats:
    cdef:
        CFileStats stats

    @staticmethod
    cdef wrap(CFileStats stats)

    cdef inline CFileStats unwrap(self) nogil


cdef class FileSelector:
    cdef:
        CFileSelector selector

    cdef inline CFileSelector unwrap(self) nogil


cdef class FileSystem:
    cdef:
        shared_ptr[CFileSystem] wrapped
        CFileSystem* fs

    cdef init(self, const shared_ptr[CFileSystem]& wrapped)

    @staticmethod
    cdef wrap(shared_ptr[CFileSystem]& sp)

    cdef inline shared_ptr[CFileSystem] unwrap(self) nogil


cdef class LocalFileSystem(FileSystem):
    cdef:
        CLocalFileSystem* localfs

    cdef init(self, const shared_ptr[CFileSystem]& wrapped)


cdef class SubTreeFileSystem(FileSystem):
    cdef:
        CSubTreeFileSystem* subtreefs

    cdef init(self, const shared_ptr[CFileSystem]& wrapped)


cdef class _MockFileSystem(FileSystem):
    cdef:
        CMockFileSystem* mockfs

    cdef init(self, const shared_ptr[CFileSystem]& wrapped)
