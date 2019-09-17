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

# distutils: language = c++

from libcpp.functional cimport function

from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport (
    InputStream as CInputStream,
    OutputStream as COutputStream,
    RandomAccessFile as CRandomAccessFile
)


cdef extern from "arrow/filesystem/api.h" namespace "arrow::fs" nogil:

    enum CFileType "arrow::fs::FileType":
        CFileType_NonExistent "arrow::fs::FileType::NonExistent"
        CFileType_Unknown "arrow::fs::FileType::Unknown"
        CFileType_File "arrow::fs::FileType::File"
        CFileType_Directory "arrow::fs::FileType::Directory"

    cdef cppclass CTimePoint "arrow::fs::TimePoint":
        pass

    cdef cppclass CFileStats "arrow::fs::FileStats":
        CFileStats()
        CFileStats(CFileStats&&)
        CFileStats& operator=(CFileStats&&)
        CFileStats(const CFileStats&)
        CFileStats& operator=(const CFileStats&)

        CFileType type()
        void set_type(CFileType type)
        c_string path()
        void set_path(const c_string& path)
        c_string base_name()
        int64_t size()
        void set_size(int64_t size)
        c_string extension()
        CTimePoint mtime()
        void set_mtime(CTimePoint mtime)

    cdef cppclass CSelector "arrow::fs::Selector":
        CSelector()
        c_string base_dir
        c_bool allow_non_existent
        c_bool recursive

    cdef cppclass CFileSystem "arrow::fs::FileSystem":
        CStatus GetTargetStats(const c_string& path, CFileStats* out)
        CStatus GetTargetStats(const vector[c_string]& paths,
                               vector[CFileStats]* out)
        CStatus GetTargetStats(const CSelector& select,
                               vector[CFileStats]* out)
        CStatus CreateDir(const c_string& path, c_bool recursive)
        CStatus DeleteDir(const c_string& path)
        CStatus DeleteFile(const c_string& path)
        CStatus DeleteFiles(const vector[c_string]& paths)
        CStatus Move(const c_string& src, const c_string& dest)
        CStatus CopyFile(const c_string& src, const c_string& dest)
        CStatus OpenInputStream(const c_string& path,
                                shared_ptr[CInputStream]* out)
        CStatus OpenInputFile(const c_string& path,
                              shared_ptr[CRandomAccessFile]* out)
        CStatus OpenOutputStream(const c_string& path,
                                 shared_ptr[COutputStream]* out)
        CStatus OpenAppendStream(const c_string& path,
                                 shared_ptr[COutputStream]* out)

    cdef cppclass CLocalFileSystem "arrow::fs::LocalFileSystem"(CFileSystem):
        LocalFileSystem()

    cdef cppclass CSubTreeFileSystem \
            "arrow::fs::SubTreeFileSystem"(CFileSystem):
        CSubTreeFileSystem(const c_string& base_path,
                           shared_ptr[CFileSystem] base_fs)
