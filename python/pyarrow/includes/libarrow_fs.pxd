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
from pyarrow.includes.libarrow cimport *


cdef extern from "arrow/filesystem/api.h" namespace "arrow::fs" nogil:

    ctypedef enum CFileType "arrow::fs::FileType":
        CFileType_NotFound "arrow::fs::FileType::NotFound"
        CFileType_Unknown "arrow::fs::FileType::Unknown"
        CFileType_File "arrow::fs::FileType::File"
        CFileType_Directory "arrow::fs::FileType::Directory"

    cdef cppclass CFileInfo "arrow::fs::FileInfo":
        CFileInfo()
        CFileInfo(CFileInfo&&)
        CFileInfo& operator=(CFileInfo&&)
        CFileInfo(const CFileInfo&)
        CFileInfo& operator=(const CFileInfo&)

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

    cdef cppclass CFileSelector "arrow::fs::FileSelector":
        CFileSelector()
        c_string base_dir
        c_bool allow_not_found
        c_bool recursive

    cdef cppclass CFileSystem "arrow::fs::FileSystem":
        shared_ptr[CFileSystem] shared_from_this()
        c_string type_name() const
        CResult[c_string] NormalizePath(c_string path)
        CResult[CFileInfo] GetFileInfo(const c_string& path)
        CResult[vector[CFileInfo]] GetFileInfo(
            const vector[c_string]& paths)
        CResult[vector[CFileInfo]] GetFileInfo(const CFileSelector& select)
        CStatus CreateDir(const c_string& path, c_bool recursive)
        CStatus DeleteDir(const c_string& path)
        CStatus DeleteFile(const c_string& path)
        CStatus DeleteFiles(const vector[c_string]& paths)
        CStatus Move(const c_string& src, const c_string& dest)
        CStatus CopyFile(const c_string& src, const c_string& dest)
        CResult[shared_ptr[CInputStream]] OpenInputStream(
            const c_string& path)
        CResult[shared_ptr[CRandomAccessFile]] OpenInputFile(
            const c_string& path)
        CResult[shared_ptr[COutputStream]] OpenOutputStream(
            const c_string& path)
        CResult[shared_ptr[COutputStream]] OpenAppendStream(
            const c_string& path)
        c_bool Equals(const CFileSystem& other)
        c_bool Equals(shared_ptr[CFileSystem] other)

    CResult[shared_ptr[CFileSystem]] CFileSystemFromUri \
        "arrow::fs::FileSystemFromUri"(const c_string& uri, c_string* out_path)
    CResult[shared_ptr[CFileSystem]] CFileSystemFromUriOrPath \
        "arrow::fs::FileSystemFromUriOrPath"(const c_string& uri,
                                             c_string* out_path)

    cdef cppclass CLocalFileSystemOptions "arrow::fs::LocalFileSystemOptions":
        c_bool use_mmap

        @staticmethod
        CLocalFileSystemOptions Defaults()

        c_bool Equals(const CLocalFileSystemOptions& other)

    cdef cppclass CLocalFileSystem "arrow::fs::LocalFileSystem"(CFileSystem):
        CLocalFileSystem()
        CLocalFileSystem(CLocalFileSystemOptions)
        CLocalFileSystemOptions options()

    cdef cppclass CSubTreeFileSystem \
            "arrow::fs::SubTreeFileSystem"(CFileSystem):
        CSubTreeFileSystem(const c_string& base_path,
                           shared_ptr[CFileSystem] base_fs)
        c_string base_path()
        shared_ptr[CFileSystem] base_fs()

    ctypedef enum CS3LogLevel "arrow::fs::S3LogLevel":
        CS3LogLevel_Off "arrow::fs::S3LogLevel::Off"
        CS3LogLevel_Fatal "arrow::fs::S3LogLevel::Fatal"
        CS3LogLevel_Error "arrow::fs::S3LogLevel::Error"
        CS3LogLevel_Warn "arrow::fs::S3LogLevel::Warn"
        CS3LogLevel_Info "arrow::fs::S3LogLevel::Info"
        CS3LogLevel_Debug "arrow::fs::S3LogLevel::Debug"
        CS3LogLevel_Trace "arrow::fs::S3LogLevel::Trace"

    cdef struct CS3GlobalOptions "arrow::fs::S3GlobalOptions":
        CS3LogLevel log_level

    cdef cppclass CS3Options "arrow::fs::S3Options":
        c_string region
        c_string endpoint_override
        c_string scheme
        c_bool background_writes
        void ConfigureDefaultCredentials()
        void ConfigureAccessKey(const c_string& access_key,
                                const c_string& secret_key)
        c_string GetAccessKey()
        c_string GetSecretKey()
        c_bool Equals(const CS3Options& other)

        @staticmethod
        CS3Options Defaults()
        @staticmethod
        CS3Options FromAccessKey(const c_string& access_key,
                                 const c_string& secret_key)

    cdef cppclass CS3FileSystem "arrow::fs::S3FileSystem"(CFileSystem):
        @staticmethod
        CResult[shared_ptr[CS3FileSystem]] Make(const CS3Options& options)
        CS3Options options()

    cdef CStatus CInitializeS3 "arrow::fs::InitializeS3"(
        const CS3GlobalOptions& options)
    cdef CStatus CFinalizeS3 "arrow::fs::FinalizeS3"()

    cdef cppclass CHdfsOptions "arrow::fs::HdfsOptions":
        HdfsConnectionConfig connection_config
        int32_t buffer_size
        int16_t replication
        int64_t default_block_size
        @staticmethod
        CResult[CHdfsOptions] FromUriString "FromUri"(
            const c_string& uri_string)
        void ConfigureEndPoint(c_string host, int port)
        void ConfigureDriver(c_bool use_hdfs3)
        void ConfigureReplication(int16_t replication)
        void ConfigureUser(c_string user_name)
        void ConfigureBufferSize(int32_t buffer_size)
        void ConfigureBlockSize(int64_t default_block_size)
        void ConfigureKerberosTicketCachePath(c_string path)

    cdef cppclass CHadoopFileSystem "arrow::fs::HadoopFileSystem"(CFileSystem):
        @staticmethod
        CResult[shared_ptr[CHadoopFileSystem]] Make(
            const CHdfsOptions& options)
        CHdfsOptions options()

    cdef cppclass CMockFileSystem "arrow::fs::internal::MockFileSystem"(
            CFileSystem):
        CMockFileSystem(CTimePoint current_time)
