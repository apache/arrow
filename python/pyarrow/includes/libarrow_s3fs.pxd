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
from pyarrow.includes.libarrow cimport CFileSystem

cdef extern from "arrow/filesystem/api.h" namespace "arrow::fs" nogil:

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

        @staticmethod
        CS3Options Defaults()
        @staticmethod
        CS3Options FromAccessKey(const c_string& access_key,
                                 const c_string& secret_key)

    cdef cppclass CS3FileSystem "arrow::fs::S3FileSystem"(CFileSystem):
        @staticmethod
        CStatus Make(const CS3Options& options, shared_ptr[CS3FileSystem]* out)

    cdef CStatus CInitializeS3 "arrow::fs::InitializeS3"(
        const CS3GlobalOptions& options)
    cdef CStatus CFinalizeS3 "arrow::fs::FinalizeS3"()
