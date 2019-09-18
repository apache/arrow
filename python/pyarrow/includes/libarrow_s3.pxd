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

    cdef struct CS3Options "arrow::fs::S3Options":
        c_string region
        c_string endpoint_override
        c_string scheme
        c_string access_key
        c_string secret_key

    cdef cppclass CS3FileSystem "arrow::fs::S3FileSystem"(CFileSystem):
        @staticmethod
        CStatus Make(const CS3Options& options, shared_ptr[CS3FileSystem]* out)
