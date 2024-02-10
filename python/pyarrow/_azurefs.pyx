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

from cython cimport binding

from pyarrow.lib cimport (check_status, pyarrow_wrap_metadata,
                          pyarrow_unwrap_metadata)
from pyarrow.lib import frombytes, tobytes, KeyValueMetadata, ensure_metadata
from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport *
from pyarrow.includes.libarrow_fs cimport *
from pyarrow._fs cimport FileSystem, TimePoint_to_ns, PyDateTime_to_TimePoint
from cython.operator cimport dereference as deref

from datetime import datetime, timedelta, timezone


cdef class AzureFileSystem(FileSystem):
    cdef:
        CAzureFileSystem* azurefs
        c_string account_key

    def __init__(self, *, account_name, account_key=None, blob_storage_authority=None, 
                 dfs_storage_authority=None, blob_storage_scheme=None, 
                 dfs_storage_scheme=None):
        cdef:
            CAzureOptions options
            shared_ptr[CAzureFileSystem] wrapped

        options.account_name = tobytes(account_name)
        if blob_storage_authority:
            options.blob_storage_authority = tobytes(blob_storage_authority)
        if dfs_storage_authority:
            options.dfs_storage_authority = tobytes(dfs_storage_authority)
        if blob_storage_scheme:
            options.blob_storage_scheme = tobytes(blob_storage_scheme)
        if dfs_storage_scheme:
            options.dfs_storage_scheme = tobytes(dfs_storage_scheme)
        
        if account_key:
            options.ConfigureAccountKeyCredential(tobytes(account_key))
            self.account_key = tobytes(account_key)
        else:
            options.ConfigureDefaultCredential()

        with nogil:
            wrapped = GetResultValue(CAzureFileSystem.Make(options))

        self.init(<shared_ptr[CFileSystem]> wrapped)

    cdef init(self, const shared_ptr[CFileSystem]& wrapped):
        FileSystem.init(self, wrapped)
        self.azurefs = <CAzureFileSystem*> wrapped.get()

    @staticmethod
    @binding(True)  # Required for cython < 3
    def _reconstruct(kwargs):
        # __reduce__ doesn't allow passing named arguments directly to the
        # reconstructor, hence this wrapper.
        return AzureFileSystem(**kwargs)

    def __reduce__(self):
        cdef CAzureOptions opts = self.azurefs.options()
        return (
            AzureFileSystem._reconstruct, (dict(
                account_name=frombytes(opts.account_name),
                account_key=frombytes(self.account_key),
                blob_storage_authority=frombytes(opts.blob_storage_authority),
                dfs_storage_authority=frombytes(opts.dfs_storage_authority),
                blob_storage_scheme=frombytes(opts.blob_storage_scheme),
                dfs_storage_scheme=frombytes(opts.dfs_storage_scheme)
            ),))
