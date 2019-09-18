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
from pyarrow.lib import _detect_compression
from pyarrow.lib cimport *


cdef class S3FileSystem(FileSystem):

    cdef:
        CS3FileSystem* s3fs

    def __init__(self, str access_key, str secret_key, str region='us-east-1',
                 str scheme='https', str endpoint_override=None):
        cdef:
            CS3Options options
            shared_ptr[CS3FileSystem] wrapped

        options.access_key = tobytes(access_key)
        options.secret_key = tobytes(secret_key)
        options.region = tobytes(region)
        options.scheme = tobytes(scheme)
        if endpoint_override is not None:
            options.endpoint_override = endpoint_override

        check_status(CS3FileSystem.Make(options, &wrapped))
        self.init(<shared_ptr[CFileSystem]> wrapped)

    cdef init(self, const shared_ptr[CFileSystem]& wrapped):
        FileSystem.init(self, wrapped)
        self.s3fs = <CS3FileSystem*> wrapped.get()
