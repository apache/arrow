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
from pyarrow.includes.libarrow cimport *
from pyarrow.includes.libarrow_s3 cimport *
from pyarrow._fs cimport FileSystem
from pyarrow.lib cimport check_status


cpdef enum S3LogLevel:
    Off = <int8_t> CS3LogLevel_Off
    Fatal = <int8_t> CS3LogLevel_Fatal
    Error = <int8_t> CS3LogLevel_Error
    Warn = <int8_t> CS3LogLevel_Warn
    Info = <int8_t> CS3LogLevel_Info
    Debug = <int8_t> CS3LogLevel_Debug
    Trace = <int8_t> CS3LogLevel_Trace


def initialize_s3(S3LogLevel log_level=S3LogLevel.Error):
    cdef CS3GlobalOptions options
    options.log_level = <CS3LogLevel> log_level
    check_status(CInitializeS3(options))


def finalize_s3():
    check_status(CFinalizeS3())


cdef class S3FileSystem(FileSystem):
    """S3-backed FileSystem implementation

    Note: S3 buckets are special and the operations available on them may be
          limited or more expensive than desired.

    Parameters
    ----------
    access_key: str, default None
        AWS Access Key ID. Pass None to use the standard AWS environment
        variables and/or configuration file.
    secret_key: str, default None
        AWS Secret Access key. Pass None to use the standard AWS environment
        variables and/or configuration file.
    region: str, default 'us-east-1'
        AWS region to connect to.
    scheme: str, default 'https'
        S3 connection transport scheme.
    endpoint_override: str, default None
        Override region with a connect string such as "localhost:9000"
    background_writes: boolean, default True
        Whether OutputStream writes will be issued in the background, without
        blocking.
    """

    cdef:
        CS3FileSystem* s3fs

    def __init__(self, access_key=None, secret_key=None, region='us-east-1',
                 scheme='https', endpoint_override=None,
                 bint background_writes=True):
        cdef:
            CS3Options options = CS3Options.Defaults()
            shared_ptr[CS3FileSystem] wrapped

        if access_key is not None or secret_key is not None:
            options.ConfigureAccessKey(tobytes(access_key),
                                       tobytes(secret_key))

        options.region = tobytes(region)
        options.scheme = tobytes(scheme)
        options.background_writes = background_writes
        if endpoint_override is not None:
            options.endpoint_override = tobytes(endpoint_override)

        check_status(CS3FileSystem.Make(options, &wrapped))
        self.init(<shared_ptr[CFileSystem]> wrapped)

    cdef init(self, const shared_ptr[CFileSystem]& wrapped):
        FileSystem.init(self, wrapped)
        self.s3fs = <CS3FileSystem*> wrapped.get()
