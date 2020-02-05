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

from pyarrow.lib cimport check_status
from pyarrow.compat import frombytes, tobytes
from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport *
from pyarrow.includes.libarrow_fs cimport *
from pyarrow._fs cimport FileSystem


cpdef enum S3LogLevel:
    Off = <int8_t> CS3LogLevel_Off
    Fatal = <int8_t> CS3LogLevel_Fatal
    Error = <int8_t> CS3LogLevel_Error
    Warn = <int8_t> CS3LogLevel_Warn
    Info = <int8_t> CS3LogLevel_Info
    Debug = <int8_t> CS3LogLevel_Debug
    Trace = <int8_t> CS3LogLevel_Trace


def initialize_s3(S3LogLevel log_level=S3LogLevel.Fatal):
    cdef CS3GlobalOptions options
    options.log_level = <CS3LogLevel> log_level
    check_status(CInitializeS3(options))


def finalize_s3():
    check_status(CFinalizeS3())


cdef class S3Options:
    """Options for S3FileSystem.

    If neither access_key nor secret_key are provided then attempts to
    initialize from AWS environment variables, otherwise both access_key and
    secret_key must be provided.

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
        CS3Options options

    # Avoid mistakingly creating attributes
    __slots__ = ()

    def __init__(self, access_key=None, secret_key=None, region=None,
                 scheme=None, endpoint_override=None, background_writes=None):
        if access_key is not None and secret_key is None:
            raise ValueError(
                'In order to initialize with explicit credentials both '
                'access_key and secret_key must be provided, '
                '`secret_key` is not set.'
            )
        elif access_key is None and secret_key is not None:
            raise ValueError(
                'In order to initialize with explicit credentials both '
                'access_key and secret_key must be provided, '
                '`access_key` is not set.'
            )
        elif access_key is not None or secret_key is not None:
            self.options = CS3Options.FromAccessKey(
                tobytes(access_key),
                tobytes(secret_key)
            )
        else:
            self.options = CS3Options.Defaults()

        if region is not None:
            self.region = region
        if scheme is not None:
            self.scheme = scheme
        if endpoint_override is not None:
            self.endpoint_override = endpoint_override
        if background_writes is not None:
            self.background_writes = background_writes

    cdef inline CS3Options unwrap(self) nogil:
        return self.options

    @property
    def region(self):
        """AWS region to connect to."""
        return frombytes(self.options.region)

    @region.setter
    def region(self, value):
        self.options.region = tobytes(value)

    @property
    def scheme(self):
        """S3 connection transport scheme."""
        return frombytes(self.options.scheme)

    @scheme.setter
    def scheme(self, value):
        self.options.scheme = tobytes(value)

    @property
    def endpoint_override(self):
        """Override region with a connect string such as localhost:9000"""
        return frombytes(self.options.endpoint_override)

    @endpoint_override.setter
    def endpoint_override(self, value):
        self.options.endpoint_override = tobytes(value)

    @property
    def background_writes(self):
        """OutputStream writes will be issued in the background"""
        return self.options.background_writes

    @background_writes.setter
    def background_writes(self, bint value):
        self.options.background_writes = value


cdef class S3FileSystem(FileSystem):
    """S3-backed FileSystem implementation

    Note: S3 buckets are special and the operations available on them may be
    limited or more expensive than desired.

    Parameters
    ----------
    options: S3Options, default None
        Options for connecting to S3. If None is passed then attempts to
        initialize the connection from AWS environment variables.
    """

    cdef:
        CS3FileSystem* s3fs

    def __init__(self, S3Options options=None):
        cdef shared_ptr[CS3FileSystem] wrapped
        options = options or S3Options()
        with nogil:
            wrapped = GetResultValue(CS3FileSystem.Make(options.unwrap()))
        self.init(<shared_ptr[CFileSystem]> wrapped)

    cdef init(self, const shared_ptr[CFileSystem]& wrapped):
        FileSystem.init(self, wrapped)
        self.s3fs = <CS3FileSystem*> wrapped.get()
