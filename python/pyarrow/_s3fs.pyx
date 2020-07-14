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

from pyarrow.lib cimport check_status
from pyarrow.lib import frombytes, tobytes
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


cdef class S3FileSystem(FileSystem):
    """S3-backed FileSystem implementation

    If neither access_key nor secret_key are provided then attempts to
    initialize from AWS environment variables, otherwise both access_key and
    secret_key must be provided.

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
    anonymous: boolean, default False
        Whether to connect anonymously if access_key and secret_key are None.
        If true, will not attempt to look up credentials using standard AWS
        configuration methods.
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

    def __init__(self, *, access_key=None, secret_key=None, anonymous=False,
                 region=None, scheme=None, endpoint_override=None,
                 bint background_writes=True):
        cdef:
            CS3Options options
            shared_ptr[CS3FileSystem] wrapped

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
            if anonymous:
                raise ValueError(
                    'Cannot pass anonymous=True together with access_key '
                    'and secret_key.')
            options = CS3Options.FromAccessKey(
                tobytes(access_key),
                tobytes(secret_key)
            )
        elif anonymous:
            options = CS3Options.Anonymous()
        else:
            options = CS3Options.Defaults()

        if region is not None:
            options.region = tobytes(region)
        if scheme is not None:
            options.scheme = tobytes(scheme)
        if endpoint_override is not None:
            options.endpoint_override = tobytes(endpoint_override)
        if background_writes is not None:
            options.background_writes = background_writes

        with nogil:
            wrapped = GetResultValue(CS3FileSystem.Make(options))

        self.init(<shared_ptr[CFileSystem]> wrapped)

    cdef init(self, const shared_ptr[CFileSystem]& wrapped):
        FileSystem.init(self, wrapped)
        self.s3fs = <CS3FileSystem*> wrapped.get()

    @classmethod
    def _reconstruct(cls, kwargs):
        return cls(**kwargs)

    def __reduce__(self):
        cdef CS3Options opts = self.s3fs.options()
        return (
            S3FileSystem._reconstruct, (dict(
                access_key=frombytes(opts.GetAccessKey()),
                secret_key=frombytes(opts.GetSecretKey()),
                region=frombytes(opts.region),
                scheme=frombytes(opts.scheme),
                endpoint_override=frombytes(opts.endpoint_override),
                background_writes=opts.background_writes
            ),)
        )
