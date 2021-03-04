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

    If neither access_key nor secret_key are provided, and role_arn is also not
    provided, then attempts to initialize from AWS environment variables,
    otherwise both access_key and secret_key must be provided.

    If role_arn is provided instead of access_key and secret_key, temporary
    credentials will be fetched by issuing a request to STS to assume the
    specified role.

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
    session_token: str, default None
        AWS Session Token.  An optional session token, required if access_key
        and secret_key are temporary credentials from STS.
    anonymous: boolean, default False
        Whether to connect anonymously if access_key and secret_key are None.
        If true, will not attempt to look up credentials using standard AWS
        configuration methods.
    role_arn: str, default None
        AWS Role ARN.  If provided instead of access_key and secret_key,
        temporary credentials will be fetched by assuming this role.
    session_name: str, default None
        An optional identifier for the assumed role session.
    external_id: str, default None
        An optional unique identifier that might be required when you assume
        a role in another account.
    load_frequency: int, default 900
        The frequency (in seconds) with which temporary credentials from an
        assumed role session will be refreshed.
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

    def __init__(self, *, access_key=None, secret_key=None, session_token=None,
                 anonymous=False, region=None, scheme=None,
                 endpoint_override=None, bint background_writes=True,
                 role_arn=None, session_name=None, external_id=None,
                 load_frequency=900):
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

        elif session_token is not None and (access_key is None or
                                            secret_key is None):
            raise ValueError(
                'In order to initialize a session with temporary credentials, '
                'both secret_key and access_key must be provided in addition '
                'to session_token.'
            )

        elif (access_key is not None or secret_key is not None):
            if anonymous:
                raise ValueError(
                    'Cannot pass anonymous=True together with access_key '
                    'and secret_key.')

            if role_arn:
                raise ValueError(
                    'Cannot provide role_arn with access_key and secret_key')

            if session_token is None:
                session_token = ""

            options = CS3Options.FromAccessKey(
                tobytes(access_key),
                tobytes(secret_key),
                tobytes(session_token)
            )
        elif anonymous:
            options = CS3Options.Anonymous()
        elif role_arn is not None:
            options = CS3Options.FromAssumeRole(
                tobytes(role_arn),
                tobytes(session_name),
                tobytes(external_id),
                load_frequency
            )
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

        role_arn = frombytes(opts.role_arn)

        # if role_arn is set, we should not re-use temporary credentials
        # but instead recreate a new assume role session
        if role_arn:
            access_key = None
            secret_key = None
            session_token = None
        else:
            access_key = frombytes(opts.GetAccessKey())
            secret_key = frombytes(opts.GetSecretKey())
            session_token = frombytes(opts.GetSessionToken())

        return (
            S3FileSystem._reconstruct, (dict(
                access_key=access_key,
                secret_key=secret_key,
                session_token=session_token,
                region=frombytes(opts.region),
                scheme=frombytes(opts.scheme),
                endpoint_override=frombytes(opts.endpoint_override),
                role_arn=role_arn,
                session_name=frombytes(opts.session_name),
                external_id=frombytes(opts.external_id),
                load_frequency=opts.load_frequency,
                background_writes=opts.background_writes
            ),)
        )

    @property
    def region(self):
        """
        The AWS region this filesystem connects to.
        """
        return frombytes(self.s3fs.region())
