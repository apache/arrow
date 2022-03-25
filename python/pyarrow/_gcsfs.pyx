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

from pyarrow.lib cimport (check_status, pyarrow_wrap_metadata,
                          pyarrow_unwrap_metadata)
from pyarrow.lib import frombytes, tobytes, KeyValueMetadata
from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport *
from pyarrow.includes.libarrow_fs cimport *
from pyarrow._fs cimport FileSystem
from cython.operator cimport dereference as deref

from datetime import datetime


cdef class GcsFileSystem(FileSystem):
    """
    GCS-backed FileSystem implementation

    By default uses the process described in https://google.aip.dev/auth/4110
    to resolve credentials. If not running on GCP this generally requires the
    environment variable GOOGLE_APPLICATION_CREDENTIALS to point to a JSON
    file containing credentials.

    Note: GCS buckets are special and the operations available on them may be
    limited or more expensive than expected compared to local file systems.

    Note: When pickling a GcsFileSystem that uses default credential resolution
    credentials are not stored in the serialized data. Therefore, when unpickling
    it is assumed that the necessary credentials are in place for the target
    process.

    Parameters
    ----------
    anonymous : boolean, default False
        Whether to connect anonymously.
        If true, will not attempt to look up credentials using standard GCP
        configuration methods.
    access_token : str, default None
        GCP access token.  If provided temporary credentials will be fetched by
        assuming this role. If specified an credential_token_expiration must be
        specified with the token.
    target_service_account : str, default None
        An optional service account to try to impersonate when accessing GCS. This
        requires the specified credential user/service_account has the necessary
        permissions.
    credential_token_expiration : datetime, default None
        Expiration for credential generated with an access token. Must be specified
        if token is specified.
    default_bucket_location : str, default 'US-CENTRAL1'
        GCP region to create buckets in.
    scheme : str, default 'https'
        GCS connection transport scheme.
    endpoint_override : str, default None
        Override endpoint with a connect string such as "localhost:9000"
    default_metadata : mapping or pyarrow.KeyValueMetadata, default None
        Default metadata for open_output_stream.  This will be ignored if
        non-empty metadata is passed to open_output_stream.
    """

    cdef:
        CGcsFileSystem* gcsfs

    def __init__(self, *, bint anonymous=False, access_token=None,
                 target_service_account=None, credential_token_expiration=None,
                 default_bucket_location='US-CENTRAL1',
                 scheme=None,
                 endpoint_override=None,
                 default_metadata=None):
        cdef:
            CGcsOptions options
            shared_ptr[CGcsFileSystem] wrapped

        # Intentional use of truthiness because empty strings aren't valid and
        # for reconstruction from pickling will give empty strings.
        if anonymous and (target_service_account or access_token):
            raise ValueError(
                'anonymous option is not compatible with target_service_account and '
                'access_token please only specify only one.'
            )
        elif ((access_token and credential_token_expiration is None) or
              (not access_token and
                  credential_token_expiration is not None)):
            raise ValueError(
                'access_token and credential_token_expiration must be '
                'specified together'
            )

        elif anonymous:
            options = CGcsOptions.Anonymous()
        elif access_token:
            options = CGcsOptions.FromAccessToken(
                tobytes(access_token),
                PyDateTime_to_TimePoint(<PyDateTime_DateTime*>credential_token_expiration))
        else:
            options = CGcsOptions.Defaults()

        if target_service_account:
            options = CGcsOptions.FromImpersonatedServiceAccount(
                options.credentials, tobytes(target_service_account))

        options.default_bucket_location = tobytes(default_bucket_location)

        if scheme is not None:
            options.scheme = tobytes(scheme)
        if endpoint_override is not None:
            options.endpoint_override = tobytes(endpoint_override)
        if default_metadata is not None:
            if not isinstance(default_metadata, KeyValueMetadata):
                default_metadata = KeyValueMetadata(default_metadata)
            options.default_metadata = pyarrow_unwrap_metadata(
                default_metadata)

        with nogil:
            wrapped = GetResultValue(CGcsFileSystem.Make(options))

        self.init(<shared_ptr[CFileSystem]> wrapped)

    cdef init(self, const shared_ptr[CFileSystem]& wrapped):
        FileSystem.init(self, wrapped)
        self.gcsfs = <CGcsFileSystem*> wrapped.get()

    @classmethod
    def _reconstruct(cls, kwargs):
        return cls(**kwargs)

    def _expiration_datetime_from_options(self):
        cdef CGcsOptions opts = self.gcsfs.options()
        expiration_ns = TimePoint_to_ns(opts.credentials.expiration())
        if expiration_ns == 0:
            return None
        ns_per_sec = 1000000000.0
        return datetime.fromtimestamp(expiration_ns / ns_per_sec)

    def __reduce__(self):
        cdef CGcsOptions opts = self.gcsfs.options()
        service_account = frombytes(opts.credentials.target_service_account())
        expiration_dt = self._expiration_datetime_from_options()
        return (
            GcsFileSystem._reconstruct, (dict(
                access_token=frombytes(opts.credentials.access_token()),
                anonymous=opts.credentials.anonymous(),
                credential_token_expiration=expiration_dt,
                target_service_account=service_account,
                scheme=frombytes(opts.scheme),
                endpoint_override=frombytes(opts.endpoint_override),
                default_bucket_location=frombytes(
                    opts.default_bucket_location),
                default_metadata=pyarrow_wrap_metadata(opts.default_metadata),
            ),))

    @property
    def default_bucket_location(self):
        """
        The GCP location this filesystem will write to.
        """
        return frombytes(self.gcsfs.options().default_bucket_location)
