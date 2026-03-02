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

import enum

from typing import Literal, TypedDict
from typing_extensions import Required, NotRequired

from ._fs import FileSystem
from .lib import KeyValueMetadata


class _ProxyOptions(TypedDict):
    scheme: Required[Literal["http", "https"]]
    host: Required[str]
    port: Required[int]
    username: NotRequired[str]
    password: NotRequired[str]


class S3LogLevel(enum.IntEnum):
    Off = enum.auto()
    Fatal = enum.auto()
    Error = enum.auto()
    Warn = enum.auto()
    Info = enum.auto()
    Debug = enum.auto()
    Trace = enum.auto()


Off = S3LogLevel.Off
Fatal = S3LogLevel.Fatal
Error = S3LogLevel.Error
Warn = S3LogLevel.Warn
Info = S3LogLevel.Info
Debug = S3LogLevel.Debug
Trace = S3LogLevel.Trace


def initialize_s3(
    log_level: S3LogLevel = S3LogLevel.Fatal, num_event_loop_threads: int = 1
) -> None: ...
def ensure_s3_initialized() -> None: ...
def finalize_s3() -> None: ...
def ensure_s3_finalized() -> None: ...
def resolve_s3_region(bucket: str) -> str: ...


class S3RetryStrategy:
    max_attempts: int
    def __init__(self, max_attempts=3) -> None: ...


class AwsStandardS3RetryStrategy(S3RetryStrategy):
    ...


class AwsDefaultS3RetryStrategy(S3RetryStrategy):
    ...


class S3FileSystem(FileSystem):
    def __init__(
        self,
        *,
        access_key: str | None = None,
        secret_key: str | None = None,
        session_token: str | None = None,
        anonymous: bool = False,
        region: str | None = None,
        request_timeout: float | None = None,
        connect_timeout: float | None = None,
        scheme: Literal["http", "https"] = "https",
        endpoint_override: str | None = None,
        background_writes: bool = True,
        default_metadata: dict | list | KeyValueMetadata | None = None,
        role_arn: str | None = None,
        session_name: str | None = None,
        external_id: str | None = None,
        load_frequency: int = 900,
        proxy_options: _ProxyOptions | dict | tuple | str | None = None,
        allow_bucket_creation: bool = False,
        allow_bucket_deletion: bool = False,
        allow_delayed_open: bool = False,
        check_directory_existence_before_creation: bool = False,
        tls_ca_file_path: str | None = None,
        retry_strategy: S3RetryStrategy =
        AwsStandardS3RetryStrategy(max_attempts=3),  # noqa: Y011
        force_virtual_addressing: bool = False,
    ): ...
    @property
    def region(self) -> str: ...
