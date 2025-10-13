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

from pyarrow._fs import (
    FileSelector,
    FileType,
    FileInfo,
    FileSystem,
    LocalFileSystem,
    SubTreeFileSystem,
    _MockFileSystem,
    FileSystemHandler,
    PyFileSystem,
    SupportedFileSystem,
)
from pyarrow._azurefs import AzureFileSystem
from pyarrow._hdfs import HadoopFileSystem
from pyarrow._gcsfs import GcsFileSystem
from pyarrow._s3fs import (
    AwsDefaultS3RetryStrategy,
    AwsStandardS3RetryStrategy,
    S3FileSystem,
    S3LogLevel,
    S3RetryStrategy,
    ensure_s3_initialized,
    finalize_s3,
    ensure_s3_finalized,
    initialize_s3,
    resolve_s3_region,
)

FileStats = FileInfo


def copy_files(
    source: str,
    destination: str,
    source_filesystem: SupportedFileSystem | None = None,
    destination_filesystem: SupportedFileSystem | None = None,
    *,
    chunk_size: int = 1024 * 1024,  # noqa: Y011
    use_threads: bool = True,
) -> None: ...


def _ensure_filesystem(
    filesystem: FileSystem | str | object,
    *,
    use_mmap: bool = False
) -> FileSystem: ...


def _resolve_filesystem_and_path(
    path: str | object,
    filesystem: FileSystem | str | object | None = None,
    *,
    memory_map: bool = False
) -> tuple[FileSystem | None, str | object]: ...


class FSSpecHandler(FileSystemHandler):  # type: ignore[misc]
    fs: SupportedFileSystem
    def __init__(self, fs: SupportedFileSystem) -> None: ...


__all__ = [
    # _fs
    "FileSelector",
    "FileType",
    "FileInfo",
    "FileSystem",
    "LocalFileSystem",
    "SubTreeFileSystem",
    "_MockFileSystem",
    "FileSystemHandler",
    "PyFileSystem",
    # _azurefs
    "AzureFileSystem",
    # _hdfs
    "HadoopFileSystem",
    # _gcsfs
    "GcsFileSystem",
    # _s3fs
    "AwsDefaultS3RetryStrategy",
    "AwsStandardS3RetryStrategy",
    "S3FileSystem",
    "S3LogLevel",
    "S3RetryStrategy",
    "ensure_s3_initialized",
    "finalize_s3",
    "ensure_s3_finalized",
    "initialize_s3",
    "resolve_s3_region",
    # fs
    "FileStats",
    "copy_files",
    "FSSpecHandler",
]
