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

import sys

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self
from typing import IO, Any, Literal, overload

from _typeshed import StrPath

from . import _orc
from ._fs import SupportedFileSystem
from .lib import KeyValueMetadata, NativeFile, RecordBatch, Schema, Table


class ORCFile:

    reader: _orc.ORCReader
    def __init__(self, source: StrPath | NativeFile | IO) -> None: ...
    @property
    def metadata(self) -> KeyValueMetadata: ...

    @property
    def schema(self) -> Schema: ...

    @property
    def nrows(self) -> int: ...

    @property
    def nstripes(self) -> int: ...

    @property
    def file_version(self) -> str: ...

    @property
    def software_version(self) -> str: ...

    @property
    def compression(self) -> Literal["UNCOMPRESSED",
                                     "ZLIB", "SNAPPY", "LZ4", "ZSTD"]: ...

    @property
    def compression_size(self) -> int: ...

    @property
    def writer(self) -> str: ...

    @property
    def writer_version(self) -> str: ...

    @property
    def row_index_stride(self) -> int: ...

    @property
    def nstripe_statistics(self) -> int: ...

    @property
    def content_length(self) -> int: ...

    @property
    def stripe_statistics_length(self) -> int: ...

    @property
    def file_footer_length(self) -> int: ...

    @property
    def file_postscript_length(self) -> int: ...

    @property
    def file_length(self) -> int: ...

    def read_stripe(self, n: int, columns: list[str | int] | None = None) -> RecordBatch: ...

    def read(self, columns: list[str | int] | None = None) -> Table: ...


class ORCWriter:

    writer: _orc.ORCWriter
    is_open: bool

    def __init__(
        self,
        where: StrPath | NativeFile | IO,
        *,
        file_version: Any = "0.12",
        batch_size: Any = 1024,
        stripe_size: Any = 64 * 1024 * 1024,  # noqa: Y011
        compression: Any = "UNCOMPRESSED",
        compression_block_size: Any = 65536,
        compression_strategy: Any = "SPEED",
        row_index_stride: Any = 10000,
        padding_tolerance: Any = 0.0,
        dictionary_key_size_threshold: Any = 0.0,
        bloom_filter_columns: Any = None,
        bloom_filter_fpp: Any = 0.05,
    ): ...
    def __enter__(self) -> Self: ...
    def __exit__(self, *args, **kwargs) -> None: ...
    def __getattr__(self, name: str) -> Any: ...
    def write(self, table: Table) -> None: ...

    def close(self) -> None: ...


def read_table(
    source: StrPath | NativeFile | IO,
    columns: list[str | int] | None = None,
    filesystem: SupportedFileSystem | str | None = None,
) -> Table: ...

# TODO: should not use Any here?
@overload
def write_table(
    table: Table,
    where: StrPath | NativeFile | IO,
    *,
    file_version: Any = "0.12",
    batch_size: Any = 1024,
    stripe_size: Any = 64 * 1024 * 1024,  # noqa: Y011
    compression: Any = "UNCOMPRESSED",
    compression_block_size: Any = 65536,
    compression_strategy: Any = "SPEED",
    row_index_stride: Any = 10000,
    padding_tolerance: Any = 0.0,
    dictionary_key_size_threshold: Any = 0.0,
    bloom_filter_columns: Any = None,
    bloom_filter_fpp: Any = 0.05,
) -> None: ...

# Deprecated argument order for backward compatibility
@overload
def write_table(
    where: StrPath | NativeFile | IO,
    table: Table,
    *,
    file_version: Any = "0.12",
    batch_size: Any = 1024,
    stripe_size: Any = 64 * 1024 * 1024,  # noqa: Y011
    compression: Any = "UNCOMPRESSED",
    compression_block_size: Any = 65536,
    compression_strategy: Any = "SPEED",
    row_index_stride: Any = 10000,
    padding_tolerance: Any = 0.0,
    dictionary_key_size_threshold: Any = 0.0,
    bloom_filter_columns: Any = None,
    bloom_filter_fpp: Any = 0.05,
) -> None: ...
