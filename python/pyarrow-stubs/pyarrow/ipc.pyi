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

from io import IOBase
from typing import Any

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

from _typeshed import StrPath
import pandas as pd
import pyarrow.lib as lib

from pyarrow.lib import (
    Alignment,
    IpcReadOptions,
    IpcWriteOptions,
    Message,
    MessageReader,
    MetadataVersion,
    ReadStats,
    RecordBatchReader,
    WriteStats,
    _ReadPandasMixin,
    get_record_batch_size,
    get_tensor_size,
    read_message,
    read_record_batch,
    read_schema,
    read_tensor,
    write_tensor,
)


class RecordBatchStreamReader(lib._RecordBatchStreamReader):
    def __init__(
        self,
        source: bytes | lib.Buffer | lib.NativeFile | IOBase,
        *,
        options: IpcReadOptions | None = None,
        memory_pool: lib.MemoryPool | None = None,
    ) -> None: ...


class RecordBatchStreamWriter(lib._RecordBatchStreamWriter):
    def __init__(
        self,
        sink: str | lib.NativeFile | IOBase,
        schema: lib.Schema,
        *,
        use_legacy_format: bool | None = None,
        options: IpcWriteOptions | None = None,
    ) -> None: ...


class RecordBatchFileReader(lib._RecordBatchFileReader):
    def __init__(
        self,
        source: bytes | lib.Buffer | lib.NativeFile | IOBase,
        footer_offset: int | None = None,
        *,
        options: IpcReadOptions | None = None,
        memory_pool: lib.MemoryPool | None = None,
    ) -> None: ...

class RecordBatchFileWriter(lib._RecordBatchFileWriter):
    def __init__(
        self,
        sink: str | lib.NativeFile | IOBase,
        schema: lib.Schema,
        *,
        use_legacy_format: bool | None = None,
        options: IpcWriteOptions | None = None,
    ) -> None: ...


def new_stream(
    sink: str | lib.NativeFile | IOBase,
    schema: lib.Schema,
    *,
    use_legacy_format: bool | None = None,
    options: IpcWriteOptions | None = None,
) -> RecordBatchStreamWriter: ...


def open_stream(
    source: bytes | int | lib.Buffer | lib.NativeFile | IOBase,
    *,
    options: Any = None,
    memory_pool: lib.MemoryPool | None = None,
) -> RecordBatchStreamReader: ...


def new_file(
    sink: str | lib.NativeFile | IOBase,
    schema: lib.Schema,
    *,
    use_legacy_format: bool | None = None,
    options: IpcWriteOptions | None = None,
    metadata: lib.KeyValueMetadata | dict[bytes, bytes] | None = None,
) -> RecordBatchFileWriter: ...


def open_file(
    source: StrPath | bytes | lib.Buffer | lib.NativeFile | IOBase,
    footer_offset: int | None = None,
    *,
    options: Any = None,
    memory_pool: lib.MemoryPool | None = None,
) -> RecordBatchFileReader: ...


def serialize_pandas(
    df: pd.DataFrame, *, nthreads: int | None = None, preserve_index: bool | None = None
) -> lib.Buffer: ...


def deserialize_pandas(
    buf: lib.Buffer, *, use_threads: bool = True) -> pd.DataFrame: ...


__all__ = [
    "Alignment",
    "IpcReadOptions",
    "IpcWriteOptions",
    "Message",
    "MessageReader",
    "MetadataVersion",
    "ReadStats",
    "RecordBatchReader",
    "WriteStats",
    "_ReadPandasMixin",
    "get_record_batch_size",
    "get_tensor_size",
    "read_message",
    "read_record_batch",
    "read_schema",
    "read_tensor",
    "write_tensor",
    "RecordBatchStreamReader",
    "RecordBatchStreamWriter",
    "RecordBatchFileReader",
    "RecordBatchFileWriter",
    "new_stream",
    "open_stream",
    "new_file",
    "open_file",
    "serialize_pandas",
    "deserialize_pandas",
]
