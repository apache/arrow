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

from collections.abc import Callable
from io import IOBase

from _typeshed import StrPath

import numpy as np

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self
if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

from typing import Any, Literal, SupportsIndex
import builtins

from pyarrow._stubs_typing import Compression, SupportPyBuffer
from pyarrow.lib import MemoryPool, _Weakrefable

from .device import Device, DeviceAllocationType, MemoryManager
from ._types import KeyValueMetadata


def have_libhdfs() -> bool: ...


def io_thread_count() -> int: ...


def set_io_thread_count(count: int) -> None: ...


Mode: TypeAlias = Literal["rb", "wb", "rb+", "ab"]


class NativeFile(_Weakrefable):

    _default_chunk_size: int

    def __enter__(self) -> Self: ...
    def __exit__(self, *args) -> None: ...
    @property
    def mode(self) -> Mode: ...

    def readable(self) -> bool: ...
    def seekable(self) -> bool: ...
    def isatty(self) -> bool: ...
    def fileno(self) -> int: ...

    @property
    def closed(self) -> bool: ...
    def close(self) -> None: ...
    def size(self) -> int: ...

    def metadata(self) -> KeyValueMetadata: ...

    def tell(self) -> int: ...

    def seek(self, position: int, whence: int = 0) -> int: ...

    def flush(self) -> None: ...

    def write(self, data: bytes | SupportPyBuffer) -> int: ...

    def read(self, nbytes: int | None = None) -> bytes: ...

    def get_stream(self, file_offset: int, nbytes: int) -> Self: ...

    def read_at(self, nbytes: int, offset: int) -> bytes: ...

    def read1(self, nbytes: int | None = None) -> bytes: ...

    def readall(self) -> bytes: ...
    def readinto(self, b: SupportPyBuffer) -> int: ...

    def readline(self, size: int | None = None) -> bytes: ...

    def readlines(self, hint: int | None = None) -> list[bytes]: ...

    def __iter__(self) -> Self: ...

    def __next__(self) -> bytes: ...
    def read_buffer(self, nbytes: int | None = None) -> Buffer: ...

    def truncate(self, pos: int | None = None) -> int: ...

    def writelines(self, lines: list[bytes]): ...

    def download(self, stream_or_path: StrPath | IOBase,
                 buffer_size: int | None = None) -> None: ...

    def upload(self, stream: IOBase, buffer_size: int | None) -> None: ...

    def writable(self): ...

# ----------------------------------------------------------------------
# Python file-like objects


class PythonFile(NativeFile):

    def __init__(self, handle: IOBase,
                 mode: Literal["r", "w"] | None = None) -> None: ...


class MemoryMappedFile(NativeFile):

    @classmethod
    def create(cls, path: str, size: float) -> Self: ...

    def _open(self, path: str,
              mode: Literal["r", "rb", "w", "wb", "r+", "r+b", "rb+"] = "r"): ...

    def resize(self, new_size: int) -> None: ...


def memory_map(
    path: str, mode: Literal["r", "rb", "w", "wb", "r+", "r+b", "rb+"] = "r"
) -> MemoryMappedFile: ...


create_memory_map = MemoryMappedFile.create


class OSFile(NativeFile):

    name: str

    def __init__(
        self,
        path: str,
        mode: Literal["r", "rb", "w", "wb", "a", "ab"] = "r",
        memory_pool: MemoryPool | None = None,
    ) -> None: ...


class FixedSizeBufferWriter(NativeFile):

    def __init__(self, buffer: Buffer) -> None: ...
    def set_memcopy_threads(self, num_threads: int) -> None: ...

    def set_memcopy_blocksize(self, blocksize: int) -> None: ...

    def set_memcopy_threshold(self, threshold: int) -> None: ...


# ----------------------------------------------------------------------
# Arrow buffers

class Buffer(_Weakrefable):

    def __len__(self) -> int: ...

    def _assert_cpu(self) -> None: ...
    @property
    def size(self) -> int: ...

    @property
    def address(self) -> int: ...

    def hex(self) -> bytes: ...

    @property
    def is_mutable(self) -> bool: ...

    @property
    def is_cpu(self) -> bool: ...

    @property
    def device(self) -> Device: ...

    @property
    def memory_manager(self) -> MemoryManager: ...

    @property
    def device_type(self) -> DeviceAllocationType: ...

    @property
    def parent(self) -> Buffer | None: ...

    def __getitem__(self, key: int | builtins.slice) -> int | Self: ...

    def slice(self, offset: int = 0, length: int | None = None) -> Self: ...

    def equals(self, other: Self) -> bool: ...

    def __buffer__(self, flags: int) -> memoryview: ...

    def __reduce_ex__(self, protocol: SupportsIndex) -> str | tuple[Any, ...]: ...
    def to_pybytes(self) -> bytes: ...


class ResizableBuffer(Buffer):

    def resize(self, new_size: int, shrink_to_fit: bool = False) -> None: ...


def allocate_buffer(
    size: int,
    memory_pool: MemoryPool | None = None,
    resizable: Literal[False] | Literal[True] | None = None  # noqa: Y030
) -> Buffer | ResizableBuffer: ...


# ----------------------------------------------------------------------
# Arrow Stream
class BufferOutputStream(NativeFile):

    def __init__(self, memory_pool: MemoryPool | None = None) -> None: ...
    def getvalue(self) -> Buffer: ...


class MockOutputStream(NativeFile):
    ...


class BufferReader(NativeFile):

    def __init__(self, obj) -> None: ...


class CompressedInputStream(NativeFile):

    def __init__(
        self,
        stream: StrPath | NativeFile | IOBase,
        compression: str | None,
    ) -> None: ...


class CompressedOutputStream(NativeFile):

    def __init__(
        self,
        stream: StrPath | NativeFile | IOBase,
        compression: str,
    ) -> None: ...


class BufferedInputStream(NativeFile):

    def __init__(self, stream: NativeFile, buffer_size: int,
                 memory_pool: MemoryPool | None = None) -> None: ...

    def detach(self) -> NativeFile: ...


class BufferedOutputStream(NativeFile):

    def __init__(self, stream: NativeFile, buffer_size: int,
                 memory_pool: MemoryPool | None = None) -> None: ...

    def detach(self) -> NativeFile: ...


class TransformInputStream(NativeFile):

    def __init__(self, stream: NativeFile,
                 transform_func: Callable[[Buffer], Any]) -> None: ...


class Transcoder:
    def __init__(self, decoder, encoder) -> None: ...
    def __call__(self, buf: Buffer): ...


def transcoding_input_stream(
    stream: NativeFile, src_encoding: str, dest_encoding: str
) -> TransformInputStream: ...


def py_buffer(obj: SupportPyBuffer | np.ndarray) -> Buffer: ...


def foreign_buffer(address: int, size: int, base: Any | None = None) -> Buffer: ...


def as_buffer(o: Buffer | SupportPyBuffer) -> Buffer: ...

# ---------------------------------------------------------------------


class CacheOptions(_Weakrefable):

    hole_size_limit: int
    range_size_limit: int
    lazy: bool
    prefetch_limit: int

    def __init__(
        self,
        *,
        hole_size_limit: int | None = None,
        range_size_limit: int | None = None,
        lazy: bool = True,
        prefetch_limit: int = 0,
    ) -> None: ...

    @classmethod
    def from_network_metrics(
        cls,
        time_to_first_byte_millis: int,
        transfer_bandwidth_mib_per_sec: int,
        ideal_bandwidth_utilization_frac: float = 0.9,
        max_ideal_request_size_mib: int = 64,
    ) -> Self: ...


class Codec(_Weakrefable):

    def __init__(self, compression: Compression | str | None,
                 compression_level: int | None = None) -> None: ...

    @classmethod
    def detect(cls, path: StrPath) -> Self: ...

    @staticmethod
    def is_available(compression: Compression | str) -> bool: ...

    @staticmethod
    def supports_compression_level(compression: Compression) -> int: ...

    @staticmethod
    def default_compression_level(compression: Compression) -> int: ...

    @staticmethod
    def minimum_compression_level(compression: Compression) -> int: ...

    @staticmethod
    def maximum_compression_level(compression: Compression) -> int: ...

    @property
    def name(self) -> Compression: ...

    @property
    def compression_level(self) -> int: ...

    def compress(
        self,
        buf: Buffer | bytes | SupportPyBuffer,
        *,
        asbytes: Literal[False] | Literal[True] | None = None,  # noqa: Y030
        memory_pool: MemoryPool | None = None,
    ) -> Buffer | bytes: ...

    def decompress(
        self,
        buf: Buffer | bytes | SupportPyBuffer,
        decompressed_size: int | None = None,
        *,
        asbytes: Literal[False] | Literal[True] | None = None,  # noqa: Y030
        memory_pool: MemoryPool | None = None,
    ) -> Buffer | bytes: ...


def compress(
    buf: Buffer | bytes | SupportPyBuffer,
    codec: Compression = "lz4",
    *,
    asbytes: Literal[False] | Literal[True] | None = None,  # noqa: Y030
    memory_pool: MemoryPool | None = None,
) -> Buffer | bytes: ...


def decompress(
    buf: Buffer | bytes | SupportPyBuffer,
    decompressed_size: int | None = None,
    codec: Compression = "lz4",
    *,
    asbytes: Literal[False] | Literal[True] | None = None,  # noqa: Y030
    memory_pool: MemoryPool | None = None,
) -> Buffer | bytes: ...


def input_stream(
    source: StrPath | Buffer | NativeFile | IOBase | SupportPyBuffer,
    compression:
    Literal["detect", "bz2", "brotli", "gzip", "lz4", "zstd"] | None = "detect",
    buffer_size: int | str | None = None,
) -> BufferReader: ...


def output_stream(
    source: StrPath | Buffer | NativeFile | IOBase | SupportPyBuffer,
    compression:
    Literal["detect", "bz2", "brotli", "gzip", "lz4", "zstd"] | None = "detect",
    buffer_size: int | None = None,
) -> NativeFile: ...


__all__ = [
    "have_libhdfs",
    "io_thread_count",
    "set_io_thread_count",
    "NativeFile",
    "PythonFile",
    "MemoryMappedFile",
    "memory_map",
    "create_memory_map",
    "OSFile",
    "FixedSizeBufferWriter",
    "Buffer",
    "ResizableBuffer",
    "allocate_buffer",
    "BufferOutputStream",
    "MockOutputStream",
    "BufferReader",
    "CompressedInputStream",
    "CompressedOutputStream",
    "BufferedInputStream",
    "BufferedOutputStream",
    "TransformInputStream",
    "Transcoder",
    "transcoding_input_stream",
    "py_buffer",
    "foreign_buffer",
    "as_buffer",
    "CacheOptions",
    "Codec",
    "compress",
    "decompress",
    "input_stream",
    "output_stream",
]
