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

from typing import Any

import cuda  # type: ignore[import-not-found]

from numba.cuda.cudadrv import driver as _numba_driver  # type: ignore[import-untyped, import-not-found] # noqa: E501

from . import lib
from ._stubs_typing import ArrayLike


class Context(lib._Weakrefable):
    def __init__(self, device_number: int = 0, handle: int | None = None) -> None: ...

    @staticmethod
    def from_numba(context: _numba_driver.Context | None = None) -> Context: ...

    def to_numba(self) -> _numba_driver.Context: ...

    @staticmethod
    def get_num_devices() -> int: ...

    @property
    def device_number(self) -> int: ...

    @property
    def handle(self) -> int: ...

    def synchronize(self) -> None: ...

    @property
    def bytes_allocated(self) -> int: ...

    def get_device_address(self, address: int) -> int: ...

    def new_buffer(self, nbytes: int) -> CudaBuffer: ...

    @property
    def memory_manager(self) -> lib.MemoryManager: ...

    @property
    def device(self) -> lib.Device: ...

    def foreign_buffer(self, address: int, size: int, base: Any |
                       None = None) -> CudaBuffer: ...

    def open_ipc_buffer(self, ipc_handle: IpcMemHandle) -> CudaBuffer: ...

    def buffer_from_data(
        self,
        data: CudaBuffer | HostBuffer | lib.Buffer | ArrayLike,
        offset: int = 0,
        size: int = -1,
    ) -> CudaBuffer: ...

    def buffer_from_object(self, obj: Any) -> CudaBuffer: ...


class IpcMemHandle(lib._Weakrefable):
    @staticmethod
    def from_buffer(opaque_handle: lib.Buffer) -> IpcMemHandle: ...

    def serialize(self, pool: lib.MemoryPool | None = None) -> lib.Buffer: ...


class CudaBuffer(lib.Buffer):
    @staticmethod
    def from_buffer(buf: lib.Buffer) -> CudaBuffer: ...

    @staticmethod
    def from_numba(mem: _numba_driver.MemoryPointer) -> CudaBuffer: ...

    def to_numba(self) -> _numba_driver.MemoryPointer: ...

    def copy_to_host(
        self,
        position: int = 0,
        nbytes: int = -1,
        buf: lib.Buffer | None = None,
        memory_pool: lib.MemoryPool | None = None,
        resizable: bool = False,
    ) -> lib.Buffer: ...

    def copy_from_host(
        self, data: lib.Buffer | ArrayLike, position: int = 0, nbytes: int = -1
    ) -> int: ...

    def copy_from_device(self, buf: CudaBuffer, position: int = 0,
                         nbytes: int = -1) -> int: ...

    def export_for_ipc(self) -> IpcMemHandle: ...

    @property
    def context(self) -> Context: ...

    def slice(self, offset: int = 0, length: int | None = None) -> CudaBuffer: ...

    def to_pybytes(self) -> bytes: ...


class HostBuffer(lib.Buffer):
    @property
    def size(self) -> int: ...


class BufferReader(lib.NativeFile):
    def __init__(self, obj: CudaBuffer) -> None: ...
    def read_buffer(self, nbytes: int | None = None) -> CudaBuffer: ...


class BufferWriter(lib.NativeFile):
    def __init__(self, obj: CudaBuffer) -> None: ...
    def writeat(self, position: int, data: ArrayLike) -> None: ...

    @property
    def buffer_size(self) -> int: ...

    @buffer_size.setter
    def buffer_size(self, buffer_size: int): ...

    @property
    def num_bytes_buffered(self) -> int: ...


def new_host_buffer(size: int, device: int = 0) -> HostBuffer: ...


def serialize_record_batch(batch: lib.RecordBatch, ctx: Context) -> CudaBuffer: ...


def read_message(
    source: CudaBuffer | cuda.BufferReader, pool: lib.MemoryManager | None = None
) -> lib.Message: ...


def read_record_batch(
    buffer: lib.Buffer,
    object: lib.Schema,
    *,
    dictionary_memo: lib.DictionaryMemo | None = None,
    pool: lib.MemoryPool | None = None,
) -> lib.RecordBatch: ...
