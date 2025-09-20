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

from pyarrow.lib import _Weakrefable

class MemoryPool(_Weakrefable):


    def release_unused(self) -> None: ...

    def bytes_allocated(self) -> int: ...

    def total_bytes_allocated(self) -> int: ...

    def max_memory(self) -> int | None: ...

    def num_allocations(self) -> int: ...

    def print_stats(self) -> None: ...

    @property
    def backend_name(self) -> str: ...


class LoggingMemoryPool(MemoryPool): ...
class ProxyMemoryPool(MemoryPool): ...


def default_memory_pool() -> MemoryPool: ...


def proxy_memory_pool(parent: MemoryPool) -> ProxyMemoryPool: ...


def logging_memory_pool(parent: MemoryPool) -> LoggingMemoryPool: ...


def system_memory_pool() -> MemoryPool: ...


def jemalloc_memory_pool() -> MemoryPool: ...


def mimalloc_memory_pool() -> MemoryPool: ...


def set_memory_pool(pool: MemoryPool) -> None: ...


def log_memory_allocations(enable: bool = True) -> None: ...


def total_allocated_bytes() -> int: ...


def jemalloc_set_decay_ms(decay_ms: int) -> None: ...


def supported_memory_backends() -> list[str]: ...


__all__ = [
    "MemoryPool",
    "LoggingMemoryPool",
    "ProxyMemoryPool",
    "default_memory_pool",
    "proxy_memory_pool",
    "logging_memory_pool",
    "system_memory_pool",
    "jemalloc_memory_pool",
    "mimalloc_memory_pool",
    "set_memory_pool",
    "log_memory_allocations",
    "total_allocated_bytes",
    "jemalloc_set_decay_ms",
    "supported_memory_backends",
]
