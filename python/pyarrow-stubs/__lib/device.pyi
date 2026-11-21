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
from typing import ClassVar

from pyarrow.lib import _Weakrefable

class DeviceAllocationType(enum.IntEnum):
    CPU = 1
    CUDA = 2
    CUDA_HOST = 3
    OPENCL = 4
    VULKAN = 7
    METAL = 8
    VPI = 9
    ROCM = 10
    ROCM_HOST = 11
    EXT_DEV = 12
    CUDA_MANAGED = 13
    ONEAPI = 14
    WEBGPU = 15
    HEXAGON = 16

class Device(_Weakrefable):
    @property
    def type_name(self) -> str: ...
    @property
    def device_id(self) -> int: ...
    @property
    def is_cpu(self) -> bool: ...
    @property
    def device_type(self) -> DeviceAllocationType: ...
    __hash__: ClassVar[None]  # type: ignore[assignment]

class MemoryManager(_Weakrefable):
    @property
    def device(self) -> Device: ...
    @property
    def is_cpu(self) -> bool: ...

def default_cpu_memory_manager() -> MemoryManager: ...

__all__ = [
    "DeviceAllocationType",
    "Device",
    "MemoryManager",
    "default_cpu_memory_manager",
]
