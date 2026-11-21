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
# mypy: warn-unused-ignores

from collections.abc import Hashable
from typing import TYPE_CHECKING

import pyarrow as pa

from _check import assert_type, check


def test_device() -> None:
    mm = pa.default_cpu_memory_manager()
    check(assert_type(mm, pa.MemoryManager), pa.MemoryManager)
    check(assert_type(mm.is_cpu, bool), bool)

    device = mm.device
    check(assert_type(device, pa.Device), pa.Device)
    check(assert_type(device.device_type, pa.DeviceAllocationType),
          pa.DeviceAllocationType)

    check(assert_type(device.device_id, int), int)
    check(assert_type(device.type_name, str), str)
    check(assert_type(device.is_cpu, bool), bool)


def test_device_invalid_usage() -> None:
    device = pa.default_cpu_memory_manager().device
    if TYPE_CHECKING:
        _: Hashable = device  # type: ignore[assignment]  # ty: ignore[invalid-assignment]  # noqa: E501
