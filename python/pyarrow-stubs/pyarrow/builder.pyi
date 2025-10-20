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

from collections.abc import Iterable

from pyarrow.lib import MemoryPool, _Weakrefable

from .array import StringArray, StringViewArray


class StringBuilder(_Weakrefable):

    def __init__(self, memory_pool: MemoryPool | None = None) -> None: ...
    def append(self, value: str | bytes | float | None): ...

    def append_values(self, values: Iterable[str | bytes | float | None]): ...

    def finish(self) -> StringArray: ...

    @property
    def null_count(self) -> int: ...
    def __len__(self) -> int: ...


class StringViewBuilder(_Weakrefable):

    def __init__(self, memory_pool: MemoryPool | None = None) -> None: ...
    def append(self, value: str | bytes | float | None): ...

    def append_values(self, values: Iterable[str | bytes | float | None]): ...

    def finish(self) -> StringViewArray: ...

    @property
    def null_count(self) -> int: ...
    def __len__(self) -> int: ...


__all__ = ["StringBuilder", "StringViewBuilder"]
