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

from collections.abc import Iterable
from typing import Any, TypeAlias, TypedDict

from pyarrow.lib import Array, ChunkedArray

from .buffer import _PyArrowBuffer


class DtypeKind(enum.IntEnum):
    INT = 0
    UINT = 1
    FLOAT = 2
    BOOL = 20
    STRING = 21  # UTF-8
    DATETIME = 22
    CATEGORICAL = 23


Dtype: TypeAlias = tuple[DtypeKind, int, str, str]


class ColumnNullType(enum.IntEnum):
    NON_NULLABLE = 0
    USE_NAN = 1
    USE_SENTINEL = 2
    USE_BITMASK = 3
    USE_BYTEMASK = 4


class ColumnBuffers(TypedDict):
    data: tuple[_PyArrowBuffer, Dtype]
    validity: tuple[_PyArrowBuffer, Dtype] | None
    offsets: tuple[_PyArrowBuffer, Dtype] | None


class CategoricalDescription(TypedDict):
    is_ordered: bool
    is_dictionary: bool
    categories: _PyArrowColumn | None


class Endianness(enum.Enum):
    LITTLE = "<"
    BIG = ">"
    NATIVE = "="
    NA = "|"


class NoBufferPresent(Exception):
    ...


class _PyArrowColumn:
    _col: Array | ChunkedArray

    def __init__(self, column: Array | ChunkedArray,
                 allow_copy: bool = True) -> None: ...

    def size(self) -> int: ...
    @property
    def offset(self) -> int: ...
    @property
    def dtype(self) -> tuple[DtypeKind, int, str, str]: ...
    @property
    def describe_categorical(self) -> CategoricalDescription: ...
    @property
    def describe_null(self) -> tuple[ColumnNullType, Any]: ...
    @property
    def null_count(self) -> int: ...
    @property
    def metadata(self) -> dict[str, Any]: ...
    def num_chunks(self) -> int: ...
    def get_chunks(self, n_chunks: int | None = None) -> Iterable[_PyArrowColumn]: ...
    def get_buffers(self) -> ColumnBuffers: ...
