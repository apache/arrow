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

import datetime as dt

from collections.abc import Collection, Iterator, Sequence
from decimal import Decimal
from typing import Any, Literal, Protocol, TypeAlias, TypeVar

import numpy as np

from numpy.typing import NDArray

from pyarrow.lib import BooleanArray, IntegerArray

ArrayLike: TypeAlias = Any
ScalarLike: TypeAlias = Any
Order: TypeAlias = Literal["ascending", "descending"]
JoinType: TypeAlias = Literal[
    "left semi",
    "right semi",
    "left anti",
    "right anti",
    "inner",
    "left outer",
    "right outer",
    "full outer",
]
Compression: TypeAlias = Literal[
    "gzip", "bz2", "brotli", "lz4", "lz4_frame", "lz4_raw", "zstd", "snappy"
]
NullEncoding: TypeAlias = Literal["mask", "encode"]
NullSelectionBehavior: TypeAlias = Literal["drop", "emit_null"]
Mask: TypeAlias = Sequence[bool | None] | NDArray[np.bool_] | BooleanArray
Indices: TypeAlias = Sequence[int] | NDArray[np.integer[Any]] | IntegerArray
PyScalar: TypeAlias = (bool | int | float | Decimal | str | bytes |
                       dt.date | dt.datetime | dt.time | dt.timedelta)

_T = TypeVar("_T")
_V = TypeVar("_V", covariant=True)

SingleOrList: TypeAlias = list[_T] | _T


class SupportEq(Protocol):
    def __eq__(self, other) -> bool: ...


class SupportLt(Protocol):
    def __lt__(self, other) -> bool: ...


class SupportGt(Protocol):
    def __gt__(self, other) -> bool: ...


class SupportLe(Protocol):
    def __le__(self, other) -> bool: ...


class SupportGe(Protocol):
    def __ge__(self, other) -> bool: ...


FilterTuple: TypeAlias = (
    tuple[str, Literal["=", "==", "!="], SupportEq]
    | tuple[str, Literal["<"], SupportLt]
    | tuple[str, Literal[">"], SupportGt]
    | tuple[str, Literal["<="], SupportLe]
    | tuple[str, Literal[">="], SupportGe]
    | tuple[str, Literal["in", "not in"], Collection]
)


class Buffer(Protocol):
    ...


class SupportPyBuffer(Protocol):
    ...


class SupportArrowStream(Protocol):
    def __arrow_c_stream__(self, requested_schema=None) -> Any: ...


class SupportPyArrowArray(Protocol):
    def __arrow_array__(self, type=None) -> Any: ...

class SupportArrowArray(Protocol):
    def __arrow_c_array__(self, requested_schema=None) -> Any: ...


class SupportArrowDeviceArray(Protocol):
    def __arrow_c_device_array__(self, requested_schema=None, **kwargs) -> Any: ...


class SupportArrowSchema(Protocol):
    def __arrow_c_schema(self) -> Any: ...


class NullableCollection(Protocol[_V]):  # pyright: ignore[reportInvalidTypeVarUse]
    def __iter__(self) -> Iterator[_V] | Iterator[_V | None]: ...
    def __len__(self) -> int: ...
    def __contains__(self, item: Any, /) -> bool: ...
