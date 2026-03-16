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

from collections.abc import Collection, Container, Iterator, Sequence, Sized
from decimal import Decimal
from typing import Any, Literal, Protocol, TypeAlias, TypeVar

import numpy as np

from numpy.typing import NDArray

from pyarrow import lib
from pyarrow.lib import ChunkedArray

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
TimeUnit: TypeAlias = Literal["s", "ms", "us", "ns"]

IntegerType: TypeAlias = (
    lib.Int8Type
    | lib.Int16Type
    | lib.Int32Type
    | lib.Int64Type
    | lib.UInt8Type
    | lib.UInt16Type
    | lib.UInt32Type
    | lib.UInt64Type
)

Mask: TypeAlias = (
    Sequence[bool | None]
    | NDArray[np.bool_]
    | lib.Array[lib.Scalar[lib.BoolType]]
    | ChunkedArray[Any]
)
Indices: TypeAlias = (
    Sequence[int | None]
    | NDArray[np.integer[Any]]
    | lib.Array[lib.Scalar[IntegerType]]
    | ChunkedArray[Any]
)

PyScalar: TypeAlias = (
    bool
    | int
    | float
    | Decimal
    | str
    | bytes
    | dt.date
    | dt.datetime
    | dt.time
    | dt.timedelta
)

_T = TypeVar("_T")
_V = TypeVar("_V", covariant=True)

SingleOrList: TypeAlias = list[_T] | _T


class SupportsEq(Protocol):
    def __eq__(self, other: object, /) -> bool: ...


class SupportsLt(Protocol):
    def __lt__(self, other: object, /) -> bool: ...


class SupportsGt(Protocol):
    def __gt__(self, other: object, /) -> bool: ...


class SupportsLe(Protocol):
    def __le__(self, other: object, /) -> bool: ...


class SupportsGe(Protocol):
    def __ge__(self, other: object, /) -> bool: ...


FilterTuple: TypeAlias = (
    tuple[str, Literal["=", "==", "!="], SupportsEq]
    | tuple[str, Literal["<"], SupportsLt]
    | tuple[str, Literal[">"], SupportsGt]
    | tuple[str, Literal["<="], SupportsLe]
    | tuple[str, Literal[">="], SupportsGe]
    | tuple[str, Literal["in", "not in"], Collection]
    | tuple[str, str, Any]  # Allow general str for operator to avoid type errors
)


class Buffer(Protocol): ...


class SupportsPyBuffer(Protocol): ...


class SupportsArrowStream(Protocol):
    def __arrow_c_stream__(self, requested_schema=None, /) -> Any: ...


class SupportsPyArrowArray(Protocol):
    def __arrow_array__(self, type=None, /) -> Any: ...


class SupportsArrowArray(Protocol):
    def __arrow_c_array__(self, requested_schema=None, /) -> Any: ...


class SupportsArrowDeviceArray(Protocol):
    def __arrow_c_device_array__(self, requested_schema=None, /, **kwargs) -> Any: ...


class SupportsArrowSchema(Protocol):
    def __arrow_c_schema__(self) -> Any: ...


class NullableCollection(Sized, Container[_V], Protocol[_V]):
    def __iter__(self) -> Iterator[_V] | Iterator[_V | None]: ...
    def __len__(self) -> int: ...
    def __contains__(self, item: Any, /) -> bool: ...
