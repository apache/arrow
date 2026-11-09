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

from _typeshed import (
    SupportsDunderGE,
    SupportsDunderGT,
    SupportsDunderLE,
    SupportsDunderLT,
)
from collections.abc import Collection, Container, Iterator, Sequence, Sized
from decimal import Decimal
from typing import Any, Literal, Protocol, TypeAlias

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
MapsAsPydicts: TypeAlias = Literal["lossy", "strict"]
NullEncoding: TypeAlias = Literal["mask", "encode"]
NullPlacement: TypeAlias = Literal["at_start", "at_end"]
NullSelectionBehavior: TypeAlias = Literal["drop", "emit_null"]
TimeUnit: TypeAlias = Literal["s", "ms", "us", "ns"]

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
NumpyScalar: TypeAlias = np.generic[Any]

Mask: TypeAlias = (
    Sequence[bool | None] | NDArray[np.bool_] | lib.BooleanArray | ChunkedArray
)
Indices: TypeAlias = (
    Sequence[int | None] | NDArray[np.integer[Any]] | lib.IntegerArray | ChunkedArray
)

class SupportsDunderEQ(Protocol):
    def __eq__(self, other: object, /) -> bool: ...

FilterTuple: TypeAlias = (
    tuple[str, Literal["=", "==", "!="], SupportsDunderEQ]
    | tuple[str, Literal["<"], SupportsDunderLT[Any]]
    | tuple[str, Literal[">"], SupportsDunderGT[Any]]
    | tuple[str, Literal["<="], SupportsDunderLE[Any]]
    | tuple[str, Literal[">="], SupportsDunderGE[Any]]
    | tuple[str, Literal["in", "not in"], Collection[Any]]
    | tuple[str, str, Any]  # Allow general str for operator to avoid type errors
)

class Buffer(Protocol): ...
class SupportsPyBuffer(Protocol): ...

class SupportsArrowStream(Protocol):
    def __arrow_c_stream__(self, requested_schema: Any = None, /) -> Any: ...

class SupportsPyArrowArray(Protocol):
    def __arrow_array__(self, type: Any = None, /) -> Any: ...

class SupportsArrowArray(Protocol):
    def __arrow_c_array__(self, requested_schema: Any = None, /) -> Any: ...

class SupportsArrowDeviceArray(Protocol):
    def __arrow_c_device_array__(
        self, requested_schema: Any = None, /, **kwargs: Any
    ) -> Any: ...

class SupportsArrowSchema(Protocol):
    def __arrow_c_schema__(self) -> Any: ...

class NullableCollection(Sized, Container[Any], Protocol):
    def __iter__(self) -> Iterator[Any]: ...
    def __len__(self) -> int: ...
    def __contains__(self, item: Any, /) -> bool: ...

# Data types without a class of their own at runtime: their factories, e.g.
# pyarrow.int32(), return plain DataType instances. These classes only exist
# for type checking, where they distinguish the results of the factories and
# the narrowing of the pyarrow.types.is_* checks.
class NullType(lib.DataType): ...
class BoolType(lib.DataType): ...
class UInt8Type(lib.DataType): ...
class Int8Type(lib.DataType): ...
class UInt16Type(lib.DataType): ...
class Int16Type(lib.DataType): ...
class UInt32Type(lib.DataType): ...
class Int32Type(lib.DataType): ...
class UInt64Type(lib.DataType): ...
class Int64Type(lib.DataType): ...
class Float16Type(lib.DataType): ...
class Float32Type(lib.DataType): ...
class Float64Type(lib.DataType): ...
class Date32Type(lib.DataType): ...
class Date64Type(lib.DataType): ...
class MonthDayNanoIntervalType(lib.DataType): ...
class StringType(lib.DataType): ...
class LargeStringType(lib.DataType): ...
class StringViewType(lib.DataType): ...
class BinaryType(lib.DataType): ...
class LargeBinaryType(lib.DataType): ...
class BinaryViewType(lib.DataType): ...

IntegerType: TypeAlias = (
    UInt8Type
    | Int8Type
    | UInt16Type
    | Int16Type
    | UInt32Type
    | Int32Type
    | UInt64Type
    | Int64Type
)
