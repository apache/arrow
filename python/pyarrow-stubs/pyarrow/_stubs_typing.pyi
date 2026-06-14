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
from typing import Any, Literal, Protocol, TypeAlias, TypeVar
from typing_extensions import TypeAliasType

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

_PyScalarT_co = TypeVar("_PyScalarT_co", bound=PyScalar, covariant=True)
_NumpyScalarT_co = TypeVar("_NumpyScalarT_co", bound=NumpyScalar, covariant=True)
_DataTypeT_co = TypeVar("_DataTypeT_co", bound=lib.DataType, covariant=True)
IntoArray = TypeAliasType(
    "IntoArray",
    Sequence[_PyScalarT_co | None]
    | NDArray[_NumpyScalarT_co]
    | lib.Array[lib.Scalar[_DataTypeT_co]]
    | ChunkedArray[Any],
    type_params=(_PyScalarT_co, _NumpyScalarT_co, _DataTypeT_co),
)

Mask: TypeAlias = IntoArray[bool, np.bool_, lib.BoolType]
Indices: TypeAlias = IntoArray[int, np.integer[Any], IntegerType]

_T = TypeVar("_T")
_V = TypeVar("_V", covariant=True)

SingleOrList: TypeAlias = list[_T] | _T

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

class NullableCollection(Sized, Container[Any], Protocol[_V]):
    def __iter__(self) -> Iterator[_V] | Iterator[_V | None]: ...
    def __len__(self) -> int: ...
    def __contains__(self, item: Any, /) -> bool: ...
