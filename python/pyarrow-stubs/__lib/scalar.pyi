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
from collections.abc import Iterator, Mapping, Sequence
from decimal import Decimal
from typing import Any, SupportsIndex, TypeAlias, overload
from uuid import UUID

import numpy as np
from pyarrow._compute import CastOptions
from pyarrow._stubs_typing import MapsAsPydicts
from pyarrow.lib import (
    Array,
    BaseExtensionType,
    Buffer,
    DataType,
    MemoryPool,
    MonthDayNano,
    Tensor,
    _Weakrefable,
)
from typing_extensions import CapsuleType

class Scalar(_Weakrefable):
    @property
    def type(self) -> DataType: ...
    @property
    def is_valid(self) -> bool: ...
    def cast(
        self,
        target_type: DataType | str | None = None,
        safe: bool | None = None,
        options: CastOptions | None = None,
        memory_pool: MemoryPool | None = None,
    ) -> Scalar: ...
    def validate(self, *, full: bool = False) -> None: ...
    def equals(self, other: Scalar) -> bool: ...
    def as_py(self, *, maps_as_pydicts: MapsAsPydicts | None = None) -> Any: ...
    def __abs__(self) -> Scalar: ...
    def __neg__(self) -> Scalar: ...
    # The binary operators call the compute functions, which return a scalar,
    # or an (chunked) array if the other operand is one.
    def __add__(self, other: Any, /) -> Any: ...
    def __sub__(self, other: Any, /) -> Any: ...
    def __mul__(self, other: Any, /) -> Any: ...
    def __truediv__(self, other: Any, /) -> Any: ...
    def __pow__(self, other: Any, mod: None = None, /) -> Any: ...
    def __and__(self, other: Any, /) -> Any: ...
    def __or__(self, other: Any, /) -> Any: ...
    def __xor__(self, other: Any, /) -> Any: ...
    def __lshift__(self, other: Any, /) -> Any: ...
    def __rshift__(self, other: Any, /) -> Any: ...

class NullScalar(Scalar):
    def as_py(self, *, maps_as_pydicts: MapsAsPydicts | None = None) -> None: ...

NA: NullScalar
_NULL: NullScalar

class BooleanScalar(Scalar):
    def as_py(self, *, maps_as_pydicts: MapsAsPydicts | None = None) -> bool | None: ...
    def __bool__(self) -> bool: ...

class UInt8Scalar(Scalar):
    def as_py(self, *, maps_as_pydicts: MapsAsPydicts | None = None) -> int | None: ...
    def __index__(self) -> int: ...

class Int8Scalar(Scalar):
    def as_py(self, *, maps_as_pydicts: MapsAsPydicts | None = None) -> int | None: ...
    def __index__(self) -> int: ...

class UInt16Scalar(Scalar):
    def as_py(self, *, maps_as_pydicts: MapsAsPydicts | None = None) -> int | None: ...
    def __index__(self) -> int: ...

class Int16Scalar(Scalar):
    def as_py(self, *, maps_as_pydicts: MapsAsPydicts | None = None) -> int | None: ...
    def __index__(self) -> int: ...

class UInt32Scalar(Scalar):
    def as_py(self, *, maps_as_pydicts: MapsAsPydicts | None = None) -> int | None: ...
    def __index__(self) -> int: ...

class Int32Scalar(Scalar):
    def as_py(self, *, maps_as_pydicts: MapsAsPydicts | None = None) -> int | None: ...
    def __index__(self) -> int: ...

class UInt64Scalar(Scalar):
    def as_py(self, *, maps_as_pydicts: MapsAsPydicts | None = None) -> int | None: ...
    def __index__(self) -> int: ...

class Int64Scalar(Scalar):
    def as_py(self, *, maps_as_pydicts: MapsAsPydicts | None = None) -> int | None: ...
    def __index__(self) -> int: ...

_IntegerScalar: TypeAlias = (
    UInt8Scalar
    | Int8Scalar
    | UInt16Scalar
    | Int16Scalar
    | UInt32Scalar
    | Int32Scalar
    | UInt64Scalar
    | Int64Scalar
)

class HalfFloatScalar(Scalar):
    def as_py(
        self, *, maps_as_pydicts: MapsAsPydicts | None = None
    ) -> float | None: ...
    def __float__(self) -> float: ...
    def __int__(self) -> int: ...

class FloatScalar(Scalar):
    def as_py(
        self, *, maps_as_pydicts: MapsAsPydicts | None = None
    ) -> float | None: ...
    def __float__(self) -> float: ...
    def __int__(self) -> int: ...

class DoubleScalar(Scalar):
    def as_py(
        self, *, maps_as_pydicts: MapsAsPydicts | None = None
    ) -> float | None: ...
    def __float__(self) -> float: ...
    def __int__(self) -> int: ...

class Decimal32Scalar(Scalar):
    def as_py(
        self, *, maps_as_pydicts: MapsAsPydicts | None = None
    ) -> Decimal | None: ...

class Decimal64Scalar(Scalar):
    def as_py(
        self, *, maps_as_pydicts: MapsAsPydicts | None = None
    ) -> Decimal | None: ...

class Decimal128Scalar(Scalar):
    def as_py(
        self, *, maps_as_pydicts: MapsAsPydicts | None = None
    ) -> Decimal | None: ...

class Decimal256Scalar(Scalar):
    def as_py(
        self, *, maps_as_pydicts: MapsAsPydicts | None = None
    ) -> Decimal | None: ...

# The value properties of the temporal scalars hold the integer stored in the
# array (e.g. days since the epoch for Date32Scalar), as_py() converts it.
class Date32Scalar(Scalar):
    @property
    def value(self) -> int | None: ...
    def as_py(
        self, *, maps_as_pydicts: MapsAsPydicts | None = None
    ) -> dt.date | None: ...

class Date64Scalar(Scalar):
    @property
    def value(self) -> int | None: ...
    def as_py(
        self, *, maps_as_pydicts: MapsAsPydicts | None = None
    ) -> dt.date | None: ...

class Time32Scalar(Scalar):
    @property
    def value(self) -> int | None: ...
    def as_py(
        self, *, maps_as_pydicts: MapsAsPydicts | None = None
    ) -> dt.time | None: ...

class Time64Scalar(Scalar):
    @property
    def value(self) -> int | None: ...
    def as_py(
        self, *, maps_as_pydicts: MapsAsPydicts | None = None
    ) -> dt.time | None: ...

class TimestampScalar(Scalar):
    @property
    def value(self) -> int | None: ...
    # A pandas.Timestamp, a datetime subclass, for nanoseconds if pandas is
    # installed.
    def as_py(
        self, *, maps_as_pydicts: MapsAsPydicts | None = None
    ) -> dt.datetime | None: ...

class DurationScalar(Scalar):
    @property
    def value(self) -> int | None: ...
    # A pandas.Timedelta, a timedelta subclass, for nanoseconds if pandas is
    # installed.
    def as_py(
        self, *, maps_as_pydicts: MapsAsPydicts | None = None
    ) -> dt.timedelta | None: ...

class MonthDayNanoIntervalScalar(Scalar):
    @property
    def value(self) -> MonthDayNano | None: ...
    def as_py(
        self, *, maps_as_pydicts: MapsAsPydicts | None = None
    ) -> MonthDayNano | None: ...

class BinaryScalar(Scalar):
    def as_buffer(self) -> Buffer | None: ...
    def as_py(
        self, *, maps_as_pydicts: MapsAsPydicts | None = None
    ) -> bytes | None: ...
    def __bytes__(self) -> bytes: ...
    # Scalars support the buffer protocol on every Python version, but only
    # have a __buffer__ method at runtime from Python 3.12 on.
    def __buffer__(self, flags: int, /) -> memoryview: ...

class LargeBinaryScalar(BinaryScalar): ...
class FixedSizeBinaryScalar(BinaryScalar): ...
class BinaryViewScalar(BinaryScalar): ...

class StringScalar(BinaryScalar):
    def as_py(  # type: ignore[override]
        self, *, maps_as_pydicts: MapsAsPydicts | None = None
    ) -> str | None: ...

class LargeStringScalar(StringScalar): ...
class StringViewScalar(StringScalar): ...

class ListScalar(Scalar, Sequence[Scalar]):
    @property
    def values(self) -> Array | None: ...
    def __len__(self) -> int: ...
    # Only integer indices, unlike Sequence, which also takes slices.
    def __getitem__(self, i: SupportsIndex) -> Scalar: ...  # type: ignore[override]
    def __iter__(self) -> Iterator[Scalar]: ...
    def as_py(
        self, *, maps_as_pydicts: MapsAsPydicts | None = None
    ) -> list[Any] | None: ...

class FixedSizeListScalar(ListScalar): ...
class LargeListScalar(ListScalar): ...
class ListViewScalar(ListScalar): ...
class LargeListViewScalar(ListScalar): ...

class StructScalar(Scalar, Mapping[str, Scalar]):
    def __len__(self) -> int: ...
    def __iter__(self) -> Iterator[str]: ...
    def __contains__(self, key: object, /) -> bool: ...
    # Look up a field by name or position.
    def __getitem__(self, key: str | bytes | int) -> Scalar: ...
    # The runtime yields a generator instead of a Mapping view
    def items(self) -> Iterator[tuple[str, Scalar]]: ...  # type: ignore[override]
    def as_py(
        self, *, maps_as_pydicts: MapsAsPydicts | None = None
    ) -> dict[str, Any] | None: ...

# A MapScalar is a list of key-item structs. Unlike a ListScalar, it iterates
# over the (key, item) pairs as Python objects, and can be indexed by key.
class MapScalar(ListScalar):
    @overload  # type: ignore[override]
    def __getitem__(self, i: SupportsIndex) -> tuple[Scalar, Scalar]: ...
    @overload
    def __getitem__(self, i: str | bytes) -> Scalar: ...
    def __iter__(self) -> Iterator[tuple[Any, Any]]: ...  # type: ignore[override]
    def keys(self) -> list[Any]: ...
    # A list of (key, item) tuples, or a dict if maps_as_pydicts is given.
    @overload  # type: ignore[override]
    def as_py(
        self, *, maps_as_pydicts: None = None
    ) -> list[tuple[Any, Any]] | None: ...
    @overload
    def as_py(self, *, maps_as_pydicts: MapsAsPydicts) -> dict[Any, Any] | None: ...

class DictionaryScalar(Scalar):
    @property
    def index(self) -> _IntegerScalar: ...
    @property
    def value(self) -> Scalar: ...
    @property
    def dictionary(self) -> Array: ...

class RunEndEncodedScalar(Scalar):
    @property
    def value(self) -> Scalar: ...

class UnionScalar(Scalar):
    @property
    def value(self) -> Scalar | None: ...
    @property
    def type_code(self) -> int: ...

class ExtensionScalar(Scalar):
    @property
    def value(self) -> Scalar | None: ...
    @staticmethod
    def from_storage(typ: BaseExtensionType, value: Any) -> ExtensionScalar: ...

class JsonScalar(ExtensionScalar): ...

class UuidScalar(ExtensionScalar):
    def as_py(self, *, maps_as_pydicts: MapsAsPydicts | None = None) -> UUID | None: ...

class FixedShapeTensorScalar(ExtensionScalar):
    def to_numpy(self) -> np.ndarray: ...
    def to_tensor(self) -> Tensor: ...
    def __dlpack__(
        self,
        *,
        stream: int | None = None,
        max_version: tuple[int, int] | None = None,
        dl_device: tuple[int, int] | None = None,
        copy: bool | None = None,
    ) -> CapsuleType: ...
    def __dlpack_device__(self) -> tuple[int, int]: ...

class OpaqueScalar(ExtensionScalar): ...

class Bool8Scalar(ExtensionScalar):
    def as_py(self, *, maps_as_pydicts: MapsAsPydicts | None = None) -> bool | None: ...

def scalar(
    value: Any,
    type: DataType | str | None = None,
    *,
    from_pandas: bool | None = None,
    memory_pool: MemoryPool | None = None,
) -> Scalar: ...

__all__ = [
    "Scalar",
    "NullScalar",
    "NA",
    "_NULL",
    "BooleanScalar",
    "UInt8Scalar",
    "Int8Scalar",
    "UInt16Scalar",
    "Int16Scalar",
    "UInt32Scalar",
    "Int32Scalar",
    "UInt64Scalar",
    "Int64Scalar",
    "HalfFloatScalar",
    "FloatScalar",
    "DoubleScalar",
    "Decimal32Scalar",
    "Decimal64Scalar",
    "Decimal128Scalar",
    "Decimal256Scalar",
    "Date32Scalar",
    "Date64Scalar",
    "Time32Scalar",
    "Time64Scalar",
    "TimestampScalar",
    "DurationScalar",
    "MonthDayNanoIntervalScalar",
    "BinaryScalar",
    "LargeBinaryScalar",
    "FixedSizeBinaryScalar",
    "BinaryViewScalar",
    "StringScalar",
    "LargeStringScalar",
    "StringViewScalar",
    "ListScalar",
    "FixedSizeListScalar",
    "LargeListScalar",
    "ListViewScalar",
    "LargeListViewScalar",
    "StructScalar",
    "MapScalar",
    "DictionaryScalar",
    "RunEndEncodedScalar",
    "UnionScalar",
    "ExtensionScalar",
    "JsonScalar",
    "UuidScalar",
    "FixedShapeTensorScalar",
    "OpaqueScalar",
    "Bool8Scalar",
    "scalar",
]
