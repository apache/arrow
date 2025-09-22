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

import collections.abc
import datetime as dt
import sys

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self
if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias
from collections.abc import Iterator
from typing import Any, Generic, Literal

import numpy as np

from pyarrow._compute import CastOptions
from pyarrow.lib import Array, Buffer, MemoryPool, MonthDayNano, Tensor, _Weakrefable
from typing_extensions import TypeVar

from ._types import (  # noqa: F401
    DataType,
    Decimal128Type,
    Date32Type,
    Date64Type,
    Time32Type,
    Time64Type,
    TimestampType,
    Decimal256Type,
    NullType,
    BoolType,
    UInt8Type,
    Int8Type,
    DurationType, MonthDayNanoIntervalType, BinaryType, LargeBinaryType,
    FixedSizeBinaryType, StringType, LargeStringType, BinaryViewType, StringViewType,
    FixedSizeListType,
    Float16Type, Float32Type, Float64Type, Decimal32Type, Decimal64Type,
    LargeListType,
    LargeListViewType,
    ListType,
    ListViewType,
    OpaqueType, DictionaryType, MapType, _BasicDataType,
    StructType, RunEndEncodedType,
    UInt16Type, Int16Type, Uint32Type, Int32Type, UInt64Type, Int64Type,
    UnionType, ExtensionType, BaseExtensionType, Bool8Type, UuidType, JsonType,
    _BasicValueT,
    _DataTypeT,
    _IndexT,
    _K,
    _Precision,
    _RunEndType,
    _Scale,
    _Size,
    _Time32Unit,
    _Time64Unit,
    _Tz,
    _Unit,
    _ValueT,
)

_AsPyTypeK = TypeVar("_AsPyTypeK")
_AsPyTypeV = TypeVar("_AsPyTypeV")
_DataType_co = TypeVar("_DataType_co", bound=DataType, covariant=True)


class Scalar(_Weakrefable, Generic[_DataType_co]):

    @property
    def type(self) -> _DataType_co: ...

    @property
    def is_valid(self) -> bool: ...

    def cast(
        self,
        target_type: None | _DataTypeT,
        safe: bool = True,
        options: CastOptions | None = None,
        memory_pool: MemoryPool | None = None,
    ) -> Self | Scalar[_DataTypeT]: ...

    def validate(self, *, full: bool = False) -> None: ...

    def equals(self, other: Scalar) -> bool: ...

    def __hash__(self) -> int: ...

    def as_py(self: Scalar[Any], *, maps_as_pydicts: Literal["lossy",
              "strict"] | None = None) -> Any: ...


_NULL: TypeAlias = None
NA = _NULL


class NullScalar(Scalar[NullType]):
    ...


class BooleanScalar(Scalar[BoolType]):
    ...


class UInt8Scalar(Scalar[UInt8Type]):
    ...


class Int8Scalar(Scalar[Int8Type]):
    ...


class UInt16Scalar(Scalar[UInt16Type]):
    ...


class Int16Scalar(Scalar[Int16Type]):
    ...


class UInt32Scalar(Scalar[Uint32Type]):
    ...


class Int32Scalar(Scalar[Int32Type]):
    ...


class UInt64Scalar(Scalar[UInt64Type]):
    ...


class Int64Scalar(Scalar[Int64Type]):
    ...


class HalfFloatScalar(Scalar[Float16Type]):
    ...


class FloatScalar(Scalar[Float32Type]):
    ...


class DoubleScalar(Scalar[Float64Type]):
    ...


class Decimal32Scalar(Scalar[Decimal32Type[_Precision, _Scale]]):
    ...


class Decimal64Scalar(Scalar[Decimal64Type[_Precision, _Scale]]):
    ...


class Decimal128Scalar(Scalar[Decimal128Type[_Precision, _Scale]]):
    ...


class Decimal256Scalar(Scalar[Decimal256Type[_Precision, _Scale]]):
    ...


class Date32Scalar(Scalar[Date32Type]):
    ...


class Date64Scalar(Scalar[Date64Type]):

    @property
    def value(self) -> dt.date | None: ...


class Time32Scalar(Scalar[Time32Type[_Time32Unit]]):

    @property
    def value(self) -> dt.time | None: ...


class Time64Scalar(Scalar[Time64Type[_Time64Unit]]):

    @property
    def value(self) -> dt.time | None: ...


class TimestampScalar(Scalar[TimestampType[_Unit, _Tz]]):

    @property
    def value(self) -> int | None: ...


class DurationScalar(Scalar[DurationType[_Unit]]):

    @property
    def value(self) -> dt.timedelta | None: ...


class MonthDayNanoIntervalScalar(Scalar[MonthDayNanoIntervalType]):

    @property
    def value(self) -> MonthDayNano | None: ...


class BinaryScalar(Scalar[BinaryType]):

    def as_buffer(self) -> Buffer: ...


class LargeBinaryScalar(Scalar[LargeBinaryType]):

    def as_buffer(self) -> Buffer: ...


class FixedSizeBinaryScalar(Scalar[FixedSizeBinaryType]):

    def as_buffer(self) -> Buffer: ...


class StringScalar(Scalar[StringType]):

    def as_buffer(self) -> Buffer: ...


class LargeStringScalar(Scalar[LargeStringType]):

    def as_buffer(self) -> Buffer: ...


class BinaryViewScalar(Scalar[BinaryViewType]):

    def as_buffer(self) -> Buffer: ...


class StringViewScalar(Scalar[StringViewType]):

    def as_buffer(self) -> Buffer: ...


class ListScalar(Scalar[ListType[_DataTypeT]]):

    @property
    def values(self) -> Array | None: ...
    def __len__(self) -> int: ...

    def __getitem__(self, i: int) -> Scalar[_DataTypeT]: ...

    def __iter__(self) -> Iterator[Array]: ...


class FixedSizeListScalar(Scalar[FixedSizeListType[_DataTypeT, _Size]]):

    @property
    def values(self) -> Array | None: ...
    def __len__(self) -> int: ...

    def __getitem__(self, i: int) -> Scalar[_DataTypeT]: ...

    def __iter__(self) -> Iterator[Array]: ...


class LargeListScalar(Scalar[LargeListType[_DataTypeT]]):

    @property
    def values(self) -> Array | None: ...
    def __len__(self) -> int: ...

    def __getitem__(self, i: int) -> Scalar[_DataTypeT]: ...

    def __iter__(self) -> Iterator[Array]: ...


class ListViewScalar(Scalar[ListViewType[_DataTypeT]]):

    @property
    def values(self) -> Array | None: ...
    def __len__(self) -> int: ...

    def __getitem__(self, i: int) -> Scalar[_DataTypeT]: ...

    def __iter__(self) -> Iterator[Array]: ...


class LargeListViewScalar(Scalar[LargeListViewType[_DataTypeT]]):

    @property
    def values(self) -> Array | None: ...
    def __len__(self) -> int: ...

    def __getitem__(self, i: int) -> Scalar[_DataTypeT]: ...

    def __iter__(self) -> Iterator[Array]: ...


class StructScalar(Scalar[StructType], collections.abc.Mapping[str, Scalar]):

    def __len__(self) -> int: ...

    def __iter__(self) -> Iterator[str]: ...

    def __getitem__(self, key: int | str) -> Scalar[Any]: ...

    def _as_py_tuple(self) -> list[tuple[str, Any]]: ...


class MapScalar(Scalar[MapType[_K, _ValueT]]):

    @property
    def values(self) -> Array | None: ...
    def __len__(self) -> int: ...

    def __getitem__(self, i: int) -> tuple[Scalar[_K], _ValueT, Any]: ...

    def __iter__(self: Scalar[
        MapType[_BasicDataType[_AsPyTypeK], _BasicDataType[_AsPyTypeV]]]
        | Scalar[MapType[Any, _BasicDataType[_AsPyTypeV]]]
        | Scalar[MapType[_BasicDataType[_AsPyTypeK], Any]]) -> (
        Iterator[tuple[_AsPyTypeK, _AsPyTypeV]]
        | Iterator[tuple[Any, _AsPyTypeV]]
        | Iterator[tuple[_AsPyTypeK, Any]]
    ): ...


class DictionaryScalar(Scalar[DictionaryType[_IndexT, _BasicValueT]]):

    @property
    def index(self) -> Scalar[_IndexT]: ...

    @property
    def value(self) -> Scalar[_BasicValueT]: ...

    @property
    def dictionary(self) -> Array: ...


class RunEndEncodedScalar(Scalar[RunEndEncodedType[_RunEndType, _BasicValueT]]):

    @property
    def value(self) -> tuple[int, _BasicValueT] | None: ...


class UnionScalar(Scalar[UnionType]):

    @property
    def value(self) -> Any | None: ...

    @property
    def type_code(self) -> str: ...


class ExtensionScalar(Scalar[ExtensionType]):

    @property
    def value(self) -> Any | None: ...

    @staticmethod
    def from_storage(typ: BaseExtensionType, value) -> ExtensionScalar: ...


class Bool8Scalar(Scalar[Bool8Type]):
    ...


class UuidScalar(Scalar[UuidType]):
    ...


class JsonScalar(Scalar[JsonType]):
    ...


class OpaqueScalar(Scalar[OpaqueType]):
    ...


class FixedShapeTensorScalar(ExtensionScalar):

    def to_numpy(self) -> np.ndarray: ...

    def to_tensor(self) -> Tensor: ...


def scalar(
    value: Any,
    type: _DataTypeT,
    *,
    from_pandas: bool | None = None,
    memory_pool: MemoryPool | None = None,
) -> Scalar[_DataTypeT]: ...


__all__ = [
    "Scalar",
    "_NULL",
    "NA",
    "NullScalar",
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
    "StringScalar",
    "LargeStringScalar",
    "BinaryViewScalar",
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
    "FixedShapeTensorScalar",
    "Bool8Scalar",
    "UuidScalar",
    "JsonScalar",
    "OpaqueScalar",
    "scalar",
]
