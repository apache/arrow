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

import sys

from collections.abc import Callable

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self
from collections.abc import Iterable, Iterator
from typing import (
    Any,
    Generic,
    Literal,
    TypeVar,
)

import numpy as np
import pandas as pd

from pandas.core.dtypes.base import ExtensionDtype
from pyarrow._compute import CastOptions
from pyarrow._stubs_typing import (
    ArrayLike,
    Indices,
    Mask,
    Order,
    SupportArrowArray,
    SupportArrowDeviceArray,
)
from pyarrow.lib import (
    Buffer,
    Device,
    MemoryManager,
    MemoryPool,
    Tensor,
    _Weakrefable,
)
from typing_extensions import deprecated
import builtins

from .scalar import (  # noqa: F401
    BinaryScalar,
    BinaryViewScalar,
    BooleanScalar,
    Date32Scalar,
    Date64Scalar,
    DictionaryScalar,
    DoubleScalar,
    DurationScalar,
    ExtensionScalar,
    FixedSizeBinaryScalar,
    FixedSizeListScalar,
    FloatScalar,
    HalfFloatScalar,
    Int16Scalar,
    Int32Scalar,
    Int64Scalar,
    Int8Scalar,
    LargeBinaryScalar,
    LargeListScalar,
    LargeStringScalar,
    ListScalar,
    ListViewScalar,
    MapScalar,
    MonthDayNanoIntervalScalar,
    NullScalar,
    RunEndEncodedScalar,
    Scalar,
    StringScalar,
    StringViewScalar,
    StructScalar,
    Time32Scalar,
    Time64Scalar,
    TimestampScalar,
    UInt16Scalar,
    UInt32Scalar,
    UInt64Scalar,
    UInt8Scalar,
    UnionScalar,
)
from .device import DeviceAllocationType
from ._types import (  # noqa: F401
    BaseExtensionType,
    BinaryType,
    DataType,
    Field,
    Float64Type,
    Int64Type,
    MapType,
    StringType,
    StructType,
    _AsPyType,
    _BasicDataType,
    _BasicValueT,
    _DataTypeT,
    _IndexT,
    _RunEndType,
    _Size,
    _Time32Unit,
    _Time64Unit,
    _Tz,
    _Unit,
)
from ._stubs_typing import NullableCollection


def array(
    values: NullableCollection[Any] | Iterable[Any] | SupportArrowArray
    | SupportArrowDeviceArray,
    type: Any | None = None,
    mask: Mask | None = None,
    size: int | None = None,
    from_pandas: bool | None = None,
    safe: bool = True,
    memory_pool: MemoryPool | None = None,
) -> ArrayLike: ...


def asarray(
    values: NullableCollection[Any] | Iterable[Any] | SupportArrowArray
    | SupportArrowDeviceArray,
    type: _DataTypeT | Any | None = None,
) -> Array[Scalar[_DataTypeT]] | ArrayLike: ...


def nulls(
    size: int,
    type: Any | None = None,
    memory_pool: MemoryPool | None = None,
) -> ArrayLike: ...


def repeat(
    value: Any,
    size: int,
    memory_pool: MemoryPool | None = None,
) -> ArrayLike: ...


def infer_type(values: Iterable[Any], mask: Mask,
               from_pandas: bool = False) -> DataType: ...


class ArrayStatistics(_Weakrefable):

    @property
    def null_count(self) -> int: ...

    @property
    def distinct_count(self) -> int: ...

    @property
    def min(self) -> Any: ...

    @property
    def is_min_exact(self) -> bool: ...

    @property
    def max(self) -> Any: ...

    @property
    def is_max_exact(self) -> bool: ...


_ConvertAs = TypeVar("_ConvertAs", pd.DataFrame, pd.Series)


class _PandasConvertible(_Weakrefable, Generic[_ConvertAs]):
    def to_pandas(
        self,
        memory_pool: MemoryPool | None = None,
        categories: list | None = None,
        strings_to_categorical: bool = False,
        zero_copy_only: bool = False,
        integer_object_nulls: bool = False,
        date_as_object: bool = True,
        timestamp_as_object: bool = False,
        use_threads: bool = True,
        deduplicate_objects: bool = True,
        ignore_metadata: bool = False,
        safe: bool = True,
        split_blocks: bool = False,
        self_destruct: bool = False,
        maps_as_pydicts: Literal["None", "lossy", "strict"] | None = None,
        types_mapper: Callable[[DataType], ExtensionDtype | None] | None = None,
        coerce_temporal_nanoseconds: bool = False,
    ) -> _ConvertAs: ...


_CastAs = TypeVar("_CastAs", bound=DataType)
_Scalar_co = TypeVar("_Scalar_co", bound=Scalar, covariant=True)
_ScalarT = TypeVar("_ScalarT", bound=Scalar)


class Array(_PandasConvertible[pd.Series], Generic[_Scalar_co]):

    def as_py(self) -> list[Any]: ...

    def diff(self, other: Self) -> str: ...

    def cast(
        self,
        target_type: _CastAs,
        safe: bool = True,
        options: CastOptions | None = None,
        memory_pool: MemoryPool | None = None,
    ) -> Array[Scalar[_CastAs]]: ...

    def view(self, target_type: _CastAs) -> Array[Scalar[_CastAs]]: ...

    def sum(self, **kwargs) -> _Scalar_co: ...

    @property
    def type(self: Array[Scalar[_DataTypeT]]) -> _DataTypeT: ...
    def unique(self) -> Self: ...

    def dictionary_encode(self, null_encoding: str = "mask") -> DictionaryArray: ...

    def value_counts(self) -> StructArray: ...

    @staticmethod
    def from_pandas(
        obj: pd.Series | np.ndarray | ArrayLike,
        *,
        mask: Mask | None = None,
        type: _DataTypeT | None = None,
        safe: bool = True,
        memory_pool: MemoryPool | None = None,
    ) -> Array[Scalar[_DataTypeT]] | Array[Scalar]: ...

    @staticmethod
    def from_buffers(
        type: _DataTypeT,
        length: int,
        buffers: list[Buffer],
        null_count: int = -1,
        offset=0,
        children: NullableCollection[Array[Scalar[_DataTypeT]]] | None = None,
    ) -> Array[Scalar[_DataTypeT]]: ...

    @property
    def null_count(self) -> int: ...
    @property
    def nbytes(self) -> int: ...

    def get_total_buffer_size(self) -> int: ...

    def __sizeof__(self) -> int: ...
    def __iter__(self) -> Iterator[_Scalar_co]: ...

    def to_string(
        self,
        *,
        indent: int = 2,
        top_level_indent: int = 0,
        window: int = 10,
        container_window: int = 2,
        skip_new_lines: bool = False,
    ) -> str: ...

    format = to_string
    def equals(self, other: Self) -> bool: ...

    def __len__(self) -> int: ...

    def is_null(self, *, nan_is_null: bool = False) -> BooleanArray: ...

    def is_nan(self) -> BooleanArray: ...

    def is_valid(self) -> BooleanArray: ...

    def fill_null(
        self: Array[Scalar[_BasicDataType[_AsPyType]]], fill_value: _AsPyType
    ) -> Array[Scalar[_BasicDataType[_AsPyType]]]: ...

    def __getitem__(self, key: int | builtins.slice) -> _Scalar_co | Self: ...

    def slice(self, offset: int = 0, length: int | None = None) -> Self: ...

    def take(self, indices: Indices) -> Self: ...

    def drop_null(self) -> Self: ...

    def filter(
        self,
        mask: Mask,
        *,
        null_selection_behavior: Literal["drop", "emit_null"] = "drop",
    ) -> Self: ...

    def index(
        self: Array[_ScalarT] | Array[Scalar[_BasicDataType[_AsPyType]]],
        value: _ScalarT | _AsPyType,
        start: int | None = None,
        end: int | None = None,
        *,
        memory_pool: MemoryPool | None = None,
    ) -> Int64Scalar: ...

    def sort(self, order: Order = "ascending", **kwargs) -> Self: ...

    def __array__(self, dtype: np.dtype | None = None,
                  copy: bool | None = None) -> np.ndarray: ...

    def to_numpy(self, zero_copy_only: bool = True,
                 writable: bool = False) -> np.ndarray: ...

    def to_pylist(
        self: Array[Scalar[_BasicDataType[_AsPyType]]],
        *,
        maps_as_pydicts: Literal["lossy", "strict"] | None = None,
    ) -> list[_AsPyType | None]: ...

    tolist = to_pylist
    def validate(self, *, full: bool = False) -> None: ...

    @property
    def offset(self) -> int: ...

    def buffers(self) -> list[Buffer | None]: ...

    def copy_to(self, destination: MemoryManager | Device) -> Self: ...

    def _export_to_c(self, out_ptr: int, out_schema_ptr: int = 0) -> None: ...

    @classmethod
    def _import_from_c(cls, in_ptr: int, type: int | DataType) -> Self: ...

    def __arrow_c_array__(self, requested_schema=None) -> Any: ...

    @classmethod
    def _import_from_c_capsule(cls, schema_capsule, array_capsule) -> Self: ...
    def _export_to_c_device(self, out_ptr: int, out_schema_ptr: int = 0) -> None: ...

    @classmethod
    def _import_from_c_device(cls, in_ptr: int, type: DataType | int) -> Self: ...

    def __arrow_c_device_array__(self, requested_schema=None, **kwargs) -> Any: ...

    @classmethod
    def _import_from_c_device_capsule(cls, schema_capsule, array_capsule) -> Self: ...
    def __dlpack__(self, stream: int | None = None) -> Any: ...

    def __dlpack_device__(self) -> tuple[int, int]: ...

    @property
    def device_type(self) -> DeviceAllocationType: ...

    @property
    def is_cpu(self) -> bool: ...

    @property
    def statistics(self) -> ArrayStatistics | None: ...


class NullArray(Array[NullScalar]):
    ...


class BooleanArray(Array[BooleanScalar]):

    @property
    def false_count(self) -> int: ...
    @property
    def true_count(self) -> int: ...


class NumericArray(Array[_ScalarT]):
    ...


class IntegerArray(NumericArray[_ScalarT]):
    ...


class FloatingPointArray(NumericArray[_ScalarT]):
    ...


class Int8Array(IntegerArray[Int8Scalar]):
    ...


class UInt8Array(IntegerArray[UInt8Scalar]):
    ...


class Int16Array(IntegerArray[Int16Scalar]):
    ...


class UInt16Array(IntegerArray[UInt16Scalar]):
    ...


class Int32Array(IntegerArray[Int32Scalar]):
    ...


class UInt32Array(IntegerArray[UInt32Scalar]):
    ...


class Int64Array(IntegerArray[Int64Scalar]):
    ...


class UInt64Array(IntegerArray[UInt64Scalar]):
    ...


class Date32Array(NumericArray[Date32Scalar]):
    ...


class Date64Array(NumericArray[Date64Scalar]):
    ...


class TimestampArray(NumericArray[TimestampScalar[_Unit, _Tz]]):
    ...


class Time32Array(NumericArray[Time32Scalar[_Time32Unit]]):
    ...


class Time64Array(NumericArray[Time64Scalar[_Time64Unit]]):
    ...


class DurationArray(NumericArray[DurationScalar[_Unit]]):
    ...


class MonthDayNanoIntervalArray(Array[MonthDayNanoIntervalScalar]):
    ...


class HalfFloatArray(FloatingPointArray[HalfFloatScalar]):
    ...


class FloatArray(FloatingPointArray[FloatScalar]):
    ...


class DoubleArray(FloatingPointArray[DoubleScalar]):
    ...


class FixedSizeBinaryArray(Array[FixedSizeBinaryScalar]):
    ...


class Decimal32Array(FixedSizeBinaryArray):
    ...


class Decimal64Array(FixedSizeBinaryArray):
    ...


class Decimal128Array(FixedSizeBinaryArray):
    ...


class Decimal256Array(FixedSizeBinaryArray):
    ...


class BaseListArray(Array[_ScalarT]):
    def flatten(self, recursive: bool = False) -> Array: ...

    def value_parent_indices(self) -> Int64Array: ...

    def value_lengths(self) -> Int32Array: ...


class ListArray(BaseListArray[_ScalarT]):

    @classmethod
    def from_arrays(
        cls,
        offsets: Int32Array | list[int],
        values: Array[Scalar[_DataTypeT]] | list[int] | list[float] | list[str]
        | list[bytes] | list,
        *,
        type: _DataTypeT | None = None,
        pool: MemoryPool | None = None,
        mask: Mask | None = None,
    ) -> (ListArray[ListScalar[
        _DataTypeT | Int64Type | Float64Type | StringType | BinaryType
    ]] | ListArray): ...

    @property
    def values(self) -> Array: ...

    @property
    def offsets(self) -> Int32Array: ...


class LargeListArray(BaseListArray[LargeListScalar[_DataTypeT]]):

    @classmethod
    def from_arrays(
        cls,
        offsets: Int64Array,
        values: Array[Scalar[_DataTypeT]] | Array,
        *,
        type: _DataTypeT | None = None,
        pool: MemoryPool | None = None,
        mask: Mask | None = None,
    ) -> LargeListArray[_DataTypeT]: ...

    @property
    def values(self) -> Array: ...

    @property
    def offsets(self) -> Int64Array: ...


class ListViewArray(BaseListArray[ListViewScalar[_DataTypeT]]):

    @classmethod
    def from_arrays(
        cls,
        offsets: Int32Array,
        values: Array[Scalar[_DataTypeT]] | Array,
        *,
        type: _DataTypeT | None = None,
        pool: MemoryPool | None = None,
        mask: Mask | None = None,
    ) -> ListViewArray[_DataTypeT]: ...

    @property
    def values(self) -> Array: ...

    @property
    def offsets(self) -> Int32Array: ...

    @property
    def sizes(self) -> Int32Array: ...


class LargeListViewArray(BaseListArray[LargeListScalar[_DataTypeT]]):

    @classmethod
    def from_arrays(
        cls,
        offsets: Int64Array,
        values: Array[Scalar[_DataTypeT]] | Array,
        *,
        type: _DataTypeT | None = None,
        pool: MemoryPool | None = None,
        mask: Mask | None = None,
    ) -> LargeListViewArray[_DataTypeT]: ...

    @property
    def values(self) -> Array: ...

    @property
    def offsets(self) -> Int64Array: ...

    @property
    def sizes(self) -> Int64Array: ...


class FixedSizeListArray(BaseListArray[FixedSizeListScalar[_DataTypeT, _Size]]):

    @classmethod
    def from_arrays(
        cls,
        values: Array[Scalar[_DataTypeT]],
        limit_size: _Size | None = None,
        *,
        type: None = None,
        mask: Mask | None = None,
    ) -> FixedSizeListArray[_DataTypeT, _Size | None]: ...

    @property
    def values(self) -> BaseListArray[ListScalar[_DataTypeT]]: ...


_MapKeyT = TypeVar("_MapKeyT", bound=_BasicDataType)
_MapItemT = TypeVar("_MapItemT", bound=_BasicDataType)


class MapArray(BaseListArray[MapScalar[_MapKeyT, _MapItemT]]):

    @classmethod
    def from_arrays(
        cls,
        offsets: Int64Array | list[int] | None,
        keys: Array[Scalar[_MapKeyT]] | None = None,
        items: Array[Scalar[_MapItemT]] | None = None,
        values: Array | None = None,
        *,
        type: MapType[_MapKeyT, _MapItemT] | None = None,
        pool: MemoryPool | None = None,
        mask: Mask | None = None,
    ) -> MapArray[_MapKeyT, _MapItemT]: ...

    @property
    def keys(self) -> Array: ...

    @property
    def items(self) -> Array: ...


class UnionArray(Array[UnionScalar]):

    @deprecated("Use fields() instead")
    def child(self, pos: int) -> Field: ...

    def field(self, pos: int) -> Array: ...

    @property
    def type_codes(self) -> Int8Array: ...

    @property
    def offsets(self) -> Int32Array: ...

    @staticmethod
    def from_dense(
        types: Int8Array,
        value_offsets: Int32Array,
        children: NullableCollection[Array],
        field_names: list[str] | None = None,
        type_codes: Int8Array | None = None,
    ) -> UnionArray: ...

    @staticmethod
    def from_sparse(
        types: Int8Array,
        children: NullableCollection[Array],
        field_names: list[str] | None = None,
        type_codes: Int8Array | None = None,
    ) -> UnionArray: ...


class StringArray(Array[StringScalar]):

    @staticmethod
    def from_buffers(  # type: ignore[override]
        length: int,
        value_offsets: Buffer,
        data: Buffer,
        null_bitmap: Buffer | None = None,
        null_count: int | None = -1,
        offset: int | None = 0,
    ) -> StringArray: ...


class LargeStringArray(Array[LargeStringScalar]):

    @staticmethod
    def from_buffers(  # type: ignore[override]
        length: int,
        value_offsets: Buffer,
        data: Buffer,
        null_bitmap: Buffer | None = None,
        null_count: int | None = -1,
        offset: int | None = 0,
    ) -> StringArray: ...


class StringViewArray(Array[StringViewScalar]):
    ...


class BinaryArray(Array[BinaryScalar]):

    @property
    def total_values_length(self) -> int: ...


class LargeBinaryArray(Array[LargeBinaryScalar]):

    @property
    def total_values_length(self) -> int: ...


class BinaryViewArray(Array[BinaryViewScalar]):
    ...


class DictionaryArray(Array[DictionaryScalar[_IndexT, _BasicValueT]]):

    def dictionary_encode(self) -> Self: ...  # type: ignore[override]
    def dictionary_decode(self) -> Array[Scalar[_BasicValueT]]: ...

    @property
    def indices(self) -> Array[Scalar[_IndexT]]: ...
    @property
    def dictionary(self) -> Array[Scalar[_BasicValueT]]: ...

    @staticmethod
    def from_buffers(  # type: ignore[override]
        type: _BasicValueT,
        length: int,
        buffers: list[Buffer],
        dictionary: Array | np.ndarray | pd.Series,
        null_count: int = -1,
        offset: int = 0,
    ) -> DictionaryArray[Any, _BasicValueT]: ...

    @staticmethod
    def from_arrays(
        indices: Indices,
        dictionary: Array | np.ndarray | pd.Series,
        mask: np.ndarray | pd.Series | BooleanArray | None = None,
        ordered: bool = False,
        from_pandas: bool = False,
        safe: bool = True,
        memory_pool: MemoryPool | None = None,
    ) -> DictionaryArray: ...


class StructArray(Array[StructScalar]):

    def field(self, index: int | str) -> Array: ...

    def flatten(self, memory_pool: MemoryPool | None = None) -> list[Array]: ...

    @staticmethod
    def from_arrays(
        arrays: Iterable[Array],
        names: list[str] | None = None,
        fields: list[Field] | None = None,
        mask=None,
        memory_pool: MemoryPool | None = None,
        type: StructType | None = None,
    ) -> StructArray: ...

    def sort(self, order: Order = "ascending", by: str |
             None = None, **kwargs) -> StructArray: ...


class RunEndEncodedArray(Array[RunEndEncodedScalar[_RunEndType, _BasicValueT]]):

    @staticmethod
    def from_arrays(
        run_ends: Int16Array | Int32Array | Int64Array,
        values: Array, type: DataType | None = None,
    ) -> RunEndEncodedArray[Any, _BasicValueT]: ...

    @staticmethod
    def from_buffers(  # type: ignore[override]
        type: DataType,
        length: int,
        buffers: list[Buffer],
        null_count: int = -1,
        offset=0,
        children: tuple[Array, Array] | None = None,
    ) -> RunEndEncodedArray[Any, _BasicValueT]: ...

    @property
    def run_ends(self) -> Array[Scalar[_RunEndType]]: ...

    @property
    def values(self) -> Array[Scalar[_BasicValueT]]: ...

    def find_physical_offset(self) -> int: ...

    def find_physical_length(self) -> int: ...


_ArrayT = TypeVar("_ArrayT", bound=Array)


class ExtensionArray(Array[ExtensionScalar], Generic[_ArrayT]):

    @property
    def storage(self) -> Any: ...

    @staticmethod
    def from_storage(typ: BaseExtensionType,
                     storage: _ArrayT) -> ExtensionArray[_ArrayT]: ...


class JsonArray(ExtensionArray[_ArrayT]):
    ...


class UuidArray(ExtensionArray[_ArrayT]):
    ...


class FixedShapeTensorArray(ExtensionArray[_ArrayT]):

    def to_numpy_ndarray(self) -> np.ndarray: ...

    def to_tensor(self) -> Tensor: ...

    @classmethod
    def from_numpy_ndarray(cls, obj: np.ndarray) -> Self: ...


class OpaqueArray(ExtensionArray[_ArrayT]):
    ...


class Bool8Array(ExtensionArray):

    def to_numpy(self, zero_copy_only: bool = ...,
                 writable: bool = ...) -> np.ndarray: ...

    @classmethod
    def from_storage(cls, storage: Int8Array) -> Self: ...  # type: ignore[override]

    @classmethod
    def from_numpy(cls, obj: np.ndarray) -> Self: ...


def concat_arrays(arrays: Iterable[_ArrayT],
                  memory_pool: MemoryPool | None = None) -> _ArrayT: ...


def _empty_array(type: _DataTypeT) -> Array[Scalar[_DataTypeT]]: ...


__all__ = [
    "array",
    "asarray",
    "nulls",
    "repeat",
    "infer_type",
    "_PandasConvertible",
    "Array",
    "NullArray",
    "BooleanArray",
    "NumericArray",
    "IntegerArray",
    "FloatingPointArray",
    "Int8Array",
    "UInt8Array",
    "Int16Array",
    "UInt16Array",
    "Int32Array",
    "UInt32Array",
    "Int64Array",
    "UInt64Array",
    "Date32Array",
    "Date64Array",
    "TimestampArray",
    "Time32Array",
    "Time64Array",
    "DurationArray",
    "MonthDayNanoIntervalArray",
    "HalfFloatArray",
    "FloatArray",
    "DoubleArray",
    "FixedSizeBinaryArray",
    "Decimal32Array",
    "Decimal64Array",
    "Decimal128Array",
    "Decimal256Array",
    "BaseListArray",
    "ListArray",
    "LargeListArray",
    "ListViewArray",
    "LargeListViewArray",
    "FixedSizeListArray",
    "MapArray",
    "UnionArray",
    "StringArray",
    "LargeStringArray",
    "StringViewArray",
    "BinaryArray",
    "LargeBinaryArray",
    "BinaryViewArray",
    "DictionaryArray",
    "StructArray",
    "RunEndEncodedArray",
    "ExtensionArray",
    "Bool8Array",
    "UuidArray",
    "JsonArray",
    "OpaqueArray",
    "FixedShapeTensorArray",
    "concat_arrays",
    "_empty_array",
    "_CastAs",
]
