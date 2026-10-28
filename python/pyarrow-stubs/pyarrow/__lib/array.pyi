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

from collections.abc import Iterable, Iterator, Sequence

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

from typing import Any, Literal

import numpy as np
import pandas as pd

from pyarrow._compute import CastOptions
from pyarrow._stubs_typing import (
    ArrayLike,
    Indices,
    Mask,
    Order,
    SupportsArrowArray,
    SupportsArrowDeviceArray,
    SupportsPyArrowArray,
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

from .scalar import Int64Scalar, Scalar
from .device import DeviceAllocationType
from pyarrow._types import BaseExtensionType, DataType, Field, StructType
from pyarrow._stubs_typing import NullableCollection

def array(
    values: NullableCollection
    | Iterable[Any]
    | SupportsArrowArray
    | SupportsArrowDeviceArray
    | SupportsPyArrowArray,
    type: Any | None = None,
    mask: Mask | pd.Series[bool] | None = None,
    size: int | None = None,
    from_pandas: bool | None = None,
    safe: bool = True,
    memory_pool: MemoryPool | None = None,
) -> ArrayLike: ...
def asarray(
    values: NullableCollection
    | Iterable[Any]
    | SupportsArrowArray
    | SupportsArrowDeviceArray,
    type: Any | None = None,
) -> ArrayLike: ...
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
def infer_type(
    values: Iterable[Any], mask: Mask | None = None, from_pandas: bool = False
) -> DataType: ...

class ArrayStatistics(_Weakrefable):
    @property
    def null_count(self) -> int | None: ...
    @property
    def distinct_count(self) -> int | None: ...
    @property
    def is_null_count_exact(self) -> bool | None: ...
    @property
    def is_distinct_count_exact(self) -> bool | None: ...
    @property
    def min(self) -> Any | None: ...
    @property
    def is_min_exact(self) -> bool | None: ...
    @property
    def max(self) -> Any | None: ...
    @property
    def is_max_exact(self) -> bool | None: ...

class _PandasConvertible(_Weakrefable):
    def to_pandas(
        self,
        memory_pool: MemoryPool | None = None,
        categories: list | tuple | None = None,
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
        types_mapper: Any = None,  # Callable[[DataType], ExtensionDtype | None] | None
        coerce_temporal_nanoseconds: bool = False,
    ) -> pd.Series | pd.DataFrame: ...

class Array(_PandasConvertible):
    def diff(self, other: Self) -> str: ...

    # Private attribute used internally (e.g., for column names in batches)
    _name: str | None

    def cast(
        self,
        target_type: DataType | str,
        safe: bool = True,
        options: CastOptions | None = None,
        memory_pool: MemoryPool | None = None,
    ) -> Array: ...
    def view(self, target_type: DataType) -> Array: ...
    def sum(self, **kwargs) -> Scalar: ...
    @property
    def type(self) -> DataType: ...
    def unique(self) -> Self: ...
    def dictionary_encode(self, null_encoding: str = "mask") -> DictionaryArray: ...
    def value_counts(self) -> StructArray: ...
    @staticmethod
    def from_pandas(
        obj: pd.Series | np.ndarray | ArrayLike,
        mask: Mask | None = None,
        type: DataType | None = None,
        safe: bool = True,
        memory_pool: MemoryPool | None = None,
    ) -> Array: ...
    @staticmethod
    def from_buffers(
        type: DataType,
        length: int,
        buffers: Sequence[Buffer | None],
        null_count: int = -1,
        offset=0,
        children: NullableCollection | None = None,
    ) -> Array: ...
    @property
    def null_count(self) -> int: ...
    @property
    def nbytes(self) -> int: ...
    def get_total_buffer_size(self) -> int: ...
    def __sizeof__(self) -> int: ...
    def __iter__(self) -> Iterator[Scalar]: ...
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
    def equals(self, other: Array | Any) -> bool: ...
    def __len__(self) -> int: ...
    def is_null(self, *, nan_is_null: bool = False) -> BooleanArray: ...
    def is_nan(self) -> BooleanArray: ...
    def is_valid(self) -> BooleanArray: ...
    def fill_null(self, fill_value: Any) -> Self: ...
    def __getitem__(self, key: int | builtins.slice) -> Scalar | Self: ...
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
        self,
        value: Any,
        start: int | None = None,
        end: int | None = None,
        *,
        memory_pool: MemoryPool | None = None,
    ) -> Int64Scalar: ...
    def sort(self, order: Order = "ascending", **kwargs) -> Self: ...
    def __array__(
        self, dtype: np.dtype | None = None, copy: bool | None = None
    ) -> np.ndarray: ...
    def to_numpy(
        self, zero_copy_only: bool = True, writable: bool = False
    ) -> np.ndarray: ...
    def to_pylist(
        self,
        *,
        maps_as_pydicts: Literal["lossy", "strict"] | None = None,
    ) -> list[Any]: ...

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

class NullArray(Array): ...

class BooleanArray(Array):
    @property
    def false_count(self) -> int: ...
    @property
    def true_count(self) -> int: ...

class NumericArray(Array): ...
class IntegerArray(NumericArray): ...
class FloatingPointArray(NumericArray): ...
class Int8Array(IntegerArray): ...
class UInt8Array(IntegerArray): ...
class Int16Array(IntegerArray): ...
class UInt16Array(IntegerArray): ...
class Int32Array(IntegerArray): ...
class UInt32Array(IntegerArray): ...
class Int64Array(IntegerArray): ...
class UInt64Array(IntegerArray): ...
class Date32Array(NumericArray): ...
class Date64Array(NumericArray): ...
class TimestampArray(NumericArray): ...
class Time32Array(NumericArray): ...
class Time64Array(NumericArray): ...
class DurationArray(NumericArray): ...
class MonthDayNanoIntervalArray(Array): ...
class HalfFloatArray(FloatingPointArray): ...
class FloatArray(FloatingPointArray): ...
class DoubleArray(FloatingPointArray): ...
class FixedSizeBinaryArray(Array): ...
class Decimal32Array(FixedSizeBinaryArray): ...
class Decimal64Array(FixedSizeBinaryArray): ...
class Decimal128Array(FixedSizeBinaryArray): ...
class Decimal256Array(FixedSizeBinaryArray): ...

class BaseListArray(Array):
    def flatten(self, recursive: bool = False) -> Array: ...
    def value_parent_indices(self) -> Int64Array: ...
    def value_lengths(self) -> Int32Array | Int64Array: ...

class ListArray(BaseListArray):
    @classmethod
    def from_arrays(
        cls,
        offsets: Int32Array | list[int] | list[int | None],
        values: Array | list[Any],
        *,
        type: DataType | None = None,
        pool: MemoryPool | None = None,
        mask: Mask | None = None,
    ) -> ListArray: ...
    @property
    def values(self) -> Array: ...
    @property
    def offsets(self) -> Int32Array: ...
    def value_lengths(self) -> Int32Array: ...

class LargeListArray(BaseListArray):
    @classmethod
    def from_arrays(
        cls,
        offsets: Int64Array | list[int] | list[int | None],
        values: Array,
        *,
        type: DataType | None = None,
        pool: MemoryPool | None = None,
        mask: Mask | None = None,
    ) -> LargeListArray: ...
    @property
    def values(self) -> Array: ...
    @property
    def offsets(self) -> Int64Array: ...
    def value_lengths(self) -> Int64Array: ...

class ListViewArray(BaseListArray):
    @classmethod
    def from_arrays(
        cls,
        offsets: Int32Array | list[int] | list[int | None],
        sizes: Int32Array | list[int] | list[int | None],
        values: Array,
        *,
        type: DataType | None = None,
        pool: MemoryPool | None = None,
        mask: Mask | None = None,
    ) -> ListViewArray: ...
    @property
    def values(self) -> Array: ...
    @property
    def offsets(self) -> Int32Array: ...
    @property
    def sizes(self) -> Int32Array: ...
    def value_lengths(self) -> Int32Array: ...

class LargeListViewArray(BaseListArray):
    @classmethod
    def from_arrays(
        cls,
        offsets: Int64Array | list[int] | list[int | None],
        sizes: Int64Array | list[int] | list[int | None],
        values: Array,
        *,
        type: DataType | None = None,
        pool: MemoryPool | None = None,
        mask: Mask | None = None,
    ) -> LargeListViewArray: ...
    @property
    def values(self) -> Array: ...
    @property
    def offsets(self) -> Int64Array: ...
    @property
    def sizes(self) -> Int64Array: ...
    def value_lengths(self) -> Int64Array: ...

class FixedSizeListArray(BaseListArray):
    @classmethod
    def from_arrays(
        cls,
        values: Array,
        list_size: int | None = None,
        *,
        type: DataType | None = None,
        mask: Mask | None = None,
    ) -> FixedSizeListArray: ...
    @property
    def values(self) -> Array: ...
    def value_lengths(self) -> Int32Array: ...

class MapArray(ListArray):
    @classmethod
    def from_arrays(  # type: ignore[override]
        cls,
        offsets: Int32Array | list[int] | list[int | None],
        keys: Array | np.ndarray | list,
        items: Array | np.ndarray | list,
        *,
        type: DataType | None = None,
        pool: MemoryPool | None = None,
        mask: Mask | None = None,
    ) -> MapArray: ...
    @property
    def keys(self) -> Array: ...
    @property
    def items(self) -> Array: ...

class UnionArray(Array):
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
        children: NullableCollection,
        field_names: list[str] | None = None,
        type_codes: Int8Array | list[int] | None = None,
    ) -> UnionArray: ...
    @staticmethod
    def from_sparse(
        types: Int8Array,
        children: NullableCollection,
        field_names: list[str] | None = None,
        type_codes: Int8Array | list[int] | None = None,
    ) -> UnionArray: ...

class StringArray(Array):
    @staticmethod
    def from_buffers(  # type: ignore[override]
        length: int,
        value_offsets: Buffer,
        data: Buffer,
        null_bitmap: Buffer | None = None,
        null_count: int | None = -1,
        offset: int | None = 0,
    ) -> StringArray: ...

class LargeStringArray(Array):
    @staticmethod
    def from_buffers(  # type: ignore[override]
        length: int,
        value_offsets: Buffer,
        data: Buffer,
        null_bitmap: Buffer | None = None,
        null_count: int | None = -1,
        offset: int | None = 0,
    ) -> LargeStringArray: ...

class StringViewArray(Array): ...

class BinaryArray(Array):
    @property
    def total_values_length(self) -> int: ...

class LargeBinaryArray(Array):
    @property
    def total_values_length(self) -> int: ...

class BinaryViewArray(Array): ...

class DictionaryArray(Array):
    def dictionary_encode(self) -> Self: ...  # type: ignore[override]
    def dictionary_decode(self) -> Array: ...
    @property
    def indices(self) -> IntegerArray: ...
    @property
    def dictionary(self) -> Array: ...
    @staticmethod
    def from_buffers(  # type: ignore[override]
        type: DataType,
        length: int,
        buffers: list[Buffer],
        dictionary: Array | np.ndarray | pd.Series,
        null_count: int = -1,
        offset: int = 0,
    ) -> DictionaryArray: ...
    @staticmethod
    def from_arrays(
        indices: Indices | Sequence[int | None],
        dictionary: Array | np.ndarray | pd.Series | list[Any],
        mask: np.ndarray | pd.Series | BooleanArray | None = None,
        ordered: bool = False,
        from_pandas: bool = False,
        safe: bool = True,
        memory_pool: MemoryPool | None = None,
    ) -> DictionaryArray: ...

class StructArray(Array):
    def field(self, index: int | str) -> Array: ...
    def flatten(self, memory_pool: MemoryPool | None = None) -> list[Array]: ...
    @staticmethod
    def from_arrays(
        arrays: Iterable[Array | np.ndarray | list],
        names: Sequence[str] | list[Field] | None = None,
        fields: list[Field] | None = None,
        mask=None,
        memory_pool: MemoryPool | None = None,
        type: StructType | None = None,
    ) -> StructArray: ...
    def sort(
        self, order: Order = "ascending", by: str | None = None, **kwargs
    ) -> StructArray: ...

class RunEndEncodedArray(Array):
    @staticmethod
    def from_arrays(
        run_ends: Int16Array | Int32Array | Int64Array | list[int],
        values: Array | list[Any],
        type: DataType | None = None,
    ) -> RunEndEncodedArray: ...
    @staticmethod
    def from_buffers(  # type: ignore[override]
        type: DataType,
        length: int,
        buffers: list[Buffer] | list[None],
        null_count: int = -1,
        offset=0,
        children: tuple[Array, Array] | list[list[int]] | None = None,
    ) -> RunEndEncodedArray: ...
    @property
    def run_ends(self) -> Int16Array | Int32Array | Int64Array: ...
    @property
    def values(self) -> Array: ...
    def find_physical_offset(self) -> int: ...
    def find_physical_length(self) -> int: ...

class ExtensionArray(Array):
    @property
    def storage(self) -> Any: ...
    @staticmethod
    def from_storage(typ: BaseExtensionType, storage: Array) -> ExtensionArray: ...

class JsonArray(ExtensionArray): ...
class UuidArray(ExtensionArray): ...

class FixedShapeTensorArray(ExtensionArray):
    def to_numpy_ndarray(self) -> np.ndarray: ...
    def to_tensor(self) -> Tensor: ...
    @classmethod
    def from_numpy_ndarray(
        cls, obj: np.ndarray, dim_names: list[str] | tuple[str, ...] | None = None
    ) -> Self: ...

class OpaqueArray(ExtensionArray): ...

class Bool8Array(ExtensionArray):
    def to_numpy(
        self, zero_copy_only: bool = ..., writable: bool = ...
    ) -> np.ndarray: ...
    @classmethod
    def from_storage(cls, storage: Int8Array) -> Self: ...  # type: ignore[override]
    @classmethod
    def from_numpy(cls, obj: np.ndarray) -> Self: ...

def concat_arrays(
    arrays: Iterable[Array], memory_pool: MemoryPool | None = None
) -> Array: ...
def _empty_array(type: DataType) -> Array: ...

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
]
