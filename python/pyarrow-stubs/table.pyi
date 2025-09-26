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

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self
if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias
from collections.abc import (
    Collection, Generator, Iterable, Iterator, Sequence, Mapping)
from typing import (Any, Generic, Literal, TypeVar)
import builtins

import numpy as np
import pandas as pd

from numpy.typing import NDArray
from pyarrow._compute import (
    CastOptions,
    CountOptions,
    FunctionOptions,
    ScalarAggregateOptions,
    TDigestOptions,
    VarianceOptions,
)
from pyarrow._stubs_typing import (
    Indices,
    Mask,
    NullEncoding,
    NullSelectionBehavior,
    Order,
    SupportArrowArray,
    SupportArrowDeviceArray,
    SupportArrowStream,
)
from pyarrow.compute import ArrayOrChunkedArray, Expression
from pyarrow.interchange.dataframe import _PyArrowDataFrame
from pyarrow.lib import Device, MemoryManager, MemoryPool, Schema
from pyarrow.lib import Field as _Field

from .array import Array, StructArray, _CastAs, _PandasConvertible
from .device import DeviceAllocationType
from .io import Buffer
from ._ipc import RecordBatchReader
from .scalar import BooleanScalar, Int64Scalar, Scalar, StructScalar
from .tensor import Tensor
from ._stubs_typing import NullableCollection
from ._types import DataType, _AsPyType, _BasicDataType, _DataTypeT

Field: TypeAlias = _Field[DataType]
_ScalarT = TypeVar("_ScalarT", bound=Scalar)
_Scalar_co = TypeVar("_Scalar_co", bound=Scalar, covariant=True)

_Aggregation: TypeAlias = Literal[
    "all",
    "any",
    "approximate_median",
    "count",
    "count_all",
    "count_distinct",
    "distinct",
    "first",
    "first_last",
    "last",
    "list",
    "max",
    "mean",
    "min",
    "min_max",
    "one",
    "product",
    "stddev",
    "sum",
    "tdigest",
    "variance",
]
_AggregationPrefixed: TypeAlias = Literal[
    "hash_all",
    "hash_any",
    "hash_approximate_median",
    "hash_count",
    "hash_count_all",
    "hash_count_distinct",
    "hash_distinct",
    "hash_first",
    "hash_first_last",
    "hash_last",
    "hash_list",
    "hash_max",
    "hash_mean",
    "hash_min",
    "hash_min_max",
    "hash_one",
    "hash_product",
    "hash_stddev",
    "hash_sum",
    "hash_tdigest",
    "hash_variance",
]
Aggregation: TypeAlias = _Aggregation | _AggregationPrefixed
AggregateOptions: TypeAlias = (ScalarAggregateOptions | CountOptions
                               | TDigestOptions | VarianceOptions | FunctionOptions)

UnarySelector: TypeAlias = str
NullarySelector: TypeAlias = tuple[()]
NarySelector: TypeAlias = list[str] | tuple[str, ...]
ColumnSelector: TypeAlias = UnarySelector | NullarySelector | NarySelector


class ChunkedArray(_PandasConvertible[pd.Series], Generic[_Scalar_co]):

    @property
    def data(self) -> Self: ...
    @property
    def type(self: ChunkedArray[Scalar[_DataTypeT]]) -> _DataTypeT: ...

    def length(self) -> int: ...

    __len__ = length

    def to_string(
        self,
        *,
        indent: int = 0,
        window: int = 5,
        container_window: int = 2,
        skip_new_lines: bool = False,
    ) -> str: ...

    format = to_string
    def validate(self, *, full: bool = False) -> None: ...

    @property
    def null_count(self) -> int: ...

    @property
    def nbytes(self) -> int: ...

    def get_total_buffer_size(self) -> int: ...

    def __sizeof__(self) -> int: ...
    def __getitem__(self, key: int | builtins.slice) -> Self | _Scalar_co: ...

    def getitem(self, i: int) -> Scalar: ...
    def is_null(self, *, nan_is_null: bool = False) -> ChunkedArray[BooleanScalar]: ...

    def is_nan(self) -> ChunkedArray[BooleanScalar]: ...

    def is_valid(self) -> ChunkedArray[BooleanScalar]: ...

    def fill_null(self, fill_value: Scalar[_DataTypeT]) -> Self: ...

    def equals(self, other: Self) -> bool: ...

    def to_numpy(self, zero_copy_only: bool = False) -> np.ndarray: ...

    def __array__(self, dtype: np.dtype | None = None,
                  copy: bool | None = None) -> np.ndarray: ...

    def cast(
        self,
        target_type: None | _CastAs = None,
        safe: bool | None = None,
        options: CastOptions | None = None,
    ) -> Self | ChunkedArray[Scalar[_CastAs]]: ...

    def dictionary_encode(self, null_encoding: NullEncoding = "mask") -> Self: ...

    def flatten(self, memory_pool: MemoryPool |
                None = None) -> list[ChunkedArray[Any]]: ...

    def combine_chunks(self, memory_pool: MemoryPool |
                       None = None) -> Array[_Scalar_co]: ...

    def unique(self) -> ChunkedArray[_Scalar_co]: ...

    def value_counts(self) -> StructArray: ...

    def slice(self, offset: int = 0, length: int | None = None) -> Self: ...

    def filter(self, mask: Mask,
               null_selection_behavior: NullSelectionBehavior = "drop") -> Self: ...

    def index(
        self: ChunkedArray[Scalar[_BasicDataType[_AsPyType]]],
        value: Scalar[_DataTypeT] | _AsPyType,
        start: int | None = None,
        end: int | None = None,
        *,
        memory_pool: MemoryPool | None = None,
    ) -> Int64Scalar: ...

    def take(self, indices: Indices) -> Self: ...

    def drop_null(self) -> Self: ...

    def sort(self, order: Order = "ascending", **kwargs) -> Self: ...

    def unify_dictionaries(self, memory_pool: MemoryPool | None = None) -> Self: ...

    @property
    def num_chunks(self) -> int: ...

    def chunk(self, i: int) -> ChunkedArray[_Scalar_co]: ...

    @property
    def chunks(self) -> list[Array[_Scalar_co]]: ...

    def iterchunks(
        self: ArrayOrChunkedArray[_ScalarT],
    ) -> Generator[Array, None, None]: ...

    def __iter__(self) -> Iterator[_Scalar_co]: ...

    def to_pylist(
        self: ChunkedArray[Scalar[_BasicDataType[_AsPyType]]],
        *,
        maps_as_pydicts: Literal["lossy", "strict"] | None = None,
    ) -> list[_AsPyType | None]: ...

    def __arrow_c_stream__(self, requested_schema=None) -> Any: ...

    @classmethod
    def _import_from_c_capsule(cls, stream) -> Self: ...

    @property
    def is_cpu(self) -> bool: ...


def chunked_array(
    arrays: Iterable[NullableCollection[Any]]
    | Iterable[Iterable[Any] | SupportArrowStream | SupportArrowArray]
    | Iterable[Array[_ScalarT]] | Array[_ScalarT],
    type: DataType | str | None = None,
) -> ChunkedArray[Scalar[Any]] | ChunkedArray[_ScalarT]: ...


_ColumnT = TypeVar("_ColumnT", bound=ArrayOrChunkedArray[Any])


class _Tabular(_PandasConvertible[pd.DataFrame], Generic[_ColumnT]):
    def __array__(self, dtype: np.dtype | None = None,
                  copy: bool | None = None) -> np.ndarray: ...

    def __dataframe__(
        self, nan_as_null: bool = False, allow_copy: bool = True
    ) -> _PyArrowDataFrame: ...

    def __getitem__(self, key: int | str | slice) -> _ColumnT | Self: ...

    def __len__(self) -> int: ...
    def column(self, i: int | str) -> _ColumnT: ...

    @property
    def column_names(self) -> list[str]: ...

    @property
    def columns(self) -> list[_ColumnT]: ...

    def drop_null(self) -> Self: ...

    def field(self, i: int | str) -> Field: ...

    @classmethod
    def from_pydict(
        cls,
        mapping: Mapping[str, ArrayOrChunkedArray[Any] | list[Any] | np.ndarray],
        schema: Schema | None = None,
        metadata: Mapping[str | bytes, str | bytes] | None = None,
    ) -> Self: ...

    @classmethod
    def from_pylist(
        cls,
        mapping: Sequence[Mapping[str, Any]],
        schema: Schema | None = None,
        metadata: Mapping[str | bytes, str | bytes] | None = None,
    ) -> Self: ...

    def itercolumns(self) -> Generator[_ColumnT, None, None]: ...

    @property
    def num_columns(self) -> int: ...
    @property
    def num_rows(self) -> int: ...
    @property
    def shape(self) -> tuple[int, int]: ...

    @property
    def schema(self) -> Schema: ...
    @property
    def nbytes(self) -> int: ...
    def sort_by(self, sorting: str | list[tuple[str, Order]], **kwargs) -> Self: ...

    def take(self, indices: Indices) -> Self: ...

    def filter(
        self,
        mask: Mask | Expression,
        null_selection_behavior: NullSelectionBehavior = "drop") -> Self: ...

    def to_pydict(
        self, *, maps_as_pydicts: Literal["lossy", "strict"] | None = None
    ) -> dict[str, list[Any]]: ...

    def to_pylist(
        self, *, maps_as_pydicts: Literal["lossy", "strict"] | None = None
    ) -> list[dict[str, Any]]: ...

    def to_string(self, *, show_metadata: bool = False,
                  preview_cols: int = 0) -> str: ...

    def remove_column(self, i: int) -> Self: ...
    def drop_columns(self, columns: str | list[str]) -> Self: ...

    def add_column(self, i: int, field_: str | Field,
                   column: ArrayOrChunkedArray[Any] | list[list[Any]]) -> Self: ...

    def append_column(
        self, field_: str | Field, column: ArrayOrChunkedArray[Any] | list[list[Any]]
    ) -> Self: ...


class RecordBatch(_Tabular[Array]):

    def validate(self, *, full: bool = False) -> None: ...

    def replace_schema_metadata(
        self, metadata: dict[str | bytes, str | bytes] | None = None
    ) -> Self: ...

    @property
    def num_columns(self) -> int: ...

    @property
    def num_rows(self) -> int: ...

    @property
    def schema(self) -> Schema: ...

    @property
    def nbytes(self) -> int: ...

    def get_total_buffer_size(self) -> int: ...

    def __sizeof__(self) -> int: ...

    def add_column(
        self, i: int, field_: str | Field, column: ArrayOrChunkedArray[Any] | list
    ) -> Self: ...

    def remove_column(self, i: int) -> Self: ...

    def set_column(self, i: int, field_: str | Field, column: Array | list) -> Self: ...

    def rename_columns(self, names: list[str] | dict[str, str]) -> Self: ...

    def serialize(self, memory_pool: MemoryPool | None = None) -> Buffer: ...

    def slice(self, offset: int = 0, length: int | None = None) -> Self: ...

    def equals(self, other: Self, check_metadata: bool = False) -> bool: ...

    def select(self, columns: Iterable[str] |
               Iterable[int] | NDArray[np.str_]) -> Self: ...

    def cast(self, target_schema: Schema, safe: bool | None = None,
             options: CastOptions | None = None) -> Self: ...

    @classmethod
    def from_arrays(
        cls,
        arrays: Collection[Array],
        names: list[str] | None = None,
        schema: Schema | None = None,
        metadata: Mapping[str | bytes, str | bytes] | None = None,
    ) -> Self: ...

    @classmethod
    def from_pandas(
        cls,
        df: pd.DataFrame,
        schema: Schema | None = None,
        preserve_index: bool | None = None,
        nthreads: int | None = None,
        columns: list[str] | None = None,
    ) -> Self: ...

    @classmethod
    def from_struct_array(
        cls, struct_array: StructArray | ChunkedArray[StructScalar]
    ) -> Self: ...

    def to_struct_array(self) -> StructArray: ...

    def to_tensor(
        self,
        null_to_nan: bool = False,
        row_major: bool = True,
        memory_pool: MemoryPool | None = None,
    ) -> Tensor: ...

    def _export_to_c(self, out_ptr: int, out_schema_ptr: int = 0): ...

    @classmethod
    def _import_from_c(cls, in_ptr: int, schema: Schema) -> Self: ...

    def __arrow_c_array__(self, requested_schema=None): ...

    def __arrow_c_stream__(self, requested_schema=None): ...

    @classmethod
    def _import_from_c_capsule(cls, schema_capsule, array_capsule) -> Self: ...

    def _export_to_c_device(self, out_ptr: int, out_schema_ptr: int = 0) -> None: ...

    @classmethod
    def _import_from_c_device(cls, in_ptr: int, schema: Schema) -> Self: ...

    def __arrow_c_device_array__(self, requested_schema=None, **kwargs): ...

    @classmethod
    def _import_from_c_device_capsule(cls, schema_capsule, array_capsule) -> Self: ...

    @property
    def device_type(self) -> DeviceAllocationType: ...

    @property
    def is_cpu(self) -> bool: ...

    def copy_to(self, destination: MemoryManager | Device) -> Self: ...


def table_to_blocks(options, table: Table, categories, extension_columns): ...


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


class Table(_Tabular[ChunkedArray[Any]]):

    def validate(self, *, full: bool = False) -> None: ...

    def slice(self, offset: int = 0, length: int | None = None) -> Self: ...

    def select(self, columns: Iterable[str] |
               Iterable[int] | NDArray[np.str_]) -> Self: ...

    def replace_schema_metadata(
        self, metadata: dict[str | bytes, str | bytes] | None = None
    ) -> Self: ...

    def flatten(self, memory_pool: MemoryPool | None = None) -> Self: ...

    def combine_chunks(self, memory_pool: MemoryPool | None = None) -> Self: ...

    def unify_dictionaries(self, memory_pool: MemoryPool | None = None) -> Self: ...

    def equals(self, other: Self, check_metadata: bool = False) -> Self: ...

    def cast(self, target_schema: Schema, safe: bool | None = None,
             options: CastOptions | None = None) -> Self: ...

    @classmethod
    def from_pandas(
        cls,
        df: pd.DataFrame,
        schema: Schema | None = None,
        preserve_index: bool | None = None,
        nthreads: int | None = None,
        columns: list[str] | None = None,
        safe: bool = True,
    ) -> Self: ...

    @classmethod
    def from_arrays(
        cls,
        arrays: Collection[ArrayOrChunkedArray[Any]],
        names: list[str] | None = None,
        schema: Schema | None = None,
        metadata: Mapping[str | bytes, str | bytes] | None = None,
    ) -> Self: ...

    @classmethod
    def from_struct_array(
        cls, struct_array: StructArray | ChunkedArray[StructScalar]
    ) -> Self: ...

    def to_struct_array(
        self, max_chunksize: int | None = None
    ) -> ChunkedArray[StructScalar]: ...

    @classmethod
    def from_batches(cls, batches: Iterable[RecordBatch],
                     schema: Schema | None = None) -> Self: ...

    def to_batches(self, max_chunksize: int | None = None) -> list[RecordBatch]: ...

    def to_reader(self, max_chunksize: int | None = None) -> RecordBatchReader: ...

    @property
    def schema(self) -> Schema: ...

    @property
    def num_columns(self) -> int: ...

    @property
    def num_rows(self) -> int: ...

    @property
    def nbytes(self) -> int: ...

    def get_total_buffer_size(self) -> int: ...

    def __sizeof__(self) -> int: ...

    def add_column(self, i: int, field_: str | Field,
                   column: ArrayOrChunkedArray[Any] | list[list[Any]]) -> Self: ...

    def remove_column(self, i: int) -> Self: ...

    def set_column(self, i: int, field_: str | Field,
                   column: ArrayOrChunkedArray[Any] | list[list[Any]]) -> Self: ...

    def rename_columns(self, names: list[str] | dict[str, str]) -> Self: ...

    def drop(self, columns: str | list[str]) -> Self: ...

    def group_by(self, keys: str | list[str],
                 use_threads: bool = True) -> TableGroupBy: ...

    def join(
        self,
        right_table: Self,
        keys: str | list[str],
        right_keys: str | list[str] | None = None,
        join_type: JoinType = "left outer",
        left_suffix: str | None = None,
        right_suffix: str | None = None,
        coalesce_keys: bool = True,
        use_threads: bool = True,
    ) -> Self: ...

    def join_asof(
        self,
        right_table: Self,
        on: str,
        by: str | list[str],
        tolerance: int,
        right_on: str | list[str] | None = None,
        right_by: str | list[str] | None = None,
    ) -> Self: ...

    def __arrow_c_stream__(self, requested_schema=None): ...

    @property
    def is_cpu(self) -> bool: ...


def record_batch(
    data: Mapping[str, list[Any] | Array[Any]]
    | Collection[Array[Any] | ChunkedArray[Any]]
    | pd.DataFrame
    | SupportArrowArray
    | SupportArrowDeviceArray,
    names: list[str] | None = None,
    schema: Schema | None = None,
    metadata: Mapping[str | bytes, str | bytes] | None = None,
) -> RecordBatch: ...


def table(
    data: Mapping[str, list[Any] | Array[Any]]
    | Collection[ArrayOrChunkedArray[Any]]
    | pd.DataFrame
    | SupportArrowArray
    | SupportArrowStream
    | SupportArrowDeviceArray,
    names: list[str] | None = None,
    schema: Schema | None = None,
    metadata: Mapping[str | bytes, str | bytes] | None = None,
    nthreads: int | None = None,
) -> Table: ...


def concat_tables(
    tables: Iterable[Table],
    memory_pool: MemoryPool | None = None,
    promote_options: Literal["none", "default", "permissive"] = "none",
    **kwargs: Any,
) -> Table: ...


class TableGroupBy:

    keys: str | list[str]

    def __init__(self, table: Table, keys: str |
                 list[str], use_threads: bool = True): ...

    def aggregate(
        self,
        aggregations: Iterable[
            tuple[ColumnSelector, Aggregation]
            | tuple[ColumnSelector, Aggregation, AggregateOptions | None]
        ],
    ) -> Table: ...

    def _table(self) -> Table: ...
    @property
    def _use_threads(self) -> bool: ...


def concat_batches(
    recordbatches: Iterable[RecordBatch], memory_pool: MemoryPool | None = None
) -> RecordBatch: ...


__all__ = [
    "ChunkedArray",
    "chunked_array",
    "_Tabular",
    "RecordBatch",
    "table_to_blocks",
    "Table",
    "record_batch",
    "table",
    "concat_tables",
    "TableGroupBy",
    "concat_batches",
]
