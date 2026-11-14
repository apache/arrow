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

import builtins
from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import Any, ClassVar, Literal, overload, Self, TypeAlias

import numpy as np
import pandas as pd
from numpy.typing import NDArray

# Placeholder (Any) until ipc.pyi is annotated; mypy rejects it as a type
# when imported through pyarrow.lib.
from pyarrow.__lib.ipc import RecordBatchReader
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
    MapsAsPydicts,
    Mask,
    NullEncoding,
    NullPlacement,
    NullSelectionBehavior,
    Order,
    SupportsArrowArray,
    SupportsArrowDeviceArray,
    SupportsArrowStream,
    SupportsPyArrowArray,
)
from pyarrow.lib import (
    Array,
    Buffer,
    DataType,
    Device,
    DeviceAllocationType,
    Field,
    Int64Scalar,
    MemoryManager,
    MemoryPool,
    Scalar,
    Schema,
    StructArray,
    Tensor,
    _PandasConvertible,
)
from typing_extensions import CapsuleType

from pyarrow.compute import Expression
from pyarrow.interchange.dataframe import _PyArrowDataFrame

_ArrayOrChunkedArray: TypeAlias = Array | ChunkedArray

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
    "kurtosis",
    "last",
    "list",
    "max",
    "mean",
    "min",
    "min_max",
    "one",
    "pivot_wider",
    "product",
    "skew",
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
    "hash_kurtosis",
    "hash_last",
    "hash_list",
    "hash_max",
    "hash_mean",
    "hash_min",
    "hash_min_max",
    "hash_one",
    "hash_pivot_wider",
    "hash_product",
    "hash_skew",
    "hash_stddev",
    "hash_sum",
    "hash_tdigest",
    "hash_variance",
]
_AggregationName: TypeAlias = _Aggregation | _AggregationPrefixed | str
_AggregateOptions: TypeAlias = (
    ScalarAggregateOptions
    | CountOptions
    | TDigestOptions
    | VarianceOptions
    | FunctionOptions
)

_ChunkedArrayConstructorInput: TypeAlias = (
    Iterable[
        Array
        | Iterable[Any]
        | SupportsArrowArray
        | SupportsArrowDeviceArray
        | SupportsPyArrowArray
    ]
    | Array
    | SupportsArrowArray
    | SupportsArrowStream
)

_UnarySelector: TypeAlias = str
_NullarySelector: TypeAlias = tuple[()]
_NarySelector: TypeAlias = list[str] | tuple[str, ...]
_ColumnSelector: TypeAlias = _UnarySelector | _NullarySelector | _NarySelector

class ChunkedArray(_PandasConvertible):
    @property
    def type(self) -> DataType: ...
    def length(self) -> int: ...

    __len__ = length

    def to_string(
        self,
        *,
        indent: int = 0,
        window: int = 5,
        container_window: int = 2,
        skip_new_lines: bool = False,
        element_size_limit: int = 100,
    ) -> str: ...
    def validate(self, *, full: bool = False) -> None: ...
    @property
    def null_count(self) -> int: ...
    @property
    def nbytes(self) -> int: ...
    def get_total_buffer_size(self) -> int: ...
    def __sizeof__(self) -> int: ...
    @overload
    def __getitem__(self, key: int | np.integer) -> Scalar: ...
    @overload
    def __getitem__(self, key: builtins.slice) -> ChunkedArray: ...
    __hash__: ClassVar[None]  # type: ignore[assignment]
    def is_null(self, *, nan_is_null: bool = False) -> ChunkedArray: ...
    def is_nan(self) -> ChunkedArray: ...
    def is_valid(self) -> ChunkedArray: ...
    def cast(
        self,
        target_type: DataType | str | None = None,
        safe: bool | None = None,
        options: CastOptions | None = None,
    ) -> ChunkedArray: ...
    def fill_null(self, fill_value: Any) -> ChunkedArray: ...
    def equals(self, other: ChunkedArray | None) -> bool: ...
    def to_numpy(self, zero_copy_only: bool = False) -> np.ndarray: ...
    def __array__(
        self, dtype: np.dtype | None = None, copy: bool | None = None
    ) -> np.ndarray: ...
    def dictionary_encode(
        self, null_encoding: NullEncoding = "mask"
    ) -> ChunkedArray: ...
    def flatten(self, memory_pool: MemoryPool | None = None) -> list[ChunkedArray]: ...
    def combine_chunks(self, memory_pool: MemoryPool | None = None) -> Array: ...
    def unique(self) -> Array: ...
    def value_counts(self) -> StructArray: ...
    def slice(self, offset: int = 0, length: int | None = None) -> ChunkedArray: ...
    def filter(
        self, mask: Mask, null_selection_behavior: NullSelectionBehavior = "drop"
    ) -> ChunkedArray: ...
    def index(
        self,
        value: Any,
        start: int | None = None,
        end: int | None = None,
        *,
        memory_pool: MemoryPool | None = None,
    ) -> Int64Scalar: ...
    def take(self, indices: Indices) -> ChunkedArray: ...
    def drop_null(self) -> ChunkedArray: ...
    def sort(
        self,
        order: Order = "ascending",
        null_placement: NullPlacement = "at_end",
        **kwargs: Any,
    ) -> ChunkedArray: ...
    def unify_dictionaries(
        self, memory_pool: MemoryPool | None = None
    ) -> ChunkedArray: ...
    @property
    def num_chunks(self) -> int: ...
    def chunk(self, i: int) -> Array: ...
    @property
    def chunks(self) -> list[Array]: ...
    def iterchunks(self) -> Iterator[Array]: ...
    def __iter__(self) -> Iterator[Scalar]: ...
    def to_pylist(
        self,
        *,
        maps_as_pydicts: MapsAsPydicts | None = None,
    ) -> list[Any]: ...
    def __arrow_c_stream__(
        self, requested_schema: CapsuleType | None = None
    ) -> CapsuleType: ...
    @staticmethod
    def _import_from_c_capsule(stream: CapsuleType) -> ChunkedArray: ...
    @property
    def is_cpu(self) -> bool: ...

def chunked_array(
    arrays: _ChunkedArrayConstructorInput,
    type: DataType | str | None = None,
) -> ChunkedArray: ...

class _Tabular(_PandasConvertible):
    def __array__(
        self, dtype: np.dtype | None = None, copy: bool | None = None
    ) -> np.ndarray: ...
    def __dataframe__(
        self, nan_as_null: bool = False, allow_copy: bool = True
    ) -> _PyArrowDataFrame: ...
    @overload
    def __getitem__(self, key: int | str) -> _ArrayOrChunkedArray: ...
    @overload
    def __getitem__(self, key: slice) -> Self: ...
    def __len__(self) -> int: ...
    __hash__: ClassVar[None]  # type: ignore[assignment]
    def column(self, i: int | str) -> _ArrayOrChunkedArray: ...
    @property
    def column_names(self) -> list[str]: ...
    @property
    def columns(self) -> list[Array] | list[ChunkedArray]: ...
    def drop_null(self) -> Self: ...
    def field(self, i: int | str) -> Field: ...
    @classmethod
    def from_pydict(
        cls,
        mapping: Mapping[
            Any,
            _ArrayOrChunkedArray
            | Iterable[Any]
            | SupportsArrowArray
            | SupportsArrowDeviceArray,
        ],
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
    def itercolumns(self) -> Iterator[_ArrayOrChunkedArray]: ...
    @property
    def num_columns(self) -> int: ...
    @property
    def num_rows(self) -> int: ...
    @property
    def shape(self) -> tuple[int, int]: ...
    @property
    def schema(self) -> Schema: ...
    def sort_by(
        self,
        sorting: str | Sequence[tuple[str, Order] | tuple[str, Order, NullPlacement]],
        **kwargs: Any,
    ) -> Self: ...
    def take(self, indices: Indices) -> Self: ...
    def filter(
        self,
        mask: Mask | Expression,
        null_selection_behavior: NullSelectionBehavior = "drop",
    ) -> Self: ...
    def to_pydict(
        self, *, maps_as_pydicts: MapsAsPydicts | None = None
    ) -> dict[str, list[Any]]: ...
    def to_pylist(
        self, *, maps_as_pydicts: MapsAsPydicts | None = None
    ) -> list[dict[str, Any]]: ...
    def to_string(
        self, *, show_metadata: bool = False, preview_cols: int = 0
    ) -> str: ...
    def remove_column(self, i: int) -> Self: ...
    def drop_columns(self, columns: str | list[str]) -> Self: ...
    def add_column(
        self,
        i: int,
        field_: str | Field,
        column: _ArrayOrChunkedArray | list[list[Any]],
    ) -> Self: ...
    def append_column(
        self, field_: str | Field, column: _ArrayOrChunkedArray | list[list[Any]]
    ) -> Self: ...

class RecordBatch(_Tabular):
    def validate(self, *, full: bool = False) -> None: ...
    def replace_schema_metadata(
        self,
        metadata: dict[str, str]
        | dict[bytes, bytes]
        | dict[bytes, str]
        | dict[str, bytes]
        | None = None,
    ) -> RecordBatch: ...
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
        self, i: int, field_: str | Field, column: _ArrayOrChunkedArray | list[Any]
    ) -> RecordBatch: ...
    def append_column(
        self, field_: str | Field, column: _ArrayOrChunkedArray | list[Any]
    ) -> RecordBatch: ...
    def remove_column(self, i: int) -> RecordBatch: ...
    def set_column(
        self, i: int, field_: str | Field, column: _ArrayOrChunkedArray | list[Any]
    ) -> RecordBatch: ...
    def rename_columns(self, names: list[str] | dict[str, str]) -> RecordBatch: ...
    def serialize(self, memory_pool: MemoryPool | None = None) -> Buffer: ...
    def slice(self, offset: int = 0, length: int | None = None) -> RecordBatch: ...
    def equals(self, other: object, check_metadata: bool = False) -> bool: ...
    def select(
        self, columns: Iterable[str] | Iterable[int] | NDArray[np.str_]
    ) -> RecordBatch: ...
    def cast(
        self,
        target_schema: Schema,
        safe: bool | None = None,
        options: CastOptions | None = None,
    ) -> RecordBatch: ...
    @staticmethod
    def from_arrays(
        arrays: list[Any],
        names: Sequence[str] | None = None,
        schema: Schema | None = None,
        metadata: Mapping[bytes, bytes]
        | Mapping[str, str]
        | Mapping[bytes, str]
        | Mapping[str, bytes]
        | None = None,
    ) -> RecordBatch: ...
    @classmethod
    def from_pandas(
        cls,
        df: pd.DataFrame,
        schema: Schema | None = None,
        preserve_index: bool | None = None,
        nthreads: int | None = None,
        columns: Iterable[str | int] | None = None,
    ) -> RecordBatch: ...
    @staticmethod
    def from_struct_array(struct_array: StructArray) -> RecordBatch: ...
    def to_struct_array(self) -> StructArray: ...
    def to_tensor(
        self,
        null_to_nan: bool = False,
        row_major: bool = True,
        memory_pool: MemoryPool | None = None,
    ) -> Tensor: ...
    def _export_to_c(self, out_ptr: int, out_schema_ptr: int = 0) -> None: ...
    @staticmethod
    def _import_from_c(in_ptr: int, schema: Schema | int) -> RecordBatch: ...
    def __arrow_c_array__(
        self, requested_schema: CapsuleType | None = None
    ) -> tuple[CapsuleType, CapsuleType]: ...
    def __arrow_c_stream__(
        self, requested_schema: CapsuleType | None = None
    ) -> CapsuleType: ...
    @staticmethod
    def _import_from_c_capsule(
        schema_capsule: CapsuleType, array_capsule: CapsuleType
    ) -> RecordBatch: ...
    def _export_to_c_device(self, out_ptr: int, out_schema_ptr: int = 0) -> None: ...
    @staticmethod
    def _import_from_c_device(in_ptr: int, schema: Schema | int) -> RecordBatch: ...
    def __arrow_c_device_array__(
        self, requested_schema: CapsuleType | None = None, **kwargs: Any
    ) -> tuple[CapsuleType, CapsuleType]: ...
    @staticmethod
    def _import_from_c_device_capsule(
        schema_capsule: CapsuleType, array_capsule: CapsuleType
    ) -> RecordBatch: ...
    @property
    def device_type(self) -> DeviceAllocationType: ...
    @property
    def is_cpu(self) -> bool: ...
    def copy_to(self, destination: MemoryManager | Device) -> RecordBatch: ...

_JoinType: TypeAlias = Literal[
    "left semi",
    "right semi",
    "left anti",
    "right anti",
    "inner",
    "left outer",
    "right outer",
    "full outer",
]

class Table(_Tabular):
    def validate(self, *, full: bool = False) -> None: ...
    def slice(self, offset: int = 0, length: int | None = None) -> Table: ...
    def select(self, columns: Iterable[str | int]) -> Table: ...
    def replace_schema_metadata(
        self,
        metadata: dict[str, str]
        | dict[bytes, bytes]
        | dict[bytes, str]
        | dict[str, bytes]
        | None = None,
    ) -> Table: ...
    def flatten(self, memory_pool: MemoryPool | None = None) -> Table: ...
    def combine_chunks(self, memory_pool: MemoryPool | None = None) -> Table: ...
    def unify_dictionaries(self, memory_pool: MemoryPool | None = None) -> Table: ...
    def equals(self, other: Table | None, check_metadata: bool = False) -> bool: ...
    def cast(
        self,
        target_schema: Schema,
        safe: bool | None = None,
        options: CastOptions | None = None,
    ) -> Table: ...
    @classmethod
    def from_pandas(
        cls,
        df: pd.DataFrame,
        schema: Schema | None = None,
        preserve_index: bool | None = None,
        nthreads: int | None = None,
        columns: Iterable[str | int] | None = None,
        safe: bool = True,
    ) -> Table: ...
    # When no schema, arrays should also support indexing
    @staticmethod
    def from_arrays(
        arrays: Sequence[
            Array
            | ChunkedArray
            | Iterable[Any]
            | SupportsArrowArray
            | SupportsArrowDeviceArray
            | SupportsPyArrowArray
        ]
        | NDArray[Any],
        names: Sequence[str] | None = None,
        schema: Schema | None = None,
        metadata: Mapping[bytes, bytes]
        | Mapping[str, str]
        | Mapping[bytes, str]
        | Mapping[str, bytes]
        | None = None,
    ) -> Table: ...
    @staticmethod
    def from_struct_array(struct_array: StructArray | ChunkedArray) -> Table: ...
    def to_struct_array(self, max_chunksize: int | None = None) -> ChunkedArray: ...
    @staticmethod
    def from_batches(
        batches: Iterable[RecordBatch], schema: Schema | None = None
    ) -> Table: ...
    def to_batches(self, max_chunksize: int | None = None) -> list[RecordBatch]: ...
    def to_tensor(
        self,
        null_to_nan: bool = False,
        row_major: bool = True,
        memory_pool: MemoryPool | None = None,
    ) -> Tensor: ...
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
    def add_column(
        self,
        i: int,
        field_: str | Field,
        column: ChunkedArray | _ChunkedArrayConstructorInput,
    ) -> Table: ...
    def append_column(
        self,
        field_: str | Field,
        column: ChunkedArray | _ChunkedArrayConstructorInput,
    ) -> Table: ...
    def remove_column(self, i: int) -> Table: ...
    def set_column(
        self,
        i: int,
        field_: str | Field,
        column: ChunkedArray | _ChunkedArrayConstructorInput,
    ) -> Table: ...
    def rename_columns(self, names: list[str] | dict[str, str]) -> Table: ...
    def drop(self, columns: str | list[str]) -> Table: ...
    def group_by(
        self, keys: str | list[str], use_threads: bool = True
    ) -> TableGroupBy: ...
    def join(
        self,
        right_table: Table,
        keys: str | list[str],
        right_keys: str | list[str] | None = None,
        join_type: _JoinType = "left outer",
        left_suffix: str | None = None,
        right_suffix: str | None = None,
        coalesce_keys: bool = True,
        use_threads: bool = True,
        filter_expression: Expression | None = None,
    ) -> Table: ...
    def join_asof(
        self,
        right_table: Table,
        on: str,
        by: str | list[str],
        tolerance: int,
        right_on: str | list[str] | None = None,
        right_by: str | list[str] | None = None,
    ) -> Table: ...
    def __arrow_c_stream__(
        self, requested_schema: CapsuleType | None = None
    ) -> CapsuleType: ...
    @property
    def is_cpu(self) -> bool: ...

def record_batch(
    data: dict[str, Any]
    | list[Any]
    | pd.DataFrame
    | SupportsArrowArray
    | SupportsArrowDeviceArray,
    names: list[str] | Schema | None = None,
    schema: Schema | None = None,
    metadata: Mapping[str | bytes, str | bytes] | None = None,
) -> RecordBatch: ...
def table(
    data: dict[str, Any]
    | list[Any]
    | tuple[Any, ...]
    | pd.DataFrame
    | SupportsArrowArray
    | SupportsArrowStream
    | SupportsArrowDeviceArray,
    names: list[str] | Schema | None = None,
    schema: Schema | None = None,
    metadata: Mapping[str | bytes, str | bytes] | None = None,
    nthreads: int | None = None,
) -> Table: ...
def concat_tables(
    tables: Iterable[Table],
    memory_pool: MemoryPool | None = None,
    promote_options: Literal["none", "default", "permissive"] = "none",
) -> Table: ...

class TableGroupBy:
    keys: str | list[str]

    def __init__(
        self, table: Table, keys: str | list[str], use_threads: bool = True
    ): ...
    def aggregate(
        self,
        aggregations: Iterable[
            tuple[_ColumnSelector, _AggregationName]
            | tuple[_ColumnSelector, _AggregationName, _AggregateOptions | None]
        ],
    ) -> Table: ...

def concat_batches(
    recordbatches: Iterable[RecordBatch], memory_pool: MemoryPool | None = None
) -> RecordBatch: ...

__all__ = [
    "ChunkedArray",
    "chunked_array",
    "_Tabular",
    "RecordBatch",
    "Table",
    "record_batch",
    "table",
    "concat_tables",
    "TableGroupBy",
    "concat_batches",
]
