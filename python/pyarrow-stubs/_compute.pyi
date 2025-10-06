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

from collections.abc import (
    Callable,
    Iterable,
    Sequence,
)

from typing import (
    Any,
    Literal,
    TypeAlias,
    TypedDict,
    overload,
)

from . import lib

_Order: TypeAlias = Literal["ascending", "descending"]
_Placement: TypeAlias = Literal["at_start", "at_end"]


class Kernel(lib._Weakrefable):
    ...


class Function(lib._Weakrefable):
    @property
    def arity(self) -> int: ...

    @property
    def kind(
        self,
    ) -> Literal["scalar", "vector", "scalar_aggregate", "hash_aggregate", "meta"]: ...
    @property
    def name(self) -> str: ...
    @property
    def num_kernels(self) -> int: ...

    def call(
        self,
        args: Iterable,
        options: FunctionOptions | None = None,
        memory_pool: lib.MemoryPool | None = None,
        length: int | None = None,
    ) -> Any: ...


class FunctionOptions(lib._Weakrefable):
    def serialize(self) -> lib.Buffer: ...
    @classmethod
    def deserialize(cls, buf: lib.Buffer) -> FunctionOptions: ...


class FunctionRegistry(lib._Weakrefable):
    def get_function(self, name: str) -> Function: ...
    def list_functions(self) -> list[str]: ...


class HashAggregateFunction(Function):
    ...


class HashAggregateKernel(Kernel):
    ...


class ScalarAggregateFunction(Function):
    ...


class ScalarAggregateKernel(Kernel):
    ...


class ScalarFunction(Function):
    ...


class ScalarKernel(Kernel):
    ...


class VectorFunction(Function):
    ...


class VectorKernel(Kernel):
    ...

# ==================== _compute.pyx Option classes ====================


class ArraySortOptions(FunctionOptions):
    def __init__(
        self,
        order: _Order = "ascending",
        null_placement: _Placement = "at_end",
    ) -> None: ...


class AssumeTimezoneOptions(FunctionOptions):
    def __init__(
        self,
        timezone: str,
        *,
        ambiguous: Literal["raise", "earliest", "latest"] = "raise",
        nonexistent: Literal["raise", "earliest", "latest"] = "raise",
    ) -> None: ...


class CastOptions(FunctionOptions):
    allow_int_overflow: bool
    allow_time_truncate: bool
    allow_time_overflow: bool
    allow_decimal_truncate: bool
    allow_float_truncate: bool
    allow_invalid_utf8: bool

    def __init__(
        self,
        target_type: lib.DataType | None = None,
        *,
        allow_int_overflow: bool | None = None,
        allow_time_truncate: bool | None = None,
        allow_time_overflow: bool | None = None,
        allow_decimal_truncate: bool | None = None,
        allow_float_truncate: bool | None = None,
        allow_invalid_utf8: bool | None = None,
    ) -> None: ...
    @staticmethod
    def safe(target_type: lib.DataType | None = None) -> CastOptions: ...
    @staticmethod
    def unsafe(target_type: lib.DataType | None = None) -> CastOptions: ...
    def is_safe(self) -> bool: ...


class CountOptions(FunctionOptions):
    def __init__(self, mode: Literal["only_valid",
                 "only_null", "all"] = "only_valid") -> None: ...


class CumulativeOptions(FunctionOptions):
    def __init__(self, start: lib.Scalar | None = None,
                 *, skip_nulls: bool = False) -> None: ...


class CumulativeSumOptions(FunctionOptions):
    def __init__(self, start: lib.Scalar | None = None,
                 *, skip_nulls: bool = False) -> None: ...


class DayOfWeekOptions(FunctionOptions):
    def __init__(self, *, count_from_zero: bool = True,
                 week_start: int = 1) -> None: ...


class DictionaryEncodeOptions(FunctionOptions):
    def __init__(self, null_encoding: Literal["mask", "encode"] = "mask") -> None: ...


class RunEndEncodeOptions(FunctionOptions):
    # TODO: default is DataType(int32)
    def __init__(self, run_end_type: lib.DataType | str = ...) -> None: ...


class ElementWiseAggregateOptions(FunctionOptions):
    def __init__(self, *, skip_nulls: bool = True) -> None: ...


class ExtractRegexOptions(FunctionOptions):
    def __init__(self, pattern: str) -> None: ...


class ExtractRegexSpanOptions(FunctionOptions):
    def __init__(self, pattern: str) -> None: ...


class FilterOptions(FunctionOptions):
    def __init__(self,
                 null_selection_behavior: Literal["drop",
                                                  "emit_null"] = "drop") -> None: ...


class IndexOptions(FunctionOptions):
    def __init__(self, value: lib.Scalar) -> None: ...


class JoinOptions(FunctionOptions):
    @overload
    def __init__(
        self, null_handling: Literal["emit_null", "skip"] = "emit_null") -> None: ...

    @overload
    def __init__(self, null_handling: Literal["replace"],
                 null_replacement: str = "") -> None: ...


class ListSliceOptions(FunctionOptions):
    def __init__(
        self,
        start: int,
        stop: int | None = None,
        step: int = 1,
        return_fixed_size_list: bool | None = None,
    ) -> None: ...


class ListFlattenOptions(FunctionOptions):
    def __init__(self, recursive: bool = False) -> None: ...


class MakeStructOptions(FunctionOptions):
    def __init__(
        self,
        field_names: Sequence[str] = (),
        *,
        field_nullability: Sequence[bool] | None = None,
        field_metadata: Sequence[lib.KeyValueMetadata] | None = None,
    ) -> None: ...


class MapLookupOptions(FunctionOptions):
    # TODO: query_key: Scalar or Object can be converted to Scalar
    def __init__(
        self, query_key: lib.Scalar, occurrence: Literal["first", "last", "all"]
    ) -> None: ...


class MatchSubstringOptions(FunctionOptions):
    def __init__(self, pattern: str, *, ignore_case: bool = False) -> None: ...


class ModeOptions(FunctionOptions):
    def __init__(self, n: int = 1, *, skip_nulls: bool = True,
                 min_count: int = 0) -> None: ...


class NullOptions(FunctionOptions):
    def __init__(self, *, nan_is_null: bool = False) -> None: ...


class PadOptions(FunctionOptions):
    def __init__(
        self, width: int, padding: str = " ", lean_left_on_odd_padding: bool = True
    ) -> None: ...


class PairwiseOptions(FunctionOptions):
    def __init__(self, period: int = 1) -> None: ...


class PartitionNthOptions(FunctionOptions):
    def __init__(self, pivot: int, *,
                 null_placement: _Placement = "at_end") -> None: ...


class WinsorizeOptions(FunctionOptions):
    def __init__(self, lower_limit: float, upper_limit: float) -> None: ...


class QuantileOptions(FunctionOptions):
    def __init__(
        self,
        q: float | Sequence[float],
        *,
        interpolation: Literal["linear", "lower",
                               "higher", "nearest", "midpoint"] = "linear",
        skip_nulls: bool = True,
        min_count: int = 0,
    ) -> None: ...


class RandomOptions(FunctionOptions):
    def __init__(self, *, initializer: int | Literal["system"] = "system") -> None: ...


class RankOptions(FunctionOptions):
    def __init__(
        self,
        sort_keys: _Order | Sequence[tuple[str, _Order]] = "ascending",
        *,
        null_placement: _Placement = "at_end",
        tiebreaker: Literal["min", "max", "first", "dense"] = "first",
    ) -> None: ...


class RankQuantileOptions(FunctionOptions):
    def __init__(
        self,
        sort_keys: _Order | Sequence[tuple[str, _Order]] = "ascending",
        *,
        null_placement: _Placement = "at_end",
    ) -> None: ...


class PivotWiderOptions(FunctionOptions):
    def __init__(
        self,
        key_names: Sequence[str],
        *,
        unexpected_key_behavior: Literal["ignore", "raise"] = "ignore",
    ) -> None: ...


class ReplaceSliceOptions(FunctionOptions):
    def __init__(self, start: int, stop: int, replacement: str) -> None: ...


class ReplaceSubstringOptions(FunctionOptions):
    def __init__(
        self, pattern: str, replacement: str, *, max_replacements: int | None = None
    ) -> None: ...


_RoundMode: TypeAlias = Literal[
    "down",
    "up",
    "towards_zero",
    "towards_infinity",
    "half_down",
    "half_up",
    "half_towards_zero",
    "half_towards_infinity",
    "half_to_even",
    "half_to_odd",
]


class RoundBinaryOptions(FunctionOptions):
    def __init__(
        self,
        round_mode: _RoundMode = "half_to_even",
    ) -> None: ...


class RoundOptions(FunctionOptions):
    def __init__(
        self,
        ndigits: int = 0,
        round_mode: _RoundMode = "half_to_even",
    ) -> None: ...


_DateTimeUint: TypeAlias = Literal[
    "year",
    "quarter",
    "month",
    "week",
    "day",
    "hour",
    "minute",
    "second",
    "millisecond",
    "microsecond",
    "nanosecond",
]


class RoundTemporalOptions(FunctionOptions):
    def __init__(
        self,
        multiple: int = 1,
        unit: _DateTimeUint = "day",
        *,
        week_starts_monday: bool = True,
        ceil_is_strictly_greater: bool = False,
        calendar_based_origin: bool = False,
    ) -> None: ...


class RoundToMultipleOptions(FunctionOptions):
    def __init__(self, multiple: float = 1.0,
                 round_mode: _RoundMode = "half_to_even") -> None: ...


class ScalarAggregateOptions(FunctionOptions):
    def __init__(self, *, skip_nulls: bool = True, min_count: int = 1) -> None: ...


class SelectKOptions(FunctionOptions):
    def __init__(self, k: int, sort_keys: Sequence[tuple[str, _Order]]) -> None: ...


class SetLookupOptions(FunctionOptions):
    def __init__(self, value_set: lib.Array, *, skip_nulls: bool = True) -> None: ...


class SliceOptions(FunctionOptions):
    def __init__(
        self, start: int, stop: int | None = None, step: int = 1) -> None: ...


class SortOptions(FunctionOptions):
    def __init__(
        self,
        sort_keys: Sequence[tuple[str, _Order]],
        *,
        null_placement: _Placement = "at_end"
    ) -> None: ...


class SplitOptions(FunctionOptions):
    def __init__(self, *, max_splits: int | None = None,
                 reverse: bool = False) -> None: ...


class SplitPatternOptions(FunctionOptions):
    def __init__(
        self, pattern: str, *, max_splits: int | None = None, reverse: bool = False
    ) -> None: ...


class StrftimeOptions(FunctionOptions):
    def __init__(self, format: str = "%Y-%m-%dT%H:%M:%S",
                 locale: str = "C") -> None: ...


class StrptimeOptions(FunctionOptions):
    def __init__(self,
                 format: str,
                 unit: Literal["s",
                               "ms",
                               "us",
                               "ns"],
                 error_is_null: bool = False) -> None: ...


class StructFieldOptions(FunctionOptions):
    def __init__(self, indices: list[str] | list[bytes] |
                 list[int] | Expression | bytes | str | int) -> None: ...


class TakeOptions(FunctionOptions):
    def __init__(self, boundscheck: bool = True) -> None: ...


class TDigestOptions(FunctionOptions):
    def __init__(
        self,
        q: float | Sequence[float] = 0.5,
        *,
        delta: int = 100,
        buffer_size: int = 500,
        skip_nulls: bool = True,
        min_count: int = 0,
    ) -> None: ...


class TrimOptions(FunctionOptions):
    def __init__(self, characters: str) -> None: ...


class Utf8NormalizeOptions(FunctionOptions):
    def __init__(self, form: Literal["NFC", "NFKC", "NFD", "NFKD"]) -> None: ...


class VarianceOptions(FunctionOptions):
    def __init__(self, *, ddof: int = 0, skip_nulls: bool = True,
                 min_count: int = 0) -> None: ...


class SkewOptions(FunctionOptions):
    def __init__(
        self, *, skip_nulls: bool = True, biased: bool = True, min_count: int = 0
    ) -> None: ...


class WeekOptions(FunctionOptions):
    def __init__(
        self,
        *,
        week_starts_monday: bool = True,
        count_from_zero: bool = False,
        first_week_is_fully_in_year: bool = False,
    ) -> None: ...

# ==================== _compute.pyx Functions ====================


def call_function(
    name: str,
    args: list,
    options: FunctionOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
    length: int | None = None,
) -> Any: ...
def function_registry() -> FunctionRegistry: ...
def get_function(name: str) -> Function: ...
def list_functions() -> list[str]: ...

# ==================== _compute.pyx Udf ====================


def call_tabular_function(
    function_name: str,
    args: Iterable | None = None,
    func_registry: FunctionRegistry | None = None) -> lib.RecordBatchReader: ...


class _FunctionDoc(TypedDict):
    summary: str
    description: str


def register_scalar_function(
    func: Callable,
    function_name: str,
    function_doc: _FunctionDoc,
    in_types: dict[str, lib.DataType],
    out_type: lib.DataType,
    func_registry: FunctionRegistry | None = None,
) -> None: ...


def register_tabular_function(
    func: Callable,
    function_name: str,
    function_doc: _FunctionDoc,
    in_types: dict[str, lib.DataType],
    out_type: lib.DataType,
    func_registry: FunctionRegistry | None = None,
) -> None: ...


def register_aggregate_function(
    func: Callable,
    function_name: str,
    function_doc: _FunctionDoc,
    in_types: dict[str, lib.DataType],
    out_type: lib.DataType,
    func_registry: FunctionRegistry | None = None,
) -> None: ...


def register_vector_function(
    func: Callable,
    function_name: str,
    function_doc: _FunctionDoc,
    in_types: dict[str, lib.DataType],
    out_type: lib.DataType,
    func_registry: FunctionRegistry | None = None,
) -> None: ...


class UdfContext:
    @property
    def batch_length(self) -> int: ...
    @property
    def memory_pool(self) -> lib.MemoryPool: ...

# ==================== _compute.pyx Expression ====================


class Expression(lib._Weakrefable):
    @staticmethod
    def from_substrait(buffer: bytes | lib.Buffer) -> Expression: ...
    def to_substrait(self, schema: lib.Schema,
                     allow_arrow_extensions: bool = False) -> lib.Buffer: ...

    def __invert__(self) -> Expression: ...
    def __and__(self, other) -> Expression: ...
    def __or__(self, other) -> Expression: ...
    def __add__(self, other) -> Expression: ...
    def __mul__(self, other) -> Expression: ...
    def __sub__(self, other) -> Expression: ...
    def __eq__(self, value: object) -> Expression: ...  # type: ignore[override]
    def __ne__(self, value: object) -> Expression: ...  # type: ignore[override]
    def __gt__(self, value: object) -> Expression: ...
    def __lt__(self, value: object) -> Expression: ...
    def __ge__(self, value: object) -> Expression: ...
    def __le__(self, value: object) -> Expression: ...
    def __truediv__(self, other) -> Expression: ...
    def is_valid(self) -> Expression: ...
    def is_null(self, nan_is_null: bool = False) -> Expression: ...
    def is_nan(self) -> Expression: ...

    def cast(
        self, type: lib.DataType, safe: bool = True, options: CastOptions | None = None
    ) -> Expression: ...
    def isin(self, values: lib.Array | Iterable) -> Expression: ...

# ==================== _compute.py ====================
