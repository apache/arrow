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

from collections.abc import Callable, Hashable, Iterable, Sequence
from types import EllipsisType
from typing import Any, ClassVar, Literal, Protocol, TypeAlias, TypedDict

# Placeholders (Any) until ipc.pyi and scalar.pyi are annotated; mypy
# rejects them as types when imported through pyarrow.lib.
from pyarrow.__lib.ipc import RecordBatchReader
from pyarrow.__lib.scalar import Scalar
from pyarrow._stubs_typing import (
    NullEncoding,
    NullPlacement,
    NullSelectionBehavior,
    Order,
    TimeUnit,
)
from pyarrow.lib import (
    Array,
    Buffer,
    ChunkedArray,
    DataType,
    KeyValueMetadata,
    MemoryPool,
    Schema,
    _Weakrefable,
)

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
_DateTimeUnit: TypeAlias = Literal[
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
# Sort keys are (name, order) or (name, order, null_placement) tuples; the
# null_placement arguments of the options are deprecated.
_SortKey: TypeAlias = (
    tuple[str | Expression | int, Order]
    | tuple[str | Expression | int, Order, NullPlacement]
)
_CountMode: TypeAlias = Literal["only_valid", "only_null", "all"]
_Form: TypeAlias = Literal["NFC", "NFKC", "NFD", "NFKD"]
_Interpolation: TypeAlias = Literal["linear", "lower", "higher", "nearest", "midpoint"]
_NullHandling: TypeAlias = Literal["emit_null", "skip", "replace"]
_Occurrence: TypeAlias = Literal["first", "last", "all"]
_Side: TypeAlias = Literal["left", "right"]
_Tiebreaker: TypeAlias = Literal["min", "max", "first", "dense"]
_TimezoneResolution: TypeAlias = Literal["raise", "earliest", "latest"]
_UnexpectedKeyBehavior: TypeAlias = Literal["ignore", "raise"]

# ==================== Kernels and functions ====================

class Kernel(_Weakrefable): ...
class ScalarKernel(Kernel): ...
class VectorKernel(Kernel): ...
class ScalarAggregateKernel(Kernel): ...
class HashAggregateKernel(Kernel): ...

class Function(_Weakrefable):
    @property
    def arity(self) -> int | EllipsisType: ...
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
        args: Iterable[Any],
        options: FunctionOptions | None = None,
        memory_pool: MemoryPool | None = None,
        length: int | None = None,
    ) -> Any: ...

class ScalarFunction(Function):
    @property
    def kernels(self) -> list[ScalarKernel]: ...

class VectorFunction(Function):
    @property
    def kernels(self) -> list[VectorKernel]: ...

class ScalarAggregateFunction(Function):
    @property
    def kernels(self) -> list[ScalarAggregateKernel]: ...

class HashAggregateFunction(Function):
    @property
    def kernels(self) -> list[HashAggregateKernel]: ...

class MetaFunction(Function):
    @property
    def kernels(self) -> list[Kernel]: ...

class FunctionRegistry(_Weakrefable):
    def get_function(self, name: str) -> Function: ...
    def list_functions(self) -> list[str]: ...

def function_registry() -> FunctionRegistry: ...
def get_function(name: str) -> Function: ...
def list_functions() -> list[str]: ...
def call_function(
    name: str,
    args: list[Any],
    options: FunctionOptions | None = None,
    memory_pool: MemoryPool | None = None,
    length: int | None = None,
) -> Any: ...

# ==================== Option classes ====================

class FunctionOptions(_Weakrefable):
    __hash__: ClassVar[None]  # type: ignore[assignment]

    def serialize(self) -> Buffer: ...
    @staticmethod
    def deserialize(buf: Buffer) -> FunctionOptions: ...

class CastOptions(FunctionOptions):
    allow_int_overflow: bool
    allow_time_truncate: bool
    allow_time_overflow: bool
    allow_decimal_truncate: bool
    allow_float_truncate: bool
    allow_invalid_utf8: bool

    def __init__(
        self,
        target_type: DataType | str | None = None,
        *,
        allow_int_overflow: bool | None = None,
        allow_time_truncate: bool | None = None,
        allow_time_overflow: bool | None = None,
        allow_decimal_truncate: bool | None = None,
        allow_float_truncate: bool | None = None,
        allow_invalid_utf8: bool | None = None,
    ) -> None: ...
    @staticmethod
    def safe(target_type: DataType | str | None = None) -> CastOptions: ...
    @staticmethod
    def unsafe(target_type: DataType | str | None = None) -> CastOptions: ...
    def is_safe(self) -> bool: ...

class ElementWiseAggregateOptions(FunctionOptions):
    def __init__(self, *, skip_nulls: bool = True) -> None: ...

class RoundOptions(FunctionOptions):
    def __init__(
        self, ndigits: int = 0, round_mode: _RoundMode = "half_to_even"
    ) -> None: ...

class RoundBinaryOptions(FunctionOptions):
    def __init__(self, round_mode: _RoundMode = "half_to_even") -> None: ...

class RoundTemporalOptions(FunctionOptions):
    def __init__(
        self,
        multiple: int = 1,
        unit: _DateTimeUnit = "day",
        *,
        week_starts_monday: bool = True,
        ceil_is_strictly_greater: bool = False,
        calendar_based_origin: bool = False,
    ) -> None: ...

class RoundToMultipleOptions(FunctionOptions):
    def __init__(
        self,
        multiple: float | Scalar = 1.0,
        round_mode: _RoundMode = "half_to_even",
    ) -> None: ...

class JoinOptions(FunctionOptions):
    def __init__(
        self,
        null_handling: _NullHandling = "emit_null",
        null_replacement: str = "",
    ) -> None: ...

class MatchSubstringOptions(FunctionOptions):
    def __init__(self, pattern: str, *, ignore_case: bool = False) -> None: ...

class PadOptions(FunctionOptions):
    def __init__(
        self, width: int, padding: str = " ", lean_left_on_odd_padding: bool = True
    ) -> None: ...

class ZeroFillOptions(FunctionOptions):
    def __init__(self, width: int, padding: str = "0") -> None: ...

class TrimOptions(FunctionOptions):
    def __init__(self, characters: str) -> None: ...

class ReplaceSubstringOptions(FunctionOptions):
    def __init__(
        self, pattern: str, replacement: str, *, max_replacements: int | None = None
    ) -> None: ...

class ExtractRegexOptions(FunctionOptions):
    def __init__(self, pattern: str) -> None: ...

class ExtractRegexSpanOptions(FunctionOptions):
    def __init__(self, pattern: str) -> None: ...

class SliceOptions(FunctionOptions):
    def __init__(self, start: int, stop: int | None = None, step: int = 1) -> None: ...

class ListSliceOptions(FunctionOptions):
    def __init__(
        self,
        start: int,
        stop: int | None = None,
        step: int = 1,
        return_fixed_size_list: bool | None = None,
    ) -> None: ...

class ReplaceSliceOptions(FunctionOptions):
    def __init__(self, start: int, stop: int, replacement: str) -> None: ...

class FilterOptions(FunctionOptions):
    def __init__(
        self, null_selection_behavior: NullSelectionBehavior = "drop"
    ) -> None: ...

class DictionaryEncodeOptions(FunctionOptions):
    def __init__(self, null_encoding: NullEncoding = "mask") -> None: ...

class RunEndEncodeOptions(FunctionOptions):
    def __init__(self, run_end_type: DataType | str = ...) -> None: ...

class InversePermutationOptions(FunctionOptions):
    def __init__(
        self, max_index: int = -1, output_type: DataType | str | None = None
    ) -> None: ...

class ScatterOptions(FunctionOptions):
    def __init__(self, max_index: int = -1) -> None: ...

class TakeOptions(FunctionOptions):
    def __init__(self, *, boundscheck: bool = True) -> None: ...

class MakeStructOptions(FunctionOptions):
    def __init__(
        self,
        field_names: Sequence[str] = (),
        *,
        field_nullability: Sequence[bool] | None = None,
        field_metadata: Sequence[KeyValueMetadata | None] | None = None,
    ) -> None: ...

class StructFieldOptions(FunctionOptions):
    def __init__(
        self,
        indices: Sequence[str | bytes | int] | Expression | bytes | str | int,
    ) -> None: ...

class ScalarAggregateOptions(FunctionOptions):
    def __init__(self, *, skip_nulls: bool = True, min_count: int = 1) -> None: ...

class CountOptions(FunctionOptions):
    def __init__(self, mode: _CountMode = "only_valid") -> None: ...

class IndexOptions(FunctionOptions):
    def __init__(self, value: Scalar) -> None: ...

class MapLookupOptions(FunctionOptions):
    def __init__(self, query_key: Any, occurrence: _Occurrence) -> None: ...

class ModeOptions(FunctionOptions):
    def __init__(
        self, n: int = 1, *, skip_nulls: bool = True, min_count: int = 0
    ) -> None: ...

class SetLookupOptions(FunctionOptions):
    def __init__(
        self, value_set: Array | ChunkedArray, *, skip_nulls: bool = False
    ) -> None: ...

class StrptimeOptions(FunctionOptions):
    def __init__(
        self, format: str, unit: TimeUnit, error_is_null: bool = False
    ) -> None: ...

class StrftimeOptions(FunctionOptions):
    def __init__(
        self, format: str = "%Y-%m-%dT%H:%M:%S", locale: str = "C"
    ) -> None: ...

class DayOfWeekOptions(FunctionOptions):
    def __init__(
        self, *, count_from_zero: bool = True, week_start: int = 1
    ) -> None: ...

class WeekOptions(FunctionOptions):
    def __init__(
        self,
        *,
        week_starts_monday: bool = True,
        count_from_zero: bool = False,
        first_week_is_fully_in_year: bool = False,
    ) -> None: ...

class AssumeTimezoneOptions(FunctionOptions):
    def __init__(
        self,
        timezone: str,
        *,
        ambiguous: _TimezoneResolution = "raise",
        nonexistent: _TimezoneResolution = "raise",
    ) -> None: ...

class NullOptions(FunctionOptions):
    def __init__(self, *, nan_is_null: bool = False) -> None: ...

class VarianceOptions(FunctionOptions):
    def __init__(
        self, *, ddof: int = 0, skip_nulls: bool = True, min_count: int = 0
    ) -> None: ...

class SkewOptions(FunctionOptions):
    def __init__(
        self, *, skip_nulls: bool = True, biased: bool = True, min_count: int = 0
    ) -> None: ...

class SplitOptions(FunctionOptions):
    def __init__(
        self, *, max_splits: int | None = None, reverse: bool = False
    ) -> None: ...

class SplitPatternOptions(FunctionOptions):
    def __init__(
        self, pattern: str, *, max_splits: int | None = None, reverse: bool = False
    ) -> None: ...

class PartitionNthOptions(FunctionOptions):
    def __init__(
        self, pivot: int, *, null_placement: NullPlacement = "at_end"
    ) -> None: ...

class WinsorizeOptions(FunctionOptions):
    def __init__(self, lower_limit: float, upper_limit: float) -> None: ...

class CumulativeOptions(FunctionOptions):
    def __init__(
        self, start: Scalar | None = None, *, skip_nulls: bool = False
    ) -> None: ...

class PairwiseOptions(FunctionOptions):
    def __init__(self, period: int = 1) -> None: ...

class ListFlattenOptions(FunctionOptions):
    def __init__(self, recursive: bool = False) -> None: ...

class ArraySortOptions(FunctionOptions):
    def __init__(
        self,
        order: Order = "ascending",
        *,
        null_placement: NullPlacement = "at_end",
    ) -> None: ...

class SearchSortedOptions(FunctionOptions):
    def __init__(self, side: _Side = "left") -> None: ...

class SortOptions(FunctionOptions):
    def __init__(
        self,
        sort_keys: Sequence[_SortKey] = (),
        *,
        null_placement: NullPlacement | None = None,
    ) -> None: ...

class SelectKOptions(FunctionOptions):
    def __init__(self, k: int, sort_keys: Sequence[_SortKey]) -> None: ...

class QuantileOptions(FunctionOptions):
    def __init__(
        self,
        q: float | Sequence[float] = 0.5,
        *,
        interpolation: _Interpolation = "linear",
        skip_nulls: bool = True,
        min_count: int = 0,
    ) -> None: ...

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

class Utf8NormalizeOptions(FunctionOptions):
    def __init__(self, form: _Form) -> None: ...

class RandomOptions(FunctionOptions):
    def __init__(self, *, initializer: Hashable = "system") -> None: ...

class RankOptions(FunctionOptions):
    def __init__(
        self,
        sort_keys: Order | Sequence[_SortKey] = "ascending",
        *,
        null_placement: NullPlacement | None = None,
        tiebreaker: _Tiebreaker = "first",
    ) -> None: ...

class RankQuantileOptions(FunctionOptions):
    def __init__(
        self,
        sort_keys: Order | Sequence[_SortKey] = "ascending",
        *,
        null_placement: NullPlacement | None = None,
    ) -> None: ...

class PivotWiderOptions(FunctionOptions):
    def __init__(
        self,
        key_names: Sequence[str],
        *,
        unexpected_key_behavior: _UnexpectedKeyBehavior = "ignore",
    ) -> None: ...

# ==================== Expression ====================

class _SubstraitMessage(Protocol):
    def SerializeToString(self) -> bytes: ...

class Expression(_Weakrefable):
    __hash__: ClassVar[None]  # type: ignore[assignment]

    @staticmethod
    def from_substrait(
        message: bytes | memoryview | Buffer | _SubstraitMessage,
    ) -> Expression: ...
    def to_substrait(
        self, schema: Schema, allow_arrow_extensions: bool = False
    ) -> memoryview: ...
    def __invert__(self) -> Expression: ...
    # The Expression must be the left operand: the reflected methods, e.g.
    # 1 + Expression, raise TypeError at runtime.
    def __and__(self, other: object, /) -> Expression: ...
    def __or__(self, other: object, /) -> Expression: ...
    def __add__(self, other: object, /) -> Expression: ...
    def __mul__(self, other: object, /) -> Expression: ...
    def __sub__(self, other: object, /) -> Expression: ...
    def __truediv__(self, other: object, /) -> Expression: ...
    def __eq__(self, other: object, /) -> Expression: ...  # type: ignore[override]
    def __ne__(self, other: object, /) -> Expression: ...  # type: ignore[override]
    def __gt__(self, other: object, /) -> Expression: ...
    def __lt__(self, other: object, /) -> Expression: ...
    def __ge__(self, other: object, /) -> Expression: ...
    def __le__(self, other: object, /) -> Expression: ...
    def is_valid(self) -> Expression: ...
    def is_null(self, nan_is_null: bool = False) -> Expression: ...
    def is_nan(self) -> Expression: ...
    def cast(
        self,
        type: DataType | str | None = None,
        safe: bool | None = None,
        options: CastOptions | None = None,
    ) -> Expression: ...
    def isin(self, values: Array | Iterable[Any]) -> Expression: ...
    def equals(self, other: Expression) -> bool: ...

# ==================== User-defined functions ====================

class UdfContext:
    @property
    def batch_length(self) -> int: ...
    @property
    def memory_pool(self) -> MemoryPool: ...

class _FunctionDoc(TypedDict):
    summary: str
    description: str

def register_scalar_function(
    func: Callable[..., Any],
    function_name: str,
    function_doc: _FunctionDoc | dict[str, str],
    in_types: dict[str, DataType | str],
    out_type: DataType | str,
    func_registry: FunctionRegistry | None = None,
) -> None: ...
def register_vector_function(
    func: Callable[..., Any],
    function_name: str,
    function_doc: _FunctionDoc | dict[str, str],
    in_types: dict[str, DataType | str],
    out_type: DataType | str,
    func_registry: FunctionRegistry | None = None,
) -> None: ...
def register_aggregate_function(
    func: Callable[..., Any],
    function_name: str,
    function_doc: _FunctionDoc | dict[str, str],
    in_types: dict[str, DataType | str],
    out_type: DataType | str,
    func_registry: FunctionRegistry | None = None,
) -> None: ...
def register_tabular_function(
    func: Callable[..., Any],
    function_name: str,
    function_doc: _FunctionDoc | dict[str, str],
    in_types: dict[str, DataType | str],
    out_type: Schema | DataType | str,
    func_registry: FunctionRegistry | None = None,
) -> None: ...
def call_tabular_function(
    function_name: str,
    args: Iterable[Any] | None = None,
    func_registry: FunctionRegistry | None = None,
) -> RecordBatchReader: ...
