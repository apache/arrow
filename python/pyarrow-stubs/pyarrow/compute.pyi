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

from collections.abc import Callable, Iterable, Sequence, Mapping
from typing import Literal, TypeAlias, TypeVar, overload, Any, ParamSpec

import numpy as np

# Option classes
from pyarrow._compute import ArraySortOptions as ArraySortOptions
from pyarrow._compute import AssumeTimezoneOptions as AssumeTimezoneOptions
from pyarrow._compute import CastOptions as CastOptions
from pyarrow._compute import CountOptions as CountOptions
from pyarrow._compute import CumulativeOptions as CumulativeOptions
from pyarrow._compute import CumulativeSumOptions as CumulativeSumOptions
from pyarrow._compute import DayOfWeekOptions as DayOfWeekOptions
from pyarrow._compute import DictionaryEncodeOptions as DictionaryEncodeOptions
from pyarrow._compute import ElementWiseAggregateOptions as ElementWiseAggregateOptions

# Expressions
from pyarrow._compute import Expression as Expression
from pyarrow._compute import ExtractRegexOptions as ExtractRegexOptions
from pyarrow._compute import ExtractRegexSpanOptions as ExtractRegexSpanOptions
from pyarrow._compute import FilterOptions as FilterOptions
from pyarrow._compute import FunctionOptions as FunctionOptions  # noqa: F401
from pyarrow._compute import IndexOptions as IndexOptions
from pyarrow._compute import JoinOptions as JoinOptions
from pyarrow._compute import ListFlattenOptions as ListFlattenOptions
from pyarrow._compute import ListSliceOptions as ListSliceOptions
from pyarrow._compute import MakeStructOptions as MakeStructOptions
from pyarrow._compute import MapLookupOptions as MapLookupOptions
from pyarrow._compute import MatchSubstringOptions as MatchSubstringOptions
from pyarrow._compute import ModeOptions as ModeOptions
from pyarrow._compute import NullOptions as NullOptions
from pyarrow._compute import PadOptions as PadOptions
from pyarrow._compute import PairwiseOptions as PairwiseOptions
from pyarrow._compute import PartitionNthOptions as PartitionNthOptions
from pyarrow._compute import PivotWiderOptions as PivotWiderOptions
from pyarrow._compute import QuantileOptions as QuantileOptions
from pyarrow._compute import RandomOptions as RandomOptions
from pyarrow._compute import RankOptions as RankOptions
from pyarrow._compute import RankQuantileOptions as RankQuantileOptions
from pyarrow._compute import ReplaceSliceOptions as ReplaceSliceOptions
from pyarrow._compute import ReplaceSubstringOptions as ReplaceSubstringOptions
from pyarrow._compute import RoundBinaryOptions as RoundBinaryOptions
from pyarrow._compute import RoundOptions as RoundOptions
from pyarrow._compute import RoundTemporalOptions as RoundTemporalOptions
from pyarrow._compute import RoundToMultipleOptions as RoundToMultipleOptions
from pyarrow._compute import RunEndEncodeOptions as RunEndEncodeOptions
from pyarrow._compute import ScalarAggregateOptions as ScalarAggregateOptions
from pyarrow._compute import SelectKOptions as SelectKOptions
from pyarrow._compute import SetLookupOptions as SetLookupOptions
from pyarrow._compute import SkewOptions as SkewOptions
from pyarrow._compute import SliceOptions as SliceOptions
from pyarrow._compute import SortOptions as SortOptions
from pyarrow._compute import SplitOptions as SplitOptions
from pyarrow._compute import SplitPatternOptions as SplitPatternOptions
from pyarrow._compute import StrftimeOptions as StrftimeOptions
from pyarrow._compute import StrptimeOptions as StrptimeOptions
from pyarrow._compute import StructFieldOptions as StructFieldOptions
from pyarrow._compute import TakeOptions as TakeOptions
from pyarrow._compute import TDigestOptions as TDigestOptions
from pyarrow._compute import TrimOptions as TrimOptions
from pyarrow._compute import Utf8NormalizeOptions as Utf8NormalizeOptions
from pyarrow._compute import VarianceOptions as VarianceOptions
from pyarrow._compute import WeekOptions as WeekOptions
from pyarrow._compute import WinsorizeOptions as WinsorizeOptions
from pyarrow._compute import ZeroFillOptions as ZeroFillOptions

# Functions
from pyarrow._compute import call_function as call_function
from pyarrow._compute import call_tabular_function as call_tabular_function
from pyarrow._compute import get_function as get_function
from pyarrow._compute import list_functions as list_functions
from pyarrow._compute import register_scalar_function as register_scalar_function
from pyarrow._compute import register_aggregate_function as register_aggregate_function
from pyarrow._compute import register_vector_function as register_vector_function
from pyarrow._compute import register_tabular_function as register_tabular_function

from pyarrow._compute import cast as cast
from pyarrow._compute import take as take
from pyarrow._compute import sort_indices as sort_indices

# Function and Kernel classes
from pyarrow._compute import Function as Function
from pyarrow._compute import Kernel as Kernel
from pyarrow._compute import ScalarFunction as ScalarFunction
from pyarrow._compute import ScalarKernel as ScalarKernel
from pyarrow._compute import VectorFunction as VectorFunction
from pyarrow._compute import VectorKernel as VectorKernel
from pyarrow._compute import ScalarAggregateFunction as ScalarAggregateFunction
from pyarrow._compute import ScalarAggregateKernel as ScalarAggregateKernel
from pyarrow._compute import HashAggregateFunction as HashAggregateFunction
from pyarrow._compute import HashAggregateKernel as HashAggregateKernel

# Udf

from pyarrow._compute import _Order, _Placement
from pyarrow._stubs_typing import ArrayLike, ScalarLike, PyScalar
from pyarrow._types import _RunEndType
from . import lib

_P = ParamSpec("_P")
_R = TypeVar("_R")


class _ExprComparable(Expression):
    def __ge__(self, other: Any) -> Expression: ...
    def __le__(self, other: Any) -> Expression: ...
    def __gt__(self, other: Any) -> Expression: ...
    def __lt__(self, other: Any) -> Expression: ...

def field(*name_or_index: str | bytes | tuple[str | int, ...] | int) -> Expression: ...
def __ge__(self, other: Any) -> Expression: ...


def scalar(value: PyScalar | lib.Scalar[Any] | Mapping | lib.int64() | None) -> Expression: ...


def _clone_signature(f: Callable[_P, _R]) -> Callable[_P, _R]: ...


# ============= compute functions =============
_DataTypeT = TypeVar("_DataTypeT", bound=lib.DataType)
_Scalar_CoT = TypeVar("_Scalar_CoT", bound=lib.Scalar, covariant=True)
_ScalarT = TypeVar("_ScalarT", bound=lib.Scalar)
_ArrayT = TypeVar("_ArrayT", bound=lib.Array | lib.ChunkedArray)
_ScalarOrArrayT = TypeVar("_ScalarOrArrayT", bound=lib.Array |
                          lib.Scalar | lib.ChunkedArray)
ArrayOrChunkedArray: TypeAlias = lib.Array[_Scalar_CoT] | lib.ChunkedArray[_Scalar_CoT]
ScalarOrArray: TypeAlias = ArrayOrChunkedArray[_Scalar_CoT] | _Scalar_CoT

SignedIntegerScalar: TypeAlias = (
    lib.Scalar[lib.Int8Type]
    | lib.Scalar[lib.Int16Type]
    | lib.Scalar[lib.Int32Type]
    | lib.Scalar[lib.Int64Type]
)
UnsignedIntegerScalar: TypeAlias = (
    lib.Scalar[lib.UInt8Type]
    | lib.Scalar[lib.UInt16Type]
    | lib.Scalar[lib.Uint32Type]
    | lib.Scalar[lib.UInt64Type]
)
IntegerScalar: TypeAlias = SignedIntegerScalar | UnsignedIntegerScalar
FloatScalar: TypeAlias = (lib.Scalar[lib.Float16Type] | lib.Scalar[lib.Float32Type]
                          | lib.Scalar[lib.Float64Type])
DecimalScalar: TypeAlias = (
    lib.Scalar[lib.Decimal32Type]
    | lib.Scalar[lib.Decimal64Type]
    | lib.Scalar[lib.Decimal128Type]
    | lib.Scalar[lib.Decimal256Type]
)
NonFloatNumericScalar: TypeAlias = IntegerScalar | DecimalScalar
NumericScalar: TypeAlias = IntegerScalar | FloatScalar | DecimalScalar
BinaryScalar: TypeAlias = (
    lib.Scalar[lib.BinaryType]
    | lib.Scalar[lib.LargeBinaryType]
    | lib.Scalar[lib.FixedSizeBinaryType]
)
StringScalar: TypeAlias = lib.Scalar[lib.StringType] | lib.Scalar[lib.LargeStringType]
StringOrBinaryScalar: TypeAlias = StringScalar | BinaryScalar
_ListScalar: TypeAlias = (
    lib.ListViewScalar[_DataTypeT] | lib.FixedSizeListScalar[_DataTypeT, Any]
)
_LargeListScalar: TypeAlias = (
    lib.LargeListScalar[_DataTypeT] | lib.LargeListViewScalar[_DataTypeT]
)
ListScalar: TypeAlias = (
    lib.ListScalar[_DataTypeT] | _ListScalar[_DataTypeT] | _LargeListScalar[_DataTypeT]
)
TemporalScalar: TypeAlias = (
    lib.Date32Scalar
    | lib.Date64Scalar
    | lib.Time32Scalar[Any]
    | lib.Time64Scalar[Any]
    | lib.TimestampScalar[Any]
    | lib.DurationScalar[Any]
    | lib.MonthDayNanoIntervalScalar
)
NumericOrDurationScalar: TypeAlias = NumericScalar | lib.DurationScalar
NumericOrTemporalScalar: TypeAlias = NumericScalar | TemporalScalar

_NumericOrTemporalScalarT = TypeVar(
    "_NumericOrTemporalScalarT", bound=NumericOrTemporalScalar)
_NumericScalarT = TypeVar("_NumericScalarT", bound=NumericScalar)
NumericArray: TypeAlias = ArrayOrChunkedArray[_NumericScalarT]
_NumericArrayT = TypeVar("_NumericArrayT", bound=NumericArray)
_NumericOrDurationT = TypeVar("_NumericOrDurationT", bound=NumericOrDurationScalar)
NumericOrDurationArray: TypeAlias = ArrayOrChunkedArray[NumericOrDurationScalar]
_NumericOrDurationArrayT = TypeVar(
    "_NumericOrDurationArrayT", bound=NumericOrDurationArray)
NumericOrTemporalArray: TypeAlias = ArrayOrChunkedArray[_NumericOrTemporalScalarT]
_NumericOrTemporalArrayT = TypeVar(
    "_NumericOrTemporalArrayT", bound=NumericOrTemporalArray)
BooleanArray: TypeAlias = ArrayOrChunkedArray[lib.BooleanScalar]
_BooleanArrayT = TypeVar("_BooleanArrayT", bound=BooleanArray)
IntegerArray: TypeAlias = ArrayOrChunkedArray[IntegerScalar]
_FloatScalarT = TypeVar("_FloatScalarT", bound=FloatScalar)
FloatArray: TypeAlias = ArrayOrChunkedArray[FloatScalar]
_FloatArrayT = TypeVar("_FloatArrayT", bound=FloatArray)
_StringScalarT = TypeVar("_StringScalarT", bound=StringScalar)
StringArray: TypeAlias = ArrayOrChunkedArray[StringScalar]
_StringArrayT = TypeVar("_StringArrayT", bound=StringArray)
_BinaryScalarT = TypeVar("_BinaryScalarT", bound=BinaryScalar)
BinaryArray: TypeAlias = ArrayOrChunkedArray[BinaryScalar]
_BinaryArrayT = TypeVar("_BinaryArrayT", bound=BinaryArray)
_StringOrBinaryScalarT = TypeVar("_StringOrBinaryScalarT", bound=StringOrBinaryScalar)
StringOrBinaryArray: TypeAlias = StringArray | BinaryArray
_StringOrBinaryArrayT = TypeVar("_StringOrBinaryArrayT", bound=StringOrBinaryArray)
_TemporalScalarT = TypeVar("_TemporalScalarT", bound=TemporalScalar)
TemporalArray: TypeAlias = ArrayOrChunkedArray[TemporalScalar]
_TemporalArrayT = TypeVar("_TemporalArrayT", bound=TemporalArray)
_ListArray: TypeAlias = ArrayOrChunkedArray[_ListScalar[_DataTypeT]]
_LargeListArray: TypeAlias = ArrayOrChunkedArray[_LargeListScalar[_DataTypeT]]
ListArray: TypeAlias = ArrayOrChunkedArray[ListScalar[_DataTypeT]]
# =============================== 1. Aggregation ===============================
def array_take(
    array: lib.Array | lib.ChunkedArray | lib.Scalar | lib.Table,
    indices: Any,
    *,
    boundscheck: bool | None = None,
    options: TakeOptions | None = None,
) -> Any: ...



# ========================= 1.1 functions =========================


def all(
    array: lib.BooleanScalar | BooleanArray,
    /,
    *,
    skip_nulls: bool = True,
    min_count: int = 1,
    options: ScalarAggregateOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.BooleanScalar: ...


any = _clone_signature(all)


def approximate_median(
    array: NumericScalar | NumericArray,
    /,
    *,
    skip_nulls: bool = True,
    min_count: int = 1,
    options: ScalarAggregateOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.DoubleScalar: ...


def count(
    array: lib.Array | lib.ChunkedArray,
    /,
    mode: Literal["only_valid", "only_null", "all"] = "only_valid",
    *,
    options: CountOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.Int64Scalar: ...


def count_distinct(
    array: lib.Array | lib.ChunkedArray,
    /,
    mode: Literal["only_valid", "only_null", "all"] = "only_valid",
    *,
    options: CountOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.Int64Scalar: ...


def first(
    array: lib.Array[_ScalarT] | lib.ChunkedArray[_ScalarT],
    /,
    *,
    skip_nulls: bool = True,
    min_count: int = 1,
    options: ScalarAggregateOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> _ScalarT: ...


def first_last(
    array: lib.Array[Any] | lib.ChunkedArray[Any],
    /,
    *,
    skip_nulls: bool = True,
    min_count: int = 1,
    options: ScalarAggregateOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.StructScalar: ...


def index(
    data: lib.Array[Any] | lib.ChunkedArray[Any],
    value: ScalarLike,
    start: int | None = None,
    end: int | None = None,
    *,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.Int64Scalar: ...


last = _clone_signature(first)
max = _clone_signature(first)
min = _clone_signature(first)
min_max = _clone_signature(first_last)


def mean(
    array: FloatScalar | FloatArray
    | lib.NumericArray[lib.Scalar[Any]]
    | lib.ChunkedArray[lib.Scalar[Any]]
    | lib.Scalar[Any],
    /,
    *,
    skip_nulls: bool = True,
    min_count: int = 1,
    options: ScalarAggregateOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.Scalar[Any]: ...


def mode(
    array: NumericScalar | NumericArray,
    /,
    n: int = 1,
    *,
    skip_nulls: bool = True,
    min_count: int = 0,
    options: ModeOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.StructArray: ...


def product(
    array: _ScalarT | lib.NumericArray[_ScalarT],
    /,
    *,
    skip_nulls: bool = True,
    min_count: int = 1,
    options: ScalarAggregateOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> _ScalarT: ...


def quantile(
    array: NumericScalar | NumericArray,
    /,
    q: float | Sequence[float] = 0.5,
    *,
    interpolation: Literal["linear", "lower",
                           "higher", "nearest", "midpoint"] = "linear",
    skip_nulls: bool = True,
    min_count: int = 0,
    options: QuantileOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.DoubleArray: ...


def stddev(
    array: NumericScalar | NumericArray,
    /,
    *,
    ddof: float = 0,
    skip_nulls: bool = True,
    min_count: int = 0,
    options: VarianceOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.DoubleScalar: ...


def sum(
    array: _NumericScalarT | NumericArray[_NumericScalarT] | lib.Expression,
    /,
    *,
    skip_nulls: bool = True,
    min_count: int = 1,
    options: ScalarAggregateOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> _NumericScalarT | lib.Expression: ...


def tdigest(
    array: NumericScalar | NumericArray,
    /,
    q: float | Sequence[float] = 0.5,
    *,
    delta: int = 100,
    buffer_size: int = 500,
    skip_nulls: bool = True,
    min_count: int = 0,
    options: TDigestOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.DoubleArray: ...


def variance(
    array: NumericScalar | NumericArray | ArrayLike,
    /,
    *,
    ddof: int = 0,
    skip_nulls: bool = True,
    min_count: int = 0,
    options: VarianceOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.DoubleScalar: ...


def winsorize(
    array: _NumericArrayT,
    /,
    lower_limit: float = 0.0,
    upper_limit: float = 1.0,
    *,
    options: WinsorizeOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> _NumericArrayT: ...


def skew(
    array: NumericScalar | NumericArray | ArrayLike,
    /,
    *,
    skip_nulls: bool = True,
    biased: bool = True,
    min_count: int = 0,
    options: SkewOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.DoubleScalar: ...


def kurtosis(
    array: NumericScalar | NumericArray | ArrayLike,
    /,
    *,
    skip_nulls: bool = True,
    biased: bool = True,
    min_count: int = 0,
    options: SkewOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.DoubleScalar: ...


def top_k_unstable(
    values: lib.Array | lib.ChunkedArray | lib.RecordBatch | lib.Table,
    k: int,
    sort_keys: list | None = None,
    *,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.Array: ...


def bottom_k_unstable(
    values: lib.Array | lib.ChunkedArray | lib.RecordBatch | lib.Table,
    k: int,
    sort_keys: list | None = None,
    *,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.Array: ...


# ========================= 2. Element-wise (“scalar”) functions =========

# ========================= 2.1 Arithmetic =========================
def abs(x: _NumericOrDurationT | _NumericOrDurationArrayT | Expression, /, *,
        memory_pool: lib.MemoryPool | None = None) -> (
    _NumericOrDurationT | _NumericOrDurationArrayT | Expression): ...


abs_checked = _clone_signature(abs)


def add(
    x: (_NumericOrTemporalScalarT | NumericOrTemporalScalar | _NumericOrTemporalArrayT
        | ArrayLike | int | Expression),
    y: (_NumericOrTemporalScalarT | NumericOrTemporalScalar | _NumericOrTemporalArrayT
        | ArrayLike | int | Expression),
    /,
    *,
    memory_pool: lib.MemoryPool | None = None,
) -> _NumericOrTemporalScalarT | _NumericOrTemporalArrayT | Expression: ...


add_checked = _clone_signature(add)


def divide(
    x: (_NumericOrTemporalScalarT | NumericOrTemporalScalar | _NumericOrTemporalArrayT
        | Expression),
    y: (_NumericOrTemporalScalarT | NumericOrTemporalScalar | _NumericOrTemporalArrayT
        | Expression),
    /,
    *,
    memory_pool: lib.MemoryPool | None = None,
) -> _NumericOrTemporalScalarT | _NumericOrTemporalArrayT | Expression: ...


divide_checked = _clone_signature(divide)


def exp(
    exponent: _FloatArrayT | ArrayOrChunkedArray[NonFloatNumericScalar] | _FloatScalarT
    | NonFloatNumericScalar | lib.DoubleScalar | Expression,
    /, *, memory_pool: lib.MemoryPool | None = None
) -> (
    _FloatArrayT | lib.DoubleArray | _FloatScalarT | lib.DoubleScalar | Expression): ...


multiply = _clone_signature(add)
multiply_checked = _clone_signature(add)


def negate(
    x: _NumericOrDurationT | _NumericOrDurationArrayT | Expression, /, *,
    memory_pool: lib.MemoryPool | None = None) -> (
    _NumericOrDurationT | _NumericOrDurationArrayT | Expression): ...


negate_checked = _clone_signature(negate)


def power(
    base: _NumericScalarT | Expression | _NumericArrayT | NumericScalar,
    exponent: _NumericScalarT | Expression | _NumericArrayT | NumericScalar,
    /,
    *,
    memory_pool: lib.MemoryPool | None = None,
) -> _NumericScalarT | _NumericArrayT | Expression: ...


power_checked = _clone_signature(power)


def sign(
    x: NumericOrDurationArray | NumericOrDurationScalar | Expression, /, *,
    memory_pool: lib.MemoryPool | None = None
) -> (
    lib.NumericArray[lib.Int8Scalar]
    | lib.NumericArray[lib.FloatScalar]
    | lib.NumericArray[lib.DoubleScalar]
    | lib.Int8Scalar | lib.FloatScalar | lib.DoubleScalar | Expression
): ...


def sqrt(
    x: NumericArray | NumericScalar | Expression, /, *,
    memory_pool: lib.MemoryPool | None = None) -> (
    FloatArray | FloatScalar | Expression): ...


sqrt_checked = _clone_signature(sqrt)

subtract = _clone_signature(add)
subtract_checked = _clone_signature(add)

# ========================= 2.1 Bit-wise functions =========================


def bit_wise_and(
    x: _NumericScalarT | _NumericArrayT | NumericScalar | Expression
    | ArrayOrChunkedArray[NumericScalar],
    y: _NumericScalarT | _NumericArrayT | NumericScalar | Expression
    | ArrayOrChunkedArray[NumericScalar],
    /, *, memory_pool: lib.MemoryPool | None = None
) -> _NumericScalarT | _NumericArrayT | Expression: ...


def bit_wise_not(
    x: _NumericScalarT | _NumericArrayT | Expression, /, *,
    memory_pool: lib.MemoryPool | None = None
) -> _NumericScalarT | _NumericArrayT | Expression: ...


bit_wise_or = _clone_signature(bit_wise_and)
bit_wise_xor = _clone_signature(bit_wise_and)
shift_left = _clone_signature(bit_wise_and)
shift_left_checked = _clone_signature(bit_wise_and)
shift_right = _clone_signature(bit_wise_and)
shift_right_checked = _clone_signature(bit_wise_and)

# ========================= 2.2 Rounding functions =========================


def ceil(
    x: _FloatScalarT | _FloatArrayT | Expression, /, *, memory_pool: lib.MemoryPool |
    None = None) -> _FloatScalarT | _FloatArrayT | Expression: ...


floor = _clone_signature(ceil)


def round(
    x: _NumericScalarT | _NumericArrayT | Expression,
    /,
    ndigits: int = 0,
    round_mode: Literal[
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
    ] = "half_to_even",
    *,
    options: RoundOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> _NumericScalarT | _NumericArrayT | Expression: ...


def round_to_multiple(
    x: _NumericScalarT | _NumericArrayT | list | Expression,
    /,
    multiple: int = 0,
    round_mode: Literal[
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
    ] = "half_to_even",
    *,
    options: RoundToMultipleOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> _NumericScalarT | _NumericArrayT | Expression: ...


def round_binary(
    x: _NumericScalarT | _NumericArrayT | int | float | list | Expression,
    s: (int | float | lib.Int8Scalar | lib.Int16Scalar | lib.Int32Scalar | lib.Int64Scalar
        | lib.Scalar | Iterable | Expression),
    /,
    round_mode: Literal[
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
    ] = "half_to_even",
    *,
    options: RoundBinaryOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> (
    _NumericScalarT | lib.NumericArray[_NumericScalarT] | _NumericArrayT
    | Expression): ...


trunc = _clone_signature(ceil)

# ========================= 2.3 Logarithmic functions =========================


def ln(
    x: FloatScalar | FloatArray | Expression, /, *,
    memory_pool: lib.MemoryPool | None = None
) -> (
    lib.FloatScalar | lib.DoubleScalar | lib.NumericArray[lib.FloatScalar]
    | lib.NumericArray[lib.DoubleScalar] | Expression): ...


ln_checked = _clone_signature(ln)
log10 = _clone_signature(ln)
log10_checked = _clone_signature(ln)
log1p = _clone_signature(ln)
log1p_checked = _clone_signature(ln)
log2 = _clone_signature(ln)
log2_checked = _clone_signature(ln)


def logb(
    x: FloatScalar | FloatArray | Expression | Any,
    b: FloatScalar | FloatArray | Expression | Any,
    /, *, memory_pool: lib.MemoryPool | None = None
) -> (
    lib.FloatScalar | lib.DoubleScalar | lib.NumericArray[lib.FloatScalar]
    | lib.NumericArray[lib.DoubleScalar] | Expression | Any): ...


logb_checked = _clone_signature(logb)

# ========================= 2.4 Trigonometric functions =========================
acos = _clone_signature(ln)
acos_checked = _clone_signature(ln)
acosh = _clone_signature(ln)
asin = _clone_signature(ln)
asin_checked = _clone_signature(ln)
asinh = _clone_signature(ln)
atan = _clone_signature(ln)
atanh = _clone_signature(ln)
cos = _clone_signature(ln)
cos_checked = _clone_signature(ln)
cosh = _clone_signature(ln)
sin = _clone_signature(ln)
sin_checked = _clone_signature(ln)
sinh = _clone_signature(ln)
tan = _clone_signature(ln)
tan_checked = _clone_signature(ln)
tanh = _clone_signature(ln)


def atan2(
    y: FloatScalar | FloatArray | Expression | Any,
    x: FloatScalar | FloatArray | Expression | Any,
    /, *, memory_pool: lib.MemoryPool | None = None
) -> (
    lib.FloatScalar | lib.DoubleScalar | lib.NumericArray[lib.FloatScalar]
    | lib.NumericArray[lib.DoubleScalar] | Expression): ...


# ========================= 2.5 Comparisons functions =========================
def equal(
    x: lib.Scalar | lib.Array | lib.ChunkedArray | list | Expression | Any,
    y: lib.Scalar | lib.Array | lib.ChunkedArray | list | Expression | Any,
    /, *, memory_pool: lib.MemoryPool | None = None
) -> lib.BooleanScalar | lib.BooleanArray | Expression: ...


greater = _clone_signature(equal)
greater_equal = _clone_signature(equal)
less = _clone_signature(equal)
less_equal = _clone_signature(equal)
not_equal = _clone_signature(equal)


def max_element_wise(
    *args: ScalarOrArray[_Scalar_CoT] | Expression | ScalarLike | ArrayLike,
    skip_nulls: bool = True,
    options: ElementWiseAggregateOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> _Scalar_CoT | Expression | lib.Scalar | lib.Array: ...


min_element_wise = _clone_signature(max_element_wise)

# ========================= 2.6 Logical functions =========================


def and_(
    x: lib.BooleanScalar | BooleanArray | Expression | ScalarOrArray[lib.BooleanScalar],
    y: lib.BooleanScalar | BooleanArray | Expression | ScalarOrArray[lib.BooleanScalar],
    /, *, memory_pool: lib.MemoryPool | None = None
) -> (
    lib.BooleanScalar | lib.BooleanArray | Expression
    | ScalarOrArray[lib.BooleanScalar]): ...


and_kleene = _clone_signature(and_)
and_not = _clone_signature(and_)
and_not_kleene = _clone_signature(and_)
or_ = _clone_signature(and_)
or_kleene = _clone_signature(and_)
xor = _clone_signature(and_)


def invert(
    x: lib.BooleanScalar | _BooleanArrayT | Expression, /, *,
    memory_pool: lib.MemoryPool | None = None
) -> lib.BooleanScalar | _BooleanArrayT | Expression: ...


# ========================= 2.10 String predicates =========================
def ascii_is_alnum(
    strings: StringScalar | StringArray | Expression, /, *,
    memory_pool: lib.MemoryPool | None = None
) -> lib.BooleanScalar | lib.BooleanArray | Expression: ...


ascii_is_alpha = _clone_signature(ascii_is_alnum)
ascii_is_decimal = _clone_signature(ascii_is_alnum)
ascii_is_lower = _clone_signature(ascii_is_alnum)
ascii_is_printable = _clone_signature(ascii_is_alnum)
ascii_is_space = _clone_signature(ascii_is_alnum)
ascii_is_upper = _clone_signature(ascii_is_alnum)
utf8_is_alnum = _clone_signature(ascii_is_alnum)
utf8_is_alpha = _clone_signature(ascii_is_alnum)
utf8_is_decimal = _clone_signature(ascii_is_alnum)
utf8_is_digit = _clone_signature(ascii_is_alnum)
utf8_is_lower = _clone_signature(ascii_is_alnum)
utf8_is_numeric = _clone_signature(ascii_is_alnum)
utf8_is_printable = _clone_signature(ascii_is_alnum)
utf8_is_space = _clone_signature(ascii_is_alnum)
utf8_is_upper = _clone_signature(ascii_is_alnum)
ascii_is_title = _clone_signature(ascii_is_alnum)
utf8_is_title = _clone_signature(ascii_is_alnum)
string_is_ascii = _clone_signature(ascii_is_alnum)

# ========================= 2.11 String transforms =========================


def ascii_capitalize(
    strings: _StringScalarT | _StringArrayT | Expression, /, *,
    memory_pool: lib.MemoryPool | None = None
) -> _StringScalarT | _StringArrayT | Expression: ...


ascii_lower = _clone_signature(ascii_capitalize)
ascii_reverse = _clone_signature(ascii_capitalize)
ascii_swapcase = _clone_signature(ascii_capitalize)
ascii_title = _clone_signature(ascii_capitalize)
ascii_upper = _clone_signature(ascii_capitalize)


def binary_length(
    strings: ScalarOrArray[StringOrBinaryScalar] | Expression,
    /,
    *,
    memory_pool: lib.MemoryPool | None = None,
) -> (
    lib.Int32Scalar | lib.Int64Scalar | lib.Int32Array | lib.Int64Array
    | Expression
): ...


def binary_repeat(
    strings: _StringOrBinaryScalarT | _StringOrBinaryArrayT | Expression,
    num_repeats: int | list[int] | list[int | None],
    /,
    *,
    memory_pool: lib.MemoryPool | None = None,
) -> (
    _StringOrBinaryScalarT | lib.Array[_StringOrBinaryScalarT] | _StringOrBinaryArrayT
    | Expression): ...


def binary_replace_slice(
    strings: _StringOrBinaryScalarT | _StringOrBinaryArrayT | Expression,
    /,
    start: int,
    stop: int,
    replacement: str | bytes,
    *,
    options: ReplaceSliceOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> _StringOrBinaryScalarT | _StringOrBinaryArrayT | Expression: ...


def binary_reverse(
    strings: _BinaryScalarT | _BinaryArrayT | Expression, /, *,
    memory_pool: lib.MemoryPool | None = None
) -> _BinaryScalarT | _BinaryArrayT | Expression: ...


def replace_substring(
    strings: _StringScalarT | _StringArrayT | Expression,
    /,
    pattern: str | bytes,
    replacement: str | bytes,
    *,
    max_replacements: int | None = None,
    options: ReplaceSubstringOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> _StringScalarT | _StringArrayT | Expression: ...


replace_substring_regex = _clone_signature(replace_substring)


def utf8_capitalize(
    strings: _StringScalarT | _StringArrayT | Expression, /, *,
    memory_pool: lib.MemoryPool | None = None
) -> _StringScalarT | _StringArrayT | Expression: ...


def utf8_length(
    strings: lib.StringScalar | lib.LargeStringScalar | lib.StringArray
    | lib.ChunkedArray[lib.StringScalar] | lib.LargeStringArray
    | lib.ChunkedArray[lib.LargeStringScalar] | Expression,
    /, *, memory_pool: lib.MemoryPool | None = None
) -> (
    lib.Int32Scalar | lib.Int64Scalar | lib.Int32Array | lib.Int64Array
    | Expression): ...


utf8_lower = _clone_signature(utf8_capitalize)


def utf8_replace_slice(
    strings: _StringScalarT | _StringArrayT | Expression,
    /,
    start: int,
    stop: int,
    replacement: str | bytes,
    *,
    options: ReplaceSliceOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> _StringScalarT | _StringArrayT | Expression: ...


utf8_reverse = _clone_signature(utf8_capitalize)
utf8_swapcase = _clone_signature(utf8_capitalize)
utf8_title = _clone_signature(utf8_capitalize)
utf8_upper = _clone_signature(utf8_capitalize)

# ========================= 2.12 String padding =========================


def ascii_center(
    strings: _StringScalarT | _StringArrayT | Expression,
    /,
    width: int | None = None,
    padding: str = " ",
    lean_left_on_odd_padding: bool = True,
    *,
    options: PadOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> _StringScalarT | _StringArrayT | Expression: ...


ascii_lpad = _clone_signature(ascii_center)
ascii_rpad = _clone_signature(ascii_center)
utf8_center = _clone_signature(ascii_center)
utf8_lpad = _clone_signature(ascii_center)
utf8_rpad = _clone_signature(ascii_center)

def utf8_zero_fill(
    strings: _StringScalarT | _StringArrayT | Expression,
    /,
    width: int | None = None,
    padding: str = "0",
    *,
    options: ZeroFillOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> _StringScalarT | _StringArrayT | Expression: ...

utf8_zfill = utf8_zero_fill

# ========================= 2.13 String trimming =========================


def ascii_ltrim(
    strings: _StringScalarT | _StringArrayT | Expression,
    /,
    characters: str,
    *,
    options: TrimOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> _StringScalarT | _StringArrayT | Expression: ...


ascii_rtrim = _clone_signature(ascii_ltrim)
ascii_trim = _clone_signature(ascii_ltrim)
utf8_ltrim = _clone_signature(ascii_ltrim)
utf8_rtrim = _clone_signature(ascii_ltrim)
utf8_trim = _clone_signature(ascii_ltrim)


def ascii_ltrim_whitespace(
    strings: _StringScalarT | _StringArrayT | Expression,
    /,
    *,
    options: TrimOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> _StringScalarT | _StringArrayT | Expression: ...


ascii_rtrim_whitespace = _clone_signature(ascii_ltrim_whitespace)
ascii_trim_whitespace = _clone_signature(ascii_ltrim_whitespace)
utf8_ltrim_whitespace = _clone_signature(ascii_ltrim_whitespace)
utf8_rtrim_whitespace = _clone_signature(ascii_ltrim_whitespace)
utf8_trim_whitespace = _clone_signature(ascii_ltrim_whitespace)

# ========================= 2.14 String splitting =========================


def ascii_split_whitespace(
    strings: _StringScalarT | lib.Array[lib.Scalar[_DataTypeT]] | Expression,
    /,
    *,
    max_splits: int | None = None,
    reverse: bool = False,
    options: SplitOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> (
    lib.ListArray[_StringScalarT] | lib.ListArray[lib.ListScalar[_DataTypeT]]
    | Expression): ...


def split_pattern(
    strings: _StringOrBinaryScalarT | lib.Array[lib.Scalar[_DataTypeT]] | Expression,
    /,
    pattern: str,
    *,
    max_splits: int | None = None,
    reverse: bool = False,
    options: SplitOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> (
    lib.ListArray[_StringOrBinaryScalarT] | lib.ListArray[lib.ListScalar[_DataTypeT]]
    | Expression): ...


split_pattern_regex = _clone_signature(split_pattern)
utf8_split_whitespace = _clone_signature(ascii_split_whitespace)

# ========================= 2.15 String component extraction =========================


def extract_regex(
    strings: StringOrBinaryScalar | StringOrBinaryArray | Expression,
    /,
    pattern: str,
    *,
    options: ExtractRegexOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.StructScalar | lib.StructArray | Expression: ...


extract_regex_span = _clone_signature(extract_regex)


# ========================= 2.16 String join =========================
def binary_join(
    strings, separator, /, *, memory_pool: lib.MemoryPool | None = None
) -> StringScalar | StringArray: ...


def binary_join_element_wise(
    *strings: str | bytes | _StringOrBinaryScalarT | _StringOrBinaryArrayT | Expression | list,
    null_handling: Literal["emit_null", "skip", "replace"] = "emit_null",
    null_replacement: str = "",
    options: JoinOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> _StringOrBinaryScalarT | _StringOrBinaryArrayT | Expression: ...


# ========================= 2.17 String Slicing =========================
def binary_slice(
    strings: _BinaryScalarT | _BinaryArrayT | Expression | lib.Scalar,
    /,
    start: int,
    stop: int | None = None,
    step: int = 1,
    *,
    options: SliceOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> _BinaryScalarT | _BinaryArrayT | Expression: ...


def utf8_slice_codeunits(
    strings: _StringScalarT | _StringArrayT | Expression,
    /,
    start: int,
    stop: int | None = None,
    step: int = 1,
    *,
    options: SliceOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> _StringScalarT | _StringArrayT | Expression: ...


def utf8_normalize(
    strings: _StringScalarT | _StringArrayT | Expression,
    /,
    form: Literal["NFC", "NFKC", "NFD", "NFKD"] = "NFC",
    *,
    options: Utf8NormalizeOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> _StringScalarT | _StringArrayT | Expression: ...


# ========================= 2.18 Containment tests =========================
def count_substring(
    strings: lib.StringScalar | lib.BinaryScalar | lib.LargeStringScalar
    | lib.LargeBinaryScalar | lib.StringArray | lib.BinaryArray
    | lib.ChunkedArray[lib.StringScalar] | lib.ChunkedArray[lib.BinaryScalar]
    | lib.LargeStringArray | lib.LargeBinaryArray
    | lib.ChunkedArray[lib.LargeStringScalar] | lib.ChunkedArray[lib.LargeBinaryScalar]
    | Expression,
    /,
    pattern: str,
    *,
    ignore_case: bool = False,
    options: MatchSubstringOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> (
    lib.Int32Scalar | lib.Int64Scalar | lib.Int32Array | lib.Int64Array
    | Expression): ...


count_substring_regex = _clone_signature(count_substring)


def ends_with(
    strings: StringScalar | BinaryScalar | StringArray | BinaryArray | Expression,
    /,
    pattern: str,
    *,
    ignore_case: bool = False,
    options: MatchSubstringOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.BooleanScalar | lib.BooleanArray | Expression: ...


find_substring = _clone_signature(count_substring)
find_substring_regex = _clone_signature(count_substring)


def index_in(
    values: lib.Scalar | lib.Array | lib.ChunkedArray | Expression,
    /,
    value_set: lib.Array | lib.ChunkedArray | Expression,
    *,
    skip_nulls: bool = False,
    options: SetLookupOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.Int32Scalar | lib.Int32Array | Expression: ...


def is_in(
    values: lib.Scalar | lib.Array | lib.ChunkedArray | Expression,
    /,
    value_set: lib.Array | lib.ChunkedArray | Expression,
    *,
    skip_nulls: bool = False,
    options: SetLookupOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.BooleanScalar | lib.BooleanArray | Expression: ...


match_like = _clone_signature(ends_with)
match_substring = _clone_signature(ends_with)
match_substring_regex = _clone_signature(ends_with)
starts_with = _clone_signature(ends_with)

# ========================= 2.19 Categorizations =========================


def is_finite(
    values: NumericScalar | lib.NullScalar | NumericArray | lib.NullArray | Expression,
    /, *, memory_pool: lib.MemoryPool | None = None
) -> lib.BooleanScalar | lib.BooleanArray | Expression: ...


is_inf = _clone_signature(is_finite)
is_nan = _clone_signature(is_finite)


def is_null(
    values: lib.Scalar | lib.Array | lib.ChunkedArray | Expression,
    /,
    *,
    nan_is_null: bool = False,
    options: NullOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.BooleanScalar | lib.BooleanArray | Expression: ...


def is_valid(
    values: lib.Scalar | lib.Array | lib.ChunkedArray | Expression | ArrayLike,
    /, *, memory_pool: lib.MemoryPool | None = None
) -> lib.BooleanScalar | lib.BooleanArray | Expression: ...


true_unless_null = _clone_signature(is_valid)

# ========================= 2.20 Selecting / multiplexing =========================


def case_when(
    cond: lib.StructScalar | lib.StructArray | lib.ChunkedArray[lib.StructScalar] | Expression,
    /,
    *cases: _ScalarOrArrayT | ArrayLike, memory_pool: lib.MemoryPool | None = None
) -> _ScalarOrArrayT | lib.Array | Expression: ...


def choose(
    indices: ArrayLike | ScalarLike,
    /,
    *values: ArrayLike | ScalarLike,
    memory_pool: lib.MemoryPool | None = None,
) -> ArrayLike | ScalarLike: ...


def coalesce(
    *values: _ScalarOrArrayT | Expression, memory_pool: lib.MemoryPool | None = None
) -> _ScalarOrArrayT | Expression: ...


def fill_null(
    values: _ScalarOrArrayT | ScalarLike, fill_value: ArrayLike | ScalarLike
) -> _ScalarOrArrayT | ScalarLike: ...


def if_else(
    cond: ArrayLike | ScalarLike,
    left: ArrayLike | ScalarLike,
    right: ArrayLike | ScalarLike,
    /,
    *,
    memory_pool: lib.MemoryPool | None = None,
) -> ArrayLike | ScalarLike: ...


# ========================= 2.21 Structural transforms =========================

def list_value_length(
    lists: _ListArray[Any] | _LargeListArray[Any] | ListArray[Any] | Expression,
    /,
    *,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.Int32Array | lib.Int64Array | Expression: ...


def make_struct(
    *args: lib.Scalar | lib.Array | lib.ChunkedArray | Expression | ArrayLike,
    field_names: list[str] | tuple[str, ...] = (),
    field_nullability: bool | None = None,
    field_metadata: list[lib.KeyValueMetadata] | None = None,
    options: MakeStructOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.StructScalar | lib.StructArray | Expression: ...


# ========================= 2.22 Conversions =========================
def ceil_temporal(
    timestamps: _TemporalScalarT | _TemporalArrayT | Expression,
    /,
    multiple: int = 1,
    unit: Literal[
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
    ] = "day",
    *,
    week_starts_monday: bool = True,
    ceil_is_strictly_greater: bool = False,
    calendar_based_origin: bool = False,
    options: RoundTemporalOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> _TemporalScalarT | _TemporalArrayT | Expression: ...


floor_temporal = _clone_signature(ceil_temporal)
round_temporal = _clone_signature(ceil_temporal)


def cast(
    arr: lib.Scalar | lib.Array | lib.ChunkedArray | lib.Table,
    target_type: _DataTypeT | str,
    safe: bool | None = None,
    options: CastOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> (
    lib.Scalar[_DataTypeT] | lib.Scalar[Any] | lib.Array[lib.Scalar[_DataTypeT]] | lib.Array[lib.Scalar[Any]]
    | lib.ChunkedArray[lib.Scalar[_DataTypeT]] | lib.ChunkedArray[lib.Scalar[Any]] | lib.Table): ...


def strftime(
    timestamps: TemporalScalar | TemporalArray | Expression,
    /,
    format: str = "%Y-%m-%dT%H:%M:%S",
    locale: str = "C",
    *,
    options: StrftimeOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.StringScalar | lib.StringArray | Expression: ...


def strptime(
    strings: StringScalar | StringArray | Expression,
    /,
    format: str,
    unit: Literal["s", "ms", "us", "ns"],
    error_is_null: bool = False,
    *,
    options: StrptimeOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.TimestampScalar | lib.TimestampArray | Expression: ...


# ========================= 2.23 Temporal component extraction =========================
def day(
    values: TemporalScalar | TemporalArray | Expression, /, *,
    memory_pool: lib.MemoryPool | None = None) -> (
        lib.Int64Scalar | lib.Int64Array | Expression
): ...


def day_of_week(
    values: TemporalScalar | TemporalArray | Expression,
    /,
    *,
    count_from_zero: bool = True,
    week_start: int = 1,
    options: DayOfWeekOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.Int64Scalar | lib.Int64Array | Expression: ...


day_of_year = _clone_signature(day)


def hour(
    values: lib.TimestampScalar[Any] | lib.Time32Scalar[Any] | lib.Time64Scalar[Any]
    | lib.TimestampArray[Any] | lib.Time32Array[Any] | lib.Time64Array[Any]
    | lib.ChunkedArray[lib.TimestampScalar[Any]]
    | lib.ChunkedArray[lib.Time32Scalar[Any]]
    | lib.ChunkedArray[lib.Time64Scalar[Any]] | Expression,
    /,
    *,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.Int64Scalar | lib.Int64Array | Expression: ...


def is_dst(
    values: lib.TimestampScalar | lib.TimestampArray[Any]
    | lib.ChunkedArray[lib.TimestampScalar] | Expression,
    /, *, memory_pool: lib.MemoryPool | None = None
) -> lib.BooleanScalar | lib.BooleanArray | Expression: ...


def iso_week(
    values: lib.TimestampScalar | lib.TimestampArray[Any]
    | lib.ChunkedArray[lib.TimestampScalar[Any]] | Expression,
    /, *, memory_pool: lib.MemoryPool | None = None
) -> lib.Int64Scalar | lib.Int64Array | Expression: ...


iso_year = _clone_signature(iso_week)


def is_leap_year(
    values: lib.TimestampScalar[Any] | lib.Date32Scalar | lib.Date64Scalar
    | lib.TimestampArray
    | lib.Date32Array
    | lib.Date64Array
    | lib.ChunkedArray[lib.TimestampScalar]
    | lib.ChunkedArray[lib.Date32Scalar]
    | lib.ChunkedArray[lib.Date64Scalar] | Expression,
    /,
    *,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.BooleanScalar | lib.BooleanArray | Expression: ...


microsecond = _clone_signature(iso_week)
millisecond = _clone_signature(iso_week)
minute = _clone_signature(iso_week)
month = _clone_signature(day_of_week)
nanosecond = _clone_signature(hour)
quarter = _clone_signature(day_of_week)
second = _clone_signature(hour)
subsecond = _clone_signature(hour)
us_week = _clone_signature(iso_week)
us_year = _clone_signature(iso_week)
year = _clone_signature(iso_week)


def week(
    values: lib.TimestampScalar | lib.TimestampArray
    | lib.ChunkedArray[lib.TimestampScalar] | Expression,
    /,
    *,
    week_starts_monday: bool = True,
    count_from_zero: bool = False,
    first_week_is_fully_in_year: bool = False,
    options: WeekOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.Int64Scalar | lib.Int64Array | Expression: ...


def year_month_day(
    values: TemporalScalar | TemporalArray | Expression, /, *,
    memory_pool: lib.MemoryPool | None = None
) -> lib.StructScalar | lib.StructArray | Expression: ...


# ========================= 2.24 Temporal difference =========================
def day_time_interval_between(start, end, /, *,
                              memory_pool: lib.MemoryPool | None = None): ...


def days_between(
    start, end, /, *, memory_pool: lib.MemoryPool | None = None
) -> lib.Int64Scalar | lib.Int64Array: ...


hours_between = _clone_signature(days_between)
microseconds_between = _clone_signature(days_between)
milliseconds_between = _clone_signature(days_between)
minutes_between = _clone_signature(days_between)


def month_day_nano_interval_between(
    start, end, /, *, memory_pool: lib.MemoryPool | None = None
) -> lib.MonthDayNanoIntervalScalar | lib.MonthDayNanoIntervalArray: ...


def month_interval_between(start, end, /, *,
                           memory_pool: lib.MemoryPool | None = None): ...


nanoseconds_between = _clone_signature(days_between)
quarters_between = _clone_signature(days_between)
seconds_between = _clone_signature(days_between)


def weeks_between(
    start,
    end,
    /,
    *,
    count_from_zero: bool = True,
    week_start: int = 1,
    options: DayOfWeekOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.Int64Scalar | lib.Int64Array: ...


years_between = _clone_signature(days_between)

# ========================= 2.25 Timezone handling =========================


def assume_timezone(
    timestamps: lib.TimestampScalar | lib.Scalar[lib.TimestampType] | lib.TimestampArray
    | lib.ChunkedArray[lib.TimestampScalar] | Expression,
    /,
    timezone: str | None = None,
    *,
    ambiguous: Literal["raise", "earliest", "latest"] = "raise",
    nonexistent: Literal["raise", "earliest", "latest"] = "raise",
    options: AssumeTimezoneOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> (
    lib.TimestampScalar | lib.TimestampArray | lib.ChunkedArray[lib.TimestampScalar]
    | Expression
): ...


def local_timestamp(
    timestamps: lib.TimestampScalar | lib.TimestampArray
    | lib.ChunkedArray[lib.TimestampScalar] | Expression,
    /, *, memory_pool: lib.MemoryPool | None = None
) -> lib.TimestampScalar | lib.TimestampArray | Expression: ...


# ========================= 2.26 Random number generation =========================
def random(
    n: int,
    *,
    initializer: Literal["system"] | int = "system",
    options: RandomOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.DoubleArray: ...


# ========================= 3. Array-wise (“vector”) functions =========================

# ========================= 3.1 Cumulative Functions =========================
def cumulative_sum(
    values: _NumericArrayT | ArrayLike | Expression,
    /,
    start: lib.Scalar | None = None,
    *,
    skip_nulls: bool = False,
    options: CumulativeSumOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> _NumericArrayT | Expression | lib.Array: ...


cumulative_sum_checked = _clone_signature(cumulative_sum)
cumulative_prod = _clone_signature(cumulative_sum)
cumulative_prod_checked = _clone_signature(cumulative_sum)
cumulative_max = _clone_signature(cumulative_sum)
cumulative_min = _clone_signature(cumulative_sum)
cumulative_mean = _clone_signature(cumulative_sum)
# ========================= 3.2 Associative transforms =========================


def dictionary_encode(
    array: _ScalarOrArrayT | Expression,
    /,
    null_encoding: Literal["mask", "encode"] = "mask",
    *,
    options=None,
    memory_pool: lib.MemoryPool | None = None,
) -> _ScalarOrArrayT | Expression: ...


def dictionary_decode(
    array: _ScalarOrArrayT | Expression,
    /,
    *,
    options=None,
    memory_pool: lib.MemoryPool | None = None,
) -> _ScalarOrArrayT | Expression: ...


def unique(array: _ArrayT | Expression, /, *, memory_pool: lib.MemoryPool |
           None = None) -> _ArrayT | Expression: ...


def value_counts(
    array: lib.Array | lib.ChunkedArray | Expression, /, *,
    memory_pool: lib.MemoryPool | None = None
) -> lib.StructArray | Expression: ...

# ========================= 3.3 Selections =========================


@overload
def array_filter(
    array: _ArrayT,
    selection_filter: list[bool] | list[bool | None] | BooleanArray,
    /,
    null_selection_behavior: Literal["drop", "emit_null"] = "drop",
    *,
    options: FilterOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> _ArrayT: ...


@overload
def array_filter(
    array: Expression,
    selection_filter: list[bool] | list[bool | None] | BooleanArray,
    /,
    null_selection_behavior: Literal["drop", "emit_null"] = "drop",
    *,
    options: FilterOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> Expression: ...


@overload
def array_take(
    array: _ArrayT,
    indices: list[int]
    | list[int | None]
    | lib.Int16Array
    | lib.Int32Array
    | lib.Int64Array
    | lib.UInt64Array
    | lib.ChunkedArray[lib.Int16Scalar]
    | lib.ChunkedArray[lib.Int32Scalar]
    | lib.ChunkedArray[lib.Int64Scalar]
    | np.ndarray
    | Expression,
    /,
    *,
    boundscheck: bool = True,
    options: TakeOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> _ArrayT: ...


@overload
def array_take(
    array: Expression,
    indices: list[int]
    | list[int | None]
    | lib.Int16Array
    | lib.Int32Array
    | lib.Int64Array
    | lib.UInt64Array
    | lib.ChunkedArray[lib.Int16Scalar]
    | lib.ChunkedArray[lib.Int32Scalar]
    | lib.ChunkedArray[lib.Int64Scalar]
    | np.ndarray
    | Expression,
    /,
    *,
    boundscheck: bool = True,
    options: TakeOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> Expression: ...


@overload
def drop_null(input: _ArrayT, /, *, memory_pool: lib.MemoryPool |
              None = None) -> _ArrayT: ...


@overload
def drop_null(
    input: Expression, /, *, memory_pool: lib.MemoryPool | None = None
) -> Expression: ...


filter = array_filter
take = array_take

# ========================= 3.4 Containment tests  =========================


def indices_nonzero(
    values: lib.BooleanArray
    | lib.NullArray
    | NumericArray
    | lib.Decimal128Array
    | lib.Decimal256Array | Expression,
    /,
    *,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.UInt64Array | Expression: ...


# ========================= 3.5 Sorts and partitions  =========================
def array_sort_indices(
    array: lib.Array | lib.ChunkedArray | Expression,
    /,
    order: _Order = "ascending",
    *,
    null_placement: _Placement = "at_end",
    options: ArraySortOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.UInt64Array | Expression: ...


def partition_nth_indices(
    array: lib.Array | lib.ChunkedArray | Expression | Iterable,
    /,
    pivot: int,
    *,
    null_placement: _Placement = "at_end",
    options: PartitionNthOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.UInt64Array | Expression: ...


def pivot_wider(
    keys: lib.Array | lib.ChunkedArray | Sequence[str],
    values: lib.Array | lib.ChunkedArray | Sequence[Any],
    /,
    key_names: Sequence[str] | None = None,
    *,
    unexpected_key_behavior: Literal["ignore", "raise"] = "ignore",
    options: PivotWiderOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.StructScalar: ...


def rank(
    input: lib.Array | lib.ChunkedArray,
    /,
    sort_keys: _Order = "ascending",
    *,
    null_placement: _Placement = "at_end",
    tiebreaker: Literal["min", "max", "first", "dense"] = "first",
    options: RankOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.UInt64Array: ...


def rank_quantile(
    input: lib.Array | lib.ChunkedArray,
    /,
    sort_keys: _Order = "ascending",
    *,
    null_placement: _Placement = "at_end",
    options: RankQuantileOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.DoubleArray: ...


def rank_normal(
    input: lib.Array | lib.ChunkedArray,
    /,
    sort_keys: _Order = "ascending",
    *,
    null_placement: _Placement = "at_end",
    options: RankQuantileOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.DoubleArray: ...

def select_k_unstable(
    input: lib.Array | lib.ChunkedArray | lib.RecordBatch | lib.Table | Expression,
    /,
    k: int | None = None,
    sort_keys: Sequence[tuple[str | Expression, str]] | None = None,
    *,
    options: SelectKOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.UInt64Array | Expression: ...


def sort_indices(
    input: lib.Array | lib.ChunkedArray | lib.RecordBatch | lib.Table | Expression,
    /,
    sort_keys: Sequence[tuple[str | Expression, _Order]] | None = None,
    *,
    null_placement: _Placement = "at_end",
    options: SortOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.UInt64Array | Expression: ...


# ========================= 3.6 Structural transforms =========================
def list_element(
    lists: lib.Array[ListScalar[_DataTypeT]] | lib.ChunkedArray[ListScalar[_DataTypeT]]
    | ListScalar[_DataTypeT] | Expression,
    index: ScalarLike, /, *, memory_pool: lib.MemoryPool | None = None
) -> (lib.Array[lib.Scalar[_DataTypeT]] | lib.ChunkedArray[lib.Scalar[_DataTypeT]]
      | _DataTypeT | Expression): ...


def list_flatten(
    lists: ArrayOrChunkedArray[ListScalar[Any]] | Expression,
    /,
    recursive: bool = False,
    *,
    options: ListFlattenOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.ListArray[Any] | Expression: ...


def list_parent_indices(
    lists: ArrayOrChunkedArray[Any] | Expression, /, *,
    memory_pool: lib.MemoryPool | None = None
) -> lib.Int64Array | Expression: ...


def list_slice(
    lists: ArrayOrChunkedArray[Any] | Expression,
    /,
    start: int,
    stop: int | None = None,
    step: int = 1,
    return_fixed_size_list: bool | None = None,
    *,
    options: ListSliceOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> lib.ListArray[Any] | Expression: ...


def map_lookup(
    container,
    /,
    query_key,
    occurrence: str,
    *,
    options: MapLookupOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
): ...


def struct_field(
    values,
    /,
    indices,
    *,
    options: StructFieldOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
): ...


def fill_null_backward(
    values: _ScalarOrArrayT | ScalarLike | Expression,
    /,
    *,
    memory_pool: lib.MemoryPool | None = None,
) -> _ScalarOrArrayT | ScalarLike | Expression: ...


def fill_null_forward(
    values: _ScalarOrArrayT | ScalarLike | Expression,
    /,
    *,
    memory_pool: lib.MemoryPool | None = None,
) -> _ScalarOrArrayT | ScalarLike | Expression: ...


def replace_with_mask(
    values: _ScalarOrArrayT | Expression,
    mask: list[bool] | list[bool | None] | BooleanArray,
    replacements,
    /,
    *,
    memory_pool: lib.MemoryPool | None = None,
) -> _ScalarOrArrayT | Expression: ...


# ========================= 3.7 Pairwise functions =========================
def pairwise_diff(
    input: _NumericOrTemporalArrayT | Expression,
    /,
    period: int = 1,
    *,
    options: PairwiseOptions | None = None,
    memory_pool: lib.MemoryPool | None = None,
) -> _NumericOrTemporalArrayT | Expression: ...


def run_end_encode(
    input: _NumericOrTemporalArrayT | Expression,
    /,
    *,
    run_end_type: _RunEndType | None = None,
    options: RunEndEncodeOptions | None = None,
    memory_pool: lib.MemoryPool | None = None
) -> _NumericOrTemporalArrayT | Expression: ...

def run_end_decode(
    input: _NumericOrTemporalArrayT | Expression,
    /,
    *,
    memory_pool: lib.MemoryPool | None = None
) -> _NumericOrTemporalArrayT | Expression: ...

pairwise_diff_checked = _clone_signature(pairwise_diff)
