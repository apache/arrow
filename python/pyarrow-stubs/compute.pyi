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

from collections.abc import Callable, Hashable, Sequence
from decimal import Decimal
from typing import Any, ParamSpec, TypeAlias, TypeVar

import numpy as np
from numpy.typing import NDArray

# Placeholders (Any) until scalar.pyi is annotated; mypy rejects them as
# types when imported through pyarrow.lib.
from pyarrow.__lib.scalar import (
    BinaryScalar,
    BinaryViewScalar,
    BooleanScalar,
    Date32Scalar,
    Date64Scalar,
    Decimal32Scalar,
    Decimal64Scalar,
    Decimal128Scalar,
    Decimal256Scalar,
    DoubleScalar,
    DurationScalar,
    FixedSizeBinaryScalar,
    FixedSizeListScalar,
    FloatScalar,
    HalfFloatScalar,
    Int8Scalar,
    Int16Scalar,
    Int32Scalar,
    Int64Scalar,
    LargeBinaryScalar,
    LargeListScalar,
    LargeListViewScalar,
    LargeStringScalar,
    ListScalar,
    ListViewScalar,
    MonthDayNanoIntervalScalar,
    Scalar,
    StringScalar,
    StringViewScalar,
    StructScalar,
    Time32Scalar,
    Time64Scalar,
    TimestampScalar,
    UInt8Scalar,
    UInt16Scalar,
    UInt32Scalar,
    UInt64Scalar,
)
from pyarrow._compute import (
    ArraySortOptions as ArraySortOptions,
    AssumeTimezoneOptions as AssumeTimezoneOptions,
    CastOptions as CastOptions,
    CountOptions as CountOptions,
    CumulativeOptions as CumulativeOptions,
    DayOfWeekOptions as DayOfWeekOptions,
    DictionaryEncodeOptions as DictionaryEncodeOptions,
    ElementWiseAggregateOptions as ElementWiseAggregateOptions,
    Expression as Expression,
    ExtractRegexOptions as ExtractRegexOptions,
    ExtractRegexSpanOptions as ExtractRegexSpanOptions,
    FilterOptions as FilterOptions,
    Function as Function,
    FunctionOptions as FunctionOptions,
    FunctionRegistry as FunctionRegistry,
    HashAggregateFunction as HashAggregateFunction,
    HashAggregateKernel as HashAggregateKernel,
    IndexOptions as IndexOptions,
    InversePermutationOptions as InversePermutationOptions,
    JoinOptions as JoinOptions,
    Kernel as Kernel,
    ListFlattenOptions as ListFlattenOptions,
    ListSliceOptions as ListSliceOptions,
    MakeStructOptions as MakeStructOptions,
    MapLookupOptions as MapLookupOptions,
    MatchSubstringOptions as MatchSubstringOptions,
    ModeOptions as ModeOptions,
    NullOptions as NullOptions,
    PadOptions as PadOptions,
    PairwiseOptions as PairwiseOptions,
    PartitionNthOptions as PartitionNthOptions,
    PivotWiderOptions as PivotWiderOptions,
    QuantileOptions as QuantileOptions,
    RandomOptions as RandomOptions,
    RankOptions as RankOptions,
    RankQuantileOptions as RankQuantileOptions,
    ReplaceSliceOptions as ReplaceSliceOptions,
    ReplaceSubstringOptions as ReplaceSubstringOptions,
    RoundBinaryOptions as RoundBinaryOptions,
    RoundOptions as RoundOptions,
    RoundTemporalOptions as RoundTemporalOptions,
    RoundToMultipleOptions as RoundToMultipleOptions,
    RunEndEncodeOptions as RunEndEncodeOptions,
    ScalarAggregateFunction as ScalarAggregateFunction,
    ScalarAggregateKernel as ScalarAggregateKernel,
    ScalarAggregateOptions as ScalarAggregateOptions,
    ScalarFunction as ScalarFunction,
    ScalarKernel as ScalarKernel,
    ScatterOptions as ScatterOptions,
    SearchSortedOptions as SearchSortedOptions,
    SelectKOptions as SelectKOptions,
    SetLookupOptions as SetLookupOptions,
    SkewOptions as SkewOptions,
    SliceOptions as SliceOptions,
    SortOptions as SortOptions,
    SplitOptions as SplitOptions,
    SplitPatternOptions as SplitPatternOptions,
    StrftimeOptions as StrftimeOptions,
    StrptimeOptions as StrptimeOptions,
    StructFieldOptions as StructFieldOptions,
    TakeOptions as TakeOptions,
    TDigestOptions as TDigestOptions,
    TrimOptions as TrimOptions,
    UdfContext as UdfContext,
    Utf8NormalizeOptions as Utf8NormalizeOptions,
    VarianceOptions as VarianceOptions,
    VectorFunction as VectorFunction,
    VectorKernel as VectorKernel,
    WeekOptions as WeekOptions,
    WinsorizeOptions as WinsorizeOptions,
    ZeroFillOptions as ZeroFillOptions,
    call_function as call_function,
    call_tabular_function as call_tabular_function,
    function_registry as function_registry,
    get_function as get_function,
    list_functions as list_functions,
    register_aggregate_function as register_aggregate_function,
    register_scalar_function as register_scalar_function,
    register_tabular_function as register_tabular_function,
    register_vector_function as register_vector_function,
)
from pyarrow._compute import (
    _CountMode,
    _DateTimeUnit,
    _Form,
    _Interpolation,
    _NullHandling,
    _Occurrence,
    _RoundMode,
    _Side,
    _SortKey,
    _Tiebreaker,
    _TimezoneResolution,
    _UnexpectedKeyBehavior,
)
from pyarrow._stubs_typing import (
    Indices,
    Mask,
    NullEncoding,
    NullPlacement,
    NullSelectionBehavior,
    Order,
    TimeUnit,
)
from pyarrow.lib import (
    Array,
    BaseListArray,
    BinaryArray,
    BinaryViewArray,
    BooleanArray,
    ChunkedArray,
    DataType,
    Date32Array,
    Date64Array,
    Decimal32Array,
    Decimal64Array,
    Decimal128Array,
    Decimal256Array,
    DictionaryArray,
    DoubleArray,
    DurationArray,
    FixedSizeBinaryArray,
    FloatingPointArray,
    Int32Array,
    Int64Array,
    IntegerArray,
    KeyValueMetadata,
    LargeBinaryArray,
    LargeStringArray,
    MemoryPool,
    MonthDayNanoIntervalArray,
    NumericArray,
    RecordBatch,
    RunEndEncodedArray,
    StringArray,
    StringViewArray,
    StructArray,
    Table,
    Time32Array,
    Time64Array,
    TimestampArray,
    UInt64Array,
)

_P = ParamSpec("_P")
_R = TypeVar("_R")

# Stub-only helper: the compute functions that share a signature are
# annotated once and the others are declared as copies of it.
def _clone_signature(f: Callable[_P, _R]) -> Callable[_P, _R]: ...
def field(*name_or_index: str | bytes | tuple[str | int, ...] | int) -> Expression: ...
def scalar(value: Any) -> Expression: ...

# The inputs and outputs of the compute functions, by kind of data. The
# scalar classes are placeholders (Any) until scalar.pyi is annotated.
_SignedIntegerScalar: TypeAlias = Int8Scalar | Int16Scalar | Int32Scalar | Int64Scalar
_UnsignedIntegerScalar: TypeAlias = (
    UInt8Scalar | UInt16Scalar | UInt32Scalar | UInt64Scalar
)
_IntegerScalar: TypeAlias = _SignedIntegerScalar | _UnsignedIntegerScalar
_FloatScalar: TypeAlias = HalfFloatScalar | FloatScalar | DoubleScalar
_DecimalScalar: TypeAlias = (
    Decimal32Scalar | Decimal64Scalar | Decimal128Scalar | Decimal256Scalar
)
_NumericScalar: TypeAlias = _IntegerScalar | _FloatScalar | _DecimalScalar
_BinaryScalar: TypeAlias = (
    BinaryScalar | LargeBinaryScalar | BinaryViewScalar | FixedSizeBinaryScalar
)
_StringScalar: TypeAlias = StringScalar | LargeStringScalar | StringViewScalar
_StringOrBinaryScalar: TypeAlias = _StringScalar | _BinaryScalar
_ListScalar: TypeAlias = (
    ListScalar
    | LargeListScalar
    | ListViewScalar
    | LargeListViewScalar
    | FixedSizeListScalar
)
_TemporalScalar: TypeAlias = (
    Date32Scalar
    | Date64Scalar
    | Time32Scalar
    | Time64Scalar
    | TimestampScalar
    | DurationScalar
    | MonthDayNanoIntervalScalar
)
_NumericOrDurationScalar: TypeAlias = _NumericScalar | DurationScalar
_NumericOrTemporalScalar: TypeAlias = _NumericScalar | _TemporalScalar

# pyarrow.array() and most of the API return plain Array and ChunkedArray
# instances, as these classes aren't generic. So the compute functions accept
# any Array or ChunkedArray, and only the return types say which kind of array
# the result is, e.g. a StringArray for a string input.
_ArrayOrChunkedArray: TypeAlias = Array | ChunkedArray
_ScalarOrArray: TypeAlias = Scalar | Array | ChunkedArray
# Python values that compute functions convert to arrays. Other values are
# converted to scalars.
_ArrayLike: TypeAlias = list[Any] | tuple[Any, ...] | NDArray[Any]
# element-wise compute input (expressions only work with element-wise functions)
_ComputeInput: TypeAlias = _ScalarOrArray | _ArrayLike | Expression
# Python literals that compute functions convert to scalars. An int is
# accepted where a float is. np.generic covers NumPy scalars.
_NumericLiteral: TypeAlias = float | Decimal | np.generic
_TemporalLiteral: TypeAlias = dt.date | dt.time | dt.timedelta | np.generic
_Literal: TypeAlias = _NumericLiteral | _TemporalLiteral | bool | str | bytes
_DecimalArray: TypeAlias = (
    Decimal32Array | Decimal64Array | Decimal128Array | Decimal256Array
)
# NumericArray includes the date, time, timestamp and duration arrays.
_NumericArray: TypeAlias = NumericArray | _DecimalArray
_NumericOrDurationArray: TypeAlias = _NumericArray
_NumericOrTemporalArray: TypeAlias = _NumericArray | MonthDayNanoIntervalArray
_StringArray: TypeAlias = StringArray | LargeStringArray | StringViewArray
_BinaryArray: TypeAlias = (
    BinaryArray | LargeBinaryArray | BinaryViewArray | FixedSizeBinaryArray
)
_StringOrBinaryArray: TypeAlias = _StringArray | _BinaryArray
_TemporalArray: TypeAlias = (
    Date32Array
    | Date64Array
    | Time32Array
    | Time64Array
    | TimestampArray
    | DurationArray
    | MonthDayNanoIntervalArray
)

# =============================== Aggregations ========================================
def all(
    array: _ScalarOrArray,
    /,
    *,
    skip_nulls: bool = True,
    min_count: int = 1,
    options: ScalarAggregateOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> BooleanScalar: ...

any = _clone_signature(all)

def approximate_median(
    array: _ScalarOrArray,
    /,
    *,
    skip_nulls: bool = True,
    min_count: int = 1,
    options: ScalarAggregateOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> DoubleScalar: ...
def count(
    array: _ScalarOrArray,
    /,
    mode: _CountMode = "only_valid",
    *,
    options: CountOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> Int64Scalar: ...
def count_distinct(
    array: _ScalarOrArray,
    /,
    mode: _CountMode = "only_valid",
    *,
    options: CountOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> Int64Scalar: ...
def first(
    array: _ScalarOrArray,
    /,
    *,
    skip_nulls: bool = True,
    min_count: int = 1,
    options: ScalarAggregateOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> Scalar: ...
def first_last(
    array: _ScalarOrArray | _ArrayLike,
    /,
    *,
    skip_nulls: bool = True,
    min_count: int = 1,
    options: ScalarAggregateOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> StructScalar: ...
def index(
    data: _ArrayOrChunkedArray,
    value: Scalar | _Literal,
    start: int | None = None,
    end: int | None = None,
    *,
    memory_pool: MemoryPool | None = None,
) -> Int64Scalar: ...
def kurtosis(
    array: _ScalarOrArray | _ArrayLike,
    /,
    *,
    skip_nulls: bool = True,
    biased: bool = True,
    min_count: int = 0,
    options: SkewOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> DoubleScalar: ...

last = _clone_signature(first)
max = _clone_signature(first)

def mean(
    array: _ScalarOrArray,
    /,
    *,
    skip_nulls: bool = True,
    min_count: int = 1,
    options: ScalarAggregateOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> DoubleScalar | _DecimalScalar: ...

min = _clone_signature(first)
min_max = _clone_signature(first_last)

def mode(
    array: _ScalarOrArray,
    /,
    n: int = 1,
    *,
    skip_nulls: bool = True,
    min_count: int = 0,
    options: ModeOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> StructArray: ...
def pivot_wider(
    pivot_keys: _ArrayOrChunkedArray | Sequence[str],
    pivot_values: _ArrayOrChunkedArray | Sequence[Any],
    /,
    key_names: Sequence[str],
    *,
    unexpected_key_behavior: _UnexpectedKeyBehavior = "ignore",
    options: PivotWiderOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> StructScalar: ...
def product(
    array: _ScalarOrArray,
    /,
    *,
    skip_nulls: bool = True,
    min_count: int = 1,
    options: ScalarAggregateOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _NumericScalar: ...
def quantile(
    array: _ScalarOrArray,
    /,
    q: float | Sequence[float] = 0.5,
    *,
    interpolation: _Interpolation = "linear",
    skip_nulls: bool = True,
    min_count: int = 0,
    options: QuantileOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> NumericArray | _DecimalArray: ...
def skew(
    array: _ScalarOrArray | _ArrayLike,
    /,
    *,
    skip_nulls: bool = True,
    biased: bool = True,
    min_count: int = 0,
    options: SkewOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> DoubleScalar: ...
def stddev(
    array: _ScalarOrArray,
    /,
    *,
    ddof: float = 0,
    skip_nulls: bool = True,
    min_count: int = 0,
    options: VarianceOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> DoubleScalar: ...
def sum(
    array: _ScalarOrArray,
    /,
    *,
    skip_nulls: bool = True,
    min_count: int = 1,
    options: ScalarAggregateOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _NumericScalar: ...
def tdigest(
    array: _ScalarOrArray,
    /,
    q: float | Sequence[float] = 0.5,
    *,
    delta: int = 100,
    buffer_size: int = 500,
    skip_nulls: bool = True,
    min_count: int = 0,
    options: TDigestOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> DoubleArray: ...
def variance(
    array: _ScalarOrArray | _ArrayLike,
    /,
    *,
    ddof: int = 0,
    skip_nulls: bool = True,
    min_count: int = 0,
    options: VarianceOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> DoubleScalar: ...

# =============================== Element-wise ("scalar") functions ===================

# ========================= Arithmetic functions =========================
def abs(
    x: _ComputeInput | _NumericLiteral | _TemporalLiteral,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _NumericOrDurationScalar | _NumericOrDurationArray | ChunkedArray | Expression: ...

abs_checked = _clone_signature(abs)

def add(
    x: _ComputeInput | _NumericLiteral | _TemporalLiteral,
    y: _ComputeInput | _NumericLiteral | _TemporalLiteral,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _NumericOrTemporalScalar | _NumericOrTemporalArray | ChunkedArray | Expression: ...

add_checked = _clone_signature(add)

def divide(
    x: _ComputeInput | _NumericLiteral | _TemporalLiteral,
    y: _ComputeInput | _NumericLiteral | _TemporalLiteral,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _NumericOrTemporalScalar | _NumericOrTemporalArray | ChunkedArray | Expression: ...

divide_checked = _clone_signature(divide)

def exp(
    exponent: _ComputeInput | _NumericLiteral,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> (
    FloatingPointArray
    | ChunkedArray
    | DoubleArray
    | _FloatScalar
    | DoubleScalar
    | Expression
): ...

expm1 = _clone_signature(exp)

def hypot(
    x: _ComputeInput | _NumericLiteral,
    y: _ComputeInput | _NumericLiteral,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _FloatScalar | FloatingPointArray | ChunkedArray | Expression: ...
def modulo(
    dividend: _ComputeInput | _NumericLiteral,
    divisor: _ComputeInput | _NumericLiteral,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _NumericScalar | _NumericArray | ChunkedArray | Expression: ...

modulo_checked = _clone_signature(modulo)
multiply = _clone_signature(add)
multiply_checked = _clone_signature(add)

def negate(
    x: _ComputeInput | _NumericLiteral | _TemporalLiteral,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _NumericOrDurationScalar | _NumericOrDurationArray | ChunkedArray | Expression: ...

negate_checked = _clone_signature(negate)

def power(
    base: _ComputeInput | _NumericLiteral,
    exponent: _ComputeInput | _NumericLiteral,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _NumericScalar | _NumericArray | ChunkedArray | Expression: ...

power_checked = _clone_signature(power)
remainder = _clone_signature(modulo)
remainder_checked = _clone_signature(modulo)

def sign(
    x: _ComputeInput | _NumericLiteral | _TemporalLiteral,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> Int8Scalar | _FloatScalar | NumericArray | ChunkedArray | Expression: ...
def sqrt(
    x: _ComputeInput | _NumericLiteral,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> FloatingPointArray | _FloatScalar | ChunkedArray | Expression: ...

sqrt_checked = _clone_signature(sqrt)
subtract = _clone_signature(add)
subtract_checked = _clone_signature(add)

# ========================= Bit-wise functions =========================
def bit_wise_and(
    x: _ComputeInput | int,
    y: _ComputeInput | int,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _NumericScalar | _NumericArray | ChunkedArray | Expression: ...
def bit_wise_not(
    x: _ComputeInput | int,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _NumericScalar | _NumericArray | ChunkedArray | Expression: ...

bit_wise_or = _clone_signature(bit_wise_and)
bit_wise_xor = _clone_signature(bit_wise_and)
shift_left = _clone_signature(bit_wise_and)
shift_left_checked = _clone_signature(bit_wise_and)
shift_right = _clone_signature(bit_wise_and)
shift_right_checked = _clone_signature(bit_wise_and)

# ========================= Rounding functions =========================
def ceil(
    x: _ComputeInput | _NumericLiteral,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> (
    _FloatScalar
    | _DecimalScalar
    | FloatingPointArray
    | _DecimalArray
    | ChunkedArray
    | Expression
): ...

floor = _clone_signature(ceil)

def round(
    x: _ComputeInput | _NumericLiteral,
    /,
    ndigits: int = 0,
    round_mode: _RoundMode = "half_to_even",
    *,
    options: RoundOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _NumericScalar | _NumericArray | ChunkedArray | Expression: ...
def round_to_multiple(
    x: _ComputeInput | _NumericLiteral,
    /,
    multiple: float | Scalar = 1.0,
    round_mode: _RoundMode = "half_to_even",
    *,
    options: RoundToMultipleOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _NumericScalar | _NumericArray | ChunkedArray | Expression: ...
def round_binary(
    x: _ComputeInput | _NumericLiteral,
    s: _ComputeInput | int,
    /,
    round_mode: _RoundMode = "half_to_even",
    *,
    options: RoundBinaryOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _NumericScalar | _NumericArray | ChunkedArray | Expression: ...

trunc = _clone_signature(ceil)

# ========================= Logarithmic functions =========================
def ln(
    x: _ComputeInput | _NumericLiteral,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _FloatScalar | FloatingPointArray | ChunkedArray | Expression: ...

ln_checked = _clone_signature(ln)
log10 = _clone_signature(ln)
log10_checked = _clone_signature(ln)
log1p = _clone_signature(ln)
log1p_checked = _clone_signature(ln)
log2 = _clone_signature(ln)
log2_checked = _clone_signature(ln)

def logb(
    x: _ComputeInput | _NumericLiteral,
    b: _ComputeInput | _NumericLiteral,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _FloatScalar | FloatingPointArray | ChunkedArray | Expression: ...

logb_checked = _clone_signature(logb)

# ========================= Trigonometric functions =========================
acos = _clone_signature(ln)
acos_checked = _clone_signature(ln)
asin = _clone_signature(ln)
asin_checked = _clone_signature(ln)
atan = _clone_signature(ln)

def atan2(
    y: _ComputeInput | _NumericLiteral,
    x: _ComputeInput | _NumericLiteral,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _FloatScalar | FloatingPointArray | ChunkedArray | Expression: ...

cos = _clone_signature(ln)
cos_checked = _clone_signature(ln)
sin = _clone_signature(ln)
sin_checked = _clone_signature(ln)
tan = _clone_signature(ln)
tan_checked = _clone_signature(ln)

# ========================= Hyperbolic trigonometric functions =========================
acosh = _clone_signature(ln)
acosh_checked = _clone_signature(ln)
asinh = _clone_signature(ln)
atanh = _clone_signature(ln)
atanh_checked = _clone_signature(ln)
cosh = _clone_signature(ln)
sinh = _clone_signature(ln)
tanh = _clone_signature(ln)

# ========================= Comparisons =========================
def equal(
    x: _ComputeInput | _Literal,
    y: _ComputeInput | _Literal,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> BooleanScalar | BooleanArray | ChunkedArray | Expression: ...

greater = _clone_signature(equal)
greater_equal = _clone_signature(equal)
less = _clone_signature(equal)
less_equal = _clone_signature(equal)
not_equal = _clone_signature(equal)

def max_element_wise(
    *args: _ComputeInput | _Literal,
    skip_nulls: bool = True,
    options: ElementWiseAggregateOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> (
    _NumericOrTemporalScalar
    | _StringOrBinaryScalar
    | _NumericOrTemporalArray
    | _StringOrBinaryArray
    | ChunkedArray
    | Expression
): ...

min_element_wise = _clone_signature(max_element_wise)

# ========================= Logical functions =========================
def and_(
    x: _ComputeInput | bool,
    y: _ComputeInput | bool,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> BooleanScalar | BooleanArray | ChunkedArray | Expression: ...

and_kleene = _clone_signature(and_)
and_not = _clone_signature(and_)
and_not_kleene = _clone_signature(and_)

def invert(
    x: _ComputeInput | bool,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> BooleanScalar | BooleanArray | ChunkedArray | Expression: ...

or_ = _clone_signature(and_)
or_kleene = _clone_signature(and_)
xor = _clone_signature(and_)

# ========================= String predicates =========================
def ascii_is_alnum(
    strings: _ComputeInput | str,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> BooleanScalar | BooleanArray | ChunkedArray | Expression: ...

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

# ========================= String transforms =========================
def ascii_capitalize(
    strings: _ComputeInput | str,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _StringScalar | _StringArray | ChunkedArray | Expression: ...

ascii_lower = _clone_signature(ascii_capitalize)
ascii_reverse = _clone_signature(ascii_capitalize)
ascii_swapcase = _clone_signature(ascii_capitalize)
ascii_title = _clone_signature(ascii_capitalize)
ascii_upper = _clone_signature(ascii_capitalize)

def binary_length(
    strings: _ComputeInput | str | bytes,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> (
    Int32Scalar | Int64Scalar | Int32Array | Int64Array | ChunkedArray | Expression
): ...
def binary_repeat(
    strings: _ComputeInput | str | bytes,
    num_repeats: _ComputeInput | int,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _StringOrBinaryScalar | _StringOrBinaryArray | ChunkedArray | Expression: ...
def binary_replace_slice(
    strings: _ComputeInput | str | bytes,
    /,
    start: int,
    stop: int,
    replacement: str | bytes,
    *,
    options: ReplaceSliceOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _StringOrBinaryScalar | _StringOrBinaryArray | ChunkedArray | Expression: ...
def binary_reverse(
    strings: _ComputeInput | bytes,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _BinaryScalar | _BinaryArray | ChunkedArray | Expression: ...
def replace_substring(
    strings: _ComputeInput | str | bytes,
    /,
    pattern: str | bytes,
    replacement: str | bytes,
    *,
    max_replacements: int | None = None,
    options: ReplaceSubstringOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _StringOrBinaryScalar | _StringOrBinaryArray | ChunkedArray | Expression: ...

replace_substring_regex = _clone_signature(replace_substring)

def utf8_capitalize(
    strings: _ComputeInput | str,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _StringScalar | _StringArray | ChunkedArray | Expression: ...
def utf8_length(
    strings: _ComputeInput | str,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> (
    Int32Scalar | Int64Scalar | Int32Array | Int64Array | ChunkedArray | Expression
): ...

utf8_lower = _clone_signature(utf8_capitalize)

def utf8_replace_slice(
    strings: _ComputeInput | str,
    /,
    start: int,
    stop: int,
    replacement: str | bytes,
    *,
    options: ReplaceSliceOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _StringScalar | _StringArray | ChunkedArray | Expression: ...

utf8_reverse = _clone_signature(utf8_capitalize)
utf8_swapcase = _clone_signature(utf8_capitalize)
utf8_title = _clone_signature(utf8_capitalize)
utf8_upper = _clone_signature(utf8_capitalize)

def utf8_normalize(
    strings: _ComputeInput | str,
    /,
    form: _Form,
    *,
    options: Utf8NormalizeOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _StringScalar | _StringArray | ChunkedArray | Expression: ...

# ========================= String padding =========================
def ascii_center(
    strings: _ComputeInput | str,
    /,
    width: int,
    padding: str = " ",
    lean_left_on_odd_padding: bool = True,
    *,
    options: PadOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _StringScalar | _StringArray | ChunkedArray | Expression: ...

ascii_lpad = _clone_signature(ascii_center)
ascii_rpad = _clone_signature(ascii_center)
utf8_center = _clone_signature(ascii_center)
utf8_lpad = _clone_signature(ascii_center)
utf8_rpad = _clone_signature(ascii_center)

def utf8_zero_fill(
    strings: _ComputeInput | str,
    /,
    width: int,
    padding: str = "0",
    *,
    options: ZeroFillOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _StringScalar | _StringArray | ChunkedArray | Expression: ...

utf8_zfill = utf8_zero_fill

# ========================= String trimming =========================
def ascii_ltrim(
    strings: _ComputeInput | str,
    /,
    characters: str | bytes,
    *,
    options: TrimOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _StringScalar | _StringArray | ChunkedArray | Expression: ...
def ascii_ltrim_whitespace(
    strings: _ComputeInput | str,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _StringScalar | _StringArray | ChunkedArray | Expression: ...

ascii_rtrim = _clone_signature(ascii_ltrim)
ascii_rtrim_whitespace = _clone_signature(ascii_ltrim_whitespace)
ascii_trim = _clone_signature(ascii_ltrim)
ascii_trim_whitespace = _clone_signature(ascii_ltrim_whitespace)
utf8_ltrim = _clone_signature(ascii_ltrim)
utf8_ltrim_whitespace = _clone_signature(ascii_ltrim_whitespace)
utf8_rtrim = _clone_signature(ascii_ltrim)
utf8_rtrim_whitespace = _clone_signature(ascii_ltrim_whitespace)
utf8_trim = _clone_signature(ascii_ltrim)
utf8_trim_whitespace = _clone_signature(ascii_ltrim_whitespace)

# ========================= String splitting =========================
def ascii_split_whitespace(
    strings: _ComputeInput | str | bytes,
    /,
    *,
    max_splits: int | None = None,
    reverse: bool = False,
    options: SplitOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _ListScalar | BaseListArray | ChunkedArray | Expression: ...
def split_pattern(
    strings: _ComputeInput | str,
    /,
    pattern: str | bytes,
    *,
    max_splits: int | None = None,
    reverse: bool = False,
    options: SplitPatternOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _ListScalar | BaseListArray | ChunkedArray | Expression: ...

split_pattern_regex = _clone_signature(split_pattern)
utf8_split_whitespace = _clone_signature(ascii_split_whitespace)

# ========================= String component extraction =========================
def extract_regex(
    strings: _ComputeInput | str | bytes,
    /,
    pattern: str | bytes,
    *,
    options: ExtractRegexOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> StructScalar | StructArray | ChunkedArray | Expression: ...
def extract_regex_span(
    strings: _ComputeInput | str | bytes,
    /,
    pattern: str | bytes,
    *,
    options: ExtractRegexSpanOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> StructScalar | StructArray | ChunkedArray | Expression: ...

# ========================= String joining =========================
def binary_join(
    strings: _ComputeInput,
    separator: _ComputeInput | str | bytes,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _StringOrBinaryScalar | _StringOrBinaryArray | ChunkedArray | Expression: ...
def binary_join_element_wise(
    *strings: _ComputeInput | str | bytes,
    null_handling: _NullHandling = "emit_null",
    null_replacement: str = "",
    options: JoinOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _StringOrBinaryScalar | _StringOrBinaryArray | ChunkedArray | Expression: ...

# ========================= String Slicing =========================
def binary_slice(
    strings: _ComputeInput | bytes,
    /,
    start: int,
    stop: int | None = None,
    step: int = 1,
    *,
    options: SliceOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _BinaryScalar | _BinaryArray | ChunkedArray | Expression: ...
def utf8_slice_codeunits(
    strings: _ComputeInput | str,
    /,
    start: int,
    stop: int | None = None,
    step: int = 1,
    *,
    options: SliceOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _StringScalar | _StringArray | ChunkedArray | Expression: ...

# ========================= Containment tests =========================
def count_substring(
    strings: _ComputeInput | str | bytes,
    /,
    pattern: str | bytes,
    *,
    ignore_case: bool = False,
    options: MatchSubstringOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> (
    Int32Scalar | Int64Scalar | Int32Array | Int64Array | ChunkedArray | Expression
): ...

count_substring_regex = _clone_signature(count_substring)

def ends_with(
    strings: _ComputeInput | str | bytes,
    /,
    pattern: str | bytes,
    *,
    ignore_case: bool = False,
    options: MatchSubstringOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> BooleanScalar | BooleanArray | ChunkedArray | Expression: ...

find_substring = _clone_signature(count_substring)
find_substring_regex = _clone_signature(count_substring)

def index_in(
    values: _ComputeInput,
    /,
    value_set: _ArrayOrChunkedArray,
    *,
    skip_nulls: bool = False,
    options: SetLookupOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> Int32Scalar | Int32Array | ChunkedArray | Expression: ...
def is_in(
    values: _ComputeInput,
    /,
    value_set: _ArrayOrChunkedArray,
    *,
    skip_nulls: bool = False,
    options: SetLookupOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> BooleanScalar | BooleanArray | ChunkedArray | Expression: ...

match_like = _clone_signature(ends_with)
match_substring = _clone_signature(ends_with)
match_substring_regex = _clone_signature(ends_with)
starts_with = _clone_signature(ends_with)

def index_in_meta_binary(
    values: _ArrayOrChunkedArray | _ArrayLike,
    value_set: _ArrayOrChunkedArray | _ArrayLike,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> Int32Scalar | Int32Array | ChunkedArray: ...
def is_in_meta_binary(
    values: _ArrayOrChunkedArray | _ArrayLike,
    value_set: _ArrayOrChunkedArray | _ArrayLike,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> BooleanScalar | BooleanArray | ChunkedArray: ...

# ========================= Categorizations =========================
def is_finite(
    values: _ComputeInput,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> BooleanScalar | BooleanArray | ChunkedArray | Expression: ...

is_inf = _clone_signature(is_finite)
is_nan = _clone_signature(is_finite)

def is_null(
    values: _ComputeInput,
    /,
    *,
    nan_is_null: bool = False,
    options: NullOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> BooleanScalar | BooleanArray | ChunkedArray | Expression: ...
def is_valid(
    values: _ComputeInput,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> BooleanScalar | BooleanArray | ChunkedArray | Expression: ...

true_unless_null = _clone_signature(is_valid)

# ========================= Selecting / multiplexing =========================
def case_when(
    cond: _ComputeInput,
    /,
    *cases: _ComputeInput | _Literal,
    memory_pool: MemoryPool | None = None,
) -> _ScalarOrArray | Expression: ...
def choose(
    indices: _ComputeInput | int,
    /,
    *values: _ComputeInput | _Literal,
    memory_pool: MemoryPool | None = None,
) -> _ScalarOrArray | Expression: ...
def coalesce(
    *values: _ComputeInput | _Literal, memory_pool: MemoryPool | None = None
) -> _ScalarOrArray | Expression: ...
def if_else(
    cond: _ComputeInput | bool | np.generic,
    left: _ComputeInput | _Literal,
    right: _ComputeInput | _Literal,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _ScalarOrArray | Expression: ...
def fill_null(
    values: _ScalarOrArray,
    fill_value: _ScalarOrArray | _Literal,
) -> _ScalarOrArray: ...

# ========================= Structural transforms =========================
def list_value_length(
    lists: _ComputeInput,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> (
    Int32Scalar | Int64Scalar | Int32Array | Int64Array | ChunkedArray | Expression
): ...
def make_struct(
    *args: _ComputeInput,
    field_names: Sequence[str] = (),
    field_nullability: Sequence[bool] | None = None,
    field_metadata: Sequence[KeyValueMetadata | None] | None = None,
    options: MakeStructOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> StructScalar | StructArray | ChunkedArray | Expression: ...

# ========================= Conversions =========================
def ceil_temporal(
    timestamps: _ComputeInput,
    /,
    multiple: int = 1,
    unit: _DateTimeUnit = "day",
    *,
    week_starts_monday: bool = True,
    ceil_is_strictly_greater: bool = False,
    calendar_based_origin: bool = False,
    options: RoundTemporalOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _TemporalScalar | _TemporalArray | ChunkedArray | Expression: ...

floor_temporal = _clone_signature(ceil_temporal)
round_temporal = _clone_signature(ceil_temporal)

def cast(
    arr: _ScalarOrArray | _ArrayLike | _Literal,
    target_type: DataType | str | None = None,
    safe: bool | None = None,
    options: CastOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> Scalar | Array | ChunkedArray: ...
def strftime(
    timestamps: _ComputeInput | _TemporalLiteral,
    /,
    format: str | bytes = "%Y-%m-%dT%H:%M:%S",
    locale: str = "C",
    *,
    options: StrftimeOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> StringScalar | StringArray | ChunkedArray | Expression: ...
def strptime(
    strings: _ComputeInput | str,
    /,
    format: str | bytes,
    unit: TimeUnit,
    error_is_null: bool = False,
    *,
    options: StrptimeOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> TimestampScalar | TimestampArray | ChunkedArray | Expression: ...
def run_end_encode(
    array: _ArrayOrChunkedArray | _ArrayLike,
    /,
    run_end_type: DataType | str = ...,
    *,
    options: RunEndEncodeOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> RunEndEncodedArray | ChunkedArray: ...
def run_end_decode(
    array: _ArrayOrChunkedArray | _ArrayLike,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _ArrayOrChunkedArray: ...

# ========================= Temporal component extraction =========================
def day(
    values: _ComputeInput | _TemporalLiteral,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> Int64Scalar | Int64Array | ChunkedArray | Expression: ...
def day_of_week(
    values: _ComputeInput | _TemporalLiteral,
    /,
    *,
    count_from_zero: bool = True,
    week_start: int = 1,
    options: DayOfWeekOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> Int64Scalar | Int64Array | ChunkedArray | Expression: ...

day_of_year = _clone_signature(day)

def hour(
    values: _ComputeInput | _TemporalLiteral,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> Int64Scalar | Int64Array | ChunkedArray | Expression: ...
def is_dst(
    values: _ComputeInput | _TemporalLiteral,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> BooleanScalar | BooleanArray | ChunkedArray | Expression: ...
def iso_week(
    values: _ComputeInput | _TemporalLiteral,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> Int64Scalar | Int64Array | ChunkedArray | Expression: ...

iso_year = _clone_signature(iso_week)

def is_leap_year(
    values: _ComputeInput | _TemporalLiteral,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> BooleanScalar | BooleanArray | ChunkedArray | Expression: ...

microsecond = _clone_signature(iso_week)
millisecond = _clone_signature(iso_week)
minute = _clone_signature(iso_week)
month = _clone_signature(day)
nanosecond = _clone_signature(hour)
quarter = _clone_signature(day)
second = _clone_signature(hour)

def subsecond(
    values: _ComputeInput | _TemporalLiteral,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> DoubleScalar | DoubleArray | ChunkedArray | Expression: ...

us_week = _clone_signature(iso_week)
us_year = _clone_signature(iso_week)

def week(
    values: _ComputeInput | _TemporalLiteral,
    /,
    *,
    week_starts_monday: bool = True,
    count_from_zero: bool = False,
    first_week_is_fully_in_year: bool = False,
    options: WeekOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> Int64Scalar | Int64Array | ChunkedArray | Expression: ...

year = _clone_signature(iso_week)

def year_month_day(
    values: _ComputeInput | _TemporalLiteral,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> StructScalar | StructArray | ChunkedArray | Expression: ...

iso_calendar = _clone_signature(year_month_day)

# ========================= Temporal difference =========================
# TODO(GH-29828): add once PyArrow supports DayTimeInterval type
# def day_time_interval_between(
#     start: _ComputeInput | _TemporalLiteral,
#     end: _ComputeInput | _TemporalLiteral,
#     /,
#     *,
#     memory_pool: MemoryPool | None = None,
# ) -> _ScalarOrArray | Expression: ...
def days_between(
    start: _ComputeInput | _TemporalLiteral,
    end: _ComputeInput | _TemporalLiteral,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> Int64Scalar | Int64Array | ChunkedArray | Expression: ...

hours_between = _clone_signature(days_between)
microseconds_between = _clone_signature(days_between)
milliseconds_between = _clone_signature(days_between)
minutes_between = _clone_signature(days_between)

def month_day_nano_interval_between(
    start: _ComputeInput | _TemporalLiteral,
    end: _ComputeInput | _TemporalLiteral,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> (
    MonthDayNanoIntervalScalar | MonthDayNanoIntervalArray | ChunkedArray | Expression
): ...

# TODO(GH-29828): add once PyArrow supports MonthInterval type
# def month_interval_between(
#     start: _ComputeInput | _TemporalLiteral,
#     end: _ComputeInput | _TemporalLiteral,
#     /,
#     *,
#     memory_pool: MemoryPool | None = None,
# ) -> _ScalarOrArray | Expression: ...

nanoseconds_between = _clone_signature(days_between)
quarters_between = _clone_signature(days_between)
seconds_between = _clone_signature(days_between)

def weeks_between(
    start: _ComputeInput | _TemporalLiteral,
    end: _ComputeInput | _TemporalLiteral,
    /,
    *,
    count_from_zero: bool = True,
    week_start: int = 1,
    options: DayOfWeekOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> Int64Scalar | Int64Array | ChunkedArray | Expression: ...

years_between = _clone_signature(days_between)

# ========================= Timezone handling =========================
def assume_timezone(
    timestamps: _ComputeInput | _TemporalLiteral,
    /,
    timezone: str | bytes,
    *,
    ambiguous: _TimezoneResolution = "raise",
    nonexistent: _TimezoneResolution = "raise",
    options: AssumeTimezoneOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> TimestampScalar | TimestampArray | ChunkedArray | Expression: ...
def local_timestamp(
    values: _ComputeInput | _TemporalLiteral,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> TimestampScalar | TimestampArray | ChunkedArray | Expression: ...

# ========================= Random number generation =========================
def random(
    n: int,
    *,
    initializer: Hashable = "system",
    options: RandomOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> DoubleArray: ...

# =============================== Array-wise ("vector") functions =====================

# ========================= Cumulative Functions =========================
def cumulative_sum(
    values: _ArrayOrChunkedArray | _ArrayLike,
    /,
    start: float | Scalar | None = None,
    *,
    skip_nulls: bool = False,
    options: CumulativeOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _NumericArray | ChunkedArray: ...

cumulative_sum_checked = _clone_signature(cumulative_sum)
cumulative_prod = _clone_signature(cumulative_sum)
cumulative_prod_checked = _clone_signature(cumulative_sum)
cumulative_max = _clone_signature(cumulative_sum)
cumulative_min = _clone_signature(cumulative_sum)
cumulative_mean = _clone_signature(cumulative_sum)

# ========================= Statistical functions =========================
def winsorize(
    array: _ScalarOrArray | _ArrayLike,
    /,
    lower_limit: float,
    upper_limit: float,
    *,
    options: WinsorizeOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _NumericArray | ChunkedArray: ...

# ========================= Associative transforms =========================
def dictionary_encode(
    array: _ScalarOrArray | _ArrayLike,
    /,
    null_encoding: NullEncoding = "mask",
    *,
    options: DictionaryEncodeOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> DictionaryArray | ChunkedArray: ...
def unique(
    array: _ScalarOrArray | _ArrayLike,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> Array: ...
def value_counts(
    array: _ScalarOrArray | _ArrayLike,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> StructArray: ...
def dictionary_decode(
    dictionary_array: _ArrayOrChunkedArray,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _ArrayOrChunkedArray: ...

# ========================= Selections =========================
def array_filter(
    array: _ArrayOrChunkedArray | _ArrayLike,
    selection_filter: Mask,
    /,
    null_selection_behavior: NullSelectionBehavior = "drop",
    *,
    options: FilterOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _ArrayOrChunkedArray: ...
def array_take(
    array: _ArrayOrChunkedArray | _ArrayLike,
    indices: Indices,
    /,
    *,
    boundscheck: bool = True,
    options: TakeOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _ArrayOrChunkedArray: ...
def drop_null(
    input: _ArrayOrChunkedArray | _ArrayLike | RecordBatch | Table,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _ArrayOrChunkedArray | RecordBatch | Table: ...
def filter(
    input: _ArrayOrChunkedArray | _ArrayLike | RecordBatch | Table,
    selection_filter: Mask,
    /,
    null_selection_behavior: NullSelectionBehavior = "drop",
    *,
    options: FilterOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _ArrayOrChunkedArray | RecordBatch | Table: ...
def inverse_permutation(
    indices: _ArrayOrChunkedArray | _ArrayLike,
    /,
    max_index: int = -1,
    output_type: DataType | str | None = None,
    *,
    options: InversePermutationOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> IntegerArray | ChunkedArray: ...
def scatter(
    values: _ArrayOrChunkedArray | _ArrayLike,
    indices: _ArrayOrChunkedArray | _ArrayLike,
    /,
    max_index: int = -1,
    *,
    options: ScatterOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _ArrayOrChunkedArray: ...
def take(
    data: _ArrayOrChunkedArray | _ArrayLike | RecordBatch | Table,
    indices: Indices,
    *,
    boundscheck: bool = True,
    memory_pool: MemoryPool | None = None,
) -> _ArrayOrChunkedArray | RecordBatch | Table: ...

# ========================= Containment tests =========================
def indices_nonzero(
    values: _ArrayOrChunkedArray | _ArrayLike | int,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> UInt64Array: ...

# ========================= Sorts and partitions =========================
def array_sort_indices(
    array: _ArrayOrChunkedArray | _ArrayLike,
    /,
    order: Order = "ascending",
    *,
    null_placement: NullPlacement = "at_end",
    options: ArraySortOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> UInt64Array | ChunkedArray: ...
def partition_nth_indices(
    array: Array | _ArrayLike,
    /,
    pivot: int,
    *,
    null_placement: NullPlacement = "at_end",
    options: PartitionNthOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> UInt64Array: ...
def rank(
    input: _ArrayOrChunkedArray | _ArrayLike,
    /,
    sort_keys: Order | Sequence[_SortKey] = "ascending",
    *,
    null_placement: NullPlacement | None = None,
    tiebreaker: _Tiebreaker = "first",
    options: RankOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> UInt64Array: ...
def rank_normal(
    input: _ArrayOrChunkedArray | _ArrayLike,
    /,
    sort_keys: Order | Sequence[_SortKey] = "ascending",
    *,
    null_placement: NullPlacement | None = None,
    options: RankQuantileOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> DoubleArray: ...
def rank_quantile(
    input: _ArrayOrChunkedArray | _ArrayLike,
    /,
    sort_keys: Order | Sequence[_SortKey] = "ascending",
    *,
    null_placement: NullPlacement | None = None,
    options: RankQuantileOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> DoubleArray: ...
def select_k_unstable(
    input: _ArrayOrChunkedArray | _ArrayLike | RecordBatch | Table,
    /,
    k: int,
    sort_keys: Sequence[_SortKey],
    *,
    options: SelectKOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> UInt64Array: ...
def sort_indices(
    input: _ArrayOrChunkedArray | _ArrayLike | RecordBatch | Table,
    /,
    sort_keys: Sequence[_SortKey] = (),
    *,
    null_placement: NullPlacement | None = None,
    options: SortOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> UInt64Array: ...
def search_sorted(
    values: _ArrayOrChunkedArray | _ArrayLike,
    needles: _ScalarOrArray | _ArrayLike | _Literal,
    /,
    side: _Side = "left",
    *,
    options: SearchSortedOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> UInt64Scalar | UInt64Array: ...
def top_k_unstable(
    values: _ArrayOrChunkedArray | RecordBatch | Table,
    k: int,
    sort_keys: Sequence[str] | None = None,
    null_placements: Sequence[NullPlacement] | None = None,
    *,
    memory_pool: MemoryPool | None = None,
) -> UInt64Array: ...
def bottom_k_unstable(
    values: _ArrayOrChunkedArray | RecordBatch | Table,
    k: int,
    sort_keys: Sequence[str] | None = None,
    null_placements: Sequence[NullPlacement] | None = None,
    *,
    memory_pool: MemoryPool | None = None,
) -> UInt64Array: ...

# ========================= Structural transforms =========================
def list_contains(
    lists: _ComputeInput,
    value: _ComputeInput | _Literal,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> BooleanScalar | BooleanArray | ChunkedArray | Expression: ...
def list_element(
    lists: _ComputeInput,
    index: _ComputeInput | int,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _ScalarOrArray | Expression: ...
def list_flatten(
    lists: _ScalarOrArray | _ArrayLike,
    /,
    recursive: bool = False,
    *,
    options: ListFlattenOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _ArrayOrChunkedArray: ...
def list_parent_indices(
    lists: _ArrayOrChunkedArray | _ArrayLike,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> Int64Array | ChunkedArray: ...
def list_slice(
    lists: _ComputeInput,
    /,
    start: int,
    stop: int | None = None,
    step: int = 1,
    return_fixed_size_list: bool | None = None,
    *,
    options: ListSliceOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _ListScalar | BaseListArray | ChunkedArray | Expression: ...
def map_lookup(
    container: _ScalarOrArray | Expression,
    /,
    query_key: Scalar | _Literal,
    occurrence: _Occurrence,
    *,
    options: MapLookupOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _ScalarOrArray | Expression: ...
def struct_field(
    values: _ScalarOrArray | Expression,
    /,
    indices: Sequence[str | bytes | int] | bytes | str | int | Expression,
    *,
    options: StructFieldOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _ScalarOrArray | Expression: ...

# ========================= Replace functions =========================
def fill_null_backward(
    values: _ArrayOrChunkedArray | _ArrayLike,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _ArrayOrChunkedArray: ...
def fill_null_forward(
    values: _ArrayOrChunkedArray | _ArrayLike,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _ArrayOrChunkedArray: ...
def replace_with_mask(
    values: _ArrayOrChunkedArray | _ArrayLike,
    mask: Mask,
    replacements: _ScalarOrArray | _ArrayLike,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _ArrayOrChunkedArray: ...

# ========================= Pairwise functions =========================
def pairwise_diff(
    input: Array | _ArrayLike,
    /,
    period: int = 1,
    *,
    options: PairwiseOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _NumericOrTemporalArray: ...

pairwise_diff_checked = _clone_signature(pairwise_diff)
