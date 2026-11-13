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
from typing import Any, Literal, ParamSpec, TypeAlias, TypeVar

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
    NullScalar,
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
from pyarrow._compute import _CountMode, _DateTimeUnit, _RoundMode, _SortKey
from pyarrow._stubs_typing import (
    ArrayLike,
    Indices,
    Mask,
    NullPlacement,
    Order,
    ScalarLike,
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
_NonFloatNumericScalar: TypeAlias = _IntegerScalar | _DecimalScalar
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
# the result is, e.g. a StringArray for a string input. The ChunkedArray of a
# chunked input is the same for every kind.
_ArrayOrChunkedArray: TypeAlias = Array | ChunkedArray
_ScalarOrArray: TypeAlias = Scalar | Array | ChunkedArray
_DecimalArray: TypeAlias = (
    Decimal32Array | Decimal64Array | Decimal128Array | Decimal256Array
)
# NumericArray includes the date, time, timestamp and duration arrays.
_NumericArray: TypeAlias = NumericArray | _DecimalArray | ChunkedArray
_NumericOrDurationArray: TypeAlias = _NumericArray
_NumericOrTemporalArray: TypeAlias = _NumericArray | MonthDayNanoIntervalArray
_IntegerArray: TypeAlias = IntegerArray | ChunkedArray
_FloatArray: TypeAlias = FloatingPointArray | ChunkedArray
_BooleanArray: TypeAlias = BooleanArray | ChunkedArray
_StringArray: TypeAlias = (
    StringArray | LargeStringArray | StringViewArray | ChunkedArray
)
_BinaryArray: TypeAlias = (
    BinaryArray
    | LargeBinaryArray
    | BinaryViewArray
    | FixedSizeBinaryArray
    | ChunkedArray
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
    | ChunkedArray
)
_ListArray: TypeAlias = BaseListArray | ChunkedArray

# =============================== 1. Aggregation ===============================

def array_take(
    array: _ArrayOrChunkedArray | RecordBatch | Table | Expression,
    indices: Indices | Expression,
    /,
    *,
    boundscheck: bool = True,
    options: TakeOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _ArrayOrChunkedArray | RecordBatch | Table | Expression: ...

# ========================= 1.1 functions =========================

def all(
    array: BooleanScalar | _ArrayOrChunkedArray | Expression,
    /,
    *,
    skip_nulls: bool = True,
    min_count: int = 1,
    options: ScalarAggregateOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> BooleanScalar | Expression: ...

any = _clone_signature(all)

def approximate_median(
    array: _NumericScalar | _ArrayOrChunkedArray | Expression,
    /,
    *,
    skip_nulls: bool = True,
    min_count: int = 1,
    options: ScalarAggregateOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> DoubleScalar | Expression: ...
def count(
    array: _ArrayOrChunkedArray | Expression,
    /,
    mode: _CountMode = "only_valid",
    *,
    options: CountOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> Int64Scalar | Expression: ...
def count_distinct(
    array: _ArrayOrChunkedArray | Expression,
    /,
    mode: _CountMode = "only_valid",
    *,
    options: CountOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> Int64Scalar | Expression: ...
def first(
    array: _ArrayOrChunkedArray | Expression,
    /,
    *,
    skip_nulls: bool = True,
    min_count: int = 1,
    options: ScalarAggregateOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> Scalar | Expression: ...

last = _clone_signature(first)

def first_last(
    array: _ArrayOrChunkedArray | Iterable[Any] | Expression,
    /,
    *,
    skip_nulls: bool = True,
    min_count: int = 1,
    options: ScalarAggregateOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> StructScalar | Expression: ...
def index(
    data: _ArrayOrChunkedArray,
    value: ScalarLike,
    start: int | None = None,
    end: int | None = None,
    *,
    memory_pool: MemoryPool | None = None,
) -> Int64Scalar: ...

max = _clone_signature(first)
min = _clone_signature(first)
min_max = _clone_signature(first_last)

def mean(
    array: _NumericScalar | _ArrayOrChunkedArray | Expression,
    /,
    *,
    skip_nulls: bool = True,
    min_count: int = 1,
    options: ScalarAggregateOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> Scalar | Expression: ...
def mode(
    array: _NumericScalar | _ArrayOrChunkedArray | Expression,
    /,
    n: int = 1,
    *,
    skip_nulls: bool = True,
    min_count: int = 0,
    options: ModeOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> StructArray | Expression: ...
def product(
    array: _NumericScalar | _ArrayOrChunkedArray | Expression,
    /,
    *,
    skip_nulls: bool = True,
    min_count: int = 1,
    options: ScalarAggregateOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _NumericScalar | Expression: ...
def quantile(
    array: _NumericScalar | _ArrayOrChunkedArray | Expression,
    /,
    q: float | Sequence[float] = 0.5,
    *,
    interpolation: Literal[
        "linear", "lower", "higher", "nearest", "midpoint"
    ] = "linear",
    skip_nulls: bool = True,
    min_count: int = 0,
    options: QuantileOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> DoubleArray | Expression: ...
def stddev(
    array: _NumericScalar | _ArrayOrChunkedArray | Expression,
    /,
    *,
    ddof: float = 0,
    skip_nulls: bool = True,
    min_count: int = 0,
    options: VarianceOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> DoubleScalar | Expression: ...
def sum(
    array: _NumericScalar | _ArrayOrChunkedArray | Expression,
    /,
    *,
    skip_nulls: bool = True,
    min_count: int = 1,
    options: ScalarAggregateOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _NumericScalar | Expression: ...
def tdigest(
    array: _NumericScalar | _ArrayOrChunkedArray | Expression,
    /,
    q: float | Sequence[float] = 0.5,
    *,
    delta: int = 100,
    buffer_size: int = 500,
    skip_nulls: bool = True,
    min_count: int = 0,
    options: TDigestOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> DoubleArray | Expression: ...
def variance(
    array: _NumericScalar | _ArrayOrChunkedArray | ArrayLike | Expression,
    /,
    *,
    ddof: int = 0,
    skip_nulls: bool = True,
    min_count: int = 0,
    options: VarianceOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> DoubleScalar | Expression: ...
def winsorize(
    array: _ArrayOrChunkedArray | Expression,
    /,
    lower_limit: float,
    upper_limit: float,
    *,
    options: WinsorizeOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _NumericArray | Expression: ...
def skew(
    array: _NumericScalar | _ArrayOrChunkedArray | ArrayLike | Expression,
    /,
    *,
    skip_nulls: bool = True,
    biased: bool = True,
    min_count: int = 0,
    options: SkewOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> DoubleScalar | Expression: ...
def kurtosis(
    array: _NumericScalar | _ArrayOrChunkedArray | ArrayLike | Expression,
    /,
    *,
    skip_nulls: bool = True,
    biased: bool = True,
    min_count: int = 0,
    options: SkewOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> DoubleScalar | Expression: ...
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

# ========================= 2. Element-wise (“scalar”) functions =========

# ========================= 2.1 Arithmetic =========================
def abs(
    x: _NumericOrDurationScalar | _ArrayOrChunkedArray | Expression | float,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _NumericOrDurationScalar | _NumericOrDurationArray | Expression: ...

abs_checked = _clone_signature(abs)

def add(
    x: _NumericOrTemporalScalar | _ArrayOrChunkedArray | ArrayLike | int | Expression,
    y: (_NumericOrTemporalScalar | _ArrayOrChunkedArray | ArrayLike | int | Expression),
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _NumericOrTemporalScalar | _NumericOrTemporalArray | Expression: ...

add_checked = _clone_signature(add)

def divide(
    x: _NumericOrTemporalScalar | _ArrayOrChunkedArray | Expression | float,
    y: _NumericOrTemporalScalar | _ArrayOrChunkedArray | Expression | float,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _NumericOrTemporalScalar | _NumericOrTemporalArray | Expression: ...

divide_checked = _clone_signature(divide)

def hypot(
    x: _NumericScalar | _ArrayOrChunkedArray | float | Expression,
    y: _NumericScalar | _ArrayOrChunkedArray | float | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _FloatScalar | _FloatArray | Expression: ...
def modulo(
    dividend: _NumericScalar | _ArrayOrChunkedArray | float | Expression,
    divisor: _NumericScalar | _ArrayOrChunkedArray | float | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _NumericScalar | _NumericArray | Expression: ...

modulo_checked = _clone_signature(modulo)
remainder = _clone_signature(modulo)
remainder_checked = _clone_signature(modulo)

def exp(
    exponent: _ArrayOrChunkedArray
    | _FloatScalar
    | _NonFloatNumericScalar
    | DoubleScalar
    | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _FloatArray | DoubleArray | _FloatScalar | DoubleScalar | Expression: ...

expm1 = _clone_signature(exp)
multiply = _clone_signature(add)
multiply_checked = _clone_signature(add)

def negate(
    x: _NumericOrDurationScalar | _ArrayOrChunkedArray | Expression | float,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _NumericOrDurationScalar | _NumericOrDurationArray | Expression: ...

negate_checked = _clone_signature(negate)

def power(
    base: _NumericScalar | Expression | _ArrayOrChunkedArray | float,
    exponent: _NumericScalar | Expression | _ArrayOrChunkedArray | float,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _NumericScalar | _NumericArray | Expression: ...

power_checked = _clone_signature(power)

def sign(
    x: _ArrayOrChunkedArray | _NumericOrDurationScalar | Expression | float,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> Int8Scalar | _FloatScalar | NumericArray | ChunkedArray | Expression: ...
def sqrt(
    x: _ArrayOrChunkedArray | _NumericScalar | Expression | float,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _FloatArray | _FloatScalar | Expression: ...

sqrt_checked = _clone_signature(sqrt)

subtract = _clone_signature(add)
subtract_checked = _clone_signature(add)

# ========================= 2.1 Bit-wise functions =========================

def bit_wise_and(
    x: _NumericScalar | _ArrayOrChunkedArray | Expression | float,
    y: _NumericScalar | _ArrayOrChunkedArray | Expression | float,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _NumericScalar | _NumericArray | Expression: ...
def bit_wise_not(
    x: _NumericScalar | _ArrayOrChunkedArray | Expression | float,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _NumericScalar | _NumericArray | Expression: ...

bit_wise_or = _clone_signature(bit_wise_and)
bit_wise_xor = _clone_signature(bit_wise_and)
shift_left = _clone_signature(bit_wise_and)
shift_left_checked = _clone_signature(bit_wise_and)
shift_right = _clone_signature(bit_wise_and)
shift_right_checked = _clone_signature(bit_wise_and)

# ========================= 2.2 Rounding functions =========================

def ceil(
    x: _FloatScalar | _ArrayOrChunkedArray | Expression | float,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _FloatScalar | _FloatArray | Expression: ...

floor = _clone_signature(ceil)

def round(
    x: _NumericScalar | _ArrayOrChunkedArray | Iterable[Any] | Expression | float,
    /,
    ndigits: int = 0,
    round_mode: _RoundMode = "half_to_even",
    *,
    options: RoundOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _NumericScalar | _NumericArray | Expression: ...
def round_to_multiple(
    x: _NumericScalar | _ArrayOrChunkedArray | Iterable[Any] | Expression | float,
    /,
    multiple: float | _NumericScalar = 1.0,
    round_mode: _RoundMode = "half_to_even",
    *,
    options: RoundToMultipleOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _NumericScalar | _NumericArray | Expression: ...
def round_binary(
    x: _NumericScalar | _ArrayOrChunkedArray | Iterable[Any] | float | Expression,
    s: _SignedIntegerScalar | _ArrayOrChunkedArray | int | Expression,
    /,
    round_mode: _RoundMode = "half_to_even",
    *,
    options: RoundBinaryOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _NumericScalar | _NumericArray | Expression: ...

trunc = _clone_signature(ceil)

# ========================= 2.3 Logarithmic functions =========================

def ln(
    x: _NumericScalar | _ArrayOrChunkedArray | Expression | float,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _FloatScalar | _FloatArray | Expression: ...

ln_checked = _clone_signature(ln)
log10 = _clone_signature(ln)
log10_checked = _clone_signature(ln)
log1p = _clone_signature(ln)
log1p_checked = _clone_signature(ln)
log2 = _clone_signature(ln)
log2_checked = _clone_signature(ln)

def logb(
    x: _NumericScalar | _ArrayOrChunkedArray | float | Expression,
    b: _NumericScalar | _ArrayOrChunkedArray | float | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _FloatScalar | _FloatArray | Expression: ...

logb_checked = _clone_signature(logb)

# ========================= 2.4 Trigonometric functions =========================
acos = _clone_signature(ln)
acos_checked = _clone_signature(ln)
acosh = _clone_signature(ln)
acosh_checked = _clone_signature(ln)
asin = _clone_signature(ln)
asin_checked = _clone_signature(ln)
asinh = _clone_signature(ln)
atan = _clone_signature(ln)
atanh_checked = _clone_signature(ln)
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
    y: _NumericScalar | _ArrayOrChunkedArray | float | Expression,
    x: _NumericScalar | _ArrayOrChunkedArray | float | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _FloatScalar | _FloatArray | Expression: ...

# ========================= 2.5 Comparisons functions =========================
def equal(
    x: _ScalarOrArray | ScalarLike | Expression,
    y: _ScalarOrArray | ScalarLike | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> BooleanScalar | _BooleanArray | Expression: ...

greater = _clone_signature(equal)
greater_equal = _clone_signature(equal)
less = _clone_signature(equal)
less_equal = _clone_signature(equal)
not_equal = _clone_signature(equal)

def max_element_wise(
    *args: _ScalarOrArray | Expression | ScalarLike | ArrayLike,
    skip_nulls: bool = True,
    options: ElementWiseAggregateOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _ScalarOrArray | Expression: ...

min_element_wise = _clone_signature(max_element_wise)

# ========================= 2.6 Logical functions =========================

def and_(
    x: BooleanScalar | _ArrayOrChunkedArray | bool | Expression,
    y: BooleanScalar | _ArrayOrChunkedArray | bool | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> BooleanScalar | _BooleanArray | Expression: ...

and_kleene = _clone_signature(and_)
and_not = _clone_signature(and_)
and_not_kleene = _clone_signature(and_)
or_ = _clone_signature(and_)
or_kleene = _clone_signature(and_)
xor = _clone_signature(and_)

def invert(
    x: BooleanScalar | _ArrayOrChunkedArray | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> BooleanScalar | _BooleanArray | Expression: ...

# ========================= 2.10 String predicates =========================
def ascii_is_alnum(
    strings: _StringScalar | _ArrayOrChunkedArray | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> BooleanScalar | _BooleanArray | Expression: ...

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
    strings: _StringScalar | _ArrayOrChunkedArray | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _StringScalar | _StringArray | Expression: ...

ascii_lower = _clone_signature(ascii_capitalize)
ascii_reverse = _clone_signature(ascii_capitalize)
ascii_swapcase = _clone_signature(ascii_capitalize)
ascii_title = _clone_signature(ascii_capitalize)
ascii_upper = _clone_signature(ascii_capitalize)

def binary_length(
    strings: _StringOrBinaryScalar | _ArrayOrChunkedArray | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> (
    Int32Scalar | Int64Scalar | Int32Array | Int64Array | ChunkedArray | Expression
): ...
def binary_repeat(
    strings: _StringOrBinaryScalar | _ArrayOrChunkedArray | Expression,
    num_repeats: int | _ArrayOrChunkedArray | Iterable[int | None] | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _StringOrBinaryScalar | _StringOrBinaryArray | Expression: ...
def binary_replace_slice(
    strings: _StringOrBinaryScalar | _ArrayOrChunkedArray | Expression,
    /,
    start: int,
    stop: int,
    replacement: str | bytes,
    *,
    options: ReplaceSliceOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _StringOrBinaryScalar | _StringOrBinaryArray | Expression: ...
def binary_reverse(
    strings: _BinaryScalar | _ArrayOrChunkedArray | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _BinaryScalar | _BinaryArray | Expression: ...
def replace_substring(
    strings: _StringScalar | _ArrayOrChunkedArray | Expression,
    /,
    pattern: str | bytes,
    replacement: str | bytes,
    *,
    max_replacements: int | None = None,
    options: ReplaceSubstringOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _StringScalar | _StringArray | Expression: ...

replace_substring_regex = _clone_signature(replace_substring)

def utf8_capitalize(
    strings: _StringScalar | _ArrayOrChunkedArray | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _StringScalar | _StringArray | Expression: ...
def utf8_length(
    strings: _StringScalar | _ArrayOrChunkedArray | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> (
    Int32Scalar | Int64Scalar | Int32Array | Int64Array | ChunkedArray | Expression
): ...

utf8_lower = _clone_signature(utf8_capitalize)

def utf8_replace_slice(
    strings: _StringScalar | _ArrayOrChunkedArray | Expression,
    /,
    start: int,
    stop: int,
    replacement: str | bytes,
    *,
    options: ReplaceSliceOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _StringScalar | _StringArray | Expression: ...

utf8_reverse = _clone_signature(utf8_capitalize)
utf8_swapcase = _clone_signature(utf8_capitalize)
utf8_title = _clone_signature(utf8_capitalize)
utf8_upper = _clone_signature(utf8_capitalize)

# ========================= 2.12 String padding =========================

def ascii_center(
    strings: _StringScalar | _ArrayOrChunkedArray | Expression,
    /,
    width: int,
    padding: str = " ",
    lean_left_on_odd_padding: bool = True,
    *,
    options: PadOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _StringScalar | _StringArray | Expression: ...

ascii_lpad = _clone_signature(ascii_center)
ascii_rpad = _clone_signature(ascii_center)
utf8_center = _clone_signature(ascii_center)
utf8_lpad = _clone_signature(ascii_center)
utf8_rpad = _clone_signature(ascii_center)

def utf8_zero_fill(
    strings: _StringScalar | _ArrayOrChunkedArray | Expression,
    /,
    width: int,
    padding: str = "0",
    *,
    options: ZeroFillOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _StringScalar | _StringArray | Expression: ...

utf8_zfill = utf8_zero_fill

# ========================= 2.13 String trimming =========================

def ascii_ltrim(
    strings: _StringScalar | _ArrayOrChunkedArray | Expression,
    /,
    characters: str,
    *,
    options: TrimOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _StringScalar | _StringArray | Expression: ...

ascii_rtrim = _clone_signature(ascii_ltrim)
ascii_trim = _clone_signature(ascii_ltrim)
utf8_ltrim = _clone_signature(ascii_ltrim)
utf8_rtrim = _clone_signature(ascii_ltrim)
utf8_trim = _clone_signature(ascii_ltrim)

def ascii_ltrim_whitespace(
    strings: _StringScalar | _ArrayOrChunkedArray | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _StringScalar | _StringArray | Expression: ...

ascii_rtrim_whitespace = _clone_signature(ascii_ltrim_whitespace)
ascii_trim_whitespace = _clone_signature(ascii_ltrim_whitespace)
utf8_ltrim_whitespace = _clone_signature(ascii_ltrim_whitespace)
utf8_rtrim_whitespace = _clone_signature(ascii_ltrim_whitespace)
utf8_trim_whitespace = _clone_signature(ascii_ltrim_whitespace)

# ========================= 2.14 String splitting =========================

def ascii_split_whitespace(
    strings: _StringScalar | _ArrayOrChunkedArray | Expression,
    /,
    *,
    max_splits: int | None = None,
    reverse: bool = False,
    options: SplitOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _ListScalar | _ListArray | Expression: ...
def split_pattern(
    strings: _StringOrBinaryScalar | _ArrayOrChunkedArray | Expression,
    /,
    pattern: str,
    *,
    max_splits: int | None = None,
    reverse: bool = False,
    options: SplitOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _ListScalar | _ListArray | Expression: ...

split_pattern_regex = _clone_signature(split_pattern)
utf8_split_whitespace = _clone_signature(ascii_split_whitespace)

# ========================= 2.15 String component extraction =========================

def extract_regex(
    strings: _StringOrBinaryScalar | _ArrayOrChunkedArray | Expression,
    /,
    pattern: str,
    *,
    options: ExtractRegexOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> StructScalar | StructArray | ChunkedArray | Expression: ...

extract_regex_span = _clone_signature(extract_regex)

# ========================= 2.16 String join =========================
def binary_join(
    strings: _ListScalar | _ArrayOrChunkedArray | Expression,
    separator: str | bytes | _StringOrBinaryScalar | _ArrayOrChunkedArray | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _StringOrBinaryScalar | _StringOrBinaryArray | Expression: ...
def binary_join_element_wise(
    *strings: str
    | bytes
    | _StringOrBinaryScalar
    | _ArrayOrChunkedArray
    | Iterable[Any]
    | Expression,
    null_handling: Literal["emit_null", "skip", "replace"] = "emit_null",
    null_replacement: str = "",
    options: JoinOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _StringOrBinaryScalar | _StringOrBinaryArray | Expression: ...

# ========================= 2.17 String Slicing =========================
def binary_slice(
    strings: _BinaryScalar | _ArrayOrChunkedArray | Expression,
    /,
    start: int,
    stop: int | None = None,
    step: int = 1,
    *,
    options: SliceOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _BinaryScalar | _BinaryArray | Expression: ...
def utf8_slice_codeunits(
    strings: _StringScalar | _ArrayOrChunkedArray | Expression,
    /,
    start: int,
    stop: int | None = None,
    step: int = 1,
    *,
    options: SliceOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _StringScalar | _StringArray | Expression: ...
def utf8_normalize(
    strings: _StringScalar | _ArrayOrChunkedArray | Expression,
    /,
    form: Literal["NFC", "NFKC", "NFD", "NFKD"],
    *,
    options: Utf8NormalizeOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _StringScalar | _StringArray | Expression: ...

# ========================= 2.18 Containment tests =========================
def count_substring(
    strings: _StringOrBinaryScalar | _ArrayOrChunkedArray | Expression,
    /,
    pattern: str,
    *,
    ignore_case: bool = False,
    options: MatchSubstringOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> (
    Int32Scalar | Int64Scalar | Int32Array | Int64Array | ChunkedArray | Expression
): ...

count_substring_regex = _clone_signature(count_substring)

def ends_with(
    strings: _StringOrBinaryScalar | _ArrayOrChunkedArray | Expression,
    /,
    pattern: str,
    *,
    ignore_case: bool = False,
    options: MatchSubstringOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> BooleanScalar | _BooleanArray | Expression: ...

find_substring = _clone_signature(count_substring)
find_substring_regex = _clone_signature(count_substring)

def index_in(
    values: _ScalarOrArray | Expression,
    /,
    value_set: _ArrayOrChunkedArray | Expression,
    *,
    skip_nulls: bool = False,
    options: SetLookupOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> Int32Scalar | Int32Array | ChunkedArray | Expression: ...
def index_in_meta_binary(
    values: _ScalarOrArray | Expression,
    value_set: _ArrayOrChunkedArray | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> Int32Scalar | Int32Array | ChunkedArray | Expression: ...
def is_in(
    values: _ScalarOrArray | Expression,
    /,
    value_set: _ArrayOrChunkedArray | Expression,
    *,
    skip_nulls: bool = False,
    options: SetLookupOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> BooleanScalar | _BooleanArray | Expression: ...

is_in_meta_binary = _clone_signature(index_in_meta_binary)
match_like = _clone_signature(ends_with)
match_substring = _clone_signature(ends_with)
match_substring_regex = _clone_signature(ends_with)
starts_with = _clone_signature(ends_with)

# ========================= 2.19 Categorizations =========================

def is_finite(
    values: _NumericScalar | NullScalar | _ArrayOrChunkedArray | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> BooleanScalar | _BooleanArray | Expression: ...

is_inf = _clone_signature(is_finite)
is_nan = _clone_signature(is_finite)

def is_null(
    values: _ScalarOrArray | Expression,
    /,
    *,
    nan_is_null: bool = False,
    options: NullOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> BooleanScalar | _BooleanArray | Expression: ...
def is_valid(
    values: _ScalarOrArray | Expression | ArrayLike,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> BooleanScalar | _BooleanArray | Expression: ...

true_unless_null = _clone_signature(is_valid)

# ========================= 2.20 Selecting / multiplexing =========================

def case_when(
    cond: StructScalar | _ArrayOrChunkedArray | Expression,
    /,
    *cases: _ScalarOrArray | ArrayLike,
    memory_pool: MemoryPool | None = None,
) -> _ScalarOrArray | Array | Expression: ...
def choose(
    indices: ArrayLike | ScalarLike,
    /,
    *values: ArrayLike | ScalarLike,
    memory_pool: MemoryPool | None = None,
) -> ArrayLike | ScalarLike: ...
def coalesce(
    *values: _ScalarOrArray | Expression, memory_pool: MemoryPool | None = None
) -> _ScalarOrArray | Expression: ...
def fill_null(
    values: _ScalarOrArray | ScalarLike, fill_value: ArrayLike | ScalarLike
) -> _ScalarOrArray | ScalarLike: ...
def if_else(
    cond: ArrayLike | ScalarLike,
    left: ArrayLike | ScalarLike,
    right: ArrayLike | ScalarLike,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> ArrayLike | ScalarLike: ...

# ========================= 2.21 Structural transforms =========================

def list_value_length(
    lists: _ArrayOrChunkedArray | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> (
    Int32Scalar | Int64Scalar | Int32Array | Int64Array | ChunkedArray | Expression
): ...
def make_struct(
    *args: _ScalarOrArray | Expression | ArrayLike,
    field_names: Sequence[str] = (),
    field_nullability: bool | None = None,
    field_metadata: Sequence[KeyValueMetadata] | None = None,
    options: MakeStructOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> StructScalar | StructArray | ChunkedArray | Expression: ...

# ========================= 2.22 Conversions =========================
def ceil_temporal(
    timestamps: _TemporalScalar | _ArrayOrChunkedArray | Expression,
    /,
    multiple: int = 1,
    unit: _DateTimeUnit = "day",
    *,
    week_starts_monday: bool = True,
    ceil_is_strictly_greater: bool = False,
    calendar_based_origin: bool = False,
    options: RoundTemporalOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _TemporalScalar | _TemporalArray | Expression: ...

floor_temporal = _clone_signature(ceil_temporal)
round_temporal = _clone_signature(ceil_temporal)

def cast(
    arr: _ScalarOrArray | Table,
    target_type: DataType | str | None = None,
    safe: bool | None = None,
    options: CastOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> Scalar | Array | ChunkedArray | Table: ...
def strftime(
    timestamps: _TemporalScalar | _ArrayOrChunkedArray | Expression,
    /,
    format: str = "%Y-%m-%dT%H:%M:%S",
    locale: str = "C",
    *,
    options: StrftimeOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> StringScalar | StringArray | ChunkedArray | Expression: ...
def strptime(
    strings: _StringScalar | _ArrayOrChunkedArray | Expression,
    /,
    format: str,
    unit: TimeUnit,
    error_is_null: bool = False,
    *,
    options: StrptimeOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> TimestampScalar | TimestampArray | ChunkedArray | Expression: ...

# ========================= 2.23 Temporal component extraction =========================
def day(
    values: _TemporalScalar | _ArrayOrChunkedArray | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> Int64Scalar | Int64Array | ChunkedArray | Expression: ...
def day_of_week(
    values: _TemporalScalar | _ArrayOrChunkedArray | Expression,
    /,
    *,
    count_from_zero: bool = True,
    week_start: int = 1,
    options: DayOfWeekOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> Int64Scalar | Int64Array | ChunkedArray | Expression: ...

day_of_year = _clone_signature(day)

def hour(
    values: TimestampScalar
    | Time32Scalar
    | Time64Scalar
    | _ArrayOrChunkedArray
    | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> Int64Scalar | Int64Array | ChunkedArray | Expression: ...
def is_dst(
    values: TimestampScalar | _ArrayOrChunkedArray | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> BooleanScalar | _BooleanArray | Expression: ...
def iso_week(
    values: TimestampScalar | _ArrayOrChunkedArray | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> Int64Scalar | Int64Array | ChunkedArray | Expression: ...

iso_year = _clone_signature(iso_week)

def is_leap_year(
    values: TimestampScalar
    | Date32Scalar
    | Date64Scalar
    | _ArrayOrChunkedArray
    | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> BooleanScalar | _BooleanArray | Expression: ...

microsecond = _clone_signature(iso_week)
millisecond = _clone_signature(iso_week)
minute = _clone_signature(iso_week)
month = _clone_signature(day)
nanosecond = _clone_signature(hour)
quarter = _clone_signature(day)
second = _clone_signature(hour)
subsecond = _clone_signature(hour)
us_week = _clone_signature(iso_week)
us_year = _clone_signature(iso_week)
year = _clone_signature(iso_week)

def week(
    values: TimestampScalar | _ArrayOrChunkedArray | Expression,
    /,
    *,
    week_starts_monday: bool = True,
    count_from_zero: bool = False,
    first_week_is_fully_in_year: bool = False,
    options: WeekOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> Int64Scalar | Int64Array | ChunkedArray | Expression: ...
def year_month_day(
    values: _TemporalScalar | _ArrayOrChunkedArray | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> StructScalar | StructArray | ChunkedArray | Expression: ...

iso_calendar = _clone_signature(year_month_day)

# ========================= 2.24 Temporal difference =========================
def day_time_interval_between(
    start: _TemporalScalar | _ArrayOrChunkedArray | Expression,
    end: _TemporalScalar | _ArrayOrChunkedArray | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _ScalarOrArray | Expression: ...
def days_between(
    start: _TemporalScalar | _ArrayOrChunkedArray | Expression,
    end: _TemporalScalar | _ArrayOrChunkedArray | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> Int64Scalar | Int64Array | ChunkedArray | Expression: ...

hours_between = _clone_signature(days_between)
microseconds_between = _clone_signature(days_between)
milliseconds_between = _clone_signature(days_between)
minutes_between = _clone_signature(days_between)

def month_day_nano_interval_between(
    start: _TemporalScalar | _ArrayOrChunkedArray | Expression,
    end: _TemporalScalar | _ArrayOrChunkedArray | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> (
    MonthDayNanoIntervalScalar | MonthDayNanoIntervalArray | ChunkedArray | Expression
): ...
def month_interval_between(
    start: _TemporalScalar | _ArrayOrChunkedArray | Expression,
    end: _TemporalScalar | _ArrayOrChunkedArray | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _ScalarOrArray | Expression: ...

nanoseconds_between = _clone_signature(days_between)
quarters_between = _clone_signature(days_between)
seconds_between = _clone_signature(days_between)

def weeks_between(
    start: _TemporalScalar | _ArrayOrChunkedArray | Expression,
    end: _TemporalScalar | _ArrayOrChunkedArray | Expression,
    /,
    *,
    count_from_zero: bool = True,
    week_start: int = 1,
    options: DayOfWeekOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> Int64Scalar | Int64Array | ChunkedArray | Expression: ...

years_between = _clone_signature(days_between)

# ========================= 2.25 Timezone handling =========================

def assume_timezone(
    timestamps: TimestampScalar | _ArrayOrChunkedArray | Expression,
    /,
    timezone: str,
    *,
    ambiguous: Literal["raise", "earliest", "latest"] = "raise",
    nonexistent: Literal["raise", "earliest", "latest"] = "raise",
    options: AssumeTimezoneOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> TimestampScalar | TimestampArray | ChunkedArray | Expression: ...
def local_timestamp(
    values: TimestampScalar | _ArrayOrChunkedArray | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> TimestampScalar | TimestampArray | ChunkedArray | Expression: ...

# ========================= 2.26 Random number generation =========================
def random(
    n: int,
    *,
    initializer: Hashable = "system",
    options: RandomOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> DoubleArray: ...

# ========================= 3. Array-wise (“vector”) functions =========================

# ========================= 3.1 Cumulative Functions =========================
def cumulative_sum(
    values: _ArrayOrChunkedArray | ArrayLike | Expression,
    /,
    start: float | Scalar | None = None,
    *,
    skip_nulls: bool = False,
    options: CumulativeOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _NumericArray | Expression: ...

cumulative_sum_checked = _clone_signature(cumulative_sum)
cumulative_prod = _clone_signature(cumulative_sum)
cumulative_prod_checked = _clone_signature(cumulative_sum)
cumulative_max = _clone_signature(cumulative_sum)
cumulative_min = _clone_signature(cumulative_sum)
cumulative_mean = _clone_signature(cumulative_sum)
# ========================= 3.2 Associative transforms =========================

def dictionary_encode(
    array: _ScalarOrArray | Expression,
    /,
    null_encoding: Literal["mask", "encode"] = "mask",
    *,
    options: DictionaryEncodeOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _ScalarOrArray | Expression: ...
def dictionary_decode(
    dictionary_array: _ScalarOrArray | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _ScalarOrArray | Expression: ...
def unique(
    array: _ArrayOrChunkedArray | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _ArrayOrChunkedArray | Expression: ...
def value_counts(
    array: _ArrayOrChunkedArray | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> StructArray | Expression: ...

# ========================= 3.3 Selections =========================

def array_filter(
    array: _ArrayOrChunkedArray | Expression,
    selection_filter: Mask | Expression,
    /,
    null_selection_behavior: Literal["drop", "emit_null"] = "drop",
    *,
    options: FilterOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _ArrayOrChunkedArray | Expression: ...
def drop_null(
    input: _ArrayOrChunkedArray | RecordBatch | Table | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _ArrayOrChunkedArray | RecordBatch | Table | Expression: ...
def filter(
    input: _ArrayOrChunkedArray | RecordBatch | Table | Expression,
    selection_filter: Mask | Expression,
    /,
    null_selection_behavior: Literal["drop", "emit_null"] = "drop",
    *,
    options: FilterOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _ArrayOrChunkedArray | RecordBatch | Table | Expression: ...
def inverse_permutation(
    indices: _ArrayOrChunkedArray | Expression,
    /,
    max_index: int = -1,
    output_type: DataType | None = None,
    *,
    options: InversePermutationOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _IntegerArray | Expression: ...
def scatter(
    values: _ArrayOrChunkedArray | Expression,
    indices: _ArrayOrChunkedArray | Expression,
    /,
    max_index: int = -1,
    *,
    options: ScatterOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _ArrayOrChunkedArray | Expression: ...
def take(
    data: _ArrayOrChunkedArray | RecordBatch | Table,
    indices: Indices,
    *,
    boundscheck: bool = True,
    memory_pool: MemoryPool | None = None,
) -> _ArrayOrChunkedArray | RecordBatch | Table: ...

# ========================= 3.4 Containment tests  =========================

def indices_nonzero(
    values: _ArrayOrChunkedArray | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> UInt64Array | Expression: ...

# ========================= 3.5 Sorts and partitions  =========================
def array_sort_indices(
    array: _ArrayOrChunkedArray | Expression,
    /,
    order: Order = "ascending",
    *,
    null_placement: NullPlacement = "at_end",
    options: ArraySortOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> UInt64Array | Expression: ...
def partition_nth_indices(
    array: _ArrayOrChunkedArray | Iterable[Any] | Expression,
    /,
    pivot: int,
    *,
    null_placement: NullPlacement = "at_end",
    options: PartitionNthOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> UInt64Array | Expression: ...
def pivot_wider(
    pivot_keys: _ArrayOrChunkedArray | Sequence[str],
    pivot_values: _ArrayOrChunkedArray | Sequence[Any],
    /,
    key_names: Sequence[str],
    *,
    unexpected_key_behavior: Literal["ignore", "raise"] = "ignore",
    options: PivotWiderOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> StructScalar: ...
def rank(
    input: _ArrayOrChunkedArray,
    /,
    sort_keys: Order | Sequence[_SortKey] = "ascending",
    *,
    null_placement: NullPlacement | None = None,
    tiebreaker: Literal["min", "max", "first", "dense"] = "first",
    options: RankOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> UInt64Array: ...
def rank_quantile(
    input: _ArrayOrChunkedArray,
    /,
    sort_keys: Order | Sequence[_SortKey] = "ascending",
    *,
    null_placement: NullPlacement | None = None,
    options: RankQuantileOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> DoubleArray: ...
def rank_normal(
    input: _ArrayOrChunkedArray,
    /,
    sort_keys: Order | Sequence[_SortKey] = "ascending",
    *,
    null_placement: NullPlacement | None = None,
    options: RankQuantileOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> DoubleArray: ...
def select_k_unstable(
    input: _ArrayOrChunkedArray | RecordBatch | Table | Expression,
    /,
    k: int,
    sort_keys: Sequence[_SortKey],
    *,
    options: SelectKOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> UInt64Array | Expression: ...
def sort_indices(
    input: _ArrayOrChunkedArray | RecordBatch | Table | Expression,
    /,
    sort_keys: Sequence[_SortKey] = (),
    *,
    null_placement: NullPlacement | None = None,
    options: SortOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> UInt64Array | Expression: ...
def search_sorted(
    values: _ArrayOrChunkedArray | Expression,
    needles: _ScalarOrArray | ScalarLike | Expression,
    /,
    side: Literal["left", "right"] = "left",
    *,
    options: SearchSortedOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> UInt64Scalar | UInt64Array | ChunkedArray | Expression: ...

# ========================= 3.6 Structural transforms =========================
def list_element(
    lists: _ListScalar | _ArrayOrChunkedArray | Expression,
    index: int | _IntegerScalar | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _ScalarOrArray | Expression: ...
def list_flatten(
    lists: _ArrayOrChunkedArray | Expression,
    /,
    recursive: bool = False,
    *,
    options: ListFlattenOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _ArrayOrChunkedArray | Expression: ...
def list_parent_indices(
    lists: _ArrayOrChunkedArray | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> Int64Array | Expression: ...
def list_slice(
    lists: _ArrayOrChunkedArray | Expression,
    /,
    start: int,
    stop: int | None = None,
    step: int = 1,
    return_fixed_size_list: bool | None = None,
    *,
    options: ListSliceOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _ListArray | Expression: ...
def map_lookup(
    container: _ScalarOrArray | Expression,
    /,
    query_key: ScalarLike,
    occurrence: Literal["first", "last", "all"],
    *,
    options: MapLookupOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _ScalarOrArray | Expression: ...
def struct_field(
    values: StructScalar | _ArrayOrChunkedArray | Expression,
    /,
    indices: Sequence[str | bytes | int] | Expression | bytes | str | int,
    *,
    options: StructFieldOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _ScalarOrArray | Expression: ...
def fill_null_backward(
    values: _ScalarOrArray | ScalarLike | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _ScalarOrArray | ScalarLike | Expression: ...
def fill_null_forward(
    values: _ScalarOrArray | ScalarLike | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _ScalarOrArray | ScalarLike | Expression: ...
def replace_with_mask(
    values: _ScalarOrArray | Expression,
    mask: Mask,
    replacements: ArrayLike,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _ScalarOrArray | Expression: ...

# ========================= 3.7 Pairwise functions =========================
def pairwise_diff(
    input: _ArrayOrChunkedArray | Expression,
    /,
    period: int = 1,
    *,
    options: PairwiseOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> _NumericOrTemporalArray | Expression: ...
def run_end_encode(
    array: _ArrayOrChunkedArray | Expression,
    /,
    run_end_type: DataType | str = ...,
    *,
    options: RunEndEncodeOptions | None = None,
    memory_pool: MemoryPool | None = None,
) -> RunEndEncodedArray | ChunkedArray | Expression: ...
def run_end_decode(
    array: _ArrayOrChunkedArray | Expression,
    /,
    *,
    memory_pool: MemoryPool | None = None,
) -> _ArrayOrChunkedArray | Expression: ...

pairwise_diff_checked = _clone_signature(pairwise_diff)
