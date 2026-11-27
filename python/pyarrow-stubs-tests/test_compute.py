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

from collections.abc import Hashable
from decimal import Decimal
from types import EllipsisType
from typing import TYPE_CHECKING, TypeAlias

import pyarrow as pa
import pyarrow.compute as pc
from pyarrow.lib import Buffer

from _check import assert_type, check

_Filtered: TypeAlias = pa.Array | pa.ChunkedArray | pa.RecordBatch | pa.Table

_Quantile: TypeAlias = (
    pa.NumericArray
    | pa.Decimal32Array
    | pa.Decimal64Array
    | pa.Decimal128Array
    | pa.Decimal256Array
)

_Numeric: TypeAlias = (
    pa.NumericArray
    | pa.Decimal32Array
    | pa.Decimal64Array
    | pa.Decimal128Array
    | pa.Decimal256Array
    | pa.ChunkedArray
)

_NumericOrTemporal: TypeAlias = _Quantile | pa.MonthDayNanoIntervalArray


def test_expression() -> None:
    # Field references and literals
    field = pc.field("a")
    check(assert_type(field, pc.Expression), pc.Expression)
    check(assert_type(pc.field("a", "b"), pc.Expression), pc.Expression)
    check(assert_type(pc.field(("a", 0)), pc.Expression), pc.Expression)
    check(assert_type(pc.scalar(1), pc.Expression), pc.Expression)

    # Operators and methods build expressions
    check(assert_type(field > 1, pc.Expression), pc.Expression)
    check(assert_type(field == 1, pc.Expression), pc.Expression)
    check(assert_type(field != 1, pc.Expression), pc.Expression)
    check(assert_type((field > 1) & (field < 3), pc.Expression), pc.Expression)
    check(assert_type(~(field > 1), pc.Expression), pc.Expression)
    check(assert_type(field + 1, pc.Expression), pc.Expression)
    check(assert_type(field / 2, pc.Expression), pc.Expression)
    check(assert_type(field.isin([1, 2]), pc.Expression), pc.Expression)
    check(assert_type(field.is_null(nan_is_null=True), pc.Expression),
          pc.Expression)
    check(assert_type(field.cast(pa.int64()), pc.Expression), pc.Expression)
    check(assert_type(field.cast("int64", safe=False), pc.Expression),
          pc.Expression)
    check(assert_type(field.equals(pc.field("a")), bool), bool)

    # Element-wise compute functions on an expression build an expression
    check(pc.abs(field), pc.Expression)
    check(pc.utf8_upper(field), pc.Expression)

    if TYPE_CHECKING:
        # Expressions define __eq__ and are unhashable.
        _: Hashable = field  # type: ignore[assignment]  # ty: ignore[invalid-assignment]  # noqa: E501
        field.cast(1)  # type: ignore[arg-type]
        pc.field(1.0)  # type: ignore[arg-type]


def test_expression_substrait() -> None:
    schema = pa.schema([("a", pa.int64())])
    message = pc.field("a").to_substrait(schema)
    check(assert_type(message, memoryview), memoryview)

    expr = pc.Expression.from_substrait(message)
    check(assert_type(expr, pc.Expression), pc.Expression)

    class Message:
        def SerializeToString(self) -> bytes:
            return bytes(message)

    check(assert_type(pc.Expression.from_substrait(bytes(message)), pc.Expression),
          pc.Expression)
    from_buffer = pc.Expression.from_substrait(pa.py_buffer(message))
    check(assert_type(from_buffer, pc.Expression), pc.Expression)
    check(assert_type(pc.Expression.from_substrait(Message()), pc.Expression),
          pc.Expression)


def test_functions() -> None:
    func = pc.get_function("sum")
    check(assert_type(func, pc.Function), pc.Function)
    check(assert_type(func.name, str), str)
    check(assert_type(func.arity, int | EllipsisType), int)
    check(assert_type(func.num_kernels, int), int)
    check(assert_type(pc.list_functions(), list[str]), list)
    check(assert_type(pc.function_registry(), pc.FunctionRegistry),
          pc.FunctionRegistry)

    func = pc.get_function("add")
    assert isinstance(func, pc.ScalarFunction)
    check(assert_type(func.kernels, list[pc.ScalarKernel]), list)
    check(assert_type(func.kernels[0], pc.ScalarKernel), pc.ScalarKernel)


def test_options() -> None:
    # Cast options
    check(assert_type(pc.CastOptions.safe(pa.int64()), pc.CastOptions),
          pc.CastOptions)
    check(assert_type(pc.CastOptions.safe().is_safe(), bool), bool)

    check(assert_type(pc.ScalarAggregateOptions(skip_nulls=False),
                      pc.ScalarAggregateOptions),
          pc.ScalarAggregateOptions)

    # Sort keys refer to a column by name or index
    check(assert_type(pc.SortOptions([("a", "ascending", "at_start")]),
                      pc.SortOptions),
          pc.SortOptions)
    check(assert_type(pc.SortOptions([(0, "ascending", "at_start")]),
                      pc.SortOptions),
          pc.SortOptions)
    check(assert_type(pc.SelectKOptions(1, [("a", "ascending", "at_end")]),
                      pc.SelectKOptions),
          pc.SelectKOptions)

    # Struct fields: metadata may be None, fields by name or index
    md = pa.KeyValueMetadata({"k": "v"})
    check(assert_type(pc.MakeStructOptions(["a", "b"], field_metadata=[None, md]),
                      pc.MakeStructOptions),
          pc.MakeStructOptions)
    check(assert_type(pc.StructFieldOptions((0, 1)), pc.StructFieldOptions),
          pc.StructFieldOptions)
    check(assert_type(pc.StructFieldOptions(["a", "b"]), pc.StructFieldOptions),
          pc.StructFieldOptions)

    # Other parameter types: ChunkedArray value set, hashable seed, Literals
    check(assert_type(pc.SetLookupOptions(pa.chunked_array([[1, 2]])),
                      pc.SetLookupOptions),
          pc.SetLookupOptions)
    check(assert_type(pc.RandomOptions(initializer="seed"), pc.RandomOptions),
          pc.RandomOptions)
    check(assert_type(pc.SearchSortedOptions(side="right"),
                      pc.SearchSortedOptions),
          pc.SearchSortedOptions)

    # Options can be serialized
    options = pc.RoundOptions(2, round_mode="half_up")
    check(assert_type(options.serialize(), Buffer), Buffer)

    if TYPE_CHECKING:
        pc.RoundOptions(2, round_mode="nearest")  # type: ignore[arg-type]
        # TODO: test invalid code once the scalar stubs are annotated
        # pc.SetLookupOptions(pa.scalar(2))
        pc.SearchSortedOptions(side="middle")  # type: ignore[arg-type]
        _: Hashable = options  # type: ignore[assignment]  # ty: ignore[invalid-assignment]  # noqa: E501


def test_array_wise_functions() -> None:
    arr = pa.array([3, 1, 2])
    table = pa.table({"a": [3, 1, 2]})

    # Sorts and selections
    check(assert_type(pc.array_sort_indices(arr), pa.UInt64Array | pa.ChunkedArray),
          pa.UInt64Array)
    check(assert_type(pc.array_sort_indices(pa.chunked_array([[3, 1], [2]])),
                      pa.UInt64Array | pa.ChunkedArray),
          pa.ChunkedArray)
    check(assert_type(pc.sort_indices(table, [("a", "descending")]),
                      pa.UInt64Array),
          pa.UInt64Array)
    check(assert_type(pc.sort_indices(table, [(0, "descending")]),
                      pa.UInt64Array),
          pa.UInt64Array)
    check(assert_type(pc.top_k_unstable(arr, 2), pa.UInt64Array), pa.UInt64Array)
    check(assert_type(pc.bottom_k_unstable(table, 2, ["a"], ["at_end"]),
                      pa.UInt64Array),
          pa.UInt64Array)
    check(assert_type(pc.indices_nonzero(arr), pa.UInt64Array),
          pa.UInt64Array)
    check(assert_type(pc.value_counts(arr), pa.StructArray),
          pa.StructArray)

    # Filters keep the input kind: table or array
    mask = pa.array([True, False, True])
    check(assert_type(pc.filter(table, mask), _Filtered), pa.Table)
    check(assert_type(pc.drop_null(table), _Filtered), pa.Table)

    # Python lists and tuples accepted
    check(assert_type(pc.winsorize([10, 4, 0], 0.1, 0.8), _Numeric), pa.Array)
    dictionary = pc.dictionary_encode(["a", "b", "a"])
    check(assert_type(dictionary, pa.DictionaryArray | pa.ChunkedArray),
          pa.DictionaryArray)
    check(assert_type(pc.dictionary_encode(pa.scalar("a")),
                      pa.DictionaryArray | pa.ChunkedArray),
          pa.DictionaryArray)
    check(assert_type(pc.dictionary_decode(dictionary), pa.Array | pa.ChunkedArray),
          pa.StringArray)
    check(assert_type(pc.unique([1, 2, 1]), pa.Array), pa.Int64Array)
    check(assert_type(pc.unique(pa.scalar("b")), pa.Array), pa.StringArray)
    check(assert_type(pc.unique(pa.chunked_array([[1], [2]])), pa.Array), pa.Int64Array)
    check(assert_type(pc.array_filter([1, 2, 3], [True, False, False]),
                      pa.Array | pa.ChunkedArray), pa.Int64Array)
    check(assert_type(pc.inverse_permutation([1, 2, 0]),
                      pa.IntegerArray | pa.ChunkedArray), pa.Int64Array)
    check(assert_type(pc.scatter([True, True, False, True], [0, 1, 3, 2]),
                      pa.Array | pa.ChunkedArray), pa.BooleanArray)
    taken = pc.take(arr, [0, 2])
    check(assert_type(taken, pa.Array | pa.ChunkedArray | pa.RecordBatch | pa.Table),
          pa.Array)
    check(assert_type(pc.partition_nth_indices([3, 1, 2], pivot=1), pa.UInt64Array),
          pa.UInt64Array)
    check(assert_type(pc.search_sorted([1, 2, 3], [0, 1, 2]),
                      pa.UInt64Scalar | pa.UInt64Array),
          pa.UInt64Array)
    # Chunked needles give a plain array
    check(assert_type(pc.search_sorted([1, 2, 3], pa.chunked_array([[0], [2]])),
                      pa.UInt64Scalar | pa.UInt64Array),
          pa.UInt64Array)

    # Containment tests
    check(assert_type(pc.indices_nonzero(1), pa.UInt64Array), pa.UInt64Array)

    # Structural transforms
    check(pc.list_contains([[1, 2, 3]], 1), pa.BooleanArray)
    check(pc.list_contains(pa.chunked_array([[[1, 2, 3]]]), 1), pa.ChunkedArray)
    check(pc.list_element([[1, 2, 3]], 1), pa.Int64Array)
    check(assert_type(pc.list_flatten([[1, 2], [3]]), pa.Array | pa.ChunkedArray),
          pa.Int64Array)
    check(assert_type(pc.list_flatten(pa.scalar([1, 2])), pa.Array | pa.ChunkedArray),
          pa.Int64Array)
    check(assert_type(pc.list_parent_indices(pa.chunked_array([[[1, 2], [3]]])),
                      pa.Int64Array | pa.ChunkedArray),
          pa.ChunkedArray)
    check(pc.struct_field(pa.array([{"a": 1}]), ("a",)), pa.Array)

    # Replace functions always return arrays
    check(assert_type(pc.fill_null_forward([1, None]), pa.Array | pa.ChunkedArray),
          pa.Int64Array)
    check(assert_type(pc.fill_null_backward([None, 1]), pa.Array | pa.ChunkedArray),
          pa.Int64Array)
    # Replacements as an array, a scalar or a Python list
    check(assert_type(pc.replace_with_mask(arr, mask, pa.array([10, 30])),
                      pa.Array | pa.ChunkedArray),
          pa.Int64Array)
    check(pc.replace_with_mask(arr, mask, pa.scalar(10)), pa.Int64Array)
    check(pc.replace_with_mask(arr, mask, [10, 30]), pa.Int64Array)

    # Pairwise functions
    check(assert_type(pc.pairwise_diff([1, 3, 6]), _NumericOrTemporal), pa.Int64Array)

    if TYPE_CHECKING:
        pc.top_k_unstable(arr, "2")  # type: ignore[arg-type]
        pc.top_k_unstable([3, 1, 2], 2)  # type: ignore[arg-type]
        pc.sort_indices(table, [("a", "up")])  # type: ignore[list-item]
        # TODO(GH-55775):
        # pc.partition_nth_indices(
        #     pa.chunked_array([[3, 1], [2]]), pivot=1)
        # Only lists, tuples and NumPy arrays are converted to arrays (_ArrayLike)
        pc.cumulative_sum(range(3))  # type: ignore[arg-type]
        # Vector and meta functions can't be evaluated as expressions, e.g.
        # ds.dataset(table).to_table(columns={"r": ...}) fails for them
        pc.filter(pc.field("a"), pc.field("b"))  # type: ignore[arg-type]
        pc.pairwise_diff(pa.chunked_array([[1, 3, 6]]))  # type: ignore[arg-type]
        # TODO: test invalid code once the scalar stubs are annotated
        # pc.variance(range(3)), pc.round("abc")


def test_element_wise_functions() -> None:
    import numpy as np
    arr = pa.array([3, 1, 2])

    check(assert_type(pc.random(3), pa.DoubleArray), pa.DoubleArray)

    # Python literals, lists and tuples accepted
    check(pc.abs([1, 2]), pa.Int64Array)
    check(pc.abs((1.5, -2.5)), pa.DoubleArray)
    check(pc.abs(1), pa.Int64Scalar)
    check(pc.abs_checked(-2.5), pa.DoubleScalar)
    check(pc.add(-2.5, 1.5), pa.DoubleScalar)
    check(pc.add_checked([2.5, 3.5], 1), pa.DoubleArray)
    check(pc.exp(2.5), pa.DoubleScalar)
    check(pc.exp([1, 2]), pa.DoubleArray)
    check(pc.divide([1, 2, 3], 1), pa.Int64Array)
    check(pc.divide(arr, 2.5), pa.Array)
    check(pc.hypot([1, 2, 3], 1), pa.DoubleArray)
    check(pc.modulo([7, -7], [3, 3]), pa.Int64Array)
    check(pc.power(arr, 2), pa.Array)
    check(pc.sqrt(pa.chunked_array([[1], [2, 3]])), pa.ChunkedArray)
    check(pc.is_valid([1.1]), pa.BooleanArray)
    check(pc.true_unless_null(1.1), pa.BooleanScalar)

    # decimal, datetime and numpy also accepted
    check(pc.sqrt(Decimal("2.25")), pa.DoubleScalar)
    check(pc.add(dt.datetime(2026, 1, 1), dt.timedelta(days=1)), pa.TimestampScalar)
    check(pc.add(arr, np.int64(1)), pa.Int64Array)

    # Bit-wise functions
    check(pc.bit_wise_and([1, 1], [1, 1]), pa.Int64Array)
    check(pc.bit_wise_not([1, 1]), pa.Int64Array)

    # Rounding returns floats, or decimals for decimal input
    check(pc.ceil(1.5), pa.DoubleScalar)
    check(pc.floor([1, 2]), pa.DoubleArray)
    check(pc.trunc(pa.scalar(Decimal("1.5"))), pa.Decimal128Scalar)
    check(pc.trunc(pa.array([Decimal("1.5")])), pa.Decimal128Array)
    check(pc.round((1.5, 2.5)), pa.Array)
    check(pc.round([1.5, 2.5]), pa.Array)

    # Logarithms return floats
    check(pc.ln(pa.array([Decimal("1.5")])), pa.DoubleArray)

    # Trigonometric functions return floats, also for decimal input
    # Python lists and tuples accepted
    check(pc.sin([1, 2]), pa.DoubleArray)
    check(pc.acos(0.5), pa.DoubleScalar)
    check(pc.atan2(1.5, [1, 2]), pa.DoubleArray)
    check(pc.cosh(pa.array([Decimal("1.5")])), pa.DoubleArray)
    check(pc.acos([1.1, 1.1]), pa.DoubleArray)
    check(pc.atan2([1.1, 1.1], [1.1, 1.1]), pa.DoubleArray)

    # Comparisons and selections accept Python literals of any type
    check(pc.equal(pa.scalar("a"), "a"), pa.BooleanScalar)
    check(pc.equal(True, False), pa.BooleanScalar)
    check(pc.max_element_wise(pa.array(["a", "b"]), "c"), pa.StringArray)
    check(pc.less(dt.datetime(2026, 1, 1), dt.datetime(2026, 1, 2)), pa.BooleanScalar)
    check(pc.greater_equal(1.1, 2.2), pa.BooleanScalar)
    check(pc.if_else(True, 1, 2), pa.Int64Scalar)
    selected = pc.if_else([True, False], [1, 2], [3, 4])
    check(assert_type(selected, pa.Scalar | pa.Array | pa.ChunkedArray | pc.Expression),
          pa.Array)
    # An Expression as the first argument gives an Expression
    check(pc.if_else(pc.field("a") > 1, 1, 2), pc.Expression)
    check(pc.if_else(np.bool_(True), 1, 2), pa.Int64Scalar)
    check(pc.choose(1, [1, 2], [3, 4]), pa.Int64Array)
    check(pc.fill_null(pa.array([1, None]), 5), pa.Int64Array)
    check(pc.equal([1, 2, 1], [1, 2, 3]), pa.BooleanArray)

    check(pc.make_struct(1, 'a', field_nullability=[True]), pa.StructScalar)
    check(pc.make_struct(1, 'a', field_metadata=[None]), pa.StructScalar)
    cond = pc.make_struct([True, False], [False, None])
    check(pc.case_when(cond, [1, 2], [12, 13]), pa.Int64Array)
    cond = pa.scalar({"a": False, "b": True})
    check(pc.case_when(cond, ["foo"], ["bar"]), pa.StringArray)
    check(pc.case_when(cond, 1, 2), pa.Int64Scalar)
    check(pc.case_when(cond, "a", "b"), pa.StringScalar)
    check(pc.case_when([{"a": True, "b": False}], [1.1], [3.3]), pa.DoubleArray)
    check(pc.choose([1, 2, 1], "a", "b", "c"), pa.StringArray)

    # Logical functions
    check(pc.and_not([True], [False]), pa.BooleanArray)

    # String functions
    check(pc.utf8_is_space([" "]), pa.BooleanArray)
    check(pc.binary_repeat(pa.array(["a"]), (2,)), pa.Array)
    check(pc.binary_repeat(b"foozfoo", [3]), pa.BinaryArray)
    check(pc.replace_substring(b"foozfoo", "foo", "bar"), pa.BinaryScalar)
    check(pc.ascii_ltrim(["foozfoo"], b"foo"), pa.StringArray)
    check(pc.split_pattern([b"foozfoo"], b"t"), pa.ListArray)
    check(pc.extract_regex(b"a1", rb"(?P<letter>[ab])(?P<digit>\d)"),
          pa.StructScalar)
    check(pc.binary_join([["a", "b"], ["c"]], "-"), pa.StringArray)
    check(pc.binary_join(pc.field("a"), pc.field("sep")), pc.Expression)
    check(pc.binary_join_element_wise(["a", "b"], ["c", "d"], "-"), pa.Array)

    # Containment tests
    check(pc.count_substring([b"abcab"], "ab"), pa.Int32Array)
    check(pc.ends_with([b"abcab"], "ab"), pa.BooleanArray)
    check(pc.index_in([1, 2, 3], pa.array([1, 2])), pa.Int32Array)
    check(pc.is_in([1, 2, 3], pa.array([1, 2])), pa.BooleanArray)
    check(pc.index_in_meta_binary([1, 2, 3], [1, 2]), pa.Int32Array)
    check(pc.is_in_meta_binary([1, 2, 3], [1, 2]), pa.BooleanArray)

    # Structural transforms
    check(pc.list_value_length([[1], [1, 2]]), pa.Int32Array)

    # Conversions
    check(pc.ceil_temporal([dt.datetime(2026, 11, 23, 10, 17)]), pa.TimestampArray)
    check(pc.cast(1, pa.float32()), pa.FloatScalar)
    check(pc.strftime(dt.datetime(2026, 11, 23), format="%Y-%m-%d"), pa.StringScalar)
    check(pc.strftime(pc.field("ts"), format=b"%Y-%m-%d"), pc.Expression)
    check(pc.strptime(["5/1/2020", None, "12/13/1900"], format='%m/%d/%Y', unit='s'),
          pa.TimestampArray)
    check(pc.strptime("5/1/2020", format=b'%m/%d/%Y', unit='s'), pa.TimestampScalar)
    ree = pc.run_end_encode([1, 1, None, None, None, 2, 2, 3])
    check(assert_type(ree, pa.RunEndEncodedArray | pa.ChunkedArray),
          pa.RunEndEncodedArray)
    check(assert_type(pc.run_end_decode(ree), pa.Array | pa.ChunkedArray), pa.Array)

    # Temporal
    date = dt.datetime(2026, 1, 1)
    check(pc.day(date), pa.Int64Scalar)
    check(pc.day([date]), pa.Int64Array)
    check(pc.hour(date), pa.Int64Scalar)
    check(pc.subsecond(date), pa.DoubleScalar)
    check(pc.is_leap_year((date,)), pa.BooleanArray)
    check(pc.days_between(date, date), pa.Int64Scalar)
    check(pc.month_day_nano_interval_between([date], [date]),
          pa.MonthDayNanoIntervalArray)
    check(pc.years_between((date, ), (date, )), pa.Int64Array)
    check(pc.assume_timezone([date], timezone=b"UTC"), pa.TimestampArray)
    check(pc.local_timestamp(date), pa.TimestampScalar)

    if TYPE_CHECKING:
        pc.is_in([1, 2, 3], pc.field("a"))  # type: ignore[arg-type]
        pc.random(3.14)  # type: ignore[arg-type]
        pc.make_struct(1, 'a', field_nullability=True)  # type: ignore[arg-type]
        # Fails when evaluated (ds.dataset(table).to_table(columns={"r": ...})):
        pc.run_end_encode(pc.field("a"))  # type: ignore[arg-type]
        # TODO: test invalid code once the scalar stubs are annotated
        # pc.bit_wise_and(1.0, 1.0), pc.bit_wise_not(1.0)
        # pc.add("a", 1), pc.sqrt(dt.timedelta(days=1)), pc.if_else("yes", 1, 2)
        # pc.index_in([1, 2, 3], options=pc.SetLookupOptions(pa.scalar(2)))
        # pc.cast(pa.table({"a": [1]}), pa.float32()), pc.cast(pc.field("a"), "int64")


def test_agg_functions() -> None:
    import numpy as np

    arr = pa.array([3, 1, 2])
    scalar = pa.scalar(1)

    # Scalar input accepted
    check(pc.count(scalar), pa.Int64Scalar)
    check(pc.count_distinct(scalar), pa.Int64Scalar)
    check(pc.first(scalar), pa.Int64Scalar)
    check(pc.last(scalar), pa.Int64Scalar)
    check(pc.first_last(scalar), pa.StructScalar)

    # Aggregations return no expression
    check(assert_type(pc.mode(arr, n=2), pa.StructArray), pa.StructArray)

    # Quantile returns doubles, or the input type for "lower", "higher", "nearest"
    check(assert_type(pc.quantile(arr, q=[0.25, 0.75]), _Quantile), pa.DoubleArray)
    check(assert_type(pc.quantile(arr, interpolation="lower"), _Quantile),
          pa.Int64Array)

    # Python lists, tuples and NumPy arrays accepted
    check(pc.variance([1, 2, 3]), pa.DoubleScalar)
    check(pc.variance((1, 2, 3)), pa.DoubleScalar)
    check(pc.variance(np.array([1.0, 2.0, 3.0])), pa.DoubleScalar)
    check(pc.first_last((1, 2)), pa.StructScalar)

    # A Python value accepted
    check(pc.index(arr, 2), pa.Int64Scalar)

    # TODO: test invalid code once the scalar stubs are annotated
    # Aggregations can't be evaluated as expressions, e.g. evaluating
    # pc.sum(pc.field("a")) in ds.dataset(table).to_table(columns=...) fails:
    # pc.sum(pc.field("a"))
