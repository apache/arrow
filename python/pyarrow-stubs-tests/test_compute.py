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

from collections.abc import Hashable
from decimal import Decimal
from types import EllipsisType
from typing import TYPE_CHECKING, TypeAlias

import pyarrow as pa
import pyarrow.compute as pc
from pyarrow.lib import Buffer

from _check import assert_type, check

_Filtered: TypeAlias = (
    pa.Array | pa.ChunkedArray | pa.RecordBatch | pa.Table | pc.Expression
)

_Quantile: TypeAlias = (
    pa.NumericArray
    | pa.Decimal32Array
    | pa.Decimal64Array
    | pa.Decimal128Array
    | pa.Decimal256Array
    | pc.Expression
)


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

    # Compute functions on an expression build an expression
    check(pc.count(field), pc.Expression)
    check(pc.mean(field), pc.Expression)

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

    check(pc.Expression.from_substrait(bytes(message)), pc.Expression)
    check(pc.Expression.from_substrait(pa.py_buffer(message)), pc.Expression)
    check(pc.Expression.from_substrait(Message()), pc.Expression)


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
        pc.SearchSortedOptions(side="middle")  # type: ignore[arg-type]
        _: Hashable = options  # type: ignore[assignment]  # ty: ignore[invalid-assignment]  # noqa: E501


def test_array_wise_functions() -> None:
    arr = pa.array([3, 1, 2])
    table = pa.table({"a": [3, 1, 2]})

    # Sorts and selections
    check(assert_type(pc.array_sort_indices(arr), pa.UInt64Array | pc.Expression),
          pa.UInt64Array)
    check(assert_type(pc.sort_indices(table, [("a", "descending")]),
                      pa.UInt64Array | pc.Expression),
          pa.UInt64Array)
    check(assert_type(pc.sort_indices(table, [(0, "descending")]),
                      pa.UInt64Array | pc.Expression),
          pa.UInt64Array)
    check(assert_type(pc.top_k_unstable(arr, 2), pa.UInt64Array), pa.UInt64Array)
    check(assert_type(pc.bottom_k_unstable(table, 2, ["a"], ["at_end"]),
                      pa.UInt64Array),
          pa.UInt64Array)
    check(assert_type(pc.indices_nonzero(arr), pa.UInt64Array | pc.Expression),
          pa.UInt64Array)
    check(assert_type(pc.value_counts(arr), pa.StructArray | pc.Expression),
          pa.StructArray)

    # Filters keep the input kind: table, array or expression
    mask = pa.array([True, False, True])
    check(assert_type(pc.filter(table, mask), _Filtered), pa.Table)
    check(assert_type(pc.drop_null(table), _Filtered), pa.Table)
    check(assert_type(pc.filter(pc.field("a"), pc.field("b")),
                      _Filtered),
          pc.Expression)

    # Python lists and tuples accepted
    check(assert_type(pc.partition_nth_indices([3, 1, 2], pivot=1),
                      pa.UInt64Array | pc.Expression),
          pa.UInt64Array)

    taken = pc.take(arr, [0, 2])
    check(assert_type(taken, pa.Array | pa.ChunkedArray | pa.RecordBatch | pa.Table),
          pa.Array)

    if TYPE_CHECKING:
        pc.top_k_unstable(arr, "2")  # type: ignore[arg-type]
        pc.sort_indices(table, [("a", "up")])  # type: ignore[list-item]
        pc.cumulative_sum(range(3))  # type: ignore[arg-type]
        # TODO: test invalid code once the scalar stubs are annotated
        # pc.variance(range(3)), pc.round("abc")


def test_element_wise_functions() -> None:
    arr = pa.array([3, 1, 2])

    check(assert_type(pc.random(3), pa.DoubleArray), pa.DoubleArray)

    # Arithmetic: Python literals, lists and tuples accepted
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

    # Bit-wise functions
    check(pc.bit_wise_and([1, 1], [1, 1]), pa.Int64Array)
    check(pc.bit_wise_not([1, 1]), pa.Int64Array)

    # Rounding returns floats, or decimals for decimal input
    check(pc.ceil(1.5), pa.DoubleScalar)
    check(pc.floor([1, 2]), pa.DoubleArray)
    check(pc.trunc(pa.scalar(Decimal("1.5"))), pa.Decimal128Scalar)
    check(pc.trunc(pa.array([Decimal("1.5")])), pa.Decimal128Array)

    # Logarithms return floats
    check(pc.ln(pa.array([Decimal("1.5")])), pa.DoubleArray)

    # Trigonometric functions return floats, also for decimal input
    # Python lists and tuples accepted
    check(pc.sin([1, 2]), pa.DoubleArray)
    check(pc.acos(0.5), pa.DoubleScalar)
    check(pc.atan2(1.5, [1, 2]), pa.DoubleArray)
    check(pc.cosh(pa.array([Decimal("1.5")])), pa.DoubleArray)

    # Python lists and tuples accepted
    check(pc.round((1.5, 2.5)), pa.Array)
    check(pc.round([1.5, 2.5]), pa.Array)
    check(pc.binary_repeat(pa.array(["a"]), (2,)), pa.Array)
    check(pc.binary_join_element_wise(["a", "b"], ["c", "d"], "-"), pa.Array)
    check(pc.struct_field(pa.array([{"a": 1}]), ("a",)), pa.Array)
    check(pc.acos([1.1, 1.1]), pa.DoubleArray)
    check(pc.atan2([1.1, 1.1], [1.1, 1.1]), pa.DoubleArray)

    # An Expression as the first argument gives an Expression
    selected = pc.if_else([True, False], [1, 2], [3, 4])
    check(assert_type(selected, pa.Scalar | pa.Array | pa.ChunkedArray | pc.Expression),
          pa.Array)
    check(pc.if_else(pc.field("a") > 1, 1, 2), pc.Expression)

    # TODO: test invalid code once the scalar stubs are annotated
    # pc.bit_wise_and(1.0, 1.0), pc.bit_wise_not(1.0)


def test_agg_functions() -> None:
    import numpy as np

    arr = pa.array([3, 1, 2])
    scalar = pa.scalar(1)

    # Scalar input accepted
    check(pc.count(scalar), pa.Int64Scalar)
    check(pc.count_distinct(scalar), pa.Int64Scalar)
    check(pc.first(scalar), pa.Int64Scalar)
    check(pc.last(scalar), pa.Int64Scalar)
    check(pc.first_last((1, 2)), pa.StructScalar)
    check(pc.first_last(scalar), pa.StructScalar)

    # Quantile returns doubles, or the input type for "lower", "higher", "nearest"
    check(assert_type(pc.quantile(arr, q=[0.25, 0.75]), _Quantile), pa.DoubleArray)
    check(assert_type(pc.quantile(arr, interpolation="lower"), _Quantile),
          pa.Int64Array)
    check(assert_type(pc.mode(arr, n=2), pa.StructArray | pc.Expression),
          pa.StructArray)

    # Python lists, tuples and NumPy arrays accepted
    check(pc.variance([1, 2, 3]), pa.DoubleScalar)
    check(pc.variance((1, 2, 3)), pa.DoubleScalar)
    check(pc.variance(np.array([1.0, 2.0, 3.0])), pa.DoubleScalar)

    # An Expression as input gives an Expression
    check(pc.mean(arr), pa.DoubleScalar)
    check(pc.mean(pc.field(0)), pc.Expression)
