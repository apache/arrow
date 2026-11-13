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
from types import EllipsisType
from typing import TYPE_CHECKING, TypeAlias

import pyarrow as pa
import pyarrow.compute as pc
from pyarrow.lib import Buffer

from _check import assert_type, check

_Filtered: TypeAlias = (
    pa.Array | pa.ChunkedArray | pa.RecordBatch | pa.Table | pc.Expression
)


def test_expression() -> None:
    field = pc.field("a")
    check(assert_type(field, pc.Expression), pc.Expression)
    check(assert_type(pc.field("a", "b"), pc.Expression), pc.Expression)
    check(assert_type(pc.field(("a", 0)), pc.Expression), pc.Expression)
    check(assert_type(pc.scalar(1), pc.Expression), pc.Expression)

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

    # Calling a compute function with an expression builds an expression.
    check(pc.count(field), pc.Expression)
    check(pc.mean(field), pc.Expression)

    if TYPE_CHECKING:
        # Expressions define __eq__ and are unhashable.
        _: Hashable = field  # type: ignore[assignment]  # ty: ignore[invalid-assignment]  # noqa: E501
        field.cast(1)  # type: ignore[arg-type]
        pc.field(1.0)  # type: ignore[arg-type]


def test_functions() -> None:
    func = pc.get_function("sum")
    check(assert_type(func, pc.Function), pc.Function)
    check(assert_type(func.name, str), str)
    check(assert_type(func.arity, int | EllipsisType), int)
    check(assert_type(func.num_kernels, int), int)
    check(assert_type(pc.list_functions(), list[str]), list)
    check(assert_type(pc.function_registry(), pc.FunctionRegistry),
          pc.FunctionRegistry)


def test_options() -> None:
    check(assert_type(pc.CastOptions.safe(pa.int64()), pc.CastOptions),
          pc.CastOptions)
    check(assert_type(pc.CastOptions.safe().is_safe(), bool), bool)
    check(assert_type(pc.ScalarAggregateOptions(skip_nulls=False),
                      pc.ScalarAggregateOptions),
          pc.ScalarAggregateOptions)
    check(assert_type(pc.SortOptions([("a", "ascending", "at_start")]),
                      pc.SortOptions),
          pc.SortOptions)
    check(assert_type(pc.SelectKOptions(1, [("a", "ascending", "at_end")]),
                      pc.SelectKOptions),
          pc.SelectKOptions)
    check(assert_type(pc.SetLookupOptions(pa.chunked_array([[1, 2]])),
                      pc.SetLookupOptions),
          pc.SetLookupOptions)
    check(assert_type(pc.RandomOptions(initializer="seed"), pc.RandomOptions),
          pc.RandomOptions)
    check(assert_type(pc.SearchSortedOptions(side="right"),
                      pc.SearchSortedOptions),
          pc.SearchSortedOptions)
    options = pc.RoundOptions(2, round_mode="half_up")
    check(assert_type(options.serialize(), Buffer), Buffer)

    if TYPE_CHECKING:
        pc.RoundOptions(2, round_mode="nearest")  # type: ignore[arg-type]
        pc.SearchSortedOptions(side="middle")  # type: ignore[arg-type]
        _: Hashable = options  # type: ignore[assignment]  # ty: ignore[invalid-assignment]  # noqa: E501


def test_array_functions() -> None:
    arr = pa.array([3, 1, 2])
    table = pa.table({"a": [3, 1, 2]})

    check(assert_type(pc.array_sort_indices(arr), pa.UInt64Array | pc.Expression),
          pa.UInt64Array)
    check(assert_type(pc.sort_indices(table, [("a", "descending")]),
                      pa.UInt64Array | pc.Expression),
          pa.UInt64Array)
    check(assert_type(pc.top_k_unstable(arr, 2), pa.UInt64Array), pa.UInt64Array)
    check(assert_type(pc.bottom_k_unstable(table, 2, ["a"], ["at_end"]),
                      pa.UInt64Array),
          pa.UInt64Array)
    check(assert_type(pc.indices_nonzero(arr), pa.UInt64Array | pc.Expression),
          pa.UInt64Array)
    check(assert_type(pc.quantile(arr, q=[0.25, 0.75]),
                      pa.DoubleArray | pc.Expression),
          pa.DoubleArray)
    check(assert_type(pc.mode(arr, n=2), pa.StructArray | pc.Expression),
          pa.StructArray)
    check(assert_type(pc.value_counts(arr), pa.StructArray | pc.Expression),
          pa.StructArray)
    check(assert_type(pc.random(3), pa.DoubleArray), pa.DoubleArray)

    mask = pa.array([True, False, True])
    check(assert_type(pc.filter(table, mask), _Filtered), pa.Table)
    check(assert_type(pc.drop_null(table), _Filtered), pa.Table)
    check(assert_type(pc.filter(pc.field("a"), pc.field("b")),
                      _Filtered),
          pc.Expression)

    # Python lists and other iterables are accepted where the stubs list them.
    check(pc.round((1.5, 2.5)), pa.Array)
    check(pc.first_last((1, 2)), pa.StructScalar)
    check(pc.binary_repeat(pa.array(["a"]), (2,)), pa.Array)
    check(pc.struct_field(pa.array([{"a": 1}]), ("a",)), pa.Array)
    check(assert_type(pc.partition_nth_indices([3, 1, 2], pivot=1),
                      pa.UInt64Array | pc.Expression),
          pa.UInt64Array)
    check(pc.round([1.5, 2.5]), pa.Array)
    check(pc.binary_join_element_wise(["a", "b"], ["c", "d"], "-"), pa.Array)

    # Python literals are accepted next to arrays.
    check(pc.power(arr, 2), pa.Array)
    check(pc.divide(arr, 2.0), pa.Array)

    taken = pc.take(arr, [0, 2])
    check(assert_type(taken, pa.Array | pa.ChunkedArray | pa.RecordBatch | pa.Table),
          pa.Array)

    if TYPE_CHECKING:
        pc.top_k_unstable(arr, "2")  # type: ignore[arg-type]
        pc.sort_indices(table, [("a", "up")])  # type: ignore[list-item]
