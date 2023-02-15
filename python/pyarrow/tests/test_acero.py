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

import pytest

import pyarrow as pa
import pyarrow.compute as pc
from pyarrow.compute import field

from pyarrow._acero import (
    TableSourceNodeOptions,
    FilterNodeOptions,
    ProjectNodeOptions,
    AggregateNodeOptions,
    HashJoinNodeOptions,
    Declaration,
)


@pytest.fixture
def table_source():
    table = pa.table({'a': [1, 2, 3], 'b': [4, 5, 6]})
    table_opts = TableSourceNodeOptions(table)
    table_source = Declaration("table_source", options=table_opts)
    return table_source


def test_declaration():

    table = pa.table({'a': [1, 2, 3], 'b': [4, 5, 6]})
    table_opts = TableSourceNodeOptions(table)
    filter_opts = FilterNodeOptions(field('a') > 1)

    # using sequence
    decl = Declaration.from_sequence([
        Declaration("table_source", options=table_opts),
        Declaration("filter", options=filter_opts)
    ])
    result = decl.to_table()
    assert result.equals(table.slice(1, 2))

    # using explicit inputs
    table_source = Declaration("table_source", options=table_opts)
    filtered = Declaration("filter", options=filter_opts, inputs=[table_source])
    result = filtered.to_table()
    assert result.equals(table.slice(1, 2))


def test_declaration_repr(table_source):

    assert "TableSourceNode" in str(table_source)
    assert "TableSourceNode" in repr(table_source)


def test_table_source():
    with pytest.raises(TypeError):
        TableSourceNodeOptions(pa.record_batch([pa.array([1, 2, 3])], ["a"]))

    table_source = TableSourceNodeOptions(None)
    decl = Declaration("table_source", table_source)
    with pytest.raises(
        ValueError, match="TableSourceNode requires table which is not null"
    ):
        _ = decl.to_table()


def test_filter(table_source):
    # referencing unknown field
    decl = Declaration.from_sequence([
        table_source,
        Declaration("filter", options=FilterNodeOptions(field("c") > 1))
    ])
    with pytest.raises(ValueError, match=r"No match for FieldRef.Name\(c\)"):
        _ = decl.to_table()

    # requires a pyarrow Expression
    with pytest.raises(TypeError):
        FilterNodeOptions(pa.array([True, False, True]))
    with pytest.raises(TypeError):
        FilterNodeOptions(None)


def test_project(table_source):
    # default name from expression
    decl = Declaration.from_sequence([
        table_source,
        Declaration("project", ProjectNodeOptions([pc.multiply(field("a"), 2)]))
    ])
    result = decl.to_table()
    assert result.schema.names == ["multiply(a, 2)"]
    assert result[0].to_pylist() == [2, 4, 6]

    # provide name
    decl = Declaration.from_sequence([
        table_source,
        Declaration("project", ProjectNodeOptions([pc.multiply(field("a"), 2)], ["a2"]))
    ])
    result = decl.to_table()
    assert result.schema.names == ["a2"]
    assert result["a2"].to_pylist() == [2, 4, 6]

    # input validation
    with pytest.raises(ValueError):
        ProjectNodeOptions([pc.multiply(field("a"), 2)], ["a2", "b2"])


def test_aggregate_scalar(table_source):
    decl = Declaration.from_sequence([
        table_source,
        Declaration("aggregate", AggregateNodeOptions([("a", "sum", None, "a_sum")]))
    ])
    result = decl.to_table()
    assert result.schema.names == ["a_sum"]
    assert result["a_sum"].to_pylist() == [6]


def test_aggregate_hash():
    table = pa.table({'a': [1, 2, None], 'b': ["foo", "bar", "foo"]})
    table_opts = TableSourceNodeOptions(table)
    table_source = Declaration("table_source", options=table_opts)

    # default options
    aggr_opts = AggregateNodeOptions(
        [("a", "hash_count", None, "count(a)")], keys=["b"])
    decl = Declaration.from_sequence([
        table_source, Declaration("aggregate", aggr_opts)
    ])
    result = decl.to_table()
    expected = pa.table({"count(a)": [1, 1], "b": ["foo", "bar"]})
    assert result.equals(expected)

    # specify function options
    aggr_opts = AggregateNodeOptions(
        [("a", "hash_count", pc.CountOptions("all"), "count(a)")], keys=["b"]
    )
    decl = Declaration.from_sequence([
        table_source, Declaration("aggregate", aggr_opts)
    ])
    result = decl.to_table()
    expected = pa.table({"count(a)": [2, 1], "b": ["foo", "bar"]})
    assert result.equals(expected)

    # wrong type of (aggregation) function
    # TODO test with kernel that matches number of arguments (arity) -> avoid segfault
    aggr_opts = AggregateNodeOptions([("a", "sum", None, "a_sum")], keys=["b"])
    decl = Declaration.from_sequence([
        table_source, Declaration("aggregate", aggr_opts)
    ])
    with pytest.raises(ValueError):
        _ = decl.to_table()


def test_hash_join():
    left = pa.table({'key': [1, 2, 3], 'a': [4, 5, 6]})
    left_source = Declaration("table_source", options=TableSourceNodeOptions(left))
    right = pa.table({'key': [2, 3, 4], 'b': [4, 5, 6]})
    right_source = Declaration("table_source", options=TableSourceNodeOptions(right))

    # inner join
    join_opts = HashJoinNodeOptions("inner", left_keys="key", right_keys="key")
    joined = Declaration(
        "hashjoin", options=join_opts, inputs=[left_source, right_source])
    result = joined.to_table()
    expected = pa.table(
        [[2, 3], [5, 6], [2, 3], [4, 5]],
        names=["key", "a", "key", "b"])
    assert result.equals(expected)

    # left join
    join_opts = HashJoinNodeOptions(
        "left outer", left_keys="key", right_keys="key")
    joined = Declaration(
        "hashjoin", options=join_opts, inputs=[left_source, right_source])
    result = joined.to_table()
    expected = pa.table(
        [[1, 2, 3], [4, 5, 6], [None, 2, 3], [None, 4, 5]],
        names=["key", "a", "key", "b"]
    )
    assert result.sort_by("a").equals(expected)

    # suffixes
    join_opts = HashJoinNodeOptions(
        "left outer", left_keys="key", right_keys="key",
        output_suffix_for_left="_left", output_suffix_for_right="_right")
    joined = Declaration(
        "hashjoin", options=join_opts, inputs=[left_source, right_source])
    result = joined.to_table()
    expected = pa.table(
        [[1, 2, 3], [4, 5, 6], [None, 2, 3], [None, 4, 5]],
        names=["key_left", "a", "key_right", "b"]
    )
    assert result.sort_by("a").equals(expected)

    # manually specifying output columns
    join_opts = HashJoinNodeOptions(
        "left outer", left_keys="key", right_keys="key",
        left_output=["key", "a"], right_output=["b"])
    joined = Declaration(
        "hashjoin", options=join_opts, inputs=[left_source, right_source])
    result = joined.to_table()
    expected = pa.table(
        [[1, 2, 3], [4, 5, 6], [None, 4, 5]],
        names=["key", "a", "b"]
    )
    assert result.sort_by("a").equals(expected)
