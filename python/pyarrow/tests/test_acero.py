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
    Declaration
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
