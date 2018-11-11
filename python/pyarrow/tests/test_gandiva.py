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
import pandas as pd


@pytest.mark.gandiva
def test_tree_exp_builder():
    import pyarrow.gandiva as gandiva

    builder = gandiva.TreeExprBuilder()

    field_a = pa.field('a', pa.int32())
    field_b = pa.field('b', pa.int32())

    schema = pa.schema([field_a, field_b])

    field_result = pa.field('res', pa.int32())

    node_a = builder.make_field(field_a)
    node_b = builder.make_field(field_b)

    condition = builder.make_function("greater_than", [node_a, node_b],
                                      pa.bool_())
    if_node = builder.make_if(condition, node_a, node_b, pa.int32())

    expr = builder.make_expression(if_node, field_result)

    projector = gandiva.make_projector(
        schema, [expr], pa.default_memory_pool())

    a = pa.array([10, 12, -20, 5], type=pa.int32())
    b = pa.array([5, 15, 15, 17], type=pa.int32())
    e = pa.array([10, 15, 15, 17], type=pa.int32())
    input_batch = pa.RecordBatch.from_arrays([a, b], names=['a', 'b'])

    r, = projector.evaluate(input_batch)
    assert r.equals(e)


@pytest.mark.gandiva
def test_table():
    import pyarrow.gandiva as gandiva

    df = pd.DataFrame({"a": [1.0, 2.0], "b": [3.0, 4.0]})
    table = pa.Table.from_pandas(df)

    builder = gandiva.TreeExprBuilder()
    node_a = builder.make_field(table.schema.field_by_name("a"))
    node_b = builder.make_field(table.schema.field_by_name("b"))

    sum = builder.make_function("add", [node_a, node_b], pa.float64())

    field_result = pa.field("c", pa.float64())
    expr = builder.make_expression(sum, field_result)

    projector = gandiva.make_projector(
        table.schema, [expr], pa.default_memory_pool())

    # TODO: Add .evaluate function which can take Tables instead of
    # RecordBatches
    r, = projector.evaluate(table.to_batches()[0])

    e = pa.Array.from_pandas(df["a"] + df["b"])
    assert r.equals(e)


@pytest.mark.gandiva
def test_filter():
    import pyarrow.gandiva as gandiva

    df = pd.DataFrame({"a": [1.0 * i for i in range(10000)]})
    table = pa.Table.from_pandas(df)

    builder = gandiva.TreeExprBuilder()
    node_a = builder.make_field(table.schema.field_by_name("a"))
    thousand = builder.make_literal(1000.0, pa.float64())
    cond = builder.make_function("less_than", [node_a, thousand], pa.bool_())
    condition = builder.make_condition(cond)

    filter = gandiva.make_filter(table.schema, condition)
    result = filter.evaluate(table.to_batches()[0], pa.default_memory_pool())
    assert result.to_array().equals(pa.array(range(1000), type=pa.uint32()))


@pytest.mark.gandiva
def test_literals():
    import pyarrow.gandiva as gandiva

    builder = gandiva.TreeExprBuilder()

    builder.make_literal(True, pa.bool_())
    builder.make_literal(0, pa.uint8())
    builder.make_literal(1, pa.uint16())
    builder.make_literal(2, pa.uint32())
    builder.make_literal(3, pa.uint64())
    builder.make_literal(4, pa.int8())
    builder.make_literal(5, pa.int16())
    builder.make_literal(6, pa.int32())
    builder.make_literal(7, pa.int64())
    builder.make_literal(8.0, pa.float32())
    builder.make_literal(9.0, pa.float64())
    builder.make_literal("hello", pa.string())
    builder.make_literal(b"world", pa.binary())

    builder.make_literal(True, "bool")
    builder.make_literal(0, "uint8")
    builder.make_literal(1, "uint16")
    builder.make_literal(2, "uint32")
    builder.make_literal(3, "uint64")
    builder.make_literal(4, "int8")
    builder.make_literal(5, "int16")
    builder.make_literal(6, "int32")
    builder.make_literal(7, "int64")
    builder.make_literal(8.0, "float32")
    builder.make_literal(9.0, "float64")
    builder.make_literal("hello", "string")
    builder.make_literal(b"world", "binary")

    with pytest.raises(TypeError):
        builder.make_literal("hello", pa.int64())
    with pytest.raises(TypeError):
        builder.make_literal(True, None)


@pytest.mark.gandiva
def test_regex():
    import pyarrow.gandiva as gandiva

    elements = ["park", "sparkle", "bright spark and fire", "spark"]
    data = pa.array(elements, type=pa.string())
    table = pa.Table.from_arrays([data], names=['a'])

    builder = gandiva.TreeExprBuilder()
    node_a = builder.make_field(table.schema.field_by_name("a"))
    regex = builder.make_literal("%spark%", pa.string())
    like = builder.make_function("like", [node_a, regex], pa.bool_())

    field_result = pa.field("b", pa.bool_())
    expr = builder.make_expression(like, field_result)

    projector = gandiva.make_projector(
        table.schema, [expr], pa.default_memory_pool())

    r, = projector.evaluate(table.to_batches()[0])
    b = pa.array([False, True, True, True], type=pa.bool_())
    assert r.equals(b)


@pytest.mark.gandiva
def test_get_registered_function_signatures():
    import pyarrow.gandiva as gandiva
    signatures = gandiva.get_registered_function_signatures()

    assert type(signatures[0].return_type()) is pa.DataType
    assert type(signatures[0].param_types()) is list
    assert hasattr(signatures[0], "name")
