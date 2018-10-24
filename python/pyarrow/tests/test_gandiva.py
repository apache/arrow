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

import pyarrow as pa
import pyarrow.gandiva as gandiva


def test_tree_exp_builder():
    builder = gandiva.TreeExprBuilder()

    field_a = pa.field('a', pa.int32())
    field_b = pa.field('b', pa.int32())

    schema = pa.schema([field_a, field_b])

    field_result = pa.field('res', pa.int32())

    node_a = builder.make_field(field_a)
    node_b = builder.make_field(field_b)

    condition = builder.make_function(b"greater_than", [node_a, node_b],
                                      pa.bool_())
    if_node = builder.make_if(condition, node_a, node_b, pa.int32())

    expr = builder.make_expression(if_node, field_result)

    projector = gandiva.make_projector(schema, [expr], pa.default_memory_pool())

    a = pa.array([10, 12, -20, 5], type=pa.int32())
    b = pa.array([5, 15, 15, 17], type=pa.int32())
    e = pa.array([10, 15, 15, 17], type=pa.int32())
    input_batch = pa.RecordBatch.from_arrays([a, b], names=['a', 'b'])

    r, = projector.evaluate(input_batch)
    assert r.equals(e)
