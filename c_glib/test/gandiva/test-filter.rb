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

class TestGandivaFilter < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    omit("Gandiva is required") unless defined?(::Gandiva)

    field1 = Arrow::Field.new("field1", Arrow::Int32DataType.new)
    field2 = Arrow::Field.new("field2", Arrow::Int32DataType.new)
    schema = Arrow::Schema.new([field1, field2])
    field_node1 = Gandiva::FieldNode.new(field1)
    field_node2 = Gandiva::FieldNode.new(field2)
    equal_function_node =
      Gandiva::FunctionNode.new("equal",
                                [field_node1, field_node2],
                                Arrow::BooleanDataType.new)
    condition = Gandiva::Condition.new(equal_function_node)
    @filter = Gandiva::Filter.new(schema, condition)

    input_arrays = [
      build_int32_array([1, 2, 3, 4]),
      build_int32_array([11, 2, 15, 4]),
    ]
    @record_batch = Arrow::RecordBatch.new(schema,
                                           input_arrays[0].length,
                                           input_arrays)
  end

  def test_evaluate
    selection_vector = Gandiva::UInt16SelectionVector.new(@record_batch.n_rows)
    @filter.evaluate(@record_batch, selection_vector)
    assert_equal(build_uint16_array([1, 3]),
                 selection_vector.to_array)
  end
end
