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

class TestProjector < Test::Unit::TestCase
  include Helper::Buildable

  def test_evaluate
    field1 = Arrow::Field.new("field1", Arrow::Int32DataType.new)
    field2 = Arrow::Field.new("field2", Arrow::Int32DataType.new)
    schema = Arrow::Schema.new([field1, field2])
    field_sum = Arrow::Field.new("add", Arrow::Int32DataType.new)
    field_subtract = Arrow::Field.new("subtract", Arrow::Int32DataType.new)
    sum_expression = Gandiva::Expression.new("add", [field1, field2], field_sum)
    subtract_expression = Gandiva::Expression.new("subtract", [field1, field2], field_subtract)
    projector = Gandiva::Projector.new(schema, [sum_expression, subtract_expression])
    input_columns = [
      build_int32_array([1, 2, 3, 4]),
      build_int32_array([11, 13, 15, 17]),
    ]
    output_columns = [
      build_int32_array([12, 15, 18, 21]),
      build_int32_array([-10, -11, -12, -13]),
    ]
    record_batch = Arrow::RecordBatch.new(schema, 4, input_columns)
    outputs = projector.evaluate(record_batch)
    assert_equal(output_columns, outputs)
  end
end
