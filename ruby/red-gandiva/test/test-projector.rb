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
  def test_evaluate
    table = Arrow::Table.new(:field1 => Arrow::Int32Array.new([1, 2, 3, 4]),
                             :field2 => Arrow::Int32Array.new([11, 13, 15, 17]))
    schema = table.schema
    field1 = schema[:field1]
    field2 = schema[:field2]
    field_node1 = Gandiva::FieldNode.new(field1)
    field_node2 = Gandiva::FieldNode.new(field2)
    add_function_node = Gandiva::FunctionNode.new("add",
                                                  [field_node1, field_node2],
                                                  Arrow::Int32DataType.new)
    add_result = Arrow::Field.new("add_result", :int32)
    add_expression = Gandiva::Expression.new(add_function_node,
                                             add_result)
    subtract_function_node = Gandiva::FunctionNode.new("subtract",
                                                       [field_node1, field_node2],
                                                       Arrow::Int32DataType.new)
    subtract_result = Arrow::Field.new("subtract_result", :int32)
    subtract_expression = Gandiva::Expression.new(subtract_function_node,
                                                  subtract_result)
    projector = Gandiva::Projector.new(schema,
                                       [add_expression, subtract_expression])
    table.each_record_batch do |record_batch|
      outputs = projector.evaluate(record_batch)
      assert_equal([
                     [12, 15, 18, 21],
                     [-10, -11, -12, -13],
                   ],
                   outputs.collect(&:values))
    end
  end
end
