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

class TestExpressionBuildable < Test::Unit::TestCase
  sub_test_case("#build_expression") do
    def setup
      field1 = Arrow::Field.new("field1", Arrow::Int32DataType.new)
      field2 = Arrow::Field.new("field2", Arrow::Int32DataType.new)
      @schema = Arrow::Schema.new([field1, field2])
    end

    test("+") do
      expression =
        @schema.build_expression do |record|
          record.field1 + record.field2
        end

      assert_equal(expression.to_s,
                  "int32 add((int32) field1, (int32) field2)")
    end

    test("if") do
      expression =
        @schema.build_expression do |record, context|
          context.if(record.field1 > record.field2)
            .then(record.field1 * record.field2)
            .else(record.field1 / record.field2)
        end
      expression_string = "if (bool greater_than((int32) field1, (int32) field2)) " +
                          "{ int32 multiply((int32) field1, (int32) field2) } " +
                          "else " +
                          "{ int32 divide((int32) field1, (int32) field2) }"
      assert_equal(expression.to_s,
                   expression_string)
    end

    test("invalid") do
      message = "The node passed to Gandiva::Expression " +
                "must belong to Gandiva::Node: <Gandiva::IfNodeQuery>"
      assert_raise(ArgumentError.new(message)) do
        @schema.build_expression do |record, context|
          context.if(record.field1 > record.field2)
            .then(record.field1 * record.field2)
        end
      end
    end
  end
end
