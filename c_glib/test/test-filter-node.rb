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

class TestFilterNode < Test::Unit::TestCase
  include Helper::Buildable

  def execute_plan(options)
    plan = Arrow::ExecutePlan.new
    numbers = build_int8_array([1, 2, 3, 4, 5])
    table = build_table(number: numbers)
    source_node_options = Arrow::SourceNodeOptions.new(table)
    source_node = plan.build_source_node(source_node_options)
    filter_node = plan.build_filter_node(source_node, options)
    sink_node_options = Arrow::SinkNodeOptions.new
    sink_node = plan.build_sink_node(filter_node,
                                     sink_node_options)
    plan.validate
    plan.start
    plan.wait
    reader = sink_node_options.get_reader(filter_node.output_schema)
    table = reader.read_all
    plan.stop
    table
  end

  def test_expression
    three_scalar = Arrow::Int8Scalar.new(3)
    three_datum = Arrow::ScalarDatum.new(three_scalar)
    expression =
      Arrow::CallExpression.new("greater",
                                [
                                  Arrow::FieldExpression.new("number"),
                                  Arrow::LiteralExpression.new(three_datum),
                                ])
    options = Arrow::FilterNodeOptions.new(expression)
    assert_equal(build_table("number" => [
                               build_int8_array([4, 5]),
                             ]),
                 execute_plan(options))
  end
end
