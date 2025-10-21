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

class TestExecutePlan < Test::Unit::TestCase
  include Helper::Buildable
  include Helper::Omittable

  def execute(plan)
    plan.validate
    plan.start
    plan.wait
    yield
    plan.stop
  end

  sub_test_case("aggregate") do
    def build_plan
      plan = Arrow::ExecutePlan.new

      record_batch =
        build_record_batch(number: build_int8_array([1, 2, 3, 4, 5]),
                           string: build_string_array(["a", "b", "a", "b", "a"]))
      source_node_options = Arrow::SourceNodeOptions.new(record_batch)
      source_node = plan.build_source_node(source_node_options)

      aggregate_node_options = yield
      aggregate_node = plan.build_aggregate_node(source_node,
                                                 aggregate_node_options)

      sink_node_options = Arrow::SinkNodeOptions.new
      sink_node = plan.build_sink_node(aggregate_node,
                                       sink_node_options)

      [plan, sink_node_options.get_reader(aggregate_node.output_schema)]
    end

    def test_by_string
      plan, reader = build_plan do
        aggregations = [
          Arrow::Aggregation.new("hash_sum", nil, "number", "sum(number)"),
          Arrow::Aggregation.new("hash_count", nil, "number", "count(number)"),
        ]
        Arrow::AggregateNodeOptions.new(aggregations, ["string"])
      end
      execute(plan) do
        assert_equal(build_table("string" => build_string_array(["a", "b"]),
                                 "sum(number)" => build_int64_array([9, 6]),
                                 "count(number)" => build_int64_array([3, 2])),
                     reader.read_all)
      end
    end
  end

  sub_test_case("hash join") do
    def build_plan
      plan = Arrow::ExecutePlan.new

      left_record_batch =
        build_record_batch(number: build_int8_array([1, 2, 3, 4, 5]),
                           string: build_string_array(["a", "b", "a", "b", "a"]))
      left_node_options = Arrow::SourceNodeOptions.new(left_record_batch)
      left_node = plan.build_source_node(left_node_options)

      right_record_batch =
        build_record_batch(right_number: build_int8_array([1, 2]),
                           right_string: build_string_array(["R-1", "R-2"]))
      right_node_options = Arrow::SourceNodeOptions.new(right_record_batch)
      right_node = plan.build_source_node(right_node_options)

      hash_join_node_options = yield
      hash_join_node = plan.build_hash_join_node(left_node,
                                                 right_node,
                                                 hash_join_node_options)

      sink_node_options = Arrow::SinkNodeOptions.new
      sink_node = plan.build_sink_node(hash_join_node,
                                       sink_node_options)

      [plan, sink_node_options.get_reader(hash_join_node.output_schema)]
    end

    def test_output_all
      plan, reader = build_plan do
        Arrow::HashJoinNodeOptions.new(:left_outer,
                                       ["number"],
                                       ["right_number"])
      end

      execute(plan) do
        left_number = build_int8_array([1, 2, 3, 4, 5])
        left_string = build_string_array(["a", "b", "a", "b", "a"])
        right_number = build_int8_array([1, 2, nil, nil, nil])
        right_string = build_string_array(["R-1", "R-2", nil, nil, nil])
        assert_equal(build_table("number" => left_number,
                                 "string" => left_string,
                                 "right_number" => right_number,
                                 "right_string" => right_string),
                     reader.read_all)
      end
    end

    def test_output_selected
      plan, reader = build_plan do
        options = Arrow::HashJoinNodeOptions.new(:left_outer,
                                                 ["number"],
                                                 ["right_number"])
        options.left_outputs = ["number"]
        options.right_outputs = ["right_number"]
        options
      end

      execute(plan) do
        left_number = build_int8_array([1, 2, 3, 4, 5])
        right_number = build_int8_array([1, 2, nil, nil, nil])
        assert_equal(build_table("number" => left_number,
                                 "right_number" => right_number),
                     reader.read_all)
      end
    end
  end
end
