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

  def setup
    @record_batch =
      build_record_batch(number: build_int8_array([1, 2, 3, 4, 5]),
                         string: build_string_array(["a", "b", "a", "b", "a"]))
    @plan = Arrow::ExecutePlan.new
    @source_node_options = Arrow::SourceNodeOptions.new(@record_batch)
    @source_node = @plan.build_source_node(@source_node_options)
    aggregations = [
      Arrow::Aggregation.new("hash_sum", nil, "number", "sum(number)"),
      Arrow::Aggregation.new("hash_count", nil, "number", "count(number)"),
    ]
    @aggregate_node_options =
      Arrow::AggregateNodeOptions.new(aggregations, ["string"])
    @aggregate_node = @plan.build_aggregate_node(@source_node,
                                                 @aggregate_node_options)
    @sink_node_options = Arrow::SinkNodeOptions.new
    @sink_node = @plan.build_sink_node(@aggregate_node,
                                       @sink_node_options)
  end

  def test_start
    @plan.validate
    @plan.start
    @plan.wait
    reader = @sink_node_options.get_reader(@aggregate_node.output_schema)
    assert_equal(build_table("sum(number)" => build_int64_array([9, 6]),
                             "count(number)" => build_int64_array([3, 2]),
                             "string" => build_string_array(["a", "b"])),
                 reader.read_all)
    @plan.stop
  end
end
