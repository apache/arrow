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

class TestSourceNode < Test::Unit::TestCase
  include Helper::Buildable

  def execute_plan(options)
    plan = Arrow::ExecutePlan.new
    source_node = plan.build_source_node(options)
    sink_node_options = Arrow::SinkNodeOptions.new
    sink_node = plan.build_sink_node(source_node,
                                     sink_node_options)
    plan.validate
    plan.start
    plan.wait
    reader = sink_node_options.get_reader(source_node.output_schema)
    table = reader.read_all
    plan.stop
    table
  end

  def test_record_batch_reader
    numbers = build_int8_array([1, 2, 3, 4, 5])
    strings = build_string_array(["a", "b", "a", "b", "a"])
    record_batch = build_record_batch(number: numbers,
                                      string: strings)
    reader = Arrow::RecordBatchReader.new([record_batch])
    options = Arrow::SourceNodeOptions.new(reader)
    assert_equal(build_table(number: numbers,
                             string: strings),
                 execute_plan(options))
  end

  def test_record_batch
    numbers = build_int8_array([1, 2, 3, 4, 5])
    strings = build_string_array(["a", "b", "a", "b", "a"])
    record_batch = build_record_batch(number: numbers,
                                      string: strings)
    options = Arrow::SourceNodeOptions.new(record_batch)
    assert_equal(build_table(number: numbers,
                             string: strings),
                 execute_plan(options))
  end

  def test_table
    numbers = build_int8_array([1, 2, 3, 4, 5])
    strings = build_string_array(["a", "b", "a", "b", "a"])
    table = build_table(number: numbers,
                        string: strings)
    options = Arrow::SourceNodeOptions.new(table)
    assert_equal(table, execute_plan(options))
  end
end
