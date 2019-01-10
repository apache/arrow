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

class RecordBatchBuilderTest < Test::Unit::TestCase
  sub_test_case(".new") do
    test("Schema") do
      schema = Arrow::Schema.new(visible: :boolean,
                                 count: :uint32)
      builder = Arrow::RecordBatchBuilder.new(schema)
      assert_equal(schema,
                   builder.schema)
    end

    test("Hash") do
      builder = Arrow::RecordBatchBuilder.new(visible: :boolean,
                                              count: :uint32)
      assert_equal(Arrow::Schema.new(visible: :boolean,
                                     count: :uint32),
                   builder.schema)
    end
  end

  sub_test_case("instance methods") do
    def setup
      @schema = Arrow::Schema.new(visible: :boolean,
                                  count: :uint32)
      @builder = Arrow::RecordBatchBuilder.new(@schema)
    end

    sub_test_case("#[]") do
      test("String") do
        assert_equal(Arrow::BooleanDataType.new,
                     @builder["visible"].value_data_type)
      end

      test("Symbol") do
        assert_equal(Arrow::BooleanDataType.new,
                     @builder[:visible].value_data_type)
      end

      test("Integer") do
        assert_equal(Arrow::UInt32DataType.new,
                     @builder[1].value_data_type)
      end
    end

    test("#append") do
      records = [
        {visible: true, count: 1},
      ]
      columns = {
        visible: [false],
        count: [2],
      }
      arrays = [
        Arrow::BooleanArray.new([true, false]),
        Arrow::UInt32Array.new([1, 2]),
      ]
      @builder.append(records, columns)
      assert_equal(Arrow::RecordBatch.new(@schema,
                                          arrays[0].length,
                                          arrays),
                   @builder.flush)
    end

    test("#append_records") do
      records = [
        {visible: true, count: 1},
        {visible: true, count: 2, garbage: "garbage"},
        {visible: true},
        [false, 4],
        nil,
        [true],
      ]
      arrays = [
        Arrow::BooleanArray.new([true, true, true, false, nil, true]),
        Arrow::UInt32Array.new([1, 2, nil, 4, nil, nil]),
      ]
      @builder.append_records(records)
      assert_equal(Arrow::RecordBatch.new(@schema,
                                          arrays[0].length,
                                          arrays),
                   @builder.flush)
    end

    test("#append_columns") do
      columns = {
        visible: [true, true, true, false, nil, true],
        count: [1, 2, nil, 4, nil, nil],
      }
      arrays = [
        Arrow::BooleanArray.new(columns[:visible]),
        Arrow::UInt32Array.new(columns[:count]),
      ]
      @builder.append_columns(columns)
      assert_equal(Arrow::RecordBatch.new(@schema,
                                          arrays[0].length,
                                          arrays),
                   @builder.flush)
    end
  end
end
