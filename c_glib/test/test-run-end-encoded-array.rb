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

class TestRunEndEncodedArray < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @run_end_data_type = Arrow::Int32DataType.new
    @value_data_type = Arrow::StringDataType.new
    @data_type = Arrow::RunEndEncodedDataType.new(@run_end_data_type,
                                                  @value_data_type)
  end

  sub_test_case(".new") do
    def test_new
      run_ends = build_int32_array([2, 5, 6])
      values = build_string_array(["hello", "hi", "world"])
      array = Arrow::RunEndEncodedArray.new(@data_type,
                                            6,
                                            run_ends,
                                            values,
                                            0)
      assert_equal(<<-STRING.chomp, array.to_s)

-- run_ends:
  [
    2,
    5,
    6
  ]
-- values:
  [
    "hello",
    "hi",
    "world"
  ]
      STRING
    end
  end

  sub_test_case("instance methods") do
    def setup
      super
      @raw_run_ends = [3, 6, 7]
      @run_ends = build_int32_array(@raw_run_ends)
      @raw_values = ["hello", "hi", "world"]
      @values = build_string_array(@raw_values)
      @offset = 4
      @array = Arrow::RunEndEncodedArray.new(@data_type,
                                             3,
                                             @run_ends,
                                             @values,
                                             @offset)
    end

    def test_run_ends
      assert_equal(@run_ends, @array.run_ends)
    end

    def test_values
      assert_equal(@values, @array.values)
    end

    def test_logical_run_ends
      logical_raw_run_ends =
        @raw_run_ends
          .collect {|v| v - @offset}
          .reject(&:negative?)
      assert_equal(build_int32_array(logical_raw_run_ends),
                   @array.logical_run_ends)
    end

    def test_logical_values
      assert_equal(build_string_array(@raw_values[1..-1]),
                   @array.logical_values)
    end

    def test_find_physical_offset
      assert_equal(1, @array.find_physical_offset)
    end

    def test_find_physical_length
      assert_equal(2, @array.find_physical_length)
    end
  end
end
