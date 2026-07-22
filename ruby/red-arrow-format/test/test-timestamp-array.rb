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

class TestTimestampArray < Test::Unit::TestCase
  def setup
    @timestamp_2019_11_17_15_09_11 = 1574003351
    @timestamp_2025_12_16_05_33_58 = 1765863238
    @values = [
      @timestamp_2019_11_17_15_09_11,
      nil,
      @timestamp_2025_12_16_05_33_58,
    ]
    @array = ArrowFormat::TimestampArray.new(:second, @values)
  end

  sub_test_case("#initialize") do
    def test_no_null
      values = [
        @timestamp_2019_11_17_15_09_11,
        @timestamp_2025_12_16_05_33_58,
      ]
      assert_equal(values,
                   ArrowFormat::TimestampArray.new(:second, values).to_a)
    end

    def test_mixed
      values = [
        @timestamp_2019_11_17_15_09_11,
        nil,
        @timestamp_2025_12_16_05_33_58,
      ]
      assert_equal(values,
                   ArrowFormat::TimestampArray.new(:second, values).to_a)
    end

    def test_time_zone
      time_zone = "UTC"
      array = ArrowFormat::TimestampArray.new([:second, time_zone], [nil])
      assert_equal(ArrowFormat::TimestampType.new(:second, time_zone),
                   array.type)
    end
  end

  sub_test_case("#==") do
    def test_no_slice
      values = [
        @timestamp_2019_11_17_15_09_11,
        nil,
        @timestamp_2025_12_16_05_33_58,
      ]
      array1 = ArrowFormat::TimestampArray.new(:second, values)
      array2 = ArrowFormat::TimestampArray.new(:second, values)
      assert_equal(array1, array2)
    end

    def test_sliced
      values = [
        @timestamp_2019_11_17_15_09_11,
        nil,
        @timestamp_2025_12_16_05_33_58,
      ]
      array1 = ArrowFormat::TimestampArray.new(:second, values)
      array2 = ArrowFormat::TimestampArray.new(:second, [0, *values, 0])
      assert_equal(array1, array2.slice(1, 3))
    end

    def test_sliced_different_content
      values = [
        @timestamp_2019_11_17_15_09_11,
        nil,
        @timestamp_2025_12_16_05_33_58,
      ]
      array1 = ArrowFormat::TimestampArray.new(:second, values)
      array2 = ArrowFormat::TimestampArray.new(:second, [0, 0, *values, 0])
      assert_not_equal(array1, array2.slice(1, 3))
    end
  end

  sub_test_case("#[]") do
    def test_valid
      assert_equal(@values[3], @array[3])
    end

    def test_null
      assert_nil(@array[1])
    end
  end
end
