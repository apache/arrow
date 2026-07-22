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

class TestDayTimeIntervalArray < Test::Unit::TestCase
  def setup
    @values = [
      [1, 100],
      nil,
      [3, 300],
    ]
    @array = ArrowFormat::DayTimeIntervalArray.new(@values)
  end

  sub_test_case("#initialize") do
    def test_no_null
      values = [
        [1, 100],
        [3, 300],
      ]
      assert_equal(values,
                   ArrowFormat::DayTimeIntervalArray.new(values).to_a)
    end

    def test_mixed
      values = [
        [1, 100],
        nil,
        [3, 300],
      ]
      assert_equal(values,
                   ArrowFormat::DayTimeIntervalArray.new(values).to_a)
    end

    def test_hash
      values = [
        {day: 1, millisecond: 100},
        nil,
        {day: 3, millisecond: 300},
      ]
      assert_equal([
                     [1, 100],
                     nil,
                     [3, 300],
                   ],
                   ArrowFormat::DayTimeIntervalArray.new(values).to_a)
    end
  end

  sub_test_case("#==") do
    def test_no_slice
      values = [
        [1, 100],
        nil,
        [3, 300],
      ]
      array1 = ArrowFormat::DayTimeIntervalArray.new(values)
      array2 = ArrowFormat::DayTimeIntervalArray.new(values)
      assert_equal(array1, array2)
    end

    def test_sliced
      values = [
        [1, 100],
        nil,
        [3, 300],
      ]
      array1 = ArrowFormat::DayTimeIntervalArray.new(values)
      array2 = ArrowFormat::DayTimeIntervalArray.new([nil, *values, nil])
      assert_equal(array1, array2.slice(1, 3))
    end

    def test_sliced_different_content
      values = [
        [1, 100],
        nil,
        [3, 300],
      ]
      array1 = ArrowFormat::DayTimeIntervalArray.new(values)
      array2 = ArrowFormat::DayTimeIntervalArray.new([nil, nil, *values, nil])
      assert_not_equal(array1, array2.slice(1, 3))
    end
  end

  sub_test_case("#[]") do
    def test_valid
      assert_equal(@values[2], @array[2])
    end

    def test_null
      assert_nil(@array[1])
    end
  end
end
