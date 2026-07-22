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

class TestTime64Array < Test::Unit::TestCase
  def setup
    @time_00_00_10_000_000 = 10 * 1_000_000
    @time_00_01_10_000_000 = (60 + 10) * 1_000_000
    @values = [@time_00_00_10_000_000, nil, @time_00_01_10_000_000]
    @array = ArrowFormat::Time64Array.new(:microsecond, @values)
  end

  sub_test_case("#initialize") do
    def test_no_null
      values = [@time_00_00_10_000_000, @time_00_01_10_000_000]
      assert_equal(values,
                   ArrowFormat::Time64Array.new(:microsecond, values).to_a)
    end

    def test_mixed
      values = [@time_00_00_10_000_000, nil, @time_00_01_10_000_000]
      assert_equal(values,
                   ArrowFormat::Time64Array.new(:microsecond, values).to_a)
    end
  end

  sub_test_case("#==") do
    def test_no_slice
      values = [@time_00_00_10_000_000, nil, @time_00_01_10_000_000]
      array1 = ArrowFormat::Time64Array.new(:microsecond, values)
      array2 = ArrowFormat::Time64Array.new(:microsecond, values)
      assert_equal(array1, array2)
    end

    def test_sliced
      values = [@time_00_00_10_000_000, nil, @time_00_01_10_000_000]
      array1 = ArrowFormat::Time64Array.new(:microsecond, values)
      array2 = ArrowFormat::Time64Array.new(:microsecond, [0, *values, 0])
      assert_equal(array1, array2.slice(1, 3))
    end

    def test_sliced_different_content
      values = [@time_00_00_10_000_000, nil, @time_00_01_10_000_000]
      array1 = ArrowFormat::Time64Array.new(:microsecond, values)
      array2 = ArrowFormat::Time64Array.new(:microsecond, [0, 0, *values, 0])
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
