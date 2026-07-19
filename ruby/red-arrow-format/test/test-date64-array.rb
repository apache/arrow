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

class TestDate64Array < Test::Unit::TestCase
  def setup
    @date_2017_08_28_00_00_00 = 1503878400000
    @date_2025_12_10_00_00_00 = 1765324800000
  end

  sub_test_case("#initialize") do
    def test_no_null
      values = [@date_2017_08_28_00_00_00, @date_2025_12_10_00_00_00]
      assert_equal(values,
                   ArrowFormat::Date64Array.new(values).to_a)
    end

    def test_mixed
      values = [@date_2017_08_28_00_00_00, nil, @date_2025_12_10_00_00_00]
      assert_equal(values,
                   ArrowFormat::Date64Array.new(values).to_a)
    end
  end

  sub_test_case("#==") do
    def test_no_slice
      values = [@date_2017_08_28_00_00_00, nil, @date_2025_12_10_00_00_00]
      array1 = ArrowFormat::Date64Array.new(values)
      array2 = ArrowFormat::Date64Array.new(values)
      assert_equal(array1, array2)
    end

    def test_sliced
      values = [@date_2017_08_28_00_00_00, nil, @date_2025_12_10_00_00_00]
      array1 = ArrowFormat::Date64Array.new(values)
      array2 = ArrowFormat::Date64Array.new([0, *values, 0])
      assert_equal(array1, array2.slice(1, 3))
    end

    def test_sliced_different_content
      values = [@date_2017_08_28_00_00_00, nil, @date_2025_12_10_00_00_00]
      array1 = ArrowFormat::Date64Array.new(values)
      array2 = ArrowFormat::Date64Array.new([0, 0, *values, 0])
      assert_not_equal(array1, array2.slice(1, 3))
    end
  end
end
