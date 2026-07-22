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

class TestBooleanArray < Test::Unit::TestCase
  def setup
    @values = [true, nil, false]
    @array = ArrowFormat::BooleanArray.new(@values)
  end

  sub_test_case("#initialize") do
    def test_no_null
      assert_equal([true, false],
                   ArrowFormat::BooleanArray.new([true, false]).to_a)
    end

    def test_null_multiple_bytes
      values = [true] * 8 + [nil, false]
      assert_equal(values,
                   ArrowFormat::BooleanArray.new(values).to_a)
    end

    def test_mixed
      assert_equal([true, nil, false],
                   ArrowFormat::BooleanArray.new([true, nil, false]).to_a)
    end
  end

  sub_test_case("#==") do
    def test_no_slice
      array1 = ArrowFormat::BooleanArray.new([true, nil, false])
      array2 = ArrowFormat::BooleanArray.new([true, nil, false])
      assert_equal(array1, array2)
    end

    def test_sliced
      array1 = ArrowFormat::BooleanArray.new([true, nil, false])
      array2 = ArrowFormat::BooleanArray.new([true, true, nil, false, true])
      assert_equal(array1, array2.slice(1, 3))
    end

    def test_sliced_different_content
      array1 = ArrowFormat::BooleanArray.new([true, nil, false])
      array2 = ArrowFormat::BooleanArray.new([true, false, nil, false, true])
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
