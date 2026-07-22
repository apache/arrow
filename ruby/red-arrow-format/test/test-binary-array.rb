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

class TestBinaryArray < Test::Unit::TestCase
  def setup
    @values = ["\x00\x01".b, nil, "\xff\x10".b]
    @array = ArrowFormat::BinaryArray.new(@values)
  end

  sub_test_case("#initialize") do
    def test_no_null
      values = ["\x00\x01".b, "\xff\x10".b]
      assert_equal(values,
                   ArrowFormat::BinaryArray.new(values).to_a)
    end

    def test_mixed
      values = ["\x00\x01".b, nil, "\xff\x10".b]
      assert_equal(values,
                   ArrowFormat::BinaryArray.new(values).to_a)
    end
  end

  sub_test_case("#==") do
    def test_no_slice
      values = ["\x00\x01".b, nil, "\xff\x10".b]
      array1 = ArrowFormat::BinaryArray.new(values)
      array2 = ArrowFormat::BinaryArray.new(values)
      assert_equal(array1, array2)
    end

    def test_sliced
      values = ["\x00\x01".b, nil, "\xff\x10".b]
      array1 = ArrowFormat::BinaryArray.new(values)
      array2 = ArrowFormat::BinaryArray.new(["".b, *values, "".b])
      assert_equal(array1, array2.slice(1, 3))
    end

    def test_sliced_different_content
      values = ["\x00\x01".b, nil, "\xff\x10".b]
      array1 = ArrowFormat::BinaryArray.new(values)
      array2 = ArrowFormat::BinaryArray.new(["".b, "".b, *values, "".b])
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
