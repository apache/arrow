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

class TestFixedSizeBinaryArray < Test::Unit::TestCase
  sub_test_case("#initialize") do
    def test_no_null
      values = ["0124".b, "abcd".b]
      assert_equal(values,
                   ArrowFormat::FixedSizeBinaryArray.new(4, values).to_a)
    end

    def test_mixed
      values = ["0124".b, nil, "abcd".b]
      assert_equal(values,
                   ArrowFormat::FixedSizeBinaryArray.new(4, values).to_a)
    end

    def test_type
      type = ArrowFormat::FixedSizeBinaryType.new(4)
      values = ["0124".b, nil, "abcd".b]
      assert_equal(values,
                   ArrowFormat::FixedSizeBinaryArray.new(type, values).to_a)
    end

    def test_too_small_value_size
      error = ArgumentError.new("value size must be 4: \"012\"")
      assert_raise(error) do
        ArrowFormat::FixedSizeBinaryArray.new(4, ["012".b])
      end
    end

    def test_too_large_value_size
      error = ArgumentError.new("value size must be 4: \"01245\"")
      assert_raise(error) do
        ArrowFormat::FixedSizeBinaryArray.new(4, ["01245".b])
      end
    end
  end

  sub_test_case("#==") do
    def test_no_slice
      values = ["0124".b, nil, "abcd".b]
      array1 = ArrowFormat::FixedSizeBinaryArray.new(4, values)
      array2 = ArrowFormat::FixedSizeBinaryArray.new(4, values)
      assert_equal(array1, array2)
    end

    def test_sliced
      pad = "0000".b
      values = ["0124".b, nil, "abcd".b]
      array1 = ArrowFormat::FixedSizeBinaryArray.new(4, values)
      array2 = ArrowFormat::FixedSizeBinaryArray.new(4, [pad, *values, pad])
      assert_equal(array1, array2.slice(1, 3))
    end

    def test_sliced_different_content
      pad = "0000".b
      values = ["0124".b, nil, "abcd".b]
      array1 = ArrowFormat::FixedSizeBinaryArray.new(4, values)
      array2 = ArrowFormat::FixedSizeBinaryArray.new(4,
                                                     [pad, pad, *values, pad])
      assert_not_equal(array1, array2.slice(1, 3))
    end
  end
end
