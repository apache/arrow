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

class TestIsIn < Test::Unit::TestCase
  include Helper::Buildable

  sub_test_case("Array") do
    def test_no_null
      left = build_int16_array([1, 0, 1, 2])
      right = build_int16_array([2, 0])
      assert_equal(build_boolean_array([false, true, false, true]),
                   left.is_in(right))
    end

    def test_null_in_left
      left = build_int16_array([1, 0, nil, 2])
      right = build_int16_array([2, 0, 3])
      assert_equal(build_boolean_array([false, true, nil, true]),
                   left.is_in(right))
    end

    def test_null_in_right
      left = build_int16_array([1, 0, 1, 2])
      right = build_int16_array([2, 0, nil, 2, 0])
      assert_equal(build_boolean_array([false, true, false, true]),
                   left.is_in(right))
    end

    def test_null_in_both
      left = build_int16_array([1, 0, nil, 2])
      right = build_int16_array([2, 0, nil, 2, 0, nil])
      assert_equal(build_boolean_array([false, true, true, true]),
                   left.is_in(right))
    end
  end

  sub_test_case("ChunkedArray") do
    def test_no_null
      left = build_int16_array([1, 0, 1, 2])
      chunks = [
        build_int16_array([1, 0]),
        build_int16_array([1, 0, 3])
      ]
      right = Arrow::ChunkedArray.new(chunks)
      assert_equal(build_boolean_array([true, true, true, false]),
                   left.is_in_chunked_array(right))
    end

    def test_null_in_left
      left = build_int16_array([1, 0, nil, 2])
      chunks = [
        build_int16_array([2, 0, 3]),
        build_int16_array([3, 0, 2, 2])
      ]
      right = Arrow::ChunkedArray.new(chunks)
      assert_equal(build_boolean_array([false, true, nil, true]),
                   left.is_in_chunked_array(right))
    end

    def test_null_in_right
      left = build_int16_array([1, 0, 1, 2])
      chunks = [
        build_int16_array([2, 0, nil, 2, 0]),
        build_int16_array([2, 3, nil])
      ]
      right = Arrow::ChunkedArray.new(chunks)
      assert_equal(build_boolean_array([false, true, false, true]),
                   left.is_in_chunked_array(right))
    end

    def test_null_in_both
      left = build_int16_array([1, 0, nil, 2])
      chunks = [
        build_int16_array([2, 0, nil, 2, 0, nil]),
        build_int16_array([2, 3, nil])
      ]
      right = Arrow::ChunkedArray.new(chunks)
      assert_equal(build_boolean_array([false, true, true, true]),
                   left.is_in_chunked_array(right))
    end
  end
end
