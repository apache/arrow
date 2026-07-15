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

class TestFloat32Array < Test::Unit::TestCase
  sub_test_case("#initialize") do
    def test_no_null
      values = [-Float::INFINITY, -0.0, +0.0, +Float::INFINITY]
      assert_equal(values,
                   ArrowFormat::Float32Array.new(values).to_a)
    end

    def test_mixed
      values = [-Float::INFINITY, -0.0, nil, +0.0, +Float::INFINITY]
      assert_equal(values,
                   ArrowFormat::Float32Array.new(values).to_a)
    end
  end

  sub_test_case("#==") do
    def test_no_slice
      values = [-Float::INFINITY, -0.0, nil, +0.0, +Float::INFINITY]
      array1 = ArrowFormat::Float32Array.new(values)
      array2 = ArrowFormat::Float32Array.new(values)
      assert_equal(array1, array2)
    end

    def test_sliced
      values = [-Float::INFINITY, -0.0, nil, +0.0, +Float::INFINITY]
      array1 = ArrowFormat::Float32Array.new(values)
      array2 = ArrowFormat::Float32Array.new([0.0, *values, 0.0])
      assert_equal(array1, array2.slice(1, 5))
    end

    def test_sliced_different_content
      values = [-Float::INFINITY, -0.0, nil, +0.0, +Float::INFINITY]
      array1 = ArrowFormat::Float32Array.new(values)
      array2 = ArrowFormat::Float32Array.new([0.0, 0.0, *values, 0.0])
      assert_not_equal(array1, array2.slice(1, 5))
    end
  end
end
