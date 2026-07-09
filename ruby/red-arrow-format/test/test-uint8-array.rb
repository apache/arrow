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

class TestUInt8Array < Test::Unit::TestCase
  sub_test_case("#initialize") do
    def test_no_null
      values = [0, (2 ** 8) - 1]
      assert_equal(values,
                   ArrowFormat::UInt8Array.new(values).to_a)
    end

    def test_mixed
      values = [0, nil, (2 ** 8) - 1]
      assert_equal(values,
                   ArrowFormat::UInt8Array.new(values).to_a)
    end
  end

  sub_test_case("#==") do
    def test_no_slice
      values = [0, nil, (2 ** 8) - 1]
      array1 = ArrowFormat::UInt8Array.new(values)
      array2 = ArrowFormat::UInt8Array.new(values)
      assert_equal(array1, array2)
    end

    def test_sliced
      values = [0, nil, (2 ** 8) - 1]
      array1 = ArrowFormat::UInt8Array.new(values)
      array2 = ArrowFormat::UInt8Array.new([0] + values + [0])
      assert_equal(array1, array2.slice(1, 3))
    end

    def test_sliced_different_content
      values = [0, nil, (2 ** 8) - 1]
      array1 = ArrowFormat::UInt8Array.new(values)
      array2 = ArrowFormat::UInt8Array.new([0, 0] + values + [0])
      assert_not_equal(array1, array2.slice(1, 3))
    end
  end
end
