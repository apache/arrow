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

class TestFixedSizeListArray < Test::Unit::TestCase
  def setup
    child = ArrowFormat::Field.new("item", ArrowFormat::Int16Type.singleton)
    @type = ArrowFormat::FixedSizeListType.new(child, 2)
  end

  sub_test_case("#initialize") do
    def test_no_null
      values = [[-1, 0], [1, 2]]
      array = ArrowFormat::FixedSizeListArray.new(@type, values)
      assert_same(@type, array.type)
      assert_same(@type.child.type, array.child.type)
      assert_equal(values, array.to_a)
    end

    def test_null_list
      values = [[1, 2], nil, [3, 4]]
      array = ArrowFormat::FixedSizeListArray.new(@type, values)
      assert_equal(1, array.n_nulls)
      assert_equal([1, 2, nil, nil, 3, 4], array.child.to_a)
      assert_equal(values, array.to_a)
    end

    def test_null_child
      values = [[1, nil], [nil, 2]]
      array = ArrowFormat::FixedSizeListArray.new(@type, values)
      assert_equal(2, array.child.n_nulls)
      assert_equal(values, array.to_a)
    end

    def test_invalid_list_size
      message = "list size must be 2: [1]"
      assert_raise(ArgumentError.new(message)) do
        ArrowFormat::FixedSizeListArray.new(@type, [[1]])
      end
    end

    def test_empty
      array = ArrowFormat::FixedSizeListArray.new(@type, [])
      assert_equal([], array.child.to_a)
      assert_equal([], array.to_a)
    end

    def test_low_level
      child = ArrowFormat::Int16Array.new([1, 2])
      array = ArrowFormat::FixedSizeListArray.new(@type, 1, nil, child)
      assert_equal([[1, 2]], array.to_a)
    end
  end
end
