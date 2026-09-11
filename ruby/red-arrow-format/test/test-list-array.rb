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

class TestListArray < Test::Unit::TestCase
  def setup
    child = ArrowFormat::Field.new("item", ArrowFormat::Int32Type.singleton)
    @type = ArrowFormat::ListType.new(child)
  end

  sub_test_case("#initialize") do
    def test_no_null
      values = [[-1, 0], [], [1, 2, 3]]
      array = ArrowFormat::ListArray.new(@type, values)
      assert_same(@type, array.type)
      assert_equal(values, array.to_a)
    end

    def test_null_list
      values = [[1, 2], nil, [], [3]]
      array = ArrowFormat::ListArray.new(@type, values)
      assert_equal([0, 2, 2, 2, 3], array.offsets)
      assert_equal([1, 2, 3], array.child.to_a)
      assert_equal(values, array.to_a)
    end

    def test_null_child
      values = [[1, nil], [], [nil, 2]]
      array = ArrowFormat::ListArray.new(@type, values)
      assert_equal([1, nil, nil, 2], array.child.to_a)
      assert_equal(values, array.to_a)
    end

    def test_empty
      array = ArrowFormat::ListArray.new(@type, [])
      assert_equal([0], array.offsets)
      assert_equal([], array.child.to_a)
      assert_equal([], array.to_a)
    end
  end
end
