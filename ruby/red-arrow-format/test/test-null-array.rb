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

class TestNullArray < Test::Unit::TestCase
  def setup
    @array = ArrowFormat::NullArray.new(3)
  end

  def test_initialize
    assert_equal([nil] * 9,
                 ArrowFormat::NullArray.new(9).to_a)
  end

  sub_test_case("#==") do
    def test_no_slice
      array1 = ArrowFormat::NullArray.new(3)
      array2 = ArrowFormat::NullArray.new(3)
      assert_equal(array1, array2)
    end

    def test_sliced
      array1 = ArrowFormat::NullArray.new(3)
      array2 = ArrowFormat::NullArray.new(5)
      assert_equal(array1, array2.slice(1, 3))
    end
  end

  def test_aref
    assert_nil(@array[1])
  end
end
