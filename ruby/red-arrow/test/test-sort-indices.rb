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

class SortIndicesTest < Test::Unit::TestCase
  def setup
    @table = Arrow::Table.new(number1: [16, -1,   2, 32, -4, -4, -8],
                              number2: [32,  2, -16,  8,  1,  4,  1])
  end

  sub_test_case("Table") do
    test("Symbol") do
      assert_equal(Arrow::UInt64Array.new([6, 4, 5, 1, 2, 0, 3]),
                   @table.sort_indices(:number1))
    end

    test("-String") do
      assert_equal(Arrow::UInt64Array.new([3, 0, 2, 1, 4, 5, 6]),
                   @table.sort_indices("-number1"))
    end

    test("Symbol, -String") do
      assert_equal(Arrow::UInt64Array.new([6, 5, 4, 1, 2, 0, 3]),
                   @table.sort_indices([:number1, "-number2"]))
    end
  end
end
