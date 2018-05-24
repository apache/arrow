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

class ArrayTest < Test::Unit::TestCase
  sub_test_case(".new") do
    test("Boolean") do
      array = Arrow::BooleanArray.new([true, false, true])
      assert_equal([true, false, true],
                   array.to_a)
    end
  end

  test("#each") do
    array = Arrow::BooleanArray.new([true, false, nil, true])
    assert_equal([true, false, nil, true],
                 array.to_a)
  end

  test("#[]") do
    array = Arrow::BooleanArray.new([true, false, nil, true])
    assert_equal([true, false, nil, true],
                 [array[0], array[1], array[2], array[3]])
  end
end
