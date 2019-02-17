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

  sub_test_case("instance methods") do
    def setup
      @values = [true, false, nil, true]
      @array = Arrow::BooleanArray.new(@values)
    end

    test("#each") do
      assert_equal(@values, @array.to_a)
    end

    sub_test_case("#[]") do
      test("valid range") do
        assert_equal(@values,
                     @array.length.times.collect {|i| @array[i]})
      end

      test("out of range") do
        assert_nil(@array[@array.length])
      end

      test("negative index") do
        assert_equal(@values.last,
                     @array[-1])
      end
    end
  end
end
