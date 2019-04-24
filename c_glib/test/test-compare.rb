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

class TestComapre < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @options = Arrow::CompareOptions.new
  end

  sub_test_case("operator") do
    def test_equal
      @options.operator = :equal
      assert_equal(build_boolean_array([true, nil, false]),
                   build_int32_array([1, nil, 3]).compare(1, @options))
    end

    def test_not_equal
      @options.operator = :not_equal
      assert_equal(build_boolean_array([false, nil, true]),
                   build_int32_array([1, nil, 3]).compare(1, @options))
    end

    def test_greater
      @options.operator = :greater
      assert_equal(build_boolean_array([false, nil, true]),
                   build_int32_array([1, nil, 3]).compare(1, @options))
    end

    def test_greater_equal
      @options.operator = :greater_equal
      assert_equal(build_boolean_array([true, nil, true]),
                   build_int32_array([1, nil, 3]).compare(1, @options))
    end

    def test_less
      @options.operator = :less
      assert_equal(build_boolean_array([false, nil, false]),
                   build_int32_array([1, nil, 3]).compare(1, @options))
    end

    def test_less_equal
      @options.operator = :less_equal
      assert_equal(build_boolean_array([true, nil, false]),
                   build_int32_array([1, nil, 3]).compare(1, @options))
    end
  end
end
