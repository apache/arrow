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

class TestReadOptions < Test::Unit::TestCase
  def setup
    @options = Arrow::ReadOptions.new
  end

  sub_test_case("max-recursion-depth") do
    def test_default
      assert_equal(64, @options.max_recursion_depth)
    end

    def test_accessor
      @options.max_recursion_depth = 29
      assert_equal(29, @options.max_recursion_depth)
    end
  end

  sub_test_case("use-threads") do
    def test_default
      assert do
        @options.use_threads?
      end
    end

    def test_accessor
      @options.use_threads = false
      assert do
        not @options.use_threads?
      end
    end
  end

  sub_test_case("#included_fields") do
    def test_default
      assert_equal([], @options.included_fields)
    end

    def test_accessor
      @options.included_fields = [1, 2, 3]
      assert_equal([1, 2, 3], @options.included_fields)
      @options.included_fields = []
      assert_equal([], @options.included_fields)
    end
  end
end
