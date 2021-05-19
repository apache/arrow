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

class TestScalarAggregateOptions < Test::Unit::TestCase
  def setup
    @options = Arrow::ScalarAggregateOptions.new
  end

  sub_test_case("skip_nulls") do
    def test_default
      assert do
        @options.skip_nulls?
      end
    end

    def test_accessor
      @options.skip_nulls = false
      assert do
        not @options.skip_nulls?
      end
    end
  end

  sub_test_case("min_count") do
    def test_default
      assert_equal(1, @options.min_count)
    end

    def test_accessor
      @options.min_count = 0
      assert_equal(0, @options.min_count)
    end
  end
end
