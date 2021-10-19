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

class TestSetLookupOptions < Test::Unit::TestCase
  include Helper::Buildable

  def test_new
    value_set = Arrow::ArrayDatum.new(build_int8_array([1, 2, 3]))
    options = Arrow::SetLookupOptions.new(value_set)
    assert_equal(value_set, options.value_set)
  end

  sub_test_case("instance methods") do
    def setup
      value_set = Arrow::ArrayDatum.new(build_int8_array([1, 2, 3]))
      @options = Arrow::SetLookupOptions.new(value_set)
    end

    def test_skip_nulls
      assert do
        not @options.skip_nulls?
      end
      @options.skip_nulls = true
      assert do
        @options.skip_nulls?
      end
    end
  end
end
