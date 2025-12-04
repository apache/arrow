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

class TestJoinOptions < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @options = Arrow::JoinOptions.new
  end

  def test_null_handling_property
    assert_equal(Arrow::JoinNullHandlingBehavior::EMIT_NULL, @options.null_handling)
    @options.null_handling = Arrow::JoinNullHandlingBehavior::SKIP
    assert_equal(Arrow::JoinNullHandlingBehavior::SKIP, @options.null_handling)
    @options.null_handling = Arrow::JoinNullHandlingBehavior::REPLACE
    assert_equal(Arrow::JoinNullHandlingBehavior::REPLACE, @options.null_handling)
  end

  def test_null_replacement_property
    assert_equal("", @options.null_replacement)
    @options.null_replacement = "NULL"
    assert_equal("NULL", @options.null_replacement)
  end

  def test_binary_join_element_wise_function
    args = [
      Arrow::ArrayDatum.new(build_string_array(["a", "b", nil])),
      Arrow::ArrayDatum.new(build_string_array(["x", "y", "z"])),
      Arrow::ScalarDatum.new(Arrow::StringScalar.new(Arrow::Buffer.new("-"))),
    ]
    binary_join_element_wise_function = Arrow::Function.find("binary_join_element_wise")

    @options.null_handling = Arrow::JoinNullHandlingBehavior::EMIT_NULL
    result = binary_join_element_wise_function.execute(args, @options).value
    assert_equal(build_string_array(["a-x", "b-y", nil]),
                 result)

    @options.null_handling = Arrow::JoinNullHandlingBehavior::SKIP
    result = binary_join_element_wise_function.execute(args, @options).value
    assert_equal(build_string_array(["a-x", "b-y", "z"]),
                 result)

    @options.null_handling = Arrow::JoinNullHandlingBehavior::REPLACE
    @options.null_replacement = "NULL"
    result = binary_join_element_wise_function.execute(args, @options).value
    assert_equal(build_string_array(["a-x", "b-y", "NULL-z"]),
                 result)
  end
end

