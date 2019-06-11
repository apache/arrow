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

class TestGandivaNativeFunction < Test::Unit::TestCase
  include Helper::DataType

  def setup
    omit("Gandiva is required") unless defined?(::Gandiva)
    @registry = Gandiva::FunctionRegistry.new
    @not = lookup("not", [boolean_data_type], boolean_data_type)
    @isnull = lookup("isnull", [int8_data_type], boolean_data_type)
  end

  def lookup(name, param_types, return_type)
    signature = Gandiva::FunctionSignature.new(name,
                                               param_types,
                                               return_type)
    @registry.lookup(signature)
  end

  def test_get_signature
    assert_kind_of(Gandiva::FunctionSignature,
                   @not.signature)
  end

  sub_test_case("equal") do
    def test_true
      assert do
        @not == @registry.lookup(@not.signature)
      end
    end

    def test_false
      assert do
        @not != @isnull
      end
    end
  end

  def test_to_string
    assert_equal(@not.signature.to_s,
                 @not.to_s)
  end

  sub_test_case("get_result_nullbale_type") do
    def test_if_null
      assert_equal(Gandiva::ResultNullableType::IF_NULL,
                   @not.result_nullable_type)
    end

    def test_never
      assert_equal(Gandiva::ResultNullableType::NEVER,
                   @isnull.result_nullable_type)
    end

    def test_internal
      to_date = lookup("to_date",
                       [string_data_type, string_data_type, int32_data_type],
                       date64_data_type)
      assert_equal(Gandiva::ResultNullableType::INTERNAL,
                   to_date.result_nullable_type)
    end
  end

  sub_test_case("need_context") do
    def test_need
      assert do
        not @not.need_context
      end
    end

    def test_not_need
      upper = lookup("upper",
                     [string_data_type],
                     string_data_type)
      assert do
        upper.need_context
      end
    end
  end

  sub_test_case("need_function_holder") do
    def test_need
      like = lookup("like",
                    [string_data_type, string_data_type],
                    boolean_data_type)
      assert do
        like.need_function_holder
      end
    end

    def test_not_need
      assert do
        not @not.need_function_holder
      end
    end
  end

  sub_test_case("can_return_errors") do
    def test_can
      divide = lookup("divide",
                      [int8_data_type, int8_data_type],
                      int8_data_type)
      assert do
        divide.can_return_errors?
      end
    end

    def test_not_can
      assert do
        not @not.can_return_errors?
      end
    end
  end
end
