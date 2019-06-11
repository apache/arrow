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

  def test_equal
    assert do
      @not == @registry.lookup(@not.signature)
    end
  end

  def test_not_equal
    assert do
      @not != @isnull
    end
  end

  def test_to_string
    assert_equal(@registry.native_functions[0].signature.to_s,
                 @registry.native_functions[0].to_s)
  end

  def test_get_result_nullable_type
    assert_equal(Gandiva::ResultNullableType::IF_NULL,
                 @registry.native_functions[0].result_nullable_type)

    isnull_int8 = lookup("isnull",
                         [int8_data_type],
                         boolean_data_type)
    assert_equal(Gandiva::ResultNullableType::NEVER,
                 isnull_int8.result_nullable_type)

    to_date = lookup("to_date",
                     [string_data_type, string_data_type, int32_data_type],
                     date64_data_type)
    assert_equal(Gandiva::ResultNullableType::INTERNAL,
                 to_date.result_nullable_type)
  end

  def test_need_context
    assert_false(@registry.native_functions[0].need_context)

    upper = lookup("upper",
                   [string_data_type],
                   string_data_type)
    assert_true(upper.need_context)
  end

  def test_need_function_holder
    assert_false(@registry.native_functions[0].need_function_holder)

    like = lookup("like",
                  [string_data_type, string_data_type],
                  boolean_data_type)
    assert_true(like.need_function_holder)
  end

  def test_can_return_errors
    assert do
      not @registry.native_functions[0].can_return_errors?
    end

    divide_int8 = lookup("divide",
                         [int8_data_type, int8_data_type],
                         int8_data_type)
    assert do
      divide_int8.can_return_errors?
    end
  end
end
