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
  def setup
    omit("Gandiva is required") unless defined?(::Gandiva)
    @registry = Gandiva::FunctionRegistry.new
  end

  def test_get_signature
    assert_kind_of(Gandiva::FunctionSignature,
                   @registry.native_functions[0].signature)
  end

  def test_to_string
    assert_equal("bool not(bool)",
                 @registry.native_functions[0].to_s)
  end

  def test_get_result_nullable_type
    assert_equal(Gandiva::ResultNullableType::IF_NULL,
                 @registry.native_functions[0].result_nullable_type)

    isnull_int8 = @registry.native_functions.find do |nf|
      nf.to_s == "bool isnull(int8)"
    end
    assert_equal(Gandiva::ResultNullableType::NEVER,
                 isnull_int8.result_nullable_type)

    to_date = @registry.native_functions.find do |nf|
      nf.to_s == "date64[ms] to_date(string, string, int32)"
    end
    assert_equal(Gandiva::ResultNullableType::INTERNAL,
                 to_date.result_nullable_type)
  end

  def test_need_context
    assert_false(@registry.native_functions[0].need_context)

    upper = @registry.native_functions.find do |nf|
      nf.to_s == "string upper(string)"
    end
    assert_true(upper.need_context)
  end

  def test_need_function_holder
    assert_false(@registry.native_functions[0].need_function_holder)

    like = @registry.native_functions.find do |nf|
      nf.to_s == "bool like(string, string)"
    end
    assert_true(like.need_function_holder)
  end

  def test_can_return_errors
    assert_false(@registry.native_functions[0].can_return_errors?)

    divide_int8 = @registry.native_functions.find do |nf|
      nf.to_s == "int8 divide(int8, int8)"
    end
    assert_true(divide_int8.can_return_errors?)
  end
end
