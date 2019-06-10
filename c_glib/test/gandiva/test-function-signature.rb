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

class TestGandivaFunctionSignature < Test::Unit::TestCase
  def setup
    omit("Gandiva is required") unless defined?(::Gandiva)
    @registry = Gandiva::FunctionRegistry.new
    @to_date = @registry.native_functions.find {|nf|
      nf.to_s == "date64[ms] to_date(string, string, int32)"
    }.signature
  end

  def test_new
    function_signature = Gandiva::FunctionSignature.new("add",
                                                        [
                                                          Arrow::Int32DataType.new,
                                                          Arrow::Int32DataType.new,
                                                        ],
                                                        Arrow::Int32DataType.new)
    assert_kind_of(Gandiva::FunctionSignature,
                   function_signature)
    assert_equal("int32 add(int32, int32)",
                 function_signature.to_s)
  end

  def test_equal
    assert_equal(Gandiva::FunctionSignature.new("add",
                                                [
                                                  Arrow::Int32DataType.new,
                                                  Arrow::Int32DataType.new,
                                                ],
                                                Arrow::Int32DataType.new),
                 Gandiva::FunctionSignature.new("add",
                                                [
                                                  Arrow::Int32DataType.new,
                                                  Arrow::Int32DataType.new,
                                                ],
                                                Arrow::Int32DataType.new))

    assert_not_equal(Gandiva::FunctionSignature.new("add",
                                                    [
                                                      Arrow::Int32DataType.new,
                                                      Arrow::Int32DataType.new,
                                                    ],
                                                    Arrow::Int32DataType.new),
                     Gandiva::FunctionSignature.new("sub",
                                                    [
                                                      Arrow::Int32DataType.new,
                                                      Arrow::Int32DataType.new,
                                                    ],
                                                    Arrow::Int32DataType.new))
  end

  def test_to_string
    assert_equal("date64[ms] to_date(string, string, int32)",
                 @to_date.to_s)
  end

  def test_get_return_type
    assert_equal(Arrow::Date64DataType.new,
                 @to_date.return_type)
  end

  def test_get_base_name
    assert_equal("to_date",
                 @to_date.base_name)
  end

  def test_get_param_types
    assert_equal([
                   Arrow::StringDataType.new,
                   Arrow::StringDataType.new,
                   Arrow::Int32DataType.new
                 ],
                 @to_date.param_types)
  end
end
