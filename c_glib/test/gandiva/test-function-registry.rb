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

class TestGandivaFunctionRegistry < Test::Unit::TestCase
  include Helper::DataType

  def setup
    omit("Gandiva is required") unless defined?(::Gandiva)
    @registry = Gandiva::FunctionRegistry.new
  end

  sub_test_case("lookup") do
    def test_found
      native_function = @registry.native_functions[0]
      assert_equal(native_function,
                   @registry.lookup(native_function.signature))
    end

    def test_not_found
      signature = Gandiva::FunctionSignature.new("nonexistent",
                                                 [],
                                                 boolean_data_type)
      assert_nil(@registry.lookup(signature))
    end
  end

  def test_native_functions
    assert_equal([Gandiva::NativeFunction],
                 @registry.native_functions.collect(&:class).uniq)
  end
end
