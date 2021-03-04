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
  include Helper::DataType

  def setup
    omit("Gandiva is required") unless defined?(::Gandiva)
    parameter_types = [string_data_type, string_data_type, int32_data_type]
    @to_date = Gandiva::FunctionSignature.new("to_date",
                                              parameter_types,
                                              date64_data_type)
  end

  def test_new
    signature = Gandiva::FunctionSignature.new("add",
                                               [
                                                 int32_data_type,
                                                 int32_data_type,
                                               ],
                                               int32_data_type)
    assert_equal("int32 add(int32, int32)",
                 signature.to_s)
  end

  sub_test_case("equal") do
    def test_true
      add_int32_1 = Gandiva::FunctionSignature.new("add",
                                                   [
                                                     int32_data_type,
                                                     int32_data_type,
                                                   ],
                                                   int32_data_type)
      add_int32_2 = Gandiva::FunctionSignature.new("add",
                                                   [
                                                     int32_data_type,
                                                     int32_data_type,
                                                   ],
                                                   int32_data_type)
      assert do
        add_int32_1 == add_int32_2
      end
    end

    def test_false
      add_int32 = Gandiva::FunctionSignature.new("add",
                                                 [
                                                   int32_data_type,
                                                   int32_data_type,
                                                 ],
                                                 int32_data_type)
      add_int16 = Gandiva::FunctionSignature.new("add",
                                                 [
                                                   int16_data_type,
                                                   int16_data_type,
                                                 ],
                                                 int16_data_type)
      assert do
        add_int32 != add_int16
      end
    end
  end

  def test_to_string
    assert_equal("date64[ms] to_date(string, string, int32)",
                 @to_date.to_s)
  end

  def test_get_return_type
    assert_equal(date64_data_type,
                 @to_date.return_type)
  end

  def test_get_base_name
    assert_equal("to_date",
                 @to_date.base_name)
  end

  def test_get_param_types
    assert_equal([
                   string_data_type,
                   string_data_type,
                   int32_data_type,
                 ],
                 @to_date.param_types)
  end
end
