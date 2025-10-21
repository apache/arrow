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

class TestRunEndEncodedDataType < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @run_end_data_type = Arrow::Int32DataType.new
    @value_data_type = Arrow::StringDataType.new
    @data_type = Arrow::RunEndEncodedDataType.new(@run_end_data_type,
                                                  @value_data_type)
  end

  def test_type
    assert_equal(Arrow::Type::RUN_END_ENCODED, @data_type.id)
  end

  def test_name
    assert_equal("run_end_encoded", @data_type.name)
  end

  def test_to_s
    assert_equal("run_end_encoded<run_ends: int32, values: string>",
                 @data_type.to_s)
  end

  def test_bit_width
    assert_equal(-1, @data_type.bit_width)
  end

  def test_run_end_data_type
    assert_equal(@run_end_data_type, @data_type.run_end_data_type)
  end

  def test_value_data_type
    assert_equal(@value_data_type, @data_type.value_data_type)
  end
end
