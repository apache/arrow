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

class TestDecimal128Array < Test::Unit::TestCase
  def test_format_value
    data_type = Arrow::Decimal128DataType.new(8, 2)
    builder = Arrow::Decimal128ArrayBuilder.new(data_type)
    decimal = Arrow::Decimal128.new("23423445")
    builder.append_value(decimal)
    array = builder.finish
    assert_equal("234234.45", array.format_value(0))
  end

  def test_value
    data_type = Arrow::Decimal128DataType.new(8, 2)
    builder = Arrow::Decimal128ArrayBuilder.new(data_type)
    decimal = Arrow::Decimal128.new("23423445")
    builder.append_value(decimal)
    array = builder.finish
    assert_equal("234234.45",
                 array.get_value(0).to_string_scale(array.value_data_type.scale))
  end
end
