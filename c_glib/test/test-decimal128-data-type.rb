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

class TestDecimal128DataType < Test::Unit::TestCase
  def test_type
    data_type = Arrow::Decimal128DataType.new(2, 0)
    assert_equal(Arrow::Type::DECIMAL128, data_type.id)
  end

  def test_name
    data_type = Arrow::Decimal128DataType.new(2, 0)
    assert_equal("decimal128", data_type.name)
  end

  def test_to_s
    data_type = Arrow::Decimal128DataType.new(2, 0)
    assert_equal("decimal128(2, 0)", data_type.to_s)
  end

  def test_precision
    data_type = Arrow::Decimal128DataType.new(8, 2)
    assert_equal(8, data_type.precision)
  end

  def test_scale
    data_type = Arrow::Decimal128DataType.new(8, 2)
    assert_equal(2, data_type.scale)
  end

  def test_decimal_data_type_new
    assert_equal(Arrow::Decimal128DataType.new(8, 2),
                 Arrow::DecimalDataType.new(8, 2))
  end

  def test_invalid_precision
    message =
      "[decimal128-data-type][new]: Invalid: Decimal precision out of range [1, 38]: 39"
    assert_raise(Arrow::Error::Invalid.new(message)) do
      Arrow::Decimal128DataType.new(39, 1)
    end
  end
end
