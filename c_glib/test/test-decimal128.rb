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

class TestDecimal128 < Test::Unit::TestCase
  include Helper::Omittable

  def test_new_string_invalid
    message =
      "[decimal128][new][string]: Invalid: " +
      "The string '1,1' is not a valid decimal128 number"
    error = assert_raise(Arrow::Error::Invalid) do
      Arrow::Decimal128.new("1,1")
    end
    assert_equal(message,
                 error.message.lines.first.chomp)
  end

  def test_copy
    decimal = Arrow::Decimal128.new("234.23445")
    assert_equal(decimal, decimal.copy)
  end

  def test_to_string_scale
    integer_data = 23423445
    string_data = "234.23445"
    decimal = Arrow::Decimal128.new(integer_data)
    assert_equal(string_data, decimal.to_string_scale(5))
  end

  def test_to_string
    string_data = "99999999999999999999999999999999999999"
    decimal = Arrow::Decimal128.new(string_data)
    assert_equal(string_data, decimal.to_s)
  end

  def test_to_bytes
    decimal = Arrow::Decimal128.new("12.3")
    assert_equal([123, 0].pack("q*"),
                 decimal.to_bytes.to_s)
  end

  def test_abs
    absolute_value = "23049223942343532412"
    negative_value = "-23049223942343532412"
    decimal = Arrow::Decimal128.new(negative_value)
    decimal.abs
    assert_equal(absolute_value, decimal.to_s)
  end

  def test_negate
    positive_value = "23049223942343532412"
    negative_value = "-23049223942343532412"
    decimal = Arrow::Decimal128.new(positive_value)
    decimal.negate
    assert_equal(negative_value, decimal.to_s)
    decimal.negate
    assert_equal(positive_value, decimal.to_s)
  end

  def test_to_integer
    integer_data = 999999999999999999
    decimal = Arrow::Decimal128.new(integer_data)
    assert_equal(integer_data, decimal.to_i)
  end

  def test_plus
    integer_data1 = 23423445
    integer_data2 = 5443
    decimal1 = Arrow::Decimal128.new(integer_data1)
    decimal2 = Arrow::Decimal128.new(integer_data2)
    decimal3 = decimal1.plus(decimal2)
    assert_equal(integer_data1 + integer_data2, decimal3.to_i)
  end

  def test_minus
    integer_data1 = 23423445
    integer_data2 = 5443
    decimal1 = Arrow::Decimal128.new(integer_data1)
    decimal2 = Arrow::Decimal128.new(integer_data2)
    decimal3 = decimal1.minus(decimal2)
    assert_equal(integer_data1 - integer_data2, decimal3.to_i)
  end

  def test_multiply
    integer_data1 = 23423445
    integer_data2 = 5443
    decimal1 = Arrow::Decimal128.new(integer_data1)
    decimal2 = Arrow::Decimal128.new(integer_data2)
    decimal3 = decimal1.multiply(decimal2)
    assert_equal(integer_data1 * integer_data2, decimal3.to_i)
  end

  def test_divide
    require_gi_bindings(3, 3, 0)
    integer_data1 = 23423445
    integer_data2 = -5443
    decimal1 = Arrow::Decimal128.new(integer_data1)
    decimal2 = Arrow::Decimal128.new(integer_data2)
    result, remainder = decimal1.divide(decimal2)
    assert_equal([
                   integer_data1.quo(integer_data2).truncate,
                   integer_data1.remainder(integer_data2),
                 ],
                 [result.to_i, remainder.to_i])
  end

  def test_divide_zero
    require_gi_bindings(3, 3, 0)
    decimal1 = Arrow::Decimal128.new(23423445)
    decimal2 = Arrow::Decimal128.new(0)
    message =
      "[decimal128][divide]: Invalid: Division by 0 in Decimal"
    assert_raise(Arrow::Error::Invalid.new(message)) do
      decimal1.divide(decimal2)
    end
  end

  def test_equal
    decimal = Arrow::Decimal128.new(10)
    other_decimal1 = Arrow::Decimal128.new(10)
    other_decimal2 = Arrow::Decimal128.new(11)
    assert_equal([
                   true,
                   false,
                 ],
                 [
                   decimal == other_decimal1,
                   decimal == other_decimal2,
                 ])
  end

  def test_not_equal
    require_gi_bindings(3, 3, 1)
    decimal = Arrow::Decimal128.new(10)
    other_decimal1 = Arrow::Decimal128.new(10)
    other_decimal2 = Arrow::Decimal128.new(11)
    assert_equal([
                   false,
                   true,
                 ],
                 [
                   decimal != other_decimal1,
                   decimal != other_decimal2,
                 ])
  end

  def test_less_than
    require_gi_bindings(3, 3, 1)
    decimal = Arrow::Decimal128.new(10)
    other_decimal1 = Arrow::Decimal128.new(11)
    other_decimal2 = Arrow::Decimal128.new(9)
    assert_equal([
                   true,
                   false,
                   false
                 ],
                 [
                   decimal < other_decimal1,
                   decimal < other_decimal2,
                   decimal < decimal,
                 ])
  end

  def test_less_than_or_equal
    require_gi_bindings(3, 3, 1)
    decimal = Arrow::Decimal128.new(10)
    other_decimal1 = Arrow::Decimal128.new(11)
    other_decimal2 = Arrow::Decimal128.new(9)
    assert_equal([
                   true,
                   false,
                   true
                 ],
                 [
                   decimal <= other_decimal1,
                   decimal <= other_decimal2,
                   decimal <= decimal
                 ])
  end

  def test_greater_than
    require_gi_bindings(3, 3, 1)
    decimal = Arrow::Decimal128.new(10)
    other_decimal1 = Arrow::Decimal128.new(11)
    other_decimal2 = Arrow::Decimal128.new(9)
    assert_equal([
                   false,
                   true,
                   false
                 ],
                 [
                   decimal > other_decimal1,
                   decimal > other_decimal2,
                   decimal > decimal
                 ])
  end

  def test_greater_than_or_equal
    require_gi_bindings(3, 3, 1)
    decimal = Arrow::Decimal128.new(10)
    other_decimal1 = Arrow::Decimal128.new(11)
    other_decimal2 = Arrow::Decimal128.new(9)
    assert_equal([
                   false,
                   true,
                   true
                 ],
                 [
                   decimal >= other_decimal1,
                   decimal >= other_decimal2,
                   decimal >= decimal
                 ])
  end

  def test_rescale
    decimal = Arrow::Decimal128.new(10)
    assert_equal(Arrow::Decimal128.new(1000),
                 decimal.rescale(1, 3))
  end

  def test_rescale_fail
    decimal = Arrow::Decimal128.new(10)
    message =
      "[decimal128][rescale]: Invalid: " +
      "Rescaling Decimal value would cause data loss"
    assert_raise(Arrow::Error::Invalid.new(message)) do
      decimal.rescale(1, -1)
    end
  end
end
