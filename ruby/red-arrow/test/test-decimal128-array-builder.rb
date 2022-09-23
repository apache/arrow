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

class Decimal128ArrayBuilderTest < Test::Unit::TestCase
  def setup
    @data_type = Arrow::Decimal128DataType.new(3, 1)
    @builder = Arrow::Decimal128ArrayBuilder.new(@data_type)
  end

  sub_test_case("#append_value") do
    test("nil") do
      @builder.append_value(nil)
      array = @builder.finish
      assert_equal(nil, array[0])
    end

    test("Arrow::Decimal128") do
      @builder.append_value(Arrow::Decimal128.new("10.1"))
      array = @builder.finish
      assert_equal(BigDecimal("10.1"),
                   array[0])
    end

    test("String") do
      @builder.append_value("10.1")
      array = @builder.finish
      assert_equal(BigDecimal("10.1"),
                   array[0])
    end

    test("Float") do
      @builder.append_value(10.1)
      array = @builder.finish
      assert_equal(BigDecimal("10.1"),
                   array[0])
    end

    test("BigDecimal") do
      @builder.append_value(BigDecimal("10.1"))
      array = @builder.finish
      assert_equal(BigDecimal("10.1"),
                   array[0])
    end

    test("BigDecimal::NAN") do
      message = "can't use NaN as an Arrow::Decimal128Array value"
      assert_raise(FloatDomainError.new(message)) do
        @builder.append_value(BigDecimal::NAN)
      end
    end

    test("BigDecimal::INFINITY") do
      message = "can't use Infinity as an Arrow::Decimal128Array value"
      assert_raise(FloatDomainError.new(message)) do
        @builder.append_value(BigDecimal::INFINITY)
      end
    end
  end

  sub_test_case("#append_values") do
    test("mixed") do
      @builder.append_values([
                               Arrow::Decimal128.new("10.1"),
                               nil,
                               "10.1",
                               10.1,
                               BigDecimal("10.1"),
                             ])
      array = @builder.finish
      assert_equal([
                     BigDecimal("10.1"),
                     nil,
                     BigDecimal("10.1"),
                     BigDecimal("10.1"),
                     BigDecimal("10.1"),
                   ],
                   array.to_a)
    end

    test("is_valids") do
      @builder.append_values([
                               Arrow::Decimal128.new("10.1"),
                               Arrow::Decimal128.new("10.1"),
                               Arrow::Decimal128.new("10.1"),
                             ],
                             [
                               true,
                               false,
                               true,
                             ])
      array = @builder.finish
      assert_equal([
                     BigDecimal("10.1"),
                     nil,
                     BigDecimal("10.1"),
                   ],
                   array.to_a)
    end

    test("packed") do
      @builder.append_values(Arrow::Decimal128.new("10.1").to_bytes.to_s * 3,
                             [true, false, true])
      array = @builder.finish
      assert_equal([
                     BigDecimal("10.1"),
                     nil,
                     BigDecimal("10.1"),
                   ],
                   array.to_a)
    end
  end
end
