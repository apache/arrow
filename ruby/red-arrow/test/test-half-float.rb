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

class HalfFloatTest < Test::Unit::TestCase
  sub_test_case(".new") do
    test("Array") do
      positive_infinity = Arrow::HalfFloat.new(0b1, 0b11111, 0b0000000000)
      assert_equal([0b1, 0b11111, 0b0000000000],
                   [
                     positive_infinity.sign,
                     positive_infinity.exponent,
                     positive_infinity.fraction,
                   ])
    end

    test("Integer - 0") do
      zero = Arrow::HalfFloat.new(0)
      assert_equal([0b0, 0b00000, 0b0000000000],
                   [
                     zero.sign,
                     zero.exponent,
                     zero.fraction,
                   ])
    end

    test("Integer - +infinity") do
      positive_infinity = Arrow::HalfFloat.new(0x7c00)
      assert_equal([0b0, 0b11111, 0b0000000000],
                   [
                     positive_infinity.sign,
                     positive_infinity.exponent,
                     positive_infinity.fraction,
                   ])
    end

    test("Integer - -infinity") do
      negative_infinity = Arrow::HalfFloat.new(0xfc00)
      assert_equal([0b1, 0b11111, 0b0000000000],
                   [
                     negative_infinity.sign,
                     negative_infinity.exponent,
                     negative_infinity.fraction,
                   ])
    end

    test("Integer - 1/3") do
      one_thirds = Arrow::HalfFloat.new(0x3555)
      assert_equal([0b0, 0b01101, 0b0101010101],
                   [
                     one_thirds.sign,
                     one_thirds.exponent,
                     one_thirds.fraction,
                   ])
    end

    test("Float - 0") do
      zero = Arrow::HalfFloat.new(0.0)
      assert_equal([0b0, 0b00000, 0b0000000000],
                   [
                     zero.sign,
                     zero.exponent,
                     zero.fraction,
                   ])
    end

    test("Float - too large") do
      positive_infinity = Arrow::HalfFloat.new(65504.1)
      assert_equal([0b0, 0b11111, 0b0000000000],
                   [
                     positive_infinity.sign,
                     positive_infinity.exponent,
                     positive_infinity.fraction,
                   ])
    end

    test("Float - +infinity") do
      positive_infinity = Arrow::HalfFloat.new(Float::INFINITY)
      assert_equal([0b0, 0b11111, 0b0000000000],
                   [
                     positive_infinity.sign,
                     positive_infinity.exponent,
                     positive_infinity.fraction,
                   ])
    end

    test("Float - too small") do
      negative_infinity = Arrow::HalfFloat.new(-65504.1)
      assert_equal([0b1, 0b11111, 0b0000000000],
                   [
                     negative_infinity.sign,
                     negative_infinity.exponent,
                     negative_infinity.fraction,
                   ])
    end

    test("Float - -infinity") do
      negative_infinity = Arrow::HalfFloat.new(-Float::INFINITY)
      assert_equal([0b1, 0b11111, 0b0000000000],
                   [
                     negative_infinity.sign,
                     negative_infinity.exponent,
                     negative_infinity.fraction,
                   ])
    end

    test("Float - 1/3") do
      one_thirds = Arrow::HalfFloat.new((2 ** -2) * (1 + 341 / 1024.0))
      assert_equal([0b0, 0b01101, 0b0101010101],
                   [
                     one_thirds.sign,
                     one_thirds.exponent,
                     one_thirds.fraction,
                   ])
    end
  end
end
