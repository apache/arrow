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

class ScalarTest < Test::Unit::TestCase
  sub_test_case(".resolve") do
    test("Scalar") do
      assert_equal(Arrow::Int32Scalar.new(29),
                   Arrow::Scalar.resolve(Arrow::Int32Scalar.new(29)))
    end

    test("true") do
      assert_equal(Arrow::BooleanScalar.new(true),
                   Arrow::Scalar.resolve(true))
    end

    test("false") do
      assert_equal(Arrow::BooleanScalar.new(false),
                   Arrow::Scalar.resolve(false))
    end

    test("Symbol") do
      assert_equal(Arrow::StringScalar.new("hello"),
                   Arrow::Scalar.resolve(:hello))
    end

    test("String") do
      assert_equal(Arrow::StringScalar.new("hello"),
                   Arrow::Scalar.resolve("hello"))
    end

    test("Integer") do
      assert_equal(Arrow::Int64Scalar.new(-29),
                   Arrow::Scalar.resolve(-29))
    end

    test("Float") do
      assert_equal(Arrow::DoubleScalar.new(2.9),
                   Arrow::Scalar.resolve(2.9))
    end

    test("Int64Scalar, :int32") do
      assert_equal(Arrow::Int32Scalar.new(-29),
                   Arrow::Scalar.resolve(Arrow::Int64Scalar.new(-29), :int32))
    end

    test("Integer, :int32") do
      assert_equal(Arrow::Int32Scalar.new(-29),
                   Arrow::Scalar.resolve(-29, :int32))
    end
  end
end
