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

class DataTypeTest < Test::Unit::TestCase
  sub_test_case(".resolve") do
    test("DataType") do
      assert_equal(Arrow::BooleanDataType.new,
                   Arrow::DataType.resolve(Arrow::BooleanDataType.new))
    end

    test("String") do
      assert_equal(Arrow::BooleanDataType.new,
                   Arrow::DataType.resolve("boolean"))
    end

    test("Symbol") do
      assert_equal(Arrow::BooleanDataType.new,
                   Arrow::DataType.resolve(:boolean))
    end

    test("Array") do
      field = Arrow::Field.new(:visible, :boolean)
      assert_equal(Arrow::ListDataType.new(field),
                   Arrow::DataType.resolve([:list, field]))
    end

    sub_test_case("Hash") do
      test("with type name") do
        field = Arrow::Field.new(:visible, :boolean)
        assert_equal(Arrow::ListDataType.new(field),
                     Arrow::DataType.resolve(type: :list, field: field))
      end

      test("with class") do
        assert_equal(Arrow::DataType.resolve(type: :decimal128, precision: 8, scale: 2),
                     Arrow::DataType.resolve(type: Arrow::Decimal128DataType.new(8, 2)))
      end
    end
  end
end
