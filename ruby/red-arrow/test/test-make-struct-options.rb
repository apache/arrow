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

class TestMakeStructOptions < Test::Unit::TestCase
  def test_make_struct_function
    a = Arrow::Int8Array.new([1, 2, 3])
    b = Arrow::BooleanArray.new([true, false, nil])
    metadata1 = {"a" => "b"}
    metadata2 = {"c" => "d"}
    options = {
      field_names: ["a", "b"],
      field_nullability: [false, true],
      field_metadata: [metadata1, metadata2]
    }
    args = [a, b]
    make_struct_function = Arrow::Function.find("make_struct")
    result = make_struct_function.execute(args, options).value

    expected = Arrow::StructArray.new(
      Arrow::StructDataType.new(
        [
          Arrow::Field.new("a", Arrow::Int8DataType.new, false),
          Arrow::Field.new("b", Arrow::BooleanDataType.new, true),
        ]
      ),
      [
        {"a" => 1, "b" => true},
        {"a" => 2, "b" => false},
        {"a" => 3, "b" => nil},
      ]
    )
    assert_equal(expected, result)
    fields = result.value_data_type.fields
    assert_equal(metadata1, fields[0].metadata)
    assert_equal(metadata2, fields[1].metadata)
  end
end
