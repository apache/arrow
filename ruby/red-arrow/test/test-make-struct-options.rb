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
  def setup
    @options = Arrow::MakeStructOptions.new
  end

  def test_field_names_property
    assert_equal([], @options.field_names)
    @options.field_names = ["a", "b", "c"]
    assert_equal(["a", "b", "c"], @options.field_names)
    @options.field_names = []
    assert_equal([], @options.field_names)
  end

  def test_field_nullability_property
    assert_equal([], @options.field_nullability)
    @options.field_nullability = [true, false, true]
    assert_equal([true, false, true], @options.field_nullability)
    @options.field_nullability = []
    assert_equal([], @options.field_nullability)
  end

  def test_field_metadata_property
    assert_equal([], @options.field_metadata)
    @options.field_metadata = [{"a" => "b"}, {"c" => "d"}]
    assert_equal([{"a" => "b"}, {"c" => "d"}], @options.field_metadata)
    @options.field_metadata = []
    assert_equal([], @options.field_metadata)
  end

  def test_make_struct_function
    a = Arrow::Int8Array.new([1, 2, 3])
    b = Arrow::BooleanArray.new([true, false, nil])
    args = [a, b]
    metadata1 = {"a" => "b"}
    metadata2 = {"c" => "d"}
    @options.field_names = ["a", "b"]
    @options.field_nullability = [false, true]
    @options.field_metadata = [metadata1, metadata2]
    make_struct_function = Arrow::Function.find("make_struct")
    result = make_struct_function.execute(args, @options).value

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
