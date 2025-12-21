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
  include Helper::Buildable

  def setup
    @options = Arrow::MakeStructOptions.new
  end

  def test_field_names_property
    assert_equal([], @options.field_names)
    @options.field_names = ["a", "b", "c"]
    assert_equal(["a", "b", "c"], @options.field_names)
  end

  def test_add_field
    @options.add_field("a", true, nil)
    @options.add_field("b", false, nil)
    metadata = {"key1" => "value1", "key2" => "value2"}
    @options.add_field("c", true, metadata)
    assert_equal(["a", "b", "c"], @options.field_names)
  end

  def test_make_struct_function
    a = build_int8_array([1, 2, 3])
    b = build_boolean_array([true, false, nil])
    args = [
      Arrow::ArrayDatum.new(a),
      Arrow::ArrayDatum.new(b),
    ]
    @options.add_field("a", true, nil)
    @options.add_field("b", true, nil)
    make_struct_function = Arrow::Function.find("make_struct")
    result = make_struct_function.execute(args, @options).value

    expected = build_struct_array(
      [
        Arrow::Field.new("a", Arrow::Int8DataType.new),
        Arrow::Field.new("b", Arrow::BooleanDataType.new),
      ],
      [
        {"a" => 1, "b" => true},
        {"a" => 2, "b" => false},
        {"a" => 3, "b" => nil},
      ]
    )
    assert_equal(expected, result)
  end

  def test_make_struct_function_with_nullability
    a = build_int8_array([1, 2, 3])
    b = build_boolean_array([true, false, nil])
    args = [
      Arrow::ArrayDatum.new(a),
      Arrow::ArrayDatum.new(b),
    ]
    @options.add_field("a", false, nil)
    @options.add_field("b", true, nil)
    make_struct_function = Arrow::Function.find("make_struct")
    result = make_struct_function.execute(args, @options).value

    expected = build_struct_array(
      [
        Arrow::Field.new("a", Arrow::Int8DataType.new, false),
        Arrow::Field.new("b", Arrow::BooleanDataType.new, true),
      ],
      [
        {"a" => 1, "b" => true},
        {"a" => 2, "b" => false},
        {"a" => 3, "b" => nil},
      ]
    )
    assert_equal(expected, result)
  end

  def test_make_struct_function_with_metadata
    a = build_int8_array([1, 2, 3])
    b = build_boolean_array([true, false, nil])
    args = [
      Arrow::ArrayDatum.new(a),
      Arrow::ArrayDatum.new(b),
    ]
    metadata1 = {"key1" => "value1"}
    metadata2 = {"key2" => "value2"}
    @options.add_field("a", true, metadata1)
    @options.add_field("b", true, metadata2)
    make_struct_function = Arrow::Function.find("make_struct")
    result = make_struct_function.execute(args, @options).value

    fields = result.value_data_type.fields
    assert_equal(metadata1, fields[0].metadata)
    assert_equal(metadata2, fields[1].metadata)
  end
end
