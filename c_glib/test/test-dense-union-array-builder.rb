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

class TestDenseUnionArrayBuilder < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @builder = Arrow::DenseUnionArrayBuilder.new
  end

  def create_dense_union_array(type_ids,
                               value_offsets,
                               fields,
                               data_type: nil)
    args = [
      build_int8_array(type_ids),
      build_int32_array(value_offsets),
      fields,
    ]
    args.unshift(data_type) if data_type
    Arrow::DenseUnionArray.new(*args)
  end

  def test_new_data_type
    number_field_data_type = Arrow::Int32DataType.new
    text_field_data_type = Arrow::StringDataType.new
    number_field = Arrow::Field.new("number", number_field_data_type)
    text_field = Arrow::Field.new("text", text_field_data_type)
    fields = [
      number_field,
      text_field,
    ]
    number_field_type_id = 2
    text_field_type_id = 9
    type_ids = [
      number_field_type_id,
      text_field_type_id,
    ]
    data_type = Arrow::DenseUnionDataType.new(fields, type_ids)

    builder = Arrow::DenseUnionArrayBuilder.new(data_type)
    number_builder = builder.get_child(0)
    text_builder = builder.get_child(1)

    expected_type_ids = []
    expected_value_offsets = []
    expected_number_values = []
    expected_text_values = []

    builder.append_value(2)
    number_builder.append_value(1)
    # expected
    expected_type_ids << number_field_type_id
    expected_value_offsets << expected_number_values.size
    expected_number_values << 1

    builder.append_value(text_field_type_id)
    text_builder.append_value("a")
    # expected
    expected_type_ids << text_field_type_id
    expected_value_offsets << expected_text_values.size
    expected_text_values << "a"

    expected_fields = [
      build_int32_array(expected_number_values),
      build_string_array(expected_text_values),
    ]
    assert_equal(create_dense_union_array(expected_type_ids,
                                          expected_value_offsets,
                                          expected_fields,
                                          data_type: data_type),
                 builder.finish)
  end

  def test_append_null
    int16_child_builder = Arrow::Int16ArrayBuilder.new
    int16_child_type_id = @builder.append_child(int16_child_builder, "0")

    type_ids = []
    value_offsets = []
    int16_child_values = []

    @builder.append_null
    # expected
    type_ids << int16_child_type_id
    value_offsets << int16_child_values.size
    int16_child_values << nil

    fields = [
      build_int16_array(int16_child_values),
    ]
    assert_equal(create_dense_union_array(type_ids, value_offsets, fields),
                 @builder.finish)
  end

  def test_append_nulls
    int16_child_builder = Arrow::Int16ArrayBuilder.new
    int16_child_type_id = @builder.append_child(int16_child_builder, "0")

    type_ids = []
    value_offsets = []
    int16_child_values = []

    @builder.append_nulls(3)
    # expected
    3.times do
      type_ids << int16_child_type_id
      value_offsets << int16_child_values.size
      int16_child_values << nil
    end

    fields = [
      build_int16_array(int16_child_values),
    ]
    assert_equal(create_dense_union_array(type_ids, value_offsets, fields),
                 @builder.finish)
  end

  def test_append_empty_value
    int16_child_builder = Arrow::Int16ArrayBuilder.new
    int16_child_type_id = @builder.append_child(int16_child_builder, "0")

    type_ids = []
    value_offsets = []
    int16_child_values = []

    @builder.append_empty_value
    # expected
    type_ids << int16_child_type_id
    value_offsets << int16_child_values.size
    int16_child_values << 0

    fields = [
      build_int16_array(int16_child_values),
    ]
    assert_equal(create_dense_union_array(type_ids, value_offsets, fields),
                 @builder.finish)
  end

  def test_append_empty_values
    int16_child_builder = Arrow::Int16ArrayBuilder.new
    int16_child_type_id = @builder.append_child(int16_child_builder, "0")

    type_ids = []
    value_offsets = []
    int16_child_values = []

    @builder.append_empty_values(3)
    # expected
    3.times do
      type_ids << int16_child_type_id
      value_offsets << int16_child_values.size
      int16_child_values << 0
    end

    fields = [
      build_int16_array(int16_child_values),
    ]
    assert_equal(create_dense_union_array(type_ids, value_offsets, fields),
                 @builder.finish)
  end

  def test_append_value
    int16_child_builder = Arrow::Int16ArrayBuilder.new
    int16_child_type_id = @builder.append_child(int16_child_builder, "0")
    string_child_builder = Arrow::StringArrayBuilder.new
    string_child_type_id = @builder.append_child(string_child_builder, "1")

    type_ids = []
    value_offsets = []
    int16_child_values = []
    string_child_values = []

    @builder.append_value(int16_child_type_id)
    int16_child_builder.append_value(1)
    # expected
    type_ids << int16_child_type_id
    value_offsets << int16_child_values.size
    int16_child_values << 1

    @builder.append_value(string_child_type_id)
    string_child_builder.append_value("a")
    # expected
    type_ids << string_child_type_id
    value_offsets << string_child_values.size
    string_child_values << "a"

    @builder.append_value(int16_child_type_id)
    int16_child_builder.append_null
    # expected
    type_ids << int16_child_type_id
    value_offsets << int16_child_values.size
    int16_child_values << nil

    @builder.append_value(string_child_type_id)
    string_child_builder.append_value("b")
    # expected
    type_ids << string_child_type_id
    value_offsets << string_child_values.size
    string_child_values << "b"

    @builder.append_value(string_child_type_id)
    string_child_builder.append_value("c")
    # expected
    type_ids << string_child_type_id
    value_offsets << string_child_values.size
    string_child_values << "c"

    fields = [
      build_int16_array(int16_child_values),
      build_string_array(string_child_values),
    ]
    assert_equal(create_dense_union_array(type_ids, value_offsets, fields),
                 @builder.finish)
  end
end
