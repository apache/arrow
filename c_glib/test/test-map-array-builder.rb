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

class TestMapArrayBuilder < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    offsets = build_int32_array([0, 2, 5])
    keys = build_string_array(["a", "b", "c", "d", "e"])
    items = build_int16_array([0, 1, 2, 3, 4])
    @map_array = Arrow::MapArray.new(offsets,
                                     keys,
                                     items)
    key_type = Arrow::StringDataType.new
    item_type = Arrow::Int16DataType.new
    data_type = Arrow::MapDataType.new(key_type, item_type)
    @builder = Arrow::MapArrayBuilder.new(data_type)
  end

  def test_append_value
    key_builder = @builder.key_builder
    item_builder = @builder.item_builder

    @builder.append_value
    key_builder.append_string("a")
    key_builder.append_string("b")
    item_builder.append_value(0)
    item_builder.append_value(1)

    @builder.append_value
    key_builder.append_string("c")
    key_builder.append_string("d")
    key_builder.append_string("e")
    item_builder.append_value(2)
    item_builder.append_value(3)
    item_builder.append_value(4)

    array = @builder.finish
    assert_equal([
                   @map_array.get_value(0),
                   @map_array.get_value(1)
                 ],
                 [
                   array.get_value(0),
                   array.get_value(1)
                 ])
  end

  def test_append_values
    key_builder = @builder.key_builder
    item_builder = @builder.item_builder
    @builder.append_values([0, 2, 5])
    key_builder.append_strings(["a", "b", "c", "d", "e"])
    item_builder.append_values([0, 1, 2, 3, 4])

    array = @builder.finish
    assert_equal([
                   @map_array.get_value(0),
                   @map_array.get_value(1)
                 ],
                 [
                   array.get_value(0),
                   array.get_value(1)
                 ])
  end

  def test_append_structs
    value_builder = @builder.value_builder

    @builder.append_value
    value_builder.append_value
    value_builder.get_field_builder(0).append_string("a")
    value_builder.get_field_builder(0).append_string("b")
    value_builder.get_field_builder(1).append_value(0)
    value_builder.get_field_builder(1).append_value(1)

    @builder.append_value
    value_builder.append_value
    value_builder.get_field_builder(0).append_string("c")
    value_builder.get_field_builder(0).append_string("d")
    value_builder.get_field_builder(0).append_string("e")
    value_builder.get_field_builder(1).append_value(2)
    value_builder.get_field_builder(1).append_value(3)
    value_builder.get_field_builder(1).append_value(4)

    array = @builder.finish
    assert_equal([
                   @map_array.get_value(0),
                   @map_array.get_value(1)
                 ],
                 [
                   array.get_value(0),
                   array.get_value(1)
                 ])
  end

  def test_append_null
    @builder.append_null
    @builder.append_null
    array = @builder.finish
    assert_equal(2, array.n_nulls)
  end

  def test_append_nulls
    @builder.append_nulls(2)
    array = @builder.finish
    assert_equal(2, array.n_nulls)
  end

  def test_append_empty_value
    offsets = build_int32_array([0, 0])
    keys = build_string_array([])
    items = build_int16_array([])
    expected_array = Arrow::MapArray.new(offsets, keys, items)
    @builder.append_empty_value
    assert_equal(expected_array,
                 @builder.finish)
  end

  def test_append_empty_values
    offsets = build_int32_array([0, 0, 0, 0])
    keys = build_string_array([])
    items = build_int16_array([])
    expected_array = Arrow::MapArray.new(offsets, keys, items)
    @builder.append_empty_values(3)
    assert_equal(expected_array,
                 @builder.finish)
  end
end
