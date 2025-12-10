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

class TestFixedSizeListArrayBuilder < Test::Unit::TestCase
  include Helper::Buildable
  include Helper::Omittable

  def setup
    require_gi_bindings(3, 1, 9)
  end

  def get_value(array, i)
    value = array.get_value(i)
    value.length.times.collect do |j|
      value.get_value(j)
    end
  end

  def test_new
    field = Arrow::Field.new("value", Arrow::Int8DataType.new)
    data_type = Arrow::FixedSizeListDataType.new(field, 2)
    builder = Arrow::FixedSizeListArrayBuilder.new(data_type)
    assert_equal(data_type, builder.value_data_type)
  end

  def test_append_value
    field = Arrow::Field.new("value", Arrow::Int8DataType.new)
    data_type = Arrow::FixedSizeListDataType.new(field, 2)
    builder = Arrow::FixedSizeListArrayBuilder.new(data_type)
    value_builder = builder.value_builder

    # Append first list: [1, 2]
    builder.append_value
    value_builder.append_value(1)
    value_builder.append_value(2)

    # Append second list: [3, 4]
    builder.append_value
    value_builder.append_value(3)
    value_builder.append_value(4)

    array = builder.finish
    assert_equal(2, array.length)
    assert_equal([1, 2], get_value(array, 0))
    assert_equal([3, 4], get_value(array, 1))
  end

  def test_append_null
    field = Arrow::Field.new("value", Arrow::Int8DataType.new)
    data_type = Arrow::FixedSizeListDataType.new(field, 2)
    builder = Arrow::FixedSizeListArrayBuilder.new(data_type)
    value_builder = builder.value_builder

    # Append first list: [1, 2]
    builder.append_value
    value_builder.append_value(1)
    value_builder.append_value(2)

    # Append null list
    builder.append_null

    # Append third list: [5, 6]
    builder.append_value
    value_builder.append_value(5)
    value_builder.append_value(6)

    array = builder.finish
    assert_equal(3, array.length)
    assert_equal([1, 2], get_value(array, 0))
    assert do
      array.null?(1)
    end
    assert_equal([5, 6], get_value(array, 2))
  end

  def test_value_builder
    field = Arrow::Field.new("value", Arrow::Int8DataType.new)
    data_type = Arrow::FixedSizeListDataType.new(field, 2)
    builder = Arrow::FixedSizeListArrayBuilder.new(data_type)
    value_builder = builder.value_builder
    assert_equal(Arrow::Int8DataType.new, value_builder.value_data_type)
  end
end

