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

class TestListArray < Test::Unit::TestCase
  include Helper::Buildable

  def test_new
    field = Arrow::Field.new("value", Arrow::Int8DataType.new)
    data_type = Arrow::ListDataType.new(field)
    value_offsets = Arrow::Buffer.new([0, 2, 5, 5].pack("l*"))
    data = Arrow::Buffer.new([1, 2, 3, 4, 5].pack("c*"))
    nulls = Arrow::Buffer.new([0b11111].pack("C*"))
    values = Arrow::Int8Array.new(5, data, nulls, 0)
    assert_equal(build_list_array(Arrow::Int8DataType.new,
                                  [[1, 2], [3, 4, 5], nil]),
                 Arrow::ListArray.new(data_type,
                                      3,
                                      value_offsets,
                                      values,
                                      Arrow::Buffer.new([0b011].pack("C*")),
                                      -1))
  end

  def test_value
    field = Arrow::Field.new("value", Arrow::Int8DataType.new)
    data_type = Arrow::ListDataType.new(field)
    builder = Arrow::ListArrayBuilder.new(data_type)
    value_builder = builder.value_builder

    builder.append_value
    value_builder.append_value(-29)
    value_builder.append_value(29)

    builder.append_value
    value_builder.append_value(-1)
    value_builder.append_value(0)
    value_builder.append_value(1)

    array = builder.finish
    value = array.get_value(1)
    assert_equal([-1, 0, 1],
                 value.length.times.collect {|i| value.get_value(i)})
  end

  def test_value_type
    field = Arrow::Field.new("value", Arrow::Int8DataType.new)
    data_type = Arrow::ListDataType.new(field)
    builder = Arrow::ListArrayBuilder.new(data_type)
    array = builder.finish
    assert_equal(Arrow::Int8DataType.new, array.value_type)
  end
end
