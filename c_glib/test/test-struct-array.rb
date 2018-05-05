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

class TestStructArray < Test::Unit::TestCase
  include Helper::Buildable

  def test_new
    fields = [
      Arrow::Field.new("score", Arrow::Int8DataType.new),
      Arrow::Field.new("enabled", Arrow::BooleanDataType.new),
    ]
    structs = [
      {
        "score" => -29,
        "enabled" => true,
      },
      {
        "score" => 2,
        "enabled" => false,
      },
      nil,
    ]
    struct_array1 = build_struct_array(fields, structs)

    data_type = Arrow::StructDataType.new(fields)
    nulls = Arrow::Buffer.new([0b11].pack("C*"))
    children = [
      Arrow::Int8Array.new(2, Arrow::Buffer.new([-29, 2].pack("C*")), nulls, 0),
      Arrow::BooleanArray.new(2, Arrow::Buffer.new([0b01].pack("C*")), nulls, 0),
    ]
    assert_equal(struct_array1,
                 Arrow::StructArray.new(data_type,
                                        3,
                                        children,
                                        Arrow::Buffer.new([0b011].pack("C*")),
                                        -1))
  end

  def test_flatten
    fields = [
      Arrow::Field.new("score", Arrow::Int8DataType.new),
      Arrow::Field.new("enabled", Arrow::BooleanDataType.new),
    ]
    data_type = Arrow::StructDataType.new(fields)
    builder = Arrow::StructArrayBuilder.new(data_type)

    builder.append
    builder.get_field_builder(0).append(-29)
    builder.get_field_builder(1).append(true)

    builder.append
    builder.field_builders[0].append(2)
    builder.field_builders[1].append(false)

    array = builder.finish
    values = array.length.times.collect do |i|
      if i.zero?
        [
          array.get_field(0).get_value(i),
          array.get_field(1).get_value(i),
        ]
      else
        array.flatten.collect do |field|
          field.get_value(i)
        end
      end
    end
    assert_equal([
                   [-29, true],
                   [2, false],
                 ],
                 values)
  end
end
