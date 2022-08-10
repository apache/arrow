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

class TestMapScalar < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @value = build_struct_array([
                                  Arrow::Field.new("key",
                                                   Arrow::StringDataType.new,
                                                   false),
                                  Arrow::Field.new("value",
                                                   Arrow::Int8DataType.new),
                                ],
                                [
                                  {
                                    "key" => "hello",
                                    "value" => 1,
                                  },
                                  {
                                    "key" => "world",
                                    "value" => 2,
                                  },
                                ])
    @scalar = Arrow::MapScalar.new(@value)
  end

  def test_data_type
    assert_equal(@value.value_data_type,
                 @scalar.data_type)
  end

  def test_valid?
    assert do
      @scalar.valid?
    end
  end

  def test_equal
    assert_equal(Arrow::MapScalar.new(@value),
                 @scalar)
  end

  def test_to_s
    assert_equal(<<-MAP.strip, @scalar.to_s)
map<string, int8>[{key:string = hello, value:int8 = 1}, {key:string = world, value:int8 = 2}]
                 MAP
  end

  def test_value
    assert_equal(@value, @scalar.value)
  end
end
