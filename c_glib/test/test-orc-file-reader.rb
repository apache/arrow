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

class TestORCFileReader < Test::Unit::TestCase
  include Helper::Buildable
  include Helper::Omittable
  include Helper::Fixture

  def setup
    omit("Require Apache Arrow ORC") unless Arrow.const_defined?(:ORCFileReader)
    path = fixture_path("TestOrcFile.test1.orc")
    input = Arrow::MemoryMappedInputStream.new(path)
    @reader = Arrow::ORCFileReader.new(input)
  end

  def test_read_type
    assert_equal(<<-SCHEMA.chomp, @reader.read_type.to_s)
boolean1: bool
byte1: int8
short1: int16
int1: int32
long1: int64
float1: float
double1: double
bytes1: binary
string1: string
middle: struct<list: list<item: struct<int1: int32, string1: string>>>
list: list<item: struct<int1: int32, string1: string>>
map: list<item: struct<key: string, value: struct<int1: int32, string1: string>>>
    SCHEMA
  end

  def test_field_indices
    require_gi(1, 42, 0)
    require_gi_bindings(3, 2, 6)
    assert_nil(@reader.field_indices)
    @reader.field_indices = [1, 3]
    assert_equal([1, 3], @reader.field_indices)
  end

  def item_fields
    [
      Arrow::Field.new("int1", Arrow::Int32DataType.new),
      Arrow::Field.new("string1", Arrow::StringDataType.new),
    ]
  end

  def item_data_type
    Arrow::StructDataType.new(item_fields)
  end

  def build_items_array(items_array)
    build_list_array(item_data_type, items_array)
  end

  def items_data_type
    Arrow::ListDataType.new(Arrow::Field.new("item", item_data_type))
  end

  def middle_fields
    [
      Arrow::Field.new("list", items_data_type),
    ]
  end

  def build_middle_array(middles)
    build_struct_array(middle_fields, middles)
  end

  def key_value_fields
    [
      Arrow::Field.new("key", Arrow::StringDataType.new),
      Arrow::Field.new("value", item_data_type),
    ]
  end

  def key_value_data_type
    Arrow::StructDataType.new(key_value_fields)
  end

  def build_key_value_array(key_value_array)
    build_list_array(key_value_data_type, key_value_array)
  end

  def middle_array
    build_middle_array([
                         {
                           "list" => [
                             {
                               "int1" => 1,
                               "string1" => "bye",
                             },
                             {
                               "int1" => 2,
                               "string1" => "sigh",
                             },
                           ],
                         },
                         {
                           "list" => [
                             {
                               "int1" => 1,
                               "string1" => "bye",
                             },
                             {
                               "int1" => 2,
                               "string1" => "sigh",
                             },
                           ],
                         },
                       ])
  end

  def list_array
    build_items_array([
                        [
                          {
                            "int1" => 3,
                            "string1" => "good",
                          },
                          {
                            "int1" => 4,
                            "string1" => "bad",
                          },
                        ],
                        [
                          {
                            "int1" => 100000000,
                            "string1" => "cat",
                          },
                          {
                            "int1" => -100000,
                            "string1" => "in",
                          },
                          {
                            "int1" => 1234,
                            "string1" => "hat",
                          },
                        ]
                      ])
  end

  def map_array
    build_key_value_array([
                            [
                            ],
                            [
                              {
                                "key" => "chani",
                                "value" => {
                                  "int1" => 5,
                                  "string1" => "chani",
                                },
                              },
                              {
                                "key" => "mauddib",
                                "value" => {
                                  "int1" => 1,
                                  "string1" => "mauddib",
                                },
                              },
                            ],
                          ])
  end

  def all_columns
    {
      "boolean1" => build_boolean_array([false, true]),
      "byte1" => build_int8_array([1, 100]),
      "short1" => build_int16_array([1024, 2048]),
      "int1" => build_int32_array([65536, 65536]),
      "long1" => build_int64_array([
                                     9223372036854775807,
                                     9223372036854775807,
                                   ]),
      "float1" => build_float_array([1.0, 2.0]),
      "double1" => build_double_array([-15.0, -5.0]),
      "bytes1" => build_binary_array(["\x00\x01\x02\x03\x04", ""]),
      "string1" => build_string_array(["hi", "bye"]),
      "middle" => middle_array,
      "list" => list_array,
      "map" => map_array,
    }
  end

  sub_test_case("#read_stripes") do
    test("all") do
      assert_equal(build_table(all_columns),
                   @reader.read_stripes)
    end

    test("select fields") do
      require_gi_bindings(3, 2, 6)
      @reader.field_indices = [1, 3]
      assert_equal(build_table("boolean1" => build_boolean_array([false, true]),
                               "short1" => build_int16_array([1024, 2048])),
                   @reader.read_stripes)
    end
  end

  sub_test_case("#read_stripe") do
    test("all") do
      assert_equal(build_record_batch(all_columns),
                   @reader.read_stripe(0))
    end

    test("select fields") do
      require_gi_bindings(3, 2, 6)
      @reader.field_indices = [1, 3]
      boolean1 = build_boolean_array([false, true])
      short1 = build_int16_array([1024, 2048])
      assert_equal(build_record_batch("boolean1" => boolean1,
                                      "short1" => short1),
                   @reader.read_stripe(0))
    end
  end

  def test_n_stripes
    assert_equal(1, @reader.n_stripes)
  end

  def test_n_rows
    assert_equal(2, @reader.n_rows)
  end
end
