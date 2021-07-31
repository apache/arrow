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

module ValuesMapArrayTests
  def build_data_type(item_type)
    Arrow::MapDataType.new(
      key: :string,
      item: item_type
    )
  end

  def build_array(item_type, values)
    Arrow::MapArray.new(build_data_type(item_type), values)
  end

  def test_null
    values = [
      {"key1" => nil},
      nil,
    ]
    target = build(:null, values)
    assert_equal(values, target.values)
  end

  def test_boolean
    values = [
      {"key1" => false},
      nil,
      {"key3" => nil},
    ]
    target = build(:boolean, values)
    assert_equal(values, target.values)
  end

  def test_int8
    values = [
      {"key1" => (2 ** 7) - 1},
      nil,
      {"key3" => nil},
    ]
    target = build(:int8, values)
    assert_equal(values, target.values)
  end

  def test_uint8
    values = [
      {"key1" => (2 ** 8) - 1},
      nil,
      {"key3" => nil},
    ]
    target = build(:uint8, values)
    assert_equal(values, target.values)
  end

  def test_uint16
    values = [
      {"key1" => (2 ** 16) - 1},
      nil,
      {"key3" => nil},
    ]
    target = build(:uint16, values)
    assert_equal(values, target.values)
  end

  def test_int32
    values = [
      {"key1" => -(2 ** 31)},
      nil,
      {"key3" => nil},
    ]
    target = build(:int32, values)
    assert_equal(values, target.values)
  end

  def test_uint32
    values = [
      {"key1" => (2 ** 32) - 1},
      nil,
      {"key3" => nil},
    ]
    target = build(:uint32, values)
    assert_equal(values, target.values)
  end

  def test_int64
    values = [
      {"key1" => -(2 ** 63)},
      nil,
      {"key3" => nil},
    ]
    target = build(:int64, values)
    assert_equal(values, target.values)
  end

  def test_uint64
    values = [
      {"key1" => (2 ** 64) - 1},
      nil,
      {"key3" => nil},
    ]
    target = build(:uint64, values)
    assert_equal(values, target.values)
  end

  def test_float
    values = [
      {"key1" => -1.0},
      nil,
      {"key3" => nil},
    ]
    target = build(:float, values)
    assert_equal(values, target.values)
  end

  def test_double
    values = [
      {"key1" => -1.0},
      nil,
      {"key3" => nil},
    ]
    target = build(:double, values)
    assert_equal(values, target.values)
  end

  def test_binary
    values = [
      {"key1" => "\xff".b},
      nil,
      {"key3" => nil},
    ]
    target = build(:binary, values)
    assert_equal(values, target.values)
  end

  def test_string
    values = [
      {"key1" => "Ruby"},
      nil,
      {"key3" => nil},
    ]
    target = build(:string, values)
    assert_equal(values, target.values)
  end

  def test_date32
    values = [
      {"key1" => Date.new(1960, 1, 1)},
      nil,
      {"key3" => nil},
    ]
    target = build(:date32, values)
    assert_equal(values, target.values)
  end

  def test_date64
    values = [
      {"key1" => DateTime.new(1960, 1, 1, 2, 9, 30)},
      nil,
      {"key3" => nil},
    ]
    target = build(:date64, values)
    assert_equal(values, target.values)
  end

  def test_timestamp_second
    values = [
      {"key1" => Time.parse("1960-01-01T02:09:30Z")},
      nil,
      {"key3" => nil},
    ]
    target = build({
                     type: :timestamp,
                     unit: :second,
                   },
                   values)
    assert_equal(values, target.values)
  end

  def test_timestamp_milli
    values = [
      {"key1" => Time.parse("1960-01-01T02:09:30.123Z")},
      nil,
      {"key3" => nil},
    ]
    target = build({
                     type: :timestamp,
                     unit: :milli,
                   },
                   values)
    assert_equal(values, target.values)
  end

  def test_timestamp_micro
    values = [
      {"key1" => Time.parse("1960-01-01T02:09:30.123456Z")},
      nil,
      {"key3" => nil},
    ]
    target = build({
                     type: :timestamp,
                     unit: :micro,
                   },
                   values)
    assert_equal(values, target.values)
  end

  def test_timestamp_nano
    values = [
      {"key1" => Time.parse("1960-01-01T02:09:30.123456789Z")},
      nil,
      {"key3" => nil},
    ]
    target = build({
                     type: :timestamp,
                     unit: :nano,
                   },
                   values)
    assert_equal(values, target.values)
  end

  def test_time32_second
    unit = Arrow::TimeUnit::SECOND
    values = [
      # 00:10:00
      {"key1" => Arrow::Time.new(unit, 60 * 10)},
      nil,
      {"key3" => nil},
    ]
    target = build({
                     type: :time32,
                     unit: :second,
                   },
                   values)
    assert_equal(values, target.values)
  end

  def test_time32_milli
    unit = Arrow::TimeUnit::MILLI
    values = [
      # 00:10:00.123
      {"key1" => Arrow::Time.new(unit, (60 * 10) * 1000 + 123)},
      nil,
      {"key3" => nil},
    ]
    target = build({
                     type: :time32,
                     unit: :milli,
                   },
                   values)
    assert_equal(values, target.values)
  end

  def test_time64_micro
    unit = Arrow::TimeUnit::MICRO
    values = [
      # 00:10:00.123456
      {"key1" => Arrow::Time.new(unit, (60 * 10) * 1_000_000 + 123_456)},
      nil,
      {"key3" => nil},
    ]
    target = build({
                     type: :time64,
                     unit: :micro,
                   },
                   values)
    assert_equal(values, target.values)
  end

  def test_time64_nano
    unit = Arrow::TimeUnit::NANO
    values = [
      # 00:10:00.123456789
      {"key1" => Arrow::Time.new(unit, (60 * 10) * 1_000_000_000 + 123_456_789)},
      nil,
      {"key3" => nil},
    ]
    target = build({
                     type: :time64,
                     unit: :nano,
                   },
                   values)
    assert_equal(values, target.values)
  end

  def test_decimal128
    values = [
      {"key1" => BigDecimal("92.92")},
      nil,
      {"key3" => nil},
    ]
    target = build({
                     type: :decimal128,
                     precision: 8,
                     scale: 2,
                   },
                   values)
    assert_equal(values, target.values)
  end

  def test_decimal256
    values = [
      {"key1" => BigDecimal("92.92")},
      nil,
      {"key3" => nil},
    ]
    target = build({
                     type: :decimal256,
                     precision: 38,
                     scale: 2,
                   },
                   values)
    assert_equal(values, target.values)
  end

  def test_list
    values = [
      {"key1" => [true, nil, false]},
      nil,
      {"key3" => nil},
    ]
    target = build({
                     type: :list,
                     field: {
                       name: :sub_element,
                       type: :boolean,
                     },
                   },
                   values)
    assert_equal(values, target.values)
  end

  def test_struct
    values = [
      {"key1" => {"field" => true}},
      nil,
      {"key3" => nil},
      {"key4" => {"field" => nil}},
    ]
    target = build({
                     type: :struct,
                     fields: [
                       {
                         name: :field,
                         type: :boolean,
                       },
                     ],
                   },
                   values)
    assert_equal(values, target.values)
  end

  def test_map
    values = [
      {"key1" => {"sub_key1" => true}},
      nil,
      {"key3" => nil},
      {"key4" => {"sub_key2" => nil}},
    ]
    target = build({
                     type: :map,
                     key: :string,
                     item: :boolean,
                   },
                   values)
    assert_equal(values, target.values)
  end

  def test_sparse_union
    omit("Need to add support for SparseUnionArrayBuilder")
    values = [
      {"key1" => {"field1" => true}},
      nil,
      {"key3" => nil},
      {"key4" => {"field2" => nil}},
    ]
    target = build({
                     type: :sparse_union,
                     fields: [
                       {
                         name: :field1,
                         type: :boolean,
                       },
                       {
                         name: :field2,
                         type: :uint8,
                       },
                     ],
                     type_codes: [0, 1],
                   },
                   values)
    assert_equal(values, target.values)
  end

  def test_dense_union
    omit("Need to add support for DenseUnionArrayBuilder")
    values = [
      {"key1" => {"field1" => true}},
      nil,
      {"key3" => nil},
      {"key4" => {"field2" => nil}},
    ]
    target = build({
                     type: :dense_union,
                     fields: [
                       {
                         name: :field1,
                         type: :boolean,
                       },
                       {
                         name: :field2,
                         type: :uint8,
                       },
                     ],
                     type_codes: [0, 1],
                   },
                   values)
    assert_equal(values, target.values)
  end

  def test_dictionary
    omit("Need to add support for DictionaryArrayBuilder")
    values = [
      {"key1" => "Ruby"},
      nil,
      {"key3" => nil},
      {"key4" => "GLib"},
    ]
    dictionary = Arrow::StringArray.new(["GLib", "Ruby"])
    target = build({
                     type: :dictionary,
                     index_data_type: :int8,
                     dictionary: dictionary,
                     ordered: true,
                   },
                   values)
    assert_equal(values, target.values)
  end
end

class ValuesArrayMapArrayTest < Test::Unit::TestCase
  include ValuesMapArrayTests

  def build(item_type, values)
    build_array(item_type, values)
  end
end

class ValuesChunkedArrayMapArrayTest < Test::Unit::TestCase
  include ValuesMapArrayTests

  def build(item_type, values)
    Arrow::ChunkedArray.new([build_array(item_type, values)])
  end
end
