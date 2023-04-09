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
      {"key1" => false, "key2" => nil},
      nil,
    ]
    target = build(:boolean, values)
    assert_equal(values, target.values)
  end

  def test_int8
    values = [
      {"key1" => (2 ** 7) - 1, "key2" => nil},
      nil,
    ]
    target = build(:int8, values)
    assert_equal(values, target.values)
  end

  def test_uint8
    values = [
      {"key1" => (2 ** 8) - 1, "key2" => nil},
      nil,
    ]
    target = build(:uint8, values)
    assert_equal(values, target.values)
  end

  def test_uint16
    values = [
      {"key1" => (2 ** 16) - 1, "key2" => nil},
      nil,
    ]
    target = build(:uint16, values)
    assert_equal(values, target.values)
  end

  def test_int32
    values = [
      {"key1" => -(2 ** 31), "key2" => nil},
      nil,
    ]
    target = build(:int32, values)
    assert_equal(values, target.values)
  end

  def test_uint32
    values = [
      {"key1" => (2 ** 32) - 1, "key2" => nil},
      nil,
    ]
    target = build(:uint32, values)
    assert_equal(values, target.values)
  end

  def test_int64
    values = [
      {"key1" => -(2 ** 63), "key2" => nil},
      nil,
    ]
    target = build(:int64, values)
    assert_equal(values, target.values)
  end

  def test_uint64
    values = [
      {"key1" => (2 ** 64) - 1, "key2" => nil},
      nil,
    ]
    target = build(:uint64, values)
    assert_equal(values, target.values)
  end

  def test_float
    values = [
      {"key1" => -1.0, "key2" => nil},
      nil,
    ]
    target = build(:float, values)
    assert_equal(values, target.values)
  end

  def test_double
    values = [
      {"key1" => -1.0, "key2" => nil},
      nil,
    ]
    target = build(:double, values)
    assert_equal(values, target.values)
  end

  def test_binary
    values = [
      {"key1" => "\xff".b, "key2" => nil},
      nil,
    ]
    target = build(:binary, values)
    assert_equal(values, target.values)
  end

  def test_string
    values = [
      {"key1" => "Ruby", "key2" => nil},
      nil,
    ]
    target = build(:string, values)
    assert_equal(values, target.values)
  end

  def test_date32
    values = [
      {"key1" => Date.new(1960, 1, 1), "key2" => nil},
      nil,
    ]
    target = build(:date32, values)
    assert_equal(values, target.values)
  end

  def test_date64
    values = [
      {"key1" => DateTime.new(1960, 1, 1, 2, 9, 30), "key2" => nil},
      nil,
    ]
    target = build(:date64, values)
    assert_equal(values, target.values)
  end

  def test_timestamp_second
    values = [
      {"key1" => Time.parse("1960-01-01T02:09:30Z"), "key2" => nil},
      nil,
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
      {"key1" => Time.parse("1960-01-01T02:09:30.123Z"), "key2" => nil},
      nil,
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
      {"key1" => Time.parse("1960-01-01T02:09:30.123456Z"), "key2" => nil},
      nil,
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
      {"key1" => Time.parse("1960-01-01T02:09:30.123456789Z"), "key2" => nil},
      nil,
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
      {"key1" => Arrow::Time.new(unit, 60 * 10), "key2" => nil},
      nil,
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
      {"key1" => Arrow::Time.new(unit, (60 * 10) * 1000 + 123), "key2" => nil},
      nil,
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
      {"key1" => Arrow::Time.new(unit, (60 * 10) * 1_000_000 + 123_456), "key2" => nil},
      nil,
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
      {"key1" => Arrow::Time.new(unit, (60 * 10) * 1_000_000_000 + 123_456_789), "key2" => nil},
      nil,
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
      {"key1" => BigDecimal("92.92"), "key2" => nil},
      nil,
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
      {"key1" => BigDecimal("92.92"), "key2" => nil},
      nil,
    ]
    target = build({
                     type: :decimal256,
                     precision: 38,
                     scale: 2,
                   },
                   values)
    assert_equal(values, target.values)
  end

  def test_month_interval
    values = [
      {"key1" => 1, "key2" => nil},
      nil,
    ]
    target = build(:month_interval, values)
    assert_equal(values, target.values)
  end

  def test_day_time_interval
    values = [
      {
        "key1" => {day: 1, millisecond: 100},
        "key2" => nil,
      },
      nil,
    ]
    target = build(:day_time_interval, values)
    assert_equal(values, target.values)
  end

  def test_month_day_nano_interval
    values = [
      {
        "key1" => {month: 1, day: 1, nanosecond: 100},
        "key2" => nil,
      },
      nil,
    ]
    target = build(:month_day_nano_interval, values)
    assert_equal(values, target.values)
  end

  def test_list
    values = [
      {"key1" => [true, nil, false], "key2" => nil},
      nil,
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
      {"key1" => {"field" => true}, "key2" => nil, "key3" => {"field" => nil}},
      nil,
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
      {"key1" => {"sub_key1" => true, "sub_key2" => nil}, "key2" => nil},
      nil,
    ]
    target = build({
                     type: :map,
                     key: :string,
                     item: :boolean,
                   },
                   values)
    assert_equal(values, target.values)
  end

  def remove_union_field_names(values)
    values.collect do |value|
      if value.nil?
        value
      else
        val = {}
        value.each do |k, v|
          v = v.values[0] unless v.nil?
          val[k] = v
        end
        val
      end
    end
  end

  def test_sparse_union
    values = [
      {
        "key1" => {"field1" => true},
        "key2" => nil,
        "key3" => {"field2" => 29},
        "key4" => {"field2" => nil},
      },
      nil,
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
    assert_equal(remove_union_field_names(values),
                 target.values)
  end

  def test_dense_union
    values = [
      {
        "key1" => {"field1" => true},
        "key2" => nil,
        "key3" => {"field2" => 29},
        "key4" => {"field2" => nil},
      },
      nil,
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
    assert_equal(remove_union_field_names(values),
                 target.values)
  end

  def test_dictionary
    values = [
      {"key1" => "Ruby", "key2" => nil, "key3" => "GLib"},
      nil,
    ]
    target = build({
                     type: :dictionary,
                     index_data_type: :int8,
                     value_data_type: :string,
                     ordered: false,
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
