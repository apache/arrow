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

module ValuesStructArrayTests
  def build_data_type(type)
    field_description = {
      name: :field,
    }
    if type.is_a?(Hash)
      field_description = field_description.merge(type)
    else
      field_description[:type] = type
    end
    Arrow::StructDataType.new([field_description])
  end

  def build_array(type, values)
    Arrow::StructArray.new(build_data_type(type), values)
  end

  def test_null
    values = [
      {"field" => nil},
      nil,
    ]
    target = build(:null, values)
    assert_equal(values, target.values)
  end

  def test_boolean
    values = [
      {"field" => true},
      nil,
      {"field" => nil},
    ]
    target = build(:boolean, values)
    assert_equal(values, target.values)
  end

  def test_int8
    values = [
      {"field" => -(2 ** 7)},
      nil,
      {"field" => nil},
    ]
    target = build(:int8, values)
    assert_equal(values, target.values)
  end

  def test_uint8
    values = [
      {"field" => (2 ** 8) - 1},
      nil,
      {"field" => nil},
    ]
    target = build(:uint8, values)
    assert_equal(values, target.values)
  end

  def test_int16
    values = [
      {"field" => -(2 ** 15)},
      nil,
      {"field" => nil},
    ]
    target = build(:int16, values)
    assert_equal(values, target.values)
  end

  def test_uint16
    values = [
      {"field" => (2 ** 16) - 1},
      nil,
      {"field" => nil},
    ]
    target = build(:uint16, values)
    assert_equal(values, target.values)
  end

  def test_int32
    values = [
      {"field" => -(2 ** 31)},
      nil,
      {"field" => nil},
    ]
    target = build(:int32, values)
    assert_equal(values, target.values)
  end

  def test_uint32
    values = [
      {"field" => (2 ** 32) - 1},
      nil,
      {"field" => nil},
    ]
    target = build(:uint32, values)
    assert_equal(values, target.values)
  end

  def test_int64
    values = [
      {"field" => -(2 ** 63)},
      nil,
      {"field" => nil},
    ]
    target = build(:int64, values)
    assert_equal(values, target.values)
  end

  def test_uint64
    values = [
      {"field" => (2 ** 64) - 1},
      nil,
      {"field" => nil},
    ]
    target = build(:uint64, values)
    assert_equal(values, target.values)
  end

  def test_float
    values = [
      {"field" => -1.0},
      nil,
      {"field" => nil},
    ]
    target = build(:float, values)
    assert_equal(values, target.values)
  end

  def test_double
    values = [
      {"field" => -1.0},
      nil,
      {"field" => nil},
    ]
    target = build(:double, values)
    assert_equal(values, target.values)
  end

  def test_binary
    values = [
      {"field" => "\xff".b},
      nil,
      {"field" => nil},
    ]
    target = build(:binary, values)
    assert_equal(values, target.values)
  end

  def test_string
    values = [
      {"field" => "Ruby"},
      nil,
      {"field" => nil},
    ]
    target = build(:string, values)
    assert_equal(values, target.values)
  end

  def test_date32
    values = [
      {"field" => Date.new(1960, 1, 1)},
      nil,
      {"field" => nil},
    ]
    target = build(:date32, values)
    assert_equal(values, target.values)
  end

  def test_date64
    values = [
      {"field" => DateTime.new(1960, 1, 1, 2, 9, 30)},
      nil,
      {"field" => nil},
    ]
    target = build(:date64, values)
    assert_equal(values, target.values)
  end

  def test_timestamp_second
    values = [
      {"field" => Time.parse("1960-01-01T02:09:30Z")},
      nil,
      {"field" => nil},
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
      {"field" => Time.parse("1960-01-01T02:09:30.123Z")},
      nil,
      {"field" => nil},
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
      {"field" => Time.parse("1960-01-01T02:09:30.123456Z")},
      nil,
      {"field" => nil},
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
      {"field" => Time.parse("1960-01-01T02:09:30.123456789Z")},
      nil,
      {"field" => nil},
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
      {"field" => Arrow::Time.new(unit, 60 * 10)},
      nil,
      {"field" => nil},
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
      {"field" => Arrow::Time.new(unit, (60 * 10) * 1000 + 123)},
      nil,
      {"field" => nil},
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
      {"field" => Arrow::Time.new(unit, (60 * 10) * 1_000_000 + 123_456)},
      nil,
      {"field" => nil},
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
      {"field" => Arrow::Time.new(unit, (60 * 10) * 1_000_000_000 + 123_456_789)},
      nil,
      {"field" => nil},
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
      {"field" => BigDecimal("92.92")},
      nil,
      {"field" => nil},
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
      {"field" => BigDecimal("92.92")},
      nil,
      {"field" => nil},
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
      {"field" => 1},
      nil,
      {"field" => nil},
    ]
    target = build(:month_interval, values)
    assert_equal(values, target.values)
  end

  def test_day_time_interval
    values = [
      {"field" => {day: 1, millisecond: 100}},
      nil,
      {"field" => nil},
    ]
    target = build(:day_time_interval, values)
    assert_equal(values, target.values)
  end

  def test_month_day_nano_interval
    values = [
      {"field" => {month: 1, day: 1, nanosecond: 100}},
      nil,
      {"field" => nil},
    ]
    target = build(:month_day_nano_interval, values)
    assert_equal(values, target.values)
  end

  def test_list
    values = [
      {"field" => [true, nil, false]},
      nil,
      {"field" => nil},
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
      {"field" => {"sub_field" => true}},
      nil,
      {"field" => nil},
      {"field" => {"sub_field" => nil}},
    ]
    target = build({
                     type: :struct,
                     fields: [
                       {
                         name: :sub_field,
                         type: :boolean,
                       },
                     ],
                   },
                   values)
    assert_equal(values, target.values)
  end

  def test_map
    values = [
      {"field" => {"key1" => true, "key2" => nil}},
      nil,
      {"field" => nil},
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
        v = value["field"]
        v = v.values[0] unless v.nil?
        {"field" => v}
      end
    end
  end

  def test_sparse_union
    values = [
      {"field" => {"field1" => true}},
      nil,
      {"field" => nil},
      {"field" => {"field2" => 29}},
      {"field" => {"field2" => nil}},
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
      {"field" => {"field1" => true}},
      nil,
      {"field" => nil},
      {"field" => {"field2" => 29}},
      {"field" => {"field2" => nil}},
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
      {"field" => "Ruby"},
      nil,
      {"field" => nil},
      {"field" => "GLib"},
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

class ValuesArrayStructArrayTest < Test::Unit::TestCase
  include ValuesStructArrayTests

  def build(type, values)
    build_array(type, values)
  end
end

class ValuesChunkedArrayStructArrayTest < Test::Unit::TestCase
  include ValuesStructArrayTests

  def build(type, values)
    Arrow::ChunkedArray.new([build_array(type, values)])
  end
end
