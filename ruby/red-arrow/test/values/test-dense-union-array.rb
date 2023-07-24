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

module ValuesDenseUnionArrayTests
  def build_data_type(type, type_codes)
    field_description = {}
    if type.is_a?(Hash)
      field_description = field_description.merge(type)
    else
      field_description[:type] = type
    end
    Arrow::DenseUnionDataType.new(fields: [
                                    field_description.merge(name: "0"),
                                    field_description.merge(name: "1"),
                                  ],
                                  type_codes: type_codes)
  end

  def build_array(type, values)
    type_codes = [0, 1]
    data_type = build_data_type(type, type_codes)
    type_ids = []
    offsets = []
    arrays = data_type.fields.collect do |field|
      sub_schema = Arrow::Schema.new([field])
      sub_records = []
      values.each do |value|
        next if value.nil?
        next unless value.key?(field.name)
        sub_records << [value[field.name]]
      end
      sub_record_batch = Arrow::RecordBatch.new(sub_schema,
                                                sub_records)
      sub_record_batch.columns[0].data
    end
    values.each do |value|
      if value.key?("0")
        type_id = type_codes[0]
        type_ids << type_id
        offsets << (type_ids.count(type_id) - 1)
      elsif value.key?("1")
        type_id = type_codes[1]
        type_ids << type_id
        offsets << (type_ids.count(type_id) - 1)
      end
    end
    Arrow::DenseUnionArray.new(data_type,
                               Arrow::Int8Array.new(type_ids),
                               Arrow::Int32Array.new(offsets),
                               arrays)
  end

  def remove_field_names(values)
    values.collect do |value|
      if value.nil?
        value
      else
        value.values[0]
      end
    end
  end

  def test_null
    values = [
      {"0" => nil},
    ]
    target = build(:null, values)
    assert_equal(remove_field_names(values),
                 target.values)
  end

  def test_boolean
    values = [
      {"0" => true},
      {"1" => nil},
    ]
    target = build(:boolean, values)
    assert_equal(remove_field_names(values),
                 target.values)
  end

  def test_int8
    values = [
      {"0" => -(2 ** 7)},
      {"1" => nil},
    ]
    target = build(:int8, values)
    assert_equal(remove_field_names(values),
                 target.values)
  end

  def test_uint8
    values = [
      {"0" => (2 ** 8) - 1},
      {"1" => nil},
    ]
    target = build(:uint8, values)
    assert_equal(remove_field_names(values),
                 target.values)
  end

  def test_int16
    values = [
      {"0" => -(2 ** 15)},
      {"1" => nil},
    ]
    target = build(:int16, values)
    assert_equal(remove_field_names(values),
                 target.values)
  end

  def test_uint16
    values = [
      {"0" => (2 ** 16) - 1},
      {"1" => nil},
    ]
    target = build(:uint16, values)
    assert_equal(remove_field_names(values),
                 target.values)
  end

  def test_int32
    values = [
      {"0" => -(2 ** 31)},
      {"1" => nil},
    ]
    target = build(:int32, values)
    assert_equal(remove_field_names(values),
                 target.values)
  end

  def test_uint32
    values = [
      {"0" => (2 ** 32) - 1},
      {"1" => nil},
    ]
    target = build(:uint32, values)
    assert_equal(remove_field_names(values),
                 target.values)
  end

  def test_int64
    values = [
      {"0" => -(2 ** 63)},
      {"1" => nil},
    ]
    target = build(:int64, values)
    assert_equal(remove_field_names(values),
                 target.values)
  end

  def test_uint64
    values = [
      {"0" => (2 ** 64) - 1},
      {"1" => nil},
    ]
    target = build(:uint64, values)
    assert_equal(remove_field_names(values),
                 target.values)
  end

  def test_float
    values = [
      {"0" => -1.0},
      {"1" => nil},
    ]
    target = build(:float, values)
    assert_equal(remove_field_names(values),
                 target.values)
  end

  def test_double
    values = [
      {"0" => -1.0},
      {"1" => nil},
    ]
    target = build(:double, values)
    assert_equal(remove_field_names(values),
                 target.values)
  end

  def test_binary
    values = [
      {"0" => "\xff".b},
      {"1" => nil},
    ]
    target = build(:binary, values)
    assert_equal(remove_field_names(values),
                 target.values)
  end

  def test_string
    values = [
      {"0" => "Ruby"},
      {"1" => nil},
    ]
    target = build(:string, values)
    assert_equal(remove_field_names(values),
                 target.values)
  end

  def test_date32
    values = [
      {"0" => Date.new(1960, 1, 1)},
      {"1" => nil},
    ]
    target = build(:date32, values)
    assert_equal(remove_field_names(values),
                 target.values)
  end

  def test_date64
    values = [
      {"0" => DateTime.new(1960, 1, 1, 2, 9, 30)},
      {"1" => nil},
    ]
    target = build(:date64, values)
    assert_equal(remove_field_names(values),
                 target.values)
  end

  def test_timestamp_second
    values = [
      {"0" => Time.parse("1960-01-01T02:09:30Z")},
      {"1" => nil},
    ]
    target = build({
                     type: :timestamp,
                     unit: :second,
                   },
                   values)
    assert_equal(remove_field_names(values),
                 target.values)
  end

  def test_timestamp_milli
    values = [
      {"0" => Time.parse("1960-01-01T02:09:30.123Z")},
      {"1" => nil},
    ]
    target = build({
                     type: :timestamp,
                     unit: :milli,
                   },
                   values)
    assert_equal(remove_field_names(values),
                 target.values)
  end

  def test_timestamp_micro
    values = [
      {"0" => Time.parse("1960-01-01T02:09:30.123456Z")},
      {"1" => nil},
    ]
    target = build({
                     type: :timestamp,
                     unit: :micro,
                   },
                   values)
    assert_equal(remove_field_names(values),
                 target.values)
  end

  def test_timestamp_nano
    values = [
      {"0" => Time.parse("1960-01-01T02:09:30.123456789Z")},
      {"1" => nil},
    ]
    target = build({
                     type: :timestamp,
                     unit: :nano,
                   },
                   values)
    assert_equal(remove_field_names(values),
                 target.values)
  end

  def test_time32_second
    unit = Arrow::TimeUnit::SECOND
    values = [
      # 00:10:00
      {"0" => Arrow::Time.new(unit, 60 * 10)},
      {"1" => nil},
    ]
    target = build({
                     type: :time32,
                     unit: :second,
                   },
                   values)
    assert_equal(remove_field_names(values),
                 target.values)
  end

  def test_time32_milli
    unit = Arrow::TimeUnit::MILLI
    values = [
      # 00:10:00.123
      {"0" => Arrow::Time.new(unit, (60 * 10) * 1000 + 123)},
      {"1" => nil},
    ]
    target = build({
                     type: :time32,
                     unit: :milli,
                   },
                   values)
    assert_equal(remove_field_names(values),
                 target.values)
  end

  def test_time64_micro
    unit = Arrow::TimeUnit::MICRO
    values = [
      # 00:10:00.123456
      {"0" => Arrow::Time.new(unit, (60 * 10) * 1_000_000 + 123_456)},
      {"1" => nil},
    ]
    target = build({
                     type: :time64,
                     unit: :micro,
                   },
                   values)
    assert_equal(remove_field_names(values),
                 target.values)
  end

  def test_time64_nano
    unit = Arrow::TimeUnit::NANO
    values = [
      # 00:10:00.123456789
      {"0" => Arrow::Time.new(unit, (60 * 10) * 1_000_000_000 + 123_456_789)},
      {"1" => nil},
    ]
    target = build({
                     type: :time64,
                     unit: :nano,
                   },
                   values)
    assert_equal(remove_field_names(values),
                 target.values)
  end

  def test_decimal128
    values = [
      {"0" => BigDecimal("92.92")},
      {"1" => nil},
    ]
    target = build({
                     type: :decimal128,
                     precision: 8,
                     scale: 2,
                   },
                   values)
    assert_equal(remove_field_names(values),
                 target.values)
  end

  def test_decimal256
    values = [
      {"0" => BigDecimal("92.92")},
      {"1" => nil},
    ]
    target = build({
                     type: :decimal256,
                     precision: 38,
                     scale: 2,
                   },
                   values)
    assert_equal(remove_field_names(values),
                 target.values)
  end

  def test_month_interval
    values = [
      {"0" => 1},
      {"1" => nil},
    ]
    target = build(:month_interval, values)
    assert_equal(remove_field_names(values),
                 target.values)
  end

  def test_day_time_interval
    values = [
      {"0" => {day: 1, millisecond: 100}},
      {"1" => nil},
    ]
    target = build(:day_time_interval, values)
    assert_equal(remove_field_names(values),
                 target.values)
  end

  def test_month_day_nano_interval
    values = [
      {"0" => {month: 1, day: 1, nanosecond: 100}},
      {"1" => nil},
    ]
    target = build(:month_day_nano_interval, values)
    assert_equal(remove_field_names(values),
                 target.values)
  end

  def test_list
    values = [
      {"0" => [true, nil, false]},
      {"1" => nil},
    ]
    target = build({
                     type: :list,
                     field: {
                       name: :sub_element,
                       type: :boolean,
                     },
                   },
                   values)
    assert_equal(remove_field_names(values),
                 target.values)
  end

  def test_struct
    values = [
      {"0" => {"sub_field" => true}},
      {"1" => nil},
      {"0" => {"sub_field" => nil}},
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
    assert_equal(remove_field_names(values),
                 target.values)
  end

  def test_map
    values = [
      {"0" => {"key1" => true, "key2" => nil}},
      {"1" => nil},
    ]
    target = build({
                     type: :map,
                     key: :string,
                     item: :boolean,
                   },
                   values)
    assert_equal(remove_field_names(values),
                 target.values)
  end

  def test_sparse_union
    values = [
      {"0" => {"field1" => true}},
      {"1" => nil},
      {"0" => {"field2" => 29}},
      {"0" => {"field2" => nil}},
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
    assert_equal(remove_field_names(remove_field_names(values)),
                 target.values)
  end

  def test_dense_union
    values = [
      {"0" => {"field1" => true}},
      {"1" => nil},
      {"0" => {"field2" => 29}},
      {"0" => {"field2" => nil}},
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
    assert_equal(remove_field_names(remove_field_names(values)),
                 target.values)
  end

  def test_dictionary
    values = [
      {"0" => "Ruby"},
      {"1" => nil},
      {"0" => "GLib"},
    ]
    target = build({
                     type: :dictionary,
                     index_data_type: :int8,
                     value_data_type: :string,
                     ordered: false,
                   },
                   values)
    assert_equal(remove_field_names(values),
                 target.values)
  end
end

class ValuesArrayDenseUnionArrayTest < Test::Unit::TestCase
  include ValuesDenseUnionArrayTests

  def build(type, values)
    build_array(type, values)
  end
end

class ValuesChunkedArrayDenseUnionArrayTest < Test::Unit::TestCase
  include ValuesDenseUnionArrayTests

  def build(type, values)
    Arrow::ChunkedArray.new([build_array(type, values)])
  end
end
