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

module RawRecordsStructArrayTests
  def build_schema(type)
    field_description = {
      name: :field,
    }
    if type.is_a?(Hash)
      field_description = field_description.merge(type)
    else
      field_description[:type] = type
    end
    {
      column: {
        type: :struct,
        fields: [
          field_description,
        ],
      },
    }
  end

  def test_null
    records = [
      [{"field" => nil}],
      [nil],
    ]
    target = build(:null, records)
    assert_equal(records, target.raw_records)
  end

  def test_boolean
    records = [
      [{"field" => true}],
      [nil],
      [{"field" => nil}],
    ]
    target = build(:boolean, records)
    assert_equal(records, target.raw_records)
  end

  def test_int8
    records = [
      [{"field" => -(2 ** 7)}],
      [nil],
      [{"field" => nil}],
    ]
    target = build(:int8, records)
    assert_equal(records, target.raw_records)
  end

  def test_uint8
    records = [
      [{"field" => (2 ** 8) - 1}],
      [nil],
      [{"field" => nil}],
    ]
    target = build(:uint8, records)
    assert_equal(records, target.raw_records)
  end

  def test_int16
    records = [
      [{"field" => -(2 ** 15)}],
      [nil],
      [{"field" => nil}],
    ]
    target = build(:int16, records)
    assert_equal(records, target.raw_records)
  end

  def test_uint16
    records = [
      [{"field" => (2 ** 16) - 1}],
      [nil],
      [{"field" => nil}],
    ]
    target = build(:uint16, records)
    assert_equal(records, target.raw_records)
  end

  def test_int32
    records = [
      [{"field" => -(2 ** 31)}],
      [nil],
      [{"field" => nil}],
    ]
    target = build(:int32, records)
    assert_equal(records, target.raw_records)
  end

  def test_uint32
    records = [
      [{"field" => (2 ** 32) - 1}],
      [nil],
      [{"field" => nil}],
    ]
    target = build(:uint32, records)
    assert_equal(records, target.raw_records)
  end

  def test_int64
    records = [
      [{"field" => -(2 ** 63)}],
      [nil],
      [{"field" => nil}],
    ]
    target = build(:int64, records)
    assert_equal(records, target.raw_records)
  end

  def test_uint64
    records = [
      [{"field" => (2 ** 64) - 1}],
      [nil],
      [{"field" => nil}],
    ]
    target = build(:uint64, records)
    assert_equal(records, target.raw_records)
  end

  def test_float
    records = [
      [{"field" => -1.0}],
      [nil],
      [{"field" => nil}],
    ]
    target = build(:float, records)
    assert_equal(records, target.raw_records)
  end

  def test_double
    records = [
      [{"field" => -1.0}],
      [nil],
      [{"field" => nil}],
    ]
    target = build(:double, records)
    assert_equal(records, target.raw_records)
  end

  def test_binary
    records = [
      [{"field" => "\xff".b}],
      [nil],
      [{"field" => nil}],
    ]
    target = build(:binary, records)
    assert_equal(records, target.raw_records)
  end

  def test_string
    records = [
      [{"field" => "Ruby"}],
      [nil],
      [{"field" => nil}],
    ]
    target = build(:string, records)
    assert_equal(records, target.raw_records)
  end

  def test_date32
    records = [
      [{"field" => Date.new(1960, 1, 1)}],
      [nil],
      [{"field" => nil}],
    ]
    target = build(:date32, records)
    assert_equal(records, target.raw_records)
  end

  def test_date64
    records = [
      [{"field" => DateTime.new(1960, 1, 1, 2, 9, 30)}],
      [nil],
      [{"field" => nil}],
    ]
    target = build(:date64, records)
    assert_equal(records, target.raw_records)
  end

  def test_timestamp_second
    records = [
      [{"field" => Time.parse("1960-01-01T02:09:30Z")}],
      [nil],
      [{"field" => nil}],
    ]
    target = build({
                     type: :timestamp,
                     unit: :second,
                   },
                   records)
    assert_equal(records, target.raw_records)
  end

  def test_timestamp_milli
    records = [
      [{"field" => Time.parse("1960-01-01T02:09:30.123Z")}],
      [nil],
      [{"field" => nil}],
    ]
    target = build({
                     type: :timestamp,
                     unit: :milli,
                   },
                   records)
    assert_equal(records, target.raw_records)
  end

  def test_timestamp_micro
    records = [
      [{"field" => Time.parse("1960-01-01T02:09:30.123456Z")}],
      [nil],
      [{"field" => nil}],
    ]
    target = build({
                     type: :timestamp,
                     unit: :micro,
                   },
                   records)
    assert_equal(records, target.raw_records)
  end

  def test_timestamp_nano
    records = [
      [{"field" => Time.parse("1960-01-01T02:09:30.123456789Z")}],
      [nil],
      [{"field" => nil}],
    ]
    target = build({
                     type: :timestamp,
                     unit: :nano,
                   },
                   records)
    assert_equal(records, target.raw_records)
  end

  def test_time32_second
    unit = Arrow::TimeUnit::SECOND
    records = [
      # 00:10:00
      [{"field" => Arrow::Time.new(unit, 60 * 10)}],
      [nil],
      [{"field" => nil}],
    ]
    target = build({
                     type: :time32,
                     unit: :second,
                   },
                   records)
    assert_equal(records, target.raw_records)
  end

  def test_time32_milli
    unit = Arrow::TimeUnit::MILLI
    records = [
      # 00:10:00.123
      [{"field" => Arrow::Time.new(unit, (60 * 10) * 1000 + 123)}],
      [nil],
      [{"field" => nil}],
    ]
    target = build({
                     type: :time32,
                     unit: :milli,
                   },
                   records)
    assert_equal(records, target.raw_records)
  end

  def test_time64_micro
    unit = Arrow::TimeUnit::MICRO
    records = [
      # 00:10:00.123456
      [{"field" => Arrow::Time.new(unit, (60 * 10) * 1_000_000 + 123_456)}],
      [nil],
      [{"field" => nil}],
    ]
    target = build({
                     type: :time64,
                     unit: :micro,
                   },
                   records)
    assert_equal(records, target.raw_records)
  end

  def test_time64_nano
    unit = Arrow::TimeUnit::NANO
    records = [
      # 00:10:00.123456789
      [{"field" => Arrow::Time.new(unit, (60 * 10) * 1_000_000_000 + 123_456_789)}],
      [nil],
      [{"field" => nil}],
    ]
    target = build({
                     type: :time64,
                     unit: :nano,
                   },
                   records)
    assert_equal(records, target.raw_records)
  end

  def test_decimal128
    records = [
      [{"field" => BigDecimal("92.92")}],
      [nil],
      [{"field" => nil}],
    ]
    target = build({
                     type: :decimal128,
                     precision: 8,
                     scale: 2,
                   },
                   records)
    assert_equal(records, target.raw_records)
  end

  def test_decimal256
    records = [
      [{"field" => BigDecimal("92.92")}],
      [nil],
      [{"field" => nil}],
    ]
    target = build({
                     type: :decimal256,
                     precision: 38,
                     scale: 2,
                   },
                   records)
    assert_equal(records, target.raw_records)
  end

  def test_month_interval
    records = [
      [{"field" => 1}],
      [nil],
      [{"field" => nil}],
    ]
    target = build(:month_interval, records)
    assert_equal(records, target.raw_records)
  end

  def test_day_time_interval
    records = [
      [{"field" => {day: 1, millisecond: 100}}],
      [nil],
      [{"field" => nil}],
    ]
    target = build(:day_time_interval, records)
    assert_equal(records, target.raw_records)
  end

  def test_month_day_nano_interval
    records = [
      [{"field" => {month: 1, day: 1, nanosecond: 100}}],
      [nil],
      [{"field" => nil}],
    ]
    target = build(:month_day_nano_interval, records)
    assert_equal(records, target.raw_records)
  end

  def test_list
    records = [
      [{"field" => [true, nil, false]}],
      [nil],
      [{"field" => nil}],
    ]
    target = build({
                     type: :list,
                     field: {
                       name: :sub_element,
                       type: :boolean,
                     },
                   },
                   records)
    assert_equal(records, target.raw_records)
  end

  def test_struct
    records = [
      [{"field" => {"sub_field" => true}}],
      [nil],
      [{"field" => nil}],
      [{"field" => {"sub_field" => nil}}],
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
                   records)
    assert_equal(records, target.raw_records)
  end

  def test_map
    records = [
      [{"field" => {"key1" => true, "key2" => nil}}],
      [nil],
      [{"field" => nil}],
    ]
    target = build({
                     type: :map,
                     key: :string,
                     item: :boolean,
                   },
                   records)
    assert_equal(records, target.raw_records)
  end

  def remove_union_field_names(records)
    records.collect do |record|
      record.collect do |column|
        if column.nil?
          column
        else
          value = column["field"]
          value = value.values[0] unless value.nil?
          {"field" => value}
        end
      end
    end
  end

  def test_sparse_union
    records = [
      [{"field" => {"field1" => true}}],
      [nil],
      [{"field" => nil}],
      [{"field" => {"field2" => 29}}],
      [{"field" => {"field2" => nil}}],
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
                   records)
    assert_equal(remove_union_field_names(records),
                 target.raw_records)
  end

  def test_dense_union
    records = [
      [{"field" => {"field1" => true}}],
      [nil],
      [{"field" => nil}],
      [{"field" => {"field2" => 29}}],
      [{"field" => {"field2" => nil}}],
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
                   records)
    assert_equal(remove_union_field_names(records),
                 target.raw_records)
  end

  def test_dictionary
    records = [
      [{"field" => "Ruby"}],
      [nil],
      [{"field" => nil}],
      [{"field" => "GLib"}],
    ]
    target = build({
                     type: :dictionary,
                     index_data_type: :int8,
                     value_data_type: :string,
                     ordered: false,
                   },
                   records)
    assert_equal(records, target.raw_records)
  end
end

class RawRecordsRecordBatchStructArrayTest < Test::Unit::TestCase
  include RawRecordsStructArrayTests

  def build(type, records)
    Arrow::RecordBatch.new(build_schema(type), records)
  end
end

class RawRecordsTableStructArrayTest < Test::Unit::TestCase
  include RawRecordsStructArrayTests

  def build(type, records)
    Arrow::Table.new(build_schema(type), records)
  end
end
