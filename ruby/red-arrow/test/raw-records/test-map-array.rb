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

module RawRecordsMapArrayTests
  def build_schema(type)
    {
      column: {
        type: :map,
        key: :string,
        item: type
      },
    }
  end

  def test_null
    records = [
      [{"key1" => nil}],
      [nil],
    ]
    target = build(:null, records)
    assert_equal(records, target.raw_records)
  end

  def test_boolean
    records = [
      [{"key1" => true, "key2" => nil}],
      [nil],
    ]
    target = build(:boolean, records)
    assert_equal(records, target.raw_records)
  end

  def test_int8
    records = [
      [{"key1" => -(2 ** 7), "key2" => nil}],
      [nil],
    ]
    target = build(:int8, records)
    assert_equal(records, target.raw_records)
  end

  def test_uint8
    records = [
      [{"key1" => (2 ** 8) - 1, "key2" => nil}],
      [nil],
    ]
    target = build(:uint8, records)
    assert_equal(records, target.raw_records)
  end

  def test_int16
    records = [
      [{"key1" => -(2 ** 15), "key2" => nil}],
      [nil],
    ]
    target = build(:int16, records)
    assert_equal(records, target.raw_records)
  end

  def test_uint16
    records = [
      [{"key1" => (2 ** 16) - 1, "key2" => nil}],
      [nil],
    ]
    target = build(:uint16, records)
    assert_equal(records, target.raw_records)
  end

  def test_int32
    records = [
      [{"key1" => -(2 ** 31), "key2" => nil}],
      [nil],
    ]
    target = build(:int32, records)
    assert_equal(records, target.raw_records)
  end

  def test_uint32
    records = [
      [{"key1" => (2 ** 32) - 1, "key2" => nil}],
      [nil],
    ]
    target = build(:uint32, records)
    assert_equal(records, target.raw_records)
  end

  def test_int64
    records = [
      [{"key1" => -(2 ** 63), "key2" => nil}],
      [nil],
    ]
    target = build(:int64, records)
    assert_equal(records, target.raw_records)
  end

  def test_uint64
    records = [
      [{"key1" => (2 ** 64) - 1, "key2" => nil}],
      [nil],
    ]
    target = build(:uint64, records)
    assert_equal(records, target.raw_records)
  end

  def test_float
    records = [
      [{"key1" => -1.0, "key2" => nil}],
      [nil],
    ]
    target = build(:float, records)
    assert_equal(records, target.raw_records)
  end

  def test_double
    records = [
      [{"key1" => -1.0, "key2" => nil}],
      [nil],
    ]
    target = build(:double, records)
    assert_equal(records, target.raw_records)
  end

  def test_binary
    records = [
      [{"key1" => "\xff".b, "key2" => nil}],
      [nil],
    ]
    target = build(:binary, records)
    assert_equal(records, target.raw_records)
  end

  def test_string
    records = [
      [{"key1" => "Ruby", "key2" => nil}],
      [nil],
    ]
    target = build(:string, records)
    assert_equal(records, target.raw_records)
  end

  def test_date32
    records = [
      [{"key1" => Date.new(1960, 1, 1), "key2" => nil}],
      [nil],
    ]
    target = build(:date32, records)
    assert_equal(records, target.raw_records)
  end

  def test_date64
    records = [
      [{"key1" => DateTime.new(1960, 1, 1, 2, 9, 30), "key2" => nil}],
      [nil],
    ]
    target = build(:date64, records)
    assert_equal(records, target.raw_records)
  end

  def test_timestamp_second
    records = [
      [{"key1" => Time.parse("1960-01-01T02:09:30Z"), "key2" => nil}],
      [nil],
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
      [{"key1" => Time.parse("1960-01-01T02:09:30.123Z"), "key2" => nil}],
      [nil],
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
      [{"key1" => Time.parse("1960-01-01T02:09:30.123456Z"), "key2" => nil}],
      [nil],
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
      [{"key1" => Time.parse("1960-01-01T02:09:30.123456789Z"), "key2" => nil}],
      [nil],
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
      [{"key1" => Arrow::Time.new(unit, 60 * 10), "key2" => nil}],
      [nil],
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
      [{"key1" => Arrow::Time.new(unit, (60 * 10) * 1000 + 123), "key2" => nil}],
      [nil],
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
      [{"key1" => Arrow::Time.new(unit, (60 * 10) * 1_000_000 + 123_456), "key2" => nil}],
      [nil],
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
      [{"key1" => Arrow::Time.new(unit, (60 * 10) * 1_000_000_000 + 123_456_789), "key2" => nil}],
      [nil],
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
      [{"key1" => BigDecimal("92.92"), "key2" => nil}],
      [nil],
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
      [{"key1" => BigDecimal("92.92"), "key2" => nil}],
      [nil],
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
      [{"key1" => 1, "key2" => nil}],
      [nil],
    ]
    target = build(:month_interval, records)
    assert_equal(records, target.raw_records)
  end

  def test_day_time_interval
    records = [
      [
        {
          "key1" => {day: 1, millisecond: 100},
          "key2" => nil,
        },
      ],
      [nil],
    ]
    target = build(:day_time_interval, records)
    assert_equal(records, target.raw_records)
  end

  def test_month_day_nano_interval
    records = [
      [
        {
          "key1" => {month: 1, day: 1, nanosecond: 100},
          "key2" => nil,
        },
      ],
      [nil],
    ]
    target = build(:month_day_nano_interval, records)
    assert_equal(records, target.raw_records)
  end

  def test_list
    records = [
      [{"key1" => [true, nil, false], "key2" => nil}],
      [nil],
    ]
    target = build({
                     type: :list,
                     field: {
                       name: :element,
                       type: :boolean,
                     },
                   },
                   records)
    assert_equal(records, target.raw_records)
  end

  def test_struct
    records = [
      [{"key1" => {"field" => true}, "key2" => nil, "key3" => {"field" => nil}}],
      [nil],
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
                   records)
    assert_equal(records, target.raw_records)
  end

  def test_map
    records = [
      [{"key1" => {"sub_key1" => true, "sub_key2" => nil}, "key2" => nil}],
      [nil],
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
          value = {}
          column.each do |k, v|
            v = v.values[0] unless v.nil?
            value[k] = v
          end
          value
        end
      end
    end
  end

  def test_sparse_union
    records = [
      [
        {
          "key1" => {"field1" => true},
          "key2" => nil,
          "key3" => {"field2" => 29},
          "key4" => {"field2" => nil},
        },
      ],
      [nil],
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
      [
        {
          "key1" => {"field1" => true},
          "key2" => nil,
          "key3" => {"field2" => 29},
          "key4" => {"field2" => nil},
        },
      ],
      [nil],
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
      [{"key1" => "Ruby", "key2" => nil, "key3" => "GLib"}],
      [nil],
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

class RawRecordsRecordBatchMapArrayTest < Test::Unit::TestCase
  include RawRecordsMapArrayTests

  def build(type, records)
    Arrow::RecordBatch.new(build_schema(type), records)
  end
end

class RawRecordsTableMapArrayTest < Test::Unit::TestCase
  include RawRecordsMapArrayTests

  def build(type, records)
    Arrow::Table.new(build_schema(type), records)
  end
end
