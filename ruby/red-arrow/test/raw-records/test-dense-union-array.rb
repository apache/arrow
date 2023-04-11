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

module RawRecordsDenseUnionArrayTests
  def build_schema(type, type_codes)
    field_description = {}
    if type.is_a?(Hash)
      field_description = field_description.merge(type)
    else
      field_description[:type] = type
    end
    {
      column: {
        type: :dense_union,
        fields: [
          field_description.merge(name: "0"),
          field_description.merge(name: "1"),
        ],
        type_codes: type_codes,
      },
    }
  end

  # TODO: Use Arrow::RecordBatch.new(build_schema(type, type_codes), records)
  def build_record_batch(type, records)
    type_codes = [0, 1]
    schema = Arrow::Schema.new(build_schema(type, type_codes))
    type_ids = []
    offsets = []
    arrays = schema.fields[0].data_type.fields.collect do |field|
      sub_schema = Arrow::Schema.new([field])
      sub_records = []
      records.each do |record|
        column = record[0]
        next if column.nil?
        next unless column.key?(field.name)
        sub_records << [column[field.name]]
      end
      sub_record_batch = Arrow::RecordBatch.new(sub_schema,
                                                sub_records)
      sub_record_batch.columns[0].data
    end
    records.each do |record|
      column = record[0]
      if column.key?("0")
        type_id = type_codes[0]
        type_ids << type_id
        offsets << (type_ids.count(type_id) - 1)
      elsif column.key?("1")
        type_id = type_codes[1]
        type_ids << type_id
        offsets << (type_ids.count(type_id) - 1)
      end
    end
    union_array = Arrow::DenseUnionArray.new(schema.fields[0].data_type,
                                             Arrow::Int8Array.new(type_ids),
                                             Arrow::Int32Array.new(offsets),
                                             arrays)
    schema = Arrow::Schema.new(column: union_array.value_data_type)
    Arrow::RecordBatch.new(schema,
                           records.size,
                           [union_array])
  end

  def remove_field_names(records)
    records.collect do |record|
      record.collect do |column|
        if column.nil?
          column
        else
          column.values[0]
        end
      end
    end
  end

  def test_null
    records = [
      [{"0" => nil}],
    ]
    target = build(:null, records)
    assert_equal(remove_field_names(records),
                 target.raw_records)
  end

  def test_boolean
    records = [
      [{"0" => true}],
      [{"1" => nil}],
    ]
    target = build(:boolean, records)
    assert_equal(remove_field_names(records),
                 target.raw_records)
  end

  def test_int8
    records = [
      [{"0" => -(2 ** 7)}],
      [{"1" => nil}],
    ]
    target = build(:int8, records)
    assert_equal(remove_field_names(records),
                 target.raw_records)
  end

  def test_uint8
    records = [
      [{"0" => (2 ** 8) - 1}],
      [{"1" => nil}],
    ]
    target = build(:uint8, records)
    assert_equal(remove_field_names(records),
                 target.raw_records)
  end

  def test_int16
    records = [
      [{"0" => -(2 ** 15)}],
      [{"1" => nil}],
    ]
    target = build(:int16, records)
    assert_equal(remove_field_names(records),
                 target.raw_records)
  end

  def test_uint16
    records = [
      [{"0" => (2 ** 16) - 1}],
      [{"1" => nil}],
    ]
    target = build(:uint16, records)
    assert_equal(remove_field_names(records),
                 target.raw_records)
  end

  def test_int32
    records = [
      [{"0" => -(2 ** 31)}],
      [{"1" => nil}],
    ]
    target = build(:int32, records)
    assert_equal(remove_field_names(records),
                 target.raw_records)
  end

  def test_uint32
    records = [
      [{"0" => (2 ** 32) - 1}],
      [{"1" => nil}],
    ]
    target = build(:uint32, records)
    assert_equal(remove_field_names(records),
                 target.raw_records)
  end

  def test_int64
    records = [
      [{"0" => -(2 ** 63)}],
      [{"1" => nil}],
    ]
    target = build(:int64, records)
    assert_equal(remove_field_names(records),
                 target.raw_records)
  end

  def test_uint64
    records = [
      [{"0" => (2 ** 64) - 1}],
      [{"1" => nil}],
    ]
    target = build(:uint64, records)
    assert_equal(remove_field_names(records),
                 target.raw_records)
  end

  def test_float
    records = [
      [{"0" => -1.0}],
      [{"1" => nil}],
    ]
    target = build(:float, records)
    assert_equal(remove_field_names(records),
                 target.raw_records)
  end

  def test_double
    records = [
      [{"0" => -1.0}],
      [{"1" => nil}],
    ]
    target = build(:double, records)
    assert_equal(remove_field_names(records),
                 target.raw_records)
  end

  def test_binary
    records = [
      [{"0" => "\xff".b}],
      [{"1" => nil}],
    ]
    target = build(:binary, records)
    assert_equal(remove_field_names(records),
                 target.raw_records)
  end

  def test_string
    records = [
      [{"0" => "Ruby"}],
      [{"1" => nil}],
    ]
    target = build(:string, records)
    assert_equal(remove_field_names(records),
                 target.raw_records)
  end

  def test_date32
    records = [
      [{"0" => Date.new(1960, 1, 1)}],
      [{"1" => nil}],
    ]
    target = build(:date32, records)
    assert_equal(remove_field_names(records),
                 target.raw_records)
  end

  def test_date64
    records = [
      [{"0" => DateTime.new(1960, 1, 1, 2, 9, 30)}],
      [{"1" => nil}],
    ]
    target = build(:date64, records)
    assert_equal(remove_field_names(records),
                 target.raw_records)
  end

  def test_timestamp_second
    records = [
      [{"0" => Time.parse("1960-01-01T02:09:30Z")}],
      [{"1" => nil}],
    ]
    target = build({
                     type: :timestamp,
                     unit: :second,
                   },
                   records)
    assert_equal(remove_field_names(records),
                 target.raw_records)
  end

  def test_timestamp_milli
    records = [
      [{"0" => Time.parse("1960-01-01T02:09:30.123Z")}],
      [{"1" => nil}],
    ]
    target = build({
                     type: :timestamp,
                     unit: :milli,
                   },
                   records)
    assert_equal(remove_field_names(records),
                 target.raw_records)
  end

  def test_timestamp_micro
    records = [
      [{"0" => Time.parse("1960-01-01T02:09:30.123456Z")}],
      [{"1" => nil}],
    ]
    target = build({
                     type: :timestamp,
                     unit: :micro,
                   },
                   records)
    assert_equal(remove_field_names(records),
                 target.raw_records)
  end

  def test_timestamp_nano
    records = [
      [{"0" => Time.parse("1960-01-01T02:09:30.123456789Z")}],
      [{"1" => nil}],
    ]
    target = build({
                     type: :timestamp,
                     unit: :nano,
                   },
                   records)
    assert_equal(remove_field_names(records),
                 target.raw_records)
  end

  def test_time32_second
    unit = Arrow::TimeUnit::SECOND
    records = [
      # 00:10:00
      [{"0" => Arrow::Time.new(unit, 60 * 10)}],
      [{"1" => nil}],
    ]
    target = build({
                     type: :time32,
                     unit: :second,
                   },
                   records)
    assert_equal(remove_field_names(records),
                 target.raw_records)
  end

  def test_time32_milli
    unit = Arrow::TimeUnit::MILLI
    records = [
      # 00:10:00.123
      [{"0" => Arrow::Time.new(unit, (60 * 10) * 1000 + 123)}],
      [{"1" => nil}],
    ]
    target = build({
                     type: :time32,
                     unit: :milli,
                   },
                   records)
    assert_equal(remove_field_names(records),
                 target.raw_records)
  end

  def test_time64_micro
    unit = Arrow::TimeUnit::MICRO
    records = [
      # 00:10:00.123456
      [{"0" => Arrow::Time.new(unit, (60 * 10) * 1_000_000 + 123_456)}],
      [{"1" => nil}],
    ]
    target = build({
                     type: :time64,
                     unit: :micro,
                   },
                   records)
    assert_equal(remove_field_names(records),
                 target.raw_records)
  end

  def test_time64_nano
    unit = Arrow::TimeUnit::NANO
    records = [
      # 00:10:00.123456789
      [{"0" => Arrow::Time.new(unit, (60 * 10) * 1_000_000_000 + 123_456_789)}],
      [{"1" => nil}],
    ]
    target = build({
                     type: :time64,
                     unit: :nano,
                   },
                   records)
    assert_equal(remove_field_names(records),
                 target.raw_records)
  end

  def test_decimal128
    records = [
      [{"0" => BigDecimal("92.92")}],
      [{"1" => nil}],
    ]
    target = build({
                     type: :decimal128,
                     precision: 8,
                     scale: 2,
                   },
                   records)
    assert_equal(remove_field_names(records),
                 target.raw_records)
  end

  def test_decimal256
    records = [
      [{"0" => BigDecimal("92.92")}],
      [{"1" => nil}],
    ]
    target = build({
                     type: :decimal256,
                     precision: 38,
                     scale: 2,
                   },
                   records)
    assert_equal(remove_field_names(records),
                 target.raw_records)
  end

  def test_month_interval
    records = [
      [{"0" => 1}],
      [{"1" => nil}],
    ]
    target = build(:month_interval, records)
    assert_equal(remove_field_names(records),
                 target.raw_records)
  end

  def test_day_time_interval
    records = [
      [{"0" => {day: 1, millisecond: 100}}],
      [{"1" => nil}],
    ]
    target = build(:day_time_interval, records)
    assert_equal(remove_field_names(records),
                 target.raw_records)
  end

  def test_month_day_nano_interval
    records = [
      [{"0" => {month: 1, day: 1, nanosecond: 100}}],
      [{"1" => nil}],
    ]
    target = build(:month_day_nano_interval, records)
    assert_equal(remove_field_names(records),
                 target.raw_records)
  end

  def test_list
    records = [
      [{"0" => [true, nil, false]}],
      [{"1" => nil}],
    ]
    target = build({
                     type: :list,
                     field: {
                       name: :sub_element,
                       type: :boolean,
                     },
                   },
                   records)
    assert_equal(remove_field_names(records),
                 target.raw_records)
  end

  def test_struct
    records = [
      [{"0" => {"sub_field" => true}}],
      [{"1" => nil}],
      [{"0" => {"sub_field" => nil}}],
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
    assert_equal(remove_field_names(records),
                 target.raw_records)
  end

  def test_map
    records = [
      [{"0" => {"key1" => true, "key2" => nil}}],
      [{"1" => nil}],
    ]
    target = build({
                     type: :map,
                     key: :string,
                     item: :boolean,
                   },
                   records)
    assert_equal(remove_field_names(records),
                 target.raw_records)
  end

  def test_sparse_union
    records = [
      [{"0" => {"field1" => true}}],
      [{"1" => nil}],
      [{"0" => {"field2" => 29}}],
      [{"0" => {"field2" => nil}}],
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
    assert_equal(remove_field_names(remove_field_names(records)),
                 target.raw_records)
  end

  def test_dense_union
    records = [
      [{"0" => {"field1" => true}}],
      [{"1" => nil}],
      [{"0" => {"field2" => 29}}],
      [{"0" => {"field2" => nil}}],
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
    assert_equal(remove_field_names(remove_field_names(records)),
                 target.raw_records)
  end

  def test_dictionary
    records = [
      [{"0" => "Ruby"}],
      [{"1" => nil}],
      [{"0" => "GLib"}],
    ]
    target = build({
                     type: :dictionary,
                     index_data_type: :int8,
                     value_data_type: :string,
                     ordered: false,
                   },
                   records)
    assert_equal(remove_field_names(records),
                 target.raw_records)
  end
end

class RawRecordsRecordBatchDenseUnionArrayTest < Test::Unit::TestCase
  include RawRecordsDenseUnionArrayTests

  def build(type, records)
    build_record_batch(type, records)
  end
end

class RawRecordsTableDenseUnionArrayTest < Test::Unit::TestCase
  include RawRecordsDenseUnionArrayTests

  def build(type, records)
    build_record_batch(type, records).to_table
  end
end
