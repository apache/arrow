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

module EachRawRecordListArrayTests
  def build_schema(type)
    field_description = {
      name: :element,
    }
    if type.is_a?(Hash)
      field_description = field_description.merge(type)
    else
      field_description[:type] = type
    end
    {
      column: {
        type: :list,
        field: field_description,
      },
    }
  end

  def test_null
    records = [
      [[nil, nil, nil]],
      [nil],
    ]
    iterated_records = []
    target = build(:null, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_boolean
    records = [
      [[true, nil, false]],
      [nil],
    ]
    iterated_records = []
    target = build(:boolean, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_int8
    records = [
      [[-(2 ** 7), nil, (2 ** 7) - 1]],
      [nil],
    ]
    iterated_records = []
    target = build(:int8, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_uint8
    records = [
      [[0, nil, (2 ** 8) - 1]],
      [nil],
    ]
    iterated_records = []
    target = build(:uint8, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_int16
    records = [
      [[-(2 ** 15), nil, (2 ** 15) - 1]],
      [nil],
    ]
    iterated_records = []
    target = build(:int16, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_uint16
    records = [
      [[0, nil, (2 ** 16) - 1]],
      [nil],
    ]
    iterated_records = []
    target = build(:uint16, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_int32
    records = [
      [[-(2 ** 31), nil, (2 ** 31) - 1]],
      [nil],
    ]
    iterated_records = []
    target = build(:int32, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_uint32
    records = [
      [[0, nil, (2 ** 32) - 1]],
      [nil],
    ]
    iterated_records = []
    target = build(:uint32, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_int64
    records = [
      [[-(2 ** 63), nil, (2 ** 63) - 1]],
      [nil],
    ]
    iterated_records = []
    target = build(:int64, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_uint64
    records = [
      [[0, nil, (2 ** 64) - 1]],
      [nil],
    ]
    iterated_records = []
    target = build(:uint64, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_float
    records = [
      [[-1.0, nil, 1.0]],
      [nil],
    ]
    iterated_records = []
    target = build(:float, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_double
    records = [
      [[-1.0, nil, 1.0]],
      [nil],
    ]
    iterated_records = []
    target = build(:double, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_binary
    records = [
      [["\x00".b, nil, "\xff".b]],
      [nil],
    ]
    iterated_records = []
    target = build(:binary, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_string
    records = [
      [
        [
          "Ruby",
          nil,
          "\u3042", # U+3042 HIRAGANA LETTER A
        ],
      ],
      [nil],
    ]
    iterated_records = []
    target = build(:string, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_date32
    records = [
      [
        [
          Date.new(1960, 1, 1),
          nil,
          Date.new(2017, 8, 23),
        ],
      ],
      [nil],
    ]
    iterated_records = []
    target = build(:date32, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_date64
    records = [
      [
        [
          DateTime.new(1960, 1, 1, 2, 9, 30),
          nil,
          DateTime.new(2017, 8, 23, 14, 57, 2),
        ],
      ],
      [nil],
    ]
    iterated_records = []
    target = build(:date64, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_timestamp_second
    records = [
      [
        [
          Time.parse("1960-01-01T02:09:30Z"),
          nil,
          Time.parse("2017-08-23T14:57:02Z"),
        ],
      ],
      [nil],
    ]
    iterated_records = []
    target = build({
                     type: :timestamp,
                     unit: :second,
                   },
                   records)
    iterated_records = []
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_timestamp_milli
    records = [
      [
        [
          Time.parse("1960-01-01T02:09:30.123Z"),
          nil,
          Time.parse("2017-08-23T14:57:02.987Z"),
        ],
      ],
      [nil],
    ]
    iterated_records = []
    target = build({
                     type: :timestamp,
                     unit: :milli,
                   },
                   records)
    iterated_records = []
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_timestamp_micro
    records = [
      [
        [
          Time.parse("1960-01-01T02:09:30.123456Z"),
          nil,
          Time.parse("2017-08-23T14:57:02.987654Z"),
        ],
      ],
      [nil],
    ]
    iterated_records = []
    target = build({
                     type: :timestamp,
                     unit: :micro,
                   },
                   records)
    iterated_records = []
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_timestamp_nano
    records = [
      [
        [
          Time.parse("1960-01-01T02:09:30.123456789Z"),
          nil,
          Time.parse("2017-08-23T14:57:02.987654321Z"),
        ],
      ],
      [nil],
    ]
    iterated_records = []
    target = build({
                     type: :timestamp,
                     unit: :nano,
                   },
                   records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_time32_second
    unit = Arrow::TimeUnit::SECOND
    records = [
      [
        [
          # 00:10:00
          Arrow::Time.new(unit, 60 * 10),
          nil,
          # 02:00:09
          Arrow::Time.new(unit, 60 * 60 * 2 + 9),
        ],
      ],
      [nil],
    ]
    iterated_records = []
    target = build({
                     type: :time32,
                     unit: :second,
                   },
                   records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_time32_milli
    unit = Arrow::TimeUnit::MILLI
    records = [
      [
        [
          # 00:10:00.123
          Arrow::Time.new(unit, (60 * 10) * 1000 + 123),
          nil,
          # 02:00:09.987
          Arrow::Time.new(unit, (60 * 60 * 2 + 9) * 1000 + 987),
        ],
      ],
      [nil],
    ]
    iterated_records = []
    target = build({
                     type: :time32,
                     unit: :milli,
                   },
                   records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_time64_micro
    unit = Arrow::TimeUnit::MICRO
    records = [
      [
        [
          # 00:10:00.123456
          Arrow::Time.new(unit, (60 * 10) * 1_000_000 + 123_456),
          nil,
          # 02:00:09.987654
          Arrow::Time.new(unit, (60 * 60 * 2 + 9) * 1_000_000 + 987_654),
        ],
      ],
      [nil],
    ]
    iterated_records = []
    target = build({
                     type: :time64,
                     unit: :micro,
                   },
                   records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_time64_nano
    unit = Arrow::TimeUnit::NANO
    records = [
      [
        [
          # 00:10:00.123456789
          Arrow::Time.new(unit, (60 * 10) * 1_000_000_000 + 123_456_789),
          nil,
          # 02:00:09.987654321
          Arrow::Time.new(unit, (60 * 60 * 2 + 9) * 1_000_000_000 + 987_654_321),
        ],
      ],
      [nil],
    ]
    iterated_records = []
    target = build({
                     type: :time64,
                     unit: :nano,
                   },
                   records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_decimal128
    records = [
      [
        [
          BigDecimal("92.92"),
          nil,
          BigDecimal("29.29"),
        ],
      ],
      [nil],
    ]
    iterated_records = []
    target = build({
                     type: :decimal128,
                     precision: 8,
                     scale: 2,
                   },
                   records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_decimal256
    records = [
      [
        [
          BigDecimal("92.92"),
          nil,
          BigDecimal("29.29"),
        ],
      ],
      [nil],
    ]
    iterated_records = []
    target = build({
                     type: :decimal256,
                     precision: 38,
                     scale: 2,
                   },
                   records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_month_interval
    records = [
      [[1, nil, 12]],
      [nil],
    ]
    iterated_records = []
    target = build(:month_interval, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_day_time_interval
    records = [
      [
        [
          {day: 1, millisecond: 100},
          nil,
          {day: 2, millisecond: 300},
        ]
      ],
      [nil],
    ]
    iterated_records = []
    target = build(:day_time_interval, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_month_day_nano_interval
    records = [
      [
        [
          {month: 1, day: 1, nanosecond: 100},
          nil,
          {month: 2, day: 3, nanosecond: 400},
        ]
      ],
      [nil],
    ]
    iterated_records = []
    target = build(:month_day_nano_interval, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_list
    records = [
      [
        [
          [
            true,
            nil,
          ],
          nil,
          [
            nil,
            false,
          ],
        ],
      ],
      [nil],
    ]
    iterated_records = []
    target = build({
                     type: :list,
                     field: {
                       name: :sub_element,
                       type: :boolean,
                     },
                   },
                   records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_struct
    records = [
      [
        [
          {"field" => true},
          nil,
          {"field" => nil},
        ],
      ],
      [nil],
    ]
    iterated_records = []
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
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_map
    records = [
      [
        [
          {"key1" => true, "key2" => nil},
          nil,
        ],
      ],
      [nil],
    ]
    iterated_records = []
    target = build({
                     type: :map,
                     key: :string,
                     item: :boolean,
                   },
                   records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def remove_union_field_names(records)
    records.collect do |record|
      record.collect do |column|
        if column.nil?
          column
        else
          column.collect do |value|
            if value.nil?
              value
            else
              value.values[0]
            end
          end
        end
      end
    end
  end

  def test_sparse_union
    records = [
      [
        [
          {"field1" => true},
          nil,
          {"field2" => 29},
          {"field2" => nil},
        ],
      ],
      [nil],
    ]
    iterated_records = []
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
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(remove_union_field_names(records),
                 iterated_records)
  end

  def test_dense_union
    records = [
      [
        [
          {"field1" => true},
          nil,
          {"field2" => 29},
          {"field2" => nil},
        ],
      ],
      [nil],
    ]
    iterated_records = []
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
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(remove_union_field_names(records),
                 iterated_records)
  end

  def test_dictionary
    records = [
      [
        [
          "Ruby",
          nil,
          "GLib",
        ],
      ],
      [nil],
    ]
    iterated_records = []
    target = build({
                     type: :dictionary,
                     index_data_type: :int8,
                     value_data_type: :string,
                     ordered: false,
                   },
                   records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end
end

class EachRawRecordRecordBatchListArrayTest < Test::Unit::TestCase
  include EachRawRecordListArrayTests

  def build(type, records)
    Arrow::RecordBatch.new(build_schema(type), records)
  end
end

class EachRawRecordTableListArrayTest < Test::Unit::TestCase
  include EachRawRecordListArrayTests

  def build(type, records)
    Arrow::Table.new(build_schema(type), records)
  end
end
