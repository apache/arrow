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

module RawRecordsListArrayTests
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
    target = build(:null, records)
    assert_equal(records, target.raw_records)
  end

  def test_boolean
    records = [
      [[true, nil, false]],
      [nil],
    ]
    target = build(:boolean, records)
    assert_equal(records, target.raw_records)
  end

  def test_int8
    records = [
      [[-(2 ** 7), nil, (2 ** 7) - 1]],
      [nil],
    ]
    target = build(:int8, records)
    assert_equal(records, target.raw_records)
  end

  def test_uint8
    records = [
      [[0, nil, (2 ** 8) - 1]],
      [nil],
    ]
    target = build(:uint8, records)
    assert_equal(records, target.raw_records)
  end

  def test_int16
    records = [
      [[-(2 ** 15), nil, (2 ** 15) - 1]],
      [nil],
    ]
    target = build(:int16, records)
    assert_equal(records, target.raw_records)
  end

  def test_uint16
    records = [
      [[0, nil, (2 ** 16) - 1]],
      [nil],
    ]
    target = build(:uint16, records)
    assert_equal(records, target.raw_records)
  end

  def test_int32
    records = [
      [[-(2 ** 31), nil, (2 ** 31) - 1]],
      [nil],
    ]
    target = build(:int32, records)
    assert_equal(records, target.raw_records)
  end

  def test_uint32
    records = [
      [[0, nil, (2 ** 32) - 1]],
      [nil],
    ]
    target = build(:uint32, records)
    assert_equal(records, target.raw_records)
  end

  def test_int64
    records = [
      [[-(2 ** 63), nil, (2 ** 63) - 1]],
      [nil],
    ]
    target = build(:int64, records)
    assert_equal(records, target.raw_records)
  end

  def test_uint64
    records = [
      [[0, nil, (2 ** 64) - 1]],
      [nil],
    ]
    target = build(:uint64, records)
    assert_equal(records, target.raw_records)
  end

  def test_float
    records = [
      [[-1.0, nil, 1.0]],
      [nil],
    ]
    target = build(:float, records)
    assert_equal(records, target.raw_records)
  end

  def test_double
    records = [
      [[-1.0, nil, 1.0]],
      [nil],
    ]
    target = build(:double, records)
    assert_equal(records, target.raw_records)
  end

  def test_binary
    records = [
      [["\x00".b, nil, "\xff".b]],
      [nil],
    ]
    target = build(:binary, records)
    assert_equal(records, target.raw_records)
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
    target = build(:string, records)
    assert_equal(records, target.raw_records)
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
    target = build(:date32, records)
    assert_equal(records, target.raw_records)
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
    target = build(:date64, records)
    assert_equal(records, target.raw_records)
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
    target = build({
                     type: :timestamp,
                     unit: :second,
                   },
                   records)
    assert_equal(records, target.raw_records)
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
    target = build({
                     type: :timestamp,
                     unit: :milli,
                   },
                   records)
    assert_equal(records, target.raw_records)
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
    target = build({
                     type: :timestamp,
                     unit: :micro,
                   },
                   records)
    assert_equal(records, target.raw_records)
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
    target = build({
                     type: :time64,
                     unit: :nano,
                   },
                   records)
    assert_equal(records, target.raw_records)
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
    target = build({
                     type: :decimal128,
                     precision: 8,
                     scale: 2,
                   },
                   records)
    assert_equal(records, target.raw_records)
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
      [
        [
          {"field" => true},
          nil,
          {"field" => nil},
        ],
      ],
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

  def test_sparse
    omit("Need to add support for SparseUnionArrayBuilder")
    records = [
      [
        [
          {"field1" => true},
          nil,
          {"field2" => nil},
        ],
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
    assert_equal(records, target.raw_records)
  end

  def test_dense
    omit("Need to add support for DenseUnionArrayBuilder")
    records = [
      [
        [
          {"field1" => true},
          nil,
          {"field2" => nil},
        ],
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
    assert_equal(records, target.raw_records)
  end

  def test_dictionary
    omit("Need to add support for DictionaryArrayBuilder")
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
    dictionary = Arrow::StringArray.new(["GLib", "Ruby"])
    target = build({
                     type: :dictionary,
                     index_data_type: :int8,
                     dictionary: dictionary,
                     ordered: true,
                   },
                   records)
    assert_equal(records, target.raw_records)
  end
end

class RawRecordsRecordBatchListArrayTest < Test::Unit::TestCase
  include RawRecordsListArrayTests

  def build(type, records)
    Arrow::RecordBatch.new(build_schema(type), records)
  end
end

class RawRecordsTableListArrayTest < Test::Unit::TestCase
  include RawRecordsListArrayTests

  def build(type, records)
    Arrow::Table.new(build_schema(type), records)
  end
end
