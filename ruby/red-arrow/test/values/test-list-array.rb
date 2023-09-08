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

module ValuesListArrayTests
  def build_data_type(type)
    field_description = {
      name: :element,
    }
    if type.is_a?(Hash)
      field_description = field_description.merge(type)
    else
      field_description[:type] = type
    end
    Arrow::ListDataType.new(field: field_description)
  end

  def build_array(type, values)
    Arrow::ListArray.new(build_data_type(type), values)
  end

  def test_null
    values = [
      [nil, nil, nil],
      nil,
    ]
    target = build(:null, values)
    assert_equal(values, target.values)
  end

  def test_boolean
    values = [
      [true, nil, false],
      nil,
    ]
    target = build(:boolean, values)
    assert_equal(values, target.values)
  end

  def test_int8
    values = [
      [-(2 ** 7), nil, (2 ** 7) - 1],
      nil,
    ]
    target = build(:int8, values)
    assert_equal(values, target.values)
  end

  def test_uint8
    values = [
      [0, nil, (2 ** 8) - 1],
      nil,
    ]
    target = build(:uint8, values)
    assert_equal(values, target.values)
  end

  def test_int16
    values = [
      [-(2 ** 15), nil, (2 ** 15) - 1],
      nil,
    ]
    target = build(:int16, values)
    assert_equal(values, target.values)
  end

  def test_uint16
    values = [
      [0, nil, (2 ** 16) - 1],
      nil,
    ]
    target = build(:uint16, values)
    assert_equal(values, target.values)
  end

  def test_int32
    values = [
      [-(2 ** 31), nil, (2 ** 31) - 1],
      nil,
    ]
    target = build(:int32, values)
    assert_equal(values, target.values)
  end

  def test_uint32
    values = [
      [0, nil, (2 ** 32) - 1],
      nil,
    ]
    target = build(:uint32, values)
    assert_equal(values, target.values)
  end

  def test_int64
    values = [
      [-(2 ** 63), nil, (2 ** 63) - 1],
      nil,
    ]
    target = build(:int64, values)
    assert_equal(values, target.values)
  end

  def test_uint64
    values = [
      [0, nil, (2 ** 64) - 1],
      nil,
    ]
    target = build(:uint64, values)
    assert_equal(values, target.values)
  end

  def test_float
    values = [
      [-1.0, nil, 1.0],
      nil,
    ]
    target = build(:float, values)
    assert_equal(values, target.values)
  end

  def test_double
    values = [
      [-1.0, nil, 1.0],
      nil,
    ]
    target = build(:double, values)
    assert_equal(values, target.values)
  end

  def test_binary
    values = [
      ["\x00".b, nil, "\xff".b],
      nil,
    ]
    target = build(:binary, values)
    assert_equal(values, target.values)
  end

  def test_string
    values = [
      [
        "Ruby",
        nil,
        "\u3042", # U+3042 HIRAGANA LETTER A
      ],
      nil,
    ]
    target = build(:string, values)
    assert_equal(values, target.values)
  end

  def test_date32
    values = [
      [
        Date.new(1960, 1, 1),
        nil,
        Date.new(2017, 8, 23),
      ],
      nil,
    ]
    target = build(:date32, values)
    assert_equal(values, target.values)
  end

  def test_date64
    values = [
      [
        DateTime.new(1960, 1, 1, 2, 9, 30),
        nil,
        DateTime.new(2017, 8, 23, 14, 57, 2),
      ],
      nil,
    ]
    target = build(:date64, values)
    assert_equal(values, target.values)
  end

  def test_timestamp_second
    values = [
      [
        Time.parse("1960-01-01T02:09:30Z"),
        nil,
        Time.parse("2017-08-23T14:57:02Z"),
      ],
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
      [
        Time.parse("1960-01-01T02:09:30.123Z"),
        nil,
        Time.parse("2017-08-23T14:57:02.987Z"),
      ],
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
      [
        Time.parse("1960-01-01T02:09:30.123456Z"),
        nil,
        Time.parse("2017-08-23T14:57:02.987654Z"),
      ],
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
      [
        Time.parse("1960-01-01T02:09:30.123456789Z"),
        nil,
        Time.parse("2017-08-23T14:57:02.987654321Z"),
      ],
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
      [
        # 00:10:00
        Arrow::Time.new(unit, 60 * 10),
        nil,
        # 02:00:09
        Arrow::Time.new(unit, 60 * 60 * 2 + 9),
      ],
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
      [
        # 00:10:00.123
        Arrow::Time.new(unit, (60 * 10) * 1000 + 123),
        nil,
        # 02:00:09.987
        Arrow::Time.new(unit, (60 * 60 * 2 + 9) * 1000 + 987),
      ],
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
      [
        # 00:10:00.123456
        Arrow::Time.new(unit, (60 * 10) * 1_000_000 + 123_456),
        nil,
        # 02:00:09.987654
        Arrow::Time.new(unit, (60 * 60 * 2 + 9) * 1_000_000 + 987_654),
      ],
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
      [
        # 00:10:00.123456789
        Arrow::Time.new(unit, (60 * 10) * 1_000_000_000 + 123_456_789),
        nil,
        # 02:00:09.987654321
        Arrow::Time.new(unit, (60 * 60 * 2 + 9) * 1_000_000_000 + 987_654_321),
      ],
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
      [
        BigDecimal("92.92"),
        nil,
        BigDecimal("29.29"),
      ],
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
      [
        BigDecimal("92.92"),
        nil,
        BigDecimal("29.29"),
      ],
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
      [
        1,
        nil,
        12,
      ],
      nil,
    ]
    target = build(:month_interval, values)
    assert_equal(values, target.values)
  end

  def test_day_time_interval
    values = [
      [
        {day: 1, millisecond: 100},
        nil,
        {day: 2, millisecond: 300},
      ],
      nil,
    ]
    target = build(:day_time_interval, values)
    assert_equal(values, target.values)
  end

  def test_month_day_nano_interval
    values = [
      [
        {month: 1, day: 1, nanosecond: 100},
        nil,
        {month: 2, day: 3, nanosecond: 400},
      ],
      nil,
    ]
    target = build(:month_day_nano_interval, values)
    assert_equal(values, target.values)
  end

  def test_list
    values = [
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
      [
        {"field" => true},
        nil,
        {"field" => nil},
      ],
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
      [
        {"key1" => true, "key2" => nil},
        nil,
      ],
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
        value.collect do |v|
          if v.nil?
            v
          else
            v.values[0]
          end
        end
      end
    end
  end

  def test_sparse_union
    values = [
      [
        {"field1" => true},
        nil,
        {"field2" => 29},
        {"field2" => nil},
      ],
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
      [
        {"field1" => true},
        nil,
        {"field2" => 29},
        {"field2" => nil},
      ],
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
      [
        "Ruby",
        nil,
        "GLib",
      ],
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

class ValuesArrayListArrayTest < Test::Unit::TestCase
  include ValuesListArrayTests

  def build(type, values)
    build_array(type, values)
  end
end

class ValuesChunkedArrayListArrayTest < Test::Unit::TestCase
  include ValuesListArrayTests

  def build(type, values)
    Arrow::ChunkedArray.new([build_array(type, values)])
  end
end
