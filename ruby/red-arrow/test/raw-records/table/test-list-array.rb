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

require_relative '../helper/list-array'

class RawRecordsTableListArrayTest < Test::Unit::TestCase
  include RawRecordsListArrayHelper

  def make_table(type, records)
    record_batch = Arrow::RecordBatch.new(fields(type), records)
    Arrow::Table.new(record_batch.schema, [record_batch])
  end

  test("NullArray") do
    records = [
      [[nil, nil, nil]],
      [nil],
    ]
    table = make_table(:null, records)
    assert_equal(records, table.raw_records)
  end

  test("BooleanArray") do
    records = [
      [[true, nil, false]],
      [nil],
    ]
    table = make_table(:boolean, records)
    assert_equal(records, table.raw_records)
  end

  test("Int8Array") do
    records = [
      [[-(2 ** 7), nil, (2 ** 7) - 1]],
      [nil],
    ]
    table = make_table(:int8, records)
    assert_equal(records, table.raw_records)
  end

  test("UInt8Array") do
    records = [
      [[0, nil, (2 ** 8) - 1]],
      [nil],
    ]
    table = make_table(:uint8, records)
    assert_equal(records, table.raw_records)
  end

  test("Int16Array") do
    records = [
      [[-(2 ** 15), nil, (2 ** 15) - 1]],
      [nil],
    ]
    table = make_table(:int16, records)
    assert_equal(records, table.raw_records)
  end

  test("UInt16Array") do
    records = [
      [[0, nil, (2 ** 16) - 1]],
      [nil],
    ]
    table = make_table(:uint16, records)
    assert_equal(records, table.raw_records)
  end

  test("Int32Array") do
    records = [
      [[-(2 ** 31), nil, (2 ** 31) - 1]],
      [nil],
    ]
    table = make_table(:int32, records)
    assert_equal(records, table.raw_records)
  end

  test("UInt32Array") do
    records = [
      [[0, nil, (2 ** 32) - 1]],
      [nil],
    ]
    table = make_table(:uint32, records)
    assert_equal(records, table.raw_records)
  end

  test("Int64Array") do
    records = [
      [[-(2 ** 63), nil, (2 ** 63) - 1]],
      [nil],
    ]
    table = make_table(:int64, records)
    assert_equal(records, table.raw_records)
  end

  test("UInt64Array") do
    records = [
      [[0, nil, (2 ** 64) - 1]],
      [nil],
    ]
    table = make_table(:uint64, records)
    assert_equal(records, table.raw_records)
  end

  test("FloatArray") do
    records = [
      [[-1.0, nil, 1.0]],
      [nil],
    ]
    table = make_table(:float, records)
    assert_equal(records, table.raw_records)
  end

  test("DoubleArray") do
    records = [
      [[-1.0, nil, 1.0]],
      [nil],
    ]
    table = make_table(:double, records)
    assert_equal(records, table.raw_records)
  end

  test("BinaryArray") do
    records = [
      [["\x00".b, nil, "\xff".b]],
      [nil],
    ]
    table = make_table(:binary, records)
    assert_equal(records, table.raw_records)
  end

  test("StringArray") do
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
    table = make_table(:string, records)
    assert_equal(records, table.raw_records)
  end

  test("Date32Array") do
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
    table = make_table(:date32, records)
    assert_equal(records, table.raw_records)
  end

  test("Date64Array") do
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
    table = make_table(:date64, records)
    assert_equal(records, table.raw_records)
  end

  sub_test_case("TimestampArray") do
    test("second") do
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
      table = make_table({
                           type: :timestamp,
                           unit: :second,
                         },
                         records)
      assert_equal(records, table.raw_records)
    end

    test("milli") do
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
      table = make_table({
                           type: :timestamp,
                           unit: :milli,
                         },
                         records)
      assert_equal(records, table.raw_records)
    end

    test("micro") do
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
      table = make_table({
                           type: :timestamp,
                           unit: :micro,
                         },
                         records)
      assert_equal(records, table.raw_records)
    end

    test("nano") do
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
      table = make_table({
                           type: :timestamp,
                           unit: :nano,
                         },
                         records)
      assert_equal(records, table.raw_records)
    end
  end

  sub_test_case("Time32Array") do
    test("second") do
      records = [
        [
          [
            60 * 10, # 00:10:00
            nil,
            60 * 60 * 2 + 9, # 02:00:09
          ],
        ],
        [nil],
      ]
      table = make_table({
                           type: :time32,
                           unit: :second,
                         },
                         records)
      assert_equal(records, table.raw_records)
    end

    test("milli") do
      records = [
        [
          [
            (60 * 10) * 1000 + 123, # 00:10:00.123
            nil,
            (60 * 60 * 2 + 9) * 1000 + 987, # 02:00:09.987
          ],
        ],
        [nil],
      ]
      table = make_table({
                           type: :time32,
                           unit: :milli,
                         },
                         records)
      assert_equal(records, table.raw_records)
    end
  end

  sub_test_case("Time64Array") do
    test("micro") do
      records = [
        [
          [
            (60 * 10) * 1_000_000 + 123_456, # 00:10:00.123456
            nil,
            (60 * 60 * 2 + 9) * 1_000_000 + 987_654, # 02:00:09.987654
          ],
        ],
        [nil],
      ]
      table = make_table({
                           type: :time64,
                           unit: :micro,
                         },
                         records)
      assert_equal(records, table.raw_records)
    end

    test("nano") do
      records = [
        [
          [
            (60 * 10) * 1_000_000_000 + 123_456_789, # 00:10:00.123456789
            nil,
            (60 * 60 * 2 + 9) * 1_000_000_000 + 987_654_321, # 02:00:09.987654321
          ],
        ],
        [nil],
      ]
      table = make_table({
                           type: :time64,
                           unit: :nano,
                         },
                         records)
      assert_equal(records, table.raw_records)
    end
  end

  test("Decimal128Array") do
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
    table = make_table({
                         type: :decimal128,
                         precision: 8,
                         scale: 2,
                       },
                       records)
    assert_equal(records, table.raw_records)
  end

  test("ListArray") do
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
    table = make_table({
                         type: :list,
                         field: {
                           name: :sub_element,
                           type: :boolean,
                         },
                       },
                       records)
    assert_equal(records, table.raw_records)
  end

  test("StructArray") do
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
    table = make_table({
                         type: :struct,
                         fields: [
                           {
                             name: :field,
                             type: :boolean,
                           },
                         ],
                       },
                       records)
    assert_equal(records, table.raw_records)
  end

  test("SparseUnionArray") do
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
    table = make_table({
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
    assert_equal(records, table.raw_records)
  end

  test("DenseUnionArray") do
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
    table = make_table({
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
    assert_equal(records, table.raw_records)
  end

  test("DictionaryArray") do
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
    table = make_table({
                         type: :dictionary,
                         index_data_type: :int8,
                         dictionary: dictionary,
                         ordered: true,
                       },
                       records)
    assert_equal(records, table.raw_records)
  end
end

