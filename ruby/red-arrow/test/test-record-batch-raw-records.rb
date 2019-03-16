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

class RecordBatchRawRecordsTest < Test::Unit::TestCase
  test("NullArray") do
    records = [
      [nil],
      [nil],
      [nil],
      [nil],
    ]
    array = Arrow::NullArray.new(records.size)
    schema = Arrow::Schema.new(column: :null)
    record_batch = Arrow::RecordBatch.new(schema,
                                          records.size,
                                          [array])
    assert_equal(records, record_batch.raw_records)
  end

  test("BooleanArray") do
    records = [
      [true],
      [nil],
      [false],
    ]
    record_batch = Arrow::RecordBatch.new({column: :boolean},
                                          records)
    assert_equal(records, record_batch.raw_records)
  end

  test("Int8Array") do
    records = [
      [-(2 ** 7)],
      [nil],
      [(2 ** 7) - 1],
    ]
    record_batch = Arrow::RecordBatch.new({column: :int8},
                                          records)
    assert_equal(records, record_batch.raw_records)
  end

  test("UInt8Array") do
    records = [
      [0],
      [nil],
      [(2 ** 8) - 1],
    ]
    record_batch = Arrow::RecordBatch.new({column: :uint8},
                                          records)
    assert_equal(records, record_batch.raw_records)
  end

  test("Int16Array") do
    records = [
      [-(2 ** 15)],
      [nil],
      [(2 ** 15) - 1],
    ]
    record_batch = Arrow::RecordBatch.new({column: :int16},
                                          records)
    assert_equal(records, record_batch.raw_records)
  end

  test("UInt16Array") do
    records = [
      [0],
      [nil],
      [(2 ** 16) - 1],
    ]
    record_batch = Arrow::RecordBatch.new({column: :uint16},
                                          records)
    assert_equal(records, record_batch.raw_records)
  end

  test("Int32Array") do
    records = [
      [-(2 ** 31)],
      [nil],
      [(2 ** 31) - 1],
    ]
    record_batch = Arrow::RecordBatch.new({column: :int32},
                                          records)
    assert_equal(records, record_batch.raw_records)
  end

  test("UInt32Array") do
    records = [
      [0],
      [nil],
      [(2 ** 32) - 1],
    ]
    record_batch = Arrow::RecordBatch.new({column: :uint32},
                                          records)
    assert_equal(records, record_batch.raw_records)
  end

  test("Int64Array") do
    records = [
      [-(2 ** 63)],
      [nil],
      [(2 ** 63) - 1],
    ]
    record_batch = Arrow::RecordBatch.new({column: :int64},
                                          records)
    assert_equal(records, record_batch.raw_records)
  end

  test("UInt64Array") do
    records = [
      [0],
      [nil],
      [(2 ** 64) - 1],
    ]
    record_batch = Arrow::RecordBatch.new({column: :uint64},
                                          records)
    assert_equal(records, record_batch.raw_records)
  end

  test("FloatArray") do
    records = [
      [-1.0],
      [nil],
      [1.0],
    ]
    record_batch = Arrow::RecordBatch.new({column: :float},
                                          records)
    assert_equal(records, record_batch.raw_records)
  end

  test("DoubleArray") do
    records = [
      [-1.0],
      [nil],
      [1.0],
    ]
    record_batch = Arrow::RecordBatch.new({column: :double},
                                          records)
    assert_equal(records, record_batch.raw_records)
  end

  test("BinaryArray") do
    records = [
      ["\x00".b],
      [nil],
      ["\xff".b],
    ]
    record_batch = Arrow::RecordBatch.new({column: :binary},
                                          records)
    assert_equal(records, record_batch.raw_records)
  end

  test("StringArray") do
    records = [
      ["Ruby"],
      [nil],
      ["\u3042"], # U+3042 HIRAGANA LETTER A
    ]
    record_batch = Arrow::RecordBatch.new({column: :string},
                                          records)
    assert_equal(records, record_batch.raw_records)
  end

  test("Date32Array") do
    records = [
      [Date.new(1960, 1, 1)],
      [nil],
      [Date.new(2017, 8, 23)],
    ]
    record_batch = Arrow::RecordBatch.new({column: :date32},
                                          records)
    assert_equal(records, record_batch.raw_records)
  end

  test("Date64Array") do
    records = [
      [DateTime.new(1960, 1, 1, 2, 9, 30)],
      [nil],
      [DateTime.new(2017, 8, 23, 14, 57, 2)],
    ]
    record_batch = Arrow::RecordBatch.new({column: :date64},
                                          records)
    assert_equal(records, record_batch.raw_records)
  end

  sub_test_case("TimestampArray") do
    test("second") do
      records = [
        [Time.parse("1960-01-01T02:09:30Z")],
        [nil],
        [Time.parse("2017-08-23T14:57:02Z")],
      ]
      record_batch = Arrow::RecordBatch.new({
                                              column: {
                                                type: :timestamp,
                                                unit: :second,
                                              }
                                            },
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    test("milli") do
      records = [
        [Time.parse("1960-01-01T02:09:30.123Z")],
        [nil],
        [Time.parse("2017-08-23T14:57:02.987Z")],
      ]
      record_batch = Arrow::RecordBatch.new({
                                              column: {
                                                type: :timestamp,
                                                unit: :milli,
                                              }
                                            },
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    test("micro") do
      records = [
        [Time.parse("1960-01-01T02:09:30.123456Z")],
        [nil],
        [Time.parse("2017-08-23T14:57:02.987654Z")],
      ]
      record_batch = Arrow::RecordBatch.new({
                                              column: {
                                                type: :timestamp,
                                                unit: :micro,
                                              }
                                            },
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    test("nano") do
      records = [
        [Time.parse("1960-01-01T02:09:30.123456789Z")],
        [nil],
        [Time.parse("2017-08-23T14:57:02.987654321Z")],
      ]
      record_batch = Arrow::RecordBatch.new({
                                              column: {
                                                type: :timestamp,
                                                unit: :nano,
                                              }
                                            },
                                            records)
      assert_equal(records, record_batch.raw_records)
    end
  end

  sub_test_case("Time32Array") do
    test("second") do
      records = [
        [60 * 10], # 00:10:00
        [nil],
        [60 * 60 * 2 + 9], # 02:00:09
      ]
      record_batch = Arrow::RecordBatch.new({
                                              column: {
                                                type: :time32,
                                                unit: :second,
                                              }
                                            },
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    test("milli") do
      records = [
        [(60 * 10) * 1000 + 123], # 00:10:00.123
        [nil],
        [(60 * 60 * 2 + 9) * 1000 + 987], # 02:00:09.987
      ]
      record_batch = Arrow::RecordBatch.new({
                                              column: {
                                                type: :time32,
                                                unit: :milli,
                                              }
                                            },
                                            records)
      assert_equal(records, record_batch.raw_records)
    end
  end

  sub_test_case("Time64Array") do
    test("micro") do
      records = [
        [(60 * 10) * 1_000_000 + 123_456], # 00:10:00.123456
        [nil],
        [(60 * 60 * 2 + 9) * 1_000_000 + 987_654], # 02:00:09.987654
      ]
      record_batch = Arrow::RecordBatch.new({
                                              column: {
                                                type: :time64,
                                                unit: :micro,
                                              }
                                            },
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    test("nano") do
      records = [
        [(60 * 10) * 1_000_000_000 + 123_456_789], # 00:10:00.123456789
        [nil],
        [(60 * 60 * 2 + 9) * 1_000_000_000 + 987_654_321], # 02:00:09.987654321
      ]
      record_batch = Arrow::RecordBatch.new({
                                              column: {
                                                type: :time64,
                                                unit: :nano,
                                              }
                                            },
                                            records)
      assert_equal(records, record_batch.raw_records)
    end
  end

  test("Decimal128Array") do
    records = [
      [BigDecimal("92.92")],
      [nil],
      [BigDecimal("29.29")],
    ]
    record_batch = Arrow::RecordBatch.new({
                                            column: {
                                              type: :decimal128,
                                              precision: 8,
                                              scale: 2,
                                            }
                                          },
                                          records)
    assert_equal(records, record_batch.raw_records)
  end

  sub_test_case("ListArray") do
    def fields(type)
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

    test("NullArray") do
      omit("Need to add support for NullArrayBuilder")
      records = [
        [[nil, nil, nil]],
        [nil],
      ]
      record_batch = Arrow::RecordBatch.new(fields(:null),
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    test("BooleanArray") do
      records = [
        [[true, nil, false]],
        [nil],
      ]
      record_batch = Arrow::RecordBatch.new(fields(:boolean),
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    test("Int8Array") do
      records = [
        [[-(2 ** 7), nil, (2 ** 7) - 1]],
        [nil],
      ]
      record_batch = Arrow::RecordBatch.new(fields(:int8),
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    test("UInt8Array") do
      records = [
        [[0, nil, (2 ** 8) - 1]],
        [nil],
      ]
      record_batch = Arrow::RecordBatch.new(fields(:uint8),
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    test("Int16Array") do
      records = [
        [[-(2 ** 15), nil, (2 ** 15) - 1]],
        [nil],
      ]
      record_batch = Arrow::RecordBatch.new(fields(:int16),
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    test("UInt16Array") do
      records = [
        [[0, nil, (2 ** 16) - 1]],
        [nil],
      ]
      record_batch = Arrow::RecordBatch.new(fields(:uint16),
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    test("Int32Array") do
      records = [
        [[-(2 ** 31), nil, (2 ** 31) - 1]],
        [nil],
      ]
      record_batch = Arrow::RecordBatch.new(fields(:int32),
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    test("UInt32Array") do
      records = [
        [[0, nil, (2 ** 32) - 1]],
        [nil],
      ]
      record_batch = Arrow::RecordBatch.new(fields(:uint32),
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    test("Int64Array") do
      records = [
        [[-(2 ** 63), nil, (2 ** 63) - 1]],
        [nil],
      ]
      record_batch = Arrow::RecordBatch.new(fields(:int64),
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    test("UInt64Array") do
      records = [
        [[0, nil, (2 ** 64) - 1]],
        [nil],
      ]
      record_batch = Arrow::RecordBatch.new(fields(:uint64),
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    test("FloatArray") do
      records = [
        [[-1.0, nil, 1.0]],
        [nil],
      ]
      record_batch = Arrow::RecordBatch.new(fields(:float),
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    test("DoubleArray") do
      records = [
        [[-1.0, nil, 1.0]],
        [nil],
      ]
      record_batch = Arrow::RecordBatch.new(fields(:double),
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    test("BinaryArray") do
      records = [
        [["\x00".b, nil, "\xff".b]],
        [nil],
      ]
      record_batch = Arrow::RecordBatch.new(fields(:binary),
                                            records)
      assert_equal(records, record_batch.raw_records)
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
      record_batch = Arrow::RecordBatch.new(fields(:string),
                                            records)
      assert_equal(records, record_batch.raw_records)
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
      record_batch = Arrow::RecordBatch.new(fields(:date32),
                                            records)
      assert_equal(records, record_batch.raw_records)
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
      record_batch = Arrow::RecordBatch.new(fields(:date64),
                                            records)
      assert_equal(records, record_batch.raw_records)
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
        record_batch = Arrow::RecordBatch.new(fields(type: :timestamp,
                                                     unit: :second),
                                              records)
        assert_equal(records, record_batch.raw_records)
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
        record_batch = Arrow::RecordBatch.new(fields(type: :timestamp,
                                                     unit: :milli),
                                              records)
        assert_equal(records, record_batch.raw_records)
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
        record_batch = Arrow::RecordBatch.new(fields(type: :timestamp,
                                                     unit: :micro),
                                              records)
        assert_equal(records, record_batch.raw_records)
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
        record_batch = Arrow::RecordBatch.new(fields(type: :timestamp,
                                                     unit: :nano),
                                              records)
        assert_equal(records, record_batch.raw_records)
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
        record_batch = Arrow::RecordBatch.new(fields(type: :time32,
                                                     unit: :second),
                                              records)
        assert_equal(records, record_batch.raw_records)
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
        record_batch = Arrow::RecordBatch.new(fields(type: :time32,
                                                     unit: :milli),
                                              records)
        assert_equal(records, record_batch.raw_records)
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
        record_batch = Arrow::RecordBatch.new(fields(type: :time64,
                                                     unit: :micro),
                                              records)
        assert_equal(records, record_batch.raw_records)
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
        record_batch = Arrow::RecordBatch.new(fields(type: :time64,
                                                     unit: :nano),
                                              records)
        assert_equal(records, record_batch.raw_records)
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
      record_batch = Arrow::RecordBatch.new(fields(type: :decimal128,
                                                   precision: 8,
                                                   scale: 2),
                                            records)
      assert_equal(records, record_batch.raw_records)
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
      record_batch = Arrow::RecordBatch.new(fields(type: :list,
                                                   field: {
                                                     name: :sub_element,
                                                     type: :boolean,
                                                   }),
                                            records)
      assert_equal(records, record_batch.raw_records)
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
      record_batch = Arrow::RecordBatch.new(fields(type: :struct,
                                                   fields: [
                                                     {
                                                       name: :field,
                                                       type: :boolean,
                                                     },
                                                   ]),
                                            records)
      assert_equal(records, record_batch.raw_records)
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
      record_batch = Arrow::RecordBatch.new(fields(type: :sparse_union,
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
                                                   type_codes: [0, 1]),
                                            records)
      assert_equal(records, record_batch.raw_records)
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
      record_batch = Arrow::RecordBatch.new(fields(type: :dense_union,
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
                                                   type_codes: [0, 1]),
                                            records)
      assert_equal(records, record_batch.raw_records)
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
      record_batch = Arrow::RecordBatch.new(fields(type: :dictionary,
                                                   index_data_type: :int8,
                                                   dictionary: dictionary,
                                                   ordered: true),
                                            records)
      assert_equal(records, record_batch.raw_records)
    end
  end

  sub_test_case("StructArray") do
    def fields(type)
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

    test("NullArray") do
      omit("Need to add support for NullArrayBuilder")
      records = [
        [{"field" => nil}],
        [nil],
      ]
      record_batch = Arrow::RecordBatch.new(fields(:null),
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    test("BooleanArray") do
      records = [
        [{"field" => true}],
        [nil],
        [{"field" => nil}],
      ]
      record_batch = Arrow::RecordBatch.new(fields(:boolean),
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    test("Int8Array") do
      records = [
        [{"field" => -(2 ** 7)}],
        [nil],
        [{"field" => nil}],
      ]
      record_batch = Arrow::RecordBatch.new(fields(:int8),
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    test("UInt8Array") do
      records = [
        [{"field" => (2 ** 8) - 1}],
        [nil],
        [{"field" => nil}],
      ]
      record_batch = Arrow::RecordBatch.new(fields(:uint8),
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    test("Int16Array") do
      records = [
        [{"field" => -(2 ** 15)}],
        [nil],
        [{"field" => nil}],
      ]
      record_batch = Arrow::RecordBatch.new(fields(:int16),
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    test("UInt16Array") do
      records = [
        [{"field" => (2 ** 16) - 1}],
        [nil],
        [{"field" => nil}],
      ]
      record_batch = Arrow::RecordBatch.new(fields(:uint16),
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    test("Int32Array") do
      records = [
        [{"field" => -(2 ** 31)}],
        [nil],
        [{"field" => nil}],
      ]
      record_batch = Arrow::RecordBatch.new(fields(:int32),
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    test("UInt32Array") do
      records = [
        [{"field" => (2 ** 32) - 1}],
        [nil],
        [{"field" => nil}],
      ]
      record_batch = Arrow::RecordBatch.new(fields(:uint32),
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    test("Int64Array") do
      records = [
        [{"field" => -(2 ** 63)}],
        [nil],
        [{"field" => nil}],
      ]
      record_batch = Arrow::RecordBatch.new(fields(:int64),
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    test("UInt64Array") do
      records = [
        [{"field" => (2 ** 64) - 1}],
        [nil],
        [{"field" => nil}],
      ]
      record_batch = Arrow::RecordBatch.new(fields(:uint64),
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    test("FloatArray") do
      records = [
        [{"field" => -1.0}],
        [nil],
        [{"field" => nil}],
      ]
      record_batch = Arrow::RecordBatch.new(fields(:float),
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    test("DoubleArray") do
      records = [
        [{"field" => -1.0}],
        [nil],
        [{"field" => nil}],
      ]
      record_batch = Arrow::RecordBatch.new(fields(:double),
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    test("BinaryArray") do
      records = [
        [{"field" => "\xff".b}],
        [nil],
        [{"field" => nil}],
      ]
      record_batch = Arrow::RecordBatch.new(fields(:binary),
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    test("StringArray") do
      records = [
        [{"field" => "Ruby"}],
        [nil],
        [{"field" => nil}],
      ]
      record_batch = Arrow::RecordBatch.new(fields(:string),
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    test("Date32Array") do
      records = [
        [{"field" => Date.new(1960, 1, 1)}],
        [nil],
        [{"field" => nil}],
      ]
      record_batch = Arrow::RecordBatch.new(fields(:date32),
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    test("Date64Array") do
      records = [
        [{"field" => DateTime.new(1960, 1, 1, 2, 9, 30)}],
        [nil],
        [{"field" => nil}],
      ]
      record_batch = Arrow::RecordBatch.new(fields(:date64),
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    sub_test_case("TimestampArray") do
      test("second") do
        records = [
          [{"field" => Time.parse("1960-01-01T02:09:30Z")}],
          [nil],
          [{"field" => nil}],
        ]
        record_batch = Arrow::RecordBatch.new(fields(type: :timestamp,
                                                     unit: :second),
                                              records)
        assert_equal(records, record_batch.raw_records)
      end

      test("milli") do
        records = [
          [{"field" => Time.parse("1960-01-01T02:09:30.123Z")}],
          [nil],
          [{"field" => nil}],
        ]
        record_batch = Arrow::RecordBatch.new(fields(type: :timestamp,
                                                     unit: :milli),
                                              records)
        assert_equal(records, record_batch.raw_records)
      end

      test("micro") do
        records = [
          [{"field" => Time.parse("1960-01-01T02:09:30.123456Z")}],
          [nil],
          [{"field" => nil}],
        ]
        record_batch = Arrow::RecordBatch.new(fields(type: :timestamp,
                                                     unit: :micro),
                                              records)
        assert_equal(records, record_batch.raw_records)
      end

      test("nano") do
        records = [
          [{"field" => Time.parse("1960-01-01T02:09:30.123456789Z")}],
          [nil],
          [{"field" => nil}],
        ]
        record_batch = Arrow::RecordBatch.new(fields(type: :timestamp,
                                                     unit: :nano),
                                              records)
        assert_equal(records, record_batch.raw_records)
      end
    end

    sub_test_case("Time32Array") do
      test("second") do
        records = [
          [{"field" => 60 * 10}], # 00:10:00
          [nil],
          [{"field" => nil}],
        ]
        record_batch = Arrow::RecordBatch.new(fields(type: :time32,
                                                     unit: :second),
                                              records)
        assert_equal(records, record_batch.raw_records)
      end

      test("milli") do
        records = [
          [{"field" => (60 * 10) * 1000 + 123}], # 00:10:00.123
          [nil],
          [{"field" => nil}],
        ]
        record_batch = Arrow::RecordBatch.new(fields(type: :time32,
                                                     unit: :milli),
                                              records)
        assert_equal(records, record_batch.raw_records)
      end
    end

    sub_test_case("Time64Array") do
      test("micro") do
        records = [
          [{"field" => (60 * 10) * 1_000_000 + 123_456}], # 00:10:00.123456
          [nil],
          [{"field" => nil}],
        ]
        record_batch = Arrow::RecordBatch.new(fields(type: :time64,
                                                     unit: :micro),
                                              records)
        assert_equal(records, record_batch.raw_records)
      end

      test("nano") do
        records = [
          # 00:10:00.123456789
          [{"field" => (60 * 10) * 1_000_000_000 + 123_456_789}],
          [nil],
          [{"field" => nil}],
        ]
        record_batch = Arrow::RecordBatch.new(fields(type: :time64,
                                                     unit: :nano),
                                              records)
        assert_equal(records, record_batch.raw_records)
      end
    end

    test("Decimal128Array") do
      records = [
        [{"field" => BigDecimal("92.92")}],
        [nil],
        [{"field" => nil}],
      ]
      record_batch = Arrow::RecordBatch.new(fields(type: :decimal128,
                                                   precision: 8,
                                                   scale: 2),
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    test("ListArray") do
      records = [
        [{"field" => [true, nil, false]}],
        [nil],
        [{"field" => nil}],
      ]
      record_batch = Arrow::RecordBatch.new(fields(type: :list,
                                                   field: {
                                                     name: :sub_element,
                                                     type: :boolean,
                                                   }),
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    test("StructArray") do
      records = [
        [{"field" => {"sub_field" => true}}],
        [nil],
        [{"field" => nil}],
        [{"field" => {"sub_field" => nil}}],
      ]
      record_batch = Arrow::RecordBatch.new(fields(type: :struct,
                                                   fields: [
                                                     {
                                                       name: :sub_field,
                                                       type: :boolean,
                                                     },
                                                   ]),
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    test("SparseUnionArray") do
      omit("Need to add support for SparseUnionArrayBuilder")
      records = [
        [{"field" => {"field1" => true}}],
        [nil],
        [{"field" => nil}],
        [{"field" => {"field2" => nil}}],
      ]
      record_batch = Arrow::RecordBatch.new(fields(type: :sparse_union,
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
                                                   type_codes: [0, 1]),
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    test("DenseUnionArray") do
      omit("Need to add support for DenseUnionArrayBuilder")
      records = [
        [{"field" => {"field1" => true}}],
        [nil],
        [{"field" => nil}],
        [{"field" => {"field2" => nil}}],
      ]
      record_batch = Arrow::RecordBatch.new(fields(type: :dense_union,
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
                                                   type_codes: [0, 1]),
                                            records)
      assert_equal(records, record_batch.raw_records)
    end

    test("DictionaryArray") do
      omit("Need to add support for DictionaryArrayBuilder")
      records = [
        [{"field" => "Ruby"}],
        [nil],
        [{"field" => nil}],
        [{"field" => "GLib"}],
      ]
      dictionary = Arrow::StringArray.new(["GLib", "Ruby"])
      record_batch = Arrow::RecordBatch.new(fields(type: :dictionary,
                                                   index_data_type: :int8,
                                                   dictionary: dictionary,
                                                   ordered: true),
                                            records)
      assert_equal(records, record_batch.raw_records)
    end
  end

  sub_test_case("SparseUnionArray") do
    def fields(type, type_codes)
      field_description = {}
      if type.is_a?(Hash)
        field_description = field_description.merge(type)
      else
        field_description[:type] = type
      end
      {
        column: {
          type: :sparse_union,
          fields: [
            field_description.merge(name: "0"),
            field_description.merge(name: "1"),
          ],
          type_codes: type_codes,
        },
      }
    end

    # TODO: Use Arrow::RecordBatch.new(fields(type), records)
    def build_record_batch(type, records)
      type_codes = [0, 1]
      schema = Arrow::Schema.new(fields(type, type_codes))
      type_ids = []
      arrays = schema.fields[0].data_type.fields.collect do |field|
        sub_schema = Arrow::Schema.new([field])
        sub_records = records.collect do |record|
          [record[0].nil? ? nil : record[0][field.name]]
        end
        sub_record_batch = Arrow::RecordBatch.new(sub_schema,
                                                  sub_records)
        sub_record_batch.columns[0]
      end
      records.each do |record|
        column = record[0]
        if column.nil?
          type_ids << nil
        elsif column.key?("0")
          type_ids << type_codes[0]
        elsif column.key?("1")
          type_ids << type_codes[1]
        end
      end
      # TODO
      # union_array = Arrow::SparseUnionArray.new(schema.fields[0].data_type,
      #                                           Arrow::Int8Array.new(type_ids),
      #                                           arrays)
      union_array = Arrow::SparseUnionArray.new(Arrow::Int8Array.new(type_ids),
                                                arrays)
      schema = Arrow::Schema.new(column: union_array.value_data_type)
      Arrow::RecordBatch.new(schema,
                             records.size,
                             [union_array])
    end

    test("NullArray") do
      omit("Need to add support for NullArrayBuilder")
      records = [
        [{"0" => nil}],
        [nil],
      ]
      record_batch = build_record_batch(:null, records)
      assert_equal(records, record_batch.raw_records)
    end

    test("BooleanArray") do
      records = [
        [{"0" => true}],
        [nil],
        [{"1" => nil}],
      ]
      record_batch = build_record_batch(:boolean, records)
      assert_equal(records, record_batch.raw_records)
    end

    test("Int8Array") do
      records = [
        [{"0" => -(2 ** 7)}],
        [nil],
        [{"1" => nil}],
      ]
      record_batch = build_record_batch(:int8, records)
      assert_equal(records, record_batch.raw_records)
    end

    test("UInt8Array") do
      records = [
        [{"0" => (2 ** 8) - 1}],
        [nil],
        [{"1" => nil}],
      ]
      record_batch = build_record_batch(:uint8, records)
      assert_equal(records, record_batch.raw_records)
    end

    test("Int16Array") do
      records = [
        [{"0" => -(2 ** 15)}],
        [nil],
        [{"1" => nil}],
      ]
      record_batch = build_record_batch(:int16, records)
      assert_equal(records, record_batch.raw_records)
    end

    test("UInt16Array") do
      records = [
        [{"0" => (2 ** 16) - 1}],
        [nil],
        [{"1" => nil}],
      ]
      record_batch = build_record_batch(:uint16, records)
      assert_equal(records, record_batch.raw_records)
    end

    test("Int32Array") do
      records = [
        [{"0" => -(2 ** 31)}],
        [nil],
        [{"1" => nil}],
      ]
      record_batch = build_record_batch(:int32, records)
      assert_equal(records, record_batch.raw_records)
    end

    test("UInt32Array") do
      records = [
        [{"0" => (2 ** 32) - 1}],
        [nil],
        [{"1" => nil}],
      ]
      record_batch = build_record_batch(:uint32, records)
      assert_equal(records, record_batch.raw_records)
    end

    test("Int64Array") do
      records = [
        [{"0" => -(2 ** 63)}],
        [nil],
        [{"1" => nil}],
      ]
      record_batch = build_record_batch(:int64, records)
      assert_equal(records, record_batch.raw_records)
    end

    test("UInt64Array") do
      records = [
        [{"0" => (2 ** 64) - 1}],
        [nil],
        [{"1" => nil}],
      ]
      record_batch = build_record_batch(:uint64, records)
      assert_equal(records, record_batch.raw_records)
    end

    test("FloatArray") do
      records = [
        [{"0" => -1.0}],
        [nil],
        [{"1" => nil}],
      ]
      record_batch = build_record_batch(:float, records)
      assert_equal(records, record_batch.raw_records)
    end

    test("DoubleArray") do
      records = [
        [{"0" => -1.0}],
        [nil],
        [{"1" => nil}],
      ]
      record_batch = build_record_batch(:double, records)
      assert_equal(records, record_batch.raw_records)
    end

    test("BinaryArray") do
      records = [
        [{"0" => "\xff".b}],
        [nil],
        [{"1" => nil}],
      ]
      record_batch = build_record_batch(:binary, records)
      assert_equal(records, record_batch.raw_records)
    end

    test("StringArray") do
      records = [
        [{"0" => "Ruby"}],
        [nil],
        [{"1" => nil}],
      ]
      record_batch = build_record_batch(:string, records)
      assert_equal(records, record_batch.raw_records)
    end

    test("Date32Array") do
      records = [
        [{"0" => Date.new(1960, 1, 1)}],
        [nil],
        [{"1" => nil}],
      ]
      record_batch = build_record_batch(:date32, records)
      assert_equal(records, record_batch.raw_records)
    end

    test("Date64Array") do
      records = [
        [{"0" => DateTime.new(1960, 1, 1, 2, 9, 30)}],
        [nil],
        [{"1" => nil}],
      ]
      record_batch = build_record_batch(:date64, records)
      assert_equal(records, record_batch.raw_records)
    end

    sub_test_case("TimestampArray") do
      test("second") do
        records = [
          [{"0" => Time.parse("1960-01-01T02:09:30Z")}],
          [nil],
          [{"1" => nil}],
        ]
        record_batch = build_record_batch({
                                            type: :timestamp,
                                            unit: :second,
                                          },
                                          records)
        assert_equal(records, record_batch.raw_records)
      end

      test("milli") do
        records = [
          [{"0" => Time.parse("1960-01-01T02:09:30.123Z")}],
          [nil],
          [{"1" => nil}],
        ]
        record_batch = build_record_batch({
                                            type: :timestamp,
                                            unit: :milli,
                                          },
                                          records)
        assert_equal(records, record_batch.raw_records)
      end

      test("micro") do
        records = [
          [{"0" => Time.parse("1960-01-01T02:09:30.123456Z")}],
          [nil],
          [{"1" => nil}],
        ]
        record_batch = build_record_batch({
                                            type: :timestamp,
                                            unit: :micro,
                                          },
                                          records)
        assert_equal(records, record_batch.raw_records)
      end

      test("nano") do
        records = [
          [{"0" => Time.parse("1960-01-01T02:09:30.123456789Z")}],
          [nil],
          [{"1" => nil}],
        ]
        record_batch = build_record_batch({
                                            type: :timestamp,
                                            unit: :nano,
                                          },
                                          records)
        assert_equal(records, record_batch.raw_records)
      end
    end

    sub_test_case("Time32Array") do
      test("second") do
        records = [
          [{"0" => 60 * 10}], # 00:10:00
          [nil],
          [{"1" => nil}],
        ]
        record_batch = build_record_batch({
                                            type: :time32,
                                            unit: :second,
                                          },
                                          records)
        assert_equal(records, record_batch.raw_records)
      end

      test("milli") do
        records = [
          [{"0" => (60 * 10) * 1000 + 123}], # 00:10:00.123
          [nil],
          [{"1" => nil}],
        ]
        record_batch = build_record_batch({
                                            type: :time32,
                                            unit: :milli,
                                          },
                                          records)
        assert_equal(records, record_batch.raw_records)
      end
    end

    sub_test_case("Time64Array") do
      test("micro") do
        records = [
          [{"0" => (60 * 10) * 1_000_000 + 123_456}], # 00:10:00.123456
          [nil],
          [{"1" => nil}],
        ]
        record_batch = build_record_batch({
                                            type: :time64,
                                            unit: :micro,
                                          },
                                          records)
        assert_equal(records, record_batch.raw_records)
      end

      test("nano") do
        records = [
          # 00:10:00.123456789
          [{"0" => (60 * 10) * 1_000_000_000 + 123_456_789}],
          [nil],
          [{"1" => nil}],
        ]
        record_batch = build_record_batch({
                                            type: :time64,
                                            unit: :nano,
                                          },
                                          records)
        assert_equal(records, record_batch.raw_records)
      end
    end

    test("Decimal128Array") do
      records = [
        [{"0" => BigDecimal("92.92")}],
        [nil],
        [{"1" => nil}],
      ]
      record_batch = build_record_batch({
                                          type: :decimal128,
                                          precision: 8,
                                          scale: 2,
                                        },
                                        records)
      assert_equal(records, record_batch.raw_records)
    end

    test("ListArray") do
      records = [
        [{"0" => [true, nil, false]}],
        [nil],
        [{"1" => nil}],
      ]
      record_batch = build_record_batch({
                                          type: :list,
                                          field: {
                                            name: :sub_element,
                                            type: :boolean,
                                          },
                                        },
                                        records)
      assert_equal(records, record_batch.raw_records)
    end

    test("StructArray") do
      records = [
        [{"0" => {"sub_field" => true}}],
        [nil],
        [{"1" => nil}],
        [{"0" => {"sub_field" => nil}}],
      ]
      record_batch = build_record_batch({
                                          type: :struct,
                                          fields: [
                                            {
                                              name: :sub_field,
                                              type: :boolean,
                                            },
                                          ],
                                        },
                                        records)
      assert_equal(records, record_batch.raw_records)
    end

    test("SparseUnionArray") do
      omit("Need to add support for SparseUnionArrayBuilder")
      records = [
        [{"0" => {"field1" => true}}],
        [nil],
        [{"1" => nil}],
        [{"0" => {"field2" => nil}}],
      ]
      record_batch = build_record_batch({
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
      assert_equal(records, record_batch.raw_records)
    end

    test("DenseUnionArray") do
      omit("Need to add support for DenseUnionArrayBuilder")
      records = [
        [{"0" => {"field1" => true}}],
        [nil],
        [{"1" => nil}],
        [{"0" => {"field2" => nil}}],
      ]
      record_batch = build_record_batch({
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
      assert_equal(records, record_batch.raw_records)
    end

    test("DictionaryArray") do
      omit("Need to add support for DictionaryArrayBuilder")
      records = [
        [{"0" => "Ruby"}],
        [nil],
        [{"1" => nil}],
        [{"0" => "GLib"}],
      ]
      dictionary = Arrow::StringArray.new(["GLib", "Ruby"])
      record_batch = build_record_batch({
                                          type: :dictionary,
                                          index_data_type: :int8,
                                          dictionary: dictionary,
                                          ordered: true,
                                        },
                                        records)
      assert_equal(records, record_batch.raw_records)
    end
  end

  sub_test_case("with basic arrays") do
    def setup
      @string_values = ["apple", "orange", "watermelon", nil, "タコ"]
      @string_array = Arrow::StringArray.new(@string_values)

      base_value = 2**20
      @uint32_values = [1, 2, 4, 8, 16].map {|x| x + base_value }
      @uint32_array = Arrow::UInt32Array.new(@uint32_values)

      @double_values = [10.1, 11.2, 12.3, 13.4, 14.4]
      @double_array = Arrow::DoubleArray.new(@double_values)

      @decimal128_values = ['123.45', '234.56', nil, '345.67', "456.78"]
      @decimal128_array = Arrow::Decimal128Array.new(
        Arrow::Decimal128DataType.new(8, 2), @decimal128_values)

      @date32_values = [
        Date.new(1993,  2, 24),
        Date.new(1996, 12, 25),
        Date.new(2013,  2, 24),
        Date.new(2020, 12, 25),
        nil
      ]
      epoch_date = Date.new(1970, 1, 1)
      @date32_array = Arrow::Date32Array.new(
        @date32_values.map {|dt| dt && (dt - epoch_date).to_i }
      )

      @date64_values = [
        nil,
        DateTime.new(1993,  2, 23, 15, 0, 0),
        DateTime.new(1996, 12, 24, 15, 0, 0),
        DateTime.new(2013,  2, 23, 15, 0, 0),
        DateTime.new(2020, 12, 24, 15, 0, 0)
      ]
      @date64_array = Arrow::Date64Array.new(
        @date64_values.map {|dt| dt && dt.to_time.gmtime.to_i * 1000 }
      )

      jst = '+09:00'
      @timestamp_values = [
        Time.new(1993,  2, 24, 0, 0, 0, jst).gmtime,
        Time.new(1996, 12, 25, 0, 0, 0, jst).gmtime,
        nil,
        Time.new(2013,  2, 24, 0, 0, 0, jst).gmtime,
        Time.new(2020, 12, 25, 0, 0, 0, jst).gmtime
      ]
      @timestamp_sec_array = Arrow::TimestampArray.new(
        :second,
        @timestamp_values.map {|ts| ts && ts.to_i }
      )
      @timestamp_msec_array = Arrow::TimestampArray.new(
        :milli,
        @timestamp_values.map {|ts| ts && (ts.to_r * 1_000).to_i }
      )
      @timestamp_usec_array = Arrow::TimestampArray.new(
        :micro,
        @timestamp_values.map {|ts| ts && (ts.to_r * 1_000_000).to_i }
      )

      @dictionary = Arrow::StringArray.new(['foo', 'bar', 'baz'])
      @dictionary_indices = [0, 1, 2, 1, 0]
      @dictionary_array = Arrow::DictionaryArray.new(
        Arrow::DictionaryDataType.new(:int8, @dictionary, true),
        Arrow::Int8Array.new(@dictionary_indices)
      )

      @schema = Arrow::Schema.new(
        string: :string,
        uint32: :uint32,
        double: :double,
        decimal128: @decimal128_array.value_data_type,
        date32: :date32,
        date64: :date64,
        timestamp_sec: @timestamp_sec_array.value_data_type,
        timestamp_msec: @timestamp_msec_array.value_data_type,
        timestamp_usec: @timestamp_usec_array.value_data_type,
        dict: @dictionary_array.value_data_type
      )

      @record_batch = Arrow::RecordBatch.new(
        @schema, @string_values.length,
        [
          @string_array,
          @uint32_array,
          @double_array,
          @decimal128_array,
          @date32_array,
          @date64_array,
          @timestamp_sec_array,
          @timestamp_msec_array,
          @timestamp_usec_array,
          @dictionary_array
        ]
      )

      @expected_columnar_result = [
        @string_values,
        @uint32_values,
        @double_values,
        @decimal128_values.map {|d| d && BigDecimal(d) },
        @date32_values,
        @date64_values,
        @timestamp_values,
        @timestamp_values,
        @timestamp_values,
        @dictionary_indices
      ]
    end

    test("default") do
      raw_records = @record_batch.raw_records
      assert_equal(@expected_columnar_result.transpose, raw_records)
    end
  end

  sub_test_case('with dense union array') do
    def setup
      sub_union_type_ids = [ 0, 1 ]
      sub_union_offsets = [ 0, 0 ]
      sub_union_children = [
        Arrow::Int32Array.new([42]),
        Arrow::StringArray.new(%w[ほげ])
      ]

      dictionary = Arrow::StringArray.new(['foo', 'bar'])
      dictionary_indices = [1, 0]

      union_type_ids = [ 0, 1, 1, 0, 2, 3, 4, 5, 6, 7, 8, 8, 9, 9 ]
      union_offsets = [ 0, 0, 1, 1, 0, 0, 1, 0, 0, 0, 0, 1, 0, 1 ]
      union_children = [
        Arrow::Int32Array.new([42, -42]),
        Arrow::StringArray.new(%w[foo ほげ]),
        Arrow::Decimal128Array.new(
          Arrow::Decimal128DataType.new(8, 2),
          ['3.14']
        ),
        Arrow::Date32Array.new([Date.new(2019, 2, 13)]),
        Arrow::Date64Array.new([nil, DateTime.new(2017, 2, 13, 12, 34, 56)]),
        Arrow::TimestampArray.new(:nano, [1131689228 * 1_000_000_000]),
        Arrow::ListArray.new(
          Arrow::ListDataType.new(name: 'int', type: :int8),
          [[1, -1, 1, 2, 3]]
        ),
        Arrow::StructArray.new(
          Arrow::StructDataType.new(
            int: :int8,
            dbl: :double,
            str: :string
          ),
          [[1, 3.14, 'xyz']]
        ),
        Arrow::DenseUnionArray.new(
          Arrow::Int8Array.new(sub_union_type_ids),
          Arrow::Int32Array.new(sub_union_offsets),
          sub_union_children
        ),
        Arrow::DictionaryArray.new(
          Arrow::DictionaryDataType.new(:int8, dictionary, true),
          Arrow::Int8Array.new(dictionary_indices)
        )
      ]

      # TODO: we should test with non-continuous type codes, but we cannot
      # because arrow-glib hasn't support to make an array of a union type
      # with non-continuous type codes.
      @union_array = Arrow::DenseUnionArray.new(
        Arrow::Int8Array.new(union_type_ids),
        Arrow::Int32Array.new(union_offsets),
        union_children
      )

      @record_batch = Arrow::RecordBatch.new(
        Arrow::Schema.new(
          union: @union_array.value_data_type
        ),
        union_type_ids.length,
        [
          @union_array
        ]
      )

      @expected_columnar_result = [
        union_type_ids.map.with_index {|tid, i|
          offset = union_offsets[i]
          child = union_children[tid]
          case child
          when Arrow::DenseUnionArray
            sub_tid = sub_union_type_ids[offset]
            sub_offset = sub_union_offsets[offset]
            sub_union_children[sub_tid][sub_offset]
          when Arrow::DictionaryArray
            dictionary_indices[offset]
          when Arrow::StructArray
            child.value_data_type.fields.map {|f|
              [f.name, child.find_field(f.name)[offset]]
            }.to_h
          when Arrow::ListArray
            child[offset].to_a
          else
            child[offset]
          end
        }
      ]
      @expected_columnar_result[0][4] = BigDecimal('3.14')
      @expected_columnar_result[0][7] = Time.utc(2005, 11, 11, 6, 7, 8)
    end

    test("default") do
      raw_records = @record_batch.raw_records
      assert_equal(@expected_columnar_result.transpose, raw_records)
    end
  end
end
