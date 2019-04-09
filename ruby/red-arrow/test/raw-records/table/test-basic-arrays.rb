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

class RawRecordsTableBasicArraysTest < Test::Unit::TestCase
  def make_table(schema, records)
    record_batch = Arrow::RecordBatch.new(schema, records)
    Arrow::Table.new(record_batch.schema, [record_batch])
  end

  test("NullArray") do
    records = [
      [nil],
      [nil],
      [nil],
    ]
    table = make_table({column: :null}, records)
    assert_equal(records, table.raw_records)
  end

  test("BooleanArray") do
    records = [
      [true],
      [nil],
      [false],
    ]
    table = make_table({column: :boolean}, records)
    assert_equal(records, table.raw_records)
  end

  test("Int8Array") do
    records = [
      [-(2 ** 7)],
      [nil],
      [(2 ** 7) - 1],
    ]
    table = make_table({column: :int8}, records)
    assert_equal(records, table.raw_records)
  end

  test("UInt8Array") do
    records = [
      [0],
      [nil],
      [(2 ** 8) - 1],
    ]
    table = make_table({column: :uint8}, records)
    assert_equal(records, table.raw_records)
  end

  test("Int16Array") do
    records = [
      [-(2 ** 15)],
      [nil],
      [(2 ** 15) - 1],
    ]
    table = make_table({column: :int16}, records)
    assert_equal(records, table.raw_records)
  end

  test("UInt16Array") do
    records = [
      [0],
      [nil],
      [(2 ** 16) - 1],
    ]
    table = make_table({column: :uint16}, records)
    assert_equal(records, table.raw_records)
  end

  test("Int32Array") do
    records = [
      [-(2 ** 31)],
      [nil],
      [(2 ** 31) - 1],
    ]
    table = make_table({column: :int32}, records)
    assert_equal(records, table.raw_records)
  end

  test("UInt32Array") do
    records = [
      [0],
      [nil],
      [(2 ** 32) - 1],
    ]
    table = make_table({column: :uint32}, records)
    assert_equal(records, table.raw_records)
  end

  test("Int64Array") do
    records = [
      [-(2 ** 63)],
      [nil],
      [(2 ** 63) - 1],
    ]
    table = make_table({column: :int64}, records)
    assert_equal(records, table.raw_records)
  end

  test("UInt64Array") do
    records = [
      [0],
      [nil],
      [(2 ** 64) - 1],
    ]
    table = make_table({column: :uint64}, records)
    assert_equal(records, table.raw_records)
  end

  test("FloatArray") do
    records = [
      [-1.0],
      [nil],
      [1.0],
    ]
    table = make_table({column: :float}, records)
    assert_equal(records, table.raw_records)
  end

  test("DoubleArray") do
    records = [
      [-1.0],
      [nil],
      [1.0],
    ]
    table = make_table({column: :double}, records)
    assert_equal(records, table.raw_records)
  end

  test("BinaryArray") do
    records = [
      ["\x00".b],
      [nil],
      ["\xff".b],
    ]
    table = make_table({column: :binary}, records)
    assert_equal(records, table.raw_records)
  end

  test("StringArray") do
    records = [
      ["Ruby"],
      [nil],
      ["\u3042"], # U+3042 HIRAGANA LETTER A
    ]
    table = make_table({column: :string}, records)
    assert_equal(records, table.raw_records)
  end

  test("Date32Array") do
    records = [
      [Date.new(1960, 1, 1)],
      [nil],
      [Date.new(2017, 8, 23)],
    ]
    table = make_table({column: :date32}, records)
    assert_equal(records, table.raw_records)
  end

  test("Date64Array") do
    records = [
      [DateTime.new(1960, 1, 1, 2, 9, 30)],
      [nil],
      [DateTime.new(2017, 8, 23, 14, 57, 2)],
    ]
    table = make_table({column: :date64}, records)
    assert_equal(records, table.raw_records)
  end

  sub_test_case("TimestampArray") do
    test("second") do
      records = [
        [Time.parse("1960-01-01T02:09:30Z")],
        [nil],
        [Time.parse("2017-08-23T14:57:02Z")],
      ]
      table = make_table({
                           column: {
                             type: :timestamp,
                             unit: :second,
                           }
                         },
                         records)
      assert_equal(records, table.raw_records)
    end

    test("milli") do
      records = [
        [Time.parse("1960-01-01T02:09:30.123Z")],
        [nil],
        [Time.parse("2017-08-23T14:57:02.987Z")],
      ]
      table = make_table({
                           column: {
                             type: :timestamp,
                             unit: :milli,
                           }
                         },
                         records)
      assert_equal(records, table.raw_records)
    end

    test("micro") do
      records = [
        [Time.parse("1960-01-01T02:09:30.123456Z")],
        [nil],
        [Time.parse("2017-08-23T14:57:02.987654Z")],
      ]
      table = make_table({
                           column: {
                             type: :timestamp,
                             unit: :micro,
                           }
                         },
                         records)
      assert_equal(records, table.raw_records)
    end

    test("nano") do
      records = [
        [Time.parse("1960-01-01T02:09:30.123456789Z")],
        [nil],
        [Time.parse("2017-08-23T14:57:02.987654321Z")],
      ]
      table = make_table({
                           column: {
                             type: :timestamp,
                             unit: :nano,
                           }
                         },
                         records)
      assert_equal(records, table.raw_records)
    end
  end

  sub_test_case("Time32Array") do
    test("second") do
      records = [
        [60 * 10], # 00:10:00
        [nil],
        [60 * 60 * 2 + 9], # 02:00:09
      ]
      table = make_table({
                           column: {
                             type: :time32,
                             unit: :second,
                           }
                         },
                         records)
      assert_equal(records, table.raw_records)
    end

    test("milli") do
      records = [
        [(60 * 10) * 1000 + 123], # 00:10:00.123
        [nil],
        [(60 * 60 * 2 + 9) * 1000 + 987], # 02:00:09.987
      ]
      table = make_table({
                           column: {
                             type: :time32,
                             unit: :milli,
                           }
                         },
                         records)
      assert_equal(records, table.raw_records)
    end
  end

  sub_test_case("Time64Array") do
    test("micro") do
      records = [
        [(60 * 10) * 1_000_000 + 123_456], # 00:10:00.123456
        [nil],
        [(60 * 60 * 2 + 9) * 1_000_000 + 987_654], # 02:00:09.987654
      ]
      table = make_table({
                           column: {
                             type: :time64,
                             unit: :micro,
                           }
                         },
                         records)
      assert_equal(records, table.raw_records)
    end

    test("nano") do
      records = [
        [(60 * 10) * 1_000_000_000 + 123_456_789], # 00:10:00.123456789
        [nil],
        [(60 * 60 * 2 + 9) * 1_000_000_000 + 987_654_321], # 02:00:09.987654321
      ]
      table = make_table({
                           column: {
                             type: :time64,
                             unit: :nano,
                           }
                         },
                         records)
      assert_equal(records, table.raw_records)
    end
  end

  test("Decimal128Array") do
    records = [
      [BigDecimal("92.92")],
      [nil],
      [BigDecimal("29.29")],
    ]
    table = make_table({
                         column: {
                           type: :decimal128,
                           precision: 8,
                           scale: 2,
                         }
                       },
                       records)
    assert_equal(records, table.raw_records)
  end
end
