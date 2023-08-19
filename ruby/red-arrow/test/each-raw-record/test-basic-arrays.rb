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

module EachRawRecordBasicArraysTests
  def test_null
    records = [
      [nil],
      [nil],
      [nil],
    ]
    iterated_records = []
    target = build({column: :null}, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_boolean
    records = [
      [true],
      [nil],
      [false],
    ]
    iterated_records = []
    target = build({column: :boolean}, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_int8
    records = [
      [-(2 ** 7)],
      [nil],
      [(2 ** 7) - 1],
    ]
    iterated_records = []
    target = build({column: :int8}, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_uint8
    records = [
      [0],
      [nil],
      [(2 ** 8) - 1],
    ]
    iterated_records = []
    target = build({column: :uint8}, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_int16
    records = [
      [-(2 ** 15)],
      [nil],
      [(2 ** 15) - 1],
    ]
    iterated_records = []
    target = build({column: :int16}, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_uint16
    records = [
      [0],
      [nil],
      [(2 ** 16) - 1],
    ]
    iterated_records = []
    target = build({column: :uint16}, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_int32
    records = [
      [-(2 ** 31)],
      [nil],
      [(2 ** 31) - 1],
    ]
    iterated_records = []
    target = build({column: :int32}, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_uint32
    records = [
      [0],
      [nil],
      [(2 ** 32) - 1],
    ]
    iterated_records = []
    target = build({column: :uint32}, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_int64
    records = [
      [-(2 ** 63)],
      [nil],
      [(2 ** 63) - 1],
    ]
    iterated_records = []
    target = build({column: :int64}, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_uint64
    records = [
      [0],
      [nil],
      [(2 ** 64) - 1],
    ]
    iterated_records = []
    target = build({column: :uint64}, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_half_float
    records = [
      [-1.5],
      [nil],
      [1.5],
    ]
    iterated_records = []
    target = build({column: :half_float}, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_float
    records = [
      [-1.0],
      [nil],
      [1.0],
    ]
    iterated_records = []
    target = build({column: :float}, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_double
    records = [
      [-1.0],
      [nil],
      [1.0],
    ]
    iterated_records = []
    target = build({column: :double}, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_binary
    records = [
      ["\x00".b],
      [nil],
      ["\xff".b],
    ]
    iterated_records = []
    target = build({column: :binary}, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_string
    records = [
      ["Ruby"],
      [nil],
      ["\u3042"], # U+3042 HIRAGANA LETTER A
    ]
    iterated_records = []
    target = build({column: :string}, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_date32
    records = [
      [Date.new(1960, 1, 1)],
      [nil],
      [Date.new(2017, 8, 23)],
    ]
    iterated_records = []
    target = build({column: :date32}, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_date64
    records = [
      [DateTime.new(1960, 1, 1, 2, 9, 30)],
      [nil],
      [DateTime.new(2017, 8, 23, 14, 57, 2)],
    ]
    iterated_records = []
    target = build({column: :date64}, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_timestamp_second
    records = [
      [Time.parse("1960-01-01T02:09:30Z")],
      [nil],
      [Time.parse("2017-08-23T14:57:02Z")],
    ]
    iterated_records = []
    target = build({
                     column: {
                       type: :timestamp,
                       unit: :second,
                     }
                   },
                   records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_timestamp_milli
    records = [
      [Time.parse("1960-01-01T02:09:30.123Z")],
      [nil],
      [Time.parse("2017-08-23T14:57:02.987Z")],
    ]
    iterated_records = []
    target = build({
                     column: {
                       type: :timestamp,
                       unit: :milli,
                     }
                   },
                   records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_timestamp_micro
    records = [
      [Time.parse("1960-01-01T02:09:30.123456Z")],
      [nil],
      [Time.parse("2017-08-23T14:57:02.987654Z")],
    ]
    iterated_records = []
    target = build({
                     column: {
                       type: :timestamp,
                       unit: :micro,
                     }
                   },
                   records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_timestamp_nano
    records = [
      [Time.parse("1960-01-01T02:09:30.123456789Z")],
      [nil],
      [Time.parse("2017-08-23T14:57:02.987654321Z")],
    ]
    iterated_records = []
    target = build({
                     column: {
                       type: :timestamp,
                       unit: :nano,
                     }
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
      [Arrow::Time.new(unit, 60 * 10)], # 00:10:00
      [nil],
      [Arrow::Time.new(unit, 60 * 60 * 2 + 9)], # 02:00:09
    ]
    iterated_records = []
    target = build({
                     column: {
                       type: :time32,
                       unit: :second,
                     }
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
      [Arrow::Time.new(unit, (60 * 10) * 1000 + 123)], # 00:10:00.123
      [nil],
      [Arrow::Time.new(unit, (60 * 60 * 2 + 9) * 1000 + 987)], # 02:00:09.987
    ]
    iterated_records = []
    target = build({
                     column: {
                       type: :time32,
                       unit: :milli,
                     }
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
      # 00:10:00.123456
      [Arrow::Time.new(unit, (60 * 10) * 1_000_000 + 123_456)],
      [nil],
      # 02:00:09.987654
      [Arrow::Time.new(unit, (60 * 60 * 2 + 9) * 1_000_000 + 987_654)],
    ]
    iterated_records = []
    target = build({
                     column: {
                       type: :time64,
                       unit: :micro,
                     }
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
      # 00:10:00.123456789
      [Arrow::Time.new(unit, (60 * 10) * 1_000_000_000 + 123_456_789)],
      [nil],
      # 02:00:09.987654321
      [Arrow::Time.new(unit, (60 * 60 * 2 + 9) * 1_000_000_000 + 987_654_321)],
    ]
    iterated_records = []
    target = build({
                     column: {
                       type: :time64,
                       unit: :nano,
                     }
                   },
                   records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_decimal128
    records = [
      [BigDecimal("92.92")],
      [nil],
      [BigDecimal("29.29")],
    ]
    iterated_records = []
    target = build({
                     column: {
                       type: :decimal128,
                       precision: 8,
                       scale: 2,
                     }
                   },
                   records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_decimal256
    records = [
      [BigDecimal("92.92")],
      [nil],
      [BigDecimal("29.29")],
    ]
    iterated_records = []
    target = build({
                     column: {
                       type: :decimal256,
                       precision: 38,
                       scale: 2,
                     }
                   },
                   records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_month_interval
    records = [
      [1],
      [nil],
      [12],
    ]
    iterated_records = []
    target = build({column: :month_interval}, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_day_time_interval
    records = [
      [{day: 1, millisecond: 100}],
      [nil],
      [{day: 2, millisecond: 300}],
    ]
    iterated_records = []
    target = build({column: :day_time_interval}, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end

  def test_month_day_nano_interval
    records = [
      [{month: 1, day: 1, nanosecond: 100}],
      [nil],
      [{month: 2, day: 3, nanosecond: 400}],
    ]
    iterated_records = []
    target = build({column: :month_day_nano_interval}, records)
    target.each_raw_record do |record|
      iterated_records << record
    end
    assert_equal(records, iterated_records)
  end
end

class EachRawRecordRecordBatchBasicArraysTest< Test::Unit::TestCase
  include EachRawRecordBasicArraysTests

  def build(schema, records)
    Arrow::RecordBatch.new(schema, records)
  end
end

class EachRawRecordTableBasicArraysTest < Test::Unit::TestCase
  include EachRawRecordBasicArraysTests

  def build(schema, records)
    Arrow::Table.new(schema,
                     [
                       Arrow::RecordBatch.new(schema, records[0, 2]),
                       Arrow::RecordBatch.new(schema, records[2..-1]),
                     ])
  end
end
