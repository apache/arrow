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

module ValuesBasicArraysTests
  def test_null
    target = build(Arrow::NullArray.new(4))
    assert_equal([nil] * 4, target.values)
  end

  def test_boolean
    values = [true, nil, false]
    target = build(Arrow::BooleanArray.new(values))
    assert_equal(values, target.values)
  end

  def test_int8
    values = [
      -(2 ** 7),
      nil,
      (2 ** 7) - 1,
    ]
    target = build(Arrow::Int8Array.new(values))
    assert_equal(values, target.values)
  end

  def test_uint8
    values = [
      0,
      nil,
      (2 ** 8) - 1,
    ]
    target = build(Arrow::UInt8Array.new(values))
    assert_equal(values, target.values)
  end

  def test_int16
    values = [
      -(2 ** 15),
      nil,
      (2 ** 15) - 1,
    ]
    target = build(Arrow::Int16Array.new(values))
    assert_equal(values, target.values)
  end

  def test_uint16
    values = [
      0,
      nil,
      (2 ** 16) - 1,
    ]
    target = build(Arrow::UInt16Array.new(values))
    assert_equal(values, target.values)
  end

  def test_int32
    values = [
      -(2 ** 31),
      nil,
      (2 ** 31) - 1,
    ]
    target = build(Arrow::Int32Array.new(values))
    assert_equal(values, target.values)
  end

  def test_uint32
    values = [
      0,
      nil,
      (2 ** 32) - 1,
    ]
    target = build(Arrow::UInt32Array.new(values))
    assert_equal(values, target.values)
  end

  def test_int64
    values = [
      -(2 ** 63),
      nil,
      (2 ** 63) - 1,
    ]
    target = build(Arrow::Int64Array.new(values))
    assert_equal(values, target.values)
  end

  def test_uint64
    values = [
      0,
      nil,
      (2 ** 64) - 1,
    ]
    target = build(Arrow::UInt64Array.new(values))
    assert_equal(values, target.values)
  end

  def test_float
    values = [
      -1.0,
      nil,
      1.0,
    ]
    target = build(Arrow::FloatArray.new(values))
    assert_equal(values, target.values)
  end

  def test_double
    values = [
      -1.0,
      nil,
      1.0,
    ]
    target = build(Arrow::DoubleArray.new(values))
    assert_equal(values, target.values)
  end

  def test_binary
    values = [
      "\x00".b,
      nil,
      "\xff".b,
    ]
    target = build(Arrow::BinaryArray.new(values))
    assert_equal(values, target.values)
  end

  def test_tring
    values = [
      "Ruby",
      nil,
      "\u3042", # U+3042 HIRAGANA LETTER A
    ]
    target = build(Arrow::StringArray.new(values))
    assert_equal(values, target.values)
  end

  def test_date32
    values = [
      Date.new(1960, 1, 1),
      nil,
      Date.new(2017, 8, 23),
    ]
    target = build(Arrow::Date32Array.new(values))
    assert_equal(values, target.values)
  end

  def test_date64
    values = [
      DateTime.new(1960, 1, 1, 2, 9, 30),
      nil,
      DateTime.new(2017, 8, 23, 14, 57, 2),
    ]
    target = build(Arrow::Date64Array.new(values))
    assert_equal(values, target.values)
  end

  def test_timestamp_second
    values = [
      Time.parse("1960-01-01T02:09:30Z"),
      nil,
      Time.parse("2017-08-23T14:57:02Z"),
    ]
    target = build(Arrow::TimestampArray.new(:second, values))
    assert_equal(values, target.values)
  end

  def test_timestamp_milli
    values = [
      Time.parse("1960-01-01T02:09:30.123Z"),
      nil,
      Time.parse("2017-08-23T14:57:02.987Z"),
    ]
    target = build(Arrow::TimestampArray.new(:milli, values))
    assert_equal(values, target.values)
  end

  def test_timestamp_micro
    values = [
      Time.parse("1960-01-01T02:09:30.123456Z"),
      nil,
      Time.parse("2017-08-23T14:57:02.987654Z"),
    ]
    target = build(Arrow::TimestampArray.new(:micro, values))
    assert_equal(values, target.values)
  end

  def test_timestamp_nano
    values = [
      Time.parse("1960-01-01T02:09:30.123456789Z"),
      nil,
      Time.parse("2017-08-23T14:57:02.987654321Z"),
    ]
    target = build(Arrow::TimestampArray.new(:nano, values))
    assert_equal(values, target.values)
  end

  def test_time32_second
    unit = Arrow::TimeUnit::SECOND
    values = [
      Arrow::Time.new(unit, 60 * 10), # 00:10:00
      nil,
      Arrow::Time.new(unit, 60 * 60 * 2 + 9), # 02:00:09
    ]
    target = build(Arrow::Time32Array.new(:second, values))
    assert_equal(values, target.values)
  end

  def test_time32_milli
    unit = Arrow::TimeUnit::MILLI
    values = [
      Arrow::Time.new(unit, (60 * 10) * 1000 + 123), # 00:10:00.123
      nil,
      Arrow::Time.new(unit, (60 * 60 * 2 + 9) * 1000 + 987), # 02:00:09.987
    ]
    target = build(Arrow::Time32Array.new(:milli, values))
    assert_equal(values, target.values)
  end

  def test_time64_micro
    unit = Arrow::TimeUnit::MICRO
    values = [
      # 00:10:00.123456
      Arrow::Time.new(unit, (60 * 10) * 1_000_000 + 123_456),
      nil,
      # 02:00:09.987654
      Arrow::Time.new(unit, (60 * 60 * 2 + 9) * 1_000_000 + 987_654),
    ]
    target = build(Arrow::Time64Array.new(:micro, values))
    assert_equal(values, target.values)
  end

  def test_time64_nano
    unit = Arrow::TimeUnit::NANO
    values = [
      # 00:10:00.123456789
      Arrow::Time.new(unit, (60 * 10) * 1_000_000_000 + 123_456_789),
      nil,
      # 02:00:09.987654321
      Arrow::Time.new(unit, (60 * 60 * 2 + 9) * 1_000_000_000 + 987_654_321),
    ]
    target = build(Arrow::Time64Array.new(:nano, values))
    assert_equal(values, target.values)
  end

  def test_decimal128
    values = [
      BigDecimal("92.92"),
      nil,
      BigDecimal("29.29"),
    ]
    data_type = Arrow::Decimal128DataType.new(8, 2)
    target = build(Arrow::Decimal128Array.new(data_type, values))
    assert_equal(values, target.values)
  end
end

class ValuesArrayBasicArraysTest < Test::Unit::TestCase
  include ValuesBasicArraysTests

  def build(array)
    array
  end
end

class ValuesChunkedArrayBasicArraysTest < Test::Unit::TestCase
  include ValuesBasicArraysTests

  def build(array)
    Arrow::ChunkedArray.new([array])
  end
end
