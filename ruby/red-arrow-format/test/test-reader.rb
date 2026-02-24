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

module ReaderTests
  def roundtrip(array)
    Dir.mktmpdir do |tmp_dir|
      table = Arrow::Table.new(value: array)
      path = File.join(tmp_dir, "data.#{file_extension}")
      table.save(path)
      File.open(path, "rb") do |input|
        reader = reader_class.new(input)
        values = []
        reader.each do |record_batch|
          values.concat(record_batch.columns[0].to_a)
        end
        [reader.schema.fields[0].type, values]
      end
    ensure
      GC.start
    end
  end

  def test_null
    type, values = roundtrip(Arrow::NullArray.new(3))
    assert_equal(["Null", [nil, nil, nil]],
                 [type.to_s, values])
  end

  def test_boolean
    type, values = roundtrip(Arrow::BooleanArray.new([true, nil, false]))
    assert_equal(["Boolean", [true, nil, false]],
                 [type.to_s, values])
  end

  def test_int8
    type, values = roundtrip(Arrow::Int8Array.new([-128, nil, 127]))
    assert_equal(["Int8", [-128, nil, 127]],
                 [type.to_s, values])
  end

  def test_uint8
    type, values = roundtrip(Arrow::UInt8Array.new([0, nil, 255]))
    assert_equal(["UInt8", [0, nil, 255]],
                 [type.to_s, values])
  end

  def test_int16
    type, values = roundtrip(Arrow::Int16Array.new([-32768, nil, 32767]))
    assert_equal(["Int16", [-32768, nil, 32767]],
                 [type.to_s, values])
  end

  def test_uint16
    type, values = roundtrip(Arrow::UInt16Array.new([0, nil, 65535]))
    assert_equal(["UInt16", [0, nil, 65535]],
                 [type.to_s, values])
  end

  def test_int32
    array = Arrow::Int32Array.new([-2147483648, nil, 2147483647])
    type, values = roundtrip(array)
    assert_equal(["Int32", [-2147483648, nil, 2147483647]],
                 [type.to_s, values])
  end

  def test_uint32
    array = Arrow::UInt32Array.new([0, nil, 4294967295])
    type, values = roundtrip(array)
    assert_equal(["UInt32", [0, nil, 4294967295]],
                 [type.to_s, values])
  end

  def test_int64
    array = Arrow::Int64Array.new([
                                    -9223372036854775808,
                                    nil,
                                    9223372036854775807
                                  ])
    type, values = roundtrip(array)
    assert_equal(["Int64", [-9223372036854775808, nil, 9223372036854775807]],
                 [type.to_s, values])
  end

  def test_uint64
    array = Arrow::UInt64Array.new([0, nil, 18446744073709551615])
    type, values = roundtrip(array)
    assert_equal(["UInt64", [0, nil, 18446744073709551615]],
                 [type.to_s, values])
  end

  def test_float32
    type, values = roundtrip(Arrow::FloatArray.new([-0.5, nil, 0.5]))
    assert_equal(["Float32", [-0.5, nil, 0.5]],
                 [type.to_s, values])
  end

  def test_float64
    type, values = roundtrip(Arrow::DoubleArray.new([-0.5, nil, 0.5]))
    assert_equal(["Float64", [-0.5, nil, 0.5]],
                 [type.to_s, values])
  end

  def test_date32
    date_2017_08_28 = 17406
    date_2025_12_09 = 20431
    array = Arrow::Date32Array.new([date_2017_08_28, nil, date_2025_12_09])
    type, values = roundtrip(array)
    assert_equal(["Date32", [date_2017_08_28, nil, date_2025_12_09]],
                 [type.to_s, values])
  end

  def test_date64
    date_2017_08_28_00_00_00 = 1503878400000
    date_2025_12_10_00_00_00 = 1765324800000
    array = Arrow::Date64Array.new([
                                     date_2017_08_28_00_00_00,
                                     nil,
                                     date_2025_12_10_00_00_00,
                                   ])
    type, values = roundtrip(array)
    assert_equal([
                   "Date64",
                   [date_2017_08_28_00_00_00, nil, date_2025_12_10_00_00_00],
                 ],
                 [type.to_s, values])
  end

  def test_time32_second
    time_00_00_10 = 10
    time_00_01_10 = 60 + 10
    array = Arrow::Time32Array.new(:second,
                                   [time_00_00_10, nil, time_00_01_10])
    type, values = roundtrip(array)
    assert_equal(["Time32(second)", [time_00_00_10, nil, time_00_01_10]],
                 [type.to_s, values])
  end

  def test_time32_millisecond
    time_00_00_10_000 = 10 * 1000
    time_00_01_10_000 = (60 + 10) * 1000
    array = Arrow::Time32Array.new(:milli,
                                   [
                                     time_00_00_10_000,
                                     nil,
                                     time_00_01_10_000,
                                   ])
    type, values = roundtrip(array)
    assert_equal([
                   "Time32(millisecond)",
                   [time_00_00_10_000, nil, time_00_01_10_000],
                 ],
                 [type.to_s, values])
  end

  def test_time64_microsecond
    time_00_00_10_000_000 = 10 * 1_000_000
    time_00_01_10_000_000 = (60 + 10) * 1_000_000
    array = Arrow::Time64Array.new(:micro,
                                   [
                                     time_00_00_10_000_000,
                                     nil,
                                     time_00_01_10_000_000,
                                   ])
    type, values = roundtrip(array)
    assert_equal([
                   "Time64(microsecond)",
                   [time_00_00_10_000_000, nil, time_00_01_10_000_000],
                 ],
                 [type.to_s, values])
  end

  def test_time64_nanosecond
    time_00_00_10_000_000_000 = 10 * 1_000_000_000
    time_00_01_10_000_000_000 = (60 + 10) * 1_000_000_000
    array = Arrow::Time64Array.new(:nano,
                                   [
                                     time_00_00_10_000_000_000,
                                     nil,
                                     time_00_01_10_000_000_000,
                                   ])
    type, values = roundtrip(array)
    assert_equal([
                   "Time64(nanosecond)",
                   [
                     time_00_00_10_000_000_000,
                     nil,
                     time_00_01_10_000_000_000,
                   ],
                 ],
                 [type.to_s, values])
  end

  def test_timestamp_second
    timestamp_2019_11_17_15_09_11 = 1574003351
    timestamp_2025_12_16_05_33_58 = 1765863238
    array = Arrow::TimestampArray.new(:second,
                                      [
                                        timestamp_2019_11_17_15_09_11,
                                        nil,
                                        timestamp_2025_12_16_05_33_58,
                                      ])
    type, values = roundtrip(array)
    assert_equal([
                   "Timestamp(second)",
                   [
                     timestamp_2019_11_17_15_09_11,
                     nil,
                     timestamp_2025_12_16_05_33_58,
                   ],
                 ],
                 [type.to_s, values])
  end

  def test_timestamp_millisecond
    timestamp_2019_11_17_15_09_11 = 1574003351 * 1_000
    timestamp_2025_12_16_05_33_58 = 1765863238 * 1_000
    array = Arrow::TimestampArray.new(:milli,
                                      [
                                        timestamp_2019_11_17_15_09_11,
                                        nil,
                                        timestamp_2025_12_16_05_33_58,
                                      ])
    type, values = roundtrip(array)
    assert_equal([
                   "Timestamp(millisecond)",
                   [
                     timestamp_2019_11_17_15_09_11,
                     nil,
                     timestamp_2025_12_16_05_33_58,
                   ],
                 ],
                 [type.to_s, values])
  end

  def test_timestamp_microsecond
    timestamp_2019_11_17_15_09_11 = 1574003351 * 1_000_000
    timestamp_2025_12_16_05_33_58 = 1765863238 * 1_000_000
    array = Arrow::TimestampArray.new(:micro,
                                      [
                                        timestamp_2019_11_17_15_09_11,
                                        nil,
                                        timestamp_2025_12_16_05_33_58,
                                      ])
    type, values = roundtrip(array)
    assert_equal([
                   "Timestamp(microsecond)",
                   [
                     timestamp_2019_11_17_15_09_11,
                     nil,
                     timestamp_2025_12_16_05_33_58,
                   ],
                 ],
                 [type.to_s, values])
  end

  def test_timestamp_nanosecond
    timestamp_2019_11_17_15_09_11 = 1574003351 * 1_000_000_000
    timestamp_2025_12_16_05_33_58 = 1765863238 * 1_000_000_000
    array = Arrow::TimestampArray.new(:nano,
                                      [
                                        timestamp_2019_11_17_15_09_11,
                                        nil,
                                        timestamp_2025_12_16_05_33_58,
                                      ])
    type, values = roundtrip(array)
    assert_equal([
                   "Timestamp(nanosecond)",
                   [
                     timestamp_2019_11_17_15_09_11,
                     nil,
                     timestamp_2025_12_16_05_33_58,
                   ],
                 ],
                 [type.to_s, values])
  end

  def test_timestamp_time_zone
    time_zone = "UTC"
    timestamp_2019_11_17_15_09_11 = 1574003351
    timestamp_2025_12_16_05_33_58 = 1765863238
    data_type = Arrow::TimestampDataType.new(:second, time_zone)
    array = Arrow::TimestampArray.new(data_type,
                                      [
                                        timestamp_2019_11_17_15_09_11,
                                        nil,
                                        timestamp_2025_12_16_05_33_58,
                                      ])
    type, values = roundtrip(array)
    assert_equal([
                   "Timestamp(second, #{time_zone})",
                   [
                     timestamp_2019_11_17_15_09_11,
                     nil,
                     timestamp_2025_12_16_05_33_58,
                   ],
                 ],
                 [type.to_s, values])
  end

  def test_year_month_interval
    type, values = roundtrip(Arrow::MonthIntervalArray.new([0, nil, 100]))
    assert_equal(["YearMonthInterval", [0, nil, 100]],
                 [type.to_s, values])
  end

  def test_day_time_interval
    array = Arrow::DayTimeIntervalArray.new([
                                              {day: 1, millisecond: 100},
                                              nil,
                                              {day: 3, millisecond: 300},
                                            ])
    type, values = roundtrip(array)
    assert_equal([
                   "DayTimeInterval",
                   [
                     [1, 100],
                     nil,
                     [3, 300],
                   ],
                 ],
                 [type.to_s, values])
  end

  def test_month_day_nano_interval
    array = Arrow::MonthDayNanoIntervalArray.new([
                                                   {
                                                     month: 1,
                                                     day: 1,
                                                     nanosecond: 100,
                                                   },
                                                   nil,
                                                   {
                                                     month: 3,
                                                     day: 3,
                                                     nanosecond: 300,
                                                   },
                                                 ])
    type, values = roundtrip(array)
    assert_equal([
                   "MonthDayNanoInterval",
                    [
                      [1, 1, 100],
                      nil,
                      [3, 3, 300],
                    ],
                 ],
                 [type.to_s, values])
  end

  def test_duration_second
    type, values = roundtrip(Arrow::DurationArray.new(:second, [0, nil, 100]))
    assert_equal(["Duration(second)", [0, nil, 100]],
                 [type.to_s, values])
  end

  def test_duration_millisecond
    array = Arrow::DurationArray.new(:milli, [0, nil, 100_000])
    type, values = roundtrip(array)
    assert_equal(["Duration(millisecond)", [0, nil, 100_000]],
                 [type.to_s, values])
  end

  def test_duration_microsecond
    array = Arrow::DurationArray.new(:micro, [0, nil, 100_000_000])
    type, values = roundtrip(array)
    assert_equal(["Duration(microsecond)", [0, nil, 100_000_000]],
                 [type.to_s, values])
  end

  def test_duration_nanosecond
    array = Arrow::DurationArray.new(:nano, [0, nil, 100_000_000_000])
    type, values = roundtrip(array)
    assert_equal(["Duration(nanosecond)", [0, nil, 100_000_000_000]],
                 [type.to_s, values])
  end

  def test_binary
    array = Arrow::BinaryArray.new(["Hello".b, nil, "World".b])
    type, values = roundtrip(array)
    assert_equal(["Binary", ["Hello".b, nil, "World".b]],
                 [type.to_s, values])
  end

  def test_large_binary
    array = Arrow::LargeBinaryArray.new(["Hello".b, nil, "World".b])
    type, values = roundtrip(array)
    assert_equal(["LargeBinary", ["Hello".b, nil, "World".b]],
                 [type.to_s, values])
  end

  def test_utf8
    array = Arrow::StringArray.new(["Hello", nil, "World"])
    type, values = roundtrip(array)
    assert_equal(["UTF8", ["Hello", nil, "World"]],
                 [type.to_s, values])
  end

  def test_large_utf8
    array = Arrow::LargeStringArray.new(["Hello", nil, "World"])
    type, values = roundtrip(array)
    assert_equal(["LargeUTF8", ["Hello", nil, "World"]],
                 [type.to_s, values])
  end

  def test_fixed_size_binary
    data_type = Arrow::FixedSizeBinaryDataType.new(4)
    array = Arrow::FixedSizeBinaryArray.new(data_type,
                                            ["0124".b, nil, "abcd".b])
    type, values = roundtrip(array)
    assert_equal(["FixedSizeBinary(4)", ["0124".b, nil, "abcd".b]],
                 [type.to_s, values])
  end

  def test_decimal128
    positive_small = "1.200"
    positive_large = ("1234567890" * 3) + "12345.678"
    negative_small = "-1.200"
    negative_large = "-" + ("1234567890" * 3) + "12345.678"
    array = Arrow::Decimal128Array.new({precision: 38, scale: 3},
                                       [
                                         positive_large,
                                         positive_small,
                                         nil,
                                         negative_small,
                                         negative_large,
                                       ])
    type, values = roundtrip(array)
    assert_equal([
                   "Decimal128(38, 3)",
                   [
                     BigDecimal(positive_large),
                     BigDecimal(positive_small),
                     nil,
                     BigDecimal(negative_small),
                     BigDecimal(negative_large),
                   ],
                 ],
                 [type.to_s, values])
  end

  def test_decimal256
    positive_small = "1.200"
    positive_large = ("1234567890" * 7) + "123.456"
    negative_small = "-1.200"
    negative_large = "-" + ("1234567890" * 7) + "123.456"
    array = Arrow::Decimal256Array.new({precision: 76, scale: 3},
                                       [
                                         positive_large,
                                         positive_small,
                                         nil,
                                         negative_small,
                                         negative_large,
                                       ])
    type, values = roundtrip(array)
    assert_equal([
                   "Decimal256(76, 3)",
                   [
                     BigDecimal(positive_large),
                     BigDecimal(positive_small),
                     nil,
                     BigDecimal(negative_small),
                     BigDecimal(negative_large),
                   ],
                 ],
                 [type.to_s, values])
  end

  def test_list
    data_type = Arrow::ListDataType.new(name: "count", type: :int8)
    array = Arrow::ListArray.new(data_type, [[-128, 127], nil, [-1, 0, 1]])
    type, values = roundtrip(array)
    assert_equal(["List<count: Int8>", [[-128, 127], nil, [-1, 0, 1]]],
                 [type.to_s, values])
  end

  def test_large_list
    data_type = Arrow::LargeListDataType.new(name: "count",
                                             type: :int8)
    array = Arrow::LargeListArray.new(data_type,
                                      [[-128, 127], nil, [-1, 0, 1]])
    type, values = roundtrip(array)
    assert_equal([
                   "LargeList<count: Int8>",
                   [
                     [-128, 127],
                     nil,
                     [-1, 0, 1],
                   ],
                 ],
                 [type.to_s, values])
  end

  def test_struct
    data_type = Arrow::StructDataType.new(count: :int8,
                                          visible: :boolean)
    array = Arrow::StructArray.new(data_type,
                                   [[-128, nil], nil, [nil, true]])
    type, values = roundtrip(array)
    assert_equal([
                   "Struct<count: Int8, visible: Boolean>",
                   [
                     [-128, nil],
                     nil,
                     [nil, true],
                   ],
                 ],
                 [type.to_s, values])
  end

  def test_dense_union
    fields = [
      Arrow::Field.new("number", :int8),
      Arrow::Field.new("text", :string),
    ]
    type_ids = [11, 13]
    data_type = Arrow::DenseUnionDataType.new(fields, type_ids)
    types = Arrow::Int8Array.new([11, 13, 11, 13, 13])
    value_offsets = Arrow::Int32Array.new([0, 0, 1, 1, 2])
    children = [
      Arrow::Int8Array.new([1, nil]),
      Arrow::StringArray.new(["a", "b", "c"])
    ]
    array = Arrow::DenseUnionArray.new(data_type,
                                       types,
                                       value_offsets,
                                       children)
    type, values = roundtrip(array)
    assert_equal([
                   "DenseUnion<number: Int8=11, text: UTF8=13>",
                   [1, "a", nil, "b", "c"],
                 ],
                 [type.to_s, values])
  end

  def test_sparse_union
    fields = [
      Arrow::Field.new("number", :int8),
      Arrow::Field.new("text", :string),
    ]
    type_ids = [11, 13]
    data_type = Arrow::SparseUnionDataType.new(fields, type_ids)
    types = Arrow::Int8Array.new([11, 13, 11, 13, 11])
    children = [
      Arrow::Int8Array.new([1, nil, nil, nil, 5]),
      Arrow::StringArray.new([nil, "b", nil, "d", nil])
    ]
    array = Arrow::SparseUnionArray.new(data_type, types, children)
    type, values = roundtrip(array)
    assert_equal([
                   "SparseUnion<number: Int8=11, text: UTF8=13>",
                   [1, "b", nil, "d", 5],
                 ],
                 [type.to_s, values])
  end

  def test_map
    data_type = Arrow::MapDataType.new(:string, :int8)
    array = Arrow::MapArray.new(data_type,
                                [
                                  {"a" => -128, "b" => 127},
                                  nil,
                                  {"c" => nil},
                                ])
    type, values = roundtrip(array)
    assert_equal([
                   "Map<UTF8, Int8>",
                   [
                     {"a" => -128, "b" => 127},
                     nil,
                     {"c" => nil},
                   ],
                 ],
                 [type.to_s, values])
  end

  def test_dictionary
    values = ["a", "b", "c", nil, "a"]
    string_array = Arrow::StringArray.new(values)
    array = string_array.dictionary_encode
    type, values = roundtrip(array)
    assert_equal([
                   "Dictionary<index=Int32, value=UTF8, ordered=false>",
                   ["a", "b", "c", nil, "a"],
                 ],
                 [type.to_s, values])
  end
end

class TestFileReader < Test::Unit::TestCase
  include ReaderTests

  def file_extension
    "arrow"
  end

  def reader_class
    ArrowFormat::FileReader
  end
end

class TestStreamingReader < Test::Unit::TestCase
  include ReaderTests

  def file_extension
    "arrows"
  end

  def reader_class
    ArrowFormat::StreamingReader
  end
end
