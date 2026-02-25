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

module WriterHelper
  def convert_time_unit(red_arrow_time_unit)
    if red_arrow_time_unit.nick == "second"
      red_arrow_time_unit.nick.to_sym
    else
      :"#{red_arrow_time_unit.nick}second"
    end
  end

  def convert_type(red_arrow_type)
    case red_arrow_type
    when Arrow::NullDataType
      ArrowFormat::NullType.singleton
    when Arrow::BooleanDataType
      ArrowFormat::BooleanType.singleton
    when Arrow::Int8DataType
      ArrowFormat::Int8Type.singleton
    when Arrow::UInt8DataType
      ArrowFormat::UInt8Type.singleton
    when Arrow::Int16DataType
      ArrowFormat::Int16Type.singleton
    when Arrow::UInt16DataType
      ArrowFormat::UInt16Type.singleton
    when Arrow::Int32DataType
      ArrowFormat::Int32Type.singleton
    when Arrow::UInt32DataType
      ArrowFormat::UInt32Type.singleton
    when Arrow::Int64DataType
      ArrowFormat::Int64Type.singleton
    when Arrow::UInt64DataType
      ArrowFormat::UInt64Type.singleton
    when Arrow::FloatDataType
      ArrowFormat::Float32Type.singleton
    when Arrow::DoubleDataType
      ArrowFormat::Float64Type.singleton
    when Arrow::Date32DataType
      ArrowFormat::Date32Type.singleton
    when Arrow::Date64DataType
      ArrowFormat::Date64Type.singleton
    when Arrow::Time32DataType
      ArrowFormat::Time32Type.new(convert_time_unit(red_arrow_type.unit))
    when Arrow::Time64DataType
      ArrowFormat::Time64Type.new(convert_time_unit(red_arrow_type.unit))
    when Arrow::TimestampDataType
      ArrowFormat::TimestampType.new(convert_time_unit(red_arrow_type.unit),
                                     red_arrow_type.time_zone&.identifier)
    when Arrow::MonthIntervalDataType
      ArrowFormat::YearMonthIntervalType.singleton
    when Arrow::DayTimeIntervalDataType
      ArrowFormat::DayTimeIntervalType.singleton
    when Arrow::MonthDayNanoIntervalDataType
      ArrowFormat::MonthDayNanoIntervalType.singleton
    when Arrow::DurationDataType
      ArrowFormat::DurationType.new(convert_time_unit(red_arrow_type.unit))
    when Arrow::BinaryDataType
      ArrowFormat::BinaryType.singleton
    when Arrow::LargeBinaryDataType
      ArrowFormat::LargeBinaryType.singleton
    when Arrow::StringDataType
      ArrowFormat::UTF8Type.singleton
    when Arrow::LargeStringDataType
      ArrowFormat::LargeUTF8Type.singleton
    when Arrow::Decimal128DataType
      ArrowFormat::Decimal128Type.new(red_arrow_type.precision,
                                      red_arrow_type.scale)
    when Arrow::Decimal256DataType
      ArrowFormat::Decimal256Type.new(red_arrow_type.precision,
                                      red_arrow_type.scale)
    when Arrow::FixedSizeBinaryDataType
      ArrowFormat::FixedSizeBinaryType.new(red_arrow_type.byte_width)
    when Arrow::MapDataType
      ArrowFormat::MapType.new(convert_field(red_arrow_type.field))
    when Arrow::ListDataType
      ArrowFormat::ListType.new(convert_field(red_arrow_type.field))
    when Arrow::LargeListDataType
      ArrowFormat::LargeListType.new(convert_field(red_arrow_type.field))
    when Arrow::StructDataType
      fields = red_arrow_type.fields.collect do |field|
        convert_field(field)
      end
      ArrowFormat::StructType.new(fields)
    when Arrow::DenseUnionDataType
      fields = red_arrow_type.fields.collect do |field|
        convert_field(field)
      end
      ArrowFormat::DenseUnionType.new(fields, red_arrow_type.type_codes)
    when Arrow::SparseUnionDataType
      fields = red_arrow_type.fields.collect do |field|
        convert_field(field)
      end
      ArrowFormat::SparseUnionType.new(fields, red_arrow_type.type_codes)
    when Arrow::DictionaryDataType
      index_type = convert_type(red_arrow_type.index_data_type)
      type = convert_type(red_arrow_type.value_data_type)
      ArrowFormat::DictionaryType.new(index_type,
                                      type,
                                      red_arrow_type.ordered?)
    else
      raise "Unsupported type: #{red_arrow_type.inspect}"
    end
  end

  def convert_field(red_arrow_field)
    type = convert_type(red_arrow_field.data_type)
    if type.is_a?(ArrowFormat::DictionaryType)
      @dictionary_id ||= 0
      dictionary_id = @dictionary_id
      @dictionary_id += 1
    else
      dictionary_id = nil
    end
    ArrowFormat::Field.new(red_arrow_field.name,
                           type,
                           nullable: red_arrow_field.nullable?,
                           dictionary_id: dictionary_id,
                           metadata: red_arrow_field.metadata)
  end

  def convert_buffer(buffer)
    return nil if buffer.nil?
    IO::Buffer.for(buffer.data.to_s.dup)
  end

  def convert_array(red_arrow_array)
    type = convert_type(red_arrow_array.value_data_type)
    case type
    when ArrowFormat::NullType
      type.build_array(red_arrow_array.size)
    when ArrowFormat::PrimitiveType
      type.build_array(red_arrow_array.size,
                       convert_buffer(red_arrow_array.null_bitmap),
                       convert_buffer(red_arrow_array.data_buffer))
    when ArrowFormat::VariableSizeBinaryType
      type.build_array(red_arrow_array.size,
                       convert_buffer(red_arrow_array.null_bitmap),
                       convert_buffer(red_arrow_array.offsets_buffer),
                       convert_buffer(red_arrow_array.data_buffer))
    when ArrowFormat::FixedSizeBinaryType
      type.build_array(red_arrow_array.size,
                       convert_buffer(red_arrow_array.null_bitmap),
                       convert_buffer(red_arrow_array.data_buffer))
    when ArrowFormat::VariableSizeListType
      type.build_array(red_arrow_array.size,
                       convert_buffer(red_arrow_array.null_bitmap),
                       convert_buffer(red_arrow_array.value_offsets_buffer),
                       convert_array(red_arrow_array.values_raw))
    when ArrowFormat::StructType
      children = red_arrow_array.fields.collect do |red_arrow_field|
        convert_array(red_arrow_field)
      end
      type.build_array(red_arrow_array.size,
                       convert_buffer(red_arrow_array.null_bitmap),
                       children)
    when ArrowFormat::DenseUnionType
      types_buffer = convert_buffer(red_arrow_array.type_ids.data_buffer)
      offsets_buffer = convert_buffer(red_arrow_array.value_offsets.data_buffer)
      children = red_arrow_array.fields.collect do |red_arrow_field|
        convert_array(red_arrow_field)
      end
      type.build_array(red_arrow_array.size,
                       types_buffer,
                       offsets_buffer,
                       children)
    when ArrowFormat::SparseUnionType
      types_buffer = convert_buffer(red_arrow_array.type_ids.data_buffer)
      children = red_arrow_array.fields.collect do |red_arrow_field|
        convert_array(red_arrow_field)
      end
      type.build_array(red_arrow_array.size,
                       types_buffer,
                       children)
    when ArrowFormat::DictionaryType
      validity_buffer = convert_buffer(red_arrow_array.null_bitmap)
      indices_buffer = convert_buffer(red_arrow_array.indices.data_buffer)
      dictionary = convert_array(red_arrow_array.dictionary)
      type.build_array(red_arrow_array.size,
                       validity_buffer,
                       indices_buffer,
                       [dictionary])
    else
      raise "Unsupported array #{red_arrow_array.inspect}"
    end
  end

  def write(writer, *inputs)
    inputs.each_with_index do |input, i|
      case input
      when ArrowFormat::RecordBatch
        record_batch = input
      else
        red_arrow_array = input
        array = convert_array(red_arrow_array)
        red_arrow_field = Arrow::Field.new("value",
                                           red_arrow_array.value_data_type,
                                           true)
        fields = [convert_field(red_arrow_field)]
        schema = ArrowFormat::Schema.new(fields)
        record_batch = ArrowFormat::RecordBatch.new(schema,
                                                    array.size,
                                                    [array])
      end
      writer.start(record_batch.schema) if i.zero?
      writer.write_record_batch(record_batch)
    end
    writer.finish
  end

  def roundtrip(*inputs)
    Dir.mktmpdir do |tmp_dir|
      path = File.join(tmp_dir, "data.#{file_extension}")
      File.open(path, "wb") do |output|
        writer = writer_class.new(output)
        write(writer, *inputs)
      end
      # pp(read(path)) # debug
      data = File.open(path, "rb", &:read).freeze
      case file_extension
      when "arrow"
        format = :arrow_file
      else
        format = :arrow_streaming
      end
      table = Arrow::Table.load(Arrow::Buffer.new(data), format: format)
      if inputs[0].is_a?(Arrow::Array)
        [table.value.data_type, table.value.values]
      else
        table
      end
    end
  end
end

module WriterTests
  def test_custom_metadata_field
    field = ArrowFormat::Field.new("value",
                                   ArrowFormat::BooleanType.new,
                                   metadata: {
                                     "key1" => "value1",
                                     "key2" => "value2",
                                   })
    schema = ArrowFormat::Schema.new([field])
    column = convert_array(Arrow::BooleanArray.new([true, nil, false]))
    record_batch = ArrowFormat::RecordBatch.new(schema, 3, [column])
    table = roundtrip(record_batch)
    assert_equal({
                   "key1" => "value1",
                   "key2" => "value2",
                 },
                 table.schema.fields[0].metadata)
  end

  def test_custom_metadata_schema
    field = ArrowFormat::Field.new("value", ArrowFormat::BooleanType.new)
    schema = ArrowFormat::Schema.new([field],
                                     metadata: {
                                       "key1" => "value1",
                                       "key2" => "value2",
                                     })
    column = convert_array(Arrow::BooleanArray.new([true, nil, false]))
    record_batch = ArrowFormat::RecordBatch.new(schema, 3, [column])
    table = roundtrip(record_batch)
    assert_equal({
                   "key1" => "value1",
                   "key2" => "value2",
                 },
                 table.schema.metadata)
  end

  def test_null
    array = Arrow::NullArray.new(3)
    type, values = roundtrip(array)
    assert_equal(["null", [nil, nil, nil]],
                 [type.to_s, values])
  end

  def test_boolean
    array = Arrow::BooleanArray.new([true, nil, false])
    type, values = roundtrip(array)
    assert_equal(["bool", [true, nil, false]],
                 [type.to_s, values])
  end

  def test_int8
    array = Arrow::Int8Array.new([-128, nil, 127])
    type, values = roundtrip(array)
    assert_equal(["int8", [-128, nil, 127]],
                 [type.to_s, values])
  end

  def test_uint8
    array = Arrow::UInt8Array.new([0, nil, 255])
    type, values = roundtrip(array)
    assert_equal(["uint8", [0, nil, 255]],
                 [type.to_s, values])
  end

  def test_int16
    array = Arrow::Int16Array.new([-32768, nil, 32767])
    type, values = roundtrip(array)
    assert_equal(["int16", [-32768, nil, 32767]],
                 [type.to_s, values])
  end

  def test_uint16
    array = Arrow::UInt16Array.new([0, nil, 65535])
    type, values = roundtrip(array)
    assert_equal(["uint16", [0, nil, 65535]],
                 [type.to_s, values])
  end

  def test_int32
    array = Arrow::Int32Array.new([-2147483648, nil, 2147483647])
    type, values = roundtrip(array)
    assert_equal(["int32", [-2147483648, nil, 2147483647]],
                 [type.to_s, values])
  end

  def test_uint32
    array = Arrow::UInt32Array.new([0, nil, 4294967295])
    type, values = roundtrip(array)
    assert_equal(["uint32", [0, nil, 4294967295]],
                 [type.to_s, values])
  end

  def test_int64
    array = Arrow::Int64Array.new([
                                    -9223372036854775808,
                                    nil,
                                    9223372036854775807
                                  ])
    type, values = roundtrip(array)
    assert_equal([
                   "int64",
                   [
                     -9223372036854775808,
                     nil,
                     9223372036854775807
                   ],
                 ],
                 [type.to_s, values])
  end

  def test_uint64
    array = Arrow::UInt64Array.new([0, nil, 18446744073709551615])
    type, values = roundtrip(array)
    assert_equal(["uint64", [0, nil, 18446744073709551615]],
                 [type.to_s, values])
  end

  def test_float32
    array = Arrow::FloatArray.new([-0.5, nil, 0.5])
    type, values = roundtrip(array)
    assert_equal(["float", [-0.5, nil, 0.5]],
                 [type.to_s, values])
  end

  def test_float64
    array = Arrow::DoubleArray.new([-0.5, nil, 0.5])
    type, values = roundtrip(array)
    assert_equal(["double", [-0.5, nil, 0.5]],
                 [type.to_s, values])
  end

  def test_date32
    date_2017_08_28 = 17406
    date_2025_12_09 = 20431
    array = Arrow::Date32Array.new([
                                     date_2017_08_28,
                                     nil,
                                     date_2025_12_09,
                                   ])
    type, values = roundtrip(array)
    assert_equal([
                   "date32[day]",
                   [Date.new(2017, 8, 28), nil, Date.new(2025, 12, 9)],
                 ],
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
                   "date64[ms]",
                   [
                     DateTime.new(2017, 8, 28, 0, 0, 0),
                     nil,
                     DateTime.new(2025, 12, 10, 0, 0, 0),
                   ],
                 ],
                 [type.to_s, values])
  end

  def test_time32_second
    time_00_00_10 = 10
    time_00_01_10 = 60 + 10
    array = Arrow::Time32Array.new(:second,
                                   [time_00_00_10, nil, time_00_01_10])
    type, values = roundtrip(array)
    assert_equal([
                   "time32[s]",
                   [
                     Arrow::Time.new(:second, time_00_00_10),
                     nil,
                     Arrow::Time.new(:second, time_00_01_10),
                   ],
                 ],
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
                   "time32[ms]",
                   [
                     Arrow::Time.new(:milli, time_00_00_10_000),
                     nil,
                     Arrow::Time.new(:milli, time_00_01_10_000),
                   ],
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
                   "time64[us]",
                   [
                     Arrow::Time.new(:micro, time_00_00_10_000_000),
                     nil,
                     Arrow::Time.new(:micro, time_00_01_10_000_000),
                   ],
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
                   "time64[ns]",
                   [
                     Arrow::Time.new(:nano, time_00_00_10_000_000_000),
                     nil,
                     Arrow::Time.new(:nano, time_00_01_10_000_000_000),
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
                   "timestamp[s]",
                   [
                     Time.at(timestamp_2019_11_17_15_09_11),
                     nil,
                     Time.at(timestamp_2025_12_16_05_33_58),
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
                   "timestamp[ms]",
                   [
                     Time.at(timestamp_2019_11_17_15_09_11 / 1_000),
                     nil,
                     Time.at(timestamp_2025_12_16_05_33_58 / 1_000),
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
                   "timestamp[us]",
                   [
                     Time.at(timestamp_2019_11_17_15_09_11 / 1_000_000),
                     nil,
                     Time.at(timestamp_2025_12_16_05_33_58 / 1_000_000),
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
                   "timestamp[ns]",
                   [
                     Time.at(timestamp_2019_11_17_15_09_11 / 1_000_000_000),
                     nil,
                     Time.at(timestamp_2025_12_16_05_33_58 / 1_000_000_000),
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
                   "timestamp[s, tz=#{time_zone}]",
                   [
                     Time.at(timestamp_2019_11_17_15_09_11),
                     nil,
                     Time.at(timestamp_2025_12_16_05_33_58),
                   ],
                 ],
                 [type.to_s, values])
  end

  def test_year_month_interval
    array = Arrow::MonthIntervalArray.new([0, nil, 100])
    type, values = roundtrip(array)
    assert_equal(["month_interval", [0, nil, 100]],
                 [type.to_s, values])
  end

  def test_day_time_interval
    array =
      Arrow::DayTimeIntervalArray.new([
                                        {day: 1, millisecond: 100},
                                        nil,
                                        {day: 3, millisecond: 300},
                                      ])
    type, values = roundtrip(array)
    assert_equal([
                   "day_time_interval",
                   [
                     {day: 1, millisecond: 100},
                     nil,
                     {day: 3, millisecond: 300},
                   ],
                 ],
                 [type.to_s, values])
  end

  def test_month_day_nano_interval
    array =
      Arrow::MonthDayNanoIntervalArray.new([
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
                   "month_day_nano_interval",
                   [
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
                   ],
                 ],
                 [type.to_s, values])
  end

  def test_duration_second
    array = Arrow::DurationArray.new(:second, [0, nil, 100])
    type, values = roundtrip(array)
    assert_equal(["duration[s]", [0, nil, 100]],
                 [type.to_s, values])
  end

  def test_duration_millisecond
    array = Arrow::DurationArray.new(:milli, [0, nil, 100])
    type, values = roundtrip(array)
    assert_equal(["duration[ms]", [0, nil, 100]],
                 [type.to_s, values])
  end

  def test_duration_microsecond
    array = Arrow::DurationArray.new(:micro, [0, nil, 100])
    type, values = roundtrip(array)
    assert_equal(["duration[us]", [0, nil, 100]],
                 [type.to_s, values])
  end

  def test_duration_nanosecond
    array = Arrow::DurationArray.new(:nano, [0, nil, 100])
    type, values = roundtrip(array)
    assert_equal(["duration[ns]", [0, nil, 100]],
                 [type.to_s, values])
  end

  def test_binary
    array = Arrow::BinaryArray.new(["Hello".b, nil, "World".b])
    type, values = roundtrip(array)
    assert_equal(["binary", ["Hello".b, nil, "World".b]],
                 [type.to_s, values])
  end

  def test_large_binary
    array = Arrow::LargeBinaryArray.new(["Hello".b, nil, "World".b])
    type, values = roundtrip(array)
    assert_equal(["large_binary", ["Hello".b, nil, "World".b]],
                 [type.to_s, values])
  end

  def test_utf8
    array = Arrow::StringArray.new(["Hello", nil, "World"])
    type, values = roundtrip(array)
    assert_equal(["string", ["Hello", nil, "World"]],
                 [type.to_s, values])
  end

  def test_large_utf8
    array = Arrow::LargeStringArray.new(["Hello", nil, "World"])
    type, values = roundtrip(array)
    assert_equal(["large_string", ["Hello", nil, "World"]],
                 [type.to_s, values])
  end

  def test_fixed_size_binary
    data_type = Arrow::FixedSizeBinaryDataType.new(4)
    array = Arrow::FixedSizeBinaryArray.new(data_type,
                                            ["0124".b, nil, "abcd".b])
    type, values = roundtrip(array)
    assert_equal(["fixed_size_binary[4]", ["0124".b, nil, "abcd".b]],
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
                   "decimal128(38, 3)",
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
                   "decimal256(76, 3)",
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
    array = Arrow::ListArray.new(data_type,
                                 [[-128, 127], nil, [-1, 0, 1]])
    type, values = roundtrip(array)
    assert_equal([
                   "list<count: int8>",
                   [[-128, 127], nil, [-1, 0, 1]],
                 ],
                 [type.to_s, values])
  end

  def test_large_list
    data_type = Arrow::LargeListDataType.new(name: "count",
                                             type: :int8)
    array = Arrow::LargeListArray.new(data_type,
                                      [[-128, 127], nil, [-1, 0, 1]])
    type, values = roundtrip(array)
    assert_equal([
                   "large_list<count: int8>",
                   [[-128, 127], nil, [-1, 0, 1]],
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
                   "map<string, int8>",
                   [
                     {"a" => -128, "b" => 127},
                     nil,
                     {"c" => nil},
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
                   "struct<count: int8, visible: bool>",
                   [
                     {"count" => -128, "visible" => nil},
                     nil,
                     {"count" => nil, "visible" => true},
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
                   "dense_union<number: int8=11, text: string=13>",
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
                   "sparse_union<number: int8=11, text: string=13>",
                   [1, "b", nil, "d", 5],
                 ],
                 [type.to_s, values])
  end

  def test_dictionary
    values = ["a", "b", "c", nil, "a"]
    string_array = Arrow::StringArray.new(values)
    array = string_array.dictionary_encode
    type, values = roundtrip(array)
    assert_equal([
                   "dictionary<values=string, " +
                   "indices=int32, " +
                   "ordered=0>",
                   ["a", "b", "c", nil, "a"],
                 ],
                 [type.to_s, values])
  end
end

module WriterDictionaryDeltaTests
  def build_schema(value_type)
    index_type = ArrowFormat::Int32Type.singleton
    ordered = false
    type = ArrowFormat::DictionaryType.new(index_type,
                                           value_type,
                                           ordered)
    field = ArrowFormat::Field.new("value",
                                   type,
                                   dictionary_id: 1)
    ArrowFormat::Schema.new([field])
  end

  def build_dictionary_array(type, indices, dictionaries)
    indices_buffer = IO::Buffer.for(indices.pack("l<*"))
    ArrowFormat::DictionaryArray.new(type,
                                     indices.size,
                                     nil,
                                     indices_buffer,
                                     dictionaries)
  end

  def build_record_batches(red_arrow_value_type, values1, values2)
    value_type = convert_type(red_arrow_value_type)
    schema = build_schema(value_type)
    type = schema.fields[0].type

    # The first record batch with new dictionary.
    raw_dictionary = values1.uniq
    red_arrow_dictionary =
      red_arrow_value_type.build_array(raw_dictionary)
    dictionary = convert_array(red_arrow_dictionary)
    indices1 = values1.collect do |value|
      raw_dictionary.index(value)
    end
    array1 = build_dictionary_array(type, indices1, [dictionary])
    record_batch =
      ArrowFormat::RecordBatch.new(schema, array1.size, [array1])

    if chunked_dictionaries?
      # The second record batch with the first dictionary and
      # a delta dictionary.
      raw_dictionary_delta = (values2.uniq - raw_dictionary)
      raw_dictionary_more = raw_dictionary + raw_dictionary_delta
      red_arrow_dictionary_delta =
        red_arrow_value_type.build_array(raw_dictionary_delta)
      dictionary_delta = convert_array(red_arrow_dictionary_delta)
      indices2 = values2.collect do |value|
        raw_dictionary_more.index(value)
      end
      array2 = build_dictionary_array(type,
                                      indices2,
                                      [dictionary, dictionary_delta])
    else
      # The second record batch with the combined dictionary.
      raw_dictionary_more = raw_dictionary | values2.uniq
      red_arrow_dictionary_more =
        red_arrow_value_type.build_array(raw_dictionary_more)
      dictionary_more = convert_array(red_arrow_dictionary_more)
      indices2 = values2.collect do |value|
        raw_dictionary_more.index(value)
      end
      array2 = build_dictionary_array(type,
                                      indices2,
                                      [dictionary_more])
    end
    record_batch_delta =
      ArrowFormat::RecordBatch.new(schema, array2.size, [array2])

    [record_batch, record_batch_delta]
  end

  def roundtrip(value_type, values1, values2)
    record_batches = build_record_batches(value_type, values1, values2)
    GC.start
    table = super(*record_batches)
    [table.value.data_type, table.value.values]
  end

  def test_boolean
    value_type = Arrow::BooleanDataType.new
    values1 = [true, true]
    values2 = [false, true, false]
    type, values = roundtrip(value_type, values1, values2)
    assert_equal([
                   "dictionary<values=bool, " +
                   "indices=int32, " +
                   "ordered=0>",
                   values1 + values2,
                 ],
                 [type.to_s, values])
  end

  def test_int8
    value_type = Arrow::Int8DataType.new
    values1 = [-128, 0, -128]
    values2 = [127, -128, 0, 127]
    type, values = roundtrip(value_type, values1, values2)
    assert_equal([
                   "dictionary<values=int8, " +
                   "indices=int32, " +
                   "ordered=0>",
                   values1 + values2,
                 ],
                 [type.to_s, values])
  end

  def test_uint8
    value_type = Arrow::UInt8DataType.new
    values1 = [1, 0, 1]
    values2 = [255, 0, 1, 255]
    type, values = roundtrip(value_type, values1, values2)
    assert_equal([
                   "dictionary<values=uint8, " +
                   "indices=int32, " +
                   "ordered=0>",
                   values1 + values2,
                 ],
                 [type.to_s, values])
  end

  def test_int16
    value_type = Arrow::Int16DataType.new
    values1 = [-32768, 0, -32768]
    values2 = [32767, -32768, 0, 32767]
    type, values = roundtrip(value_type, values1, values2)
    assert_equal([
                   "dictionary<values=int16, " +
                   "indices=int32, " +
                   "ordered=0>",
                   values1 + values2,
                 ],
                 [type.to_s, values])
  end

  def test_uint16
    value_type = Arrow::UInt16DataType.new
    values1 = [1, 0, 1]
    values2 = [65535, 0, 1, 65535]
    type, values = roundtrip(value_type, values1, values2)
    assert_equal([
                   "dictionary<values=uint16, " +
                   "indices=int32, " +
                   "ordered=0>",
                   values1 + values2,
                 ],
                 [type.to_s, values])
  end

  def test_int32
    value_type = Arrow::Int32DataType.new
    values1 = [-2147483648, 0, -2147483648]
    values2 = [2147483647, -2147483648, 0, 2147483647]
    type, values = roundtrip(value_type, values1, values2)
    assert_equal([
                   "dictionary<values=int32, " +
                   "indices=int32, " +
                   "ordered=0>",
                   values1 + values2,
                 ],
                 [type.to_s, values])
  end

  def test_uint32
    value_type = Arrow::UInt32DataType.new
    values1 = [1, 0, 1]
    values2 = [4294967295, 0, 1, 4294967295]
    type, values = roundtrip(value_type, values1, values2)
    assert_equal([
                   "dictionary<values=uint32, " +
                   "indices=int32, " +
                   "ordered=0>",
                   values1 + values2,
                 ],
                 [type.to_s, values])
  end

  def test_int64
    value_type = Arrow::Int64DataType.new
    values1 = [
      -9223372036854775808,
      0,
      -9223372036854775808,
    ]
    values2 = [
      9223372036854775807,
      -9223372036854775808,
      0,
      9223372036854775807,
    ]
    type, values = roundtrip(value_type, values1, values2)
    assert_equal([
                   "dictionary<values=int64, " +
                   "indices=int32, " +
                   "ordered=0>",
                   values1 + values2,
                 ],
                 [type.to_s, values])
  end

  def test_uint64
    value_type = Arrow::UInt64DataType.new
    values1 = [1, 0, 1]
    values2 = [
      18446744073709551615,
      0,
      1,
      18446744073709551615,
    ]
    type, values = roundtrip(value_type, values1, values2)
    assert_equal([
                   "dictionary<values=uint64, " +
                   "indices=int32, " +
                   "ordered=0>",
                   values1 + values2,
                 ],
                 [type.to_s, values])
  end

  def test_float32
    value_type = Arrow::FloatDataType.new
    values1 = [-0.5, 0.0, -0.5]
    values2 = [0.5, -0.5, 0.0, 0.5]
    type, values = roundtrip(value_type, values1, values2)
    assert_equal([
                   "dictionary<values=float, " +
                   "indices=int32, " +
                   "ordered=0>",
                   values1 + values2,
                 ],
                 [type.to_s, values])
  end

  def test_float64
    value_type = Arrow::DoubleDataType.new
    values1 = [-0.5, 0.0, -0.5]
    values2 = [0.5, -0.5, 0.0, 0.5]
    type, values = roundtrip(value_type, values1, values2)
    assert_equal([
                   "dictionary<values=double, " +
                   "indices=int32, " +
                   "ordered=0>",
                   values1 + values2,
                 ],
                 [type.to_s, values])
  end

  def test_date32
    date_2017_08_28 = 17406
    date_2025_12_09 = 20431
    value_type = Arrow::Date32DataType.new
    values1 = [date_2017_08_28, date_2017_08_28]
    values2 = [date_2025_12_09, date_2017_08_28, date_2025_12_09]
    type, values = roundtrip(value_type, values1, values2)
    assert_equal([
                   "dictionary<values=date32[day], " +
                   "indices=int32, " +
                   "ordered=0>",
                   [
                     Date.new(2017, 8, 28),
                     Date.new(2017, 8, 28),
                     Date.new(2025, 12, 9),
                     Date.new(2017, 8, 28),
                     Date.new(2025, 12, 9),
                   ],
                 ],
                 [type.to_s, values])
  end

  def test_date64
    date_2017_08_28_00_00_00 = 1503878400000
    date_2025_12_10_00_00_00 = 1765324800000
    value_type = Arrow::Date64DataType.new
    values1 = [date_2017_08_28_00_00_00, date_2017_08_28_00_00_00]
    values2 = [
      date_2025_12_10_00_00_00,
      date_2017_08_28_00_00_00,
      date_2025_12_10_00_00_00,
    ]
    type, values = roundtrip(value_type, values1, values2)
    assert_equal([
                   "dictionary<values=date64[ms], " +
                   "indices=int32, " +
                   "ordered=0>",
                   [
                     DateTime.new(2017, 8, 28),
                     DateTime.new(2017, 8, 28),
                     DateTime.new(2025, 12, 10),
                     DateTime.new(2017, 8, 28),
                     DateTime.new(2025, 12, 10),
                   ],
                 ],
                 [type.to_s, values])
  end

  def test_time32
    time_00_00_10 = 10
    time_00_01_10 = 60 + 10
    value_type = Arrow::Time32DataType.new(:second)
    values1 = [time_00_00_10, time_00_00_10]
    values2 = [time_00_01_10, time_00_00_10, time_00_01_10]
    type, values = roundtrip(value_type, values1, values2)
    assert_equal([
                   "dictionary<values=time32[s], " +
                   "indices=int32, " +
                   "ordered=0>",
                   [
                     Arrow::Time.new(:second, time_00_00_10),
                     Arrow::Time.new(:second, time_00_00_10),
                     Arrow::Time.new(:second, time_00_01_10),
                     Arrow::Time.new(:second, time_00_00_10),
                     Arrow::Time.new(:second, time_00_01_10),
                   ],
                 ],
                 [type.to_s, values])
  end

  def test_time64
    time_00_00_10_000_000 = 10 * 1_000_000
    time_00_01_10_000_000 = (60 + 10) * 1_000_000
    value_type = Arrow::Time64DataType.new(:micro)
    values1 = [time_00_00_10_000_000, time_00_00_10_000_000]
    values2 = [
      time_00_01_10_000_000,
      time_00_00_10_000_000,
      time_00_01_10_000_000,
    ]
    type, values = roundtrip(value_type, values1, values2)
    assert_equal([
                   "dictionary<values=time64[us], " +
                   "indices=int32, " +
                   "ordered=0>",
                   [
                     Arrow::Time.new(:micro, time_00_00_10_000_000),
                     Arrow::Time.new(:micro, time_00_00_10_000_000),
                     Arrow::Time.new(:micro, time_00_01_10_000_000),
                     Arrow::Time.new(:micro, time_00_00_10_000_000),
                     Arrow::Time.new(:micro, time_00_01_10_000_000),
                   ],
                 ],
                 [type.to_s, values])
  end

  def test_timestamp
    timestamp_2019_11_17_15_09_11 = 1574003351
    timestamp_2025_12_16_05_33_58 = 1765863238
    value_type = Arrow::TimestampDataType.new(:second)
    values1 = [
      timestamp_2019_11_17_15_09_11,
      timestamp_2019_11_17_15_09_11,
    ]
    values2 = [
      timestamp_2025_12_16_05_33_58,
      timestamp_2019_11_17_15_09_11,
      timestamp_2025_12_16_05_33_58,
    ]
    type, values = roundtrip(value_type, values1, values2)
    assert_equal([
                   "dictionary<values=timestamp[s], " +
                   "indices=int32, " +
                   "ordered=0>",
                   [
                     Time.at(timestamp_2019_11_17_15_09_11),
                     Time.at(timestamp_2019_11_17_15_09_11),
                     Time.at(timestamp_2025_12_16_05_33_58),
                     Time.at(timestamp_2019_11_17_15_09_11),
                     Time.at(timestamp_2025_12_16_05_33_58),
                   ],
                 ],
                 [type.to_s, values])
  end

  def test_year_month_interval
    value_type = Arrow::MonthIntervalDataType.new
    values1 = [100, 0, 100]
    values2 = [1000, 100, 0, 1000]
    type, values = roundtrip(value_type, values1, values2)
    assert_equal([
                   "dictionary<values=month_interval, " +
                   "indices=int32, " +
                   "ordered=0>",
                   values1 + values2,
                 ],
                 [type.to_s, values])
  end

  def test_day_time_interval
    value_type = Arrow::DayTimeIntervalDataType.new
    values1 = [
      {day: 1, millisecond: 100},
      {day: 1, millisecond: 100},
    ]
    values2 = [
      {day: 3, millisecond: 300},
      {day: 1, millisecond: 100},
      {day: 3, millisecond: 300},
    ]
    type, values = roundtrip(value_type, values1, values2)
    assert_equal([
                   "dictionary<values=day_time_interval, " +
                   "indices=int32, " +
                   "ordered=0>",
                   values1 + values2,
                 ],
                 [type.to_s, values])
  end

  def test_month_day_nano_interval
    value_type = Arrow::MonthDayNanoIntervalDataType.new
    values1 = [
      {month: 1, day: 1, nanosecond: 100},
      {month: 1, day: 1, nanosecond: 100},
    ]
    values2 = [
      {month: 3, day: 3, nanosecond: 300},
      {month: 1, day: 1, nanosecond: 100},
      {month: 3, day: 3, nanosecond: 300},
    ]
    type, values = roundtrip(value_type, values1, values2)
    assert_equal([
                   "dictionary<values=month_day_nano_interval, " +
                   "indices=int32, " +
                   "ordered=0>",
                   values1 + values2,
                 ],
                 [type.to_s, values])
  end

  def test_duration
    value_type = Arrow::DurationDataType.new(:second)
    values1 = [100, 0, 100]
    values2 = [1000, 100, 0, 1000]
    type, values = roundtrip(value_type, values1, values2)
    assert_equal([
                   "dictionary<values=duration[s], " +
                   "indices=int32, " +
                   "ordered=0>",
                   values1 + values2,
                 ],
                 [type.to_s, values])
  end

  def test_binary
    value_type = Arrow::BinaryDataType.new
    values1 = ["ab".b, "c".b, "ab".b]
    values2 = ["c".b, "de".b, "ab".b, "de".b]
    type, values = roundtrip(value_type, values1, values2)
    assert_equal([
                   "dictionary<values=binary, " +
                   "indices=int32, " +
                   "ordered=0>",
                   values1 + values2,
                 ],
                 [type.to_s, values])
  end

  def test_large_binary
    value_type = Arrow::LargeBinaryDataType.new
    values1 = ["ab".b, "c".b, "ab".b]
    values2 = ["c".b, "de".b, "ab".b, "de".b]
    type, values = roundtrip(value_type, values1, values2)
    assert_equal([
                   "dictionary<values=large_binary, " +
                   "indices=int32, " +
                   "ordered=0>",
                   values1 + values2,
                 ],
                 [type.to_s, values])
  end

  def test_utf8
    value_type = Arrow::StringDataType.new
    values1 = ["ab", "c", "ab"]
    values2 = ["c", "de", "ab", "de"]
    type, values = roundtrip(value_type, values1, values2)
    assert_equal([
                   "dictionary<values=string, " +
                   "indices=int32, " +
                   "ordered=0>",
                   values1 + values2,
                 ],
                 [type.to_s, values])
  end

  def test_large_utf8
    value_type = Arrow::LargeStringDataType.new
    values1 = ["ab", "c", "ab"]
    values2 = ["c", "de", "ab", "de"]
    type, values = roundtrip(value_type, values1, values2)
    assert_equal([
                   "dictionary<values=large_string, " +
                   "indices=int32, " +
                   "ordered=0>",
                   values1 + values2,
                 ],
                 [type.to_s, values])
  end

  def test_fixed_size_binary
    value_type = Arrow::FixedSizeBinaryDataType.new(2)
    values1 = ["ab".b, "cd".b, "ab".b]
    values2 = ["ef".b, "cd".b, "ab".b, "ef".b]
    type, values = roundtrip(value_type, values1, values2)
    assert_equal([
                   "dictionary<values=fixed_size_binary[2], " +
                   "indices=int32, " +
                   "ordered=0>",
                   values1 + values2,
                 ],
                 [type.to_s, values])
  end

  def test_decimal128
    positive_small = "1.200"
    positive_large = ("1234567890" * 3) + "12345.678"
    negative_small = "-1.200"
    negative_large = "-" + ("1234567890" * 3) + "12345.678"
    value_type = Arrow::Decimal128DataType.new(precision: 38,
                                               scale: 3)
    values1 = [positive_small, negative_small, positive_small]
    values2 = [
      positive_large,
      positive_small,
      negative_small,
      positive_large,
      negative_large,
    ]
    type, values = roundtrip(value_type, values1, values2)
    assert_equal([
                   "dictionary<values=decimal128(38, 3), " +
                   "indices=int32, " +
                   "ordered=0>",
                   (values1 + values2).collect {|v| BigDecimal(v)},
                 ],
                 [type.to_s, values])
  end

  def test_decimal256
    positive_small = "1.200"
    positive_large = ("1234567890" * 7) + "123.456"
    negative_small = "-1.200"
    negative_large = "-" + ("1234567890" * 7) + "123.456"
    value_type = Arrow::Decimal256DataType.new(precision: 76,
                                               scale: 3)
    values1 = [positive_small, negative_small, positive_small]
    values2 = [
      positive_large,
      positive_small,
      negative_small,
      positive_large,
      negative_large,
    ]
    type, values = roundtrip(value_type, values1, values2)
    assert_equal([
                   "dictionary<values=decimal256(76, 3), " +
                   "indices=int32, " +
                   "ordered=0>",
                   (values1 + values2).collect {|v| BigDecimal(v)},
                 ],
                 [type.to_s, values])
  end
end

class TestFileWriter < Test::Unit::TestCase
  include WriterHelper

  def file_extension
    "arrow"
  end

  def writer_class
    ArrowFormat::FileWriter
  end

  def read(path)
    File.open(path, "rb") do |input|
      reader = ArrowFormat::FileReader.new(input)
      reader.to_a.collect do |record_batch|
        record_batch.to_h.tap do |hash|
          hash.each do |key, value|
            hash[key] = value.to_a
          end
        end
      end
    end
  end

  sub_test_case("Basic") do
    include WriterTests
  end

  sub_test_case("Dictionary: delta") do
    include WriterDictionaryDeltaTests

    def chunked_dictionaries?
      true
    end
  end

  sub_test_case("Dictionary: delta: slice") do
    include WriterDictionaryDeltaTests

    def chunked_dictionaries?
      false
    end
  end
end

class TestStreamingWriter < Test::Unit::TestCase
  include WriterHelper

  def file_extension
    "arrows"
  end

  def writer_class
    ArrowFormat::StreamingWriter
  end

  def read(path)
    File.open(path, "rb") do |input|
      reader = ArrowFormat::StreamingReader.new(input)
      reader.collect do |record_batch|
        record_batch.to_h.tap do |hash|
          hash.each do |key, value|
            hash[key] = value.to_a
          end
        end
      end
    end
  end

  sub_test_case("Basic") do
    include WriterTests
  end

  sub_test_case("Dictionary: delta") do
    include WriterDictionaryDeltaTests

    def chunked_dictionaries?
      true
    end
  end

  sub_test_case("Dictionary: delta: slice") do
    include WriterDictionaryDeltaTests

    def chunked_dictionaries?
      false
    end
  end
end
