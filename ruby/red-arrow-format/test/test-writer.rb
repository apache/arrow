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

module WriterTests
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
    when Arrow::BinaryDataType
      ArrowFormat::BinaryType.singleton
    when Arrow::LargeBinaryDataType
      ArrowFormat::LargeBinaryType.singleton
    when Arrow::StringDataType
      ArrowFormat::UTF8Type.singleton
    when Arrow::LargeStringDataType
      ArrowFormat::LargeUTF8Type.singleton
    else
      raise "Unsupported type: #{red_arrow_type.inspect}"
    end
  end

  def convert_buffer(buffer)
    return nil if buffer.nil?
    IO::Buffer.for(buffer.data.to_s)
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
    else
      raise "Unsupported array #{red_arrow_array.inspect}"
    end
  end

  class << self
    def included(base)
      base.class_eval do
        sub_test_case("Null") do
          def build_array
            Arrow::NullArray.new(3)
          end

          def test_write
            assert_equal([nil, nil, nil],
                         @values)
          end
        end

        sub_test_case("Boolean") do
          def build_array
            Arrow::BooleanArray.new([true, nil, false])
          end

          def test_write
            assert_equal([true, nil, false],
                         @values)
          end
        end

        sub_test_case("Int8") do
          def build_array
            Arrow::Int8Array.new([-128, nil, 127])
          end

          def test_write
            assert_equal([-128, nil, 127],
                         @values)
          end
        end

        sub_test_case("UInt8") do
          def build_array
            Arrow::UInt8Array.new([0, nil, 255])
          end

          def test_write
            assert_equal([0, nil, 255],
                         @values)
          end
        end

        sub_test_case("Int16") do
          def build_array
            Arrow::Int16Array.new([-32768, nil, 32767])
          end

          def test_write
            assert_equal([-32768, nil, 32767],
                         @values)
          end
        end

        sub_test_case("UInt16") do
          def build_array
            Arrow::UInt16Array.new([0, nil, 65535])
          end

          def test_write
            assert_equal([0, nil, 65535],
                         @values)
          end
        end

        sub_test_case("Int32") do
          def build_array
            Arrow::Int32Array.new([-2147483648, nil, 2147483647])
          end

          def test_write
            assert_equal([-2147483648, nil, 2147483647],
                         @values)
          end
        end

        sub_test_case("UInt32") do
          def build_array
            Arrow::UInt32Array.new([0, nil, 4294967295])
          end

          def test_write
            assert_equal([0, nil, 4294967295],
                         @values)
          end
        end

        sub_test_case("Int64") do
          def build_array
            Arrow::Int64Array.new([
                                    -9223372036854775808,
                                    nil,
                                    9223372036854775807
                                  ])
          end

          def test_write
            assert_equal([
                           -9223372036854775808,
                           nil,
                           9223372036854775807
                         ],
                         @values)
          end
        end

        sub_test_case("UInt64") do
          def build_array
            Arrow::UInt64Array.new([0, nil, 18446744073709551615])
          end

          def test_write
            assert_equal([0, nil, 18446744073709551615],
                         @values)
          end
        end

        sub_test_case("Float32") do
          def build_array
            Arrow::FloatArray.new([-0.5, nil, 0.5])
          end

          def test_write
            assert_equal([-0.5, nil, 0.5],
                         @values)
          end
        end

        sub_test_case("Float64") do
          def build_array
            Arrow::DoubleArray.new([-0.5, nil, 0.5])
          end

          def test_write
            assert_equal([-0.5, nil, 0.5],
                         @values)
          end
        end

        sub_test_case("Date32") do
          def setup(&block)
            @date_2017_08_28 = 17406
            @date_2025_12_09 = 20431
            super(&block)
          end

          def build_array
            Arrow::Date32Array.new([@date_2017_08_28, nil, @date_2025_12_09])
          end

          def test_write
            assert_equal([Date.new(2017, 8, 28), nil, Date.new(2025, 12, 9)],
                         @values)
          end
        end

        sub_test_case("Date64") do
          def setup(&block)
            @date_2017_08_28_00_00_00 = 1503878400000
            @date_2025_12_10_00_00_00 = 1765324800000
            super(&block)
          end

          def build_array
            Arrow::Date64Array.new([
                                     @date_2017_08_28_00_00_00,
                                     nil,
                                     @date_2025_12_10_00_00_00,
                                   ])
          end

          def test_write
            assert_equal([
                           DateTime.new(2017, 8, 28, 0, 0, 0),
                           nil,
                           DateTime.new(2025, 12, 10, 0, 0, 0),
                         ],
                         @values)
          end
        end

        sub_test_case("Binary") do
          def build_array
            Arrow::BinaryArray.new(["Hello".b, nil, "World".b])
          end

          def test_write
            assert_equal(["Hello".b, nil, "World".b],
                         @values)
          end
        end

        sub_test_case("LargeBinary") do
          def build_array
            Arrow::LargeBinaryArray.new(["Hello".b, nil, "World".b])
          end

          def test_write
            assert_equal(["Hello".b, nil, "World".b],
                         @values)
          end
        end

        sub_test_case("String") do
          def build_array
            Arrow::StringArray.new(["Hello", nil, "World"])
          end

          def test_write
            assert_equal(["Hello", nil, "World"],
                         @values)
          end
        end

        sub_test_case("LargeString") do
          def build_array
            Arrow::LargeStringArray.new(["Hello", nil, "World"])
          end

          def test_write
            assert_equal(["Hello", nil, "World"],
                         @values)
          end
        end
      end
    end
  end
end

class TestFileWriter < Test::Unit::TestCase
  include WriterTests

  def setup
    Dir.mktmpdir do |tmp_dir|
      path = File.join(tmp_dir, "data.arrow")
      File.open(path, "wb") do |output|
        writer = ArrowFormat::FileWriter.new(output)
        red_arrow_array = build_array
        array = convert_array(red_arrow_array)
        fields = [
          ArrowFormat::Field.new("value",
                                 array.type,
                                 true,
                                 nil),
        ]
        schema = ArrowFormat::Schema.new(fields)
        record_batch = ArrowFormat::RecordBatch.new(schema, array.size, [array])
        writer.start(schema)
        writer.write_record_batch(record_batch)
        writer.finish
      end
      data = File.open(path, "rb", &:read).freeze
      table = Arrow::Table.load(Arrow::Buffer.new(data), format: :arrow)
      @values = table.value.values
    end
  end
end

class TestStreamingWriter < Test::Unit::TestCase
  include WriterTests

  def setup
    Dir.mktmpdir do |tmp_dir|
      path = File.join(tmp_dir, "data.arrows")
      File.open(path, "wb") do |output|
        writer = ArrowFormat::StreamingWriter.new(output)
        red_arrow_array = build_array
        array = convert_array(red_arrow_array)
        fields = [
          ArrowFormat::Field.new("value",
                                 array.type,
                                 true,
                                 nil),
        ]
        schema = ArrowFormat::Schema.new(fields)
        record_batch = ArrowFormat::RecordBatch.new(schema, array.size, [array])
        writer.start(schema)
        writer.write_record_batch(record_batch)
        writer.finish
      end
      data = File.open(path, "rb", &:read).freeze
      table = Arrow::Table.load(Arrow::Buffer.new(data), format: :arrows)
      @values = table.value.values
    end
  end
end
