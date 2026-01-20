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
    when ArrowFormat::BooleanType
      type.build_array(red_arrow_array.size,
                       convert_buffer(red_arrow_array.null_bitmap),
                       convert_buffer(red_arrow_array.data_buffer))
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
