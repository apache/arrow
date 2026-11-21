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

class TestParquetArrowFileReader < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    omit("Parquet is required") unless defined?(::Parquet)
    Tempfile.create(["data", ".parquet"]) do |file|
      @file = file
      @a_array = build_string_array(["foo", "bar"])
      @b_array = build_int32_array([123, 456])
      @table = build_table("a" => @a_array,
                           "b" => @b_array)
      writer = Parquet::ArrowFileWriter.new(@table.schema, @file.path)
      chunk_size = 1
      writer.write_table(@table, chunk_size)
      writer.close
      yield
    end
  end

  def open_reader(source=@file.path, *args)
    reader = Parquet::ArrowFileReader.new(source, *args)
    begin
      yield(reader)
    ensure
      reader.close
      reader.unref
    end
  end

  sub_test_case(".new(properties)") do
    data("path" => :path, "stream" => :stream)
    test("read") do |source_type|
      properties = Parquet::ReaderProperties.new
      properties.enable_buffered_stream
      properties.pre_buffer = false
      properties.buffer_size = 4096
      source = if source_type == :path
                 @file.path
               else
                 Arrow::FileInputStream.new(@file.path)
               end
      begin
        open_reader(source, properties) do |reader|
          assert_equal(@table, reader.read_table)
          assert_equal(build_table("a" => @a_array.slice(1, 1),
                                   "b" => @b_array.slice(1, 1)),
                       reader.read_row_group(1))
        end
      ensure
        source.unref if source_type == :stream
      end
    end

    test("nonexistent path") do
      properties = Parquet::ReaderProperties.new
      assert_raise(Arrow::Error::Io) do
        Parquet::ArrowFileReader.new("#{@file.path}.nonexistent", properties)
      end
    end

    data("path" => :path, "stream" => :stream)
    test("invalid file") do |source_type|
      Tempfile.create("invalid-parquet") do |file|
        file.write("not a parquet file")
        file.flush
        source = if source_type == :path
                   file.path
                 else
                   Arrow::FileInputStream.new(file.path)
                 end
        properties = Parquet::ReaderProperties.new
        begin
          assert_raise(Arrow::Error::Invalid) do
            Parquet::ArrowFileReader.new(source, properties)
          end
        ensure
          source.unref if source_type == :stream
        end
      end
    end
  end

  def test_schema
    open_reader do |reader|
      assert_equal(<<-SCHEMA.chomp, reader.schema.to_s)
a: string
b: int32
      SCHEMA
    end
  end

  sub_test_case("#read_row_group") do
    test("with column indices") do
      open_reader do |reader|
        assert_equal(build_table("b" => @b_array.slice(0, 1)),
                     reader.read_row_group(0, [-1]))
      end
    end

    test("without column indices") do
      open_reader do |reader|
        assert_equal(build_table("a" => @a_array.slice(1, 1),
                                 "b" => @b_array.slice(1, 1)),
                     reader.read_row_group(1))
      end
    end
  end

  def test_read_column
    open_reader do |reader|
      assert_equal([
                     Arrow::ChunkedArray.new([@a_array]),
                     Arrow::ChunkedArray.new([@b_array]),
                   ],
                   [
                     reader.read_column_data(0),
                     reader.read_column_data(-1),
                   ])
    end
  end

  def test_n_rows
    open_reader do |reader|
      assert_equal(2, reader.n_rows)
    end
  end

  sub_test_case("#close") do
    test("opened file: closed") do
      open_reader do |reader|
        reader.close
        message = "[parquet][arrow][file-reader][read-table]: " +
                  "Invalid: Invalid operation on closed file"
        assert_raise(Arrow::Error::Invalid.new(message)) do
          reader.read_table
        end
      end
    end

    test("given source: not closed") do
      source = Arrow::FileInputStream.new(@file.path)
      begin
        open_reader(source) do |reader|
          reader.close
        end
        assert_equal("PAR1", source.read_at_bytes(0, 4).to_s)
      ensure
        source.unref
      end
    end
  end
end
