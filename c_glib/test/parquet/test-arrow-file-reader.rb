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
      @reader = Parquet::ArrowFileReader.new(@file.path)
      begin
        yield
      ensure
        @reader.unref
      end
    end
  end

  sub_test_case(".new with properties") do
    data("path" => :path, "stream" => :stream)
    test("read") do |source_type|
      properties = Parquet::ReaderProperties.new
      properties.enable_buffered_stream
      properties.buffer_size = 4096
      source = if source_type == :path
                 @file.path
               else
                 Arrow::FileInputStream.new(@file.path)
               end
      reader = Parquet::ArrowFileReader.new(source, properties)
      begin
        # The reader owns a copy of the properties and the native source.
        properties.disable_buffered_stream
        properties.buffer_size = 0
        properties.unref
        source.unref if source_type == :stream
        assert_equal(@table, reader.read_table)
        assert_equal(build_table("a" => @a_array.slice(1, 1),
                                 "b" => @b_array.slice(1, 1)),
                     reader.read_row_group(1))
      ensure
        reader.close
        reader.unref
      end
    end

    data("path" => :path, "stream" => :stream)
    test("default properties") do |source_type|
      source = if source_type == :path
                 @file.path
               else
                 Arrow::FileInputStream.new(@file.path)
               end
      reader = Parquet::ArrowFileReader.new(source, nil)
      begin
        assert_equal(@table, reader.read_table)
      ensure
        reader.close
        reader.unref
        source.unref if source_type == :stream
      end
    end

    test("missing path") do
      properties = Parquet::ReaderProperties.new
      assert_raise(Arrow::Error::Io) do
        Parquet::ArrowFileReader.new("#{@file.path}.missing", properties)
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
    assert_equal(<<-SCHEMA.chomp, @reader.schema.to_s)
a: string
b: int32
    SCHEMA
  end

  sub_test_case("#read_row_group") do
    test("with column indices") do
      assert_equal(build_table("b" => @b_array.slice(0, 1)),
                   @reader.read_row_group(0, [-1]))
    end

    test("without column indices") do
      assert_equal(build_table("a" => @a_array.slice(1, 1),
                               "b" => @b_array.slice(1, 1)),
                   @reader.read_row_group(1))
    end
  end

  def test_read_column
    assert_equal([
                   Arrow::ChunkedArray.new([@a_array]),
                   Arrow::ChunkedArray.new([@b_array]),
                 ],
                 [
                   @reader.read_column_data(0),
                   @reader.read_column_data(-1),
                 ])
  end

  def test_n_rows
    assert_equal(2, @reader.n_rows)
  end
end
