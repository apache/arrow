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

class TestParquetRowGroupMetadata < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    omit("Parquet is required") unless defined?(::Parquet)
    @file = Tempfile.open(["data", ".parquet"])
    @string_array = build_string_array([nil, "hello"])
    fields = [
      Arrow::Field.new("int8", Arrow::Int8DataType.new),
      Arrow::Field.new("boolean", Arrow::BooleanDataType.new),
    ]
    structs = [
      {
        "int8" => -29,
        "boolean" => true,
      },
      nil,
    ]
    @struct_array = build_struct_array(fields, structs)
    @table = build_table("string" => @string_array,
                         "struct" => @struct_array)
    writer = Parquet::ArrowFileWriter.new(@table.schema, @file.path)
    chunk_size = 1
    writer.write_table(@table, chunk_size)
    writer.close
    reader = Parquet::ArrowFileReader.new(@file.path)
    @metadata = reader.metadata.get_row_group(0)
  end

  test("#==") do
    reader = Parquet::ArrowFileReader.new(@file.path)
    other_metadata = reader.metadata.get_row_group(0)
    assert do
      @metadata == other_metadata
    end
  end

  test("#n_columns") do
    assert_equal(3, @metadata.n_columns)
  end

  sub_test_case("#get_column_chunk") do
    test("out of range") do
      message = "[parquet][row-group-metadata][get-column-chunk]: IOError: " +
                "The file only has 3 columns, requested metadata for column: 3"
      assert_raise(Arrow::Error::Io.new(message)) do
        @metadata.get_column_chunk(3)
      end
    end
  end

  test("#n_rows") do
    assert_equal(1, @metadata.n_rows)
  end

  test("#total_size") do
    assert do
      @metadata.total_size > 0
    end
  end

  test("#total_compressed_size") do
    assert do
      @metadata.total_compressed_size > 0
    end
  end

  test("#file_offset") do
    assert do
      @metadata.file_offset > 0
    end
  end

  test("#can_decompress?") do
    assert do
      @metadata.can_decompress?
    end
  end
end
