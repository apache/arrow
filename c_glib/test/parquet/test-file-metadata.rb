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

class TestParquetFileMetadata < Test::Unit::TestCase
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
    @metadata = reader.metadata
  end

  test("#==") do
    reader = Parquet::ArrowFileReader.new(@file.path)
    other_metadata = reader.metadata
    assert do
      @metadata == other_metadata
    end
  end

  test("#n_columns") do
    assert_equal(3, @metadata.n_columns)
  end

  test("#n_schema_elements") do
    assert_equal(5, @metadata.n_schema_elements)
  end

  test("#n_rows") do
    assert_equal(2, @metadata.n_rows)
  end

  test("#n_row_groups") do
    assert_equal(2, @metadata.n_row_groups)
  end

  sub_test_case("#get_row_group") do
    test("out of range") do
      message = "[parquet][file-metadata][get-row-group]: IOError: " +
                "The file only has 2 row groups, " +
                "requested metadata for row group: 2"
      assert_raise(Arrow::Error::Io.new(message)) do
        @metadata.get_row_group(2)
      end
    end
  end

  test("#created_by") do
    assert_equal("parquet-cpp-arrow version 1.0.0",
                 @metadata.created_by.gsub(/ [\d.]+(?:-SNAPSHOT)?\z/, " 1.0.0"))
  end

  test("#size") do
    assert do
      @metadata.size > 0
    end
  end

  test("#can_decompress?") do
    assert do
      @metadata.can_decompress?
    end
  end
end
