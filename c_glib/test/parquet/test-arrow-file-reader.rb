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
    @file = Tempfile.open(["data", ".parquet"])
    @a_array = build_string_array(["foo", "bar"])
    @b_array = build_int32_array([123, 456])
    @table = build_table("a" => @a_array,
                         "b" => @b_array)
    writer = Parquet::ArrowFileWriter.new(@table.schema, @file.path)
    chunk_size = 2
    writer.write_table(@table, chunk_size)
    writer.close
    @reader = Parquet::ArrowFileReader.new(@file.path)
  end

  def test_schema
    assert_equal(<<-SCHEMA.chomp, @reader.schema.to_s)
a: string
b: int32
    SCHEMA
  end

  def test_select_schema
    assert_equal(<<-SCHEMA.chomp, @reader.select_schema([0]).to_s)
a: string
    SCHEMA
    assert_equal(<<-SCHEMA.chomp, @reader.select_schema([1]).to_s)
b: int32
    SCHEMA
    assert_equal(<<-SCHEMA.chomp, @reader.select_schema([0, 1]).to_s)
a: string
b: int32
    SCHEMA
  end

  def test_read_column
    a = @reader.read_column(0)
    assert_equal([
                   "a: string",
                   Arrow::ChunkedArray.new([@a_array]).to_s,
                 ],
                 [
                   a.field.to_s,
                   a.data.to_s,
                 ])

    b = @reader.read_column(1)
    assert_equal([
                   "b: int32",
                   Arrow::ChunkedArray.new([@b_array]).to_s,
                 ],
                 [
                   b.field.to_s,
                   b.data.to_s,
                 ])
  end
end
