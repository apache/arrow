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

class TestParquetArrow < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    omit("Parquet is required") unless defined?(::Parquet)
  end

  def test_read_write
    tempfile = Tempfile.open(["data", ".parquet"])

    values = [true, nil, false, true]
    chunk_size = 2

    field = Arrow::Field.new("enabled", Arrow::BooleanDataType.new)
    schema = Arrow::Schema.new([field])
    writer = Parquet::ArrowFileWriter.new(schema, tempfile.path)
    begin
      columns = [
        Arrow::Column.new(field, build_boolean_array(values)),
      ]
      table = Arrow::Table.new(schema, columns)
      writer.write_table(table, chunk_size)
    ensure
      writer.close
    end

    reader = Parquet::ArrowFileReader.new(tempfile.path)
    reader.use_threads = true
    assert_equal(values.length / chunk_size, reader.n_row_groups)
    table = reader.read_table
    table_data = table.n_columns.times.collect do |i|
      column = table.get_column(i)
      data = []
      column.data.chunks.each do |chunk|
        chunk.length.times do |j|
          if chunk.null?(j)
            data << nil
          else
            data << chunk.get_value(j)
          end
        end
      end
      [column.name, data]
    end
    assert_equal([["enabled", values]], table_data)
  end

  def test_schema
    tempfile = Tempfile.open(["data", ".parquet"])

    table = build_table("a" => build_string_array(["foo", "bar"]),
                        "b" => build_int32_array([123, 456]))
    writer = Parquet::ArrowFileWriter.new(table.schema, tempfile.path)
    writer.write_table(table, 2)
    writer.close

    reader = Parquet::ArrowFileReader.new(tempfile.path)
    assert_equal(<<-SCHEMA.chomp, reader.schema.to_s)
a: string
b: int32
    SCHEMA
  end

  def test_select_schema
    tempfile = Tempfile.open(["data", ".parquet"])

    table = build_table("a" => build_string_array(["foo", "bar"]),
                        "b" => build_int32_array([123, 456]))
    writer = Parquet::ArrowFileWriter.new(table.schema, tempfile.path)
    writer.write_table(table, 2)
    writer.close

    reader = Parquet::ArrowFileReader.new(tempfile.path)
    assert_equal(<<-SCHEMA.chomp, reader.select_schema([0]).to_s)
a: string
    SCHEMA
    assert_equal(<<-SCHEMA.chomp, reader.select_schema([1]).to_s)
b: int32
    SCHEMA
    assert_equal(<<-SCHEMA.chomp, reader.select_schema([0, 1]).to_s)
a: string
b: int32
    SCHEMA
  end

  def test_read_column
    tempfile = Tempfile.open(["data", ".parquet"])

    string_array = build_string_array(["foo", "bar"])
    int32_array = build_int32_array([123, 456])
    table = build_table("a" => string_array,
                        "b" => int32_array)
    writer = Parquet::ArrowFileWriter.new(table.schema, tempfile.path)
    writer.write_table(table, 2)
    writer.close

    reader = Parquet::ArrowFileReader.new(tempfile.path)

    column_a = reader.read_column(0)
    assert_equal([
                   "a: string",
                   Arrow::ChunkedArray.new([string_array]).to_s,
                 ],
                 [
                   column_a.field.to_s,
                   column_a.data.to_s,
                 ])

    column_b = reader.read_column(1)
    assert_equal([
                   "b: int32",
                   Arrow::ChunkedArray.new([int32_array]).to_s,
                 ],
                 [
                   column_b.field.to_s,
                   column_b.data.to_s,
                 ])
  end
end
