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

class TestArrowFileReader < Test::Unit::TestCase
  def setup
    @schema = Arrow::Schema.new(visible: :boolean)
    table = Arrow::Table.new(@schema, [[true], [false]])
    Tempfile.create(["red-parquet", ".parquet"]) do |file|
      @file = file
      Parquet::ArrowFileWriter.open(table.schema, @file.path) do |writer|
        chunk_size = 1
        writer.write_table(table, chunk_size)
      end
      yield
    end
  end

  sub_test_case("#each_row_group") do
    test("block") do
      Arrow::FileInputStream.open(@file.path) do |input|
        reader = Parquet::ArrowFileReader.new(input)
        row_groups = []
        reader.each_row_group do |row_group|
          row_groups << row_group
        end
        assert_equal([
                       Arrow::Table.new(@schema, [[true]]),
                       Arrow::Table.new(@schema, [[false]])
                     ],
                     row_groups)
      end
    end

    test("without block") do
      Arrow::FileInputStream.open(@file.path) do |input|
        reader = Parquet::ArrowFileReader.new(input)
        each_row_group = reader.each_row_group
        assert_equal({
                       size: 2,
                       to_a: [
                         Arrow::Table.new(@schema, [[true]]),
                         Arrow::Table.new(@schema, [[false]])
                       ],
                     },
                     {
                       size: each_row_group.size,
                       to_a: each_row_group.to_a,
                     })
      end
    end
  end
end
