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
    visible_field = Arrow::Field.new("visible", :boolean)
    @schema = Arrow::Schema.new([visible_field])
    visible_arrays = [Arrow::BooleanArray.new([true, false])]
    visible_array = Arrow::ChunkedArray.new(visible_arrays)
    table = Arrow::Table.new(@schema, [visible_array])
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
    test("without block") do
      first_visible_arrays = [Arrow::BooleanArray.new([true])]
      first_visible_array = Arrow::ChunkedArray.new(first_visible_arrays)
      second_visible_arrays = [Arrow::BooleanArray.new([false])]
      second_visible_array = Arrow::ChunkedArray.new(second_visible_arrays)

      Arrow::FileInputStream.open(@file.path) do |input|
        reader = Parquet::ArrowFileReader.new(input)
        each_row_group = reader.each_row_group
        assert_equal({
                       size: 2,
                       to_a: [
                         Arrow::Table.new(@schema, [first_visible_array]),
                         Arrow::Table.new(@schema, [second_visible_array])
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
