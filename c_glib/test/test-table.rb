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

class TestTable < Test::Unit::TestCase
  include Helper::Buildable
  include Helper::Omittable

  sub_test_case(".new") do
    def setup
      @fields = [
        Arrow::Field.new("visible", Arrow::BooleanDataType.new),
        Arrow::Field.new("valid", Arrow::BooleanDataType.new),
      ]
      @schema = Arrow::Schema.new(@fields)
    end

    def dump_table(table)
      table.n_columns.times.collect do |i|
        field = table.schema.get_field(i)
        chunked_array = table.get_column_data(i)
        values = []
        chunked_array.chunks.each do |chunk|
          chunk.length.times do |j|
            values << chunk.get_value(j)
          end
        end
        [
          field.name,
          values,
        ]
      end
    end

    def test_arrays
      require_gi_bindings(3, 3, 1)
      arrays = [
        build_boolean_array([true]),
        build_boolean_array([false]),
      ]
      table = Arrow::Table.new(@schema, arrays)
      assert_equal([
                     ["visible", [true]],
                     ["valid", [false]],
                   ],
                   dump_table(table))
    end

    def test_chunked_arrays
      require_gi_bindings(3, 3, 1)
      arrays = [
        Arrow::ChunkedArray.new([build_boolean_array([true]),
                                 build_boolean_array([false])]),
        Arrow::ChunkedArray.new([build_boolean_array([false]),
                                 build_boolean_array([true])]),
      ]
      table = Arrow::Table.new(@schema, arrays)
      assert_equal([
                     ["visible", [true, false]],
                     ["valid", [false, true]],
                   ],
                   dump_table(table))
    end

    def test_record_batches
      require_gi_bindings(3, 3, 1)
      record_batches = [
        build_record_batch({
                             "visible" => build_boolean_array([true]),
                             "valid" => build_boolean_array([false])
                           }),
        build_record_batch({
                             "visible" => build_boolean_array([false]),
                             "valid" => build_boolean_array([true])
                           }),
      ]
      table = Arrow::Table.new(@schema, record_batches)

      assert_equal([
                     ["visible", [true, false]],
                     ["valid", [false, true]],
                   ],
                   dump_table(table))
    end
  end

  sub_test_case("instance methods") do
    def setup
      @fields = [
        Arrow::Field.new("visible", Arrow::BooleanDataType.new),
        Arrow::Field.new("valid", Arrow::BooleanDataType.new),
      ]
      @schema = Arrow::Schema.new(@fields)
      @columns = [
        build_boolean_array([true]),
        build_boolean_array([false]),
      ]
      @table = Arrow::Table.new(@schema, @columns)
    end

    def test_equal
      other_table = Arrow::Table.new(@schema, @columns)
      assert_equal(@table, other_table)
    end

    def test_equal_metadata
      other_table = Arrow::Table.new(@schema, @columns)
      assert do
        @table.equal_metadata(other_table, true)
      end
    end

    def test_schema
      assert_equal(["visible", "valid"],
                   @table.schema.fields.collect(&:name))
    end

    def test_column_data
      assert_equal([
                     Arrow::ChunkedArray.new([build_boolean_array([true])]),
                     Arrow::ChunkedArray.new([build_boolean_array([false])]),
                   ],
                   [
                     @table.get_column_data(0),
                     @table.get_column_data(-1),
                   ])
    end

    def test_n_columns
      assert_equal(2, @table.n_columns)
    end

    def test_n_rows
      assert_equal(1, @table.n_rows)
    end

    def test_add_column
      field = Arrow::Field.new("added", Arrow::BooleanDataType.new)
      chunked_array = Arrow::ChunkedArray.new([build_boolean_array([true])])
      new_table = @table.add_column(1, field, chunked_array)
      assert_equal(["visible", "added", "valid"],
                   new_table.schema.fields.collect(&:name))
    end

    def test_remove_column
      new_table = @table.remove_column(0)
      assert_equal(["valid"],
                   new_table.schema.fields.collect(&:name))
    end

    def test_replace_column
      field = Arrow::Field.new("added", Arrow::BooleanDataType.new)
      chunked_array = Arrow::ChunkedArray.new([build_boolean_array([true])])
      new_table = @table.replace_column(0, field, chunked_array)
      assert_equal(["added", "valid"],
                   new_table.schema.fields.collect(&:name))
    end

    def test_to_s
      table = build_table("valid" => build_boolean_array([true, false, true]))
      assert_equal(<<-TABLE, table.to_s)
valid: bool
----
valid:
  [
    [
      true,
      false,
      true
    ]
  ]
      TABLE
    end

    sub_test_case("#concatenate") do
      def test_without_options
        table = build_table("visible" =>
                            build_boolean_array([true, false, true, false]))
        table1 = build_table("visible" => build_boolean_array([true]))
        table2 = build_table("visible" => build_boolean_array([false, true]))
        table3 = build_table("visible" => build_boolean_array([false]))
        assert_equal(table, table1.concatenate([table2, table3]))
      end

      def test_with_options
        options = Arrow::TableConcatenateOptions.new
        options.unify_schemas = true
        table = build_table("a" => build_int32_array([1, nil, 3]),
                            "b" => build_int32_array([10, nil, 30]),
                            "c" => build_int32_array([nil, 200, nil]))
        table1 = build_table("a" => build_int32_array([1]),
                             "b" => build_int32_array([10]))
        table2 = build_table("c" => build_int32_array([200]))
        table3 = build_table("a" => build_int32_array([3]),
                             "b" => build_int32_array([30]))
        assert_equal(table, table1.concatenate([table2, table3], options))
      end
    end

    sub_test_case("#slice") do
      test("offset: positive") do
        visibles = [true, false, true]
        table = build_table("visible" => build_boolean_array(visibles))
        assert_equal(build_table("visible" => build_boolean_array([false, true])),
                     table.slice(1, 2))
      end

      test("offset: negative") do
        visibles = [true, false, true]
        table = build_table("visible" => build_boolean_array(visibles))
        assert_equal(build_table("visible" => build_boolean_array([false, true])),
                     table.slice(-2, 2))
      end
    end

    def test_combine_chunks
      table = build_table(
        "visible" => Arrow::ChunkedArray::new([build_boolean_array([true, false, true]),
                                               build_boolean_array([false, true]),
                                               build_boolean_array([false])])
      )
      combined_table = table.combine_chunks
      all_values = combined_table.n_columns.times.collect do |i|
        column = combined_table.get_column_data(i)
        column.n_chunks.times.collect do |j|
          column.get_chunk(j).values
        end
      end
      assert_equal([[[true, false, true, false, true, false]]],
                   all_values)
    end

    sub_test_case("#write_as_feather") do
      def setup
        super
        @tempfile = Tempfile.open("arrow-table-write-as-feather")
        begin
          yield
        ensure
          @tempfile.close!
        end
      end

      def read_feather
        input = Arrow::MemoryMappedInputStream.new(@tempfile.path)
        reader = Arrow::FeatherFileReader.new(input)
        begin
          yield(reader.read)
        ensure
          input.close
        end
      end

      test("default") do
        output = Arrow::FileOutputStream.new(@tempfile.path, false)
        @table.write_as_feather(output)
        output.close

        read_feather do |read_table|
          assert_equal(@table, read_table)
        end
      end

      test("compression") do
        output = Arrow::FileOutputStream.new(@tempfile.path, false)
        properties = Arrow::FeatherWriteProperties.new
        properties.compression = :zstd
        @table.write_as_feather(output, properties)
        output.close

        read_feather do |read_table|
          assert_equal(@table, read_table)
        end
      end
    end
  end
end
