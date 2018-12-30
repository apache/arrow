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
        column = table.get_column(i)
        values = []
        column.data.chunks.each do |chunk|
          chunk.length.times do |j|
            values << chunk.get_value(j)
          end
        end
        [
          column.name,
          values,
        ]
      end
    end

    def test_columns
      columns = [
        Arrow::Column.new(@fields[0], build_boolean_array([true])),
        Arrow::Column.new(@fields[1], build_boolean_array([false])),
      ]
      table = Arrow::Table.new(@schema, columns)
      assert_equal([
                     ["visible", [true]],
                     ["valid", [false]],
                   ],
                   dump_table(table))
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
      fields = [
        Arrow::Field.new("visible", Arrow::BooleanDataType.new),
        Arrow::Field.new("valid", Arrow::BooleanDataType.new),
      ]
      schema = Arrow::Schema.new(fields)
      columns = [
        Arrow::Column.new(fields[0], build_boolean_array([true])),
        Arrow::Column.new(fields[1], build_boolean_array([false])),
      ]
      @table = Arrow::Table.new(schema, columns)
    end

    def test_equal
      fields = [
        Arrow::Field.new("visible", Arrow::BooleanDataType.new),
        Arrow::Field.new("valid", Arrow::BooleanDataType.new),
      ]
      schema = Arrow::Schema.new(fields)
      columns = [
        Arrow::Column.new(fields[0], build_boolean_array([true])),
        Arrow::Column.new(fields[1], build_boolean_array([false])),
      ]
      other_table = Arrow::Table.new(schema, columns)
      assert_equal(@table, other_table)
    end

    def test_schema
      assert_equal(["visible", "valid"],
                   @table.schema.fields.collect(&:name))
    end

    def test_column
      assert_equal("valid", @table.get_column(1).name)
    end

    def test_n_columns
      assert_equal(2, @table.n_columns)
    end

    def test_n_rows
      assert_equal(1, @table.n_rows)
    end

    def test_add_column
      field = Arrow::Field.new("added", Arrow::BooleanDataType.new)
      column = Arrow::Column.new(field, build_boolean_array([true]))
      new_table = @table.add_column(1, column)
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
      column = Arrow::Column.new(field, build_boolean_array([true]))
      new_table = @table.replace_column(0, column)
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
  end
end
