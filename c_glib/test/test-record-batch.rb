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

class TestRecordBatch < Test::Unit::TestCase
  include Helper::Buildable
  include Helper::Omittable

  sub_test_case(".new") do
    def test_valid
      fields = [
        Arrow::Field.new("visible", Arrow::BooleanDataType.new),
        Arrow::Field.new("valid", Arrow::BooleanDataType.new),
      ]
      schema = Arrow::Schema.new(fields)
      columns = [
        build_boolean_array([true]),
        build_boolean_array([false]),
      ]
      record_batch = Arrow::RecordBatch.new(schema, 1, columns)
      assert_equal(1, record_batch.n_rows)
    end

    def test_no_columns
      fields = [
        Arrow::Field.new("visible", Arrow::BooleanDataType.new),
      ]
      schema = Arrow::Schema.new(fields)
      message = "[record-batch][new]: " +
        "Invalid: Number of columns did not match schema"
      assert_raise(Arrow::Error::Invalid.new(message)) do
        Arrow::RecordBatch.new(schema, 0, [])
      end
    end
  end

  sub_test_case("instance methods") do
    def setup
      @visible_field = Arrow::Field.new("visible", Arrow::BooleanDataType.new)
      @visible_values = [true, false, true, false, true]
      @valid_field = Arrow::Field.new("valid", Arrow::BooleanDataType.new)
      @valid_values = [false, true, false, true, false]

      fields = [
        @visible_field,
        @valid_field,
      ]
      schema = Arrow::Schema.new(fields)
      columns = [
        build_boolean_array(@visible_values),
        build_boolean_array(@valid_values),
      ]
      @record_batch = Arrow::RecordBatch.new(schema,
                                             @visible_values.size,
                                             columns)
    end

    def test_export
      require_gi_bindings(3, 4, 8)
      success, c_abi_array, c_abi_schema = @record_batch.export
      schema = Arrow::Schema.import(c_abi_schema)
      assert_equal([success, @record_batch],
                   [true, Arrow::RecordBatch.import(c_abi_array, schema)])
    end

    sub_test_case("#equal") do
      def setup
        require_gi_bindings(3, 4, 2)

        @fields = [
          Arrow::Field.new("visible", Arrow::BooleanDataType.new),
          Arrow::Field.new("valid", Arrow::BooleanDataType.new),
        ]
        @schema = Arrow::Schema.new(@fields)
        @columns = [
          build_boolean_array([true, false, true, false, true]),
          build_boolean_array([false, true, false, true, false]),
        ]
        @record_batch = Arrow::RecordBatch.new(@schema, 5, @columns)
      end

      def test_equal
        other_record_batch = Arrow::RecordBatch.new(@schema, 5, @columns)
        assert_equal(@record_batch, other_record_batch)
      end

      def test_equal_metadata
        schema_with_meta = @schema.with_metadata("key" => "value")
        other_record_batch = Arrow::RecordBatch.new(schema_with_meta, 5, @columns)

        assert @record_batch.equal_metadata(other_record_batch, false)
        assert do
          not @record_batch.equal_metadata(other_record_batch, true)
        end
      end
    end

    def test_schema
      assert_equal(["visible", "valid"],
                   @record_batch.schema.fields.collect(&:name))
    end

    sub_test_case("#column_data") do
      def test_positive
        assert_equal(build_boolean_array(@valid_values),
                     @record_batch.get_column_data(1))
      end

      def test_negative
        assert_equal(build_boolean_array(@visible_values),
                     @record_batch.get_column_data(-2))
      end

      def test_positive_out_of_index
        assert_nil(@record_batch.get_column_data(2))
      end

      def test_negative_out_of_index
        assert_nil(@record_batch.get_column_data(-3))
      end
    end

    def test_n_columns
      assert_equal(2, @record_batch.n_columns)
    end

    def test_n_rows
      assert_equal(5, @record_batch.n_rows)
    end

    def test_slice
      sub_record_batch = @record_batch.slice(3, 2)
      sub_visible_values = sub_record_batch.n_rows.times.collect do |i|
        sub_record_batch.get_column_data(0).get_value(i)
      end
      assert_equal([false, true],
                   sub_visible_values)
    end

    def test_to_s
      assert_equal(<<-PRETTY_PRINT, @record_batch.to_s)
visible:   [
    true,
    false,
    true,
    false,
    true
  ]
valid:   [
    false,
    true,
    false,
    true,
    false
  ]
      PRETTY_PRINT
    end

    def test_add_column
      field = Arrow::Field.new("added", Arrow::BooleanDataType.new)
      column = build_boolean_array([false, false, true, true, true])
      new_record_batch = @record_batch.add_column(1, field, column)
      assert_equal(["visible", "added", "valid"],
                   new_record_batch.schema.fields.collect(&:name))
    end

    def test_remove_column
      new_record_batch = @record_batch.remove_column(0)
      assert_equal(["valid"],
                   new_record_batch.schema.fields.collect(&:name))
    end

    def test_serialize
      buffer = @record_batch.serialize
      input_stream = Arrow::BufferInputStream.new(buffer)
      assert_equal(@record_batch,
                   input_stream.read_record_batch(@record_batch.schema))
    end

    sub_test_case("#validate") do
      def setup
        @id_field = Arrow::Field.new("id", Arrow::UInt8DataType.new)
        @name_field = Arrow::Field.new("name", Arrow::StringDataType.new)
        @schema = Arrow::Schema.new([@id_field, @name_field])

        @id_value = build_uint_array([1])
        @name_value = build_string_array(["abc"])
        @values = [@id_value, @name_value]
      end

      def test_valid
        n_rows = @id_value.length
        record_batch = Arrow::RecordBatch.new(@schema, n_rows, @values)

        assert do
          record_batch.validate
        end
      end

      def test_invalid
        message = "[record-batch][validate]: Invalid: " +
          "Number of rows in column 0 did not match batch: 1 vs 2"
        n_rows = @id_value.length + 1 # incorrect number of rows

        record_batch = Arrow::RecordBatch.new(@schema, n_rows, @values)
        assert_raise(Arrow::Error::Invalid.new(message)) do
          record_batch.validate
        end
      end
    end
  end
end
