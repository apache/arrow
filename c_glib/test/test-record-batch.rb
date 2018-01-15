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

    def test_equal
      fields = [
        Arrow::Field.new("visible", Arrow::BooleanDataType.new),
        Arrow::Field.new("valid", Arrow::BooleanDataType.new),
      ]
      schema = Arrow::Schema.new(fields)
      columns = [
        build_boolean_array([true, false, true, false, true]),
        build_boolean_array([false, true, false, true, false]),
      ]
      other_record_batch = Arrow::RecordBatch.new(schema, 5, columns)
      assert_equal(@record_batch,
                   other_record_batch)
    end

    def test_schema
      assert_equal(["visible", "valid"],
                   @record_batch.schema.fields.collect(&:name))
    end

    sub_test_case("#column") do
      def test_positive
        assert_equal(build_boolean_array(@valid_values),
                     @record_batch.get_column(1))
      end

      def test_negative
        assert_equal(build_boolean_array(@visible_values),
                     @record_batch.get_column(-2))
      end

      def test_positive_out_of_index
        assert_nil(@record_batch.get_column(2))
      end

      def test_negative_out_of_index
        assert_nil(@record_batch.get_column(-3))
      end
    end

    def test_columns
      assert_equal([5, 5],
                   @record_batch.columns.collect(&:length))
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
        sub_record_batch.get_column(0).get_value(i)
      end
      assert_equal([false, true],
                   sub_visible_values)
    end

    def test_to_s
      assert_equal(<<-PRETTY_PRINT, @record_batch.to_s)
visible: [true, false, true, false, true]
valid: [false, true, false, true, false]
      PRETTY_PRINT
    end
  end
end
