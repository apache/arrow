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

class TestFilter < Test::Unit::TestCase
  include Helper::Buildable

  sub_test_case("FilterOptions") do
    def test_default_null_selection_behavior
      assert_equal(Arrow::FilterNullSelectionBehavior::DROP,
                   Arrow::FilterOptions.new.null_selection_behavior)
    end
  end

  sub_test_case("Array") do
    def setup
      @filter = build_boolean_array([false, true, true, nil])
    end

    def test_filter
      assert_equal(build_int16_array([1, 0]),
                   build_int16_array([0, 1, 0, 2]).filter(@filter))
    end

    def test_filter_emit_null
      options = Arrow::FilterOptions.new
      options.null_selection_behavior = :emit_null
      assert_equal(build_int16_array([1, 0, nil]),
                   build_int16_array([0, 1, 0, 2]).filter(@filter, options))
    end

    def test_invalid_array_length
      filter = build_boolean_array([false, true, true, false])
      assert_raise(Arrow::Error::Invalid) do
        build_int16_array([0, 1, 0]).filter(filter)
      end
    end
  end

  sub_test_case("Table") do
    def setup
      fields = [
        Arrow::Field.new("visible", Arrow::BooleanDataType.new),
        Arrow::Field.new("valid", Arrow::BooleanDataType.new),
      ]
      @schema = Arrow::Schema.new(fields)
      arrays = [
        build_boolean_array([true, false, true]),
        build_boolean_array([false, true, true]),
      ]
      @table = Arrow::Table.new(@schema, arrays)
    end

    def test_filter
      filter = build_boolean_array([false, true, nil])
      arrays = [
        build_boolean_array([false]),
        build_boolean_array([true]),
      ]
      filtered_table = Arrow::Table.new(@schema, arrays)
      assert_equal(filtered_table,
                   @table.filter(filter))
    end

    def test_filter_emit_null
      filter = build_boolean_array([false, true, nil])
      arrays = [
        build_boolean_array([false, nil]),
        build_boolean_array([true, nil]),
      ]
      filtered_table = Arrow::Table.new(@schema, arrays)
      options = Arrow::FilterOptions.new
      options.null_selection_behavior = :emit_null
      assert_equal(filtered_table,
                   @table.filter(filter, options))
    end

    def test_filter_chunked_array
      chunks = [
        build_boolean_array([false]),
        build_boolean_array([true, nil]),
      ]
      filter = Arrow::ChunkedArray.new(chunks)
      arrays = [
        build_boolean_array([false]),
        build_boolean_array([true]),
      ]
      filtered_table = Arrow::Table.new(@schema, arrays)
      assert_equal(filtered_table,
                   @table.filter_chunked_array(filter))
    end

    def test_filter_chunked_array_emit_null
      chunks = [
        build_boolean_array([false]),
        build_boolean_array([true, nil]),
      ]
      filter = Arrow::ChunkedArray.new(chunks)
      arrays = [
        build_boolean_array([false, nil]),
        build_boolean_array([true, nil]),
      ]
      filtered_table = Arrow::Table.new(@schema, arrays)
      options = Arrow::FilterOptions.new
      options.null_selection_behavior = :emit_null
      assert_equal(filtered_table,
                   @table.filter_chunked_array(filter, options))
    end

    def test_invalid_array_length
      filter = build_boolean_array([false, true, true, false])
      assert_raise(Arrow::Error::Invalid) do
        @table.filter(filter)
      end
    end
  end

  sub_test_case("ChunkedArray") do
    def setup
      chunks = [
        build_boolean_array([true, false]),
        build_boolean_array([true]),
      ]
      @chunked_array = Arrow::ChunkedArray.new(chunks)
    end

    def test_filter
      filter = build_boolean_array([false, true, nil])
      chunks = [
        build_boolean_array([false]),
      ]
      filtered_chunked_array = Arrow::ChunkedArray.new(chunks)
      assert_equal(filtered_chunked_array,
                   @chunked_array.filter(filter))
    end

    def test_filter_emit_null
      filter = build_boolean_array([false, true, nil])
      chunks = [
        build_boolean_array([false]),
        build_boolean_array([nil]),
      ]
      filtered_chunked_array = Arrow::ChunkedArray.new(chunks)
      options = Arrow::FilterOptions.new
      options.null_selection_behavior = :emit_null
      assert_equal(filtered_chunked_array,
                   @chunked_array.filter(filter, options))
    end

    def test_filter_chunked_array
      chunks = [
        build_boolean_array([false]),
        build_boolean_array([true, nil]),
      ]
      filter = Arrow::ChunkedArray.new(chunks)
      filtered_chunks = [
        build_boolean_array([false]),
      ]
      filtered_chunked_array = Arrow::ChunkedArray.new(filtered_chunks)
      assert_equal(filtered_chunked_array,
                   @chunked_array.filter_chunked_array(filter))
    end

    def test_filter_chunked_array_emit_null
      chunks = [
        build_boolean_array([false]),
        build_boolean_array([true, nil]),
      ]
      filter = Arrow::ChunkedArray.new(chunks)
      filtered_chunks = [
        build_boolean_array([false]),
        build_boolean_array([nil]),
      ]
      filtered_chunked_array = Arrow::ChunkedArray.new(filtered_chunks)
      options = Arrow::FilterOptions.new
      options.null_selection_behavior = :emit_null
      assert_equal(filtered_chunked_array,
                   @chunked_array.filter_chunked_array(filter, options))
    end

    def test_invalid_array_length
      filter = build_boolean_array([false, true, true, false])
      assert_raise(Arrow::Error::Invalid) do
        @chunked_array.filter(filter)
      end
    end
  end

  sub_test_case("RecordBatch") do
    def setup
      fields = [
        Arrow::Field.new("visible", Arrow::BooleanDataType.new),
        Arrow::Field.new("valid", Arrow::BooleanDataType.new),
      ]
      @schema = Arrow::Schema.new(fields)
      columns = [
        build_boolean_array([true, false, true]),
        build_boolean_array([false, true, false]),
      ]
      @record_batch = Arrow::RecordBatch.new(@schema, 3, columns)
    end

    def test_filter
      filter = build_boolean_array([false, true, nil])
      columns = [
        build_boolean_array([false]),
        build_boolean_array([true]),
      ]
      filtered_record_batch = Arrow::RecordBatch.new(@schema, 1, columns)
      assert_equal(filtered_record_batch,
                   @record_batch.filter(filter))
    end

    def test_filter_emit_null
      filter = build_boolean_array([false, true, nil])
      columns = [
        build_boolean_array([false, nil]),
        build_boolean_array([true, nil]),
      ]
      filtered_record_batch = Arrow::RecordBatch.new(@schema, 2, columns)
      options = Arrow::FilterOptions.new
      options.null_selection_behavior = :emit_null
      assert_equal(filtered_record_batch,
                   @record_batch.filter(filter, options))
    end

    def test_invalid_array_length
      filter = build_boolean_array([false, true, true, false])
      assert_raise(Arrow::Error::Invalid) do
        @record_batch.filter(filter)
      end
    end
  end
end
