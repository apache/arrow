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

class TestTake < Test::Unit::TestCase
  include Helper::Buildable
  include Helper::Omittable

  sub_test_case("Array") do
    def test_no_null
      indices = build_int16_array([1, 0, 2])
      assert_equal(build_int16_array([0, 1, 2]),
                   build_int16_array([1, 0 ,2]).take(indices))
    end

    def test_null
      indices = build_int16_array([2, nil, 0])
      assert_equal(build_int16_array([2, nil, 1]),
                   build_int16_array([1, 0, 2]).take(indices))
    end

    def test_out_of_index
      indices = build_int16_array([1, 2, 3])
      assert_raise(Arrow::Error::Index) do
        build_int16_array([0, 1, 2]).take(indices)
      end
    end

    def test_chunked_array
      taken_chunks = [
        build_int16_array([0, 1]),
        build_int16_array([2])
      ]
      taken_chunked_array = Arrow::ChunkedArray.new(taken_chunks)
      indices_chunks = [
        build_int16_array([1, 0]),
        build_int16_array([2])
      ]
      indices = Arrow::ChunkedArray.new(indices_chunks)
      assert_equal(taken_chunked_array,
                   build_int16_array([1, 0, 2]).take_chunked_array(indices))
    end
  end

  sub_test_case("Table") do
    def setup
      fields = [
        Arrow::Field.new("field1", Arrow::Int16DataType.new),
        Arrow::Field.new("field2", Arrow::Int16DataType.new)
      ]
      @schema = Arrow::Schema.new(fields)
      arrays = [
        build_int16_array([0, 1, 2]),
        build_int16_array([3, 5, 4])
      ]
      @table = Arrow::Table.new(@schema, arrays)
    end

    def test_no_null
      arrays = [
        build_int16_array([1, 0, 2]),
        build_int16_array([5, 3, 4])
      ]
      taken_table = Arrow::Table.new(@schema, arrays)
      indices = build_int16_array([1, 0, 2])
      assert_equal(taken_table,
                   @table.take(indices))
    end

    def test_null
      arrays = [
        build_int16_array([2, nil, 0]),
        build_int16_array([4, nil, 3])
      ]
      taken_table = Arrow::Table.new(@schema, arrays)
      indices = build_int16_array([2, nil, 0])
      assert_equal(taken_table,
                   @table.take(indices))
    end

    def test_out_of_index
      indices = build_int16_array([1, 2, 3])
      assert_raise(Arrow::Error::Index) do
        @table.take(indices)
      end
    end

    def test_chunked_array
      arrays = [
        build_int16_array([1, 0, 2]),
        build_int16_array([5, 3, 4])
      ]
      taken_table = Arrow::Table.new(@schema, arrays)
      chunks = [
        build_int16_array([1, 0]),
        build_int16_array([2])
      ]
      indices = Arrow::ChunkedArray.new(chunks)
      assert_equal(taken_table,
                   @table.take_chunked_array(indices))
    end
  end

  sub_test_case("ChunkedArray") do
    def setup
      chunks = [
        build_int16_array([1, 0]),
        build_int16_array([2]),
      ]
      @chunked_array = Arrow::ChunkedArray.new(chunks)
    end

    def test_no_null
      chunks = [
        build_int16_array([0, 1]),
        build_int16_array([2])
      ]
      taken_chunked_array = Arrow::ChunkedArray.new(chunks)
      indices = build_int16_array([1, 0, 2])
      assert_equal(taken_chunked_array,
                   @chunked_array.take(indices))
    end

    def test_null
      chunks = [
        build_int16_array([2, nil]),
        build_int16_array([1])
      ]
      taken_chunked_array = Arrow::ChunkedArray.new(chunks)
      indices = build_int16_array([2, nil, 0])
      assert_equal(taken_chunked_array,
                   @chunked_array.take(indices))
    end

    def test_out_of_index
      indices = build_int16_array([1, 2, 3])
      assert_raise(Arrow::Error::Index) do
        @chunked_array.take(indices)
      end
    end

    def test_chunked_array
      taken_chunks = [
        build_int16_array([0, 1]),
        build_int16_array([2])
      ]
      taken_chunked_array = Arrow::ChunkedArray.new(taken_chunks)
      indices_chunks = [
        build_int16_array([1, 0]),
        build_int16_array([2])
      ]
      indices = Arrow::ChunkedArray.new(indices_chunks)
      assert_equal(taken_chunked_array,
                   @chunked_array.take_chunked_array(indices))
    end
  end

  sub_test_case("RecordBatch") do
    def setup
      fields = [
        Arrow::Field.new("field1", Arrow::Int16DataType.new),
        Arrow::Field.new("field2", Arrow::Int16DataType.new)
      ]
      @schema = Arrow::Schema.new(fields)
      columns = [
        build_int16_array([1, 0, 2]),
        build_int16_array([3, 5, 4])
      ]
      @record_batch = Arrow::RecordBatch.new(@schema, 3, columns)
    end

    def test_no_null
      columns = [
        build_int16_array([0, 1, 2]),
        build_int16_array([5, 3, 4])
      ]
      taken_record_batch = Arrow::RecordBatch.new(@schema, 3, columns)
      indices = build_int16_array([1, 0, 2])
      assert_equal(taken_record_batch,
                   @record_batch.take(indices))
    end

    def test_null
      columns = [
        build_int16_array([2, nil, 1]),
        build_int16_array([4, nil, 3])
      ]
      taken_record_batch = Arrow::RecordBatch.new(@schema, 3, columns)
      indices = build_int16_array([2, nil, 0])
      assert_equal(taken_record_batch,
                   @record_batch.take(indices))
    end

    def test_out_of_index
      indices = build_int16_array([1, 2, 3])
      assert_raise(Arrow::Error::Index) do
        @record_batch.take(indices)
      end
    end
  end
end
