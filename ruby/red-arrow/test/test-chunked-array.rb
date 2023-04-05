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

class ChunkedArrayTest < Test::Unit::TestCase
  test("#each") do
    arrays = [
      Arrow::BooleanArray.new([true, false]),
      Arrow::BooleanArray.new([nil, true]),
    ]
    chunked_array = Arrow::ChunkedArray.new(arrays)
    assert_equal([true, false, nil, true],
                 chunked_array.to_a)
  end

  sub_test_case("#pack") do
    test("basic array") do
      arrays = [
        Arrow::BooleanArray.new([true, false]),
        Arrow::BooleanArray.new([nil, true]),
      ]
      chunked_array = Arrow::ChunkedArray.new(arrays)
      packed_chunked_array = chunked_array.pack
      assert_equal([
                     Arrow::BooleanArray,
                     [true, false, nil, true],
                   ],
                   [
                     packed_chunked_array.class,
                     packed_chunked_array.to_a,
                   ])
    end

    test("TimestampArray") do
      type = Arrow::TimestampDataType.new(:nano)
      arrays = [
        Arrow::TimestampArrayBuilder.new(type).build([Time.at(0)]),
        Arrow::TimestampArrayBuilder.new(type).build([Time.at(1)]),
      ]
      chunked_array = Arrow::ChunkedArray.new(arrays)
      packed_chunked_array = chunked_array.pack
      assert_equal([
                     Arrow::TimestampArray,
                     [Time.at(0), Time.at(1)],
                   ],
                   [
                     packed_chunked_array.class,
                     packed_chunked_array.to_a,
                   ])
    end
  end

  sub_test_case("#==") do
    def setup
      arrays = [
        Arrow::BooleanArray.new([true]),
        Arrow::BooleanArray.new([false, true]),
      ]
      @chunked_array = Arrow::ChunkedArray.new(arrays)
    end

    test("Arrow::ChunkedArray") do
      assert do
        @chunked_array == @chunked_array
      end
    end

    test("not Arrow::ChunkedArray") do
      assert do
        not (@chunked_array == 29)
      end
    end
  end

  sub_test_case("#filter") do
    def setup
      arrays = [
        Arrow::BooleanArray.new([false, true]),
        Arrow::BooleanArray.new([false, true, false]),
      ]
      @chunked_array = Arrow::ChunkedArray.new(arrays)
      @options = Arrow::FilterOptions.new
      @options.null_selection_behavior = :emit_null
    end

    test("Array: boolean") do
      filter = [nil, true, true, false, true]
      chunks = [
        Arrow::BooleanArray.new([nil, true]),
        Arrow::BooleanArray.new([false, false]),
      ]
      filtered_chunked_array = Arrow::ChunkedArray.new(chunks)
      assert_equal(filtered_chunked_array,
                   @chunked_array.filter(filter, @options))
    end

    test("Arrow::BooleanArray") do
      filter = Arrow::BooleanArray.new([nil, true, true, false, true])
      chunks = [
        Arrow::BooleanArray.new([nil, true]),
        Arrow::BooleanArray.new([false, false]),
      ]
      filtered_chunked_array = Arrow::ChunkedArray.new(chunks)
      assert_equal(filtered_chunked_array,
                   @chunked_array.filter(filter, @options))
    end

    test("Arrow::ChunkedArray") do
      chunks = [
        Arrow::BooleanArray.new([nil, true]),
        Arrow::BooleanArray.new([true, false, true]),
      ]
      filter = Arrow::ChunkedArray.new(chunks)
      filtered_chunks = [
        Arrow::BooleanArray.new([nil, true]),
        Arrow::BooleanArray.new([false, false]),
      ]
      filtered_chunked_array = Arrow::ChunkedArray.new(filtered_chunks)
      assert_equal(filtered_chunked_array,
                   @chunked_array.filter(filter, @options))
    end
  end

  sub_test_case("#take") do
    def setup
      chunks = [
        Arrow::Int16Array.new([1, 0]),
        Arrow::Int16Array.new([2]),
      ]
      @chunked_array = Arrow::ChunkedArray.new(chunks)
    end

    test("Arrow: boolean") do
      chunks = [
        Arrow::Int16Array.new([0, 1]),
        Arrow::Int16Array.new([2])
      ]
      taken_chunked_array = Arrow::ChunkedArray.new(chunks)
      indices = [1, 0, 2]
      assert_equal(taken_chunked_array,
                   @chunked_array.take(indices))
    end

    test("Arrow::Array") do
      chunks = [
        Arrow::Int16Array.new([0, 1]),
        Arrow::Int16Array.new([2])
      ]
      taken_chunked_array = Arrow::ChunkedArray.new(chunks)
      indices = Arrow::Int16Array.new([1, 0, 2])
      assert_equal(taken_chunked_array,
                   @chunked_array.take(indices))
    end

    test("Arrow::ChunkedArray") do
      taken_chunks = [
        Arrow::Int16Array.new([0, 1]),
        Arrow::Int16Array.new([2])
      ]
      taken_chunked_array = Arrow::ChunkedArray.new(taken_chunks)
      indices_chunks = [
        Arrow::Int16Array.new([1, 0]),
        Arrow::Int16Array.new([2])
      ]
      indices = Arrow::ChunkedArray.new(indices_chunks)
      assert_equal(taken_chunked_array,
                   @chunked_array.take(indices))
    end
  end

  test("#cast") do
    chunked_array = Arrow::ChunkedArray.new([[1, nil, 3]])
    assert_equal(Arrow::ChunkedArray.new([["1", nil, "3"]]),
                 chunked_array.cast(:string))
  end

  test("#index") do
    arrays = [
      Arrow::Int32Array.new([1, 2]),
      Arrow::Int32Array.new([3, 4, 5]),
    ]
    chunked_array = Arrow::ChunkedArray.new(arrays)
    assert_equal(2, chunked_array.index(3))
  end
end
