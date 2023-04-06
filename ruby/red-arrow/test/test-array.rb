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

class ArrayTest < Test::Unit::TestCase
  sub_test_case(".new") do
    test("Boolean") do
      array = Arrow::BooleanArray.new([true, false, true])
      assert_equal([true, false, true],
                   array.to_a)
    end
  end

  sub_test_case("instance methods") do
    def setup
      @values = [true, false, nil, true]
      @array = Arrow::BooleanArray.new(@values)
    end

    test("#each") do
      assert_equal(@values, @array.to_a)
    end

    sub_test_case("#[]") do
      test("valid range") do
        assert_equal(@values,
                     @array.length.times.collect {|i| @array[i]})
      end

      test("out of range") do
        assert_nil(@array[@array.length])
      end

      test("negative index") do
        assert_equal(@values.last,
                     @array[-1])
      end
    end

    sub_test_case("#==") do
      test("Arrow::Array") do
        assert do
          @array == @array
        end
      end

      test("not Arrow::Array") do
        assert do
          not (@array == 29)
        end
      end
    end

    sub_test_case("#equal_array?") do
      test("no options") do
        array1 = Arrow::FloatArray.new([1.1, Float::NAN])
        array2 = Arrow::FloatArray.new([1.1, Float::NAN])
        assert do
          not array1.equal_array?(array2)
        end
      end

      test("approx") do
        array1 = Arrow::FloatArray.new([1.1])
        array2 = Arrow::FloatArray.new([1.100001])
        assert do
          array1.equal_array?(array2, approx: true)
        end
      end

      test("nans-equal") do
        array1 = Arrow::FloatArray.new([1.1, Float::NAN])
        array2 = Arrow::FloatArray.new([1.1, Float::NAN])
        assert do
          array1.equal_array?(array2, nans_equal: true)
        end
      end

      test("absolute-tolerance") do
        array1 = Arrow::FloatArray.new([1.1])
        array2 = Arrow::FloatArray.new([1.101])
        assert do
          array1.equal_array?(array2, approx: true, absolute_tolerance: 0.01)
        end
      end
    end

    sub_test_case("#cast") do
      test("Symbol") do
        assert_equal(Arrow::Int32Array.new([1, 2, 3]),
                     Arrow::StringArray.new(["1", "2", "3"]).cast(:int32))
      end
    end
  end

  sub_test_case("#filter") do
    def setup
      values = [true, false, false, true]
      @array = Arrow::BooleanArray.new(values)
      @options = Arrow::FilterOptions.new
      @options.null_selection_behavior = :emit_null
    end

    test("Array: boolean") do
      filter = [nil, true, true, false]
      filtered_array = Arrow::BooleanArray.new([nil, false, false])
      assert_equal(filtered_array,
                   @array.filter(filter, @options))
    end

    test("Arrow::BooleanArray") do
      filter = Arrow::BooleanArray.new([nil, true, true, false])
      filtered_array = Arrow::BooleanArray.new([nil, false, false])
      assert_equal(filtered_array,
                   @array.filter(filter, @options))
    end

    test("Arrow::ChunkedArray") do
      chunks = [
        Arrow::BooleanArray.new([nil, true]),
        Arrow::BooleanArray.new([true, false]),
      ]
      filter = Arrow::ChunkedArray.new(chunks)
      filtered_array = Arrow::BooleanArray.new([nil, false, false])
      assert_equal(filtered_array,
                   @array.filter(filter, @options))
    end
  end

  sub_test_case("#take") do
    def setup
      values = [1, 0 ,2]
      @array = Arrow::Int16Array.new(values)
    end

    test("Arrow: boolean") do
      indices = [1, 0, 2]
      assert_equal(Arrow::Int16Array.new([0, 1, 2]),
                   @array.take(indices))
    end

    test("Arrow::Array") do
      indices = Arrow::Int16Array.new([1, 0, 2])
      assert_equal(Arrow::Int16Array.new([0, 1, 2]),
                   @array.take(indices))
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
                   @array.take(indices))
    end
  end

  sub_test_case("#is_in") do
    def setup
      values = [1, 0, 1, 2]
      @array = Arrow::Int16Array.new(values)
    end

    test("Arrow: Array") do
      right = [2, 0]
      assert_equal(Arrow::BooleanArray.new([false, true, false, true]),
                   @array.is_in(right))
    end

    test("Arrow::Array") do
      right = Arrow::Int16Array.new([2, 0])
      assert_equal(Arrow::BooleanArray.new([false, true, false, true]),
                   @array.is_in(right))
    end

    test("Arrow::ChunkedArray") do
      chunks = [
        Arrow::Int16Array.new([1, 4]),
        Arrow::Int16Array.new([0, 3])
      ]
      right = Arrow::ChunkedArray.new(chunks)
      assert_equal(Arrow::BooleanArray.new([true, true, true, false]),
                   @array.is_in(right))
    end
  end

  sub_test_case("#concatenate") do
    test("Arrow::Array: same") do
      assert_equal(Arrow::Int32Array.new([1, 2, nil, 4 ,5, 6]),
                   Arrow::Int32Array.new([1, 2, nil]).
                     concatenate(Arrow::Int32Array.new([4, 5]),
                                 Arrow::Int32Array.new([6])))
    end

    test("Arrow::Array: castable") do
      assert_equal(Arrow::Int32Array.new([1, 2, nil, 4 ,5, 6]),
                   Arrow::Int32Array.new([1, 2, nil]).
                     concatenate(Arrow::Int8Array.new([4, 5]),
                                 Arrow::UInt32Array.new([6])))
    end

    test("Arrow::Array: non-castable") do
      assert_raise(Arrow::Error::Invalid) do
        Arrow::Int32Array.new([1, 2, nil]).
          concatenate(Arrow::StringArray.new(["X"]))
      end
    end

    test("Array") do
      assert_equal(Arrow::Int32Array.new([1, 2, nil, 4 ,nil, 6]),
                   Arrow::Int32Array.new([1, 2, nil]).
                     concatenate([4, nil],
                                 [6]))
    end

    test("invalid") do
      message = "[array][resolve] can't build int32 array: 4"
      assert_raise(ArgumentError.new(message)) do
        Arrow::Int32Array.new([1, 2, nil]).
          concatenate(4)
      end
    end
  end

  sub_test_case("#+") do
    test("Arrow::Array: same") do
      assert_equal(Arrow::Int32Array.new([1, 2, nil, 4 ,5, 6]),
                   Arrow::Int32Array.new([1, 2, nil]) +
                   Arrow::Int32Array.new([4, 5, 6]))
    end

    test("Arrow::Array: castable") do
      assert_equal(Arrow::Int32Array.new([1, 2, nil, 4 ,5, 6]),
                   Arrow::Int32Array.new([1, 2, nil]) +
                   Arrow::Int8Array.new([4, 5, 6]))
    end

    test("Arrow::Array: non-castable") do
      assert_raise(Arrow::Error::Invalid) do
        Arrow::Int32Array.new([1, 2, nil]) +
          Arrow::StringArray.new(["X"])
      end
    end

    test("Array") do
      assert_equal(Arrow::Int32Array.new([1, 2, nil, 4 ,nil, 6]),
                   Arrow::Int32Array.new([1, 2, nil]) +
                   [4, nil, 6])
    end

    test("invalid") do
      message = "[array][resolve] can't build int32 array: 4"
      assert_raise(ArgumentError.new(message)) do
        Arrow::Int32Array.new([1, 2, nil]) + 4
      end
    end
  end

  sub_test_case("#resolve") do
    test("Arrow::Array: same") do
      assert_equal(Arrow::Int32Array.new([1, 2, nil]),
                   Arrow::Int32Array.new([]).
                     resolve(Arrow::Int32Array.new([1, 2, nil])))
    end

    test("Arrow::Array: castable") do
      assert_equal(Arrow::Int32Array.new([1, 2, nil]),
                   Arrow::Int32Array.new([]).
                     resolve(Arrow::Int8Array.new([1, 2, nil])))
    end

    test("Arrow::Array: non-castable") do
      assert_raise(Arrow::Error::Invalid) do
        Arrow::Int32Array.new([]) +
          Arrow::StringArray.new(["X"])
      end
    end

    test("Array: non-parametric") do
      assert_equal(Arrow::Int32Array.new([1, 2, nil]),
                   Arrow::Int32Array.new([]).
                     resolve([1, 2, nil]))
    end

    test("Array: parametric") do
      list_data_type = Arrow::ListDataType.new(name: "visible", type: :boolean)
      list_array = Arrow::ListArray.new(list_data_type, [])
      assert_equal(Arrow::ListArray.new(list_data_type,
                                        [
                                          [true, false],
                                          nil,
                                        ]),
                   list_array.resolve([
                                        [true, false],
                                        nil,
                                      ]))
    end

    test("invalid") do
      message = "[array][resolve] can't build int32 array: 4"
      assert_raise(ArgumentError.new(message)) do
        Arrow::Int32Array.new([]).resolve(4)
      end
    end
  end

  sub_test_case("#index") do
    test("Integer") do
      assert_equal(2,
                   Arrow::Int32Array.new([1, 2, 3, 4, 5]).index(3))
    end
  end
end
