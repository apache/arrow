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

class TestFunction < Test::Unit::TestCase
  include Helper::Buildable

  def test_all
    or_function = Arrow::Function.find("or")
    assert do
      Arrow::Function.all.include?(or_function)
    end
  end

  sub_test_case("#execute") do
    def test_array
      or_function = Arrow::Function.find("or")
      args = [
        Arrow::ArrayDatum.new(build_boolean_array([true, false, false])),
        Arrow::ArrayDatum.new(build_boolean_array([true, false, true])),
      ]
      assert_equal(build_boolean_array([true, false, true]),
                   or_function.execute(args).value)
    end

    def test_chunked_array
      or_function = Arrow::Function.find("or")
      chunked_arrays = [
        Arrow::ChunkedArray.new([
                                  build_boolean_array([true]),
                                  build_boolean_array([false, false]),
                                ]),
        Arrow::ChunkedArray.new([
                                  build_boolean_array([true, false]),
                                  build_boolean_array([true]),
                                ]),
      ]
      args = chunked_arrays.collect do |chunked_array|
        Arrow::ChunkedArrayDatum.new(chunked_array)
      end
      expected_array = build_boolean_array([true, false, true])
      expected = Arrow::ChunkedArray.new([expected_array])
      assert_equal(expected,
                   or_function.execute(args).value)
    end

    def test_input_scalar
      add_function = Arrow::Function.find("add")
      args = [
        Arrow::ArrayDatum.new(build_int8_array([1, 2, 3])),
        Arrow::ScalarDatum.new(Arrow::Int8Scalar.new(5)),
      ]
      assert_equal(build_int8_array([6, 7, 8]),
                   add_function.execute(args).value)
    end

    def test_output_scalar
      sum_function = Arrow::Function.find("sum")
      args = [
        Arrow::ArrayDatum.new(build_int8_array([1, 2, 3])),
      ]
      assert_equal(Arrow::Int64Scalar.new(6),
                   sum_function.execute(args).value)
    end

    def test_options
      cast_function = Arrow::Function.find("cast")
      args = [
        Arrow::ArrayDatum.new(build_string_array(["1", "2", "-3"])),
      ]
      options = Arrow::CastOptions.new
      options.to_data_type = Arrow::Int8DataType.new
      assert_equal(build_int8_array([1, 2, -3]),
                   cast_function.execute(args, options).value)
    end
  end

  def test_name
    or_function = Arrow::Function.find("or")
    assert_equal("or", or_function.name)
  end

  def test_to_s
    or_function = Arrow::Function.find("or")
    assert_equal("or(x, y): Logical 'or' boolean values",
                 or_function.to_s)
  end

  sub_test_case("#default_options") do
    def test_nonexistent
      or_function = Arrow::Function.find("or")
      assert_nil(or_function.default_options)
    end

    def test_scalar_aggregate_options
      sum_function = Arrow::Function.find("sum")
      assert_equal(Arrow::ScalarAggregateOptions.new,
                   sum_function.default_options)
    end

    def test_count_options
      count_function = Arrow::Function.find("count")
      assert_equal(Arrow::CountOptions.new,
                   count_function.default_options)
    end

    def test_filter_options
      filter_function = Arrow::Function.find("filter")
      assert_equal(Arrow::FilterOptions.new,
                   filter_function.default_options)
    end

    def test_take_options
      take_function = Arrow::Function.find("take")
      assert_equal(Arrow::TakeOptions.new,
                   take_function.default_options)
    end

    def test_array_sort_options
      array_sort_indices_function = Arrow::Function.find("array_sort_indices")
      assert_equal(Arrow::ArraySortOptions.new(:ascending),
                   array_sort_indices_function.default_options)
    end

    def test_sort_options
      sort_indices_function = Arrow::Function.find("sort_indices")
      assert_equal(Arrow::SortOptions.new,
                   sort_indices_function.default_options)
    end

    def test_variance_options
      stddev_function = Arrow::Function.find("stddev")
      assert_equal(Arrow::VarianceOptions.new,
                   stddev_function.default_options)
    end

    def test_round_options
      round_function = Arrow::Function.find("round")
      assert_equal(Arrow::RoundOptions.new,
                   round_function.default_options)
    end

    def test_round_to_multiple_options
      round_to_multiple_function = Arrow::Function.find("round_to_multiple")
      assert_equal(Arrow::RoundToMultipleOptions.new,
                   round_to_multiple_function.default_options)
    end
  end

  sub_test_case("#options_type") do
    def test_nonexistent
      or_function = Arrow::Function.find("or")
      assert_equal(GLib::Type::NONE,
                   or_function.options_type)
    end

    def test_cast_options
      cast_function = Arrow::Function.find("cast")
      assert_equal(Arrow::CastOptions.gtype,
                   cast_function.options_type)
    end

    def test_scalar_aggregate_options
      sum_function = Arrow::Function.find("sum")
      assert_equal(Arrow::ScalarAggregateOptions.gtype,
                   sum_function.options_type)
    end

    def test_count_options
      count_function = Arrow::Function.find("count")
      assert_equal(Arrow::CountOptions.gtype,
                   count_function.options_type)
    end

    def test_filter_options
      filter_function = Arrow::Function.find("filter")
      assert_equal(Arrow::FilterOptions.gtype,
                   filter_function.options_type)
    end

    def test_take_options
      take_function = Arrow::Function.find("take")
      assert_equal(Arrow::TakeOptions.gtype,
                   take_function.options_type)
    end

    def test_array_sort_options
      array_sort_indices_function = Arrow::Function.find("array_sort_indices")
      assert_equal(Arrow::ArraySortOptions.gtype,
                   array_sort_indices_function.options_type)
    end

    def test_sort_options
      sort_indices_function = Arrow::Function.find("sort_indices")
      assert_equal(Arrow::SortOptions.gtype,
                   sort_indices_function.options_type)
    end

    def test_set_lookup_options
      is_in_function = Arrow::Function.find("is_in")
      assert_equal(Arrow::SetLookupOptions.gtype,
                   is_in_function.options_type)
    end

    def test_variance_options
      stddev_function = Arrow::Function.find("stddev")
      assert_equal(Arrow::VarianceOptions.gtype,
                   stddev_function.options_type)
    end

    def test_round_options
      round_function = Arrow::Function.find("round")
      assert_equal(Arrow::RoundOptions.gtype,
                   round_function.options_type)
    end

    def test_round_to_multiple_options
      round_to_multiple_function = Arrow::Function.find("round_to_multiple")
      assert_equal(Arrow::RoundToMultipleOptions.gtype,
                   round_to_multiple_function.options_type)
    end
  end
end
