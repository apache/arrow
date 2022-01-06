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
end
