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

class TestModeOptions < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @options = Arrow::ModeOptions.new
  end

  def test_n
    assert_equal(1, @options.n)
    @options.n = 2
    assert_equal(2, @options.n)
  end

  def test_skip_nulls
    assert do
      @options.skip_nulls?
    end
    @options.skip_nulls = false
    assert do
      not @options.skip_nulls?
    end
  end

  def test_min_count
    assert_equal(0, @options.min_count)
    @options.min_count = 1
    assert_equal(1, @options.min_count)
  end

  def test_mode_function_with_all_options
    args = [
      Arrow::ArrayDatum.new(build_int32_array([1, 2, 2, 3, 3, 3, 4])),
    ]
    @options.n = 2
    @options.skip_nulls = false
    @options.min_count = 2
    mode_function = Arrow::Function.find("mode")
    result = mode_function.execute(args, @options).value
    expected = build_struct_array(
      [
        Arrow::Field.new("mode", Arrow::Int32DataType.new),
        Arrow::Field.new("count", Arrow::Int64DataType.new),
      ],
      [
        {"mode" => 3, "count" => 3},
        {"mode" => 2, "count" => 2},
      ]
    )
    assert_equal(expected, result)
  end
end

