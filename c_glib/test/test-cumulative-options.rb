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

class TestCumulativeOptions < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @options = Arrow::CumulativeOptions.new
  end

  def test_start_property
    assert_nil(@options.start)
    start_scalar = Arrow::Int64Scalar.new(10)
    @options.start = start_scalar
    assert_equal(start_scalar, @options.start)
    @options.start = nil
    assert_nil(@options.start)
  end

  def test_skip_nulls_property
    assert do
      !@options.skip_nulls?
    end
    @options.skip_nulls = true
    assert do
      @options.skip_nulls?
    end
  end

  def test_cumulative_sum_with_skip_nulls
    args = [
      Arrow::ArrayDatum.new(build_int64_array([1, 2, 3, nil, 4, 5])),
    ]
    @options.skip_nulls = true
    cumulative_sum_function = Arrow::Function.find("cumulative_sum")
    assert_equal(build_int64_array([1, 3, 6, nil, 10, 15]),
                 cumulative_sum_function.execute(args, @options).value)
  end

  def test_cumulative_sum_with_start
    args = [
      Arrow::ArrayDatum.new(build_int64_array([1, 2, 3])),
    ]
    @options.start = Arrow::Int64Scalar.new(10)
    cumulative_sum_function = Arrow::Function.find("cumulative_sum")
    assert_equal(build_int64_array([11, 13, 16]),
                 cumulative_sum_function.execute(args, @options).value)
  end
end
