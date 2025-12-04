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

class TestPairwiseOptions < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @options = Arrow::PairwiseOptions.new
  end

  def test_periods_property
    assert_equal(1, @options.periods)
    @options.periods = 2
    assert_equal(2, @options.periods)
    @options.periods = -1
    assert_equal(-1, @options.periods)
  end

  def test_pairwise_diff_function
    args = [
      Arrow::ArrayDatum.new(build_int32_array([1, 2, 4, 7, 11])),
    ]
    pairwise_diff_function = Arrow::Function.find("pairwise_diff")
    @options.periods = 2
    result = pairwise_diff_function.execute(args, @options).value
    assert_equal(build_int32_array([nil, nil, 3, 5, 7]), result)
  end
end

