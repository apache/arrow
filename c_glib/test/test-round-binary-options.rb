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

class TestRoundBinaryOptions < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @options = Arrow::RoundBinaryOptions.new
  end

  def test_mode
    assert_equal(Arrow::RoundMode::HALF_TO_EVEN, @options.mode)
    @options.mode = :down
    assert_equal(Arrow::RoundMode::DOWN, @options.mode)
  end

  def test_round_binary_function
    args = [
      Arrow::ArrayDatum.new(build_double_array([5.0])),
      Arrow::ArrayDatum.new(build_int32_array([-1])),
    ]
    @options.mode = :half_towards_zero
    round_binary_function = Arrow::Function.find("round_binary")
    result = round_binary_function.execute(args, @options).value
    expected = build_double_array([0.0])
    assert_equal(expected, result)
  end
end
