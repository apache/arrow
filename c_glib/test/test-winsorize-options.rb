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

class TestWinsorizeOptions < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @options = Arrow::WinsorizeOptions.new
  end

  def test_lower_limit_property
    assert_equal(0.0, @options.lower_limit)
    @options.lower_limit = 0.05
    assert_equal(0.05, @options.lower_limit)
  end

  def test_upper_limit_property
    assert_equal(1.0, @options.upper_limit)
    @options.upper_limit = 0.95
    assert_equal(0.95, @options.upper_limit)
  end

  def test_winsorize_function
    args = [
      Arrow::ArrayDatum.new(build_double_array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 
10.0])),
    ]
    @options.lower_limit = 0.1
    @options.upper_limit = 0.9
    winsorize_function = Arrow::Function.find("winsorize")
    result = winsorize_function.execute(args, @options).value
    expected = build_double_array([2.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 9.0])
    assert_equal(expected, result)
  end
end

