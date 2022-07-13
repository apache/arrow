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

class TestQuantileOptions < Test::Unit::TestCase
  def setup
    @options = Arrow::QuantileOptions.new
  end

  def test_interpolation
    assert_equal(Arrow::QuantileInterpolation::LINEAR, @options.interpolation)
    @options.interpolation = :lower
    assert_equal(Arrow::QuantileInterpolation::LOWER, @options.interpolation)
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
    @options.min_count = 29
    assert_equal(29, @options.min_count)
  end

  def test_q
    assert_equal([0.5], @options.qs)
    @options.qs = [0.1, 0.2, 0.9]
    assert_equal([0.1, 0.2, 0.9], @options.qs)
    @options.q = 0.7
    assert_equal([0.7], @options.qs)
  end
end
