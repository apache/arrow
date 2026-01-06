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

class TestDayOfWeekOptions < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @options = Arrow::DayOfWeekOptions.new
  end

  def test_count_from_zero_property
    assert do
      @options.count_from_zero?
    end
    @options.count_from_zero = false
    assert do
      !@options.count_from_zero?
    end
  end

  def test_week_start_property
    assert_equal(1, @options.week_start)
    @options.week_start = 7
    assert_equal(7, @options.week_start)
  end

  def test_day_of_week_function_with_count_from_zero_false
    omit("std::chrono not available on Windows MinGW") if Gem.win_platform?
    args = [
      # 2017-09-09T10:33:10Z (Saturday)
      Arrow::ArrayDatum.new(build_timestamp_array(:milli, [1504953190000])),
    ]
    @options.count_from_zero = false
    day_of_week_function = Arrow::Function.find("day_of_week")
    assert_equal(build_int64_array([6]),
                 day_of_week_function.execute(args, @options).value)
  end

  def test_day_of_week_function_with_week_start
    omit("std::chrono not available on Windows MinGW") if Gem.win_platform?
    args = [
      # 2017-09-09T10:33:10Z (Saturday)
      Arrow::ArrayDatum.new(build_timestamp_array(:milli, [1504953190000])),
    ]
    @options.week_start = 2
    day_of_week_function = Arrow::Function.find("day_of_week")
    assert_equal(build_int64_array([4]),
                 day_of_week_function.execute(args, @options).value)
  end
end
