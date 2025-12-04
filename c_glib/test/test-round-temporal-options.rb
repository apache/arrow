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

class TestRoundTemporalOptions < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @options = Arrow::RoundTemporalOptions.new
  end

  def test_multiple
    assert_equal(1, @options.multiple)
    @options.multiple = 3
    assert_equal(3, @options.multiple)
  end

  def test_unit
    assert_equal(Arrow::CalendarUnit::DAY, @options.unit)
    @options.unit = :hour
    assert_equal(Arrow::CalendarUnit::HOUR, @options.unit)
  end

  def test_week_starts_monday
    assert_equal(true, @options.week_starts_monday?)
    @options.week_starts_monday = false
    assert_equal(false, @options.week_starts_monday?)
  end

  def test_ceil_is_strictly_greater
    assert_equal(false, @options.ceil_is_strictly_greater?)
    @options.ceil_is_strictly_greater = true
    assert_equal(true, @options.ceil_is_strictly_greater?)
  end

  def test_calendar_based_origin
    assert_equal(false, @options.calendar_based_origin?)
    @options.calendar_based_origin = true
    assert_equal(true, @options.calendar_based_origin?)
  end

  def test_round_temporal_function
    # 1504953190000 = 2017-09-09 10:33:10 UTC
    args = [
      Arrow::ArrayDatum.new(build_timestamp_array(:milli, [1504953190000])),
    ]
    @options.multiple = 5
    @options.unit = :minute
    round_temporal_function = Arrow::Function.find("round_temporal")
    result = round_temporal_function.execute(args, @options).value
    # 1504953300000 = 2017-09-09 10:35:00 UTC
    expected = build_timestamp_array(:milli, [1504953300000])
    assert_equal(expected, result)
  end
end
