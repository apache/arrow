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

class TestWeekOptions < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @options = Arrow::WeekOptions.new
  end

  def test_week_starts_monday_property
    assert do
      @options.week_starts_monday?
    end
    @options.week_starts_monday = false
    assert do
      !@options.week_starts_monday?
    end
  end

  def test_count_from_zero_property
    assert do
      !@options.count_from_zero?
    end
    @options.count_from_zero = true
    assert do
      @options.count_from_zero?
    end
  end

  def test_first_week_is_fully_in_year_property
    assert do
      !@options.first_week_is_fully_in_year?
    end
    @options.first_week_is_fully_in_year = true
    assert do
      @options.first_week_is_fully_in_year?
    end
  end

  def test_week_function_with_week_starts_monday
    omit("Missing tzdata on Windows") if Gem.win_platform?
    # January 1, 2023 (Sunday)
    args = [
      Arrow::ArrayDatum.new(build_timestamp_array(:milli, [1672531200000])),
    ]
    @options.week_starts_monday = true
    week_function = Arrow::Function.find("week")
    result = week_function.execute(args, @options).value
    assert_equal(build_int64_array([52]), result)

    @options.week_starts_monday = false
    result = week_function.execute(args, @options).value
    assert_equal(build_int64_array([1]), result)
  end
end
