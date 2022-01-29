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

class TestDayMillisecond < Test::Unit::TestCase
  def test_equal
    day_millisecond = Arrow::DayMillisecond.new(3, 100)
    other_day_millisecond1 = Arrow::DayMillisecond.new(3, 100)
    other_day_millisecond2 = Arrow::DayMillisecond.new(3, 101)
    assert_equal([
                   true,
                   false,
                 ],
                 [
                   day_millisecond == other_day_millisecond1,
                   day_millisecond == other_day_millisecond2,
                 ])
  end

  def test_not_equal
    day_millisecond = Arrow::DayMillisecond.new(3, 100)
    other_day_millisecond1 = Arrow::DayMillisecond.new(3, 100)
    other_day_millisecond2 = Arrow::DayMillisecond.new(3, 101)
    assert_equal([
                   false,
                   true,
                 ],
                 [
                   day_millisecond != other_day_millisecond1,
                   day_millisecond != other_day_millisecond2,
                 ])
  end

  def test_less_than
    day_millisecond = Arrow::DayMillisecond.new(3, 100)
    other_day_millisecond = Arrow::DayMillisecond.new(5, 100)
    assert_equal([
                   true,
                   false,
                 ],
                 [
                   day_millisecond < other_day_millisecond,
                   day_millisecond < day_millisecond,
                 ])
  end
end
