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

class TestMonthDayNanoIntervalArray < Test::Unit::TestCase
  def test_value
    month_day_nano = Arrow::MonthDayNano.new(3, 30, 100)

    builder = Arrow::MonthDayNanoIntervalArrayBuilder.new
    builder.append_value(month_day_nano)
    array = builder.finish
    assert_equal(month_day_nano, array.get_value(0))
  end

  def test_values
    first_month_day_nano = Arrow::MonthDayNano.new(1, 10, 100)
    second_month_day_nano = Arrow::MonthDayNano.new(3, 30, 100)

    builder = Arrow::MonthDayNanoIntervalArrayBuilder.new
    builder.append_value(first_month_day_nano)
    builder.append_value(second_month_day_nano)
    array = builder.finish
    assert_equal([first_month_day_nano, second_month_day_nano], array.values)
  end
end
