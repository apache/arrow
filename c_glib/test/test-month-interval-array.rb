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

class TestMonthIntervalArray < Test::Unit::TestCase
  def test_buffer
    builder = Arrow::MonthIntervalArrayBuilder.new
    builder.append_value(0)
    builder.append_value(1)
    builder.append_value(12)
    array = builder.finish
    assert_equal([0, 1, 12].pack("l*"),
                 array.buffer.data.to_s)
  end

  def test_value
    builder = Arrow::MonthIntervalArrayBuilder.new
    builder.append_value(1)
    array = builder.finish
    assert_equal(1, array.get_value(0))
  end

  def test_values
    builder = Arrow::MonthIntervalArrayBuilder.new
    builder.append_value(0)
    builder.append_value(1)
    builder.append_value(12)
    array = builder.finish
    assert_equal([0, 1, 12], array.values)
  end
end
