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

class TestDurationArray < Test::Unit::TestCase
  include Helper::Buildable

  def test_new
    raw_data = [0, 1000]
    data_type = Arrow::DurationDataType.new(:second)
    assert_equal(build_duration_array(:second, [*raw_data, nil]),
                 Arrow::DurationArray.new(data_type,
                                          3,
                                          Arrow::Buffer.new(raw_data.pack("q*")),
                                          Arrow::Buffer.new([0b011].pack("C*")),
                                          -1))
  end

  def test_buffer
    raw_data = [0, 1000]
    array = build_duration_array(:milli, raw_data)
    assert_equal(raw_data.pack("q*"),
                 array.buffer.data.to_s)
  end

  def test_value
    duration = 1000
    array = build_duration_array(:milli, [duration])
    assert_equal(duration, array.get_value(0))
  end

  def test_values
    raw_data = [0, 1000]
    array = build_duration_array(:milli, raw_data)
    assert_equal(raw_data, array.values)
  end
end
