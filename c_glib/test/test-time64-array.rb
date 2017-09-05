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

class TestTime64Array < Test::Unit::TestCase
  include Helper::Buildable
  include Helper::Omittable

  def test_new
    midnight = 0
    after_midnight = 60 * 10 * 1000 * 1000 # 00:10:00.000000
    raw_data = [midnight, after_midnight]
    data_type = Arrow::Time64DataType.new(:micro)
    assert_equal(build_time64_array(:micro, [*raw_data, nil]),
                 Arrow::Time64Array.new(data_type,
                                        3,
                                        Arrow::Buffer.new(raw_data.pack("q*")),
                                        Arrow::Buffer.new([0b011].pack("C*")),
                                        -1))
  end

  def test_buffer
    midnight = 0
    after_midnight = 60 * 10 * 1000 * 1000 # 00:10:00.000000
    raw_data = [midnight, after_midnight]
    array = build_time64_array(:micro, raw_data)
    assert_equal(raw_data.pack("q*"),
                 array.buffer.data.to_s)
  end

  def test_value
    after_midnight = 60 * 10 * 1000 * 1000 # 00:10:00.000000
    array = build_time64_array(:micro, [after_midnight])
    assert_equal(after_midnight, array.get_value(0))
  end

  def test_values
    midnight = 0
    after_midnight = 60 * 10 * 1000 * 1000 # 00:10:00.000000
    raw_data = [midnight, after_midnight]
    array = build_time64_array(:micro, raw_data)
    assert_equal(raw_data, array.values)
  end
end
