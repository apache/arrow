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

class TestTime32Array < Test::Unit::TestCase
  include Helper::Buildable
  include Helper::Omittable

  def test_new
    midnight = 0
    after_midnight = 60 * 10 # 00:10:00
    raw_data = [midnight, after_midnight]
    data_type = Arrow::Time32DataType.new(:second)
    assert_equal(build_time32_array(:second, [*raw_data, nil]),
                 Arrow::Time32Array.new(data_type,
                                        3,
                                        Arrow::Buffer.new(raw_data.pack("l*")),
                                        Arrow::Buffer.new([0b011].pack("C*")),
                                        -1))
  end

  def test_buffer
    midnight = 0
    after_midnight = 60 * 10 # 00:10:00
    raw_data = [midnight, after_midnight]
    array = build_time32_array(:second, raw_data)
    assert_equal(raw_data.pack("l*"),
                 array.buffer.data.to_s)
  end

  def test_value
    after_midnight = 60 * 10 # 00:10:00
    array = build_time32_array(:second, [after_midnight])
    assert_equal(after_midnight, array.get_value(0))
  end

  def test_values
    midnight = 0
    after_midnight = 60 * 10 # 00:10:00
    raw_data = [midnight, after_midnight]
    array = build_time32_array(:second, raw_data)
    assert_equal(raw_data, array.values)
  end

  sub_test_case("unit") do
    def test_second
      array = build_time32_array(:second, [])
      assert_equal(Arrow::TimeUnit::SECOND, array.value_data_type.unit)
    end

    def test_milli
      array = build_time32_array(:milli, [])
      assert_equal(Arrow::TimeUnit::MILLI, array.value_data_type.unit)
    end
  end
end
