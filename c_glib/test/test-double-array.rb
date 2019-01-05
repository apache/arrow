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

class TestDoubleArray < Test::Unit::TestCase
  include Helper::Buildable
  include Helper::Omittable

  def test_new
    assert_equal(build_double_array([-1.1, 2.2, nil]),
                 Arrow::DoubleArray.new(3,
                                        Arrow::Buffer.new([-1.1, 2.2].pack("d*")),
                                        Arrow::Buffer.new([0b011].pack("C*")),
                                        -1))
  end

  def test_buffer
    builder = Arrow::DoubleArrayBuilder.new
    builder.append_value(-1.1)
    builder.append_value(2.2)
    builder.append_value(-4.4)
    array = builder.finish
    assert_equal([-1.1, 2.2, -4.4].pack("d*"), array.buffer.data.to_s)
  end

  def test_value
    builder = Arrow::DoubleArrayBuilder.new
    builder.append_value(1.5)
    array = builder.finish
    assert_in_delta(1.5, array.get_value(0))
  end

  def test_values
    require_gi_bindings(3, 1, 7)
    builder = Arrow::DoubleArrayBuilder.new
    builder.append_value(1.5)
    builder.append_value(3)
    builder.append_value(4.5)
    array = builder.finish
    assert_equal([1.5, 3.0, 4.5], array.values)
  end
end
