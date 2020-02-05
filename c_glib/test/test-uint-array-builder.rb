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

class TestUIntArrayBuilder < Test::Unit::TestCase
  include Helper::Buildable

  def test_uint8
    values = [0, 2]
    assert_equal(build_uint_array([*values, nil]),
                 Arrow::UInt8Array.new(3,
                                       Arrow::Buffer.new(values.pack("C*")),
                                       Arrow::Buffer.new([0b011].pack("C*")),
                                       -1))
  end

  def test_uint16
    border_value = 2 ** 8
    values = [0, border_value]
    assert_equal(build_uint_array([*values, nil]),
                 Arrow::UInt16Array.new(3,
                                       Arrow::Buffer.new(values.pack("S*")),
                                       Arrow::Buffer.new([0b011].pack("C*")),
                                       -1))
  end

  def test_uint32
    border_value = 2 ** 16
    values = [0, border_value]
    assert_equal(build_uint_array([*values, nil]),
                 Arrow::UInt32Array.new(3,
                                       Arrow::Buffer.new(values.pack("L*")),
                                       Arrow::Buffer.new([0b011].pack("C*")),
                                       -1))
  end

  def test_uint64
    border_value = 2 ** 32
    values = [0, border_value]
    assert_equal(build_uint_array([*values, nil]),
                 Arrow::UInt64Array.new(3,
                                        Arrow::Buffer.new(values.pack("Q*")),
                                        Arrow::Buffer.new([0b011].pack("C*")),
                                        -1))
  end
end
