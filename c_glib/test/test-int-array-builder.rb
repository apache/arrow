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

class TestIntArrayBuilder < Test::Unit::TestCase
  include Helper::Buildable

  def test_int8
    values = [-1, 2]
    assert_equal(build_int_array([*values, nil]),
                 Arrow::Int8Array.new(3,
                                      Arrow::Buffer.new(values.pack("c*")),
                                      Arrow::Buffer.new([0b011].pack("C*")),
                                      -1))
  end

  def test_int16
    border_value = (2 ** (8 - 1))
    values = [-1, border_value]
    assert_equal(build_int_array([*values, nil]),
                 Arrow::Int16Array.new(3,
                                       Arrow::Buffer.new(values.pack("s*")),
                                       Arrow::Buffer.new([0b011].pack("C*")),
                                       -1))
  end

  def test_int32
    border_value = (2 ** (16 - 1))
    values = [-1, border_value]
    assert_equal(build_int_array([*values, nil]),
                 Arrow::Int32Array.new(3,
                                       Arrow::Buffer.new(values.pack("l*")),
                                       Arrow::Buffer.new([0b011].pack("C*")),
                                       -1))
  end

  def test_int64
    border_value = (2 ** (32 - 1))
    values = [-1, border_value]
    assert_equal(build_int_array([*values, nil]),
                 Arrow::Int64Array.new(3,
                                       Arrow::Buffer.new(values.pack("q*")),
                                       Arrow::Buffer.new([0b011].pack("C*")),
                                       -1))
  end
end
