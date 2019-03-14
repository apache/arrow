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

class TestBooleanArray < Test::Unit::TestCase
  include Helper::Buildable
  include Helper::Omittable

  def test_new
    assert_equal(build_boolean_array([true, false, nil]),
                 Arrow::BooleanArray.new(3,
                                         Arrow::Buffer.new([0b001].pack("C*")),
                                         Arrow::Buffer.new([0b011].pack("C*")),
                                         -1))
  end

  def test_buffer
    builder = Arrow::BooleanArrayBuilder.new
    builder.append_value(true)
    builder.append_value(false)
    builder.append_value(true)
    array = builder.finish
    assert_equal([0b101].pack("C*"), array.buffer.data.to_s)
  end

  def test_value
    builder = Arrow::BooleanArrayBuilder.new
    builder.append_value(true)
    array = builder.finish
    assert_equal(true, array.get_value(0))
  end

  def test_values
    require_gi_bindings(3, 3, 1)
    builder = Arrow::BooleanArrayBuilder.new
    builder.append_value(true)
    builder.append_value(false)
    builder.append_value(true)
    array = builder.finish
    assert_equal([true, false, true], array.values)
  end

  def test_invert
    assert_equal(build_boolean_array([true, nil, false]),
                 build_boolean_array([false, nil, true]).invert)
  end

  def test_and
    left = build_boolean_array([true, false, nil, true])
    right = build_boolean_array([true, nil, true, false])
    assert_equal(build_boolean_array([true, nil, nil, false]),
                 left.and(right))
  end

  def test_or
    left = build_boolean_array([true, false, nil, false])
    right = build_boolean_array([false, nil, true, false])
    assert_equal(build_boolean_array([true, nil, nil, false]),
                 left.or(right))
  end

  def test_xor
    left = build_boolean_array([true, false, nil, true])
    right = build_boolean_array([false, nil, true, true])
    assert_equal(build_boolean_array([true, nil, nil, false]),
                 left.xor(right))
  end
end
