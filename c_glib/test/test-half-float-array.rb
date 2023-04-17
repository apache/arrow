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

class TestHalfFloatArray < Test::Unit::TestCase
  include Helper::Buildable
  include Helper::Omittable

  def setup
    @one = 0x3c00
    @zero = 0x0000
    @positive_infinity = 0x8c00
  end

  def test_new
    values = [@one, @zero, @positive_infinity, nil]
    data = values[0..-2].pack("S*")
    null_bitmap = [0b0111].pack("C*")
    assert_equal(build_half_float_array(values),
                 Arrow::HalfFloatArray.new(4,
                                           Arrow::Buffer.new(data),
                                           Arrow::Buffer.new(null_bitmap),
                                           -1))
  end

  def test_buffer
    builder = Arrow::HalfFloatArrayBuilder.new
    builder.append_value(@one)
    builder.append_value(@zero)
    builder.append_value(@positive_infinity)
    array = builder.finish
    assert_equal([@one, @zero, @positive_infinity].pack("S*"),
                 array.buffer.data.to_s)
  end

  def test_value
    builder = Arrow::HalfFloatArrayBuilder.new
    builder.append_value(@one)
    array = builder.finish
    assert_in_delta(@one, array.get_value(0))
  end

  def test_values
    require_gi_bindings(3, 1, 7)
    builder = Arrow::HalfFloatArrayBuilder.new
    builder.append_value(@one)
    builder.append_value(@zero)
    builder.append_value(@positive_infinity)
    array = builder.finish
    assert_equal([@one, @zero, @positive_infinity],
                 array.values)
  end
end
