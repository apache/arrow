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

class TestFixedSizeBinaryArray < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @data_type = Arrow::FixedSizeBinaryDataType.new(4)
  end

  def test_new
    args = [
      @data_type,
      3,
      Arrow::Buffer.new("0123abcd0000"),
      Arrow::Buffer.new([0b011].pack("C*")),
      -1,
    ]
    assert_equal(build_fixed_size_binary_array(@data_type,
                                               ["0123", "abcd", nil]),
                 Arrow::FixedSizeBinaryArray.new(*args))
  end

  def test_buffer
    array = build_fixed_size_binary_array(@data_type,
                                          ["0123", "abcd", "0000"])
    assert_equal("0123abcd0000", array.buffer.data.to_s)
  end

  def test_byte_width
    array = build_fixed_size_binary_array(@data_type, ["0123"])
    assert_equal(@data_type.byte_width, array.byte_width)
  end

  def test_value
    data_type = Arrow::FixedSizeBinaryDataType.new(40)
    values = ["0123" * 10]
    value = build_fixed_size_binary_array(data_type, values).get_value(0)
    GC.start
    assert_equal(values[0], value.to_s)
  end

  def test_values_bytes
    data_type = Arrow::FixedSizeBinaryDataType.new(40)
    values = ["0123" * 10, "abcd" * 10, "0000" * 10]
    values_bytes =
      build_fixed_size_binary_array(data_type, values).values_bytes
    GC.start
    assert_equal(values.join(""), values_bytes.to_s)
  end
end
