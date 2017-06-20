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

class TestUInt64Array < Test::Unit::TestCase
  include Helper::Buildable

  def test_new
    assert_equal(build_uint64_array([1, 2, nil]),
                 Arrow::UInt64Array.new(3,
                                        Arrow::Buffer.new([1, 2].pack("Q*")),
                                        Arrow::Buffer.new([0b011].pack("C*")),
                                        -1))
  end

  def test_buffer
    builder = Arrow::UInt64ArrayBuilder.new
    builder.append(1)
    builder.append(2)
    builder.append(4)
    array = builder.finish
    assert_equal([1, 2, 4].pack("Q*"), array.buffer.data.to_s)
  end

  def test_value
    builder = Arrow::UInt64ArrayBuilder.new
    builder.append(1)
    array = builder.finish
    assert_equal(1, array.get_value(0))
  end
end
