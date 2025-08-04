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

class TestBinaryViewArray < Test::Unit::TestCase
  def test_new
    short_binary_data = "test"
    short_view_buffer_space = 12
    short_view_buffer = [short_binary_data.size].pack("l")
    short_view_buffer += short_binary_data.ljust(short_view_buffer_space, "\x00")

    arrow_view_buffer = Arrow::Buffer.new(short_view_buffer)
    arrow_data_buffer = Arrow::Buffer.new(short_binary_data)
    bitmap = Arrow::Buffer.new([0b1].pack("C*"))

    binary_view_array = Arrow::BinaryViewArray.new(1,
                                                   arrow_view_buffer,
                                                   [arrow_data_buffer],
                                                   bitmap,
                                                   0,
                                                   0)
    assert do
      binary_view_array.validate_full
    end
    assert_equal(short_binary_data, binary_view_array.get_value(0).to_s)
  end
end
