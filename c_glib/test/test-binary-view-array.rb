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
    view_bytes = 16
    buffer_string = "test"
    view_data = [buffer_string.size].pack("l")
    view_data += buffer_string.ljust(12, "\x00")

    view = Arrow::Buffer.new(view_data)
    data_buffer = Arrow::Buffer.new(buffer_string)
    bitmap = Arrow::Buffer.new([0b1].pack("C*"))

    binary_view = Arrow::BinaryViewArray.new(1, view, [data_buffer], bitmap, 0, 0)
    assert do
      str_view.validate_full
    end
    assert_equal(buffer_string, str_view.get_value(0).to_s)
  end
end
