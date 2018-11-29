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

class TestMutableBuffer < Test::Unit::TestCase
  def setup
    @data = "Hello"
    @buffer = Arrow::MutableBuffer.new(@data)
  end

  def test_new_bytes
    bytes_data = GLib::Bytes.new(@data)
    buffer = Arrow::MutableBuffer.new(bytes_data)
    if GLib.check_binding_version?(3, 2, 2)
      assert_equal(bytes_data.pointer, buffer.mutable_data.pointer)
    else
      assert_equal(@data, buffer.mutable_data.to_s)
    end
  end

  def test_mutable?
    assert do
      @buffer.mutable?
    end
  end

  def test_mutable_data
    assert_equal(@data, @buffer.mutable_data.to_s)
  end

  def test_slice
    sliced_buffer = @buffer.slice(1, 3)
    assert_equal(@data[1, 3], sliced_buffer.data.to_s)
  end

  sub_test_case("#set_data") do
    test("offset") do
      @buffer.set_data(1, "EL")
      assert_equal("HELlo", @buffer.data.to_s)
    end

    test("replace") do
      @buffer.set_data(0, "World")
      assert_equal("World", @buffer.data.to_s)
    end

    test("offset: too large") do
      message = "[mutable-buffer][set-data]: Data is too large: <(5 + 1) > (5)>"
      assert_raise(Arrow::Error::Invalid.new(message)) do
        @buffer.set_data(5, "X")
      end
    end

    test("data too large") do
      message = "[mutable-buffer][set-data]: Data is too large: <(0 + 6) > (5)>"
      assert_raise(Arrow::Error::Invalid.new(message)) do
        @buffer.set_data(0, @data + "!")
      end
    end
  end
end
