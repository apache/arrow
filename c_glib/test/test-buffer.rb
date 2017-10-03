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

class TestBuffer < Test::Unit::TestCase
  include Helper::Omittable

  def setup
    @data = "Hello"
    @buffer = Arrow::Buffer.new(@data)
  end

  def test_equal
    assert_equal(@buffer,
                 Arrow::Buffer.new(@data.dup))
  end

  def test_equal_n_bytes
    buffer1 = Arrow::Buffer.new("Hello!")
    buffer2 = Arrow::Buffer.new("Hello World!")
    assert do
      buffer1.equal_n_bytes(buffer2, 5)
    end
  end

  def test_mutable?
    assert do
      not @buffer.mutable?
    end
  end

  def test_capacity
    assert_equal(@data.bytesize, @buffer.capacity)
  end

  def test_data
    assert_equal(@data, @buffer.data.to_s)
  end

  def test_mutable_data
    require_gi_bindings(3, 1, 2)
    assert_nil(@buffer.mutable_data)
  end

  def test_size
    assert_equal(@data.bytesize, @buffer.size)
  end

  def test_parent
    assert_nil(@buffer.parent)
  end

  def test_copy
    copied_buffer = @buffer.copy(1, 3)
    assert_equal(@data[1, 3], copied_buffer.data.to_s)
  end

  def test_slice
    sliced_buffer = @buffer.slice(1, 3)
    assert_equal(@data[1, 3], sliced_buffer.data.to_s)
  end
end
