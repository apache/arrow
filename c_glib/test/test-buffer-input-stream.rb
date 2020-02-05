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

class TestBufferInputStream < Test::Unit::TestCase
  def test_read
    buffer = Arrow::Buffer.new("Hello World")
    buffer_input_stream = Arrow::BufferInputStream.new(buffer)
    read_buffer = buffer_input_stream.read(5)
    assert_equal("Hello", read_buffer.data.to_s)
  end

  def test_advance
    buffer = Arrow::Buffer.new("Hello World")
    buffer_input_stream = Arrow::BufferInputStream.new(buffer)
    buffer_input_stream.advance(6)
    read_buffer = buffer_input_stream.read(5)
    assert_equal("World", read_buffer.data.to_s)
  end

  def test_align
    buffer = Arrow::Buffer.new("Hello World")
    buffer_input_stream = Arrow::BufferInputStream.new(buffer)
    buffer_input_stream.advance(3)
    buffer_input_stream.align(8)
    read_buffer = buffer_input_stream.read(3)
    assert_equal("rld", read_buffer.data.to_s)
  end

  def test_peek
    buffer = Arrow::Buffer.new("Hello World")
    buffer_input_stream = Arrow::BufferInputStream.new(buffer)
    peeked_data = buffer_input_stream.peek(5)
    assert_equal(buffer_input_stream.read(5).data.to_s,
                 peeked_data.to_s)
  end

  def test_gio_input_stream
    # U+3042 HIRAGANA LETTER A
    data = "\u3042"
    convert_encoding = "cp932"
    buffer = Arrow::Buffer.new(data)
    buffer_input_stream = Arrow::BufferInputStream.new(buffer)
    converter = Gio::CharsetConverter.new(convert_encoding, "UTF-8")
    convert_input_stream =
      Gio::ConverterInputStream.new(buffer_input_stream, converter)
    gio_input_stream = Arrow::GIOInputStream.new(convert_input_stream)
    raw_read_data = gio_input_stream.read(10).data.to_s
    assert_equal(data.encode(convert_encoding),
                 raw_read_data.dup.force_encoding(convert_encoding))
  end
end
