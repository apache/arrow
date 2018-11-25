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

class TestCompressedInputStream < Test::Unit::TestCase
  include Helper::Buildable

  def test_read
    data = "Hello"

    output = StringIO.new
    Zlib::GzipWriter.wrap(output) do |gz|
      gz.write(data)
    end

    codec = Arrow::Codec.new(:gzip)
    buffer = Arrow::Buffer.new(output.string)
    raw_input = Arrow::BufferInputStream.new(buffer)
    input = Arrow::CompressedInputStream.new(codec, raw_input)
    assert_equal(data, input.read(data.bytesize).data.to_s)
    input.close
    raw_input.close
  end

  def test_raw
    buffer = Arrow::Buffer.new("Hello")
    raw_input = Arrow::BufferInputStream.new(buffer)
    codec = Arrow::Codec.new(:gzip)
    input = Arrow::CompressedInputStream.new(codec, raw_input)
    assert_equal(raw_input, input.raw)
  end
end
