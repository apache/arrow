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

class TestCompressedOutputStream < Test::Unit::TestCase
  include Helper::Buildable

  def test_write
    data = "Hello"
    buffer = Arrow::ResizableBuffer.new(8)
    raw_output = Arrow::BufferOutputStream.new(buffer)
    codec = Arrow::Codec.new(:gzip)
    output = Arrow::CompressedOutputStream.new(codec, raw_output)
    output.write(data)
    output.close

    input = StringIO.new(buffer.data.to_s)
    Zlib::GzipReader.wrap(input) do |gz|
      assert_equal(data, gz.read)
    end
  end

  def test_raw
    buffer = Arrow::ResizableBuffer.new(8)
    raw_output = Arrow::BufferOutputStream.new(buffer)
    codec = Arrow::Codec.new(:gzip)
    output = Arrow::CompressedOutputStream.new(codec, raw_output)
    assert_equal(raw_output, output.raw)
  end
end
