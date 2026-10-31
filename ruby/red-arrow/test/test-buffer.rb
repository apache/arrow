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

class BufferTest < Test::Unit::TestCase
  sub_test_case(".new") do
    test("copies String data") do
      data = "Hello"
      buffer = Arrow::Buffer.new(data)
      data.replace("World")
      assert_equal("Hello", buffer.data.to_s)
    end

    data("String" => false,
         "GLib::Bytes" => true)
    test("GC with an array holding the native buffer") do |use_bytes|
      chunked_array = build_buffer_backed_array(use_bytes)
      GC.start
      Array.new(10_000) { [7, 7, 7].pack("l*") }
      assert_equal([1, 2, 3], chunked_array.to_a)
    end

    data("String" => false,
         "GLib::Bytes" => true)
    test("GC with a slice holding the native buffer") do |use_bytes|
      sliced_array = build_buffer_backed_array(use_bytes).slice(1, 2)
      GC.start
      Array.new(10_000) { [7, 7, 7].pack("l*") }
      assert_equal([2, 3], sliced_array.to_a)
    end

    def build_buffer_backed_array(use_bytes)
      data = [1, 2, 3].pack("l*")
      data = GLib::Bytes.new(data) if use_bytes
      buffer = Arrow::Buffer.new(data)
      array = Arrow::Int32Array.new(3, buffer, nil, 0)
      Arrow::ChunkedArray.new([array])
    end
  end

  sub_test_case("instance methods") do
    def setup
      @buffer = Arrow::Buffer.new("Hello")
    end

    sub_test_case("#==") do
      test("Arrow::Buffer") do
        assert do
          @buffer == @buffer
        end
      end

      test("not Arrow::Buffer") do
        assert do
          not (@buffer == 29)
        end
      end
    end
  end
end
