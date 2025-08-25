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
    test("GC") do
      data = "Hello"
      data_id = data.object_id
      weak_map = ObjectSpace::WeakMap.new
      weak_map[data_id] = data
      _buffer = Arrow::Buffer.new(data)
      data = nil
      GC.start
      assert_equal("Hello", weak_map[data_id])
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
