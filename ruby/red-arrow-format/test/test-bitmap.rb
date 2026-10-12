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

class TestBitmap < Test::Unit::TestCase
  sub_test_case("#==") do
    def test_no_slice
      buffer1 = IO::Buffer.for("\xf0")
      buffer2 = IO::Buffer.for("\xf0")
      assert_equal(ArrowFormat::Bitmap.new(buffer1, 0, 8),
                   ArrowFormat::Bitmap.new(buffer2, 0, 8))
    end

    def test_sliced
      buffer1 = IO::Buffer.for("\xf0")
      buffer2 = IO::Buffer.for("\x00\xf0")
      assert_equal(ArrowFormat::Bitmap.new(buffer1, 0, 8),
                   ArrowFormat::Bitmap.new(buffer2, 8, 8))
    end
  end
end
