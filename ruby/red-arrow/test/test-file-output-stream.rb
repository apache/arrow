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

class TestFileOutputStream < Test::Unit::TestCase
  sub_test_case(".open") do
    def setup
      @file = Tempfile.open("arrow-file-output-stream")
      @file.write("Hello")
      @file.close
    end

    def test_default
      Arrow::FileOutputStream.open(@file.path) do |file|
        file.write(" World")
      end
      assert_equal(" World", File.read(@file.path))
    end

    def test_options_append
      Arrow::FileOutputStream.open(@file.path, append: true) do |file|
        file.write(" World")
      end
      assert_equal("Hello World", File.read(@file.path))
    end

    def test_append_true
      Arrow::FileOutputStream.open(@file.path, true) do |file|
        file.write(" World")
      end
      assert_equal("Hello World", File.read(@file.path))
    end

    def test_append_false
      Arrow::FileOutputStream.open(@file.path, false) do |file|
        file.write(" World")
      end
      assert_equal(" World", File.read(@file.path))
    end
  end
end
